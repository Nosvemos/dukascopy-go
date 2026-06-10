package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/checkpoint"
	"github.com/Nosvemos/dukascopy-go/pkg/csvout"
	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

func runDownload(args []string, stdout io.Writer, stderr io.Writer) error {
	fs := flag.NewFlagSet("download", flag.ContinueOnError)
	fs.SetOutput(stdout)
	fs.Usage = func() {
		fmt.Fprintf(stdout, "%sdownload:%s Download historical Dukascopy tick and bar data to CSV/Parquet\n\n", colorize(colorCyan), colorize(colorReset))
		fmt.Fprint(stdout, "Usage:\n  dukascopy-go download [options]\n\nOptions:\n")
		fs.PrintDefaults()
		fmt.Fprint(stdout, "\nExamples:\n  dukascopy-go download --symbol eurusd --timeframe m1 --last 30d --output ./eurusd_m1.csv\n  dukascopy-go download --symbol xauusd,gbpusd --timeframe d1 --from 2024-01-01 --to 2024-02-01 --output ./data/\n  dukascopy-go download --symbol eurusd --timeframe tick --last 1d --output ./eurusd_tick.parquet --parallelism 4 --partition day\n")
	}

	symbol := fs.String("symbol", "", "instrument symbol such as xauusd or eur/usd")
	timeframe := fs.String("timeframe", "m1", "tick, m1, m3, m5, m15, m30, h1, h4, d1, w1, mn1")
	granularity := fs.String("granularity", "", "deprecated alias for --timeframe")
	side := fs.String("side", "bid", "bid or ask")
	simpleOutput := fs.Bool("simple", false, "write the reduced CSV column set")
	fullOutput := fs.Bool("full", false, "write the full CSV column set with bid/ask columns")
	fusedOutput := fs.Bool("fused", false, "write the fused CSV column set with bid/ask and spread but no mid columns")
	customColumns := fs.String("custom-columns", "", "comma-separated CSV column list")
	lastValue := fs.String("last", "", "download the last N duration (e.g. 30d, 1y, 6mo) instead of --from/--to")
	fromValue := fs.String("from", "", "inclusive RFC3339 start timestamp")
	toValue := fs.String("to", "", "inclusive RFC3339 end timestamp")
	outputPath := fs.String("output", "", "target CSV path")
	live := fs.Bool("live", false, "keep polling and append newly completed rows until interrupted")
	pollInterval := fs.Duration("poll-interval", 5*time.Second, "delay between live polling cycles such as 5s or 1m")
	retries := fs.Int("retries", 3, "retry count for transient HTTP errors")
	retryBackoff := fs.Duration("retry-backoff", 500*time.Millisecond, "base retry backoff duration such as 500ms or 2s")
	rateLimit := fs.Duration("rate-limit", 0, "minimum delay between HTTP requests such as 100ms or 1s")
	progress := fs.Bool("progress", false, "force-enable the interactive progress dashboard")
	noProgress := fs.Bool("no-progress", false, "disable the interactive progress dashboard")
	tuiTheme := fs.String("tui-theme", "default", "interactive dashboard color theme: default, catppuccin, nord, gruvbox, dracula")
	resume := fs.Bool("resume", false, "append missing rows to an existing CSV when possible")
	partition := fs.String("partition", "none", "partition mode: none, auto, hour, day, week, month, year")
	parallelism := fs.Int("parallelism", 1, "partition worker count")
	checkpointManifest := fs.String("checkpoint-manifest", "", "optional checkpoint manifest path for partitioned downloads")
	baseURL := fs.String("base-url", readBaseURL(), "Dukascopy API base URL")
	timezone := fs.String("timezone", "UTC", "target output timezone location (e.g. Europe/London, EET, EST)")
	timestampFormat := fs.String("timestamp-format", "", "custom timestamp layout format")
	csvDelimiter := fs.String("csv-delimiter", ",", "custom CSV separator character")
	noHeader := fs.Bool("no-header", false, "suppress header row in output CSV files")
	preset := fs.String("preset", "", "backtest output preset profile (mt4, mt5, backtrader, ninjatrader)")
	proxyFile := fs.String("proxy-file", "", "optional proxy list file path (HTTP/SOCKS5)")
	engine := fs.String("engine", "jetta", "downloader engine: jetta or datafeed")
	fillGaps := fs.String("fill-gaps", "none", "gap filling mode: none, forward")
	cacheDir := fs.String("cache-dir", "./.dukascopy_cache", "temporary cache directory path")
	keepCache := fs.Bool("keep-cache", false, "keep temporary cache files after successful download")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if strings.Contains(*symbol, ",") {
		symbols := strings.Split(*symbol, ",")
		fmt.Fprintf(stdout, "%sbatch%s downloading %d symbols...\n", colorize(colorCyan), colorize(colorReset), len(symbols))
		for _, sym := range symbols {
			sym = strings.TrimSpace(sym)
			if sym == "" {
				continue
			}

			formattedOutput := formatMultiSymbolOutputPath(*outputPath, sym)

			var nextArgs []string
			for i := 0; i < len(args); {
				arg := args[i]
				if arg == "--symbol" || arg == "-symbol" || arg == "--output" || arg == "-output" {
					i += 2
				} else if strings.HasPrefix(arg, "--symbol=") || strings.HasPrefix(arg, "-symbol=") || strings.HasPrefix(arg, "--output=") || strings.HasPrefix(arg, "-output=") {
					i += 1
				} else {
					nextArgs = append(nextArgs, arg)
					i++
				}
			}
			nextArgs = append(nextArgs, "--symbol", sym, "--output", formattedOutput)

			fmt.Fprintf(stdout, "%sbatch%s starting download for %s -> %s\n", colorize(colorCyan), colorize(colorReset), sym, formattedOutput)
			if err := runDownload(nextArgs, stdout, stderr); err != nil {
				return fmt.Errorf("download for symbol %s failed: %w", sym, err)
			}
		}
		return nil
	}

	engineVal := dukascopy.Engine(strings.ToLower(strings.TrimSpace(*engine)))
	if engineVal != dukascopy.EngineJetta && engineVal != dukascopy.EngineDatafeed {
		return fmt.Errorf("unknown engine %q (supported: jetta, datafeed)", *engine)
	}
	if err := applyDownloadConfigDefaults(
		fs,
		timeframe,
		side,
		simpleOutput,
		fullOutput,
		customColumns,
		live,
		pollInterval,
		retries,
		retryBackoff,
		rateLimit,
		progress,
		resume,
		partition,
		parallelism,
		checkpointManifest,
		baseURL,
		cacheDir,
		keepCache,
	); err != nil {
		return err
	}

	if *preset != "" {
		switch strings.ToLower(strings.TrimSpace(*preset)) {
		case "mt4":
			*noHeader = true
			if *timestampFormat == "" {
				*timestampFormat = "2006.01.02 15:04"
			}
			*simpleOutput = true
		case "mt5":
			*noHeader = true
			if *timestampFormat == "" {
				*timestampFormat = "2006.01.02 15:04:05"
			}
			*simpleOutput = true
		case "backtrader":
			if *timestampFormat == "" {
				*timestampFormat = "2006-01-02 15:04:05"
			}
			*simpleOutput = true
		case "ninjatrader":
			*noHeader = true
			*csvDelimiter = ";"
			if *timestampFormat == "" {
				*timestampFormat = "20060102 150405"
			}
			*simpleOutput = true
		default:
			return fmt.Errorf("unknown preset %q (supported: mt4, mt5, backtrader, ninjatrader)", *preset)
		}
	}

	var loc *time.Location
	if strings.ToLower(strings.TrimSpace(*timezone)) == "eet" {
		var err error
		loc, err = time.LoadLocation("Europe/Athens")
		if err != nil {
			loc = time.FixedZone("EET", 2*60*60)
		}
	} else if strings.ToLower(strings.TrimSpace(*timezone)) == "est" {
		var err error
		loc, err = time.LoadLocation("America/New_York")
		if err != nil {
			loc = time.FixedZone("EST", -5*60*60)
		}
	} else if strings.TrimSpace(*timezone) != "" && strings.ToUpper(strings.TrimSpace(*timezone)) != "UTC" {
		var err error
		loc, err = time.LoadLocation(strings.TrimSpace(*timezone))
		if err != nil {
			return fmt.Errorf("invalid timezone %q: %w", *timezone, err)
		}
	} else {
		loc = time.UTC
	}

	csvout.OutputLocation = loc

	if *timestampFormat != "" {
		csvout.OutputTimestampFormat = *timestampFormat
	} else {
		csvout.OutputTimestampFormat = time.RFC3339Nano
	}

	if *csvDelimiter != "" {
		runes := []rune(*csvDelimiter)
		if len(runes) != 1 {
			return fmt.Errorf("--csv-delimiter must be a single character, got %q", *csvDelimiter)
		}
		csvout.CSVDelimiter = runes[0]
	} else {
		csvout.CSVDelimiter = ','
	}

	csvout.HideCSVHeader = *noHeader
	csvout.FillGaps = strings.ToLower(strings.TrimSpace(*fillGaps))

	if strings.TrimSpace(*symbol) == "" {
		return errors.New("--symbol is required")
	}
	if strings.TrimSpace(*fromValue) == "" && strings.TrimSpace(*lastValue) == "" {
		return errors.New("either --from or --last is required")
	}
	if !*live && strings.TrimSpace(*toValue) == "" && strings.TrimSpace(*lastValue) == "" {
		return errors.New("--to is required when --last is not provided")
	}
	if strings.TrimSpace(*outputPath) == "" {
		return errors.New("--output is required")
	}
	if *live && strings.TrimSpace(*toValue) != "" {
		return errors.New("--to cannot be combined with --live")
	}

	var from, to time.Time
	var err error
	if strings.TrimSpace(*lastValue) != "" {
		to = time.Now().UTC()
		if !*live && strings.TrimSpace(*toValue) != "" {
			to, err = parseFlexibleTime(*toValue)
			if err != nil {
				return fmt.Errorf("--to %w", err)
			}
		}
		from, err = parseLookback(*lastValue, to)
		if err != nil {
			return fmt.Errorf("--last %w", err)
		}
	} else {
		from, err = parseFlexibleTime(*fromValue)
		if err != nil {
			return fmt.Errorf("--from %w", err)
		}
		to = from
		if !*live {
			to, err = parseFlexibleTime(*toValue)
			if err != nil {
				return fmt.Errorf("--to %w", err)
			}
		}
	}

	// Clamp future dates to the current system time to avoid redundant HTTP requests
	now := time.Now().UTC()
	if to.After(now) {
		to = now
	}
	if from.After(now) {
		from = now
	}

	if !*live && to.Before(from) {
		return errors.New("--to must be the same as or later than --from")
	}
	if *pollInterval <= 0 {
		return errors.New("--poll-interval must be greater than 0")
	}
	if *retries < 0 {
		return errors.New("--retries must be 0 or greater")
	}
	if *retryBackoff <= 0 {
		return errors.New("--retry-backoff must be greater than 0")
	}
	if *rateLimit < 0 {
		return errors.New("--rate-limit must be 0 or greater")
	}
	if *parallelism <= 0 {
		return errors.New("--parallelism must be greater than 0")
	}
	if *simpleOutput && *fullOutput {
		return errors.New("--simple and --full cannot be used together")
	}
	if *fusedOutput && (*simpleOutput || *fullOutput) {
		return errors.New("only one of --simple, --full, or --fused can be used")
	}
	if strings.TrimSpace(*customColumns) != "" && (*simpleOutput || *fullOutput) {
		return errors.New("--custom-columns cannot be combined with --simple or --full")
	}
	if strings.TrimSpace(*customColumns) != "" && *fusedOutput {
		return errors.New("--custom-columns cannot be combined with --fused")
	}

	timeframeValue := strings.TrimSpace(*timeframe)
	if strings.TrimSpace(*granularity) != "" {
		timeframeValue = strings.TrimSpace(*granularity)
	}

	normalizedTimeframe := dukascopy.NormalizeGranularity(dukascopy.Granularity(timeframeValue))
	profile := csvout.ProfileSimple
	if *fullOutput {
		profile = csvout.ProfileFull
	} else if *fusedOutput {
		profile = csvout.ProfileFused
	}

	barColumns := csvout.BarColumnsForProfile(profile)
	tickColumns := csvout.TickColumnsForProfile(profile)
	if !*simpleOutput && !*fullOutput && !*fusedOutput && strings.TrimSpace(*customColumns) == "" {
		tickColumns = csvout.TickColumnsForProfile(csvout.ProfileFull)
	}
	if strings.TrimSpace(*customColumns) != "" {
		if normalizedTimeframe == dukascopy.GranularityTick {
			tickColumns, err = csvout.ParseTickColumns(*customColumns)
			if err != nil {
				return err
			}
		} else {
			barColumns, err = csvout.ParseBarColumns(*customColumns)
			if err != nil {
				return err
			}
		}
	}

	request := dukascopy.DownloadRequest{
		Symbol:      *symbol,
		Granularity: normalizedTimeframe,
		Side:        dukascopy.PriceSide(*side),
		From:        from.UTC(),
		To:          inclusiveDownloadEnd(to.UTC()),
	}

	resultKind := dukascopy.ResultKindBar
	if normalizedTimeframe == dukascopy.GranularityTick {
		resultKind = dukascopy.ResultKindTick
	}

	partitionValue := strings.TrimSpace(*partition)
	if partitionValue == "" && strings.TrimSpace(*checkpointManifest) != "" {
		partitionValue = partitionAuto
	}
	if partitionValue == partitionNone && strings.TrimSpace(*checkpointManifest) != "" {
		partitionValue = partitionAuto
	}
	normalizedPartition, err := normalizePartition(partitionValue, normalizedTimeframe)
	if err != nil {
		return err
	}
	if *live && strings.HasSuffix(strings.ToLower(strings.TrimSpace(*outputPath)), ".parquet") && normalizedPartition == partitionNone {
		normalizedPartition, err = normalizePartition(partitionAuto, normalizedTimeframe)
		if err != nil {
			return err
		}
	}
	outputToStdout := strings.TrimSpace(*outputPath) == "-"
	if outputToStdout {
		if *resume {
			return errors.New("--resume cannot be combined with --output -")
		}
		if normalizedPartition != partitionNone && !*live {
			return errors.New("--partition cannot be combined with --output -")
		}
	}
	if *parallelism > 1 && normalizedPartition == partitionNone {
		return errors.New("--parallelism greater than 1 requires --partition")
	}
	if err := validateLiveOptions(*live, *outputPath, normalizedPartition, *checkpointManifest, barColumns, tickColumns, resultKind); err != nil {
		return err
	}
	if csvout.ColumnsContainTimestamp(barColumns) || csvout.ColumnsContainTimestamp(tickColumns) {
		if strings.HasSuffix(strings.ToLower(strings.TrimSpace(*outputPath)), ".parquet") && *resume {
			return errors.New("--resume is not supported for parquet output; use --partition for durable long-range parquet downloads")
		}
	}

	progressConfigured := flagWasSet(fs, "progress")
	if !progressConfigured && activeConfig != nil && activeConfig.Download.Progress != nil {
		progressConfigured = true
	}
	progressEnabled := *progress
	if *noProgress {
		progressEnabled = false
	} else if !progressConfigured && !outputToStdout && isInteractiveTerminal(stderr) {
		progressEnabled = true
	}

	client := dukascopy.NewClient(*baseURL, defaultHTTPTimeout).
		WithEngine(engineVal).
		WithRetries(*retries).
		WithBackoff(*retryBackoff).
		WithRateLimit(*rateLimit)

	if *proxyFile != "" {
		if err := client.LoadProxies(*proxyFile); err != nil {
			return fmt.Errorf("failed to load proxy file %s: %w", *proxyFile, err)
		}
	}

	progressWriter := stderr
	if progressEnabled {
		printer := newProgressPrinter(stderr, *tuiTheme)
		printer.SetDownloadMeta(*symbol, string(normalizedTimeframe), string(request.Side), *outputPath, normalizedPartition, *parallelism, request.From, request.To)
		progressWriter = printer
		client = client.WithProgress(printer.Print)
		defer printer.Finish()
	}
	ctx, cancel := newDownloadContext()
	defer cancel()

	if *live {
		manifestPath := strings.TrimSpace(*checkpointManifest)
		if normalizedPartition != partitionNone && manifestPath == "" {
			if outputToStdout {
				manifestPath = defaultLiveStdoutManifestPath(*symbol, normalizedTimeframe)
			} else {
				manifestPath = checkpoint.DefaultManifestPath(*outputPath)
			}
		}
		storageOutputPath := *outputPath
		if outputToStdout && normalizedPartition != partitionNone {
			storageOutputPath = defaultLiveStdoutCachePath(manifestPath)
		}
		return runLiveDownload(ctx, client, stdout, progressWriter, *outputPath, storageOutputPath, manifestPath, request, resultKind, barColumns, tickColumns, normalizedPartition, *parallelism, *pollInterval)
	}

	manifestPath := strings.TrimSpace(*checkpointManifest)
	if manifestPath == "" {
		manifestPath = checkpoint.DefaultManifestPath(*outputPath)
	}

	keepCacheVal := *keepCache
	if !flagWasSet(fs, "keep-cache") && normalizedPartition != partitionNone {
		keepCacheVal = true
	}

	resumeState, dedupeRecord, err := prepareResume(*resume, *outputPath, resultKind, barColumns, tickColumns, &request)
	if err != nil {
		return err
	}
	if !request.From.Before(request.To) {
		fmt.Fprintf(stdout, "%sresume%s no new rows needed for %s\n", colorize(colorCyan), colorize(colorReset), *outputPath)
		return nil
	}

	appended, err := runChunkedDownload(
		ctx,
		client,
		stdout,
		progressWriter,
		*outputPath,
		manifestPath,
		request,
		resultKind,
		barColumns,
		tickColumns,
		normalizedPartition,
		*parallelism,
		*cacheDir,
		keepCacheVal,
		resumeState,
		dedupeRecord,
	)
	if err != nil {
		return err
	}

	if !outputToStdout {
		label := "bars"
		if resultKind == dukascopy.ResultKindTick {
			label = "ticks"
		}
		fmt.Fprintf(stdout, "%swrote%s %d %s to %s\n", colorize(colorGreen), colorize(colorReset), appended, label, *outputPath)
	}
	return nil
}

func loadBidAskBars(ctx context.Context, client *dukascopy.Client, request dukascopy.DownloadRequest) (dukascopy.Instrument, []dukascopy.Bar, []dukascopy.Bar, error) {
	instrument, bidBars, bidErr := client.DownloadBarsForSide(ctx, request, dukascopy.PriceSideBid)
	if bidErr == nil {
		_, askBars, askErr := client.DownloadBarsForSide(ctx, request, dukascopy.PriceSideAsk)
		if askErr == nil {
			return instrument, bidBars, askBars, nil
		}
	}

	tickRequest := request
	tickRequest.Granularity = dukascopy.GranularityTick
	tickResult, err := client.Download(ctx, tickRequest)
	if err != nil {
		return dukascopy.Instrument{}, nil, nil, err
	}

	bidBars, err = dukascopy.AggregateTicksToBars(tickResult.Ticks, request.Granularity, dukascopy.PriceSideBid, request.From, request.To)
	if err != nil {
		return dukascopy.Instrument{}, nil, nil, err
	}
	askBars, err := dukascopy.AggregateTicksToBars(tickResult.Ticks, request.Granularity, dukascopy.PriceSideAsk, request.From, request.To)
	if err != nil {
		return dukascopy.Instrument{}, nil, nil, err
	}
	return tickResult.Instrument, bidBars, askBars, nil
}

func runSingleDownload(
	ctx context.Context,
	client *dukascopy.Client,
	stdout io.Writer,
	outputPath string,
	outputToStdout bool,
	resumeState *csvout.ResumeState,
	dedupeRecord []string,
	request dukascopy.DownloadRequest,
	resultKind dukascopy.ResultKind,
	barColumns []string,
	tickColumns []string,
) (int, error) {
	result, err := client.Download(ctx, request)
	if err != nil {
		return 0, err
	}

	if result.Kind == dukascopy.ResultKindTick {
		if outputToStdout {
			return len(result.Ticks), csvout.WriteTicksToWriter(stdout, result.Instrument, tickColumns, result.Ticks)
		}
		return writeTickOutput(outputPath, resumeState, dedupeRecord, result.Instrument, tickColumns, result.Ticks)
	}

	if csvout.BarColumnsNeedBidAsk(barColumns) {
		instrument, bidBars, askBars, err := loadBidAskBars(ctx, client, request)
		if err != nil {
			return 0, err
		}
		if outputToStdout {
			return len(bidBars), csvout.WriteBarsToWriter(stdout, instrument, barColumns, nil, bidBars, askBars)
		}
		return writeBarOutput(outputPath, resumeState, dedupeRecord, instrument, barColumns, nil, bidBars, askBars)
	}

	if outputToStdout {
		return len(result.Bars), csvout.WriteBarsToWriter(stdout, result.Instrument, barColumns, result.Bars, nil, nil)
	}
	return writeBarOutput(outputPath, resumeState, dedupeRecord, result.Instrument, barColumns, result.Bars, nil, nil)
}

func prepareResume(enabled bool, outputPath string, resultKind dukascopy.ResultKind, barColumns []string, tickColumns []string, request *dukascopy.DownloadRequest) (*csvout.ResumeState, []string, error) {
	if !enabled {
		return nil, nil, nil
	}

	expectedColumns := barColumns
	if resultKind == dukascopy.ResultKindTick {
		expectedColumns = tickColumns
	}
	if !csvout.ColumnsContainTimestamp(expectedColumns) {
		return nil, nil, errors.New("--resume requires the selected columns to include timestamp")
	}

	state, err := csvout.InspectExistingCSV(outputPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, nil
		}
		return nil, nil, err
	}

	if len(state.Columns) > 0 && !csvout.HeadersMatch(expectedColumns, state.Columns) {
		return nil, nil, fmt.Errorf("existing CSV header does not match the selected columns for %s", outputPath)
	}

	var dedupeRecord []string
	if state.HasRows && (request.From.Before(state.LastTime) || request.From.Equal(state.LastTime)) {
		inclusiveTo := request.To.Add(-time.Nanosecond)
		if !state.LastTime.Before(inclusiveTo) {
			request.From = request.To
			return &state, nil, nil
		}
		request.From = state.LastTime
		dedupeRecord = state.LastRecord
	}

	return &state, dedupeRecord, nil
}

func writeTickOutput(outputPath string, resumeState *csvout.ResumeState, dedupeRecord []string, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) (int, error) {
	if resumeState == nil || !resumeState.Exists || len(resumeState.Columns) == 0 {
		if err := csvout.WriteTicks(outputPath, instrument, columns, ticks); err != nil {
			return 0, err
		}
		return len(ticks), nil
	}

	tempPath, err := createResumeTempPath(outputPath)
	if err != nil {
		return 0, err
	}
	defer os.Remove(tempPath)

	if err := csvout.WriteTicks(tempPath, instrument, columns, ticks); err != nil {
		return 0, err
	}

	return csvout.MergeResumeCSV(outputPath, tempPath, dedupeRecord)
}

func writeBarOutput(outputPath string, resumeState *csvout.ResumeState, dedupeRecord []string, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) (int, error) {
	if resumeState == nil || !resumeState.Exists || len(resumeState.Columns) == 0 {
		if err := csvout.WriteBars(outputPath, instrument, columns, primaryBars, bidBars, askBars); err != nil {
			return 0, err
		}
		if csvout.BarColumnsNeedBidAsk(columns) {
			return len(bidBars), nil
		}
		return len(primaryBars), nil
	}

	tempPath, err := createResumeTempPath(outputPath)
	if err != nil {
		return 0, err
	}
	defer os.Remove(tempPath)

	if err := csvout.WriteBars(tempPath, instrument, columns, primaryBars, bidBars, askBars); err != nil {
		return 0, err
	}

	return csvout.MergeResumeCSV(outputPath, tempPath, dedupeRecord)
}
