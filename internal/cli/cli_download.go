package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/checkpoint"
	"github.com/Nosvemos/dukascopy-go/pkg/cloud"
	"github.com/Nosvemos/dukascopy-go/pkg/csvout"
	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
	"github.com/Nosvemos/dukascopy-go/pkg/features"
)

func runDownload(args []string, stdout io.Writer, stderr io.Writer) error {
	fs, f := newDownloadFlagSet(stdout)
	symbol := f.symbol
	timeframe := f.timeframe
	granularity := f.granularity
	side := f.side
	simpleOutput := f.simpleOutput
	fullOutput := f.fullOutput
	fusedOutput := f.fusedOutput
	customColumns := f.customColumns
	lastValue := f.lastValue
	fromValue := f.fromValue
	toValue := f.toValue
	outputPath := f.outputPath
	live := f.live
	pollInterval := f.pollInterval
	retries := f.retries
	retryBackoff := f.retryBackoff
	rateLimit := f.rateLimit
	progress := f.progress
	noProgress := f.noProgress
	tuiTheme := f.tuiTheme
	resume := f.resume
	partition := f.partition
	parallelism := f.parallelism
	checkpointManifest := f.checkpointManifest
	baseURL := f.baseURL
	timezone := f.timezone
	timestampFormat := f.timestampFormat
	csvDelimiter := f.csvDelimiter
	noHeader := f.noHeader
	preset := f.preset
	proxyFile := f.proxyFile
	engine := f.engine
	fillGaps := f.fillGaps
	cacheDir := f.cacheDir
	keepCache := f.keepCache
	hive := f.hive
	featuresFlag := f.featuresFlag
	indicatorsFlag := f.indicatorsFlag
	barType := f.barType
	barSize := f.barSize
	tickCount := f.tickCount
	cleanOutliers := f.cleanOutliers

	if err := fs.Parse(args); err != nil {
		return err
	}

	if strings.Contains(*symbol, ",") {
		symbols := splitSymbols(*symbol)
		if err := validateMultiSymbolOutput(*outputPath, symbols); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "%sbatch%s downloading %d symbols...\n", colorize(colorCyan), colorize(colorReset), len(symbols))
		for _, sym := range symbols {

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
		hive,
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
		case "backtrader", "vectorbt":
			if *timestampFormat == "" {
				*timestampFormat = "2006-01-02 15:04:05"
			}
			*simpleOutput = true
		case "nautilus":
			if *timestampFormat == "" {
				*timestampFormat = time.RFC3339Nano
			}
			*simpleOutput = true
		case "freqtrade":
			if *timestampFormat == "" {
				*timestampFormat = "2006-01-02 15:04:05"
			}
			*simpleOutput = true
		case "lean":
			*noHeader = true
			if *timestampFormat == "" {
				*timestampFormat = "20060102 15:04"
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
			return fmt.Errorf("unknown preset %q (supported: mt4, mt5, backtrader, vectorbt, nautilus, freqtrade, lean, ninjatrader)", *preset)
		}
	}

	loc, err := loadOutputLocation(*timezone)
	if err != nil {
		return err
	}

	csvout.ConfigMutex.Lock()
	csvout.OutputLocation = loc

	if *timestampFormat != "" {
		csvout.OutputTimestampFormat = *timestampFormat
	} else {
		csvout.OutputTimestampFormat = time.RFC3339Nano
	}

	if *csvDelimiter != "" {
		runes := []rune(*csvDelimiter)
		if len(runes) != 1 {
			csvout.ConfigMutex.Unlock()
			return fmt.Errorf("--csv-delimiter must be a single character, got %q", *csvDelimiter)
		}
		csvout.CSVDelimiter = runes[0]
	} else {
		csvout.CSVDelimiter = ','
	}

	fillGapsValue, err := validateFillGaps(*fillGaps)
	if err != nil {
		csvout.ConfigMutex.Unlock()
		return err
	}
	csvout.HideCSVHeader = *noHeader
	csvout.FillGaps = fillGapsValue
	csvout.ConfigMutex.Unlock()
	// Snapshot of the user's output format, applied when the final output is
	// written (cached chunks always use the internal format).
	outputConfig := csvout.DefaultConfig()

	if strings.TrimSpace(*symbol) == "" {
		return errors.New("--symbol is required")
	}
	if strings.TrimSpace(*fromValue) == "" && strings.TrimSpace(*lastValue) == "" {
		return errors.New("either --from or --last is required")
	}
	if strings.TrimSpace(*fromValue) != "" && strings.TrimSpace(*lastValue) != "" {
		return errors.New("--from and --last cannot be used together")
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

	now := time.Now().UTC()
	var from, to time.Time
	if strings.TrimSpace(*lastValue) != "" {
		to = now
		if !*live && strings.TrimSpace(*toValue) != "" {
			to, err = ParseFlexibleTime(*toValue)
			if err != nil {
				return fmt.Errorf("--to %w", err)
			}
		}
		from, err = parseLookback(*lastValue, to)
		if err != nil {
			return fmt.Errorf("--last %w", err)
		}
	} else {
		from, err = ParseFlexibleTime(*fromValue)
		if err != nil {
			return fmt.Errorf("--from %w", err)
		}
		if from.After(now) {
			return fmt.Errorf("--from %s is in the future", from.UTC().Format(time.RFC3339))
		}
		to = from
		if !*live {
			to, err = ParseFlexibleTime(*toValue)
			if err != nil {
				return fmt.Errorf("--to %w", err)
			}
		}
	}

	// Clamp future dates to the current system time to avoid redundant HTTP requests
	to = clampDownloadEnd(to, now)
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
	if err := validateParallelism(*parallelism); err != nil {
		return err
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
	parsedBarType, err := dukascopy.ParseBarType(*barType)
	if err != nil {
		return err
	}

	// The kind of rows written is fixed here, before any step that depends on
	// it: custom bar types (tick/volume/dollar) sample ticks into bars.
	resultKind := dukascopy.ResultKindBar
	if normalizedTimeframe == dukascopy.GranularityTick && parsedBarType == dukascopy.BarTypeTime {
		resultKind = dukascopy.ResultKindTick
	}
	if parsedBarType != dukascopy.BarTypeTime {
		if *barSize <= 0 {
			return fmt.Errorf("--bar-size must be > 0 when --bar-type is %q", *barType)
		}
		if *live {
			return errors.New("--bar-type other than time is only supported for historical downloads")
		}
	}

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
		if resultKind == dukascopy.ResultKindTick {
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
	if parsedBarType != dukascopy.BarTypeTime && csvout.BarColumnsNeedBidAsk(barColumns) {
		return fmt.Errorf("--bar-type %s bars are sampled from one price side; use columns without bid/ask/spread/mid values (e.g. --simple)", parsedBarType)
	}

	tickCountEnabled := *tickCount
	for _, column := range barColumns {
		if column == "tick_count" {
			tickCountEnabled = true
			break
		}
	}
	if tickCountEnabled {
		if normalizedTimeframe == dukascopy.GranularityTick {
			return errors.New("tick_count can only be added to time-based bars, not --timeframe tick")
		}
		if *live {
			return errors.New("tick_count is only supported for historical downloads")
		}
		if parsedBarType != dukascopy.BarTypeTime {
			return errors.New("tick_count is only supported with --bar-type time")
		}
		if !containsString(barColumns, "tick_count") {
			barColumns = append(barColumns, "tick_count")
		}
	}

	request := dukascopy.DownloadRequest{
		Symbol:      *symbol,
		Granularity: normalizedTimeframe,
		Side:        dukascopy.PriceSide(*side),
		From:        from.UTC(),
		To:          inclusiveDownloadEnd(to.UTC()),
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
	if *hive && normalizedPartition == partitionNone {
		return errors.New("--hive requires --partition")
	}
	if *resume && normalizedPartition != partitionNone && !*live {
		return errors.New("--resume cannot be combined with --partition (or --checkpoint-manifest); partitioned downloads resume from their checkpoint manifest automatically")
	}
	isCloud := cloud.IsCloudPath(*outputPath)
	if isCloud && normalizedPartition != partitionNone {
		return errors.New("--partition (and --checkpoint-manifest) cannot be combined with a cloud (s3://) output")
	}
	if isCloud && *live {
		return errors.New("--live cannot write to a cloud (s3://) output")
	}
	if err := validateLiveOptions(*live, *outputPath, normalizedPartition, *checkpointManifest, barColumns, tickColumns, resultKind); err != nil {
		return err
	}
	if csvout.ColumnsContainTimestamp(barColumns) || csvout.ColumnsContainTimestamp(tickColumns) {
		outLower := strings.ToLower(strings.TrimSpace(*outputPath))
		if (strings.HasSuffix(outLower, ".parquet") || strings.HasSuffix(outLower, ".arrow") || strings.HasSuffix(outLower, ".ipc") || strings.HasSuffix(outLower, ".feather")) && *resume {
			return errors.New("--resume is not supported for parquet/arrow output; use --partition for durable long-range downloads")
		}
	}

	var featureSpecs []features.FeatureSpec
	activeFeatures := strings.TrimSpace(*featuresFlag)
	if activeFeatures == "" && strings.TrimSpace(*indicatorsFlag) != "" {
		activeFeatures = strings.TrimSpace(*indicatorsFlag)
	}
	if activeFeatures != "" {
		specs, err := features.ParseFeatureSpecs(activeFeatures)
		if err != nil {
			return fmt.Errorf("invalid --features: %w", err)
		}
		featureSpecs = specs
	}
	if len(featureSpecs) > 0 {
		if err := validateFeatureOptions(*live, resultKind, normalizedPartition, *resume, outputToStdout, barColumns); err != nil {
			return err
		}
	}

	outlierCfg, err := dukascopy.ParseOutlierConfig(*cleanOutliers)
	if err != nil {
		return fmt.Errorf("invalid --clean-outliers: %w", err)
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

	client, err := dukascopy.NewClient(*baseURL, defaultHTTPTimeout)
	if err != nil {
		return err
	}
	client = client.
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

	targetLocalPath := *outputPath
	var cloudCfg cloud.S3Config
	if isCloud {
		cfg, err := cloud.ParseCloudURI(*outputPath)
		if err != nil {
			return err
		}
		cloudCfg = cfg
		targetLocalPath = filepath.Join(*cacheDir, "cloud_upload_tmp_"+filepath.Base(cfg.Key))
	}

	// The default manifest lives next to the local file actually written (for
	// cloud outputs that is the local upload temp, never a path built from the
	// s3:// URI).
	manifestPath := strings.TrimSpace(*checkpointManifest)
	if manifestPath == "" {
		manifestPath = checkpoint.DefaultManifestPath(targetLocalPath)
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

	appended, err := runChunkedDownload(ctx, client, stdout, progressWriter, chunkedDownloadOptions{
		OutputPath:         targetLocalPath,
		CheckpointManifest: manifestPath,
		Request:            request,
		ResultKind:         resultKind,
		BarColumns:         barColumns,
		TickColumns:        tickColumns,
		PartitionMode:      normalizedPartition,
		Parallelism:        *parallelism,
		CacheDir:           *cacheDir,
		KeepCache:          keepCacheVal,
		ResumeState:        resumeState,
		DedupeRecord:       dedupeRecord,
		Hive:               *hive,
		BarType:            parsedBarType,
		BarSize:            *barSize,
		OutlierCfg:         outlierCfg,
		OutlierSpec:        strings.ToLower(strings.TrimSpace(*cleanOutliers)),
		Engine:             string(engineVal),
		FeatureSpecs:       featureSpecs,
		OutputConfig:       outputConfig,
	})
	if err != nil {
		if isCloud {
			_ = os.Remove(targetLocalPath)
		}
		return err
	}

	label := "bars"
	if resultKind == dukascopy.ResultKindTick {
		label = "ticks"
	}

	if isCloud {
		fmt.Fprintf(stdout, "%suploading%s %d %s to %s...\n", colorize(colorCyan), colorize(colorReset), appended, label, *outputPath)
		if err := cloud.UploadFile(ctx, cloudCfg, targetLocalPath); err != nil {
			_ = os.Remove(targetLocalPath)
			return fmt.Errorf("cloud upload failed: %w", err)
		}
		_ = os.Remove(targetLocalPath)
		fmt.Fprintf(stdout, "%suploaded%s %d %s successfully to %s\n", colorize(colorGreen), colorize(colorReset), appended, label, *outputPath)
		return nil
	}

	if !outputToStdout {
		fmt.Fprintf(stdout, "%swrote%s %d %s to %s\n", colorize(colorGreen), colorize(colorReset), appended, label, *outputPath)
	}
	return nil
}

// validateFeatureOptions rejects --features for outputs and modes the
// post-merge feature step cannot produce.
func validateFeatureOptions(live bool, resultKind dukascopy.ResultKind, partitionMode string, resume bool, outputToStdout bool, barColumns []string) error {
	switch {
	case live:
		return errors.New("--features is only supported for historical downloads, not --live")
	case resultKind != dukascopy.ResultKindBar:
		return errors.New("--features requires bar output; use a bar --timeframe (m1..mn1) or a --bar-type other than time")
	case partitionMode != partitionNone:
		return errors.New("--features cannot be combined with --partition, --hive or --checkpoint-manifest")
	case resume:
		return errors.New("--features cannot be combined with --resume (feature columns are recomputed over the whole series)")
	case outputToStdout:
		return errors.New("--features cannot be combined with --output -")
	case csvout.BarColumnsNeedBidAsk(barColumns):
		return errors.New("--features supports only single-side bar columns (timestamp, open, high, low, close, volume, tick_count); drop --full/--fused or bid/ask/spread/mid columns")
	}
	return nil
}

// downloadFlags holds the parsed values of the download command flags.
type downloadFlags struct {
	symbol             *string
	timeframe          *string
	granularity        *string
	side               *string
	simpleOutput       *bool
	fullOutput         *bool
	fusedOutput        *bool
	customColumns      *string
	lastValue          *string
	fromValue          *string
	toValue            *string
	outputPath         *string
	live               *bool
	pollInterval       *time.Duration
	retries            *int
	retryBackoff       *time.Duration
	rateLimit          *time.Duration
	progress           *bool
	noProgress         *bool
	tuiTheme           *string
	resume             *bool
	partition          *string
	parallelism        *int
	checkpointManifest *string
	baseURL            *string
	timezone           *string
	timestampFormat    *string
	csvDelimiter       *string
	noHeader           *bool
	preset             *string
	proxyFile          *string
	engine             *string
	fillGaps           *string
	cacheDir           *string
	keepCache          *bool
	hive               *bool
	featuresFlag       *string
	indicatorsFlag     *string
	barType            *string
	barSize            *float64
	tickCount          *bool
	cleanOutliers      *string
}

// newDownloadFlagSet defines the download command flags. sync uses the same
// flag set so it accepts and forwards every download flag.
func newDownloadFlagSet(stdout io.Writer) (*flag.FlagSet, *downloadFlags) {
	fs := flag.NewFlagSet("download", flag.ContinueOnError)
	fs.SetOutput(stdout)
	fs.Usage = func() {
		fmt.Fprintf(stdout, "%sdownload:%s Download historical Dukascopy tick and bar data to CSV/Parquet\n\n", colorize(colorCyan), colorize(colorReset))
		fmt.Fprint(stdout, "Usage:\n  dukascopy-go download [options]\n\nOptions:\n")
		fs.PrintDefaults()
		fmt.Fprint(stdout, "\nExamples:\n  dukascopy-go download --symbol eurusd --timeframe m1 --last 30d --output ./eurusd_m1.csv\n  dukascopy-go download --symbol xauusd,gbpusd --timeframe d1 --from 2024-01-01 --to 2024-02-01 --output ./data/\n  dukascopy-go download --symbol eurusd --timeframe tick --last 1d --output ./eurusd_tick.parquet --parallelism 4 --partition day\n")
	}
	f := &downloadFlags{}
	f.symbol = fs.String("symbol", "", "instrument symbol such as xauusd or eur/usd")
	f.timeframe = fs.String("timeframe", "m1", "tick, m1, m3, m5, m15, m30, h1, h4, d1, w1, mn1")
	f.granularity = fs.String("granularity", "", "deprecated alias for --timeframe")
	f.side = fs.String("side", "bid", "bid or ask")
	f.simpleOutput = fs.Bool("simple", false, "write the reduced CSV column set")
	f.fullOutput = fs.Bool("full", false, "write the full CSV column set with bid/ask columns")
	f.fusedOutput = fs.Bool("fused", false, "write the fused CSV column set with bid/ask and spread but no mid columns")
	f.customColumns = fs.String("custom-columns", "", "comma-separated CSV column list")
	f.lastValue = fs.String("last", "", "download the last N duration (e.g. 30d, 1y, 6mo) instead of --from/--to")
	f.fromValue = fs.String("from", "", "inclusive RFC3339 start timestamp")
	f.toValue = fs.String("to", "", "inclusive RFC3339 end timestamp")
	f.outputPath = fs.String("output", "", "target CSV path")
	f.live = fs.Bool("live", false, "keep polling and append newly completed rows until interrupted")
	f.pollInterval = fs.Duration("poll-interval", 5*time.Second, "delay between live polling cycles such as 5s or 1m")
	f.retries = fs.Int("retries", 3, "retry count for transient HTTP errors")
	f.retryBackoff = fs.Duration("retry-backoff", 500*time.Millisecond, "base retry backoff duration such as 500ms or 2s")
	f.rateLimit = fs.Duration("rate-limit", 0, "minimum delay between HTTP requests such as 100ms or 1s")
	f.progress = fs.Bool("progress", false, "force-enable the interactive progress dashboard")
	f.noProgress = fs.Bool("no-progress", false, "disable the interactive progress dashboard")
	f.tuiTheme = fs.String("tui-theme", "default", "interactive dashboard color theme: default, catppuccin, nord, gruvbox, dracula")
	f.resume = fs.Bool("resume", false, "append missing rows to an existing CSV when possible")
	f.partition = fs.String("partition", "none", "partition mode: none, auto, hour, day, week, month, year")
	f.parallelism = fs.Int("parallelism", 1, "parallel chunk download workers (1-32)")
	f.checkpointManifest = fs.String("checkpoint-manifest", "", "optional checkpoint manifest path for partitioned downloads")
	f.baseURL = fs.String("base-url", readBaseURL(), "Dukascopy API base URL")
	f.timezone = fs.String("timezone", "UTC", "target output timezone location (e.g. Europe/London, EET, EST)")
	f.timestampFormat = fs.String("timestamp-format", "", "custom timestamp layout format")
	f.csvDelimiter = fs.String("csv-delimiter", ",", "custom CSV separator character")
	f.noHeader = fs.Bool("no-header", false, "suppress header row in output CSV files")
	f.preset = fs.String("preset", "", "backtest output preset profile (mt4, mt5, backtrader, vectorbt, nautilus, freqtrade, lean, ninjatrader); counts as a column-profile flag, so the config file's simple/full/custom_columns keys are ignored")
	f.proxyFile = fs.String("proxy-file", "", "optional proxy list file path (HTTP/SOCKS5)")
	f.engine = fs.String("engine", "jetta", "downloader engine: jetta or datafeed")
	f.fillGaps = fs.String("fill-gaps", "none", "gap filling mode: none, forward")
	f.cacheDir = fs.String("cache-dir", "./.dukascopy_cache", "temporary cache directory path")
	f.keepCache = fs.Bool("keep-cache", false, "keep temporary cache files after successful download")
	f.hive = fs.Bool("hive", false, "enable Hive-style partitioning directory output (e.g. year=YYYY/month=MM/...)")
	f.featuresFlag = fs.String("features", "", "comma-separated ML features / indicators (e.g. returns:log,rsi:14,ema:20,ema:50,atr:14,vwap,volatility:garman-klass)")
	f.indicatorsFlag = fs.String("indicators", "", "alias for --features")
	f.barType = fs.String("bar-type", "time", "sampling bar type: time, tick, volume, dollar")
	f.barSize = fs.Float64("bar-size", 0, "size for alternative bar sampling (e.g. 500 for tick bars, 1000 for volume bars)")
	f.tickCount = fs.Bool("tick-count", false, "append source tick record counts per UTC time bar (additional tick download; not transaction volume)")
	f.cleanOutliers = fs.String("clean-outliers", "none", "outlier scrubbing mode (none, auto, median:window:threshold)")
	return fs, f
}
