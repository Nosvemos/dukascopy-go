package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/buildinfo"
	"github.com/Nosvemos/dukascopy-go/internal/checkpoint"
	"github.com/Nosvemos/dukascopy-go/internal/csvout"
	"github.com/Nosvemos/dukascopy-go/internal/dukascopy"

	_ "time/tzdata"
)

const (
	defaultBaseURL     = "https://jetta.dukascopy.com"
	defaultHTTPTimeout = 30 * time.Second
	colorReset         = "\033[0m"
	colorBold          = "\033[1m"
	colorRed           = "\033[31m"
	colorGreen         = "\033[32m"
	colorCyan          = "\033[36m"
	colorYellow        = "\033[33m"
)

func Run(args []string, stdout io.Writer, stderr io.Writer) int {
	loadedArgs, err := loadActiveConfig(args)
	if err != nil {
		fmt.Fprintf(stderr, "%serror:%s %v\n", colorize(colorRed), colorize(colorReset), err)
		return 1
	}
	args = loadedArgs

	if len(args) == 0 {
		if isInteractiveTerminal(stdout) {
			return runWizard(stdout, stderr)
		}
		printUsage(stderr)
		return 2
	}

	switch args[0] {
	case "version", "--version", "-v":
		printVersion(stdout)
		return 0
	case "list-timeframes", "--list-timeframes":
		printTimeframes(stdout)
		return 0
	case "instruments":
		if err := runInstruments(args[1:], stdout); err != nil {
			fmt.Fprintf(stderr, "%serror:%s %v\n", colorize(colorRed), colorize(colorReset), err)
			return 1
		}
		return 0
	case "stats":
		if err := runStats(args[1:], stdout); err != nil {
			fmt.Fprintf(stderr, "%serror:%s %v\n", colorize(colorRed), colorize(colorReset), err)
			return 1
		}
		return 0
	case "manifest":
		if err := runManifest(args[1:], stdout); err != nil {
			fmt.Fprintf(stderr, "%serror:%s %v\n", colorize(colorRed), colorize(colorReset), err)
			return 1
		}
		return 0
	case "download":
		if err := runDownload(args[1:], stdout, stderr); err != nil {
			fmt.Fprintf(stderr, "%serror:%s %v\n", colorize(colorRed), colorize(colorReset), err)
			return 1
		}
		return 0
	case "live":
		if err := runLiveStream(args[1:], stdout, stderr); err != nil {
			fmt.Fprintf(stderr, "%serror:%s %v\n", colorize(colorRed), colorize(colorReset), err)
			return 1
		}
		return 0
	case "db-load":
		if err := runDBLoad(args[1:], stdout, stderr); err != nil {
			fmt.Fprintf(stderr, "%serror:%s %v\n", colorize(colorRed), colorize(colorReset), err)
			return 1
		}
		return 0
	case "help", "-h", "--help":
		printUsage(stdout)
		return 0
	default:
		fmt.Fprintf(stderr, "%serror:%s unknown command %q\n\n", colorize(colorRed), colorize(colorReset), args[0])
		printUsage(stderr)
		return 2
	}
}

func printUsage(w io.Writer) {
	fmt.Fprintf(w, "%sdukascopy-go%s\n\n", colorize(colorBold), colorize(colorReset))
	fmt.Fprintf(w, "Version: %s\n\n", buildinfo.VersionString())
	fmt.Fprintf(w, "Global flags: --config <path.json>\n\n")
	fmt.Fprintf(w, "%sCommands%s\n", colorize(colorCyan), colorize(colorReset))
	fmt.Fprint(w, `  instruments  Search Dukascopy instruments
  stats        Print dataset statistics
  manifest     Inspect or verify checkpoint manifests
  download     Download historical data and save it as CSV or Parquet
  live         Stream real-time ticks/bars to stdout and optional WebSocket server
  db-load      Ingest a CSV or Parquet file directly into ClickHouse or InfluxDB
  list-timeframes  Print supported timeframe values
  version      Print build version information

examples:
  dukascopy-go instruments
  dukascopy-go instruments --query xauusd
  dukascopy-go download --symbol xauusd --timeframe m1 --from 2024-01-02T00:00:00Z --output ./xauusd.csv
  dukascopy-go download --symbol xauusd --timeframe d1 --from 2024-01-01T00:00:00Z --to 2024-02-01T00:00:00Z --output ./xauusd.parquet
  dukascopy-go live --symbol eurusd --timeframe tick --format jsonl
  dukascopy-go db-load --input ./eurusd_m1.csv --db clickhouse --url http://localhost:8123 --table eurusd_m1
`)
}

func runInstruments(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("instruments", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	query := fs.String("query", "", "instrument search text such as xauusd or eur/usd")
	limit := fs.Int("limit", 20, "maximum number of rows to print")
	jsonOutput := fs.Bool("json", false, "print matching instruments as JSON")
	baseURL := fs.String("base-url", readBaseURL(), "Dukascopy API base URL")
	if err := fs.Parse(args); err != nil {
		return err
	}
	applyInstrumentConfigDefaults(fs, limit, baseURL)
	if *limit <= 0 {
		return errors.New("--limit must be greater than 0")
	}

	client := dukascopy.NewClient(*baseURL, defaultHTTPTimeout)
	ctx, cancel := context.WithTimeout(context.Background(), defaultHTTPTimeout)
	defer cancel()

	instruments, err := client.ListInstruments(ctx)
	if err != nil {
		return err
	}

	matches := dukascopy.FilterInstruments(instruments, *query, *limit)
	if len(matches) == 0 {
		return fmt.Errorf("no instruments found for %q", *query)
	}

	if *jsonOutput {
		data, err := json.MarshalIndent(matches, "", "  ")
		if err != nil {
			return err
		}
		fmt.Fprintln(stdout, string(data))
		return nil
	}

	printInstrumentTable(stdout, matches)
	return nil
}

func runDownload(args []string, stdout io.Writer, stderr io.Writer) error {
	fs := flag.NewFlagSet("download", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	symbol := fs.String("symbol", "", "instrument symbol such as xauusd or eur/usd")
	timeframe := fs.String("timeframe", "m1", "tick, m1, m3, m5, m15, m30, h1, h4, d1, w1, mn1")
	granularity := fs.String("granularity", "", "deprecated alias for --timeframe")
	side := fs.String("side", "bid", "bid or ask")
	simpleOutput := fs.Bool("simple", false, "write the reduced CSV column set")
	fullOutput := fs.Bool("full", false, "write the full CSV column set with bid/ask columns")
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

	if err := fs.Parse(args); err != nil {
		return err
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
	if strings.TrimSpace(*customColumns) != "" && (*simpleOutput || *fullOutput) {
		return errors.New("--custom-columns cannot be combined with --simple or --full")
	}

	timeframeValue := strings.TrimSpace(*timeframe)
	if strings.TrimSpace(*granularity) != "" {
		timeframeValue = strings.TrimSpace(*granularity)
	}

	normalizedTimeframe := dukascopy.NormalizeGranularity(dukascopy.Granularity(timeframeValue))
	profile := csvout.ProfileSimple
	if *fullOutput {
		profile = csvout.ProfileFull
	}

	barColumns := csvout.BarColumnsForProfile(profile)
	tickColumns := csvout.TickColumnsForProfile(profile)
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
		printer := newProgressPrinter(stderr)
		printer.SetDownloadMeta(*symbol, string(normalizedTimeframe), string(request.Side), *outputPath, normalizedPartition, *parallelism)
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

	if normalizedPartition != partitionNone {
		manifestPath := strings.TrimSpace(*checkpointManifest)
		if manifestPath == "" {
			manifestPath = checkpoint.DefaultManifestPath(*outputPath)
		}
		return runPartitionedDownload(ctx, client, stdout, progressWriter, *outputPath, manifestPath, request, resultKind, barColumns, tickColumns, normalizedPartition, *parallelism)
	}

	resumeState, dedupeRecord, err := prepareResume(*resume, *outputPath, resultKind, barColumns, tickColumns, &request)
	if err != nil {
		return err
	}
	if !request.From.Before(request.To) {
		fmt.Fprintf(stdout, "%sresume%s no new rows needed for %s\n", colorize(colorCyan), colorize(colorReset), *outputPath)
		return nil
	}

	appended, err := runSingleDownload(ctx, client, stdout, *outputPath, outputToStdout, resumeState, dedupeRecord, request, resultKind, barColumns, tickColumns)
	if err != nil {
		return err
	}
	if outputToStdout {
		return nil
	}

	label := "bars"
	if resultKind == dukascopy.ResultKindTick {
		label = "ticks"
	}
	fmt.Fprintf(stdout, "%swrote%s %d %s to %s\n", colorize(colorGreen), colorize(colorReset), appended, label, *outputPath)
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

func readBaseURL() string {
	if value := strings.TrimSpace(os.Getenv("DUKASCOPY_API_BASE_URL")); value != "" {
		return value
	}
	return defaultBaseURL
}

func newDownloadContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt)
}

func parseFlexibleTime(value string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, value); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02", value); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02 15:04", value); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02 15:04:05", value); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("must be RFC3339 or YYYY-MM-DD format")
}

func parseLookback(s string, now time.Time) (time.Time, error) {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return time.Time{}, fmt.Errorf("empty duration")
	}

	if d, err := time.ParseDuration(s); err == nil {
		return now.Add(-d), nil
	}

	var val int
	var unit string
	for i, r := range s {
		if r < '0' || r > '9' {
			valStr := s[:i]
			unit = s[i:]
			var err error
			val, err = strconv.Atoi(valStr)
			if err != nil {
				return time.Time{}, fmt.Errorf("invalid lookback number: %s", valStr)
			}
			break
		}
	}
	if unit == "" {
		return time.Time{}, fmt.Errorf("missing unit in lookback (use d, w, mo, y)")
	}

	switch unit {
	case "d", "day", "days":
		return now.AddDate(0, 0, -val), nil
	case "w", "week", "weeks":
		return now.AddDate(0, 0, -val*7), nil
	case "mo", "month", "months":
		return now.AddDate(0, -val, 0), nil
	case "y", "yr", "year", "years":
		return now.AddDate(-val, 0, 0), nil
	default:
		return time.Time{}, fmt.Errorf("unsupported lookback unit: %s", unit)
	}
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

func inclusiveDownloadEnd(value time.Time) time.Time {
	return value.UTC().Add(time.Nanosecond)
}

func colorize(code string) string {
	if strings.TrimSpace(os.Getenv("NO_COLOR")) != "" {
		return ""
	}
	return code
}

func printVersion(w io.Writer) {
	fmt.Fprintf(w, "%sdukascopy-go%s %s\n", colorize(colorBold), colorize(colorReset), buildinfo.VersionString())
	if buildinfo.Commit != "none" {
		fmt.Fprintf(w, "commit: %s\n", buildinfo.Commit)
	}
	if buildinfo.Date != "unknown" {
		fmt.Fprintf(w, "date:   %s\n", buildinfo.Date)
	}
}

func printTimeframes(w io.Writer) {
	fmt.Fprintf(w, "%sSupported timeframes%s\n", colorize(colorCyan), colorize(colorReset))
	fmt.Fprint(w, `  tick  raw tick quotes
  m1    native 1-minute bars
  m3    aggregated from m1
  m5    aggregated from m1
  m15   aggregated from m1
  m30   aggregated from m1
  h1    native 1-hour bars
  h4    aggregated from h1
  d1    native 1-day bars
  w1    aggregated from d1
  mn1   aggregated from d1
`)
}

func printInstrumentTable(w io.Writer, instruments []dukascopy.Instrument) {
	nameWidth := maxStringWidth("NAME", instrumentFieldLengths(instruments, func(instrument dukascopy.Instrument) string {
		return instrument.Name
	}))
	codeWidth := maxStringWidth("CODE", instrumentFieldLengths(instruments, func(instrument dukascopy.Instrument) string {
		return instrument.Code
	}))

	fmt.Fprintf(
		w,
		"%s%-*s  %-*s  %s%s\n",
		colorize(colorCyan),
		nameWidth,
		"NAME",
		codeWidth,
		"CODE",
		"DESCRIPTION",
		colorize(colorReset),
	)

	fmt.Fprintf(
		w,
		"%s%s  %s  %s%s\n",
		colorize(colorYellow),
		strings.Repeat("-", nameWidth),
		strings.Repeat("-", codeWidth),
		strings.Repeat("-", maxInt(11, 24)),
		colorize(colorReset),
	)

	for _, instrument := range instruments {
		fmt.Fprintf(
			w,
			"%-*s  %-*s  %s\n",
			nameWidth,
			instrument.Name,
			codeWidth,
			instrument.Code,
			instrument.Description,
		)
	}
}

func instrumentFieldLengths(instruments []dukascopy.Instrument, selector func(dukascopy.Instrument) string) []int {
	lengths := make([]int, 0, len(instruments))
	for _, instrument := range instruments {
		lengths = append(lengths, len(selector(instrument)))
	}
	return lengths
}

func maxStringWidth(defaultLabel string, lengths []int) int {
	maxWidth := len(defaultLabel)
	for _, length := range lengths {
		if length > maxWidth {
			maxWidth = length
		}
	}
	return maxWidth
}

func maxInt(a int, b int) int {
	if a > b {
		return a
	}
	return b
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

func createResumeTempPath(outputPath string) (string, error) {
	dir := filepath.Dir(outputPath)
	pattern := filepath.Base(outputPath) + ".resume-*.csv"
	if strings.HasSuffix(strings.ToLower(strings.TrimSpace(outputPath)), ".gz") {
		base := filepath.Base(strings.TrimSuffix(outputPath, ".gz"))
		pattern = base + ".resume-*.csv.gz"
	}
	file, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return "", err
	}
	path := file.Name()
	if err := file.Close(); err != nil {
		return "", err
	}
	return path, nil
}
