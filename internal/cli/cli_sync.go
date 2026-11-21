package cli

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/csvout"
	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

// syncOwnFlags are handled by sync itself and never forwarded verbatim.
var syncOwnFlags = map[string]bool{
	"symbol": true,
	"output": true,
	"from":   true,
	"last":   true,
	"to":     true,
	"resume": true,
}

func runSync(args []string, stdout io.Writer, stderr io.Writer) error {
	// sync accepts every download flag (it forwards them to download), with
	// --from/--last as fallbacks for a missing file and --to defaulting to now.
	fs, f := newDownloadFlagSet(stdout)
	fs.Init("sync", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintf(stdout, "%ssync:%s Sync an existing dataset file up to the present moment\n\n", colorize(colorCyan), colorize(colorReset))
		fmt.Fprint(stdout, "Usage:\n  dukascopy-go sync [options]\n\nAccepts every download option. --from/--last are only used when the file does not exist yet; --to defaults to now.\n\nOptions:\n")
		fs.PrintDefaults()
		fmt.Fprint(stdout, "\nExamples:\n  dukascopy-go sync --symbol eurusd --output ./eurusd_m1.csv\n  dukascopy-go sync --symbol eurusd --timeframe h1 --output ./eurusd_h1.csv\n  dukascopy-go sync --symbol eurusd,gbpusd --output ./data/\n  dukascopy-go sync --symbol eurusd --from 2024-01-01 --output ./eurusd_m1.csv\n")
	}

	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}

	symbol, outputPath, fromValue, lastValue, toValue := f.symbol, f.outputPath, f.fromValue, f.lastValue, f.toValue

	if strings.TrimSpace(*symbol) == "" {
		return errors.New("--symbol is required")
	}
	if strings.TrimSpace(*outputPath) == "" {
		return errors.New("--output is required")
	}
	if strings.TrimSpace(*outputPath) == "-" {
		return errors.New("sync needs a file --output, not - (stdout)")
	}

	// Multi-symbol batch sync.
	if strings.Contains(*symbol, ",") {
		symbols := splitSymbols(*symbol)
		if err := validateMultiSymbolOutput(*outputPath, symbols); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "%sbatch%s syncing %d symbols...\n", colorize(colorCyan), colorize(colorReset), len(symbols))
		for _, sym := range symbols {
			formattedOutput := formatMultiSymbolOutputPath(*outputPath, sym)
			nextArgs := append(forwardedFlags(fs, map[string]bool{"symbol": true, "output": true}), "--symbol", sym, "--output", formattedOutput)

			fmt.Fprintf(stdout, "%sbatch%s starting sync for %s -> %s\n", colorize(colorCyan), colorize(colorReset), sym, formattedOutput)
			if err := runSync(nextArgs, stdout, stderr); err != nil {
				return fmt.Errorf("sync for symbol %s failed: %w", sym, err)
			}
		}
		return nil
	}

	targetPath := *outputPath
	timeframe := requestedSyncTimeframe(fs, f)

	_, statErr := os.Stat(targetPath)
	exists := statErr == nil

	var from time.Time
	var resume bool

	if exists {
		lower := strings.ToLower(targetPath)
		if strings.HasSuffix(lower, ".parquet") || strings.HasSuffix(lower, ".arrow") || strings.HasSuffix(lower, ".ipc") || strings.HasSuffix(lower, ".feather") || csvout.IsJSONLPath(lower) {
			return errors.New("sync supports CSV files only; use partition mode for durable long-range parquet/arrow downloads")
		}

		state, err := csvout.InspectExistingCSV(targetPath)
		if err != nil {
			if os.IsNotExist(err) {
				exists = false
			} else {
				return fmt.Errorf("inspect existing CSV %s failed: %w", targetPath, err)
			}
		}

		if exists {
			if !state.HasRows {
				if strings.TrimSpace(*fromValue) == "" && strings.TrimSpace(*lastValue) == "" {
					return fmt.Errorf("target file %s exists but is empty; please provide fallback --from or --last to initialize", targetPath)
				}
			} else {
				if err := checkSyncFileTimeframe(targetPath, timeframe, *f.timestampFormat, *f.timezone); err != nil {
					return err
				}
				from = state.LastTime
				resume = true
			}
		}
	}

	if !exists || from.IsZero() {
		if strings.TrimSpace(*fromValue) == "" && strings.TrimSpace(*lastValue) == "" {
			return fmt.Errorf("target file %s does not exist; please provide fallback --from or --last to initialize", targetPath)
		}
	}

	to := time.Now().UTC()
	if strings.TrimSpace(*toValue) != "" {
		parsedTo, err := ParseFlexibleTime(*toValue)
		if err != nil {
			return fmt.Errorf("--to %w", err)
		}
		to = parsedTo.UTC()
	}

	downloadArgs := forwardedFlags(fs, syncOwnFlags)
	if resume && !flagWasSet(fs, "partition") {
		// Appending to the file is incompatible with partitioned output; keep a
		// partition default from the config file from turning sync into an error.
		downloadArgs = append(downloadArgs, "--partition=none")
	}
	downloadArgs = append(downloadArgs, "--symbol", *symbol, "--output", targetPath)

	if resume {
		downloadArgs = append(downloadArgs, "--resume", "--from", from.Format(time.RFC3339Nano))
	} else if strings.TrimSpace(*fromValue) != "" {
		downloadArgs = append(downloadArgs, "--from", *fromValue)
	} else if strings.TrimSpace(*lastValue) != "" {
		downloadArgs = append(downloadArgs, "--last", *lastValue)
	}
	downloadArgs = append(downloadArgs, "--to", to.Format(time.RFC3339Nano))

	fromLabel := "none (fresh)"
	if !from.IsZero() {
		fromLabel = from.Format(time.RFC3339Nano)
	}
	fmt.Fprintf(stdout, "%ssyncing%s %s in-place starting from: %s -> to: %s\n", colorize(colorCyan), colorize(colorReset), *symbol, fromLabel, to.Format(time.RFC3339Nano))

	return runDownload(downloadArgs, stdout, stderr)
}

// forwardedFlags renders every flag set on the command line, except the
// skipped ones, as --name=value arguments (this form is unambiguous for
// boolean flags too).
func forwardedFlags(fs *flag.FlagSet, skip map[string]bool) []string {
	var forwarded []string
	fs.Visit(func(current *flag.Flag) {
		if skip[current.Name] {
			return
		}
		forwarded = append(forwarded, "--"+current.Name+"="+current.Value.String())
	})
	return forwarded
}

// requestedSyncTimeframe returns the timeframe the forwarded download will
// use: --granularity, --timeframe, the config file default, then m1.
func requestedSyncTimeframe(fs *flag.FlagSet, f *downloadFlags) string {
	if strings.TrimSpace(*f.granularity) != "" {
		return strings.TrimSpace(*f.granularity)
	}
	if flagWasSet(fs, "timeframe") {
		return strings.TrimSpace(*f.timeframe)
	}
	if activeConfig != nil && strings.TrimSpace(activeConfig.Download.Timeframe) != "" {
		return strings.TrimSpace(activeConfig.Download.Timeframe)
	}
	return strings.TrimSpace(*f.timeframe)
}

// timeframeInterval returns the nominal bar interval of a timeframe (0 for
// tick and for unknown values).
func timeframeInterval(granularity dukascopy.Granularity) time.Duration {
	switch dukascopy.NormalizeGranularity(granularity) {
	case dukascopy.GranularityM1:
		return time.Minute
	case dukascopy.GranularityM3:
		return 3 * time.Minute
	case dukascopy.GranularityM5:
		return 5 * time.Minute
	case dukascopy.GranularityM15:
		return 15 * time.Minute
	case dukascopy.GranularityM30:
		return 30 * time.Minute
	case dukascopy.GranularityH1:
		return time.Hour
	case dukascopy.GranularityH4:
		return 4 * time.Hour
	case dukascopy.GranularityD1:
		return 24 * time.Hour
	case dukascopy.GranularityW1:
		return 7 * 24 * time.Hour
	case dukascopy.GranularityMN1:
		return 28 * 24 * time.Hour
	default:
		return 0
	}
}

// intervalMatchesTimeframe reports whether the smallest gap between rows of
// a file is consistent with granularity.
func intervalMatchesTimeframe(interval time.Duration, granularity dukascopy.Granularity) bool {
	normalized := dukascopy.NormalizeGranularity(granularity)
	if normalized == dukascopy.GranularityMN1 {
		return interval >= 28*24*time.Hour && interval <= 31*24*time.Hour
	}
	return interval == timeframeInterval(normalized)
}

// describeInterval names the timeframe matching interval, for error hints.
func describeInterval(interval time.Duration) string {
	for _, candidate := range supportedTimeframes {
		if candidate != dukascopy.GranularityTick && intervalMatchesTimeframe(interval, candidate) {
			return string(candidate)
		}
	}
	if interval < time.Minute {
		return "tick"
	}
	return interval.String()
}

// syncIntervalSampleRows is how many trailing rows are used to infer a file's
// bar interval; the smallest positive gap among them is the interval, which
// is robust to weekend and holiday gaps.
const syncIntervalSampleRows = 64

// checkSyncFileTimeframe refuses to append bars of timeframe to a file whose
// rows are spaced for a different timeframe.
func checkSyncFileTimeframe(path string, timeframe string, timestampFormat string, timezone string) error {
	granularity, err := validateTimeframe(timeframe)
	if err != nil {
		return err
	}
	interval, ok, err := detectFileInterval(path, timestampFormat, timezone)
	if err != nil {
		return fmt.Errorf("detect interval of %s: %w", path, err)
	}
	if !ok {
		return nil
	}
	if granularity == dukascopy.GranularityTick {
		if interval >= time.Minute && interval%time.Minute == 0 {
			return fmt.Errorf("%s looks like %s bars (rows %s apart) but sync would append tick rows; pass --timeframe %s", path, describeInterval(interval), interval, describeInterval(interval))
		}
		return nil
	}
	if !intervalMatchesTimeframe(interval, granularity) {
		return fmt.Errorf("%s looks like %s data (rows %s apart) but sync would append %s bars; pass --timeframe %s", path, describeInterval(interval), interval, granularity, describeInterval(interval))
	}
	return nil
}

// detectFileInterval returns the smallest positive gap between the
// timestamps of the last rows of a CSV/CSV.GZ file. ok is false when the file
// has fewer than two distinct timestamps. The delimiter is detected from the
// header line.
func detectFileInterval(path string, timestampFormat string, timezone string) (time.Duration, bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, false, err
	}
	defer file.Close()

	var source io.Reader = file
	if strings.HasSuffix(strings.ToLower(path), ".gz") {
		gz, err := gzip.NewReader(file)
		if err != nil {
			return 0, false, err
		}
		defer gz.Close()
		source = gz
	}
	buffered := bufio.NewReaderSize(source, 64*1024)
	firstLine, err := buffered.Peek(4096)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, bufio.ErrBufferFull) {
		return 0, false, err
	}
	header := string(firstLine)
	if index := strings.IndexAny(header, "\r\n"); index >= 0 {
		header = header[:index]
	}
	delimiter := detectDelimiter(header)

	loc, err := loadOutputLocation(timezone)
	if err != nil {
		loc = time.UTC
	}
	cfg := &csvout.Config{Location: loc, TimestampFormat: timestampFormat}

	reader := csv.NewReader(buffered)
	reader.Comma = delimiter
	reader.FieldsPerRecord = -1
	reader.ReuseRecord = true
	tsIndex := 0
	first := true
	// Ring buffer of the last raw timestamp cells; only those are parsed.
	var ring [syncIntervalSampleRows]string
	count := 0
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return 0, false, err
		}
		if first {
			first = false
			if index := indexOfColumn(record, "timestamp"); index >= 0 {
				tsIndex = index
				continue
			}
		}
		if tsIndex >= len(record) {
			continue
		}
		ring[count%syncIntervalSampleRows] = record[tsIndex]
		count++
	}

	start := 0
	if count > syncIntervalSampleRows {
		start = count - syncIntervalSampleRows
	}
	times := make([]time.Time, 0, syncIntervalSampleRows)
	for i := start; i < count; i++ {
		ts, err := cfg.ParseTimestamp(ring[i%syncIntervalSampleRows])
		if err != nil {
			return 0, false, err
		}
		times = append(times, ts)
	}

	var smallest time.Duration
	for i := 1; i < len(times); i++ {
		gap := times[i].Sub(times[i-1])
		if gap > 0 && (smallest == 0 || gap < smallest) {
			smallest = gap
		}
	}
	return smallest, smallest > 0, nil
}

// detectDelimiter picks the most frequent of the usual CSV separators in a
// header line, defaulting to a comma.
func detectDelimiter(header string) rune {
	best := ','
	bestCount := strings.Count(header, ",")
	for _, candidate := range []rune{';', '\t', '|'} {
		if count := strings.Count(header, string(candidate)); count > bestCount {
			best = candidate
			bestCount = count
		}
	}
	return best
}
