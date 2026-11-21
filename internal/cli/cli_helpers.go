package cli

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/buildinfo"
	"github.com/Nosvemos/dukascopy-go/pkg/csvout"
	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

func printUsage(w io.Writer) {
	fmt.Fprintf(w, "%sdukascopy-go%s\n\n", colorize(colorBold), colorize(colorReset))
	fmt.Fprintf(w, "Version: %s\n\n", buildinfo.VersionString())
	fmt.Fprintf(w, "Global flags: --config <path.json>\n\n")
	fmt.Fprintf(w, "%sCommands%s\n", colorize(colorCyan), colorize(colorReset))
	fmt.Fprint(w, `  instruments  Search Dukascopy instruments
  stats        Print dataset statistics
  manifest     Inspect or verify checkpoint manifests
  download     Download historical data and save it as CSV or Parquet
  sync         Sync an existing dataset file up to the present moment
  live         Stream real-time ticks/bars to stdout and optional WebSocket server
  watch        Poll prices/indicators and print or POST an alert when a condition is met
  db-load      Ingest a CSV or Parquet file directly into ClickHouse or InfluxDB
  list-timeframes  Print supported timeframe values
  version      Print build version information

examples:
  dukascopy-go instruments
  dukascopy-go instruments --query xauusd
  dukascopy-go download --symbol xauusd --timeframe m1 --from 2024-01-02T00:00:00Z --output ./xauusd.csv
  dukascopy-go sync --symbol xauusd --output ./xauusd.csv
  dukascopy-go download --symbol xauusd --timeframe d1 --from 2024-01-01T00:00:00Z --to 2024-02-01T00:00:00Z --output ./xauusd.parquet
  dukascopy-go live --symbol eurusd --timeframe tick --format jsonl
  dukascopy-go watch --symbol eurusd --condition "rsi < 30" --timeframe m1
  dukascopy-go db-load --input ./eurusd_m1.csv --db clickhouse --url http://localhost:8123 --table eurusd_m1
`)
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

func readBaseURL() string {
	if value := strings.TrimSpace(os.Getenv("DUKASCOPY_API_BASE_URL")); value != "" {
		return value
	}
	return defaultBaseURL
}

// newDownloadContext returns a context cancelled on Ctrl+C (SIGINT) or
// SIGTERM. syscall.SIGTERM exists on every platform Go supports, including
// Windows (where it is never delivered, so only os.Interrupt matters there).
func newDownloadContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// maxParallelism caps --parallelism.
const maxParallelism = 32

// splitSymbols splits a comma-separated --symbol value, dropping empty items.
func splitSymbols(value string) []string {
	var symbols []string
	for _, sym := range strings.Split(value, ",") {
		if sym = strings.TrimSpace(sym); sym != "" {
			symbols = append(symbols, sym)
		}
	}
	return symbols
}

// validateMultiSymbolOutput checks --output for a multi-symbol run: it is
// required, and stdout ("-") cannot hold several symbols.
func validateMultiSymbolOutput(outputPath string, symbols []string) error {
	if strings.TrimSpace(outputPath) == "" {
		return errors.New("--output is required (with several symbols use a directory, e.g. ./data/, or a {symbol} pattern)")
	}
	if strings.TrimSpace(outputPath) == "-" && len(symbols) > 1 {
		return errors.New("--output - (stdout) cannot be used with more than one symbol; use a directory or a {symbol} pattern")
	}
	return nil
}

// supportedTimeframes are the values accepted by --timeframe.
var supportedTimeframes = []dukascopy.Granularity{
	dukascopy.GranularityTick,
	dukascopy.GranularityM1,
	dukascopy.GranularityM3,
	dukascopy.GranularityM5,
	dukascopy.GranularityM15,
	dukascopy.GranularityM30,
	dukascopy.GranularityH1,
	dukascopy.GranularityH4,
	dukascopy.GranularityD1,
	dukascopy.GranularityW1,
	dukascopy.GranularityMN1,
}

// validateTimeframe normalizes and checks a timeframe value.
func validateTimeframe(value string) (dukascopy.Granularity, error) {
	normalized := dukascopy.NormalizeGranularity(dukascopy.Granularity(strings.TrimSpace(value)))
	for _, supported := range supportedTimeframes {
		if normalized == supported {
			return normalized, nil
		}
	}
	return "", fmt.Errorf("unsupported timeframe %q (supported: tick, m1, m3, m5, m15, m30, h1, h4, d1, w1, mn1)", value)
}

// validateSide checks a bid/ask side value (any casing).
func validateSide(value string) (dukascopy.PriceSide, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "bid":
		return dukascopy.PriceSideBid, nil
	case "ask":
		return dukascopy.PriceSideAsk, nil
	default:
		return "", fmt.Errorf("unsupported side %q (supported: bid, ask)", value)
	}
}

// validateFillGaps checks a --fill-gaps value.
func validateFillGaps(value string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(value))
	switch normalized {
	case "", "none":
		return "none", nil
	case "forward":
		return normalized, nil
	default:
		return "", fmt.Errorf("unsupported --fill-gaps value %q (supported: none, forward)", value)
	}
}

// validateParallelism checks --parallelism.
func validateParallelism(value int) error {
	if value < 1 || value > maxParallelism {
		return fmt.Errorf("--parallelism must be between 1 and %d, got %d", maxParallelism, value)
	}
	return nil
}

// loadOutputLocation resolves a --timezone value ("" and "UTC" mean UTC;
// "EET" and "EST" are aliases for Europe/Athens and America/New_York).
func loadOutputLocation(value string) (*time.Location, error) {
	tz := strings.TrimSpace(value)
	switch {
	case strings.EqualFold(tz, "eet"):
		if loc, err := time.LoadLocation("Europe/Athens"); err == nil {
			return loc, nil
		}
		return time.FixedZone("EET", 2*60*60), nil
	case strings.EqualFold(tz, "est"):
		if loc, err := time.LoadLocation("America/New_York"); err == nil {
			return loc, nil
		}
		return time.FixedZone("EST", -5*60*60), nil
	case tz == "" || strings.EqualFold(tz, "utc"):
		return time.UTC, nil
	default:
		loc, err := time.LoadLocation(tz)
		if err != nil {
			return nil, fmt.Errorf("invalid timezone %q: %w", value, err)
		}
		return loc, nil
	}
}

// newDetectedCSVReader returns a CSV reader over r whose delimiter (comma,
// semicolon, tab or pipe) is detected from the header line, so files written
// with --csv-delimiter or a preset such as ninjatrader can be read back.
func newDetectedCSVReader(r io.Reader) *csv.Reader {
	buffered := bufio.NewReaderSize(r, 64*1024)
	peek, _ := buffered.Peek(4096)
	header := string(peek)
	if index := strings.IndexAny(header, "\r\n"); index >= 0 {
		header = header[:index]
	}
	reader := csv.NewReader(buffered)
	reader.Comma = detectDelimiter(header)
	return reader
}

// loadTimestampConfig parses timestamps of files being loaded or repaired:
// RFC3339 and every preset layout (zone-less layouts in UTC).
var loadTimestampConfig = &csvout.Config{Location: time.UTC}

// parseLoadTimestamp parses a timestamp cell written by any download format
// (RFC3339, presets, --timestamp-format layouts known to csvout) or a bare
// integer of Unix milliseconds.
func parseLoadTimestamp(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if t, err := loadTimestampConfig.ParseTimestamp(value); err == nil {
		return t, nil
	}
	if ms, err := strconv.ParseInt(value, 10, 64); err == nil {
		return time.UnixMilli(ms).UTC(), nil
	}
	return time.Time{}, fmt.Errorf("unrecognized timestamp format: %q", value)
}

// redactURL hides the password of a URL (e.g. redis://user:secret@host)
// before it is printed. Unparseable values are replaced entirely.
func redactURL(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "<invalid URL>"
	}
	return parsed.Redacted()
}

// clampDownloadEnd clamps an end time in the future to now.
func clampDownloadEnd(to time.Time, now time.Time) time.Time {
	if to.After(now) {
		return now
	}
	return to
}

func ParseFlexibleTime(value string) (time.Time, error) {
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

func inclusiveDownloadEnd(value time.Time) time.Time {
	return value.UTC().Add(time.Nanosecond)
}

func colorize(code string) string {
	if strings.TrimSpace(os.Getenv("NO_COLOR")) != "" {
		return ""
	}
	return code
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

func instrumentFieldLengths(instruments []dukascopy.Instrument, selector func(dukascopy.Instrument) string) []int {
	lengths := make([]int, 0, len(instruments))
	for _, instrument := range instruments {
		lengths = append(lengths, len(selector(instrument)))
	}
	return lengths
}

func safeSymbolFilename(sym string) string {
	s := strings.ToLower(strings.TrimSpace(sym))
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.ReplaceAll(s, "-", "_")
	s = strings.ReplaceAll(s, " ", "_")
	return s
}

func formatMultiSymbolOutputPath(outputPath string, sym string) string {
	safeSym := safeSymbolFilename(sym)
	lowerPath := strings.ToLower(outputPath)

	if strings.Contains(lowerPath, "{symbol}") {
		formatted := outputPath
		for {
			l := strings.ToLower(formatted)
			idx := strings.Index(l, "{symbol}")
			if idx < 0 {
				break
			}
			formatted = formatted[:idx] + safeSym + formatted[idx+8:]
		}
		return formatted
	}

	fi, err := os.Stat(outputPath)
	isDir := err == nil && fi.IsDir()
	if isDir || strings.HasSuffix(outputPath, "/") || strings.HasSuffix(outputPath, "\\") {
		ext := ".csv"
		if strings.HasSuffix(lowerPath, ".parquet") {
			ext = ".parquet"
		} else if strings.HasSuffix(lowerPath, ".jsonl") {
			ext = ".jsonl"
		} else if strings.HasSuffix(lowerPath, ".gz") {
			ext = ".csv.gz"
		}
		return filepath.Join(outputPath, safeSym+ext)
	}

	ext := filepath.Ext(outputPath)
	base := strings.TrimSuffix(outputPath, ext)

	if ext == ".gz" && strings.HasSuffix(strings.ToLower(base), ".csv") {
		ext = ".csv.gz"
		base = strings.TrimSuffix(base, ".csv")
	}

	return fmt.Sprintf("%s-%s%s", base, safeSym, ext)
}
