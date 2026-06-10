package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/buildinfo"
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

func newDownloadContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt)
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
