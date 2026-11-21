package csvout

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	parquet "github.com/parquet-go/parquet-go"
)

const timestampLayout = time.RFC3339Nano

// flexibleTimestampLayouts lists all known timestamp formats that the tool may
// produce via --preset or --timestamp-format flags.  They are tried in order
// from most-specific to least-specific to avoid ambiguous matches.
var flexibleTimestampLayouts = []string{
	time.RFC3339Nano,            // default output format
	time.RFC3339,                // RFC3339 without nanoseconds
	"2006-01-02T15:04:05Z07:00", // explicit RFC3339 variant
	"2006-01-02 15:04:05",       // backtrader preset
	"2006.01.02 15:04:05",       // mt5 preset
	"2006.01.02 15:04",          // mt4 preset
	"20060102 150405",           // ninjatrader preset
	"2006-01-02 15:04",          // common short format
	"2006-01-02",                // date-only
}

// unixMilliMinDigits is the minimum number of digits a bare integer must have
// to be interpreted as Unix milliseconds. 13 digits covers every instant from
// 2001-09-09 onwards; shorter integers are rejected rather than silently being
// read as dates in 1970.
const unixMilliMinDigits = 13

// ParseTimestamp parses a timestamp cell written by this package (or by any of
// the CLI presets) back into an instant, returned in UTC.
//
// The configured TimestampFormat is tried first, then a list of known preset
// layouts. Layouts without a zone are interpreted in c.Location (UTC when nil),
// so a file written with a non-UTC location and a zone-less layout round-trips
// to the same instant. As a last resort a bare integer with at least 13 digits
// is read as Unix milliseconds.
func (c *Config) ParseTimestamp(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, fmt.Errorf("empty timestamp value")
	}

	loc := time.UTC
	layout := ""
	if c != nil {
		if c.Location != nil {
			loc = c.Location
		}
		layout = c.TimestampFormat
	}

	if layout != "" {
		if t, err := time.ParseInLocation(layout, value, loc); err == nil {
			return t.UTC(), nil
		}
	}
	for _, fallback := range flexibleTimestampLayouts {
		if fallback == layout {
			continue
		}
		if t, err := time.ParseInLocation(fallback, value, loc); err == nil {
			return t.UTC(), nil
		}
	}

	if isUnixMilliCandidate(value) {
		if ms, err := strconv.ParseInt(value, 10, 64); err == nil {
			return time.UnixMilli(ms).UTC(), nil
		}
	}

	return time.Time{}, fmt.Errorf("unrecognized timestamp format: %q", value)
}

func isUnixMilliCandidate(value string) bool {
	if len(value) < unixMilliMinDigits {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// ParseFlexibleTimestamp parses a timestamp cell using the package-level
// default configuration (OutputLocation / OutputTimestampFormat). See
// (*Config).ParseTimestamp for the rules.
func ParseFlexibleTimestamp(value string) (time.Time, error) {
	return DefaultConfig().ParseTimestamp(value)
}

// parseFlexibleTimestamp is kept for internal callers and tests.
func parseFlexibleTimestamp(value string) (time.Time, error) {
	return ParseFlexibleTimestamp(value)
}

// Profile names a predefined set of output columns.
type Profile string

const (
	ProfileSimple Profile = "simple"
	ProfileFull   Profile = "full"
	ProfileFused  Profile = "fused"
)

var simpleBarColumns = []string{"timestamp", "open", "high", "low", "close", "volume"}
var fullBarColumns = []string{"timestamp", "mid_open", "mid_high", "mid_low", "mid_close", "spread", "volume", "bid_open", "bid_high", "bid_low", "bid_close", "ask_open", "ask_high", "ask_low", "ask_close"}
var fusedBarColumns = []string{"timestamp", "bid_open", "bid_high", "bid_low", "bid_close", "ask_open", "ask_high", "ask_low", "ask_close", "spread", "volume"}
var simpleTickColumns = []string{"timestamp", "bid", "ask"}
var fullTickColumns = []string{"timestamp", "bid", "ask", "bid_volume", "ask_volume"}
var fusedTickColumns = []string{"timestamp", "bid", "ask", "spread", "bid_volume", "ask_volume"}

type csvRecordWriter interface {
	Write(record []string) error
	Flush()
	Error() error
}

type csvRecordReader interface {
	Read() ([]string, error)
}

// Package-level defaults captured by DefaultConfig. Guard writes with
// ConfigMutex.
var (
	// OutputLocation is the time zone timestamps are written in.
	OutputLocation *time.Location = time.UTC
	// OutputTimestampFormat is the Go layout used for timestamp cells.
	OutputTimestampFormat string = time.RFC3339Nano
	// CSVDelimiter is the CSV field separator.
	CSVDelimiter rune = ','
	// HideCSVHeader suppresses the header row on output.
	HideCSVHeader bool = false
	// FillGaps selects gap filling for bar output ("none" or "forward").
	FillGaps string = "none"
)

// ConfigMutex guards the package-level defaults above.
var ConfigMutex sync.RWMutex

// Config holds output settings. Methods on Config never read the package-level
// globals, so independent configurations can be used concurrently.
type Config struct {
	Location             *time.Location
	TimestampFormat      string
	CSVDelimiter         rune
	HideHeader           bool
	FillGaps             string
	WriterFactory        func(io.Writer) csvRecordWriter
	ReaderFactory        func(io.Reader) csvRecordReader
	ParquetWriterFactory func(*os.File, *parquet.Schema) parquetRecordWriter
	ParquetReaderFactory func(*os.File, *parquet.Schema) parquetRecordReader
}

// DefaultConfig returns a Config snapshot of the package-level defaults.
func DefaultConfig() *Config {
	ConfigMutex.RLock()
	defer ConfigMutex.RUnlock()

	loc := OutputLocation
	if loc == nil {
		loc = time.UTC
	}
	layout := OutputTimestampFormat
	if layout == "" {
		layout = time.RFC3339Nano
	}
	delim := CSVDelimiter
	if delim == 0 {
		delim = ','
	}

	var wFactory func(io.Writer) csvRecordWriter
	if reflect.ValueOf(csvWriterFactory).Pointer() != reflect.ValueOf(defaultCSVWriterFactory).Pointer() {
		wFactory = csvWriterFactory
	}

	var rFactory func(io.Reader) csvRecordReader
	if reflect.ValueOf(csvReaderFactory).Pointer() != reflect.ValueOf(defaultCSVReaderFactory).Pointer() {
		rFactory = csvReaderFactory
	}

	var pWFactory func(*os.File, *parquet.Schema) parquetRecordWriter
	if reflect.ValueOf(parquetWriterFactory).Pointer() != reflect.ValueOf(defaultParquetWriterFactory).Pointer() {
		pWFactory = parquetWriterFactory
	}

	var pRFactory func(*os.File, *parquet.Schema) parquetRecordReader
	if reflect.ValueOf(parquetReaderFactory).Pointer() != reflect.ValueOf(defaultParquetReaderFactory).Pointer() {
		pRFactory = parquetReaderFactory
	}

	return &Config{
		Location:             loc,
		TimestampFormat:      layout,
		CSVDelimiter:         delim,
		HideHeader:           HideCSVHeader,
		FillGaps:             FillGaps,
		WriterFactory:        wFactory,
		ReaderFactory:        rFactory,
		ParquetWriterFactory: pWFactory,
		ParquetReaderFactory: pRFactory,
	}
}

func (c *Config) csvWriterFactory(w io.Writer) csvRecordWriter {
	if c.WriterFactory != nil {
		return c.WriterFactory(w)
	}
	writer := csv.NewWriter(w)
	writer.Comma = c.CSVDelimiter
	return writer
}

var defaultCSVWriterFactory = func(w io.Writer) csvRecordWriter {
	writer := csv.NewWriter(w)
	writer.Comma = CSVDelimiter
	return writer
}

var csvWriterFactory = defaultCSVWriterFactory

func (c *Config) csvReaderFactory(r io.Reader) csvRecordReader {
	if c.ReaderFactory != nil {
		return c.ReaderFactory(r)
	}
	reader := csv.NewReader(r)
	reader.Comma = c.CSVDelimiter
	return reader
}

var defaultCSVReaderFactory = func(r io.Reader) csvRecordReader {
	reader := csv.NewReader(r)
	reader.Comma = CSVDelimiter
	return reader
}

var csvReaderFactory = defaultCSVReaderFactory

// ResumeState describes an existing output file for --resume.
type ResumeState struct {
	Exists     bool
	Columns    []string
	HasRows    bool
	LastRecord []string
	LastTime   time.Time
}

// FileAudit summarises a written file: data rows, size and SHA-256.
type FileAudit struct {
	Rows   int
	Bytes  int64
	SHA256 string
}

// BarColumnsForProfile returns the bar columns of a profile, or nil.
func BarColumnsForProfile(profile Profile) []string {
	switch profile {
	case ProfileSimple:
		return cloneColumns(simpleBarColumns)
	case ProfileFull:
		return cloneColumns(fullBarColumns)
	case ProfileFused:
		return cloneColumns(fusedBarColumns)
	default:
		return nil
	}
}

// TickColumnsForProfile returns the tick columns of a profile, or nil.
func TickColumnsForProfile(profile Profile) []string {
	switch profile {
	case ProfileSimple:
		return cloneColumns(simpleTickColumns)
	case ProfileFull:
		return cloneColumns(fullTickColumns)
	case ProfileFused:
		return cloneColumns(fusedTickColumns)
	default:
		return nil
	}
}

// ParseBarColumns parses a comma-separated list of bar column names.
func ParseBarColumns(value string) ([]string, error) {
	return parseColumns(value, map[string]struct{}{
		"timestamp":  {},
		"open":       {},
		"high":       {},
		"low":        {},
		"close":      {},
		"mid_open":   {},
		"mid_high":   {},
		"mid_low":    {},
		"mid_close":  {},
		"spread":     {},
		"volume":     {},
		"tick_count": {},
		"bid_open":   {},
		"bid_high":   {},
		"bid_low":    {},
		"bid_close":  {},
		"ask_open":   {},
		"ask_high":   {},
		"ask_low":    {},
		"ask_close":  {},
	})
}

// ParseTickColumns parses a comma-separated list of tick column names.
func ParseTickColumns(value string) ([]string, error) {
	return parseColumns(value, map[string]struct{}{
		"timestamp":  {},
		"bid":        {},
		"ask":        {},
		"spread":     {},
		"bid_volume": {},
		"ask_volume": {},
	})
}

// BarColumnsNeedBidAsk reports whether any column needs separate bid and ask bars.
func BarColumnsNeedBidAsk(columns []string) bool {
	for _, column := range columns {
		if strings.HasPrefix(column, "bid_") || strings.HasPrefix(column, "ask_") || strings.HasPrefix(column, "mid_") || column == "spread" {
			return true
		}
	}
	return false
}

// ColumnsContainTimestamp reports whether columns include "timestamp".
func ColumnsContainTimestamp(columns []string) bool {
	for _, column := range columns {
		if strings.EqualFold(strings.TrimSpace(column), "timestamp") {
			return true
		}
	}
	return false
}
