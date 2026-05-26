package csvout

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	parquet "github.com/parquet-go/parquet-go"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

const timestampLayout = time.RFC3339Nano

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

var OutputLocation *time.Location = time.UTC
var OutputTimestampFormat string = time.RFC3339Nano
var CSVDelimiter rune = ','
var HideCSVHeader bool = false

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

func DefaultConfig() *Config {
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

func (c *Config) formatTime(t time.Time) string {
	loc := c.Location
	if loc == nil {
		loc = time.UTC
	}
	layout := c.TimestampFormat
	if layout == "" {
		layout = time.RFC3339Nano
	}
	return t.In(loc).Format(layout)
}

func formatTime(t time.Time) string {
	return DefaultConfig().formatTime(t)
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

type ResumeState struct {
	Exists     bool
	Columns    []string
	HasRows    bool
	LastRecord []string
	LastTime   time.Time
}

type FileAudit struct {
	Rows   int
	Bytes  int64
	SHA256 string
}

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

func ParseBarColumns(value string) ([]string, error) {
	return parseColumns(value, map[string]struct{}{
		"timestamp": {},
		"open":      {},
		"high":      {},
		"low":       {},
		"close":     {},
		"mid_open":  {},
		"mid_high":  {},
		"mid_low":   {},
		"mid_close": {},
		"spread":    {},
		"volume":    {},
		"bid_open":  {},
		"bid_high":  {},
		"bid_low":   {},
		"bid_close": {},
		"ask_open":  {},
		"ask_high":  {},
		"ask_low":   {},
		"ask_close": {},
	})
}

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

func BarColumnsNeedBidAsk(columns []string) bool {
	for _, column := range columns {
		if strings.HasPrefix(column, "bid_") || strings.HasPrefix(column, "ask_") || strings.HasPrefix(column, "mid_") || column == "spread" {
			return true
		}
	}
	return false
}

func (c *Config) formatPrimaryBarColumn(column string, scale int, bar dukascopy.Bar) (string, error) {
	switch column {
	case "timestamp":
		return c.formatTime(bar.Time), nil
	case "open":
		return formatPrice(bar.Open, scale), nil
	case "high":
		return formatPrice(bar.High, scale), nil
	case "low":
		return formatPrice(bar.Low, scale), nil
	case "close":
		return formatPrice(bar.Close, scale), nil
	case "mid_open":
		return formatPrice(bar.Open, scale), nil
	case "mid_high":
		return formatPrice(bar.High, scale), nil
	case "mid_low":
		return formatPrice(bar.Low, scale), nil
	case "mid_close":
		return formatPrice(bar.Close, scale), nil
	case "volume":
		return formatVolume(bar.Volume), nil
	default:
		return "", fmt.Errorf("column %q requires bid/ask data or is unsupported for simple bars", column)
	}
}

func formatPrimaryBarColumn(column string, scale int, bar dukascopy.Bar) (string, error) {
	return DefaultConfig().formatPrimaryBarColumn(column, scale, bar)
}

func (c *Config) formatBarColumn(column string, scale int, bid dukascopy.Bar, ask dukascopy.Bar) (string, error) {
	roundedBidOpen := roundToScale(bid.Open, scale)
	roundedBidHigh := roundToScale(bid.High, scale)
	roundedBidLow := roundToScale(bid.Low, scale)
	roundedBidClose := roundToScale(bid.Close, scale)
	roundedAskOpen := roundToScale(ask.Open, scale)
	roundedAskHigh := roundToScale(ask.High, scale)
	roundedAskLow := roundToScale(ask.Low, scale)
	roundedAskClose := roundToScale(ask.Close, scale)

	switch column {
	case "timestamp":
		return c.formatTime(bid.Time), nil
	case "open":
		return formatMidPrice(midpoint(roundedBidOpen, roundedAskOpen), scale), nil
	case "high":
		return formatMidPrice(midpoint(roundedBidHigh, roundedAskHigh), scale), nil
	case "low":
		return formatMidPrice(midpoint(roundedBidLow, roundedAskLow), scale), nil
	case "close":
		return formatMidPrice(midpoint(roundedBidClose, roundedAskClose), scale), nil
	case "mid_open":
		return formatMidPrice(midpoint(roundedBidOpen, roundedAskOpen), scale), nil
	case "mid_high":
		return formatMidPrice(midpoint(roundedBidHigh, roundedAskHigh), scale), nil
	case "mid_low":
		return formatMidPrice(midpoint(roundedBidLow, roundedAskLow), scale), nil
	case "mid_close":
		return formatMidPrice(midpoint(roundedBidClose, roundedAskClose), scale), nil
	case "spread":
		return formatPrice(roundedAskClose-roundedBidClose, scale), nil
	case "volume":
		return formatVolume(bid.Volume), nil
	case "bid_open":
		return formatPrice(roundedBidOpen, scale), nil
	case "bid_high":
		return formatPrice(roundedBidHigh, scale), nil
	case "bid_low":
		return formatPrice(roundedBidLow, scale), nil
	case "bid_close":
		return formatPrice(roundedBidClose, scale), nil
	case "ask_open":
		return formatPrice(roundedAskOpen, scale), nil
	case "ask_high":
		return formatPrice(roundedAskHigh, scale), nil
	case "ask_low":
		return formatPrice(roundedAskLow, scale), nil
	case "ask_close":
		return formatPrice(roundedAskClose, scale), nil
	default:
		return "", fmt.Errorf("unsupported bar column %q", column)
	}
}

func formatBarColumn(column string, scale int, bid dukascopy.Bar, ask dukascopy.Bar) (string, error) {
	return DefaultConfig().formatBarColumn(column, scale, bid, ask)
}

func (c *Config) formatTickColumn(column string, scale int, tick dukascopy.Tick) (string, error) {
	switch column {
	case "timestamp":
		return c.formatTime(tick.Time), nil
	case "bid":
		return formatPrice(tick.Bid, scale), nil
	case "ask":
		return formatPrice(tick.Ask, scale), nil
	case "spread":
		return formatPrice(tick.Ask-tick.Bid, scale), nil
	case "bid_volume":
		return formatVolume(tick.BidVolume), nil
	case "ask_volume":
		return formatVolume(tick.AskVolume), nil
	default:
		return "", fmt.Errorf("unsupported tick column %q", column)
	}
}

func formatTickColumn(column string, scale int, tick dukascopy.Tick) (string, error) {
	return DefaultConfig().formatTickColumn(column, scale, tick)
}

func parseColumns(value string, allowed map[string]struct{}) ([]string, error) {
	parts := strings.Split(value, ",")
	columns := make([]string, 0, len(parts))
	for _, part := range parts {
		column := strings.TrimSpace(strings.ToLower(part))
		if column == "" {
			continue
		}
		if _, ok := allowed[column]; !ok {
			return nil, fmt.Errorf("unsupported column %q", column)
		}
		columns = append(columns, column)
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("at least one column must be provided")
	}
	return columns, nil
}

func cloneColumns(columns []string) []string {
	cloned := make([]string, len(columns))
	copy(cloned, columns)
	return cloned
}

func recordsEqual(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func indexOfColumn(columns []string, needle string) int {
	for index, column := range columns {
		if strings.EqualFold(strings.TrimSpace(column), needle) {
			return index
		}
	}
	return -1
}

func formatPrice(value float64, scale int) string {
	if scale <= 0 {
		return strconv.FormatFloat(value, 'f', -1, 64)
	}
	return strconv.FormatFloat(value, 'f', scale, 64)
}

func formatMidPrice(value float64, scale int) string {
	precision := scale + 1
	if precision < 0 {
		precision = -1
	}
	factor := math.Pow10(precision)
	rounded := math.Round(value*factor) / factor
	return strconv.FormatFloat(rounded, 'f', -1, 64)
}

func formatVolume(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func midpoint(a float64, b float64) float64 {
	return (a + b) / 2
}

func roundToScale(value float64, scale int) float64 {
	if scale < 0 {
		return value
	}
	factor := math.Pow10(scale)
	return math.Round(value*factor) / factor
}

func HeadersMatch(expected []string, actual []string) bool {
	if len(expected) != len(actual) {
		return false
	}
	for index := range expected {
		if expected[index] != actual[index] {
			return false
		}
	}
	return true
}

type combinedBarRow struct {
	Time time.Time
	Bid  dukascopy.Bar
	Ask  dukascopy.Bar
}

func combineBarRows(bidBars []dukascopy.Bar, askBars []dukascopy.Bar) ([]combinedBarRow, error) {
	if len(bidBars) != len(askBars) {
		return nil, fmt.Errorf("bid/ask bar length mismatch: %d vs %d", len(bidBars), len(askBars))
	}

	rows := make([]combinedBarRow, 0, len(bidBars))
	for index := range bidBars {
		if !bidBars[index].Time.Equal(askBars[index].Time) {
			return nil, fmt.Errorf("bid/ask timestamp mismatch at row %d: %s vs %s", index, bidBars[index].Time.UTC().Format(timestampLayout), askBars[index].Time.UTC().Format(timestampLayout))
		}
		rows = append(rows, combinedBarRow{
			Time: bidBars[index].Time,
			Bid:  bidBars[index],
			Ask:  askBars[index],
		})
	}

	return rows, nil
}

func ColumnsContainTimestamp(columns []string) bool {
	for _, column := range columns {
		if strings.EqualFold(strings.TrimSpace(column), "timestamp") {
			return true
		}
	}
	return false
}

var FillGaps string = "none"

func timeframeInterval(timeframe string) time.Duration {
	switch strings.ToLower(strings.TrimSpace(timeframe)) {
	case "m1", "minute":
		return time.Minute
	case "m3":
		return 3 * time.Minute
	case "m5":
		return 5 * time.Minute
	case "m15":
		return 15 * time.Minute
	case "m30":
		return 30 * time.Minute
	case "h1", "hour":
		return time.Hour
	case "h4":
		return 4 * time.Hour
	case "d1", "day":
		return 24 * time.Hour
	case "w1":
		return 7 * 24 * time.Hour
	default:
		return 0
	}
}
