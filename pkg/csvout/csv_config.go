package csvout

import (
	"encoding/csv"
	"io"
	"os"
	"reflect"
	"strings"
	"time"

	parquet "github.com/parquet-go/parquet-go"
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
var FillGaps string = "none"

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

func ColumnsContainTimestamp(columns []string) bool {
	for _, column := range columns {
		if strings.EqualFold(strings.TrimSpace(column), "timestamp") {
			return true
		}
	}
	return false
}
