package csvout

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	parquet "github.com/parquet-go/parquet-go"
)

const parquetColumnsMetadataKey = "dukascopy.columns"

type parquetRecordWriter interface {
	SetKeyValueMetadata(key string, value string)
	Write(rows []map[string]any) (int, error)
	Close() error
}

type parquetRecordReader interface {
	Read(rows []map[string]any) (int, error)
	Close() error
}

var parquetCompression = parquet.Compression(&parquet.Zstd)

func newParquetWriter(file *os.File, schema *parquet.Schema) parquetRecordWriter {
	return parquet.NewGenericWriter[map[string]any](file, schema, parquetCompression)
}

var defaultParquetWriterFactory = func(file *os.File, schema *parquet.Schema) parquetRecordWriter {
	return newParquetWriter(file, schema)
}

var parquetWriterFactory = defaultParquetWriterFactory

var defaultParquetReaderFactory = func(file *os.File, schema *parquet.Schema) parquetRecordReader {
	return parquet.NewGenericReader[map[string]any](file, schema)
}

var parquetReaderFactory = defaultParquetReaderFactory

func (c *Config) parquetWriterFactory(file *os.File, schema *parquet.Schema) parquetRecordWriter {
	if c.ParquetWriterFactory != nil {
		return c.ParquetWriterFactory(file, schema)
	}
	return newParquetWriter(file, schema)
}

func (c *Config) parquetReaderFactory(file *os.File, schema *parquet.Schema) parquetRecordReader {
	if c.ParquetReaderFactory != nil {
		return c.ParquetReaderFactory(file, schema)
	}
	return parquet.NewGenericReader[map[string]any](file, schema)
}

func parquetSchemaForColumns(columns []string) *parquet.Schema {
	group := make(parquet.Group, len(columns))
	for _, column := range columns {
		group[column] = parquetNodeForColumn(column)
	}
	return parquet.NewSchema("row", group)
}

// baseDataColumns lists every bar and tick column the package itself produces
// (see ParseBarColumns / ParseTickColumns). Any other column is a computed
// feature column, which may hold missing values (e.g. indicator warm-up rows).
var baseDataColumns = map[string]struct{}{
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
	"bid":        {},
	"ask":        {},
	"bid_volume": {},
	"ask_volume": {},
}

// isFeatureColumn reports whether column is a computed feature column rather
// than one of the base price/volume/timestamp columns.
func isFeatureColumn(column string) bool {
	_, ok := baseDataColumns[strings.ToLower(strings.TrimSpace(column))]
	return !ok
}

// isFiniteFloat reports whether value is neither NaN nor ±Inf.
func isFiniteFloat(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func parquetNodeForColumn(column string) parquet.Node {
	if strings.EqualFold(strings.TrimSpace(column), "timestamp") {
		return parquet.String()
	}
	if strings.EqualFold(strings.TrimSpace(column), "tick_count") {
		return parquet.Leaf(parquet.Int64Type)
	}
	if isFeatureColumn(column) {
		// Feature values can be missing (NaN/Inf warm-up rows); store them as
		// NULL rather than as IEEE NaN.
		return parquet.Optional(parquet.Leaf(parquet.DoubleType))
	}
	return parquet.Leaf(parquet.DoubleType)
}

func parquetValueForColumn(column string, value string) (any, error) {
	if strings.EqualFold(strings.TrimSpace(column), "timestamp") {
		return value, nil
	}
	if strings.EqualFold(strings.TrimSpace(column), "tick_count") {
		number, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse parquet tick count for column %q: %w", column, err)
		}
		return number, nil
	}
	number, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return nil, fmt.Errorf("parse parquet numeric value for column %q: %w", column, err)
	}
	return number, nil
}

func openParquetFile(path string) (*os.File, *parquet.File, func() error, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, nil, err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, nil, nil, err
	}

	parquetFile, err := parquet.OpenFile(file, info.Size())
	if err != nil {
		file.Close()
		return nil, nil, nil, err
	}

	closeFile := func() error {
		return file.Close()
	}
	return file, parquetFile, closeFile, nil
}

func parquetRowCount(path string) (int, error) {
	_, parquetFile, closeFile, err := openParquetFile(path)
	if err != nil {
		return 0, err
	}
	defer closeFile()
	return int(parquetFile.NumRows()), nil
}

func parquetColumns(file *parquet.File) []string {
	if value, ok := file.Lookup(parquetColumnsMetadataKey); ok && strings.TrimSpace(value) != "" {
		columns := make([]string, 0)
		for _, column := range strings.Split(value, ",") {
			trimmed := strings.TrimSpace(column)
			if trimmed != "" {
				columns = append(columns, trimmed)
			}
		}
		if len(columns) > 0 {
			return columns
		}
	}

	schemaColumns := file.Schema().Columns()
	columns := make([]string, 0, len(schemaColumns))
	for _, path := range schemaColumns {
		if len(path) == 0 {
			continue
		}
		columns = append(columns, path[len(path)-1])
	}
	return columns
}

func parquetRecordFromCSVRecord(columns []string, record []string) (map[string]any, error) {
	row := make(map[string]any, len(columns))
	for index, column := range columns {
		if index >= len(record) {
			return nil, fmt.Errorf("malformed CSV record for parquet conversion")
		}
		if isFeatureColumn(column) {
			// An empty (or legacy NaN/Inf) feature cell is a missing value:
			// leave it out so the optional Parquet column stores NULL.
			cell := strings.TrimSpace(record[index])
			if cell == "" {
				continue
			}
			number, err := strconv.ParseFloat(cell, 64)
			if err != nil {
				return nil, fmt.Errorf("parse parquet numeric value for column %q: %w", column, err)
			}
			if isFiniteFloat(number) {
				row[column] = number
			}
			continue
		}
		value, err := parquetValueForColumn(column, record[index])
		if err != nil {
			return nil, err
		}
		row[column] = value
	}
	return row, nil
}

func parquetRecordStrings(columns []string, row map[string]any) []string {
	record := make([]string, 0, len(columns))
	for _, column := range columns {
		record = append(record, parquetStringValue(row[column]))
	}
	return record
}

func parquetStringValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(typed), 'f', -1, 64)
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case int32:
		return strconv.FormatInt(int64(typed), 10)
	case uint64:
		return strconv.FormatUint(typed, 10)
	case uint32:
		return strconv.FormatUint(uint64(typed), 10)
	case time.Time:
		return typed.UTC().Format(timestampLayout)
	default:
		return fmt.Sprint(value)
	}
}

// rowTimestamp returns the instant stored in a Parquet row's "timestamp"
// column. String values are parsed with c.ParseTimestamp so files written with
// any timestamp format / location round-trip.
func (c *Config) rowTimestamp(row map[string]any) (time.Time, error) {
	value, ok := row["timestamp"]
	if !ok || value == nil {
		return time.Time{}, fmt.Errorf("row has no timestamp value")
	}

	switch typed := value.(type) {
	case string:
		return c.ParseTimestamp(typed)
	case time.Time:
		return typed.UTC(), nil
	default:
		return c.ParseTimestamp(fmt.Sprint(value))
	}
}

// parquetTimestampFromRow is rowTimestamp on DefaultConfig, reporting success
// as a bool.
func parquetTimestampFromRow(row map[string]any) (time.Time, bool) {
	timestamp, err := DefaultConfig().rowTimestamp(row)
	return timestamp, err == nil
}

func isParquetPath(path string) bool {
	return strings.HasSuffix(strings.ToLower(strings.TrimSpace(path)), ".parquet")
}
