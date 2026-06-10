package csvout

import (
	"fmt"
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

type parquetCompressionCodec interface {
	// dummy interface just to type parquetCompression correctly
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

func parquetNodeForColumn(column string) parquet.Node {
	if strings.EqualFold(strings.TrimSpace(column), "timestamp") {
		return parquet.String()
	}
	return parquet.Leaf(parquet.DoubleType)
}

func parquetValueForColumn(column string, value string) (any, error) {
	if strings.EqualFold(strings.TrimSpace(column), "timestamp") {
		return value, nil
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

func parquetTimestampFromRow(row map[string]any) (time.Time, bool) {
	value, ok := row["timestamp"]
	if !ok {
		return time.Time{}, false
	}

	switch typed := value.(type) {
	case string:
		timestamp, err := time.Parse(timestampLayout, typed)
		if err != nil {
			return time.Time{}, false
		}
		return timestamp.UTC(), true
	case time.Time:
		return typed.UTC(), true
	default:
		timestamp, err := time.Parse(timestampLayout, fmt.Sprint(value))
		if err != nil {
			return time.Time{}, false
		}
		return timestamp.UTC(), true
	}
}

func isParquetPath(path string) bool {
	return strings.HasSuffix(strings.ToLower(strings.TrimSpace(path)), ".parquet")
}
