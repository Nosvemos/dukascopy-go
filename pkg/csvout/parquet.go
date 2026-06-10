package csvout

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
	"sort"

	parquet "github.com/parquet-go/parquet-go"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
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

// parquetCompression is the codec applied to every parquet file we write.
// Dukascopy tick/bar data compresses heavily (near-monotonic timestamps, tiny
// price deltas, repetitive volumes); zstd cuts uncompressed output ~5-6x.
// Previously the writer used no codec, so every column was stored raw.
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

func (c *Config) writeBarsParquet(outputPath string, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	records, err := c.buildBarParquetRecords(instrument, columns, primaryBars, bidBars, askBars)
	if err != nil {
		return err
	}
	return c.writeParquetRecords(outputPath, columns, records)
}

func (c *Config) writeTicksParquet(outputPath string, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	records, err := c.buildTickParquetRecords(instrument, columns, ticks)
	if err != nil {
		return err
	}
	return c.writeParquetRecords(outputPath, columns, records)
}

func (c *Config) buildBarParquetRecords(instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) ([]map[string]any, error) {
	if BarColumnsNeedBidAsk(columns) {
		rows, err := combineBarRows(bidBars, askBars)
		if err != nil {
			return nil, err
		}

		records := make([]map[string]any, 0, len(rows))
		for _, row := range rows {
			record := make(map[string]any, len(columns))
			for _, column := range columns {
				value, err := c.formatBarColumn(column, instrument.PriceScale, row.Bid, row.Ask)
				if err != nil {
					return nil, err
				}
				typed, err := parquetValueForColumn(column, value)
				if err != nil {
					return nil, err
				}
				record[column] = typed
			}
			records = append(records, record)
		}
		return records, nil
	}

	records := make([]map[string]any, 0, len(primaryBars))
	for _, bar := range primaryBars {
		record := make(map[string]any, len(columns))
		for _, column := range columns {
			value, err := c.formatPrimaryBarColumn(column, instrument.PriceScale, bar)
			if err != nil {
				return nil, err
			}
			typed, err := parquetValueForColumn(column, value)
			if err != nil {
				return nil, err
			}
			record[column] = typed
		}
		records = append(records, record)
	}
	return records, nil
}

func (c *Config) buildTickParquetRecords(instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) ([]map[string]any, error) {
	records := make([]map[string]any, 0, len(ticks))
	for _, tick := range ticks {
		record := make(map[string]any, len(columns))
		for _, column := range columns {
			value, err := c.formatTickColumn(column, instrument.PriceScale, tick)
			if err != nil {
				return nil, err
			}
			typed, err := parquetValueForColumn(column, value)
			if err != nil {
				return nil, err
			}
			record[column] = typed
		}
		records = append(records, record)
	}
	return records, nil
}

func (c *Config) writeParquetRecords(outputPath string, columns []string, records []map[string]any) error {
	if err := ensureParentDir(outputPath); err != nil {
		return err
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return err
	}

	schema := parquetSchemaForColumns(columns)
	writer := c.parquetWriterFactory(file, schema)
	defer file.Close()
	writer.SetKeyValueMetadata(parquetColumnsMetadataKey, strings.Join(columns, ","))
	if len(records) > 0 {
		if _, err := writer.Write(records); err != nil {
			return err
		}
	}
	if err := writer.Close(); err != nil {
		return err
	}
	return nil
}

func writeParquetRecords(outputPath string, columns []string, records []map[string]any) error {
	return DefaultConfig().writeParquetRecords(outputPath, columns, records)
}

type ParquetStreamWriter struct {
	file   *os.File
	writer parquetRecordWriter
}

func (c *Config) CreateParquetStreamWriter(outputPath string, columns []string) (*ParquetStreamWriter, error) {
	if err := ensureParentDir(outputPath); err != nil {
		return nil, err
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return nil, err
	}

	schema := parquetSchemaForColumns(columns)
	writer := c.parquetWriterFactory(file, schema)
	writer.SetKeyValueMetadata(parquetColumnsMetadataKey, strings.Join(columns, ","))

	return &ParquetStreamWriter{
		file:   file,
		writer: writer,
	}, nil
}

func CreateParquetStreamWriter(outputPath string, columns []string) (*ParquetStreamWriter, error) {
	return DefaultConfig().CreateParquetStreamWriter(outputPath, columns)
}

func (w *ParquetStreamWriter) WriteBatch(records []map[string]any) error {
	if len(records) == 0 {
		return nil
	}
	_, err := w.writer.Write(records)
	return err
}

func (w *ParquetStreamWriter) Close() error {
	var errs []string
	if err := w.writer.Close(); err != nil {
		errs = append(errs, err.Error())
	}
	if err := w.file.Close(); err != nil {
		errs = append(errs, err.Error())
	}
	if len(errs) > 0 {
		return fmt.Errorf("parquet stream writer close failed: %s", strings.Join(errs, "; "))
	}
	return nil
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

func auditParquet(path string) (FileAudit, error) {
	file, err := os.Open(path)
	if err != nil {
		return FileAudit{}, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return FileAudit{}, err
	}

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return FileAudit{}, err
	}

	rowCount, err := parquetRowCount(path)
	if err != nil {
		return FileAudit{}, err
	}

	return FileAudit{
		Rows:   rowCount,
		Bytes:  info.Size(),
		SHA256: hex.EncodeToString(hasher.Sum(nil)),
	}, nil
}

func inspectParquet(path string) (CSVStats, error) {
	return DefaultConfig().inspectParquetWithOptions(path, InspectOptions{})
}

func inspectParquetWithOptions(path string, options InspectOptions) (CSVStats, error) {
	return DefaultConfig().inspectParquetWithOptions(path, options)
}

func (c *Config) inspectParquetWithOptions(path string, options InspectOptions) (CSVStats, error) {
	stats := CSVStats{
		Path:       path,
		Compressed: false,
		Format:     "parquet",
		GapSymbol:  defaultGapSymbol(path, options.Symbol),
	}
	stats.GapProfile = ResolveGapMarketProfile(stats.GapSymbol, options.MarketProfile)

	file, parquetFile, closeFile, err := openParquetFile(path)
	if err != nil {
		return CSVStats{}, err
	}
	defer closeFile()

	stats.Columns = parquetColumns(parquetFile)
	timestampIndex := indexOfColumn(stats.Columns, "timestamp")
	stats.HasTimestamp = timestampIndex >= 0

	reader := c.parquetReaderFactory(file, parquetFile.Schema())
	defer reader.Close()

	seenRows := make(map[string]int)
	seenTimestamps := make(map[string]int)
	var intervals []time.Duration
	var gapObservations []gapObservation
	var previousTimestamp time.Time

	for {
		rows := make([]map[string]any, 256)
		for index := range rows {
			rows[index] = make(map[string]any, len(stats.Columns))
		}

		count, err := reader.Read(rows)
		if err != nil && !errors.Is(err, io.EOF) {
			return CSVStats{}, err
		}

		for _, row := range rows[:count] {
			record := parquetRecordStrings(stats.Columns, row)
			stats.Rows++

			rowKey := strings.Join(record, "\x1f")
			if seenRows[rowKey] > 0 {
				stats.DuplicateRows++
			}
			seenRows[rowKey]++

			if !stats.HasTimestamp || timestampIndex >= len(record) {
				continue
			}

			timestamp, err := time.Parse(timestampLayout, record[timestampIndex])
			if err != nil {
				return CSVStats{}, fmt.Errorf("parse parquet timestamp %q: %w", record[timestampIndex], err)
			}
			timestamp = timestamp.UTC()
			if stats.FirstTimestamp.IsZero() || timestamp.Before(stats.FirstTimestamp) {
				stats.FirstTimestamp = timestamp
			}
			if stats.LastTimestamp.IsZero() || timestamp.After(stats.LastTimestamp) {
				stats.LastTimestamp = timestamp
			}

			stampKey := timestamp.Format(timestampLayout)
			if seenTimestamps[stampKey] > 0 {
				stats.DuplicateStamps++
			}
			seenTimestamps[stampKey]++

			if !previousTimestamp.IsZero() {
				delta := timestamp.Sub(previousTimestamp)
				if delta > 0 {
					intervals = append(intervals, delta)
					gapObservations = append(gapObservations, gapObservation{
						Previous: previousTimestamp,
						Current:  timestamp,
						Interval: delta,
					})
				} else if delta < 0 {
					stats.OutOfOrderRows++
				}
			}
			previousTimestamp = timestamp
		}

		if errors.Is(err, io.EOF) {
			break
		}
	}

	expectedInterval := inferExpectedInterval(intervals)
	stats.InferredTimeframe = inferTimeframe(intervals)
	if expectedInterval > 0 {
		stats.ExpectedInterval = expectedInterval.String()
		applyGapStats(&stats, gapObservations, expectedInterval, stats.GapSymbol, stats.GapProfile, options)
	}

	return stats, nil
}

func (c *Config) assembleParquetFromCSVParts(outputPath string, partPaths []string, from time.Time, to time.Time) error {
	if len(partPaths) == 0 {
		return fmt.Errorf("no partition files were provided for assembly")
	}

	tempPath, err := createAtomicTempPath(outputPath)
	if err != nil {
		return err
	}
	defer os.Remove(tempPath)

	var (
		columns        []string
		writer         parquetRecordWriter
		file           *os.File
		lastTimestamp  string
		lastRecordKey  string
		headerPrepared bool
	)

	closeWriter := func() error {
		currentWriter := writer
		currentFile := file
		writer = nil
		file = nil

		if currentWriter != nil {
			if err := currentWriter.Close(); err != nil {
				if currentFile != nil {
					currentFile.Close()
				}
				return err
			}
		}
		if currentFile != nil {
			return currentFile.Close()
		}
		return nil
	}
	defer closeWriter()

	for _, partPath := range partPaths {
		_, readerFactory, closeReader, err := c.openCSVReader(partPath)
		if err != nil {
			return err
		}

		reader := readerFactory()
		partHeader, err := reader.Read()
		if err != nil {
			closeReader()
			if errors.Is(err, io.EOF) {
				continue
			}
			return err
		}

		if !headerPrepared {
			columns = cloneColumns(partHeader)
			file, err = os.Create(tempPath)
			if err != nil {
				closeReader()
				return err
			}
			writer = c.parquetWriterFactory(file, parquetSchemaForColumns(columns))
			writer.SetKeyValueMetadata(parquetColumnsMetadataKey, strings.Join(columns, ","))
			headerPrepared = true
		} else if !HeadersMatch(columns, partHeader) {
			closeReader()
			return fmt.Errorf("partition file %s header does not match the assembled output header", partPath)
		}

		timestampIndex := indexOfColumn(columns, "timestamp")
		if timestampIndex < 0 {
			closeReader()
			return fmt.Errorf("partition file %s does not contain a timestamp column", partPath)
		}

		for {
			record, readErr := reader.Read()
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				closeReader()
				return readErr
			}
			if len(record) == 0 {
				continue
			}
			if timestampIndex >= len(record) {
				closeReader()
				return fmt.Errorf("partition file %s contains a malformed row", partPath)
			}

			timestamp, err := time.Parse(timestampLayout, record[timestampIndex])
			if err != nil {
				closeReader()
				return fmt.Errorf("parse partition timestamp %q: %w", record[timestampIndex], err)
			}
			timestamp = timestamp.UTC()
			if timestamp.Before(from) || !timestamp.Before(to) {
				continue
			}

			currentTimestamp := timestamp.Format(timestampLayout)
			recordKey := strings.Join(record, "\x1f")
			if currentTimestamp == lastTimestamp {
				if recordKey != lastRecordKey {
					closeReader()
					return fmt.Errorf("conflicting duplicate timestamp %s while assembling %s", currentTimestamp, outputPath)
				}
				continue
			}

			row, err := parquetRecordFromCSVRecord(columns, record)
			if err != nil {
				closeReader()
				return err
			}
			if _, err := writer.Write([]map[string]any{row}); err != nil {
				closeReader()
				return err
			}

			lastTimestamp = currentTimestamp
			lastRecordKey = recordKey
		}

		if err := closeReader(); err != nil {
			return err
		}
	}

	if err := closeWriter(); err != nil {
		return err
	}
	return replaceFile(tempPath, outputPath)
}

func (c *Config) extractRangeFromParquet(sourcePath string, outputPath string, from time.Time, to time.Time) error {
	file, parquetFile, closeFile, err := openParquetFile(sourcePath)
	if err != nil {
		return err
	}
	defer closeFile()

	columns := parquetColumns(parquetFile)
	if indexOfColumn(columns, "timestamp") < 0 {
		return fmt.Errorf("source parquet %s does not contain a timestamp column", sourcePath)
	}

	reader := c.parquetReaderFactory(file, parquetFile.Schema())
	defer reader.Close()

	if isParquetPath(outputPath) {
		tempPath, err := createAtomicTempPath(outputPath)
		if err != nil {
			return err
		}
		defer os.Remove(tempPath)

		outFile, err := os.Create(tempPath)
		if err != nil {
			return err
		}
		writer := c.parquetWriterFactory(outFile, parquetSchemaForColumns(columns))
		writer.SetKeyValueMetadata(parquetColumnsMetadataKey, strings.Join(columns, ","))

		for {
			rows := make([]map[string]any, 256)
			for index := range rows {
				rows[index] = make(map[string]any, len(columns))
			}

			count, err := reader.Read(rows)
			if err != nil && !errors.Is(err, io.EOF) {
				writer.Close()
				outFile.Close()
				return err
			}

			filtered := make([]map[string]any, 0, count)
			for _, row := range rows[:count] {
				timestamp, ok := parquetTimestampFromRow(row)
				if !ok {
					continue
				}
				if timestamp.Before(from) || !timestamp.Before(to) {
					continue
				}
				filtered = append(filtered, row)
			}
			if len(filtered) > 0 {
				if _, err := writer.Write(filtered); err != nil {
					writer.Close()
					outFile.Close()
					return err
				}
			}

			if errors.Is(err, io.EOF) {
				break
			}
		}

		if err := writer.Close(); err != nil {
			outFile.Close()
			return err
		}
		if err := outFile.Close(); err != nil {
			return err
		}
		return replaceFile(tempPath, outputPath)
	}

	tempPath, err := createAtomicTempPath(outputPath)
	if err != nil {
		return err
	}
	defer os.Remove(tempPath)

	_, csvWriter, closeWriter, err := c.createCSVWriter(tempPath)
	if err != nil {
		return err
	}
	if err := csvWriter.Write(columns); err != nil {
		closeWriter()
		return err
	}

	for {
		rows := make([]map[string]any, 256)
		for index := range rows {
			rows[index] = make(map[string]any, len(columns))
		}

		count, err := reader.Read(rows)
		if err != nil && !errors.Is(err, io.EOF) {
			closeWriter()
			return err
		}

		for _, row := range rows[:count] {
			timestamp, ok := parquetTimestampFromRow(row)
			if !ok {
				continue
			}
			if timestamp.Before(from) || !timestamp.Before(to) {
				continue
			}
			record := parquetRecordStrings(columns, row)
			if err := csvWriter.Write(record); err != nil {
				closeWriter()
				return err
			}
		}

		if errors.Is(err, io.EOF) {
			break
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		closeWriter()
		return err
	}
	if err := closeWriter(); err != nil {
		return err
	}
	return replaceFile(tempPath, outputPath)
}

func (c *Config) extractRangeCSVToParquet(sourcePath string, outputPath string, from time.Time, to time.Time) error {
	_, readerFactory, closeReader, err := c.openCSVReader(sourcePath)
	if err != nil {
		return err
	}
	defer closeReader()

	reader := readerFactory()
	header, err := reader.Read()
	if err != nil {
		return err
	}

	timestampIndex := indexOfColumn(header, "timestamp")
	if timestampIndex < 0 {
		return fmt.Errorf("source CSV %s does not contain a timestamp column", sourcePath)
	}

	tempPath, err := createAtomicTempPath(outputPath)
	if err != nil {
		return err
	}
	defer os.Remove(tempPath)

	file, err := os.Create(tempPath)
	if err != nil {
		return err
	}
	writer := c.parquetWriterFactory(file, parquetSchemaForColumns(header))
	writer.SetKeyValueMetadata(parquetColumnsMetadataKey, strings.Join(header, ","))

	for {
		record, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			writer.Close()
			file.Close()
			return readErr
		}
		if len(record) == 0 {
			continue
		}
		if timestampIndex >= len(record) {
			writer.Close()
			file.Close()
			return fmt.Errorf("source CSV %s contains a malformed row", sourcePath)
		}

		timestamp, err := time.Parse(timestampLayout, record[timestampIndex])
		if err != nil {
			writer.Close()
			file.Close()
			return fmt.Errorf("parse source CSV timestamp %q: %w", record[timestampIndex], err)
		}
		timestamp = timestamp.UTC()
		if timestamp.Before(from) || !timestamp.Before(to) {
			continue
		}

		row, err := parquetRecordFromCSVRecord(header, record)
		if err != nil {
			writer.Close()
			file.Close()
			return err
		}
		if _, err := writer.Write([]map[string]any{row}); err != nil {
			writer.Close()
			file.Close()
			return err
		}
	}

	if err := writer.Close(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return replaceFile(tempPath, outputPath)
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

func (c *Config) cleanParquetDuplicates(path string) (int, error) {
	file, parquetFile, closeFile, err := openParquetFile(path)
	if err != nil {
		return 0, err
	}

	columns := parquetColumns(parquetFile)
	timestampIndex := indexOfColumn(columns, "timestamp")
	if timestampIndex < 0 {
		closeFile()
		return 0, fmt.Errorf("parquet does not contain a timestamp column")
	}

	reader := c.parquetReaderFactory(file, parquetFile.Schema())

	type parquetRowRecord struct {
		timestamp time.Time
		row       map[string]any
	}

	var records []parquetRowRecord
	seenTimestamps := make(map[string]bool)
	duplicatesCount := 0

	for {
		rows := make([]map[string]any, 256)
		for index := range rows {
			rows[index] = make(map[string]any, len(columns))
		}

		count, err := reader.Read(rows)
		if err != nil && !errors.Is(err, io.EOF) {
			reader.Close()
			closeFile()
			return 0, err
		}

		for _, row := range rows[:count] {
			timestamp, ok := parquetTimestampFromRow(row)
			if !ok {
				continue
			}
			timestamp = timestamp.UTC()
			stampKey := timestamp.Format(timestampLayout)
			if seenTimestamps[stampKey] {
				duplicatesCount++
				continue
			}
			seenTimestamps[stampKey] = true
			records = append(records, parquetRowRecord{timestamp: timestamp, row: row})
		}

		if errors.Is(err, io.EOF) {
			break
		}
	}

	reader.Close()
	closeFile()

	// Sort chronologically
	sort.SliceStable(records, func(i, j int) bool {
		return records[i].timestamp.Before(records[j].timestamp)
	})

	// Write back atomically
	tempPath, err := createAtomicTempPath(path)
	if err != nil {
		return 0, err
	}
	defer os.Remove(tempPath)

	plainRecords := make([]map[string]any, len(records))
	for i, rec := range records {
		plainRecords[i] = rec.row
	}

	if err := c.writeParquetRecords(tempPath, columns, plainRecords); err != nil {
		return 0, err
	}

	if err := replaceFile(tempPath, path); err != nil {
		return 0, err
	}

	return duplicatesCount, nil
}

func buildBarParquetRecords(instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) ([]map[string]any, error) {
	return DefaultConfig().buildBarParquetRecords(instrument, columns, primaryBars, bidBars, askBars)
}

func buildTickParquetRecords(instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) ([]map[string]any, error) {
	return DefaultConfig().buildTickParquetRecords(instrument, columns, ticks)
}

func extractRangeCSVToParquet(sourcePath string, outputPath string, from time.Time, to time.Time) error {
	return DefaultConfig().extractRangeCSVToParquet(sourcePath, outputPath, from, to)
}

func extractRangeFromParquet(sourcePath string, outputPath string, from time.Time, to time.Time) error {
	return DefaultConfig().extractRangeFromParquet(sourcePath, outputPath, from, to)
}

func assembleParquetFromCSVParts(outputPath string, partPaths []string, from time.Time, to time.Time) error {
	return DefaultConfig().assembleParquetFromCSVParts(outputPath, partPaths, from, to)
}

func writeBarsParquet(outputPath string, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	return DefaultConfig().writeBarsParquet(outputPath, instrument, columns, primaryBars, bidBars, askBars)
}

func writeTicksParquet(outputPath string, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	return DefaultConfig().writeTicksParquet(outputPath, instrument, columns, ticks)
}

func cleanParquetDuplicates(path string) (int, error) {
	return DefaultConfig().cleanParquetDuplicates(path)
}

