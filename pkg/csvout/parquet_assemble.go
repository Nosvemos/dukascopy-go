package csvout

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)

// parquetWriteBatchSize is the number of rows buffered before a Parquet write.
const parquetWriteBatchSize = 4096

// parquetBatcher buffers rows and writes them to a Parquet writer in batches.
type parquetBatcher struct {
	writer parquetRecordWriter
	rows   []map[string]any
}

func newParquetBatcher(writer parquetRecordWriter) *parquetBatcher {
	return &parquetBatcher{writer: writer, rows: make([]map[string]any, 0, parquetWriteBatchSize)}
}

func (b *parquetBatcher) add(row map[string]any) error {
	b.rows = append(b.rows, row)
	if len(b.rows) >= parquetWriteBatchSize {
		return b.flush()
	}
	return nil
}

func (b *parquetBatcher) flush() error {
	if len(b.rows) == 0 {
		return nil
	}
	_, err := b.writer.Write(b.rows)
	// Rows are not reused by callers, so a fresh slice avoids retaining them.
	b.rows = make([]map[string]any, 0, parquetWriteBatchSize)
	return err
}

func (c *Config) assembleParquetFromCSVParts(outputPath string, partPaths []string, from time.Time, to time.Time) (retErr error) {
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
		batcher        *parquetBatcher
		file           *os.File
		lastTimestamp  int64
		haveLast       bool
		lastRecordKey  string
		headerPrepared bool
	)

	closeWriter := func() error {
		currentWriter := writer
		currentFile := file
		writer = nil
		file = nil

		var errs []error
		if currentWriter != nil {
			errs = append(errs, currentWriter.Close())
		}
		if currentFile != nil {
			errs = append(errs, currentFile.Close())
		}
		return errors.Join(errs...)
	}
	defer func() {
		if err := closeWriter(); err != nil {
			retErr = errors.Join(retErr, err)
		}
	}()

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
			batcher = newParquetBatcher(writer)
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
			if readErr != nil {
				if errors.Is(readErr, io.EOF) {
					break
				}
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

			timestamp, err := c.ParseTimestamp(record[timestampIndex])
			if err != nil {
				closeReader()
				return fmt.Errorf("parse partition timestamp %q: %w", record[timestampIndex], err)
			}
			if timestamp.Before(from) || !timestamp.Before(to) {
				continue
			}

			currentTimestamp := timestamp.UnixNano()
			recordKey := strings.Join(record, "\x1f")
			if haveLast && currentTimestamp == lastTimestamp {
				if recordKey != lastRecordKey {
					closeReader()
					return fmt.Errorf("conflicting duplicate timestamp %s while assembling %s", timestamp.Format(timestampLayout), outputPath)
				}
				continue
			}

			row, err := parquetRecordFromCSVRecord(columns, record)
			if err != nil {
				closeReader()
				return err
			}
			if err := batcher.add(row); err != nil {
				closeReader()
				return err
			}

			lastTimestamp = currentTimestamp
			haveLast = true
			lastRecordKey = recordKey
		}

		if err := closeReader(); err != nil {
			return err
		}
	}

	if batcher != nil {
		if err := batcher.flush(); err != nil {
			return err
		}
	}
	if err := closeWriter(); err != nil {
		return err
	}
	return commitAtomic(tempPath, outputPath)
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
	seenTimestamps := make(map[int64]struct{})
	duplicatesCount := 0
	rowNumber := 0

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
			rowNumber++
			timestamp, parseErr := c.rowTimestamp(row)
			if parseErr != nil {
				reader.Close()
				closeFile()
				return 0, fmt.Errorf("parquet %s row %d: %w", path, rowNumber, parseErr)
			}
			stampKey := timestamp.UnixNano()
			if _, seen := seenTimestamps[stampKey]; seen {
				duplicatesCount++
				continue
			}
			seenTimestamps[stampKey] = struct{}{}
			records = append(records, parquetRowRecord{timestamp: timestamp, row: row})
		}

		if errors.Is(err, io.EOF) {
			break
		}
	}

	if err := errors.Join(reader.Close(), closeFile()); err != nil {
		return 0, err
	}

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

	if err := commitAtomic(tempPath, path); err != nil {
		return 0, err
	}

	return duplicatesCount, nil
}

func cleanParquetDuplicates(path string) (int, error) {
	return DefaultConfig().cleanParquetDuplicates(path)
}

func assembleParquetFromCSVParts(outputPath string, partPaths []string, from time.Time, to time.Time) error {
	return DefaultConfig().assembleParquetFromCSVParts(outputPath, partPaths, from, to)
}
