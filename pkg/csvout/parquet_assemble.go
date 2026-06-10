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
			if errors.Is(err, io.EOF) || err.Error() == "EOF" {
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
			if readErr != nil {
				if readErr.Error() == "EOF" {
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

func cleanParquetDuplicates(path string) (int, error) {
	return DefaultConfig().cleanParquetDuplicates(path)
}

func assembleParquetFromCSVParts(outputPath string, partPaths []string, from time.Time, to time.Time) error {
	return DefaultConfig().assembleParquetFromCSVParts(outputPath, partPaths, from, to)
}
