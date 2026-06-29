package csvout

import (
	"fmt"
	"os"
	"sort"
	"time"
)

func (c *Config) AssembleCSVFromParts(outputPath string, partPaths []string, from time.Time, to time.Time) error {
	if isParquetPath(outputPath) {
		return c.assembleParquetFromCSVParts(outputPath, partPaths, from, to)
	}
	if len(partPaths) == 0 {
		return fmt.Errorf("no partition files were provided for assembly")
	}

	tempPath, err := createAtomicTempPath(outputPath)
	if err != nil {
		return err
	}
	defer os.Remove(tempPath)

	if err := ensureParentDir(tempPath); err != nil {
		return err
	}

	_, csvWriter, closeWriter, err := c.createCSVWriter(tempPath)
	if err != nil {
		return err
	}
	headerWritten := false
	var header []string
	timestampIndex := -1
	lastTimestamp := ""
	var lastRecord []string

	for _, partPath := range partPaths {
		_, readerFactory, closeReader, errReader := c.openCSVReader(partPath)
		if errReader != nil {
			closeWriter()
			return errReader
		}

		reader := readerFactory()
		partHeader, err := reader.Read()
		if err != nil {
			closeReader()
			if err.Error() == "EOF" {
				continue
			}
			closeWriter()
			return err
		}

		if !headerWritten {
			header = cloneColumns(partHeader)
			timestampIndex = indexOfColumn(header, "timestamp")
			if timestampIndex < 0 {
				closeReader()
				closeWriter()
				return fmt.Errorf("partition file %s does not contain a timestamp column", partPath)
			}
			if !c.HideHeader {
				if err := csvWriter.Write(header); err != nil {
					closeReader()
					closeWriter()
					return err
				}
			}
			headerWritten = true
		} else if !HeadersMatch(header, partHeader) {
			closeReader()
			closeWriter()
			return fmt.Errorf("partition file %s header does not match the assembled CSV header", partPath)
		}

		for {
			record, readErr := reader.Read()
			if readErr != nil {
				if readErr.Error() == "EOF" {
					break
				}
				closeReader()
				closeWriter()
				return readErr
			}
			if len(record) == 0 {
				continue
			}
			if timestampIndex >= len(record) {
				closeReader()
				closeWriter()
				return fmt.Errorf("partition file %s contains a malformed row", partPath)
			}

			timestamp, err := parseFlexibleTimestamp(record[timestampIndex])
			if err != nil {
				closeReader()
				closeWriter()
				return fmt.Errorf("parse partition timestamp %q: %w", record[timestampIndex], err)
			}
			if timestamp.Before(from) || !timestamp.Before(to) {
				continue
			}

			currentTimestamp := timestamp.Format(timestampLayout)
			if currentTimestamp == lastTimestamp {
				if !recordsEqual(record, lastRecord) {
					closeReader()
					closeWriter()
					return fmt.Errorf("conflicting duplicate timestamp %s while assembling %s", currentTimestamp, outputPath)
				}
				continue
			}

			if err := csvWriter.Write(record); err != nil {
				closeReader()
				closeWriter()
				return err
			}
			lastTimestamp = currentTimestamp
			lastRecord = cloneColumns(record)
		}

		closeReader()
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

func AssembleCSVFromParts(outputPath string, partPaths []string, from time.Time, to time.Time) error {
	return DefaultConfig().AssembleCSVFromParts(outputPath, partPaths, from, to)
}

func (c *Config) CleanDuplicates(path string) (int, error) {
	if isParquetPath(path) {
		return c.cleanParquetDuplicates(path)
	}

	// Warn about potential high memory usage for very large files
	if info, statErr := os.Stat(path); statErr == nil {
		sizeMB := info.Size() / (1024 * 1024)
		if sizeMB > 500 {
			fmt.Fprintf(os.Stderr, "warning: CleanDuplicates loads the entire file into memory; %s is %d MB which may cause high memory usage\n", path, sizeMB)
		}
	}

	_, readerFactory, closeReader, err := c.openCSVReader(path)
	if err != nil {
		return 0, err
	}

	reader := readerFactory()
	header, err := reader.Read()
	if err != nil {
		closeReader()
		return 0, err
	}

	timestampIndex := indexOfColumn(header, "timestamp")
	if timestampIndex < 0 {
		closeReader()
		return 0, fmt.Errorf("CSV does not contain a timestamp column")
	}

	type rowRecord struct {
		timestamp time.Time
		record    []string
	}

	var records []rowRecord
	seenTimestamps := make(map[string]bool)
	duplicatesCount := 0

	for {
		record, readErr := reader.Read()
		if readErr != nil {
			if readErr.Error() == "EOF" {
				break
			}
			closeReader()
			return 0, readErr
		}
		if len(record) == 0 {
			continue
		}

		t, err := parseFlexibleTimestamp(record[timestampIndex])
		if err != nil {
			closeReader()
			return 0, fmt.Errorf("failed to parse timestamp %q: %w", record[timestampIndex], err)
		}

		stampKey := t.Format(timestampLayout)
		if seenTimestamps[stampKey] {
			duplicatesCount++
			continue
		}
		seenTimestamps[stampKey] = true
		records = append(records, rowRecord{timestamp: t, record: record})
	}
	closeReader()

	// Sort records chronologically to fix out-of-order rows
	sort.SliceStable(records, func(i, j int) bool {
		return records[i].timestamp.Before(records[j].timestamp)
	})

	tempPath, err := createAtomicTempPath(path)
	if err != nil {
		return 0, err
	}
	defer os.Remove(tempPath)

	_, csvWriter, closeWriter, err := c.createCSVWriter(tempPath)
	if err != nil {
		return 0, err
	}

	if err := csvWriter.Write(header); err != nil {
		closeWriter()
		return 0, err
	}

	for _, rec := range records {
		if err := csvWriter.Write(rec.record); err != nil {
			closeWriter()
			return 0, err
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		closeWriter()
		return 0, err
	}
	if err := closeWriter(); err != nil {
		return 0, err
	}

	if err := replaceFile(tempPath, path); err != nil {
		return 0, err
	}

	return duplicatesCount, nil
}

func CleanDuplicates(path string) (int, error) {
	return DefaultConfig().CleanDuplicates(path)
}
