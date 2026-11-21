package csvout

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// ExtractCSVRange writes the rows of sourcePath with from <= timestamp < to
// to outputPath atomically. Either side may be CSV, CSV.GZ or Parquet.
func (c *Config) ExtractCSVRange(sourcePath string, outputPath string, from time.Time, to time.Time) error {
	if isParquetPath(sourcePath) {
		return c.extractRangeFromParquet(sourcePath, outputPath, from, to)
	}
	if isParquetPath(outputPath) {
		return c.extractRangeCSVToParquet(sourcePath, outputPath, from, to)
	}
	_, readerFactory, closeReader, err := c.openCSVReader(sourcePath)
	if err != nil {
		return err
	}
	defer closeReader()

	csvReader := readerFactory()
	header, err := csvReader.Read()
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

	_, csvWriter, closeWriter, err := c.createCSVWriter(tempPath)
	if err != nil {
		return err
	}

	if !c.HideHeader {
		if err := csvWriter.Write(header); err != nil {
			closeWriter()
			return err
		}
	}

	for {
		record, readErr := csvReader.Read()
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			closeWriter()
			return readErr
		}
		if len(record) == 0 {
			continue
		}
		if timestampIndex >= len(record) {
			closeWriter()
			return fmt.Errorf("source CSV %s contains a malformed row", sourcePath)
		}

		timestamp, err := c.ParseTimestamp(record[timestampIndex])
		if err != nil {
			closeWriter()
			return fmt.Errorf("parse source CSV timestamp %q: %w", record[timestampIndex], err)
		}
		if timestamp.Before(from) || !timestamp.Before(to) {
			continue
		}

		if err := csvWriter.Write(record); err != nil {
			closeWriter()
			return err
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

	return commitAtomic(tempPath, outputPath)
}

// ExtractCSVRange is (*Config).ExtractCSVRange on DefaultConfig.
func ExtractCSVRange(sourcePath string, outputPath string, from time.Time, to time.Time) error {
	return DefaultConfig().ExtractCSVRange(sourcePath, outputPath, from, to)
}

// StreamCSVRowsAfter copies rows of path whose timestamp is after after (all
// rows when after is zero) to w as CSV. It returns the number of rows written
// and the last timestamp written (after when none).
func (c *Config) StreamCSVRowsAfter(path string, w io.Writer, after time.Time, includeHeader bool) (int, time.Time, error) {
	if isParquetPath(path) {
		return 0, time.Time{}, fmt.Errorf("streaming parquet rows to writer is not supported")
	}

	_, readerFactory, closeReader, err := c.openCSVReader(path)
	if err != nil {
		return 0, time.Time{}, err
	}
	defer closeReader()

	csvReader := readerFactory()
	header, err := csvReader.Read()
	if err != nil {
		return 0, time.Time{}, err
	}

	timestampIndex := indexOfColumn(header, "timestamp")
	if timestampIndex < 0 {
		return 0, time.Time{}, fmt.Errorf("source CSV %s does not contain a timestamp column", path)
	}

	csvWriter := c.csvWriterFactory(w)
	flushed := false
	defer func() {
		if !flushed {
			csvWriter.Flush()
		}
	}()

	if includeHeader && !c.HideHeader {
		if err := csvWriter.Write(header); err != nil {
			return 0, time.Time{}, err
		}
	}

	rows := 0
	lastTimestamp := after
	for {
		record, readErr := csvReader.Read()
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return 0, time.Time{}, readErr
		}
		if len(record) == 0 {
			continue
		}
		if timestampIndex >= len(record) {
			return 0, time.Time{}, fmt.Errorf("source CSV %s contains a malformed row", path)
		}

		timestamp, err := c.ParseTimestamp(record[timestampIndex])
		if err != nil {
			return 0, time.Time{}, fmt.Errorf("parse source CSV timestamp %q: %w", record[timestampIndex], err)
		}
		if !after.IsZero() && (!timestamp.After(after)) {
			continue
		}

		if err := csvWriter.Write(record); err != nil {
			return 0, time.Time{}, err
		}
		rows++
		lastTimestamp = timestamp
	}

	// Flush before checking Error so a failure writing the last buffered rows
	// is reported.
	csvWriter.Flush()
	flushed = true
	if err := csvWriter.Error(); err != nil {
		return 0, time.Time{}, err
	}
	return rows, lastTimestamp, nil
}

// StreamCSVRowsAfter is (*Config).StreamCSVRowsAfter on DefaultConfig.
func StreamCSVRowsAfter(path string, w io.Writer, after time.Time, includeHeader bool) (int, time.Time, error) {
	return DefaultConfig().StreamCSVRowsAfter(path, w, after, includeHeader)
}

// InspectExistingCSV reads outputPath and returns its header, last row and
// last timestamp for resuming a download.
func (c *Config) InspectExistingCSV(outputPath string) (ResumeState, error) {
	_, readerFactory, closeReader, err := c.openCSVReader(outputPath)
	if err != nil {
		return ResumeState{}, err
	}
	defer closeReader()

	reader := readerFactory()
	header, err := reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return ResumeState{Exists: true}, nil
		}
		return ResumeState{}, err
	}

	state := ResumeState{
		Exists:  true,
		Columns: cloneColumns(header),
	}

	lastRecord := []string(nil)
	for {
		record, readErr := reader.Read()
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return ResumeState{}, readErr
		}
		if len(record) == 0 {
			continue
		}
		lastRecord = cloneColumns(record)
	}

	if len(lastRecord) == 0 {
		return state, nil
	}

	timestampIndex := indexOfColumn(header, "timestamp")
	if timestampIndex < 0 {
		return ResumeState{}, fmt.Errorf("existing CSV %s does not include a timestamp column, so --resume cannot be used", outputPath)
	}
	if timestampIndex >= len(lastRecord) {
		return ResumeState{}, fmt.Errorf("existing CSV %s has a malformed last row", outputPath)
	}

	lastTime, err := c.ParseTimestamp(lastRecord[timestampIndex])
	if err != nil {
		return ResumeState{}, fmt.Errorf("parse existing CSV timestamp %q: %w", lastRecord[timestampIndex], err)
	}

	state.HasRows = true
	state.LastRecord = lastRecord
	state.LastTime = lastTime
	return state, nil
}

// InspectExistingCSV is (*Config).InspectExistingCSV on DefaultConfig.
func InspectExistingCSV(outputPath string) (ResumeState, error) {
	return DefaultConfig().InspectExistingCSV(outputPath)
}

// GetResumeDuplicateTail returns up to maxRows trailing data rows of
// outputPath, each joined with the 0x1f separator; nil when the file is missing.
func GetResumeDuplicateTail(outputPath string, maxRows int) ([]string, error) {
	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		return nil, nil
	}
	_, csvReader, closeReader, err := openCSVReader(outputPath)
	if err != nil {
		return nil, err
	}
	defer closeReader()

	if _, err := csvReader.Read(); err != nil {
		return nil, nil
	}

	var allRows []string
	for {
		rec, rErr := csvReader.Read()
		if rErr != nil {
			if errors.Is(rErr, io.EOF) {
				break
			}
			return nil, rErr
		}
		if len(rec) > 0 {
			serialized := strings.Join(rec, "\x1f")
			allRows = append(allRows, serialized)
			if len(allRows) > maxRows {
				allRows = allRows[1:]
			}
		}
	}
	return allRows, nil
}
