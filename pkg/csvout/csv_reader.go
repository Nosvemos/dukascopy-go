package csvout

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

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
			if readErr.Error() == "EOF" {
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

		timestamp, err := parseFlexibleTimestamp(record[timestampIndex])
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

	return replaceFile(tempPath, outputPath)
}

func ExtractCSVRange(sourcePath string, outputPath string, from time.Time, to time.Time) error {
	return DefaultConfig().ExtractCSVRange(sourcePath, outputPath, from, to)
}

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
	defer csvWriter.Flush()

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
			if readErr.Error() == "EOF" {
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

		timestamp, err := parseFlexibleTimestamp(record[timestampIndex])
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

	if err := csvWriter.Error(); err != nil {
		return 0, time.Time{}, err
	}
	return rows, lastTimestamp, nil
}

func StreamCSVRowsAfter(path string, w io.Writer, after time.Time, includeHeader bool) (int, time.Time, error) {
	return DefaultConfig().StreamCSVRowsAfter(path, w, after, includeHeader)
}

func (c *Config) InspectExistingCSV(outputPath string) (ResumeState, error) {
	_, readerFactory, closeReader, err := c.openCSVReader(outputPath)
	if err != nil {
		return ResumeState{}, err
	}
	defer closeReader()

	reader := readerFactory()
	header, err := reader.Read()
	if err != nil {
		if err.Error() == "EOF" {
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
			if readErr.Error() == "EOF" {
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

	lastTime, err := parseFlexibleTimestamp(lastRecord[timestampIndex])
	if err != nil {
		return ResumeState{}, fmt.Errorf("parse existing CSV timestamp %q: %w", lastRecord[timestampIndex], err)
	}

	state.HasRows = true
	state.LastRecord = lastRecord
	state.LastTime = lastTime
	return state, nil
}

func InspectExistingCSV(outputPath string) (ResumeState, error) {
	return DefaultConfig().InspectExistingCSV(outputPath)
}

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
			if rErr.Error() == "EOF" {
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
