package csvout

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

func WriteBars(outputPath string, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	if isParquetPath(outputPath) {
		return writeBarsParquet(outputPath, instrument, columns, primaryBars, bidBars, askBars)
	}
	if IsJSONLPath(outputPath) {
		return writeBarsJSONL(outputPath, instrument, columns, primaryBars, bidBars, askBars)
	}
	if err := ensureParentDir(outputPath); err != nil {
		return err
	}

	_, csvWriter, closeWriter, err := createCSVWriter(outputPath)
	if err != nil {
		return err
	}
	defer closeWriter()

	return writeBarsCSV(csvWriter, instrument, columns, primaryBars, bidBars, askBars)
}

func WriteBarsToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	csvWriter := csvWriterFactory(w)
	defer csvWriter.Flush()

	return writeBarsCSV(csvWriter, instrument, columns, primaryBars, bidBars, askBars)
}

func WriteBarsRowsToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	csvWriter := csvWriterFactory(w)
	defer csvWriter.Flush()

	return writeBarsCSVRows(csvWriter, instrument, columns, primaryBars, bidBars, askBars, false)
}

func WriteTicks(outputPath string, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	if isParquetPath(outputPath) {
		return writeTicksParquet(outputPath, instrument, columns, ticks)
	}
	if IsJSONLPath(outputPath) {
		return writeTicksJSONL(outputPath, instrument, columns, ticks)
	}
	if err := ensureParentDir(outputPath); err != nil {
		return err
	}

	_, csvWriter, closeWriter, err := createCSVWriter(outputPath)
	if err != nil {
		return err
	}
	defer closeWriter()

	return writeTicksCSV(csvWriter, instrument, columns, ticks)
}

func WriteTicksToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	csvWriter := csvWriterFactory(w)
	defer csvWriter.Flush()

	return writeTicksCSV(csvWriter, instrument, columns, ticks)
}

func WriteTicksRowsToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	csvWriter := csvWriterFactory(w)
	defer csvWriter.Flush()

	return writeTicksCSVRows(csvWriter, instrument, columns, ticks, false)
}

func writeBarsCSV(csvWriter csvRecordWriter, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	return writeBarsCSVRows(csvWriter, instrument, columns, primaryBars, bidBars, askBars, true)
}

func writeBarsCSVRows(csvWriter csvRecordWriter, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar, includeHeader bool) error {
	if includeHeader && !HideCSVHeader {
		if err := csvWriter.Write(columns); err != nil {
			return err
		}
	}

	if BarColumnsNeedBidAsk(columns) {
		rows, err := combineBarRows(bidBars, askBars)
		if err != nil {
			return err
		}

		for _, row := range rows {
			record := make([]string, 0, len(columns))
			for _, column := range columns {
				value, valueErr := formatBarColumn(column, instrument.PriceScale, row.Bid, row.Ask)
				if valueErr != nil {
					return valueErr
				}
				record = append(record, value)
			}
			if err := csvWriter.Write(record); err != nil {
				return err
			}
		}

		return csvWriter.Error()
	}

	for _, bar := range primaryBars {
		record := make([]string, 0, len(columns))
		for _, column := range columns {
			value, err := formatPrimaryBarColumn(column, instrument.PriceScale, bar)
			if err != nil {
				return err
			}
			record = append(record, value)
		}
		if err := csvWriter.Write(record); err != nil {
			return err
		}
	}

	return csvWriter.Error()
}

func writeTicksCSV(csvWriter csvRecordWriter, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	return writeTicksCSVRows(csvWriter, instrument, columns, ticks, true)
}

func writeTicksCSVRows(csvWriter csvRecordWriter, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick, includeHeader bool) error {
	if includeHeader && !HideCSVHeader {
		if err := csvWriter.Write(columns); err != nil {
			return err
		}
	}

	for _, tick := range ticks {
		record := make([]string, 0, len(columns))
		for _, column := range columns {
			value, valueErr := formatTickColumn(column, instrument.PriceScale, tick)
			if valueErr != nil {
				return valueErr
			}
			record = append(record, value)
		}
		if err := csvWriter.Write(record); err != nil {
			return err
		}
	}

	return csvWriter.Error()
}

func WriteBarsAtomic(outputPath string, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	tempPath, err := createAtomicTempPath(outputPath)
	if err != nil {
		return err
	}
	defer os.Remove(tempPath)

	if err := WriteBars(tempPath, instrument, columns, primaryBars, bidBars, askBars); err != nil {
		return err
	}
	return replaceFile(tempPath, outputPath)
}

func WriteTicksAtomic(outputPath string, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	tempPath, err := createAtomicTempPath(outputPath)
	if err != nil {
		return err
	}
	defer os.Remove(tempPath)

	if err := WriteTicks(tempPath, instrument, columns, ticks); err != nil {
		return err
	}
	return replaceFile(tempPath, outputPath)
}

func AssembleCSVFromParts(outputPath string, partPaths []string, from time.Time, to time.Time) error {
	if isParquetPath(outputPath) {
		return assembleParquetFromCSVParts(outputPath, partPaths, from, to)
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

	_, csvWriter, closeWriter, err := createCSVWriter(tempPath)
	if err != nil {
		return err
	}
	headerWritten := false
	var header []string
	timestampIndex := -1
	lastTimestamp := ""
	var lastRecord []string

	for _, partPath := range partPaths {
		file, err := os.Open(partPath)
		if err != nil {
			closeWriter()
			return err
		}

		reader := csvReaderFactory(file)
		partHeader, err := reader.Read()
		if err != nil {
			file.Close()
			if errors.Is(err, io.EOF) {
				continue
			}
			closeWriter()
			return err
		}

		if !headerWritten {
			header = cloneColumns(partHeader)
			timestampIndex = indexOfColumn(header, "timestamp")
			if timestampIndex < 0 {
				file.Close()
				closeWriter()
				return fmt.Errorf("partition file %s does not contain a timestamp column", partPath)
			}
			if !HideCSVHeader {
				if err := csvWriter.Write(header); err != nil {
					file.Close()
					closeWriter()
					return err
				}
			}
			headerWritten = true
		} else if !HeadersMatch(header, partHeader) {
			file.Close()
			closeWriter()
			return fmt.Errorf("partition file %s header does not match the assembled CSV header", partPath)
		}

		for {
			record, readErr := reader.Read()
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				file.Close()
				closeWriter()
				return readErr
			}
			if len(record) == 0 {
				continue
			}
			if timestampIndex >= len(record) {
				file.Close()
				closeWriter()
				return fmt.Errorf("partition file %s contains a malformed row", partPath)
			}

			timestamp, err := time.Parse(timestampLayout, record[timestampIndex])
			if err != nil {
				file.Close()
				closeWriter()
				return fmt.Errorf("parse partition timestamp %q: %w", record[timestampIndex], err)
			}
			timestamp = timestamp.UTC()
			if timestamp.Before(from) || !timestamp.Before(to) {
				continue
			}

			currentTimestamp := timestamp.Format(timestampLayout)
			if currentTimestamp == lastTimestamp {
				if !recordsEqual(record, lastRecord) {
					file.Close()
					closeWriter()
					return fmt.Errorf("conflicting duplicate timestamp %s while assembling %s", currentTimestamp, outputPath)
				}
				continue
			}

			if err := csvWriter.Write(record); err != nil {
				file.Close()
				closeWriter()
				return err
			}
			lastTimestamp = currentTimestamp
			lastRecord = cloneColumns(record)
		}

		file.Close()
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
	if isParquetPath(sourcePath) {
		return extractRangeFromParquet(sourcePath, outputPath, from, to)
	}
	if isParquetPath(outputPath) {
		return extractRangeCSVToParquet(sourcePath, outputPath, from, to)
	}
	_, csvReader, closeReader, err := openCSVReader(sourcePath)
	if err != nil {
		return err
	}
	defer closeReader()

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

	_, csvWriter, closeWriter, err := createCSVWriter(tempPath)
	if err != nil {
		return err
	}

	if !HideCSVHeader {
		if err := csvWriter.Write(header); err != nil {
			closeWriter()
			return err
		}
	}

	for {
		record, readErr := csvReader.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
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

		timestamp, err := time.Parse(timestampLayout, record[timestampIndex])
		if err != nil {
			closeWriter()
			return fmt.Errorf("parse source CSV timestamp %q: %w", record[timestampIndex], err)
		}
		timestamp = timestamp.UTC()
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

func StreamCSVRowsAfter(path string, w io.Writer, after time.Time, includeHeader bool) (int, time.Time, error) {
	if isParquetPath(path) {
		return 0, time.Time{}, fmt.Errorf("streaming parquet rows to writer is not supported")
	}

	_, csvReader, closeReader, err := openCSVReader(path)
	if err != nil {
		return 0, time.Time{}, err
	}
	defer closeReader()

	header, err := csvReader.Read()
	if err != nil {
		return 0, time.Time{}, err
	}

	timestampIndex := indexOfColumn(header, "timestamp")
	if timestampIndex < 0 {
		return 0, time.Time{}, fmt.Errorf("source CSV %s does not contain a timestamp column", path)
	}

	csvWriter := csvWriterFactory(w)
	defer csvWriter.Flush()

	if includeHeader && !HideCSVHeader {
		if err := csvWriter.Write(header); err != nil {
			return 0, time.Time{}, err
		}
	}

	rows := 0
	lastTimestamp := after
	for {
		record, readErr := csvReader.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return 0, time.Time{}, readErr
		}
		if len(record) == 0 {
			continue
		}
		if timestampIndex >= len(record) {
			return 0, time.Time{}, fmt.Errorf("source CSV %s contains a malformed row", path)
		}

		timestamp, err := time.Parse(timestampLayout, record[timestampIndex])
		if err != nil {
			return 0, time.Time{}, fmt.Errorf("parse source CSV timestamp %q: %w", record[timestampIndex], err)
		}
		timestamp = timestamp.UTC()
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

func CleanDuplicates(path string) (int, error) {
	if isParquetPath(path) {
		return cleanParquetDuplicates(path)
	}

	_, reader, closeReader, err := openCSVReader(path)
	if err != nil {
		return 0, err
	}

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
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			closeReader()
			return 0, readErr
		}
		if len(record) == 0 {
			continue
		}

		t, err := time.Parse(timestampLayout, record[timestampIndex])
		if err != nil {
			closeReader()
			return 0, fmt.Errorf("failed to parse timestamp %q: %w", record[timestampIndex], err)
		}
		t = t.UTC()

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

	_, csvWriter, closeWriter, err := createCSVWriter(tempPath)
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

func MergeResumeCSV(existingPath string, tempPath string, duplicateTail []string) (int, error) {
	_, reader, closeReader, err := openCSVReader(tempPath)
	if err != nil {
		return 0, err
	}
	defer closeReader()

	if _, err := reader.Read(); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, nil
		}
		return 0, err
	}

	_, writer, closeWriter, err := openAppendCSVWriter(existingPath)
	if err != nil {
		return 0, err
	}
	defer closeWriter()

	foundDuplicateTail := duplicateTail == nil
	foundAnyRows := false
	appended := 0

	for {
		record, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return 0, readErr
		}
		if len(record) == 0 {
			continue
		}

		foundAnyRows = true
		if !foundDuplicateTail {
			if recordsEqual(record, duplicateTail) {
				foundDuplicateTail = true
			}
			continue
		}

		if err := writer.Write(record); err != nil {
			return 0, err
		}
		appended++
	}

	if !foundAnyRows {
		return 0, nil
	}

	if !foundDuplicateTail {
		return 0, fmt.Errorf("existing CSV tail was not found in resumed data; aborting to avoid corrupting %s", existingPath)
	}

	return appended, writer.Error()
}

func InspectExistingCSV(outputPath string) (ResumeState, error) {
	_, reader, closeReader, err := openCSVReader(outputPath)
	if err != nil {
		return ResumeState{}, err
	}
	defer closeReader()

	header, err := reader.Read()
	if errors.Is(err, io.EOF) {
		return ResumeState{Exists: true}, nil
	}
	if err != nil {
		return ResumeState{}, err
	}

	state := ResumeState{
		Exists:  true,
		Columns: cloneColumns(header),
	}

	lastRecord := []string(nil)
	for {
		record, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
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

	lastTime, err := time.Parse(timestampLayout, lastRecord[timestampIndex])
	if err != nil {
		return ResumeState{}, fmt.Errorf("parse existing CSV timestamp %q: %w", lastRecord[timestampIndex], err)
	}

	state.HasRows = true
	state.LastRecord = lastRecord
	state.LastTime = lastTime.UTC()
	return state, nil
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
		if errors.Is(rErr, io.EOF) {
			break
		}
		if rErr != nil {
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

func ensureParentDir(outputPath string) error {
	dir := filepath.Dir(outputPath)
	if dir == "." || dir == "" {
		return nil
	}
	return os.MkdirAll(dir, 0o755)
}

func createAtomicTempPath(outputPath string) (string, error) {
	if err := ensureParentDir(outputPath); err != nil {
		return "", err
	}

	pattern := filepath.Base(outputPath) + ".tmp-*"
	if isParquetPath(outputPath) {
		base := filepath.Base(strings.TrimSuffix(outputPath, ".parquet"))
		pattern = base + ".tmp-*.parquet"
	} else if isGzipPath(outputPath) {
		base := filepath.Base(strings.TrimSuffix(outputPath, ".gz"))
		pattern = base + ".tmp-*.gz"
	}

	file, err := os.CreateTemp(filepath.Dir(outputPath), pattern)
	if err != nil {
		return "", err
	}
	path := file.Name()
	if err := file.Close(); err != nil {
		return "", err
	}
	return path, nil
}

func replaceFile(sourcePath string, targetPath string) error {
	if err := ensureParentDir(targetPath); err != nil {
		return err
	}
	if _, err := os.Stat(targetPath); err == nil {
		if err := os.Remove(targetPath); err != nil {
			return err
		}
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	return os.Rename(sourcePath, targetPath)
}

func createCSVWriter(outputPath string) (*os.File, csvRecordWriter, func() error, error) {
	file, err := os.Create(outputPath)
	if err != nil {
		return nil, nil, nil, err
	}

	bufWriter := bufio.NewWriterSize(file, 64*1024)

	if isGzipPath(outputPath) {
		gzipWriter := gzip.NewWriter(bufWriter)
		writer := csvWriterFactory(gzipWriter)
		closeWriter := func() error {
			writer.Flush()
			if err := writer.Error(); err != nil {
				gzipWriter.Close()
				bufWriter.Flush()
				file.Close()
				return err
			}
			if err := gzipWriter.Close(); err != nil {
				bufWriter.Flush()
				file.Close()
				return err
			}
			if err := bufWriter.Flush(); err != nil {
				file.Close()
				return err
			}
			return file.Close()
		}
		return file, writer, closeWriter, nil
	}

	writer := csvWriterFactory(bufWriter)
	closeWriter := func() error {
		writer.Flush()
		if err := writer.Error(); err != nil {
			bufWriter.Flush()
			file.Close()
			return err
		}
		if err := bufWriter.Flush(); err != nil {
			file.Close()
			return err
		}
		return file.Close()
	}
	return file, writer, closeWriter, nil
}

func openCSVReader(path string) (*os.File, csvRecordReader, func() error, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, nil, err
	}

	bufReader := bufio.NewReaderSize(file, 64*1024)

	if isGzipPath(path) {
		gzipReader, err := gzip.NewReader(bufReader)
		if err != nil {
			file.Close()
			return nil, nil, nil, err
		}
		closeReader := func() error {
			if err := gzipReader.Close(); err != nil {
				file.Close()
				return err
			}
			return file.Close()
		}
		return file, csvReaderFactory(gzipReader), closeReader, nil
	}

	closeReader := func() error {
		return file.Close()
	}
	return file, csvReaderFactory(bufReader), closeReader, nil
}

func openAppendCSVWriter(path string) (*os.File, csvRecordWriter, func() error, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		return nil, nil, nil, err
	}

	bufWriter := bufio.NewWriterSize(file, 64*1024)

	if isGzipPath(path) {
		gzipWriter := gzip.NewWriter(bufWriter)
		writer := csvWriterFactory(gzipWriter)
		closeWriter := func() error {
			writer.Flush()
			if err := writer.Error(); err != nil {
				gzipWriter.Close()
				bufWriter.Flush()
				file.Close()
				return err
			}
			if err := gzipWriter.Close(); err != nil {
				bufWriter.Flush()
				file.Close()
				return err
			}
			if err := bufWriter.Flush(); err != nil {
				file.Close()
				return err
			}
			return file.Close()
		}
		return file, writer, closeWriter, nil
	}

	writer := csvWriterFactory(bufWriter)
	closeWriter := func() error {
		writer.Flush()
		if err := writer.Error(); err != nil {
			bufWriter.Flush()
			file.Close()
			return err
		}
		if err := bufWriter.Flush(); err != nil {
			file.Close()
			return err
		}
		return file.Close()
	}
	return file, writer, closeWriter, nil
}

func isGzipPath(path string) bool {
	return strings.HasSuffix(strings.ToLower(strings.TrimSpace(path)), ".gz")
}
