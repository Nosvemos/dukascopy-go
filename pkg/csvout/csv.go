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

func (c *Config) WriteBars(outputPath string, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	if isParquetPath(outputPath) {
		return c.writeBarsParquet(outputPath, instrument, columns, primaryBars, bidBars, askBars)
	}
	if IsJSONLPath(outputPath) {
		return writeBarsJSONL(outputPath, instrument, columns, primaryBars, bidBars, askBars)
	}
	if err := ensureParentDir(outputPath); err != nil {
		return err
	}

	_, csvWriter, closeWriter, err := c.createCSVWriter(outputPath)
	if err != nil {
		return err
	}
	defer closeWriter()

	return c.writeBarsCSV(csvWriter, instrument, columns, primaryBars, bidBars, askBars)
}

func WriteBars(outputPath string, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	return DefaultConfig().WriteBars(outputPath, instrument, columns, primaryBars, bidBars, askBars)
}

func (c *Config) WriteBarsToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	csvWriter := c.csvWriterFactory(w)
	defer csvWriter.Flush()

	return c.writeBarsCSV(csvWriter, instrument, columns, primaryBars, bidBars, askBars)
}

func WriteBarsToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	return DefaultConfig().WriteBarsToWriter(w, instrument, columns, primaryBars, bidBars, askBars)
}

func (c *Config) WriteBarsRowsToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	csvWriter := c.csvWriterFactory(w)
	defer csvWriter.Flush()

	return c.writeBarsCSVRows(csvWriter, instrument, columns, primaryBars, bidBars, askBars, false)
}

func WriteBarsRowsToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	return DefaultConfig().WriteBarsRowsToWriter(w, instrument, columns, primaryBars, bidBars, askBars)
}

func (c *Config) WriteTicks(outputPath string, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	if isParquetPath(outputPath) {
		return c.writeTicksParquet(outputPath, instrument, columns, ticks)
	}
	if IsJSONLPath(outputPath) {
		return writeTicksJSONL(outputPath, instrument, columns, ticks)
	}
	if err := ensureParentDir(outputPath); err != nil {
		return err
	}

	_, csvWriter, closeWriter, err := c.createCSVWriter(outputPath)
	if err != nil {
		return err
	}
	defer closeWriter()

	return c.writeTicksCSV(csvWriter, instrument, columns, ticks)
}

func WriteTicks(outputPath string, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	return DefaultConfig().WriteTicks(outputPath, instrument, columns, ticks)
}

func (c *Config) WriteTicksToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	csvWriter := c.csvWriterFactory(w)
	defer csvWriter.Flush()

	return c.writeTicksCSV(csvWriter, instrument, columns, ticks)
}

func WriteTicksToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	return DefaultConfig().WriteTicksToWriter(w, instrument, columns, ticks)
}

func (c *Config) WriteTicksRowsToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	csvWriter := c.csvWriterFactory(w)
	defer csvWriter.Flush()

	return c.writeTicksCSVRows(csvWriter, instrument, columns, ticks, false)
}

func WriteTicksRowsToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	return DefaultConfig().WriteTicksRowsToWriter(w, instrument, columns, ticks)
}

func (c *Config) writeBarsCSV(csvWriter csvRecordWriter, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	return c.writeBarsCSVRows(csvWriter, instrument, columns, primaryBars, bidBars, askBars, true)
}

func (c *Config) writeBarsCSVRows(csvWriter csvRecordWriter, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar, includeHeader bool) error {
	if includeHeader && !c.HideHeader {
		if err := csvWriter.Write(columns); err != nil {
			return err
		}
	}

	var interval time.Duration
	if len(primaryBars) > 1 {
		var intervals []time.Duration
		for i := 1; i < len(primaryBars); i++ {
			d := primaryBars[i].Time.Sub(primaryBars[i-1].Time)
			if d > 0 {
				intervals = append(intervals, d)
			}
		}
		interval = inferExpectedInterval(intervals)
	} else if len(bidBars) > 1 {
		var intervals []time.Duration
		for i := 1; i < len(bidBars); i++ {
			d := bidBars[i].Time.Sub(bidBars[i-1].Time)
			if d > 0 {
				intervals = append(intervals, d)
			}
		}
		interval = inferExpectedInterval(intervals)
	}

	if BarColumnsNeedBidAsk(columns) {
		rows, err := combineBarRows(bidBars, askBars)
		if err != nil {
			return err
		}

		var prevTime time.Time
		var prevBidClose float64
		var prevAskClose float64

		for i, row := range rows {
			if i > 0 && interval > 0 && c.FillGaps == "forward" {
				expectedNext := prevTime.Add(interval)
				for expectedNext.Before(row.Time) {
					if IsExpectedGapForProfile(expectedNext.Add(-interval), expectedNext.Add(interval), interval, instrument.Code, "auto") {
						expectedNext = expectedNext.Add(interval)
						continue
					}

					syntheticBidBar := dukascopy.Bar{
						Time:   expectedNext,
						Open:   prevBidClose,
						High:   prevBidClose,
						Low:    prevBidClose,
						Close:  prevBidClose,
						Volume: 0,
					}
					syntheticAskBar := dukascopy.Bar{
						Time:   expectedNext,
						Open:   prevAskClose,
						High:   prevAskClose,
						Low:    prevAskClose,
						Close:  prevAskClose,
						Volume: 0,
					}

					record := make([]string, 0, len(columns))
					for _, column := range columns {
						value, valueErr := c.formatBarColumn(column, instrument.PriceScale, syntheticBidBar, syntheticAskBar)
						if valueErr != nil {
							return valueErr
						}
						record = append(record, value)
					}
					if err := csvWriter.Write(record); err != nil {
						return err
					}

					expectedNext = expectedNext.Add(interval)
				}
			}

			record := make([]string, 0, len(columns))
			for _, column := range columns {
				value, valueErr := c.formatBarColumn(column, instrument.PriceScale, row.Bid, row.Ask)
				if valueErr != nil {
					return valueErr
				}
				record = append(record, value)
			}
			if err := csvWriter.Write(record); err != nil {
				return err
			}

			prevTime = row.Time
			prevBidClose = row.Bid.Close
			prevAskClose = row.Ask.Close
		}

		return csvWriter.Error()
	}

	var prevTime time.Time
	var prevClose float64

	for i, bar := range primaryBars {
		if i > 0 && interval > 0 && c.FillGaps == "forward" {
			expectedNext := prevTime.Add(interval)
			for expectedNext.Before(bar.Time) {
				if IsExpectedGapForProfile(expectedNext.Add(-interval), expectedNext.Add(interval), interval, instrument.Code, "auto") {
					expectedNext = expectedNext.Add(interval)
					continue
				}

				syntheticBar := dukascopy.Bar{
					Time:   expectedNext,
					Open:   prevClose,
					High:   prevClose,
					Low:    prevClose,
					Close:  prevClose,
					Volume: 0,
				}

				record := make([]string, 0, len(columns))
				for _, column := range columns {
					value, err := c.formatPrimaryBarColumn(column, instrument.PriceScale, syntheticBar)
					if err != nil {
						return err
					}
					record = append(record, value)
				}
				if err := csvWriter.Write(record); err != nil {
					return err
				}

				expectedNext = expectedNext.Add(interval)
			}
		}

		record := make([]string, 0, len(columns))
		for _, column := range columns {
			value, err := c.formatPrimaryBarColumn(column, instrument.PriceScale, bar)
			if err != nil {
				return err
			}
			record = append(record, value)
		}
		if err := csvWriter.Write(record); err != nil {
			return err
		}

		prevTime = bar.Time
		prevClose = bar.Close
	}

	return csvWriter.Error()
}

func (c *Config) writeTicksCSV(csvWriter csvRecordWriter, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	return c.writeTicksCSVRows(csvWriter, instrument, columns, ticks, true)
}

func (c *Config) writeTicksCSVRows(csvWriter csvRecordWriter, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick, includeHeader bool) error {
	if includeHeader && !c.HideHeader {
		if err := csvWriter.Write(columns); err != nil {
			return err
		}
	}

	for _, tick := range ticks {
		record := make([]string, 0, len(columns))
		for _, column := range columns {
			value, valueErr := c.formatTickColumn(column, instrument.PriceScale, tick)
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

func (c *Config) WriteBarsAtomic(outputPath string, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	tempPath, err := createAtomicTempPath(outputPath)
	if err != nil {
		return err
	}
	defer os.Remove(tempPath)

	if err := c.WriteBars(tempPath, instrument, columns, primaryBars, bidBars, askBars); err != nil {
		return err
	}
	return replaceFile(tempPath, outputPath)
}

func WriteBarsAtomic(outputPath string, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	return DefaultConfig().WriteBarsAtomic(outputPath, instrument, columns, primaryBars, bidBars, askBars)
}

func (c *Config) WriteTicksAtomic(outputPath string, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	tempPath, err := createAtomicTempPath(outputPath)
	if err != nil {
		return err
	}
	defer os.Remove(tempPath)

	if err := c.WriteTicks(tempPath, instrument, columns, ticks); err != nil {
		return err
	}
	return replaceFile(tempPath, outputPath)
}

func WriteTicksAtomic(outputPath string, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	return DefaultConfig().WriteTicksAtomic(outputPath, instrument, columns, ticks)
}

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
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
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

			timestamp, err := time.Parse(timestampLayout, record[timestampIndex])
			if err != nil {
				closeReader()
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

func StreamCSVRowsAfter(path string, w io.Writer, after time.Time, includeHeader bool) (int, time.Time, error) {
	return DefaultConfig().StreamCSVRowsAfter(path, w, after, includeHeader)
}

func (c *Config) CleanDuplicates(path string) (int, error) {
	if isParquetPath(path) {
		return c.cleanParquetDuplicates(path)
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

func (c *Config) MergeResumeCSV(existingPath string, tempPath string, duplicateTail []string) (int, error) {
	_, readerFactory, closeReader, err := c.openCSVReader(tempPath)
	if err != nil {
		return 0, err
	}
	defer closeReader()

	reader := readerFactory()
	if _, err := reader.Read(); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, nil
		}
		return 0, err
	}

	_, writer, closeWriter, err := c.openAppendCSVWriter(existingPath)
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

func MergeResumeCSV(existingPath string, tempPath string, duplicateTail []string) (int, error) {
	return DefaultConfig().MergeResumeCSV(existingPath, tempPath, duplicateTail)
}

func (c *Config) InspectExistingCSV(outputPath string) (ResumeState, error) {
	_, readerFactory, closeReader, err := c.openCSVReader(outputPath)
	if err != nil {
		return ResumeState{}, err
	}
	defer closeReader()

	reader := readerFactory()
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

func (c *Config) createCSVWriter(outputPath string) (*os.File, csvRecordWriter, func() error, error) {
	file, err := os.Create(outputPath)
	if err != nil {
		return nil, nil, nil, err
	}

	bufWriter := bufio.NewWriterSize(file, 64*1024)

	if isGzipPath(outputPath) {
		gzipWriter := gzip.NewWriter(bufWriter)
		writer := c.csvWriterFactory(gzipWriter)
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

	writer := c.csvWriterFactory(bufWriter)
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

func (c *Config) openCSVReader(path string) (*os.File, csvReaderFactoryFunc, func() error, error) {
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
		return file, func() csvRecordReader { return c.csvReaderFactory(gzipReader) }, closeReader, nil
	}

	closeReader := func() error {
		return file.Close()
	}
	return file, func() csvRecordReader { return c.csvReaderFactory(bufReader) }, closeReader, nil
}

type csvReaderFactoryFunc func() csvRecordReader

func (c *Config) openAppendCSVWriter(path string) (*os.File, csvRecordWriter, func() error, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		return nil, nil, nil, err
	}

	bufWriter := bufio.NewWriterSize(file, 64*1024)

	if isGzipPath(path) {
		gzipWriter := gzip.NewWriter(bufWriter)
		writer := c.csvWriterFactory(gzipWriter)
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

	writer := c.csvWriterFactory(bufWriter)
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

func openCSVReader(path string) (*os.File, csvRecordReader, func() error, error) {
	file, readerFactory, closeReader, err := DefaultConfig().openCSVReader(path)
	if err != nil {
		return nil, nil, nil, err
	}
	return file, readerFactory(), closeReader, nil
}

func openAppendCSVWriter(path string) (*os.File, csvRecordWriter, func() error, error) {
	return DefaultConfig().openAppendCSVWriter(path)
}

func createCSVWriter(outputPath string) (*os.File, csvRecordWriter, func() error, error) {
	return DefaultConfig().createCSVWriter(outputPath)
}

func writeBarsCSVRows(csvWriter csvRecordWriter, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar, includeHeader bool) error {
	return DefaultConfig().writeBarsCSVRows(csvWriter, instrument, columns, primaryBars, bidBars, askBars, includeHeader)
}

func writeTicksCSVRows(csvWriter csvRecordWriter, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick, includeHeader bool) error {
	return DefaultConfig().writeTicksCSVRows(csvWriter, instrument, columns, ticks, includeHeader)
}

func writeBarsCSV(csvWriter csvRecordWriter, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	return DefaultConfig().writeBarsCSV(csvWriter, instrument, columns, primaryBars, bidBars, askBars)
}

func writeTicksCSV(csvWriter csvRecordWriter, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	return DefaultConfig().writeTicksCSV(csvWriter, instrument, columns, ticks)
}
