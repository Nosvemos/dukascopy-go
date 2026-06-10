package csvout

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

func (c *Config) WriteBars(outputPath string, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) (retErr error) {
	if isParquetPath(outputPath) {
		return c.writeBarsParquet(outputPath, instrument, columns, primaryBars, bidBars, askBars)
	}
	if isArrowPath(outputPath) {
		return c.writeBarsArrow(outputPath, instrument, columns, primaryBars, bidBars, askBars)
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
	defer func() {
		if err := closeWriter(); err != nil && retErr == nil {
			retErr = err
		}
	}()

	return c.writeBarsCSV(csvWriter, instrument, columns, primaryBars, bidBars, askBars)
}

func WriteBars(outputPath string, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	return DefaultConfig().WriteBars(outputPath, instrument, columns, primaryBars, bidBars, askBars)
}

func (c *Config) WriteBarsToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) (retErr error) {
	csvWriter := c.csvWriterFactory(w)
	defer func() {
		csvWriter.Flush()
		if err := csvWriter.Error(); err != nil && retErr == nil {
			retErr = err
		}
	}()

	return c.writeBarsCSV(csvWriter, instrument, columns, primaryBars, bidBars, askBars)
}

func WriteBarsToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	return DefaultConfig().WriteBarsToWriter(w, instrument, columns, primaryBars, bidBars, askBars)
}

func (c *Config) WriteBarsRowsToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) (retErr error) {
	csvWriter := c.csvWriterFactory(w)
	defer func() {
		csvWriter.Flush()
		if err := csvWriter.Error(); err != nil && retErr == nil {
			retErr = err
		}
	}()

	return c.writeBarsCSVRows(csvWriter, instrument, columns, primaryBars, bidBars, askBars, false)
}

func WriteBarsRowsToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	return DefaultConfig().WriteBarsRowsToWriter(w, instrument, columns, primaryBars, bidBars, askBars)
}

func (c *Config) WriteTicks(outputPath string, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) (retErr error) {
	if isParquetPath(outputPath) {
		return c.writeTicksParquet(outputPath, instrument, columns, ticks)
	}
	if isArrowPath(outputPath) {
		return c.writeTicksArrow(outputPath, instrument, columns, ticks)
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
	defer func() {
		if err := closeWriter(); err != nil && retErr == nil {
			retErr = err
		}
	}()

	return c.writeTicksCSV(csvWriter, instrument, columns, ticks)
}

func WriteTicks(outputPath string, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	return DefaultConfig().WriteTicks(outputPath, instrument, columns, ticks)
}

func (c *Config) WriteTicksToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) (retErr error) {
	csvWriter := c.csvWriterFactory(w)
	defer func() {
		csvWriter.Flush()
		if err := csvWriter.Error(); err != nil && retErr == nil {
			retErr = err
		}
	}()

	return c.writeTicksCSV(csvWriter, instrument, columns, ticks)
}

func WriteTicksToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	return DefaultConfig().WriteTicksToWriter(w, instrument, columns, ticks)
}

func (c *Config) WriteTicksRowsToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) (retErr error) {
	csvWriter := c.csvWriterFactory(w)
	defer func() {
		csvWriter.Flush()
		if err := csvWriter.Error(); err != nil && retErr == nil {
			retErr = err
		}
	}()

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

func (c *Config) MergeResumeCSV(existingPath string, tempPath string, duplicateTail []string) (int, error) {
	_, readerFactory, closeReader, err := c.openCSVReader(tempPath)
	if err != nil {
		return 0, err
	}
	defer closeReader()

	reader := readerFactory()
	if _, err := reader.Read(); err != nil {
		if os.IsNotExist(err) || err.Error() == "EOF" {
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
		if readErr != nil {
			if readErr.Error() == "EOF" {
				break
			}
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
