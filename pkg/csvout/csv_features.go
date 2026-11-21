package csvout

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

// featureValue returns feature j of bar i, or false when the feature row is
// missing or shorter than the feature name list, or the value is NaN/±Inf
// (e.g. an indicator warm-up row). A false result is written as a missing
// value: an empty CSV cell, or null in Parquet, Arrow and JSONL.
func featureValue(featRows [][]float64, i int, j int) (float64, bool) {
	if i >= len(featRows) || j >= len(featRows[i]) {
		return 0, false
	}
	value := featRows[i][j]
	if !isFiniteFloat(value) {
		return 0, false
	}
	return value, true
}

// WriteBarsWithFeatures writes bars and their computed feature columns to
// outputPath. The format is chosen from the extension (CSV, CSV.GZ, Parquet,
// Arrow/Feather/IPC, JSONL). Every row has exactly one value per feature name:
// a missing or NaN/±Inf feature value is written as an empty CSV cell (null in
// the other formats) and extra values beyond len(featNames) are ignored.
func (c *Config) WriteBarsWithFeatures(
	outputPath string,
	instrument dukascopy.Instrument,
	baseColumns []string,
	bars []dukascopy.Bar,
	featNames []string,
	featRows [][]float64,
) (retErr error) {
	allColumns := make([]string, 0, len(baseColumns)+len(featNames))
	allColumns = append(allColumns, baseColumns...)
	allColumns = append(allColumns, featNames...)

	if isParquetPath(outputPath) {
		records, err := c.buildBarFeatureParquetRecords(instrument, baseColumns, bars, featNames, featRows)
		if err != nil {
			return err
		}
		return c.writeParquetRecords(outputPath, allColumns, records)
	}

	if isArrowPath(outputPath) {
		records, err := c.buildBarFeatureParquetRecords(instrument, baseColumns, bars, featNames, featRows)
		if err != nil {
			return err
		}
		return c.writeArrowRecords(outputPath, allColumns, records)
	}

	if IsJSONLPath(outputPath) {
		return c.writeBarFeaturesJSONL(outputPath, instrument, baseColumns, bars, featNames, featRows)
	}

	if err := ensureParentDir(outputPath); err != nil {
		return err
	}

	_, csvWriter, closeWriter, err := c.createCSVWriter(outputPath)
	if err != nil {
		return err
	}
	defer func() {
		if err := closeWriter(); err != nil {
			retErr = errors.Join(retErr, err)
		}
	}()

	if !c.HideHeader {
		if err := csvWriter.Write(allColumns); err != nil {
			return err
		}
	}

	for i, bar := range bars {
		record := make([]string, 0, len(allColumns))
		for _, col := range baseColumns {
			val, err := c.formatPrimaryBarColumn(col, instrument.PriceScale, bar)
			if err != nil {
				return err
			}
			record = append(record, val)
		}
		for j := range featNames {
			if featVal, ok := featureValue(featRows, i, j); ok {
				record = append(record, strconv.FormatFloat(featVal, 'f', -1, 64))
			} else {
				record = append(record, "")
			}
		}
		if err := csvWriter.Write(record); err != nil {
			return err
		}
	}

	return csvWriter.Error()
}

// WriteBarsWithFeaturesAtomic writes bars with features atomically via a temp file.
func (c *Config) WriteBarsWithFeaturesAtomic(
	outputPath string,
	instrument dukascopy.Instrument,
	baseColumns []string,
	bars []dukascopy.Bar,
	featNames []string,
	featRows [][]float64,
) error {
	tempPath, err := createAtomicTempPath(outputPath)
	if err != nil {
		return err
	}
	defer os.Remove(tempPath)

	if err := c.WriteBarsWithFeatures(tempPath, instrument, baseColumns, bars, featNames, featRows); err != nil {
		return err
	}
	return commitAtomic(tempPath, outputPath)
}

func (c *Config) barFeatureValues(
	instrument dukascopy.Instrument,
	baseColumns []string,
	bar dukascopy.Bar,
	i int,
	featNames []string,
	featRows [][]float64,
	values []any,
) error {
	for k, col := range baseColumns {
		val, err := c.formatPrimaryBarColumn(col, instrument.PriceScale, bar)
		if err != nil {
			return err
		}
		typed, err := parquetValueForColumn(col, val)
		if err != nil {
			return err
		}
		values[k] = typed
	}
	for j := range featNames {
		if featVal, ok := featureValue(featRows, i, j); ok {
			values[len(baseColumns)+j] = featVal
		} else {
			values[len(baseColumns)+j] = nil
		}
	}
	return nil
}

func (c *Config) buildBarFeatureParquetRecords(
	instrument dukascopy.Instrument,
	baseColumns []string,
	bars []dukascopy.Bar,
	featNames []string,
	featRows [][]float64,
) ([]map[string]any, error) {
	allColumns := append(cloneColumns(baseColumns), featNames...)
	values := make([]any, len(allColumns))
	records := make([]map[string]any, 0, len(bars))
	for i, bar := range bars {
		if err := c.barFeatureValues(instrument, baseColumns, bar, i, featNames, featRows, values); err != nil {
			return nil, err
		}
		record := make(map[string]any, len(allColumns))
		for k, col := range allColumns {
			if values[k] != nil {
				record[col] = values[k]
			}
		}
		records = append(records, record)
	}
	return records, nil
}

func (c *Config) writeBarFeaturesJSONL(
	outputPath string,
	instrument dukascopy.Instrument,
	baseColumns []string,
	bars []dukascopy.Bar,
	featNames []string,
	featRows [][]float64,
) error {
	return c.writeJSONLFile(outputPath, func(w io.Writer) error {
		allColumns := append(cloneColumns(baseColumns), featNames...)
		values := make([]any, len(allColumns))
		out := newJSONLRowWriter(w)
		for i, bar := range bars {
			if err := c.barFeatureValues(instrument, baseColumns, bar, i, featNames, featRows, values); err != nil {
				return err
			}
			if err := out.writeRow(allColumns, values); err != nil {
				return err
			}
		}
		return out.flush()
	})
}

// WriteBarsWithFeatures writes bars with features using DefaultConfig.
func WriteBarsWithFeatures(
	outputPath string,
	instrument dukascopy.Instrument,
	baseColumns []string,
	bars []dukascopy.Bar,
	featNames []string,
	featRows [][]float64,
) error {
	return DefaultConfig().WriteBarsWithFeatures(outputPath, instrument, baseColumns, bars, featNames, featRows)
}

// WriteBarsWithFeaturesAtomic writes bars with features atomically using DefaultConfig.
func WriteBarsWithFeaturesAtomic(
	outputPath string,
	instrument dukascopy.Instrument,
	baseColumns []string,
	bars []dukascopy.Bar,
	featNames []string,
	featRows [][]float64,
) error {
	return DefaultConfig().WriteBarsWithFeaturesAtomic(outputPath, instrument, baseColumns, bars, featNames, featRows)
}

// ReadBarsFromCSV parses a CSV file (optionally .gz) written with c's delimiter
// and timestamp settings back into bars. The first row must be a header.
// Prices come from the first of open/mid_*/bid_* columns present. Any cell
// that cannot be parsed is reported with its data row number (1-based, header
// excluded) and column name.
func (c *Config) ReadBarsFromCSV(path string) ([]dukascopy.Bar, []string, error) {
	_, readerFactory, closeReader, err := c.openCSVReader(path)
	if err != nil {
		return nil, nil, err
	}
	defer closeReader()

	reader := readerFactory()
	header, err := reader.Read()
	if err != nil {
		return nil, nil, err
	}

	timeIdx := -1
	openIdx := -1
	highIdx := -1
	lowIdx := -1
	closeIdx := -1
	volIdx := -1
	tickCountIdx := -1

	for i, col := range header {
		switch strings.ToLower(strings.TrimSpace(col)) {
		case "timestamp":
			timeIdx = i
		case "open", "mid_open", "bid_open":
			if openIdx == -1 {
				openIdx = i
			}
		case "high", "mid_high", "bid_high":
			if highIdx == -1 {
				highIdx = i
			}
		case "low", "mid_low", "bid_low":
			if lowIdx == -1 {
				lowIdx = i
			}
		case "close", "mid_close", "bid_close":
			if closeIdx == -1 {
				closeIdx = i
			}
		case "volume":
			if volIdx == -1 {
				volIdx = i
			}
		case "tick_count":
			if tickCountIdx == -1 {
				tickCountIdx = i
			}
		}
	}

	parseFloatCell := func(record []string, idx int, row int, target *float64) error {
		if idx < 0 {
			return nil
		}
		if idx >= len(record) {
			return fmt.Errorf("%s: data row %d is malformed: missing column %q", path, row, header[idx])
		}
		value, err := strconv.ParseFloat(strings.TrimSpace(record[idx]), 64)
		if err != nil {
			return fmt.Errorf("%s: data row %d column %q: %w", path, row, header[idx], err)
		}
		*target = value
		return nil
	}

	var bars []dukascopy.Bar
	for row := 1; ; row++ {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("%s: data row %d: %w", path, row, err)
		}
		if len(record) == 0 {
			continue
		}

		var b dukascopy.Bar
		if timeIdx >= 0 {
			if timeIdx >= len(record) {
				return nil, nil, fmt.Errorf("%s: data row %d is malformed: missing column %q", path, row, header[timeIdx])
			}
			t, err := c.ParseTimestamp(record[timeIdx])
			if err != nil {
				return nil, nil, fmt.Errorf("%s: data row %d column %q: %w", path, row, header[timeIdx], err)
			}
			b.Time = t
		}
		for _, cell := range []struct {
			idx    int
			target *float64
		}{
			{openIdx, &b.Open},
			{highIdx, &b.High},
			{lowIdx, &b.Low},
			{closeIdx, &b.Close},
			{volIdx, &b.Volume},
		} {
			if err := parseFloatCell(record, cell.idx, row, cell.target); err != nil {
				return nil, nil, err
			}
		}
		if tickCountIdx >= 0 {
			if tickCountIdx >= len(record) {
				return nil, nil, fmt.Errorf("%s: data row %d is malformed: missing column %q", path, row, header[tickCountIdx])
			}
			count, err := strconv.ParseInt(strings.TrimSpace(record[tickCountIdx]), 10, 64)
			if err != nil {
				return nil, nil, fmt.Errorf("%s: data row %d column %q: %w", path, row, header[tickCountIdx], err)
			}
			b.TickCount = count
		}
		bars = append(bars, b)
	}

	return bars, header, nil
}

// ReadBarsFromCSV parses a CSV file back into bars using DefaultConfig.
func ReadBarsFromCSV(path string) ([]dukascopy.Bar, []string, error) {
	return DefaultConfig().ReadBarsFromCSV(path)
}
