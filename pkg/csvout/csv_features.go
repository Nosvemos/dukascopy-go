package csvout

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

// WriteBarsWithFeatures writes bars and their computed feature columns to outputPath.
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
		if err := closeWriter(); err != nil && retErr == nil {
			retErr = err
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
		if i < len(featRows) {
			for _, featVal := range featRows[i] {
				record = append(record, strconv.FormatFloat(featVal, 'f', -1, 64))
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
	return replaceFile(tempPath, outputPath)
}

func (c *Config) buildBarFeatureParquetRecords(
	instrument dukascopy.Instrument,
	baseColumns []string,
	bars []dukascopy.Bar,
	featNames []string,
	featRows [][]float64,
) ([]map[string]any, error) {
	records := make([]map[string]any, 0, len(bars))
	for i, bar := range bars {
		record := make(map[string]any, len(baseColumns)+len(featNames))
		for _, col := range baseColumns {
			val, err := c.formatPrimaryBarColumn(col, instrument.PriceScale, bar)
			if err != nil {
				return nil, err
			}
			typed, err := parquetValueForColumn(col, val)
			if err != nil {
				return nil, err
			}
			record[col] = typed
		}
		if i < len(featRows) {
			for j, featName := range featNames {
				if j < len(featRows[i]) {
					record[featName] = featRows[i][j]
				}
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
) (retErr error) {
	if err := ensureParentDir(outputPath); err != nil {
		return err
	}
	file, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer func() {
		if err := file.Close(); err != nil && retErr == nil {
			retErr = err
		}
	}()

	encoder := json.NewEncoder(file)
	for i, bar := range bars {
		record := make(map[string]any, len(baseColumns)+len(featNames))
		for _, col := range baseColumns {
			val, err := c.formatPrimaryBarColumn(col, instrument.PriceScale, bar)
			if err != nil {
				return err
			}
			typed, err := parquetValueForColumn(col, val)
			if err != nil {
				return err
			}
			record[col] = typed
		}
		if i < len(featRows) {
			for j, featName := range featNames {
				if j < len(featRows[i]) {
					record[featName] = featRows[i][j]
				}
			}
		}
		if err := encoder.Encode(record); err != nil {
			return err
		}
	}
	return nil
}

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

// ReadBarsFromCSV parses a CSV file back into a slice of dukascopy.Bar.
func ReadBarsFromCSV(path string) ([]dukascopy.Bar, []string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	reader := csv.NewReader(bufio.NewReader(file))
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
		}
	}

	var bars []dukascopy.Bar
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, nil, err
		}

		var b dukascopy.Bar
		if timeIdx >= 0 && timeIdx < len(record) {
			if t, err := parseFlexibleTimestamp(record[timeIdx]); err == nil {
				b.Time = t
			}
		}
		if openIdx >= 0 && openIdx < len(record) {
			b.Open, _ = strconv.ParseFloat(record[openIdx], 64)
		}
		if highIdx >= 0 && highIdx < len(record) {
			b.High, _ = strconv.ParseFloat(record[highIdx], 64)
		}
		if lowIdx >= 0 && lowIdx < len(record) {
			b.Low, _ = strconv.ParseFloat(record[lowIdx], 64)
		}
		if closeIdx >= 0 && closeIdx < len(record) {
			b.Close, _ = strconv.ParseFloat(record[closeIdx], 64)
		}
		if volIdx >= 0 && volIdx < len(record) {
			b.Volume, _ = strconv.ParseFloat(record[volIdx], 64)
		}
		bars = append(bars, b)
	}

	return bars, header, nil
}
