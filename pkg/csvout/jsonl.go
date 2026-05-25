package csvout

import (
	"encoding/json"
	"io"
	"os"
	"strings"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

func writeTicksJSONL(outputPath string, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	if err := ensureParentDir(outputPath); err != nil {
		return err
	}
	file, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer file.Close()
	return WriteTicksJSONLToWriter(file, instrument, columns, ticks)
}

func WriteTicksJSONLToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	encoder := json.NewEncoder(w)
	for _, tick := range ticks {
		record := make(map[string]any, len(columns))
		for _, column := range columns {
			valStr, err := formatTickColumn(column, instrument.PriceScale, tick)
			if err != nil {
				return err
			}
			val, _ := parquetValueForColumn(column, valStr) // Reuse conversion logic
			record[column] = val
		}
		if err := encoder.Encode(record); err != nil {
			return err
		}
	}
	return nil
}

func writeBarsJSONL(outputPath string, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	if err := ensureParentDir(outputPath); err != nil {
		return err
	}
	file, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer file.Close()
	return WriteBarsJSONLToWriter(file, instrument, columns, primaryBars, bidBars, askBars)
}

func WriteBarsJSONLToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	encoder := json.NewEncoder(w)
	if BarColumnsNeedBidAsk(columns) {
		rows, err := combineBarRows(bidBars, askBars)
		if err != nil {
			return err
		}
		for _, row := range rows {
			record := make(map[string]any, len(columns))
			for _, column := range columns {
				valStr, err := formatBarColumn(column, instrument.PriceScale, row.Bid, row.Ask)
				if err != nil {
					return err
				}
				val, _ := parquetValueForColumn(column, valStr)
				record[column] = val
			}
			if err := encoder.Encode(record); err != nil {
				return err
			}
		}
		return nil
	}

	for _, bar := range primaryBars {
		record := make(map[string]any, len(columns))
		for _, column := range columns {
			valStr, err := formatPrimaryBarColumn(column, instrument.PriceScale, bar)
			if err != nil {
				return err
			}
			val, _ := parquetValueForColumn(column, valStr)
			record[column] = val
		}
		if err := encoder.Encode(record); err != nil {
			return err
		}
	}
	return nil
}

func IsJSONLPath(path string) bool {
	lower := strings.ToLower(strings.TrimSpace(path))
	return strings.HasSuffix(lower, ".jsonl") || strings.HasSuffix(lower, ".json")
}
