package csvout

import (
	"fmt"
	"os"
	"strings"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

func (c *Config) writeBarsParquet(outputPath string, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	records, err := c.buildBarParquetRecords(instrument, columns, primaryBars, bidBars, askBars)
	if err != nil {
		return err
	}
	return c.writeParquetRecords(outputPath, columns, records)
}

func (c *Config) writeTicksParquet(outputPath string, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	records, err := c.buildTickParquetRecords(instrument, columns, ticks)
	if err != nil {
		return err
	}
	return c.writeParquetRecords(outputPath, columns, records)
}

func (c *Config) buildBarParquetRecords(instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) ([]map[string]any, error) {
	if BarColumnsNeedBidAsk(columns) {
		rows, err := combineBarRows(bidBars, askBars)
		if err != nil {
			return nil, err
		}

		records := make([]map[string]any, 0, len(rows))
		for _, row := range rows {
			record := make(map[string]any, len(columns))
			for _, column := range columns {
				value, err := c.formatBarColumn(column, instrument.PriceScale, row.Bid, row.Ask)
				if err != nil {
					return nil, err
				}
				typed, err := parquetValueForColumn(column, value)
				if err != nil {
					return nil, err
				}
				record[column] = typed
			}
			records = append(records, record)
		}
		return records, nil
	}

	records := make([]map[string]any, 0, len(primaryBars))
	for _, bar := range primaryBars {
		record := make(map[string]any, len(columns))
		for _, column := range columns {
			value, err := c.formatPrimaryBarColumn(column, instrument.PriceScale, bar)
			if err != nil {
				return nil, err
			}
			typed, err := parquetValueForColumn(column, value)
			if err != nil {
				return nil, err
			}
			record[column] = typed
		}
		records = append(records, record)
	}
	return records, nil
}

func (c *Config) buildTickParquetRecords(instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) ([]map[string]any, error) {
	records := make([]map[string]any, 0, len(ticks))
	for _, tick := range ticks {
		record := make(map[string]any, len(columns))
		for _, column := range columns {
			value, err := c.formatTickColumn(column, instrument.PriceScale, tick)
			if err != nil {
				return nil, err
			}
			typed, err := parquetValueForColumn(column, value)
			if err != nil {
				return nil, err
			}
			record[column] = typed
		}
		records = append(records, record)
	}
	return records, nil
}

func (c *Config) writeParquetRecords(outputPath string, columns []string, records []map[string]any) error {
	if err := ensureParentDir(outputPath); err != nil {
		return err
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer file.Close()

	schema := parquetSchemaForColumns(columns)
	writer := c.parquetWriterFactory(file, schema)
	writer.SetKeyValueMetadata(parquetColumnsMetadataKey, strings.Join(columns, ","))
	if len(records) > 0 {
		if _, err := writer.Write(records); err != nil {
			return err
		}
	}
	if err := writer.Close(); err != nil {
		return err
	}
	return nil
}

func writeParquetRecords(outputPath string, columns []string, records []map[string]any) error {
	return DefaultConfig().writeParquetRecords(outputPath, columns, records)
}

type ParquetStreamWriter struct {
	file   *os.File
	writer parquetRecordWriter
}

func (c *Config) CreateParquetStreamWriter(outputPath string, columns []string) (*ParquetStreamWriter, error) {
	if err := ensureParentDir(outputPath); err != nil {
		return nil, err
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return nil, err
	}

	schema := parquetSchemaForColumns(columns)
	writer := c.parquetWriterFactory(file, schema)
	writer.SetKeyValueMetadata(parquetColumnsMetadataKey, strings.Join(columns, ","))

	return &ParquetStreamWriter{
		file:   file,
		writer: writer,
	}, nil
}

func CreateParquetStreamWriter(outputPath string, columns []string) (*ParquetStreamWriter, error) {
	return DefaultConfig().CreateParquetStreamWriter(outputPath, columns)
}

func (w *ParquetStreamWriter) WriteBatch(records []map[string]any) error {
	if len(records) == 0 {
		return nil
	}
	_, err := w.writer.Write(records)
	return err
}

func (w *ParquetStreamWriter) Close() error {
	var errs []string
	if err := w.writer.Close(); err != nil {
		errs = append(errs, err.Error())
	}
	if err := w.file.Close(); err != nil {
		errs = append(errs, err.Error())
	}
	if len(errs) > 0 {
		return fmt.Errorf("parquet stream writer close failed: %s", strings.Join(errs, "; "))
	}
	return nil
}

func buildBarParquetRecords(instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) ([]map[string]any, error) {
	return DefaultConfig().buildBarParquetRecords(instrument, columns, primaryBars, bidBars, askBars)
}

func buildTickParquetRecords(instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) ([]map[string]any, error) {
	return DefaultConfig().buildTickParquetRecords(instrument, columns, ticks)
}

func writeBarsParquet(outputPath string, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	return DefaultConfig().writeBarsParquet(outputPath, instrument, columns, primaryBars, bidBars, askBars)
}

func writeTicksParquet(outputPath string, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	return DefaultConfig().writeTicksParquet(outputPath, instrument, columns, ticks)
}
