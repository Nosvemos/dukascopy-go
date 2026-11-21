package csvout

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	"github.com/apache/arrow/go/v18/arrow/memory"
)

func isArrowPath(path string) bool {
	p := strings.ToLower(strings.TrimSpace(path))
	return strings.HasSuffix(p, ".arrow") || strings.HasSuffix(p, ".ipc") || strings.HasSuffix(p, ".feather")
}

func arrowSchemaForColumns(columns []string) *arrow.Schema {
	fields := make([]arrow.Field, len(columns))
	for i, col := range columns {
		if strings.EqualFold(strings.TrimSpace(col), "timestamp") {
			fields[i] = arrow.Field{Name: col, Type: arrow.BinaryTypes.String}
		} else if strings.EqualFold(strings.TrimSpace(col), "tick_count") {
			fields[i] = arrow.Field{Name: col, Type: arrow.PrimitiveTypes.Int64}
		} else if isFeatureColumn(col) {
			// Feature values can be missing (NaN/Inf warm-up rows) and are
			// stored as nulls.
			fields[i] = arrow.Field{Name: col, Type: arrow.PrimitiveTypes.Float64, Nullable: true}
		} else {
			fields[i] = arrow.Field{Name: col, Type: arrow.PrimitiveTypes.Float64}
		}
	}
	return arrow.NewSchema(fields, nil)
}

func (c *Config) writeBarsArrow(outputPath string, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	records, err := c.buildBarParquetRecords(instrument, columns, primaryBars, bidBars, askBars)
	if err != nil {
		return err
	}
	return c.writeArrowRecords(outputPath, columns, records)
}

func (c *Config) writeTicksArrow(outputPath string, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	records, err := c.buildTickParquetRecords(instrument, columns, ticks)
	if err != nil {
		return err
	}
	return c.writeArrowRecords(outputPath, columns, records)
}

func (c *Config) writeArrowRecords(outputPath string, columns []string, records []map[string]any) error {
	if err := ensureParentDir(outputPath); err != nil {
		return err
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	return errors.Join(writeArrowRecordsTo(file, columns, records), file.Close())
}

// writeArrowRecordsTo writes records as an Arrow IPC file to w. The IPC footer
// is written by the final Close, whose error is returned.
func writeArrowRecordsTo(w io.Writer, columns []string, records []map[string]any) error {
	schema := arrowSchemaForColumns(columns)
	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer builder.Release()

	writer, err := ipc.NewFileWriter(w, ipc.WithSchema(schema))
	if err != nil {
		return err
	}

	if len(records) > 0 {
		appendArrowRecords(builder, columns, records)
		rec := builder.NewRecord()
		writeErr := writer.Write(rec)
		rec.Release()
		if writeErr != nil {
			return errors.Join(writeErr, writer.Close())
		}
	}
	return writer.Close()
}

// appendArrowRecords appends records to builder in column order. Missing or
// unconvertible values, and NaN/±Inf feature values, become nulls.
func appendArrowRecords(builder *array.RecordBuilder, columns []string, records []map[string]any) {
	builder.Reserve(len(records))
	featureCols := make([]bool, len(columns))
	for i, col := range columns {
		featureCols[i] = isFeatureColumn(col)
	}
	appendFloat := func(b *array.Float64Builder, i int, value float64) {
		if featureCols[i] && !isFiniteFloat(value) {
			b.AppendNull()
			return
		}
		b.Append(value)
	}
	for _, record := range records {
		for i, col := range columns {
			val := record[col]
			fieldBuilder := builder.Field(i)
			if val == nil {
				fieldBuilder.AppendNull()
				continue
			}
			switch b := fieldBuilder.(type) {
			case *array.StringBuilder:
				if strVal, ok := val.(string); ok {
					b.Append(strVal)
				} else {
					b.Append(fmt.Sprintf("%v", val))
				}
			case *array.Float64Builder:
				if floatVal, ok := val.(float64); ok {
					appendFloat(b, i, floatVal)
				} else if intVal, ok := val.(int); ok {
					b.Append(float64(intVal))
				} else if strVal, ok := val.(string); ok {
					if f, err := strconv.ParseFloat(strVal, 64); err == nil {
						appendFloat(b, i, f)
					} else {
						b.AppendNull()
					}
				} else {
					b.AppendNull()
				}
			case *array.Int64Builder:
				if intVal, ok := val.(int64); ok {
					b.Append(intVal)
				} else if strVal, ok := val.(string); ok {
					if number, err := strconv.ParseInt(strVal, 10, 64); err == nil {
						b.Append(number)
					} else {
						b.AppendNull()
					}
				} else {
					b.AppendNull()
				}
			}
		}
	}
}

// ArrowStreamWriter writes record batches incrementally to an Arrow IPC file.
type ArrowStreamWriter struct {
	file    *os.File
	writer  *ipc.FileWriter
	schema  *arrow.Schema
	builder *array.RecordBuilder
	columns []string
}

// CreateArrowStreamWriter creates outputPath and returns a writer for columns.
func (c *Config) CreateArrowStreamWriter(outputPath string, columns []string) (*ArrowStreamWriter, error) {
	if err := ensureParentDir(outputPath); err != nil {
		return nil, err
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return nil, err
	}

	schema := arrowSchemaForColumns(columns)
	allocator := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(allocator, schema)

	writer, err := ipc.NewFileWriter(file, ipc.WithSchema(schema))
	if err != nil {
		builder.Release()
		file.Close()
		return nil, err
	}

	return &ArrowStreamWriter{
		file:    file,
		writer:  writer,
		schema:  schema,
		builder: builder,
		columns: columns,
	}, nil
}

// CreateArrowStreamWriter is (*Config).CreateArrowStreamWriter on DefaultConfig.
func CreateArrowStreamWriter(outputPath string, columns []string) (*ArrowStreamWriter, error) {
	return DefaultConfig().CreateArrowStreamWriter(outputPath, columns)
}

// WriteBatch appends records as one Arrow record batch.
func (w *ArrowStreamWriter) WriteBatch(records []map[string]any) error {
	if len(records) == 0 {
		return nil
	}

	appendArrowRecords(w.builder, w.columns, records)
	rec := w.builder.NewRecord()
	defer rec.Release()

	return w.writer.Write(rec)
}

// Close writes the IPC footer and closes the file. It reports every failure.
func (w *ArrowStreamWriter) Close() error {
	var errs []error
	if w.builder != nil {
		w.builder.Release()
		w.builder = nil
	}
	if w.writer != nil {
		if err := w.writer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("arrow stream writer close failed: %w", err))
		}
		w.writer = nil
	}
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			errs = append(errs, fmt.Errorf("arrow stream writer close failed: %w", err))
		}
		w.file = nil
	}
	return errors.Join(errs...)
}
