package csvout

import (
	"fmt"
	"os"
	"path/filepath"
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
	defer file.Close()

	schema := arrowSchemaForColumns(columns)
	allocator := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(allocator, schema)
	defer builder.Release()

	writer, err := ipc.NewFileWriter(file, ipc.WithSchema(schema))
	if err != nil {
		return err
	}
	defer writer.Close()

	if len(records) > 0 {
		builder.Reserve(len(records))
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
						b.Append(floatVal)
					} else if intVal, ok := val.(int); ok {
						b.Append(float64(intVal))
					} else if strVal, ok := val.(string); ok {
						if f, err := strconv.ParseFloat(strVal, 64); err == nil {
							b.Append(f)
						} else {
							b.AppendNull()
						}
					} else {
						b.AppendNull()
					}
				}
			}
		}
		rec := builder.NewRecord()
		defer rec.Release()
		if err := writer.Write(rec); err != nil {
			return err
		}
	}
	return nil
}

type ArrowStreamWriter struct {
	file    *os.File
	writer  *ipc.FileWriter
	schema  *arrow.Schema
	builder *array.RecordBuilder
	columns []string
}

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

func CreateArrowStreamWriter(outputPath string, columns []string) (*ArrowStreamWriter, error) {
	return DefaultConfig().CreateArrowStreamWriter(outputPath, columns)
}

func (w *ArrowStreamWriter) WriteBatch(records []map[string]any) error {
	if len(records) == 0 {
		return nil
	}

	w.builder.Reserve(len(records))
	for _, record := range records {
		for i, col := range w.columns {
			val := record[col]
			fieldBuilder := w.builder.Field(i)
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
					_ = filepath.Separator // reference filepath just in case to satisfy imports if needed
				}
			case *array.Float64Builder:
				if floatVal, ok := val.(float64); ok {
					b.Append(floatVal)
				} else if intVal, ok := val.(int); ok {
					b.Append(float64(intVal))
				} else if strVal, ok := val.(string); ok {
					if f, err := strconv.ParseFloat(strVal, 64); err == nil {
						b.Append(f)
					} else {
						b.AppendNull()
					}
				} else {
					b.AppendNull()
				}
			}
		}
	}

	rec := w.builder.NewRecord()
	defer rec.Release()

	return w.writer.Write(rec)
}

func (w *ArrowStreamWriter) Close() error {
	var errs []string
	if w.builder != nil {
		w.builder.Release()
		w.builder = nil
	}
	if w.writer != nil {
		if err := w.writer.Close(); err != nil {
			errs = append(errs, err.Error())
		}
		w.writer = nil
	}
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			errs = append(errs, err.Error())
		}
		w.file = nil
	}
	if len(errs) > 0 {
		return fmt.Errorf("arrow stream writer close failed: %s", strings.Join(errs, "; "))
	}
	return nil
}

func writeBarsArrow(outputPath string, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	return DefaultConfig().writeBarsArrow(outputPath, instrument, columns, primaryBars, bidBars, askBars)
}

func writeTicksArrow(outputPath string, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	return DefaultConfig().writeTicksArrow(outputPath, instrument, columns, ticks)
}
