package csvout

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"strings"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

// jsonlRowWriter emits one JSON object per line with keys in column order.
// Value types match the historical map-based encoding: timestamp as a string,
// tick_count as an integer, every other column as a number, and null for a
// value that could not be converted or is NaN/Inf.
type jsonlRowWriter struct {
	w   *bufio.Writer
	buf bytes.Buffer
}

func newJSONLRowWriter(w io.Writer) *jsonlRowWriter {
	return &jsonlRowWriter{w: bufio.NewWriterSize(w, 64*1024)}
}

func (j *jsonlRowWriter) writeRow(keys []string, values []any) error {
	j.buf.Reset()
	j.buf.WriteByte('{')
	for i, key := range keys {
		if i > 0 {
			j.buf.WriteByte(',')
		}
		encodedKey, err := json.Marshal(key)
		if err != nil {
			return err
		}
		j.buf.Write(encodedKey)
		j.buf.WriteByte(':')
		value := values[i]
		if f, ok := value.(float64); ok && (math.IsNaN(f) || math.IsInf(f, 0)) {
			// JSON has no NaN/Inf; e.g. indicator warm-up rows become null.
			value = nil
		}
		encodedValue, err := json.Marshal(value)
		if err != nil {
			return err
		}
		j.buf.Write(encodedValue)
	}
	j.buf.WriteString("}\n")
	_, err := j.w.Write(j.buf.Bytes())
	return err
}

func (j *jsonlRowWriter) flush() error {
	return j.w.Flush()
}

// jsonlValue converts a formatted cell to its JSON value. Conversion errors
// yield null, as the map-based encoder did.
func jsonlValue(column string, formatted string) any {
	value, err := parquetValueForColumn(column, formatted)
	if err != nil {
		return nil
	}
	return value
}

// isGzipJSONLPath reports whether path is a gzip-compressed JSON Lines path.
func isGzipJSONLPath(path string) bool {
	lower := strings.ToLower(strings.TrimSpace(path))
	return strings.HasSuffix(lower, ".jsonl.gz") || strings.HasSuffix(lower, ".json.gz")
}

// createJSONLFile creates outputPath and returns a writer (gzip-wrapped for
// .jsonl.gz / .json.gz) and a close function that reports every failure.
func createJSONLFile(outputPath string) (io.Writer, func() error, error) {
	if err := ensureParentDir(outputPath); err != nil {
		return nil, nil, err
	}
	file, err := os.Create(outputPath)
	if err != nil {
		return nil, nil, err
	}
	if isGzipJSONLPath(outputPath) {
		gzipWriter := gzip.NewWriter(file)
		return gzipWriter, func() error {
			return errors.Join(gzipWriter.Close(), file.Close())
		}, nil
	}
	return file, file.Close, nil
}

func (c *Config) writeJSONLFile(outputPath string, write func(io.Writer) error) (retErr error) {
	w, closeFile, err := createJSONLFile(outputPath)
	if err != nil {
		return err
	}
	defer func() {
		if err := closeFile(); err != nil {
			retErr = errors.Join(retErr, err)
		}
	}()
	return write(w)
}

func (c *Config) writeTicksJSONL(outputPath string, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	return c.writeJSONLFile(outputPath, func(w io.Writer) error {
		return c.WriteTicksJSONLToWriter(w, instrument, columns, ticks)
	})
}

func writeTicksJSONL(outputPath string, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	return DefaultConfig().writeTicksJSONL(outputPath, instrument, columns, ticks)
}

// WriteTicksJSONLToWriter writes ticks as JSON Lines to w, one object per tick
// with keys in column order, using c's location and timestamp format.
func (c *Config) WriteTicksJSONLToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	out := newJSONLRowWriter(w)
	values := make([]any, len(columns))
	for _, tick := range ticks {
		for i, column := range columns {
			valStr, err := c.formatTickColumn(column, instrument.PriceScale, tick)
			if err != nil {
				return err
			}
			values[i] = jsonlValue(column, valStr)
		}
		if err := out.writeRow(columns, values); err != nil {
			return err
		}
	}
	return out.flush()
}

// WriteTicksJSONLToWriter writes ticks as JSON Lines using DefaultConfig.
func WriteTicksJSONLToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	return DefaultConfig().WriteTicksJSONLToWriter(w, instrument, columns, ticks)
}

func (c *Config) writeBarsJSONL(outputPath string, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	return c.writeJSONLFile(outputPath, func(w io.Writer) error {
		return c.WriteBarsJSONLToWriter(w, instrument, columns, primaryBars, bidBars, askBars)
	})
}

func writeBarsJSONL(outputPath string, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	return DefaultConfig().writeBarsJSONL(outputPath, instrument, columns, primaryBars, bidBars, askBars)
}

// WriteBarsJSONLToWriter writes bars as JSON Lines to w, one object per bar
// with keys in column order, using c's location and timestamp format. Columns
// that need bid/ask data are built from bidBars/askBars, otherwise primaryBars
// is used.
func (c *Config) WriteBarsJSONLToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	out := newJSONLRowWriter(w)
	values := make([]any, len(columns))
	if BarColumnsNeedBidAsk(columns) {
		rows, err := combineBarRows(bidBars, askBars)
		if err != nil {
			return err
		}
		for _, row := range rows {
			for i, column := range columns {
				valStr, err := c.formatBarColumn(column, instrument.PriceScale, row.Bid, row.Ask)
				if err != nil {
					return err
				}
				values[i] = jsonlValue(column, valStr)
			}
			if err := out.writeRow(columns, values); err != nil {
				return err
			}
		}
		return out.flush()
	}

	for _, bar := range primaryBars {
		for i, column := range columns {
			valStr, err := c.formatPrimaryBarColumn(column, instrument.PriceScale, bar)
			if err != nil {
				return err
			}
			values[i] = jsonlValue(column, valStr)
		}
		if err := out.writeRow(columns, values); err != nil {
			return err
		}
	}
	return out.flush()
}

// WriteBarsJSONLToWriter writes bars as JSON Lines using DefaultConfig.
func WriteBarsJSONLToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	return DefaultConfig().WriteBarsJSONLToWriter(w, instrument, columns, primaryBars, bidBars, askBars)
}

// IsJSONLPath reports whether path names a JSON Lines output (.jsonl or .json,
// optionally gzip-compressed with a trailing .gz).
func IsJSONLPath(path string) bool {
	lower := strings.ToLower(strings.TrimSpace(path))
	lower = strings.TrimSuffix(lower, ".gz")
	return strings.HasSuffix(lower, ".jsonl") || strings.HasSuffix(lower, ".json")
}
