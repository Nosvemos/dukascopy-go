package csvout

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	parquet "github.com/parquet-go/parquet-go"

	"github.com/Nosvemos/dukascopy-go/internal/dukascopy"
)

type stubCSVWriter struct {
	failWriteAt int
	writeErr    error
	errorErr    error
	writes      int
}

func (w *stubCSVWriter) Write(record []string) error {
	if w.failWriteAt >= 0 && w.writes == w.failWriteAt {
		w.writes++
		return w.writeErr
	}
	w.writes++
	return nil
}

func (w *stubCSVWriter) Flush() {}

func (w *stubCSVWriter) Error() error {
	return w.errorErr
}

type stubCSVReadResult struct {
	record []string
	err    error
}

type stubCSVReader struct {
	results []stubCSVReadResult
	index   int
}

func (r *stubCSVReader) Read() ([]string, error) {
	if r.index >= len(r.results) {
		return nil, io.EOF
	}
	result := r.results[r.index]
	r.index++
	return result.record, result.err
}

type stubParquetWriter struct {
	failWriteAt int
	writeErr    error
	closeErr    error
	writes      int
}

func (w *stubParquetWriter) SetKeyValueMetadata(key string, value string) {}

func (w *stubParquetWriter) Write(rows []map[string]any) (int, error) {
	if w.failWriteAt >= 0 && w.writes == w.failWriteAt {
		w.writes++
		return 0, w.writeErr
	}
	w.writes++
	return len(rows), nil
}

func (w *stubParquetWriter) Close() error {
	return w.closeErr
}

type stubParquetReadResult struct {
	rows []map[string]any
	err  error
}

type stubParquetReader struct {
	results []stubParquetReadResult
	index   int
}

func (r *stubParquetReader) Read(rows []map[string]any) (int, error) {
	if r.index >= len(r.results) {
		return 0, io.EOF
	}
	result := r.results[r.index]
	r.index++
	for index := range result.rows {
		rows[index] = result.rows[index]
	}
	return len(result.rows), result.err
}

func (r *stubParquetReader) Close() error {
	return nil
}

func withCSVFactories(writerFactory func(io.Writer) csvRecordWriter, readerFactory func(io.Reader) csvRecordReader) func() {
	previousWriterFactory := csvWriterFactory
	previousReaderFactory := csvReaderFactory
	if writerFactory != nil {
		csvWriterFactory = writerFactory
	}
	if readerFactory != nil {
		csvReaderFactory = readerFactory
	}
	return func() {
		csvWriterFactory = previousWriterFactory
		csvReaderFactory = previousReaderFactory
	}
}

func withParquetFactories(writerFactory func(*os.File, *parquet.Schema) parquetRecordWriter, readerFactory func(*os.File, *parquet.Schema) parquetRecordReader) func() {
	previousWriterFactory := parquetWriterFactory
	previousReaderFactory := parquetReaderFactory
	if writerFactory != nil {
		parquetWriterFactory = writerFactory
	}
	if readerFactory != nil {
		parquetReaderFactory = readerFactory
	}
	return func() {
		parquetWriterFactory = previousWriterFactory
		parquetReaderFactory = previousReaderFactory
	}
}

func TestCSVGapWriterBranches(t *testing.T) {
	if got := BarColumnsForProfile(ProfileFull); len(got) == 0 || got[0] != "timestamp" {
		t.Fatalf("unexpected full bar columns: %v", got)
	}
	if got := TickColumnsForProfile(ProfileSimple); len(got) == 0 || got[0] != "timestamp" {
		t.Fatalf("unexpected simple tick columns: %v", got)
	}

	dir := t.TempDir()
	parentFile := filepath.Join(dir, "parent")
	if err := os.WriteFile(parentFile, []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := WriteBars(filepath.Join(parentFile, "bars.csv"), dukascopy.Instrument{}, []string{"timestamp"}, nil, nil, nil); err == nil {
		t.Fatal("expected WriteBars createCSVWriter error")
	}
	if err := WriteTicks(filepath.Join(parentFile, "ticks.csv"), dukascopy.Instrument{}, []string{"timestamp"}, nil); err == nil {
		t.Fatal("expected WriteTicks createCSVWriter error")
	}
	if err := WriteBars(dir, dukascopy.Instrument{}, []string{"timestamp"}, nil, nil, nil); err == nil {
		t.Fatal("expected WriteBars os.Create error")
	}
	if err := WriteTicks(dir, dukascopy.Instrument{}, []string{"timestamp"}, nil); err == nil {
		t.Fatal("expected WriteTicks os.Create error")
	}

	bar := dukascopy.Bar{Time: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), Open: 1, High: 2, Low: 0.5, Close: 1.5, Volume: 3}
	bid := dukascopy.Bar{Time: bar.Time, Open: 1, High: 2, Low: 0.5, Close: 1.4, Volume: 3}
	ask := dukascopy.Bar{Time: bar.Time, Open: 1.2, High: 2.2, Low: 0.7, Close: 1.6, Volume: 3}
	tick := dukascopy.Tick{Time: bar.Time, Bid: 1.1, Ask: 1.2, BidVolume: 2, AskVolume: 3}

	t.Run("writeBarsCSV header and row failures", func(t *testing.T) {
		writer := &stubCSVWriter{failWriteAt: 0, writeErr: errors.New("header write")}
		if err := writeBarsCSV(writer, dukascopy.Instrument{PriceScale: 3}, []string{"timestamp"}, []dukascopy.Bar{bar}, nil, nil); err == nil {
			t.Fatal("expected header write error")
		}

		writer = &stubCSVWriter{failWriteAt: 1, writeErr: errors.New("row write")}
		if err := writeBarsCSV(writer, dukascopy.Instrument{PriceScale: 3}, []string{"timestamp", "open"}, []dukascopy.Bar{bar}, nil, nil); err == nil {
			t.Fatal("expected row write error")
		}

		writer = &stubCSVWriter{errorErr: errors.New("flush error")}
		if err := writeBarsCSV(writer, dukascopy.Instrument{PriceScale: 3}, []string{"timestamp", "open"}, []dukascopy.Bar{bar}, nil, nil); err == nil {
			t.Fatal("expected writer.Error failure")
		}
	})

	t.Run("writeBarsCSV value and combine failures", func(t *testing.T) {
		if err := writeBarsCSV(&stubCSVWriter{}, dukascopy.Instrument{PriceScale: 3}, []string{"wat"}, []dukascopy.Bar{bar}, nil, nil); err == nil {
			t.Fatal("expected simple bar format error")
		}
		if err := writeBarsCSV(&stubCSVWriter{}, dukascopy.Instrument{PriceScale: 3}, []string{"spread"}, nil, []dukascopy.Bar{bid}, nil); err == nil {
			t.Fatal("expected combineBarRows error")
		}
		writer := &stubCSVWriter{failWriteAt: 1, writeErr: errors.New("row write")}
		if err := writeBarsCSV(writer, dukascopy.Instrument{PriceScale: 3}, []string{"timestamp", "spread"}, nil, []dukascopy.Bar{bid}, []dukascopy.Bar{ask}); err == nil {
			t.Fatal("expected bid/ask row write error")
		}
		writer = &stubCSVWriter{errorErr: errors.New("flush error")}
		if err := writeBarsCSV(writer, dukascopy.Instrument{PriceScale: 3}, []string{"timestamp", "spread"}, nil, []dukascopy.Bar{bid}, []dukascopy.Bar{ask}); err == nil {
			t.Fatal("expected bid/ask writer.Error failure")
		}
	})

	t.Run("writeTicksCSV branches", func(t *testing.T) {
		writer := &stubCSVWriter{failWriteAt: 0, writeErr: errors.New("header write")}
		if err := writeTicksCSV(writer, dukascopy.Instrument{PriceScale: 3}, []string{"timestamp"}, []dukascopy.Tick{tick}); err == nil {
			t.Fatal("expected tick header write error")
		}
		if err := writeTicksCSV(&stubCSVWriter{}, dukascopy.Instrument{PriceScale: 3}, []string{"wat"}, []dukascopy.Tick{tick}); err == nil {
			t.Fatal("expected tick format error")
		}
		writer = &stubCSVWriter{failWriteAt: 1, writeErr: errors.New("row write")}
		if err := writeTicksCSV(writer, dukascopy.Instrument{PriceScale: 3}, []string{"timestamp", "bid"}, []dukascopy.Tick{tick}); err == nil {
			t.Fatal("expected tick row write error")
		}
		writer = &stubCSVWriter{errorErr: errors.New("flush error")}
		if err := writeTicksCSV(writer, dukascopy.Instrument{PriceScale: 3}, []string{"timestamp", "bid"}, []dukascopy.Tick{tick}); err == nil {
			t.Fatal("expected tick writer.Error failure")
		}
	})

	if err := WriteBarsAtomic(filepath.Join(dir, "bad-bars.csv"), dukascopy.Instrument{PriceScale: 3}, []string{"wat"}, []dukascopy.Bar{bar}, nil, nil); err == nil {
		t.Fatal("expected WriteBarsAtomic inner write error")
	}
	if err := WriteTicksAtomic(filepath.Join(dir, "bad-ticks.csv"), dukascopy.Instrument{PriceScale: 3}, []string{"wat"}, []dukascopy.Tick{tick}); err == nil {
		t.Fatal("expected WriteTicksAtomic inner write error")
	}

	if _, err := parseColumns(" , ", map[string]struct{}{"timestamp": {}}); err == nil {
		t.Fatal("expected parseColumns empty selection error")
	}
	if got := formatMidPrice(1.2345, -2); got == "" {
		t.Fatal("expected negative-scale formatMidPrice output")
	}
	if got := estimateMissingIntervals(time.Minute, 0); got != 0 {
		t.Fatalf("expected zero missing intervals when expected interval is invalid, got %d", got)
	}
}

func TestCSVGapAssemblyAndExtractBranches(t *testing.T) {
	dir := t.TempDir()
	partPath := filepath.Join(dir, "part.csv")
	validContent := "timestamp,open\n2024-01-01T00:00:00Z,1\n"
	if err := os.WriteFile(partPath, []byte(validContent), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	t.Run("assembly missing part and empty header branches", func(t *testing.T) {
		if err := AssembleCSVFromParts(filepath.Join(dir, "missing.csv"), []string{filepath.Join(dir, "missing-part.csv")}, time.Time{}, time.Now()); err == nil {
			t.Fatal("expected missing part open error")
		}

		emptyPart := filepath.Join(dir, "empty-part.csv")
		if err := os.WriteFile(emptyPart, nil, 0o644); err != nil {
			t.Fatalf("WriteFile returned error: %v", err)
		}
		if err := AssembleCSVFromParts(filepath.Join(dir, "assembled.csv"), []string{emptyPart, partPath}, time.Time{}, time.Now()); err != nil {
			t.Fatalf("expected EOF header part to be skipped, got %v", err)
		}
	})

	t.Run("assembly writer and reader failures", func(t *testing.T) {
		restore := withCSVFactories(
			func(io.Writer) csvRecordWriter { return &stubCSVWriter{failWriteAt: 0, writeErr: errors.New("header write")} },
			nil,
		)
		defer restore()
		if err := AssembleCSVFromParts(filepath.Join(dir, "header-write.csv"), []string{partPath}, time.Time{}, time.Now()); err == nil {
			t.Fatal("expected assembly header write error")
		}
	})

	t.Run("assembly row write and flush failures", func(t *testing.T) {
		restore := withCSVFactories(
			func(io.Writer) csvRecordWriter { return &stubCSVWriter{failWriteAt: 1, writeErr: errors.New("row write")} },
			nil,
		)
		defer restore()
		if err := AssembleCSVFromParts(filepath.Join(dir, "row-write.csv"), []string{partPath}, time.Time{}, time.Now()); err == nil {
			t.Fatal("expected assembly row write error")
		}

		restore = withCSVFactories(
			func(io.Writer) csvRecordWriter { return &stubCSVWriter{errorErr: errors.New("flush error")} },
			nil,
		)
		defer restore()
		if err := AssembleCSVFromParts(filepath.Join(dir, "flush-error.csv"), []string{partPath}, time.Time{}, time.Now()); err == nil {
			t.Fatal("expected assembly flush error")
		}
	})

	t.Run("assembly reader error branches", func(t *testing.T) {
		restore := withCSVFactories(nil, func(io.Reader) csvRecordReader {
			return &stubCSVReader{results: []stubCSVReadResult{
				{record: nil, err: errors.New("header read")},
			}}
		})
		defer restore()
		if err := AssembleCSVFromParts(filepath.Join(dir, "header-read.csv"), []string{partPath}, time.Time{}, time.Now()); err == nil {
			t.Fatal("expected assembly header read error")
		}

		restore = withCSVFactories(nil, func(io.Reader) csvRecordReader {
			return &stubCSVReader{results: []stubCSVReadResult{
				{record: []string{"timestamp", "open"}},
				{record: nil, err: errors.New("row read")},
			}}
		})
		defer restore()
		if err := AssembleCSVFromParts(filepath.Join(dir, "row-read.csv"), []string{partPath}, time.Time{}, time.Now()); err == nil {
			t.Fatal("expected assembly row read error")
		}
	})

	t.Run("extract branches", func(t *testing.T) {
		emptySource := filepath.Join(dir, "empty-source.csv")
		if err := os.WriteFile(emptySource, nil, 0o644); err != nil {
			t.Fatalf("WriteFile returned error: %v", err)
		}
		if err := ExtractCSVRange(emptySource, filepath.Join(dir, "out.csv"), time.Time{}, time.Now()); err == nil {
			t.Fatal("expected empty source header error")
		}

		restore := withCSVFactories(
			func(io.Writer) csvRecordWriter { return &stubCSVWriter{failWriteAt: 0, writeErr: errors.New("header write")} },
			nil,
		)
		defer restore()
		if err := ExtractCSVRange(partPath, filepath.Join(dir, "header-write-out.csv"), time.Time{}, time.Now()); err == nil {
			t.Fatal("expected extract header write error")
		}

		restore = withCSVFactories(
			func(io.Writer) csvRecordWriter { return &stubCSVWriter{failWriteAt: 1, writeErr: errors.New("row write")} },
			nil,
		)
		defer restore()
		if err := ExtractCSVRange(partPath, filepath.Join(dir, "row-write-out.csv"), time.Time{}, time.Now()); err == nil {
			t.Fatal("expected extract row write error")
		}

		restore = withCSVFactories(
			func(io.Writer) csvRecordWriter { return &stubCSVWriter{errorErr: errors.New("flush error")} },
			nil,
		)
		defer restore()
		if err := ExtractCSVRange(partPath, filepath.Join(dir, "flush-out.csv"), time.Time{}, time.Now()); err == nil {
			t.Fatal("expected extract flush error")
		}

		restore = withCSVFactories(nil, func(io.Reader) csvRecordReader {
			return &stubCSVReader{results: []stubCSVReadResult{
				{record: []string{"timestamp", "open"}},
				{record: nil, err: errors.New("row read")},
			}}
		})
		defer restore()
		if err := ExtractCSVRange(partPath, filepath.Join(dir, "row-read-out.csv"), time.Time{}, time.Now()); err == nil {
			t.Fatal("expected extract read error")
		}
	})
}

func TestCSVGapInspectAndResumeBranches(t *testing.T) {
	dir := t.TempDir()

	empty := filepath.Join(dir, "empty.csv")
	if err := os.WriteFile(empty, nil, 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	state, err := InspectExistingCSV(empty)
	if err != nil || !state.Exists {
		t.Fatalf("expected empty file to be resumable, got %+v %v", state, err)
	}

	restore := withCSVFactories(nil, func(io.Reader) csvRecordReader {
		return &stubCSVReader{results: []stubCSVReadResult{
			{record: []string{"timestamp", "open"}},
			{record: nil, err: errors.New("read fail")},
		}}
	})
	if _, err := InspectExistingCSV(filepath.Join(dir, "synthetic.csv")); err == nil {
		restore()
		t.Fatal("expected InspectExistingCSV read error")
	}
	restore()

	badLastRow := filepath.Join(dir, "bad-last-row.csv")
	if err := os.WriteFile(badLastRow, []byte("timestamp,open\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	restore = withCSVFactories(nil, func(io.Reader) csvRecordReader {
		return &stubCSVReader{results: []stubCSVReadResult{
			{record: []string{"open", "timestamp"}},
			{record: []string{}, err: nil},
			{record: []string{"1"}, err: nil},
			{record: nil, err: io.EOF},
		}}
	})
	if _, err := InspectExistingCSV(badLastRow); err == nil {
		restore()
		t.Fatal("expected malformed last row error")
	}
	restore()

	badTimestamp := filepath.Join(dir, "bad-ts.csv")
	if err := os.WriteFile(badTimestamp, []byte("timestamp,open\nbad,1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if _, err := InspectExistingCSV(badTimestamp); err == nil {
		t.Fatal("expected bad timestamp error")
	}

	tempPath := filepath.Join(dir, "temp.csv")
	if _, err := MergeResumeCSV(filepath.Join(dir, "missing-existing.csv"), tempPath, nil); err == nil {
		t.Fatal("expected missing temp error")
	}
	if err := os.WriteFile(tempPath, nil, 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	existingPath := filepath.Join(dir, "existing.csv")
	if err := os.WriteFile(existingPath, []byte("timestamp,open\n2024-01-01T00:00:00Z,1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if appended, err := MergeResumeCSV(existingPath, tempPath, nil); err != nil || appended != 0 {
		t.Fatalf("expected empty temp merge result, got %d %v", appended, err)
	}

	restore = withCSVFactories(func(io.Writer) csvRecordWriter {
		return &stubCSVWriter{failWriteAt: 0, writeErr: errors.New("append write")}
	}, nil)
	if err := os.WriteFile(tempPath, []byte("timestamp,open\n2024-01-01T00:00:01Z,2\n"), 0o644); err != nil {
		restore()
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if _, err := MergeResumeCSV(existingPath, tempPath, nil); err == nil {
		restore()
		t.Fatal("expected append write error")
	}
	restore()
}

func TestParquetGapBranches(t *testing.T) {
	dir := t.TempDir()
	bar := dukascopy.Bar{Time: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), Open: 1, High: 2, Low: 0.5, Close: 1.5, Volume: 3}
	bid := dukascopy.Bar{Time: bar.Time, Open: 1, High: 2, Low: 0.5, Close: 1.4, Volume: 3}
	ask := dukascopy.Bar{Time: bar.Time, Open: 1.2, High: 2.2, Low: 0.7, Close: 1.6, Volume: 3}
	tick := dukascopy.Tick{Time: bar.Time, Bid: 1.1, Ask: 1.2, BidVolume: 2, AskVolume: 3}

	if err := writeBarsParquet(filepath.Join(dir, "bars.parquet"), dukascopy.Instrument{PriceScale: 3}, []string{"wat"}, []dukascopy.Bar{bar}, nil, nil); err == nil {
		t.Fatal("expected writeBarsParquet build error")
	}
	if err := writeTicksParquet(filepath.Join(dir, "ticks.parquet"), dukascopy.Instrument{PriceScale: 3}, []string{"wat"}, []dukascopy.Tick{tick}); err == nil {
		t.Fatal("expected writeTicksParquet build error")
	}
	if _, err := buildBarParquetRecords(dukascopy.Instrument{PriceScale: 3}, []string{"wat"}, []dukascopy.Bar{bar}, nil, nil); err == nil {
		t.Fatal("expected buildBarParquetRecords simple error")
	}
	if _, err := buildBarParquetRecords(dukascopy.Instrument{PriceScale: 3}, []string{"spread", "wat"}, nil, []dukascopy.Bar{bid}, []dukascopy.Bar{ask}); err == nil {
		t.Fatal("expected buildBarParquetRecords bid/ask error")
	}
	if _, err := buildTickParquetRecords(dukascopy.Instrument{PriceScale: 3}, []string{"wat"}, []dukascopy.Tick{tick}); err == nil {
		t.Fatal("expected buildTickParquetRecords error")
	}

	restore := withParquetFactories(func(*os.File, *parquet.Schema) parquetRecordWriter {
		return &stubParquetWriter{failWriteAt: 0, writeErr: errors.New("parquet write")}
	}, nil)
	if err := writeParquetRecords(filepath.Join(dir, "fail-write.parquet"), []string{"timestamp"}, []map[string]any{{"timestamp": "2024-01-02T00:00:00Z"}}); err == nil {
		restore()
		t.Fatal("expected writeParquetRecords write error")
	}
	restore()

	restore = withParquetFactories(func(*os.File, *parquet.Schema) parquetRecordWriter {
		return &stubParquetWriter{closeErr: errors.New("parquet close")}
	}, nil)
	if err := writeParquetRecords(filepath.Join(dir, "fail-close.parquet"), []string{"timestamp"}, nil); err == nil {
		restore()
		t.Fatal("expected writeParquetRecords close error")
	}
	restore()

	validParquet := filepath.Join(dir, "valid.parquet")
	if err := writeParquetRecords(validParquet, []string{"timestamp", "open"}, []map[string]any{{"timestamp": "2024-01-02T00:00:00Z", "open": 1.0}}); err != nil {
		t.Fatalf("writeParquetRecords returned error: %v", err)
	}

	restore = withParquetFactories(nil, func(*os.File, *parquet.Schema) parquetRecordReader {
		return &stubParquetReader{results: []stubParquetReadResult{
			{rows: nil, err: errors.New("parquet read")},
		}}
	})
	if _, err := inspectParquet(validParquet); err == nil {
		restore()
		t.Fatal("expected inspectParquet read error")
	}
	restore()

	partCSV := filepath.Join(dir, "part.csv")
	if err := os.WriteFile(partCSV, []byte("timestamp,open\n2024-01-02T00:00:00Z,1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	restore = withParquetFactories(func(*os.File, *parquet.Schema) parquetRecordWriter {
		return &stubParquetWriter{failWriteAt: 0, writeErr: errors.New("parquet write")}
	}, nil)
	if err := assembleParquetFromCSVParts(filepath.Join(dir, "assembled.parquet"), []string{partCSV}, time.Time{}, time.Now()); err == nil {
		restore()
		t.Fatal("expected assembleParquetFromCSVParts write error")
	}
	restore()

	restore = withParquetFactories(func(*os.File, *parquet.Schema) parquetRecordWriter {
		return &stubParquetWriter{closeErr: errors.New("parquet close")}
	}, nil)
	if err := assembleParquetFromCSVParts(filepath.Join(dir, "assembled-close.parquet"), []string{partCSV}, time.Time{}, time.Now()); err == nil {
		restore()
		t.Fatal("expected assembleParquetFromCSVParts close error")
	}
	restore()

	restore = withCSVFactories(nil, func(io.Reader) csvRecordReader {
		return &stubCSVReader{results: []stubCSVReadResult{
			{record: []string{"timestamp", "open"}},
			{record: nil, err: errors.New("csv row read")},
		}}
	})
	if err := assembleParquetFromCSVParts(filepath.Join(dir, "assembled-read.parquet"), []string{partCSV}, time.Time{}, time.Now()); err == nil {
		restore()
		t.Fatal("expected assembleParquetFromCSVParts row read error")
	}
	restore()

	restore = withParquetFactories(nil, func(*os.File, *parquet.Schema) parquetRecordReader {
		return &stubParquetReader{results: []stubParquetReadResult{
			{rows: nil, err: errors.New("parquet read")},
		}}
	})
	if err := extractRangeFromParquet(validParquet, filepath.Join(dir, "out.csv"), time.Time{}, time.Now()); err == nil {
		restore()
		t.Fatal("expected extractRangeFromParquet read error")
	}
	restore()

	restore = withParquetFactories(func(*os.File, *parquet.Schema) parquetRecordWriter {
		return &stubParquetWriter{failWriteAt: 0, writeErr: errors.New("parquet write")}
	}, nil)
	if err := extractRangeFromParquet(validParquet, filepath.Join(dir, "out.parquet"), time.Time{}, time.Now()); err == nil {
		restore()
		t.Fatal("expected extractRangeFromParquet parquet write error")
	}
	restore()

	restore = withParquetFactories(func(*os.File, *parquet.Schema) parquetRecordWriter {
		return &stubParquetWriter{closeErr: errors.New("parquet close")}
	}, nil)
	if err := extractRangeCSVToParquet(partCSV, filepath.Join(dir, "csv-to-parquet.parquet"), time.Time{}, time.Now()); err == nil {
		restore()
		t.Fatal("expected extractRangeCSVToParquet close error")
	}
	restore()

	restore = withParquetFactories(func(*os.File, *parquet.Schema) parquetRecordWriter {
		return &stubParquetWriter{failWriteAt: 0, writeErr: errors.New("parquet write")}
	}, nil)
	if err := extractRangeCSVToParquet(partCSV, filepath.Join(dir, "csv-to-parquet-write.parquet"), time.Time{}, time.Now()); err == nil {
		restore()
		t.Fatal("expected extractRangeCSVToParquet write error")
	}
	restore()

	badValueCSV := filepath.Join(dir, "bad-value.csv")
	if err := os.WriteFile(badValueCSV, []byte("timestamp,open\n2024-01-02T00:00:00Z,wat\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := extractRangeCSVToParquet(badValueCSV, filepath.Join(dir, "bad-value.parquet"), time.Time{}, time.Now()); err == nil {
		t.Fatal("expected extractRangeCSVToParquet record conversion error")
	}
}

func TestCSVGapAdditionalBranches(t *testing.T) {
	dir := t.TempDir()
	partPath := filepath.Join(dir, "part.csv")
	if err := os.WriteFile(partPath, []byte("timestamp,open\n2024-01-01T00:00:00Z,1\n2024-01-01T00:02:00Z,2\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	if err := AssembleCSVFromParts(filepath.Join(dir, "none.csv"), nil, time.Time{}, time.Now()); err == nil {
		t.Fatal("expected no parts error")
	}
	if err := AssembleCSVFromParts(filepath.Join(dir, "wrapped.parquet"), []string{filepath.Join(dir, "missing.csv")}, time.Time{}, time.Now()); err == nil {
		t.Fatal("expected parquet wrapper error")
	}
	parentFile := filepath.Join(dir, "parent-file")
	if err := os.WriteFile(parentFile, []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := AssembleCSVFromParts(filepath.Join(parentFile, "out.csv"), []string{partPath}, time.Time{}, time.Now()); err == nil {
		t.Fatal("expected assembly temp path error")
	}

	noTimestamp := filepath.Join(dir, "no-timestamp.csv")
	if err := os.WriteFile(noTimestamp, []byte("open\n1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := AssembleCSVFromParts(filepath.Join(dir, "no-timestamp-out.csv"), []string{noTimestamp}, time.Time{}, time.Now()); err == nil {
		t.Fatal("expected missing timestamp error")
	}

	restore := withCSVFactories(nil, func(io.Reader) csvRecordReader {
		return &stubCSVReader{results: []stubCSVReadResult{
			{record: []string{"open", "timestamp"}},
			{record: []string{}, err: nil},
			{record: []string{"1"}, err: nil},
		}}
	})
	if err := AssembleCSVFromParts(filepath.Join(dir, "malformed.csv"), []string{partPath}, time.Time{}, time.Now()); err == nil {
		restore()
		t.Fatal("expected malformed assembly row error")
	}
	restore()

	duplicatePart := filepath.Join(dir, "duplicate.csv")
	if err := os.WriteFile(duplicatePart, []byte("timestamp,open\n2024-01-01T00:00:00Z,1\n2024-01-01T00:00:00Z,1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := AssembleCSVFromParts(filepath.Join(dir, "duplicate-out.csv"), []string{duplicatePart}, time.Time{}, time.Now()); err != nil {
		t.Fatalf("expected duplicate identical rows to be accepted, got %v", err)
	}
	if err := AssembleCSVFromParts(filepath.Join(dir, "filtered-out.csv"), []string{duplicatePart}, time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), time.Now()); err != nil {
		t.Fatalf("expected out-of-range rows to be skipped, got %v", err)
	}

	if err := ExtractCSVRange(filepath.Join(dir, "missing.csv"), filepath.Join(dir, "out.csv"), time.Time{}, time.Now()); err == nil {
		t.Fatal("expected missing source error")
	}
	if err := ExtractCSVRange(filepath.Join(dir, "missing.parquet"), filepath.Join(dir, "from-parquet.csv"), time.Time{}, time.Now()); err == nil {
		t.Fatal("expected parquet source wrapper error")
	}
	if err := ExtractCSVRange(partPath, filepath.Join(dir, "wrapped-out.parquet"), time.Time{}, time.Now()); err != nil {
		t.Fatalf("expected parquet output wrapper success, got %v", err)
	}
	if err := ExtractCSVRange(partPath, filepath.Join(parentFile, "extract.csv"), time.Time{}, time.Now()); err == nil {
		t.Fatal("expected extract temp path error")
	}

	restore = withCSVFactories(nil, func(io.Reader) csvRecordReader {
		return &stubCSVReader{results: []stubCSVReadResult{
			{record: []string{"open", "timestamp"}},
			{record: []string{}, err: nil},
			{record: []string{"1"}, err: nil},
		}}
	})
	if err := ExtractCSVRange(partPath, filepath.Join(dir, "extract-malformed.csv"), time.Time{}, time.Now()); err == nil {
		restore()
		t.Fatal("expected extract malformed row error")
	}
	restore()

	if _, err := AuditCSV(filepath.Join(dir, "missing.csv")); err == nil {
		t.Fatal("expected AuditCSV missing file error")
	}
	badGzip := filepath.Join(dir, "bad.csv.gz")
	if err := os.WriteFile(badGzip, []byte("not-gzip"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if _, err := AuditCSV(badGzip); err == nil {
		t.Fatal("expected AuditCSV gzip error")
	}
	goodGzip := filepath.Join(dir, "good.csv.gz")
	if err := WriteTicks(goodGzip, dukascopy.Instrument{PriceScale: 3}, []string{"timestamp", "bid"}, []dukascopy.Tick{{
		Time: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		Bid:  1.1,
	}}); err != nil {
		t.Fatalf("WriteTicks returned error: %v", err)
	}
	if audit, err := AuditCSV(goodGzip); err != nil || audit.Rows != 1 {
		t.Fatalf("expected gzip audit success, got %+v %v", audit, err)
	}
	restore = withCSVFactories(nil, func(io.Reader) csvRecordReader {
		return &stubCSVReader{results: []stubCSVReadResult{
			{record: []string{"timestamp", "open"}},
			{record: nil, err: errors.New("audit read")},
		}}
	})
	if _, err := AuditCSV(partPath); err == nil {
		restore()
		t.Fatal("expected AuditCSV read error")
	}
	restore()
	restore = withCSVFactories(nil, func(io.Reader) csvRecordReader {
		return &stubCSVReader{results: []stubCSVReadResult{
			{record: []string{"timestamp", "open"}},
			{record: []string{}, err: nil},
			{record: []string{"2024-01-01T00:00:00Z", "1"}, err: nil},
			{record: nil, err: io.EOF},
		}}
	})
	if audit, err := AuditCSV(partPath); err != nil || audit.Rows != 1 {
		restore()
		t.Fatalf("expected AuditCSV empty-row branch success, got %+v %v", audit, err)
	}
	restore()

	if _, err := InspectCSV(filepath.Join(dir, "missing.csv")); err == nil {
		t.Fatal("expected InspectCSV missing file error")
	}
	empty := filepath.Join(dir, "empty.csv")
	if err := os.WriteFile(empty, nil, 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if _, err := InspectCSV(empty); err == nil {
		t.Fatal("expected InspectCSV empty header error")
	}
	statsPath := filepath.Join(dir, "stats.csv")
	if err := os.WriteFile(statsPath, []byte("timestamp,open\n2024-01-01T00:00:00Z,1\n2024-01-01T00:00:00Z,1\n2024-01-01T00:01:00Z,2\n2024-01-01T00:04:00Z,3\n2023-12-31T23:59:00Z,4\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	stats, err := InspectCSV(statsPath)
	if err != nil {
		t.Fatalf("InspectCSV returned error: %v", err)
	}
	if stats.DuplicateRows == 0 || stats.DuplicateStamps == 0 || stats.OutOfOrderRows == 0 || stats.GapCount == 0 || stats.LargestGap == "" {
		t.Fatalf("expected duplicate/gap stats, got %+v", stats)
	}
	restore = withCSVFactories(nil, func(io.Reader) csvRecordReader {
		return &stubCSVReader{results: []stubCSVReadResult{
			{record: []string{"timestamp", "open"}},
			{record: nil, err: errors.New("inspect read")},
		}}
	})
	if _, err := InspectCSV(partPath); err == nil {
		restore()
		t.Fatal("expected InspectCSV read error")
	}
	restore()
	restore = withCSVFactories(nil, func(io.Reader) csvRecordReader {
		return &stubCSVReader{results: []stubCSVReadResult{
			{record: []string{"timestamp", "open"}},
			{record: []string{}, err: nil},
			{record: []string{"2024-01-01T00:00:00Z", "1"}, err: nil},
			{record: nil, err: io.EOF},
		}}
	})
	if stats, err := InspectCSV(partPath); err != nil || stats.Rows != 1 {
		restore()
		t.Fatalf("expected InspectCSV empty-row branch success, got %+v %v", stats, err)
	}
	restore()

	if HeadersMatch([]string{"timestamp"}, []string{"timestamp", "open"}) {
		t.Fatal("expected HeadersMatch length mismatch to be false")
	}

	restore = withCSVFactories(nil, func(io.Reader) csvRecordReader {
		return &stubCSVReader{results: []stubCSVReadResult{
			{record: nil, err: errors.New("header read")},
		}}
	})
	if _, err := InspectExistingCSV(partPath); err == nil {
		restore()
		t.Fatal("expected InspectExistingCSV header error")
	}
	restore()
	restore = withCSVFactories(nil, func(io.Reader) csvRecordReader {
		return &stubCSVReader{results: []stubCSVReadResult{
			{record: []string{"timestamp", "open"}},
			{record: nil, err: errors.New("existing read")},
		}}
	})
	if _, err := InspectExistingCSV(partPath); err == nil {
		restore()
		t.Fatal("expected InspectExistingCSV row error")
	}
	restore()

	tempPath := filepath.Join(dir, "resume.csv")
	if err := os.WriteFile(tempPath, []byte("timestamp,open\n2024-01-01T00:00:01Z,2\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if _, err := MergeResumeCSV(filepath.Join(dir, "missing-existing.csv"), partPath, nil); err == nil {
		t.Fatal("expected missing existing file error")
	}
	restore = withCSVFactories(nil, func(io.Reader) csvRecordReader {
		return &stubCSVReader{results: []stubCSVReadResult{
			{record: []string{"timestamp", "open"}},
			{record: nil, err: errors.New("resume read")},
		}}
	})
	if _, err := MergeResumeCSV(tempPath, partPath, nil); err == nil {
		restore()
		t.Fatal("expected resume row read error")
	}
	restore()
	restore = withCSVFactories(nil, func(io.Reader) csvRecordReader {
		return &stubCSVReader{results: []stubCSVReadResult{
			{record: nil, err: errors.New("resume header")},
		}}
	})
	if _, err := MergeResumeCSV(tempPath, partPath, nil); err == nil {
		restore()
		t.Fatal("expected resume header error")
	}
	restore()
	restore = withCSVFactories(nil, func(io.Reader) csvRecordReader {
		return &stubCSVReader{results: []stubCSVReadResult{
			{record: []string{"timestamp", "open"}},
			{record: []string{}, err: nil},
			{record: []string{"2024-01-01T00:00:01Z", "2"}, err: nil},
			{record: nil, err: io.EOF},
		}}
	})
	if appended, err := MergeResumeCSV(tempPath, partPath, nil); err != nil || appended != 1 {
		restore()
		t.Fatalf("expected resume empty-row branch success, got %d %v", appended, err)
	}
	restore()

	if _, err := MergeResumeCSV(tempPath, partPath, []string{"2024-01-01T00:09:00Z", "9"}); err == nil {
		t.Fatal("expected duplicate tail missing error")
	}
}
