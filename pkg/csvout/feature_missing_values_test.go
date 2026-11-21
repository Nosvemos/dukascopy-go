package csvout

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	"github.com/apache/arrow/go/v18/arrow/memory"
	parquet "github.com/parquet-go/parquet-go"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

var (
	missingFeatBase  = []string{"timestamp", "open", "high", "low", "close", "volume"}
	missingFeatNames = []string{"sma_3", "rsi_14"}
	missingFeatStart = time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
)

// missingFeatFixture returns 4 bars and feature rows containing NaN, ±Inf, a
// short (missing) final row, and finite values that must round-trip exactly.
func missingFeatFixture() ([]dukascopy.Bar, [][]float64) {
	bars := testBars(missingFeatStart, 4, time.Minute)
	rows := [][]float64{
		{math.NaN(), math.Inf(1)},
		{1.25, math.Inf(-1)},
		{0.1 + 0.2, -3.0000000000000004},
		// row 3 deliberately absent
	}
	return bars, rows
}

// missingFeatWant is the expected per-row feature value; nil means missing.
func missingFeatWant() [][]*float64 {
	f := func(v float64) *float64 { return &v }
	return [][]*float64{
		{nil, nil},
		{f(1.25), nil},
		{f(0.1 + 0.2), f(-3.0000000000000004)},
		{nil, nil},
	}
}

func checkFeatureCells(t *testing.T, label string, header []string, rows [][]string) {
	t.Helper()
	want := missingFeatWant()
	if len(rows) != len(want) {
		t.Fatalf("%s: got %d rows, want %d", label, len(rows), len(want))
	}
	for r, wantRow := range want {
		for j, name := range missingFeatNames {
			idx := indexOfColumn(header, name)
			if idx < 0 {
				t.Fatalf("%s: header %v lacks %q", label, header, name)
			}
			cell := rows[r][idx]
			if wantRow[j] == nil {
				if cell != "" {
					t.Fatalf("%s: row %d %s = %q, want empty", label, r, name, cell)
				}
				continue
			}
			got, err := strconv.ParseFloat(cell, 64)
			if err != nil || got != *wantRow[j] {
				t.Fatalf("%s: row %d %s = %q, want %v", label, r, name, cell, *wantRow[j])
			}
		}
	}
}

func readAllCSV(t *testing.T, path string) ([]string, [][]string) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var r io.Reader = file
	if filepath.Ext(path) == ".gz" {
		zr, err := gzip.NewReader(file)
		if err != nil {
			t.Fatal(err)
		}
		defer zr.Close()
		r = zr
	}
	records, err := csv.NewReader(r).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	return records[0], records[1:]
}

func TestFeaturesNonFiniteCSVAndGzipEmptyCells(t *testing.T) {
	bars, featRows := missingFeatFixture()
	for _, name := range []string{"f.csv", "f.csv.gz"} {
		path := filepath.Join(t.TempDir(), name)
		if err := WriteBarsWithFeatures(path, testInstrument, missingFeatBase, bars, missingFeatNames, featRows); err != nil {
			t.Fatalf("%s: %v", name, err)
		}

		header, rows := readAllCSV(t, path)
		checkFeatureCells(t, name, header, rows)

		// The package reader must accept empty feature cells.
		readBars, readHeader, err := ReadBarsFromCSV(path)
		if err != nil {
			t.Fatalf("%s: ReadBarsFromCSV: %v", name, err)
		}
		if len(readBars) != len(bars) || len(readHeader) != len(missingFeatBase)+len(missingFeatNames) {
			t.Fatalf("%s: read %d bars / header %v", name, len(readBars), readHeader)
		}
		for i := range bars {
			if !readBars[i].Time.Equal(bars[i].Time) || readBars[i].Volume != bars[i].Volume {
				t.Fatalf("%s: bar %d = %+v, want %+v", name, i, readBars[i], bars[i])
			}
		}

		// Inspect and duplicate cleaning treat cells as text and must not fail.
		stats, err := InspectCSV(path)
		if err != nil || stats.Rows != len(bars) {
			t.Fatalf("%s: InspectCSV rows=%d err=%v", name, stats.Rows, err)
		}
		if _, err := CleanDuplicates(path); err != nil {
			t.Fatalf("%s: CleanDuplicates: %v", name, err)
		}
		header, rows = readAllCSV(t, path)
		checkFeatureCells(t, name+" after clean", header, rows)
	}
}

func TestFeaturesNonFiniteJSONLNull(t *testing.T) {
	bars, featRows := missingFeatFixture()
	path := filepath.Join(t.TempDir(), "f.jsonl")
	if err := WriteBarsWithFeatures(path, testInstrument, missingFeatBase, bars, missingFeatNames, featRows); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	want := missingFeatWant()
	scanner := bufio.NewScanner(file)
	r := 0
	for ; scanner.Scan(); r++ {
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(scanner.Bytes(), &raw); err != nil {
			t.Fatalf("row %d: %v", r, err)
		}
		obj := make(map[string]*float64)
		for _, name := range missingFeatNames {
			msg, ok := raw[name]
			if !ok {
				t.Fatalf("row %d lacks key %q", r, name)
			}
			var v *float64
			if err := json.Unmarshal(msg, &v); err != nil {
				t.Fatalf("row %d %s: %v", r, name, err)
			}
			obj[name] = v
		}
		for j, name := range missingFeatNames {
			got, w := obj[name], want[r][j]
			if (got == nil) != (w == nil) || (got != nil && *got != *w) {
				t.Fatalf("row %d %s = %v, want %v", r, name, got, w)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if r != len(want) {
		t.Fatalf("got %d JSONL rows, want %d", r, len(want))
	}
}

// readParquetFeatures reads path with the parquet-go library directly, checks
// the feature columns are optional and the base columns required, and returns
// the feature values (nil = NULL).
func readParquetFeatures(t *testing.T, path string) [][]*float64 {
	t.Helper()
	_, pf, closeFile, err := openParquetFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer closeFile()

	for _, field := range pf.Schema().Fields() {
		if isFeatureColumn(field.Name()) {
			if !field.Optional() {
				t.Fatalf("%s: feature column %q is not optional", path, field.Name())
			}
		} else if !field.Required() {
			t.Fatalf("%s: base column %q is not required", path, field.Name())
		}
	}

	leafIndex := make(map[string]int)
	for i, columnPath := range pf.Schema().Columns() {
		leafIndex[columnPath[len(columnPath)-1]] = i
	}

	reader := parquet.NewReader(pf)
	defer reader.Close()
	var out [][]*float64
	for {
		row := make(parquet.Row, 0)
		rows := []parquet.Row{row}
		n, err := reader.ReadRows(rows)
		if n == 1 {
			values := make([]*float64, len(missingFeatNames))
			for _, value := range rows[0] {
				for j, name := range missingFeatNames {
					if value.Column() != leafIndex[name] {
						continue
					}
					if value.IsNull() {
						values[j] = nil
					} else {
						v := value.Double()
						values[j] = &v
					}
				}
			}
			out = append(out, values)
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func assertFeatureValues(t *testing.T, label string, got [][]*float64) {
	t.Helper()
	want := missingFeatWant()
	if len(got) != len(want) {
		t.Fatalf("%s: got %d rows, want %d", label, len(got), len(want))
	}
	for r := range want {
		for j, name := range missingFeatNames {
			g, w := got[r][j], want[r][j]
			if (g == nil) != (w == nil) || (g != nil && *g != *w) {
				if g != nil && math.IsNaN(*g) {
					t.Fatalf("%s: row %d %s is NaN, want %v", label, r, name, w)
				}
				t.Fatalf("%s: row %d %s = %v, want %v", label, r, name, g, w)
			}
		}
	}
}

func TestFeaturesNonFiniteParquetNull(t *testing.T) {
	bars, featRows := missingFeatFixture()
	dir := t.TempDir()
	path := filepath.Join(dir, "f.parquet")
	if err := WriteBarsWithFeatures(path, testInstrument, missingFeatBase, bars, missingFeatNames, featRows); err != nil {
		t.Fatal(err)
	}
	assertFeatureValues(t, "written", readParquetFeatures(t, path))

	// Base columns still round-trip.
	osFile, pf, closeFile, err := openParquetFile(path)
	if err != nil {
		t.Fatal(err)
	}
	reader := parquet.NewGenericReader[map[string]any](osFile, pf.Schema())
	maps := make([]map[string]any, len(bars))
	for i := range maps {
		maps[i] = map[string]any{}
	}
	if n, err := reader.Read(maps); n != len(bars) || (err != nil && !errors.Is(err, io.EOF)) {
		t.Fatalf("generic read n=%d err=%v", n, err)
	}
	reader.Close()
	closeFile()
	for i, bar := range bars {
		if maps[i]["volume"] != bar.Volume || maps[i]["timestamp"] != DefaultConfig().formatTime(bar.Time) {
			t.Fatalf("row %d base values = %v", i, maps[i])
		}
		if maps[i]["sma_3"] != nil && i != 1 && i != 2 {
			t.Fatalf("row %d sma_3 = %v, want nil", i, maps[i]["sma_3"])
		}
	}

	// Package readers: inspect, extract (to CSV and Parquet), clean duplicates.
	stats, err := inspectParquet(path)
	if err != nil || stats.Rows != len(bars) {
		t.Fatalf("inspectParquet rows=%d err=%v", stats.Rows, err)
	}

	from, to := missingFeatStart, missingFeatStart.Add(time.Hour)
	csvOut := filepath.Join(dir, "extract.csv")
	if err := extractRangeFromParquet(path, csvOut, from, to); err != nil {
		t.Fatalf("extract parquet->csv: %v", err)
	}
	header, rows := readAllCSV(t, csvOut)
	checkFeatureCells(t, "extract parquet->csv", header, rows)

	pqOut := filepath.Join(dir, "extract.parquet")
	if err := extractRangeFromParquet(path, pqOut, from, to); err != nil {
		t.Fatalf("extract parquet->parquet: %v", err)
	}
	assertFeatureValues(t, "extract parquet->parquet", readParquetFeatures(t, pqOut))

	if _, err := cleanParquetDuplicates(path); err != nil {
		t.Fatalf("cleanParquetDuplicates: %v", err)
	}
	assertFeatureValues(t, "clean duplicates", readParquetFeatures(t, path))

	// CSV -> Parquet conversions read empty feature cells as NULL.
	csvPath := filepath.Join(dir, "src.csv")
	if err := WriteBarsWithFeatures(csvPath, testInstrument, missingFeatBase, bars, missingFeatNames, featRows); err != nil {
		t.Fatal(err)
	}
	fromCSV := filepath.Join(dir, "fromcsv.parquet")
	if err := extractRangeCSVToParquet(csvPath, fromCSV, from, to); err != nil {
		t.Fatalf("extract csv->parquet: %v", err)
	}
	assertFeatureValues(t, "extract csv->parquet", readParquetFeatures(t, fromCSV))

	assembled := filepath.Join(dir, "assembled.parquet")
	if err := assembleParquetFromCSVParts(assembled, []string{csvPath}, from, to); err != nil {
		t.Fatalf("assemble csv parts->parquet: %v", err)
	}
	assertFeatureValues(t, "assemble", readParquetFeatures(t, assembled))
}

func TestParquetRecordFromCSVRecordFeatureCells(t *testing.T) {
	columns := []string{"timestamp", "close", "ema"}
	for _, cell := range []string{"", "  ", "NaN", "+Inf", "-Inf"} {
		row, err := parquetRecordFromCSVRecord(columns, []string{"2024-01-02T00:00:00Z", "1.1", cell})
		if err != nil {
			t.Fatalf("cell %q: %v", cell, err)
		}
		if _, ok := row["ema"]; ok {
			t.Fatalf("cell %q: ema = %v, want absent", cell, row["ema"])
		}
	}
	if _, err := parquetRecordFromCSVRecord(columns, []string{"2024-01-02T00:00:00Z", "1.1", "abc"}); err == nil {
		t.Fatal("expected error for unparsable feature cell")
	}
	// Base columns keep their strict parsing.
	if _, err := parquetRecordFromCSVRecord(columns, []string{"2024-01-02T00:00:00Z", "", "1"}); err == nil {
		t.Fatal("expected error for empty base column")
	}
}

func TestFeaturesNonFiniteArrowNull(t *testing.T) {
	bars, featRows := missingFeatFixture()
	for _, name := range []string{"f.arrow", "f.feather"} {
		path := filepath.Join(t.TempDir(), name)
		if err := WriteBarsWithFeatures(path, testInstrument, missingFeatBase, bars, missingFeatNames, featRows); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		file, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		reader, err := ipc.NewFileReader(file, ipc.WithAllocator(memory.NewGoAllocator()))
		if err != nil {
			file.Close()
			t.Fatal(err)
		}

		schema := reader.Schema()
		for _, field := range schema.Fields() {
			if isFeatureColumn(field.Name) != field.Nullable {
				t.Fatalf("%s: field %q nullable=%v", name, field.Name, field.Nullable)
			}
		}

		got := make([][]*float64, 0, len(bars))
		var volumes []float64
		for i := 0; i < reader.NumRecords(); i++ {
			rec, err := reader.Record(i)
			if err != nil {
				t.Fatal(err)
			}
			cols := make([]*array.Float64, len(missingFeatNames))
			for j, feat := range missingFeatNames {
				idx := schema.FieldIndices(feat)
				if len(idx) != 1 || rec.Column(idx[0]).DataType().ID() != arrow.FLOAT64 {
					t.Fatalf("%s: column %q missing or not float64", name, feat)
				}
				cols[j] = rec.Column(idx[0]).(*array.Float64)
			}
			vol := rec.Column(schema.FieldIndices("volume")[0]).(*array.Float64)
			for r := 0; r < int(rec.NumRows()); r++ {
				row := make([]*float64, len(missingFeatNames))
				for j, col := range cols {
					if !col.IsNull(r) {
						v := col.Value(r)
						row[j] = &v
					}
				}
				got = append(got, row)
				volumes = append(volumes, vol.Value(r))
			}
		}
		reader.Close()
		file.Close()

		assertFeatureValues(t, name, got)
		for i, bar := range bars {
			if volumes[i] != bar.Volume {
				t.Fatalf("%s: volume[%d] = %v, want %v", name, i, volumes[i], bar.Volume)
			}
		}
	}
}

func TestAppendArrowRecordsNonFiniteOnlyNullsFeatureColumns(t *testing.T) {
	columns := []string{"close", "feat"}
	schema := arrowSchemaForColumns(columns)
	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer builder.Release()
	appendArrowRecords(builder, columns, []map[string]any{
		{"close": 1.5, "feat": math.NaN()},
		{"close": 2.5, "feat": "Inf"},
		{"close": 3.5, "feat": "4.25"},
	})
	rec := builder.NewRecord()
	defer rec.Release()
	feat := rec.Column(1).(*array.Float64)
	if !feat.IsNull(0) || !feat.IsNull(1) || feat.IsNull(2) || feat.Value(2) != 4.25 {
		t.Fatalf("feat column = %v", feat)
	}
	if rec.Column(0).NullN() != 0 {
		t.Fatalf("base column has nulls: %v", rec.Column(0))
	}
}
