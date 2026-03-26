package csvout

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	parquet "github.com/parquet-go/parquet-go"

	"github.com/Nosvemos/dukascopy-go/internal/dukascopy"
)

func TestParquetRemainingBranches(t *testing.T) {
	dir := t.TempDir()
	instrument := dukascopy.Instrument{PriceScale: 3}
	primaryBars := []dukascopy.Bar{{Time: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), Open: 100, High: 101, Low: 99, Close: 100.5, Volume: 1}}
	bidBars := []dukascopy.Bar{{Time: primaryBars[0].Time, Open: 100.0, High: 101.0, Low: 99.0, Close: 100.3, Volume: 1}}
	askBars := []dukascopy.Bar{{Time: primaryBars[0].Time, Open: 100.2, High: 101.2, Low: 99.2, Close: 100.7, Volume: 1}}

	if _, err := buildBarParquetRecords(instrument, []string{"timestamp", "open"}, primaryBars, nil, nil); err != nil {
		t.Fatalf("buildBarParquetRecords simple returned error: %v", err)
	}
	if err := writeBarsParquet(filepath.Join(dir, "bars.parquet"), instrument, []string{"timestamp", "mid_close", "spread"}, nil, bidBars, askBars); err != nil {
		t.Fatalf("writeBarsParquet returned error: %v", err)
	}
	if err := writeTicksParquet(filepath.Join(dir, "ticks.parquet"), instrument, []string{"timestamp", "bid"}, []dukascopy.Tick{{Time: primaryBars[0].Time, Bid: 100.1}}); err != nil {
		t.Fatalf("writeTicksParquet returned error: %v", err)
	}

	path := filepath.Join(dir, "rows.parquet")
	if err := writeParquetRecords(path, []string{"timestamp", "mid_close"}, []map[string]any{{"timestamp": "2024-01-02T00:00:00Z", "mid_close": 100.5}}); err != nil {
		t.Fatalf("writeParquetRecords returned error: %v", err)
	}
	file, pf, closeFile, err := openParquetFile(path)
	if err != nil {
		t.Fatalf("openParquetFile returned error: %v", err)
	}
	_ = file
	defer closeFile()
	if cols := parquetColumns(pf); len(cols) != 2 {
		t.Fatalf("unexpected parquet columns: %v", cols)
	}

	pathNoMeta := filepath.Join(dir, "rows_no_meta.parquet")
	f, err := os.Create(pathNoMeta)
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	writer := parquet.NewGenericWriter[map[string]any](f, parquetSchemaForColumns([]string{"timestamp", "mid_close"}))
	if _, err := writer.Write([]map[string]any{{"timestamp": "2024-01-02T00:00:00Z", "mid_close": 100.5}}); err != nil {
		t.Fatalf("writer.Write returned error: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close returned error: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("file.Close returned error: %v", err)
	}
	_, pf2, closeFile2, err := openParquetFile(pathNoMeta)
	if err != nil {
		t.Fatalf("openParquetFile no meta returned error: %v", err)
	}
	defer closeFile2()
	if cols := parquetColumns(pf2); len(cols) != 2 {
		t.Fatalf("unexpected schema-derived parquet columns: %v", cols)
	}

	outParquet := filepath.Join(dir, "extract.parquet")
	if err := extractRangeFromParquet(path, outParquet, time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), time.Date(2024, 1, 2, 0, 1, 0, 0, time.UTC)); err != nil {
		t.Fatalf("extractRangeFromParquet parquet output returned error: %v", err)
	}

	if parquetStringValue(uint32(7)) != "7" || parquetStringValue(uint64(8)) != "8" || parquetStringValue(float32(1.5)) != "1.5" {
		t.Fatal("unexpected parquetStringValue branch formatting")
	}
	if _, ok := parquetTimestampFromRow(map[string]any{"timestamp": time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)}); !ok {
		t.Fatal("expected parquetTimestampFromRow to accept time.Time")
	}
	if _, ok := parquetTimestampFromRow(map[string]any{"timestamp": 123}); ok {
		t.Fatal("expected parquetTimestampFromRow default parse branch to fail for invalid value")
	}
}
