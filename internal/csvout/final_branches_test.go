package csvout

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	parquet "github.com/parquet-go/parquet-go"

	"github.com/Nosvemos/dukascopy-go/internal/dukascopy"
)

func TestCSVMiscBranches(t *testing.T) {
	if err := ensureParentDir("file.csv"); err != nil {
		t.Fatalf("ensureParentDir current-dir returned error: %v", err)
	}

	dir := t.TempDir()
	parentFile := filepath.Join(dir, "parent")
	if err := os.WriteFile(parentFile, []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := replaceFile(filepath.Join(dir, "missing"), filepath.Join(parentFile, "child.csv")); err == nil {
		t.Fatal("expected replaceFile parent-dir error")
	}

	badGzip := filepath.Join(dir, "bad.csv.gz")
	if err := os.WriteFile(badGzip, []byte("not gzip"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if _, _, _, err := openCSVReader(badGzip); err == nil {
		t.Fatal("expected openCSVReader gzip error")
	}

	if got := estimateMissingIntervals(90*time.Second, time.Minute); got != 1 {
		t.Fatalf("unexpected missing interval estimate: %d", got)
	}
}

func TestFormatColumnCoverage(t *testing.T) {
	bar := dukascopy.Bar{Time: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), Open: 100, High: 101, Low: 99, Close: 100.5, Volume: 2}
	bid := dukascopy.Bar{Time: bar.Time, Open: 100.0, High: 101.0, Low: 99.0, Close: 100.3, Volume: 2}
	ask := dukascopy.Bar{Time: bar.Time, Open: 100.2, High: 101.2, Low: 99.2, Close: 100.7, Volume: 2}
	tick := dukascopy.Tick{Time: bar.Time, Bid: 100.1, Ask: 100.3, BidVolume: 1, AskVolume: 2}

	for _, column := range []string{"timestamp", "high", "low", "close", "mid_open", "mid_high", "mid_low", "mid_close", "volume"} {
		if _, err := formatPrimaryBarColumn(column, 3, bar); err != nil {
			t.Fatalf("formatPrimaryBarColumn(%s) error: %v", column, err)
		}
	}
	for _, column := range []string{"timestamp", "open", "high", "low", "close", "mid_open", "mid_high", "mid_low", "mid_close", "spread", "volume", "bid_open", "bid_high", "bid_low", "bid_close", "ask_open", "ask_high", "ask_low", "ask_close"} {
		if _, err := formatBarColumn(column, 3, bid, ask); err != nil {
			t.Fatalf("formatBarColumn(%s) error: %v", column, err)
		}
	}
	for _, column := range []string{"timestamp", "bid", "ask", "bid_volume", "ask_volume"} {
		if _, err := formatTickColumn(column, 3, tick); err != nil {
			t.Fatalf("formatTickColumn(%s) error: %v", column, err)
		}
	}
}

func TestWriterBranchesAndParquetInspectError(t *testing.T) {
	instrument := dukascopy.Instrument{PriceScale: 3}
	var out bytes.Buffer
	if err := WriteBarsToWriter(&out, instrument, []string{"timestamp", "open"}, []dukascopy.Bar{{Time: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), Open: 1}}, nil, nil); err != nil {
		t.Fatalf("WriteBarsToWriter primary returned error: %v", err)
	}

	dir := t.TempDir()
	parquetPath := filepath.Join(dir, "bad-ts.parquet")
	f, err := os.Create(parquetPath)
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	writer := parquet.NewGenericWriter[map[string]any](f, parquetSchemaForColumns([]string{"timestamp"}))
	writer.SetKeyValueMetadata(parquetColumnsMetadataKey, "timestamp")
	if _, err := writer.Write([]map[string]any{{"timestamp": "bad"}}); err != nil {
		t.Fatalf("writer.Write returned error: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close returned error: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("file.Close returned error: %v", err)
	}
	if _, err := inspectParquet(parquetPath); err == nil {
		t.Fatal("expected inspectParquet timestamp parse error")
	}
}
