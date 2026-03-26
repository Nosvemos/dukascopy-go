package csvout

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/dukascopy"
)

func TestAssembleAndExtractErrorBranches(t *testing.T) {
	dir := t.TempDir()
	if err := AssembleCSVFromParts(filepath.Join(dir, "out.csv"), nil, time.Now(), time.Now().Add(time.Hour)); err == nil {
		t.Fatal("expected empty parts error")
	}

	part1 := filepath.Join(dir, "part1.csv")
	part2 := filepath.Join(dir, "part2.csv")
	if err := os.WriteFile(part1, []byte("timestamp,open\n2024-01-01T00:00:00Z,1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := os.WriteFile(part2, []byte("timestamp,close\n2024-01-01T00:01:00Z,2\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := AssembleCSVFromParts(filepath.Join(dir, "mismatch.csv"), []string{part1, part2}, time.Time{}, time.Now()); err == nil {
		t.Fatal("expected header mismatch error")
	}
}

func TestParquetErrorBranches(t *testing.T) {
	dir := t.TempDir()
	badFile := filepath.Join(dir, "bad.parquet")
	if err := os.WriteFile(badFile, []byte("not parquet"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if _, _, _, err := openParquetFile(badFile); err == nil {
		t.Fatal("expected openParquetFile error")
	}
	if _, err := auditParquet(badFile); err == nil {
		t.Fatal("expected auditParquet error")
	}

	instrument := dukascopy.Instrument{PriceScale: 3}
	if _, err := buildBarParquetRecords(instrument, []string{"timestamp", "spread"}, nil, []dukascopy.Bar{{Time: time.Now()}}, nil); err == nil {
		t.Fatal("expected bid/ask parquet build error")
	}

	noTimestampCSV := filepath.Join(dir, "no-ts.csv")
	if err := os.WriteFile(noTimestampCSV, []byte("open\n1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := extractRangeCSVToParquet(noTimestampCSV, filepath.Join(dir, "out.parquet"), time.Now(), time.Now().Add(time.Hour)); err == nil {
		t.Fatal("expected missing timestamp CSV->parquet error")
	}
}
