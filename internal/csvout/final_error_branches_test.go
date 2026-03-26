package csvout

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/dukascopy"
)

func TestCSVOutputFailureBranches(t *testing.T) {
	dir := t.TempDir()
	parentFile := filepath.Join(dir, "parent")
	if err := os.WriteFile(parentFile, []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	instrument := dukascopy.Instrument{PriceScale: 3}
	bar := dukascopy.Bar{Time: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), Open: 1}
	tick := dukascopy.Tick{Time: bar.Time, Bid: 1, Ask: 2}

	if err := WriteBars(filepath.Join(parentFile, "bars.csv"), instrument, []string{"timestamp", "open"}, []dukascopy.Bar{bar}, nil, nil); err == nil {
		t.Fatal("expected WriteBars parent-dir error")
	}
	if err := WriteTicks(filepath.Join(parentFile, "ticks.csv"), instrument, []string{"timestamp", "bid", "ask"}, []dukascopy.Tick{tick}); err == nil {
		t.Fatal("expected WriteTicks parent-dir error")
	}
	if err := WriteBarsAtomic(filepath.Join(parentFile, "bars.csv"), instrument, []string{"timestamp", "open"}, []dukascopy.Bar{bar}, nil, nil); err == nil {
		t.Fatal("expected WriteBarsAtomic temp-path error")
	}
	if err := WriteTicksAtomic(filepath.Join(parentFile, "ticks.csv"), instrument, []string{"timestamp", "bid", "ask"}, []dukascopy.Tick{tick}); err == nil {
		t.Fatal("expected WriteTicksAtomic temp-path error")
	}

	if _, _, _, err := createCSVWriter(filepath.Join(parentFile, "writer.csv")); err == nil {
		t.Fatal("expected createCSVWriter error")
	}

	headerOnly := filepath.Join(dir, "header.csv")
	if err := os.WriteFile(headerOnly, []byte("timestamp,open\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	audit, err := AuditCSV(headerOnly)
	if err != nil {
		t.Fatalf("AuditCSV header-only returned error: %v", err)
	}
	if audit.Rows != 0 {
		t.Fatalf("expected 0 header-only rows, got %d", audit.Rows)
	}

	noTS := filepath.Join(dir, "no-ts.csv")
	if err := os.WriteFile(noTS, []byte("open\n1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := ExtractCSVRange(noTS, filepath.Join(dir, "out.csv"), time.Time{}, time.Now().Add(time.Hour)); err == nil {
		t.Fatal("expected missing timestamp extract error")
	}

	if got := BarColumnsForProfile(Profile("weird")); got != nil {
		t.Fatalf("expected nil unknown bar profile, got %v", got)
	}
	if got := TickColumnsForProfile(Profile("weird")); got != nil {
		t.Fatalf("expected nil unknown tick profile, got %v", got)
	}
	if got := inferTimeframe([]time.Duration{2 * time.Second}); got != "2s" {
		t.Fatalf("expected inferTimeframe default duration string, got %q", got)
	}
}

func TestParquetAdditionalFailureBranches(t *testing.T) {
	dir := t.TempDir()
	noTSParquet := filepath.Join(dir, "no-ts.parquet")
	if err := writeParquetRecords(noTSParquet, []string{"open"}, []map[string]any{{"open": 1.0}}); err != nil {
		t.Fatalf("writeParquetRecords returned error: %v", err)
	}
	if err := extractRangeFromParquet(noTSParquet, filepath.Join(dir, "out.csv"), time.Time{}, time.Now().Add(time.Hour)); err == nil {
		t.Fatal("expected missing timestamp parquet extract error")
	}

	parentFile := filepath.Join(dir, "parent")
	if err := os.WriteFile(parentFile, []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := writeParquetRecords(filepath.Join(parentFile, "bad.parquet"), []string{"timestamp"}, []map[string]any{{"timestamp": "2024-01-01T00:00:00Z"}}); err == nil {
		t.Fatal("expected writeParquetRecords ensureParentDir error")
	}

	if !strings.Contains(parquetStringValue(uint32(7)), "7") {
		t.Fatal("expected parquetStringValue uint32 branch")
	}
	if !strings.Contains(parquetStringValue(int32(7)), "7") {
		t.Fatal("expected parquetStringValue int32 branch")
	}
	if !strings.Contains(parquetStringValue(int64(7)), "7") {
		t.Fatal("expected parquetStringValue int64 branch")
	}
	if !strings.Contains(parquetStringValue(uint64(7)), "7") {
		t.Fatal("expected parquetStringValue uint64 branch")
	}
	if !strings.Contains(parquetStringValue(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)), "2024-01-01T00:00:00Z") {
		t.Fatal("expected parquetStringValue time branch")
	}

	var out bytes.Buffer
	instrument := dukascopy.Instrument{PriceScale: 3}
	if err := WriteTicksToWriter(&out, instrument, []string{"timestamp", "bid", "ask"}, nil); err != nil {
		t.Fatalf("WriteTicksToWriter empty slice returned error: %v", err)
	}
	if !strings.Contains(out.String(), "timestamp,bid,ask") {
		t.Fatalf("unexpected WriteTicksToWriter output: %s", out.String())
	}
}
