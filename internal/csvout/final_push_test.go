package csvout

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCSVAndParquetAdditionalEdgeBranches(t *testing.T) {
	dir := t.TempDir()

	emptyCSV := filepath.Join(dir, "empty.csv")
	if err := os.WriteFile(emptyCSV, nil, 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	audit, err := AuditCSV(emptyCSV)
	if err != nil {
		t.Fatalf("AuditCSV empty file returned error: %v", err)
	}
	if audit.Rows != 0 {
		t.Fatalf("expected 0 rows for empty CSV, got %d", audit.Rows)
	}

	noTimestampPart := filepath.Join(dir, "no-ts.csv")
	if err := os.WriteFile(noTimestampPart, []byte("open\n1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := AssembleCSVFromParts(filepath.Join(dir, "assembled.csv"), []string{noTimestampPart}, time.Time{}, time.Now().Add(time.Hour)); err == nil {
		t.Fatal("expected missing timestamp assembly error")
	}

	malformedPart := filepath.Join(dir, "malformed.csv")
	if err := os.WriteFile(malformedPart, []byte("open,timestamp\n1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := AssembleCSVFromParts(filepath.Join(dir, "assembled-malformed.csv"), []string{malformedPart}, time.Time{}, time.Now().Add(time.Hour)); err == nil {
		t.Fatal("expected malformed row assembly error")
	}

	badTimestampPart := filepath.Join(dir, "bad-ts.csv")
	if err := os.WriteFile(badTimestampPart, []byte("timestamp,open\nbad,1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := AssembleCSVFromParts(filepath.Join(dir, "assembled-bad-ts.csv"), []string{badTimestampPart}, time.Time{}, time.Now().Add(time.Hour)); err == nil {
		t.Fatal("expected bad timestamp assembly error")
	}

	dup1 := filepath.Join(dir, "dup1.csv")
	dup2 := filepath.Join(dir, "dup2.csv")
	if err := os.WriteFile(dup1, []byte("timestamp,open\n2024-01-01T00:00:00Z,1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := os.WriteFile(dup2, []byte("timestamp,open\n2024-01-01T00:00:00Z,2\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := AssembleCSVFromParts(filepath.Join(dir, "assembled-dup.csv"), []string{dup1, dup2}, time.Time{}, time.Now().Add(time.Hour)); err == nil {
		t.Fatal("expected conflicting duplicate assembly error")
	}

	if err := ExtractCSVRange(malformedPart, filepath.Join(dir, "range.csv"), time.Time{}, time.Now().Add(time.Hour)); err == nil {
		t.Fatal("expected malformed CSV extract error")
	}
	if err := ExtractCSVRange(badTimestampPart, filepath.Join(dir, "range-bad-ts.csv"), time.Time{}, time.Now().Add(time.Hour)); err == nil {
		t.Fatal("expected bad timestamp extract error")
	}
	if _, err := InspectCSV(badTimestampPart); err == nil {
		t.Fatal("expected InspectCSV timestamp parse error")
	}

	if got := parquetStringValue(true); got != "true" {
		t.Fatalf("unexpected default parquetStringValue result: %q", got)
	}
	if _, ok := parquetTimestampFromRow(map[string]any{"timestamp": 123}); ok {
		t.Fatal("expected numeric parquet timestamp parse failure")
	}
	if _, err := parquetRecordFromCSVRecord([]string{"timestamp", "open"}, []string{"2024-01-01T00:00:00Z"}); err == nil {
		t.Fatal("expected malformed parquet record conversion error")
	}

	emptyParquet := filepath.Join(dir, "empty.parquet")
	if err := writeParquetRecords(emptyParquet, []string{"timestamp", "open"}, nil); err != nil {
		t.Fatalf("writeParquetRecords empty slice returned error: %v", err)
	}
	parquetAudit, err := AuditCSV(emptyParquet)
	if err != nil {
		t.Fatalf("AuditCSV parquet returned error: %v", err)
	}
	if parquetAudit.Rows != 0 {
		t.Fatalf("expected 0 rows for empty parquet, got %d", parquetAudit.Rows)
	}

	if err := extractRangeCSVToParquet(malformedPart, filepath.Join(dir, "range.parquet"), time.Time{}, time.Now().Add(time.Hour)); err == nil {
		t.Fatal("expected malformed CSV->parquet error")
	}
	if err := extractRangeCSVToParquet(badTimestampPart, filepath.Join(dir, "range-bad-ts.parquet"), time.Time{}, time.Now().Add(time.Hour)); err == nil {
		t.Fatal("expected bad timestamp CSV->parquet error")
	}

	statsPath := filepath.Join(dir, "no-ts-inspect.csv")
	if err := os.WriteFile(statsPath, []byte("open\n1\n2\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	stats, err := InspectCSV(statsPath)
	if err != nil {
		t.Fatalf("InspectCSV without timestamp returned error: %v", err)
	}
	if stats.HasTimestamp || stats.Rows != 2 {
		t.Fatalf("unexpected no-timestamp stats: %+v", stats)
	}
	if ColumnsContainTimestamp([]string{"open", "close"}) {
		t.Fatal("expected ColumnsContainTimestamp false branch")
	}
	if !strings.Contains(emptyParquet, ".parquet") {
		t.Fatal("expected parquet path sanity check")
	}
}
