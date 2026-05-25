package csvout

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestInspectExistingCSVErrorBranches(t *testing.T) {
	dir := t.TempDir()
	noTimestamp := filepath.Join(dir, "notimestamp.csv")
	if err := os.WriteFile(noTimestamp, []byte("open\n1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if _, err := InspectExistingCSV(noTimestamp); err == nil {
		t.Fatal("expected missing timestamp error")
	}

	malformed := filepath.Join(dir, "malformed.csv")
	if err := os.WriteFile(malformed, []byte("timestamp,open\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if state, err := InspectExistingCSV(malformed); err != nil || !state.Exists || state.HasRows {
		t.Fatalf("unexpected malformed-empty state: %+v %v", state, err)
	}
}

func TestAuditCSVAndInspectCSVMoreBranches(t *testing.T) {
	dir := t.TempDir()
	empty := filepath.Join(dir, "empty.csv")
	if err := os.WriteFile(empty, []byte(""), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if audit, err := AuditCSV(empty); err != nil || audit.Rows != 0 {
		t.Fatalf("expected empty CSV audit to succeed with zero rows, got %+v %v", audit, err)
	}

	outOfOrder := filepath.Join(dir, "ooo.csv")
	content := "timestamp,open\n2024-01-01T00:01:00Z,2\n2024-01-01T00:00:00Z,1\n"
	if err := os.WriteFile(outOfOrder, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	stats, err := InspectCSV(outOfOrder)
	if err != nil {
		t.Fatalf("InspectCSV returned error: %v", err)
	}
	if stats.OutOfOrderRows == 0 {
		t.Fatalf("expected out-of-order rows in stats: %+v", stats)
	}
}

func TestAssembleExtractAndMergeMoreBranches(t *testing.T) {
	dir := t.TempDir()
	emptyPart := filepath.Join(dir, "empty.csv")
	if err := os.WriteFile(emptyPart, []byte(""), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := AssembleCSVFromParts(filepath.Join(dir, "out.csv"), []string{emptyPart}, time.Time{}, time.Now()); err != nil {
		t.Fatalf("expected empty-part assembly to be skipped cleanly, got %v", err)
	}

	tempPath := filepath.Join(dir, "temp.csv")
	if err := os.WriteFile(tempPath, []byte("timestamp,open\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	existingPath := filepath.Join(dir, "existing.csv")
	if err := os.WriteFile(existingPath, []byte("timestamp,open\n2024-01-01T00:00:00Z,1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if appended, err := MergeResumeCSV(existingPath, tempPath, nil); err != nil || appended != 0 {
		t.Fatalf("expected empty temp merge to no-op, got %d %v", appended, err)
	}
}
