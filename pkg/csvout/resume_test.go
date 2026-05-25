package csvout

import (
	"os"
	"path/filepath"
	"testing"
)

func TestInspectExistingCSVReadsLastTimestamp(t *testing.T) {
	outputPath := filepath.Join(t.TempDir(), "bars.csv")
	content := "timestamp,open\n2024-01-02T00:00:00Z,100.0\n2024-01-02T00:01:00Z,101.0\n"
	if err := os.WriteFile(outputPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	state, err := InspectExistingCSV(outputPath)
	if err != nil {
		t.Fatalf("InspectExistingCSV() error = %v", err)
	}
	if !state.Exists || !state.HasRows {
		t.Fatalf("InspectExistingCSV() = %+v, want existing rows", state)
	}
	if got := state.LastTime.UTC().Format(timestampLayout); got != "2024-01-02T00:01:00Z" {
		t.Fatalf("LastTime = %s, want 2024-01-02T00:01:00Z", got)
	}
}

func TestMergeResumeCSVAppendsOnlyMissingRows(t *testing.T) {
	dir := t.TempDir()
	existingPath := filepath.Join(dir, "existing.csv")
	tempPath := filepath.Join(dir, "temp.csv")

	existing := "timestamp,open\n2024-01-02T00:00:00Z,100.0\n2024-01-02T00:01:00Z,101.0\n"
	temp := "timestamp,open\n2024-01-02T00:01:00Z,101.0\n2024-01-02T00:02:00Z,102.0\n"

	if err := os.WriteFile(existingPath, []byte(existing), 0o644); err != nil {
		t.Fatalf("write existing csv: %v", err)
	}
	if err := os.WriteFile(tempPath, []byte(temp), 0o644); err != nil {
		t.Fatalf("write temp csv: %v", err)
	}

	appended, err := MergeResumeCSV(existingPath, tempPath, []string{"2024-01-02T00:01:00Z", "101.0"})
	if err != nil {
		t.Fatalf("MergeResumeCSV() error = %v", err)
	}
	if appended != 1 {
		t.Fatalf("MergeResumeCSV() appended = %d, want 1", appended)
	}

	data, err := os.ReadFile(existingPath)
	if err != nil {
		t.Fatalf("read merged csv: %v", err)
	}
	want := "timestamp,open\n2024-01-02T00:00:00Z,100.0\n2024-01-02T00:01:00Z,101.0\n2024-01-02T00:02:00Z,102.0\n"
	if string(data) != want {
		t.Fatalf("merged csv = %q, want %q", string(data), want)
	}
}

func TestInspectExistingCSVAndMergeResumeCSVWithGzip(t *testing.T) {
	dir := t.TempDir()
	existingPath := filepath.Join(dir, "existing.csv.gz")
	tempPath := filepath.Join(dir, "temp.csv.gz")

	writeGzipCSVFixture(t, existingPath, [][]string{
		{"timestamp", "open"},
		{"2024-01-02T00:00:00Z", "100.0"},
		{"2024-01-02T00:01:00Z", "101.0"},
	})
	writeGzipCSVFixture(t, tempPath, [][]string{
		{"timestamp", "open"},
		{"2024-01-02T00:01:00Z", "101.0"},
		{"2024-01-02T00:02:00Z", "102.0"},
	})

	state, err := InspectExistingCSV(existingPath)
	if err != nil {
		t.Fatalf("InspectExistingCSV() gzip error = %v", err)
	}
	if !state.Exists || !state.HasRows {
		t.Fatalf("InspectExistingCSV() gzip = %+v, want existing rows", state)
	}

	appended, err := MergeResumeCSV(existingPath, tempPath, []string{"2024-01-02T00:01:00Z", "101.0"})
	if err != nil {
		t.Fatalf("MergeResumeCSV() gzip error = %v", err)
	}
	if appended != 1 {
		t.Fatalf("MergeResumeCSV() gzip appended = %d, want 1", appended)
	}

	state, err = InspectExistingCSV(existingPath)
	if err != nil {
		t.Fatalf("InspectExistingCSV() gzip after merge error = %v", err)
	}
	if got := state.LastTime.UTC().Format(timestampLayout); got != "2024-01-02T00:02:00Z" {
		t.Fatalf("gzip LastTime = %s, want 2024-01-02T00:02:00Z", got)
	}
}

func TestAuditCSVReturnsStableRowCountAndHash(t *testing.T) {
	outputPath := filepath.Join(t.TempDir(), "audit.csv")
	content := "timestamp,open\n2024-01-02T00:00:00Z,100.0\n2024-01-02T00:01:00Z,101.0\n"
	if err := os.WriteFile(outputPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	audit, err := AuditCSV(outputPath)
	if err != nil {
		t.Fatalf("AuditCSV() error = %v", err)
	}
	if audit.Rows != 2 {
		t.Fatalf("AuditCSV().Rows = %d, want 2", audit.Rows)
	}
	if audit.Bytes != int64(len(content)) {
		t.Fatalf("AuditCSV().Bytes = %d, want %d", audit.Bytes, len(content))
	}
	if audit.SHA256 == "" {
		t.Fatal("AuditCSV().SHA256 is empty")
	}
}

func writeGzipCSVFixture(t *testing.T, path string, records [][]string) {
	t.Helper()

	_, writer, closeWriter, err := createCSVWriter(path)
	if err != nil {
		t.Fatalf("createCSVWriter() error = %v", err)
	}
	for _, record := range records {
		if err := writer.Write(record); err != nil {
			t.Fatalf("writer.Write() error = %v", err)
		}
	}
	if err := closeWriter(); err != nil {
		t.Fatalf("closeWriter() error = %v", err)
	}
}
