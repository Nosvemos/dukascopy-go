package checkpoint

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/csvout"
)

func TestVerifyPartAndOutput(t *testing.T) {
	dir := t.TempDir()
	partPath := filepath.Join(dir, "part.csv")
	outputPath := filepath.Join(dir, "output.csv")
	content := "timestamp,mid_close\n2024-01-01T00:00:00Z,1.1\n2024-01-01T00:01:00Z,1.2\n"
	if err := os.WriteFile(partPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := os.WriteFile(outputPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	audit, err := csvout.AuditCSV(partPath)
	if err != nil {
		t.Fatalf("AuditCSV returned error: %v", err)
	}

	partResult := verifyPart(ManifestPart{
		ID:     "part-1",
		Status: "completed",
		Rows:   audit.Rows,
		Bytes:  audit.Bytes,
		SHA256: audit.SHA256,
	}, partPath)
	if !partResult.Valid {
		t.Fatalf("expected valid part verification, got %+v", partResult)
	}

	outputResult := verifyOutput(ManifestOutput{
		Rows:   audit.Rows,
		Bytes:  audit.Bytes,
		SHA256: audit.SHA256,
	}, outputPath)
	if !outputResult.Valid {
		t.Fatalf("expected valid output verification, got %+v", outputResult)
	}
}

func TestVerifyPartDetectsMismatch(t *testing.T) {
	dir := t.TempDir()
	partPath := filepath.Join(dir, "part.csv")
	content := "timestamp,mid_close\n2024-01-01T00:00:00Z,1.1\n"
	if err := os.WriteFile(partPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	result := verifyPart(ManifestPart{
		ID:     "part-1",
		Status: "completed",
		Rows:   99,
		Bytes:  1,
		SHA256: "bad",
	}, partPath)
	if result.Valid {
		t.Fatal("expected invalid result")
	}
	if !strings.Contains(result.Problem, "row mismatch") {
		t.Fatalf("expected row mismatch problem, got %q", result.Problem)
	}
}

func TestVerifyManifestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	partPath := filepath.Join(dir, "part-1.csv")
	outputPath := filepath.Join(dir, "dataset.csv")
	content := "timestamp,mid_close\n2024-01-01T00:00:00Z,1.1\n2024-01-01T00:01:00Z,1.2\n"
	if err := os.WriteFile(partPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := os.WriteFile(outputPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	partAudit, err := csvout.AuditCSV(partPath)
	if err != nil {
		t.Fatalf("AuditCSV returned error: %v", err)
	}
	outputAudit, err := csvout.AuditCSV(outputPath)
	if err != nil {
		t.Fatalf("AuditCSV returned error: %v", err)
	}

	manifestPath := filepath.Join(dir, "dataset.csv.manifest.json")
	manifest := Manifest{
		Version:    CurrentManifestVersion,
		OutputPath: outputPath,
		PartsDir:   dir,
		Symbol:     "xauusd",
		Timeframe:  "m1",
		Side:       "BID",
		ResultKind: "bar",
		Columns:    []string{"timestamp", "mid_close"},
		Partition:  "day",
		CreatedAt:  time.Now().UTC(),
		Parts: []ManifestPart{
			{
				ID:     "part-1",
				Start:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				End:    time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
				File:   filepath.Base(partPath),
				Status: "completed",
				Rows:   partAudit.Rows,
				Bytes:  partAudit.Bytes,
				SHA256: partAudit.SHA256,
			},
		},
		FinalOutput: &ManifestOutput{
			Rows:   outputAudit.Rows,
			Bytes:  outputAudit.Bytes,
			SHA256: outputAudit.SHA256,
		},
	}

	if err := Save(manifestPath, manifest); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	report, err := VerifyManifest(manifestPath)
	if err != nil {
		t.Fatalf("VerifyManifest returned error: %v", err)
	}
	if !report.Valid {
		t.Fatalf("expected valid report, got %+v", report)
	}
	if report.FinalOutput == nil || !report.FinalOutput.Valid {
		t.Fatalf("expected valid final output verification, got %+v", report.FinalOutput)
	}
}
