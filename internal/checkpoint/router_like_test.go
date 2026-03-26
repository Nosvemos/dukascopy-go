package checkpoint

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadMissingAndVerifyWithoutFinalOutput(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "missing.manifest.json")); err == nil {
		t.Fatal("expected missing manifest load error")
	}

	dir := t.TempDir()
	partPath := filepath.Join(dir, "part.csv")
	if err := os.WriteFile(partPath, []byte("timestamp,open\n2024-01-01T00:00:00Z,1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	manifestPath := filepath.Join(dir, "dataset.manifest.json")
	manifest := Manifest{
		Version:    CurrentManifestVersion,
		OutputPath: filepath.Join(dir, "dataset.csv"),
		PartsDir:   dir,
		Symbol:     "xauusd",
		Timeframe:  "m1",
		Side:       "BID",
		ResultKind: "bar",
		Columns:    []string{"timestamp", "open"},
		Partition:  "day",
		Parts: []ManifestPart{{
			ID:     "part-1",
			File:   filepath.Base(partPath),
			Status: "completed",
			Rows:   1,
		}},
	}
	if err := Save(manifestPath, manifest); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}
	report, err := VerifyManifest(manifestPath)
	if err != nil {
		t.Fatalf("VerifyManifest returned error: %v", err)
	}
	if !report.Valid || report.FinalOutput != nil {
		t.Fatalf("unexpected verification report without final output: %+v", report)
	}
}
