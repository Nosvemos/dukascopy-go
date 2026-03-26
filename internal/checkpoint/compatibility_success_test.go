package checkpoint

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestValidateCompatibilitySuccessAndVerifyLoadBranches(t *testing.T) {
	base := Manifest{
		Version:    CurrentManifestVersion,
		OutputPath: "dataset.csv",
		PartsDir:   "dataset.parts",
		Symbol:     "xauusd",
		Timeframe:  "m1",
		Side:       "BID",
		ResultKind: "bar",
		Columns:    []string{"timestamp", "open"},
		Partition:  "day",
		Parts: []ManifestPart{{
			ID:    "part-1",
			Start: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			End:   time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			File:  "part-1.csv",
		}},
	}

	if err := ValidateCompatibility(base, base); err != nil {
		t.Fatalf("expected compatible manifests, got %v", err)
	}

	if _, err := VerifyManifest(filepath.Join(t.TempDir(), "missing.manifest.json")); err == nil {
		t.Fatal("expected VerifyManifest load error for missing file")
	}
}

func TestSaveOverwriteAndVerifyOutputMissingBranches(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dataset.manifest.json")
	manifest := Manifest{
		Version:    CurrentManifestVersion,
		OutputPath: "dataset.csv",
		PartsDir:   "dataset.parts",
		Symbol:     "xauusd",
		Timeframe:  "m1",
		Side:       "BID",
		ResultKind: "bar",
		Columns:    []string{"timestamp"},
		Partition:  "day",
	}

	if err := Save(path, manifest); err != nil {
		t.Fatalf("first Save returned error: %v", err)
	}
	if err := os.WriteFile(path, []byte(`{"version":1}`), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := Save(path, manifest); err != nil {
		t.Fatalf("overwrite Save returned error: %v", err)
	}

	result := verifyOutput(ManifestOutput{Rows: 1}, filepath.Join(dir, "missing.csv"))
	if result.Exists || result.Valid {
		t.Fatalf("expected missing final output to be invalid, got %+v", result)
	}
}
