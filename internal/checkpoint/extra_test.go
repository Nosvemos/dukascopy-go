package checkpoint

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDefaultPathsAndLoadValidation(t *testing.T) {
	if got := DefaultManifestPath("dataset.csv"); got != "dataset.csv.manifest.json" {
		t.Fatalf("unexpected default manifest path: %q", got)
	}
	if got := DefaultPartsDir("dataset.csv"); got != "dataset.csv.parts" {
		t.Fatalf("unexpected default parts dir: %q", got)
	}

	path := filepath.Join(t.TempDir(), "bad.manifest.json")
	if err := os.WriteFile(path, []byte(`{"version":999}`), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected unsupported version error")
	}
}

func TestVerifyHelpersDetectMissingAndMismatchedFiles(t *testing.T) {
	missing := verifyPart(ManifestPart{ID: "part-1", Status: "completed"}, filepath.Join(t.TempDir(), "missing.csv"))
	if missing.Exists || missing.Valid {
		t.Fatalf("expected missing file to be invalid, got %+v", missing)
	}

	partPath := filepath.Join(t.TempDir(), "part.csv")
	if err := os.WriteFile(partPath, []byte("timestamp,mid_close\n2024-01-01T00:00:00Z,1.1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	result := verifyPart(ManifestPart{ID: "part-1", Status: "failed", Rows: 1}, partPath)
	if result.Valid || !strings.Contains(result.Problem, "manifest status") {
		t.Fatalf("expected invalid status problem, got %+v", result)
	}

	output := verifyOutput(ManifestOutput{Rows: 5}, partPath)
	if output.Valid || !strings.Contains(output.Problem, "row mismatch") {
		t.Fatalf("expected output row mismatch, got %+v", output)
	}
}

func TestValidateCompatibilityDetectsRangeMismatch(t *testing.T) {
	left := Manifest{
		OutputPath: "dataset.csv",
		PartsDir:   "dataset.parts",
		Symbol:     "xauusd",
		Timeframe:  "m1",
		Side:       "BID",
		ResultKind: "bar",
		Columns:    []string{"timestamp"},
		Partition:  "day",
		Parts: []ManifestPart{
			{ID: "a", File: "a.csv"},
		},
	}
	right := left
	right.Parts = []ManifestPart{{ID: "b", File: "b.csv"}}
	if err := ValidateCompatibility(left, right); err == nil {
		t.Fatal("expected partition mismatch error")
	}
}
