package checkpoint

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestValidateCompatibilityMismatchBranches(t *testing.T) {
	base := Manifest{
		OutputPath: "dataset.csv",
		PartsDir:   "dataset.parts",
		Symbol:     "xauusd",
		Timeframe:  "m1",
		Side:       "BID",
		ResultKind: "bar",
		Columns:    []string{"timestamp"},
		Partition:  "day",
		Parts: []ManifestPart{{
			ID:    "a",
			Start: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			End:   time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			File:  "a.csv",
		}},
	}

	cases := []func(Manifest) Manifest{
		func(m Manifest) Manifest { m.OutputPath = "other.csv"; return m },
		func(m Manifest) Manifest { m.Symbol = "eurusd"; return m },
		func(m Manifest) Manifest { m.Timeframe = "h1"; return m },
		func(m Manifest) Manifest { m.Side = "ASK"; return m },
		func(m Manifest) Manifest { m.ResultKind = "tick"; return m },
		func(m Manifest) Manifest { m.Partition = "month"; return m },
		func(m Manifest) Manifest { m.PartsDir = "other.parts"; return m },
		func(m Manifest) Manifest { m.Parts = append(m.Parts, ManifestPart{ID: "b"}); return m },
	}

	for _, mutate := range cases {
		expected := mutate(base)
		if err := ValidateCompatibility(base, expected); err == nil {
			t.Fatal("expected compatibility mismatch")
		}
	}
}

func TestSaveLoadAndVerifyMoreBranches(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "dataset.manifest.json")
	manifest := Manifest{
		Version:    CurrentManifestVersion,
		OutputPath: filepath.Join(dir, "dataset.csv"),
		PartsDir:   filepath.Join(dir, "parts"),
		Symbol:     "xauusd",
		Timeframe:  "m1",
		Side:       "BID",
		ResultKind: "bar",
		Columns:    []string{"timestamp"},
		Partition:  "day",
		Parts:      []ManifestPart{{ID: "a", File: "a.csv", Status: "completed"}},
	}
	if err := os.MkdirAll(manifest.PartsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll returned error: %v", err)
	}
	if err := Save(manifestPath, manifest); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}
	if err := Save(manifestPath, manifest); err != nil {
		t.Fatalf("second Save returned error: %v", err)
	}
	if _, err := VerifyManifest(manifestPath); err != nil {
		t.Fatalf("VerifyManifest returned error: %v", err)
	}
}
