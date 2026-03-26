package checkpoint

import (
	"path/filepath"
	"testing"
	"time"
)

func TestSaveAndLoadManifestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dataset.csv.manifest.json")
	now := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	manifest := Manifest{
		Version:    CurrentManifestVersion,
		OutputPath: "dataset.csv",
		PartsDir:   "dataset.csv.parts",
		Symbol:     "xauusd",
		Timeframe:  "m1",
		Side:       "BID",
		ResultKind: "bar",
		Columns:    []string{"timestamp", "mid_close"},
		Partition:  "day",
		CreatedAt:  now,
		Parts: []ManifestPart{
			{ID: "part-1", File: "part-1.csv", Status: "completed", Rows: 3},
			{ID: "part-2", File: "part-2.csv", Status: "pending", Rows: 0},
		},
		FinalOutput: &ManifestOutput{Rows: 3, Bytes: 128, SHA256: "deadbeef"},
	}

	if err := Save(path, manifest); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if loaded.OutputPath != manifest.OutputPath {
		t.Fatalf("expected output path %q, got %q", manifest.OutputPath, loaded.OutputPath)
	}
	if loaded.Summary.TotalParts != 2 || loaded.Summary.CompletedParts != 1 || loaded.Summary.PendingParts != 1 {
		t.Fatalf("unexpected summary after round trip: %+v", loaded.Summary)
	}
	if loaded.Summary.TotalRows != 3 || loaded.Summary.OutputRows != 3 || loaded.Summary.OutputBytes != 128 {
		t.Fatalf("unexpected aggregate summary after round trip: %+v", loaded.Summary)
	}
}

func TestValidateCompatibilityRejectsColumnMismatch(t *testing.T) {
	existing := Manifest{
		OutputPath: "dataset.csv",
		PartsDir:   "dataset.csv.parts",
		Symbol:     "xauusd",
		Timeframe:  "m1",
		Side:       "BID",
		ResultKind: "bar",
		Columns:    []string{"timestamp", "mid_close"},
		Partition:  "day",
		Parts: []ManifestPart{
			{
				ID:    "part-1",
				Start: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				End:   time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
				File:  "part-1.csv",
			},
		},
	}
	expected := existing
	expected.Columns = []string{"timestamp", "spread"}

	err := ValidateCompatibility(existing, expected)
	if err == nil {
		t.Fatal("expected compatibility error")
	}
}

func TestFindPartAndRefreshSummary(t *testing.T) {
	manifest := Manifest{
		Parts: []ManifestPart{
			{ID: "part-1", Status: "completed", Rows: 2},
			{ID: "part-2", Status: "failed", Rows: 0},
			{ID: "part-3", Status: "running", Rows: 0},
			{ID: "part-4", Status: "pending", Rows: 0},
		},
	}

	if FindPart(&manifest, "part-3") == nil {
		t.Fatal("expected to find part-3")
	}
	if FindPart(&manifest, "missing") != nil {
		t.Fatal("expected missing part lookup to return nil")
	}

	RefreshSummary(&manifest)
	if manifest.Summary.TotalParts != 4 || manifest.Summary.CompletedParts != 1 || manifest.Summary.FailedParts != 1 || manifest.Summary.RunningParts != 1 || manifest.Summary.PendingParts != 1 {
		t.Fatalf("unexpected summary: %+v", manifest.Summary)
	}
}
