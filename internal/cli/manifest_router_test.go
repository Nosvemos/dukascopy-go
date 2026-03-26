package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/checkpoint"
)

func TestManifestRouterCases(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	dir := t.TempDir()
	outputPath := filepath.Join(dir, "dataset.csv")
	partPath := filepath.Join(dir, "part.csv")
	if err := os.WriteFile(outputPath, []byte("timestamp,open\n2024-01-01T00:00:00Z,1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := os.WriteFile(partPath, []byte("timestamp,open\n2024-01-01T00:00:00Z,1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	manifestPath := checkpoint.DefaultManifestPath(outputPath)
	manifest := checkpoint.Manifest{
		Version:    checkpoint.CurrentManifestVersion,
		OutputPath: outputPath,
		PartsDir:   dir,
		Symbol:     "xauusd",
		Timeframe:  "m1",
		Side:       "BID",
		ResultKind: "bar",
		Columns:    []string{"timestamp", "open"},
		Partition:  "day",
		Parts: []checkpoint.ManifestPart{{
			ID:        "part-1",
			Start:     time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			End:       time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			File:      filepath.Base(partPath),
			Status:    "completed",
			Rows:      1,
			UpdatedAt: time.Now().UTC(),
		}},
	}
	if err := checkpoint.Save(manifestPath, manifest); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	var out bytes.Buffer
	if err := runManifest([]string{"inspect", "--manifest", manifestPath}, &out); err != nil {
		t.Fatalf("runManifest inspect returned error: %v", err)
	}
	if !strings.Contains(out.String(), "Manifest") {
		t.Fatalf("unexpected inspect output: %s", out.String())
	}

	out.Reset()
	if err := runManifest([]string{"verify", "--manifest", manifestPath}, &out); err != nil {
		t.Fatalf("runManifest verify returned error: %v", err)
	}

	out.Reset()
	if err := runManifest([]string{"repair", "--manifest", manifestPath}, &out); err != nil {
		t.Fatalf("runManifest repair returned error: %v", err)
	}

	tempPath := outputPath + ".tmp-123"
	if err := os.WriteFile(tempPath, []byte("tmp"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	out.Reset()
	if err := runManifest([]string{"prune", "--manifest", manifestPath}, &out); err != nil {
		t.Fatalf("runManifest prune returned error: %v", err)
	}
	if !strings.Contains(out.String(), "removed") {
		t.Fatalf("unexpected prune output: %s", out.String())
	}
}

func TestManifestParseAndBoundaryBranches(t *testing.T) {
	if err := runManifestInspect([]string{"--bad-flag"}, &bytes.Buffer{}); err == nil {
		t.Fatal("expected runManifestInspect parse error")
	}
	if err := runManifestVerify([]string{"--bad-flag"}, &bytes.Buffer{}); err == nil {
		t.Fatal("expected runManifestVerify parse error")
	}
	if err := runManifestRepair([]string{"--bad-flag"}, &bytes.Buffer{}); err == nil {
		t.Fatal("expected runManifestRepair parse error")
	}
	if err := runManifestPrune([]string{"--bad-flag"}, &bytes.Buffer{}); err == nil {
		t.Fatal("expected runManifestPrune parse error")
	}

	base := time.Date(2024, 1, 3, 10, 0, 0, 0, time.UTC)
	if next, err := nextPartitionBoundary(base, partitionWeek); err != nil || next.Weekday() != time.Monday {
		t.Fatalf("unexpected weekly boundary: %v %v", next, err)
	}
	if next, err := nextPartitionBoundary(base, partitionMonth); err != nil || next.Day() != 1 || next.Month() != time.February {
		t.Fatalf("unexpected monthly boundary: %v %v", next, err)
	}
	if next, err := nextPartitionBoundary(base, partitionYear); err != nil || next.Year() != 2025 {
		t.Fatalf("unexpected yearly boundary: %v %v", next, err)
	}
	if _, err := buildPartitions(base, base.Add(time.Hour), "weird"); err == nil {
		t.Fatal("expected buildPartitions unsupported mode error")
	}
}
