package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-data-downloader/internal/checkpoint"
	"github.com/Nosvemos/dukascopy-data-downloader/internal/csvout"
)

func TestManifestVerifyCanCheckDataQuality(t *testing.T) {
	outputPath := filepath.Join(t.TempDir(), "quality.csv")
	content := strings.Join([]string{
		"timestamp,open,high,low,close,volume",
		"2024-01-02T00:00:00Z,1,1,1,1,1",
		"2024-01-02T00:01:00Z,1,1,1,1,1",
		"2024-01-02T00:01:00Z,2,2,2,2,2",
	}, "\n") + "\n"
	if err := os.WriteFile(outputPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write output csv: %v", err)
	}

	audit, err := csvout.AuditCSV(outputPath)
	if err != nil {
		t.Fatalf("audit output csv: %v", err)
	}

	manifest := checkpoint.Manifest{
		Version:    checkpoint.CurrentManifestVersion,
		OutputPath: outputPath,
		PartsDir:   outputPath + ".parts",
		Symbol:     "xauusd",
		Timeframe:  "m1",
		Side:       "BID",
		ResultKind: "bar",
		Columns:    []string{"timestamp", "open", "high", "low", "close", "volume"},
		Partition:  "none",
		CreatedAt:  time.Now().UTC(),
		Completed:  true,
		FinalOutput: &checkpoint.ManifestOutput{
			Rows:      audit.Rows,
			Bytes:     audit.Bytes,
			SHA256:    audit.SHA256,
			UpdatedAt: time.Now().UTC(),
		},
	}
	manifestPath := checkpoint.DefaultManifestPath(outputPath)
	if err := checkpoint.Save(manifestPath, manifest); err != nil {
		t.Fatalf("save manifest: %v", err)
	}

	output := runCLIExpectError(
		t,
		"",
		"manifest", "verify",
		"--manifest", manifestPath,
		"--check-data-quality",
	)

	if !strings.Contains(output, "quality invalid") || !strings.Contains(output, "duplicate timestamps detected: 1") {
		t.Fatalf("expected quality failure in verify output, got: %s", output)
	}
}
