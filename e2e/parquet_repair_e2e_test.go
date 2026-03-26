package e2e

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestManifestRepairCanRestoreMissingCSVPartFromParquetOutput(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-repair.parquet")
	runCLI(
		t,
		server.URL,
		"download",
		"--symbol", "xauusd",
		"--timeframe", "m1",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-04T00:00:00Z",
		"--output", outputPath,
		"--simple",
		"--partition", "auto",
	)

	manifestPath := outputPath + ".manifest.json"
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}

	var manifest struct {
		PartsDir string `json:"parts_dir"`
		Parts    []struct {
			File string `json:"file"`
		} `json:"parts"`
	}
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if len(manifest.Parts) == 0 {
		t.Fatalf("expected manifest parts: %s", string(manifestData))
	}

	partPath := filepath.Join(manifest.PartsDir, manifest.Parts[0].File)
	if err := os.Remove(partPath); err != nil {
		t.Fatalf("remove partition part: %v", err)
	}

	repairOutput := runCLI(
		t,
		server.URL,
		"manifest", "repair",
		"--output", outputPath,
	)
	if _, err := os.Stat(partPath); err != nil {
		t.Fatalf("expected missing part to be restored from parquet output: %v", err)
	}
	if len(repairOutput) == 0 {
		t.Fatal("expected repair command to print a result")
	}
}
