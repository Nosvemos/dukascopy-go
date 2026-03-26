package e2e

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestManifestPruneRemovesOrphansAndTemps(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-prune.csv")
	manifestPath := outputPath + ".manifest.json"

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

	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var manifest struct {
		Parts []struct {
			File string `json:"file"`
		} `json:"parts"`
	}
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if len(manifest.Parts) == 0 {
		t.Fatalf("expected manifest parts: %s", string(manifestData))
	}

	partsDir := outputPath + ".parts"
	keptPart := filepath.Join(partsDir, manifest.Parts[0].File)
	orphanPart := filepath.Join(partsDir, "orphan.csv")
	partTemp := filepath.Join(partsDir, "orphan.tmp-junk.csv")
	manifestTemp := manifestPath + ".tmp-junk"
	outputTemp := outputPath + ".resume-junk.csv"

	for _, file := range []string{orphanPart, partTemp, manifestTemp, outputTemp} {
		if err := os.WriteFile(file, []byte("junk\n"), 0o644); err != nil {
			t.Fatalf("write temp file %s: %v", file, err)
		}
	}

	output := runCLI(
		t,
		server.URL,
		"manifest", "prune",
		"--output", outputPath,
	)
	if !strings.Contains(output, "removed 4 obsolete file(s)") {
		t.Fatalf("unexpected prune output: %s", output)
	}

	if _, err := os.Stat(keptPart); err != nil {
		t.Fatalf("expected referenced part to remain: %v", err)
	}
	for _, file := range []string{orphanPart, partTemp, manifestTemp, outputTemp} {
		if _, err := os.Stat(file); !os.IsNotExist(err) {
			t.Fatalf("expected file to be pruned: %s", file)
		}
	}
}
