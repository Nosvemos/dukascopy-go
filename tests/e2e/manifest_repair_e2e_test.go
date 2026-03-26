package e2e

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestManifestRepairRebuildsFinalOutput(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-repair-final.csv")
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

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read final output: %v", err)
	}
	tampered := strings.Replace(string(data), "102.500", "102.501", 1)
	if err := os.WriteFile(outputPath, []byte(tampered), 0o644); err != nil {
		t.Fatalf("tamper final output: %v", err)
	}

	output := runCLI(
		t,
		server.URL,
		"manifest", "repair",
		"--output", outputPath,
	)
	if !strings.Contains(output, "rebuilt final output") {
		t.Fatalf("unexpected repair output: %s", output)
	}

	repaired, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read repaired output: %v", err)
	}
	if strings.Contains(string(repaired), "102.501") {
		t.Fatalf("expected repaired final output, got: %s", string(repaired))
	}
}

func TestManifestRepairRebuildsMissingPartFromFinalOutput(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-repair-part.csv")
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

	partPath := filepath.Join(outputPath+".parts", manifest.Parts[0].File)
	if err := os.Remove(partPath); err != nil {
		t.Fatalf("remove part file: %v", err)
	}

	output := runCLI(
		t,
		server.URL,
		"manifest", "repair",
		"--manifest", manifestPath,
	)
	if !strings.Contains(output, "repaired 1 part(s)") {
		t.Fatalf("unexpected repair output: %s", output)
	}

	if _, err := os.Stat(partPath); err != nil {
		t.Fatalf("expected missing part to be rebuilt: %v", err)
	}

	verifyOutput := runCLI(
		t,
		server.URL,
		"manifest", "verify",
		"--manifest", manifestPath,
	)
	if !strings.Contains(verifyOutput, "verified manifest is consistent") {
		t.Fatalf("expected verify to succeed after repair: %s", verifyOutput)
	}
}
