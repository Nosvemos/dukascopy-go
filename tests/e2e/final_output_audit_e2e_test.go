package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPartitionedDownloadReassemblesTamperedFinalOutput(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-final-audit.csv")
	manifestPath := outputPath + ".checkpoint.json"

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
		"--checkpoint-manifest", manifestPath,
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
		"download",
		"--symbol", "xauusd",
		"--timeframe", "m1",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-04T00:00:00Z",
		"--output", outputPath,
		"--simple",
		"--partition", "auto",
		"--checkpoint-manifest", manifestPath,
	)
	if !strings.Contains(output, "wrote 6 bars") {
		t.Fatalf("unexpected output: %s", output)
	}

	finalData, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read repaired output: %v", err)
	}
	if strings.Contains(string(finalData), "102.501") {
		t.Fatalf("expected final output to be reassembled from valid partitions: %s", string(finalData))
	}
}
