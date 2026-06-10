package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDownloadWithGapFillingE2E(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-minute-gaps.csv")
	output := runCLI(
		t,
		server.URL,
		"download",
		"--symbol", "xauusd",
		"--granularity", "minute",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:03:00Z",
		"--output", outputPath,
		"--simple",
		"--fill-gaps", "forward",
	)

	if !strings.Contains(output, "wrote 3 bars") {
		t.Fatalf("unexpected download output: %s", output)
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, "timestamp,open,high,low,close,volume") {
		t.Fatalf("missing simple header: %s", content)
	}

	// Check that we can run with gap-filling without errors.
	if !strings.Contains(content, "2024-01-02T00:02:00Z") {
		t.Fatalf("missing expected simple row with gap-filling: %s", content)
	}
}
