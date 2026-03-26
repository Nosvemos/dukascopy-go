package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDownloadCustomBarColumns(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-custom-bars.csv")
	output := runCLI(
		t,
		server.URL,
		"download",
		"--symbol", "xauusd",
		"--timeframe", "m1",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:03:00Z",
		"--output", outputPath,
		"--custom-columns", "timestamp,bid_open,ask_open,volume",
	)

	if !strings.Contains(output, "wrote 3 bars") {
		t.Fatalf("unexpected download output: %s", output)
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, "timestamp,bid_open,ask_open,volume") {
		t.Fatalf("missing custom header: %s", content)
	}
	if !strings.Contains(content, "2024-01-02T00:00:00Z,100.000,100.200,1100") {
		t.Fatalf("missing custom row: %s", content)
	}
}

func TestDownloadCustomTickColumns(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-custom-ticks.csv")
	output := runCLI(
		t,
		server.URL,
		"download",
		"--symbol", "xauusd",
		"--timeframe", "tick",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:00:02Z",
		"--output", outputPath,
		"--custom-columns", "timestamp,bid,ask_volume",
	)

	if !strings.Contains(output, "wrote 3 ticks") {
		t.Fatalf("unexpected download output: %s", output)
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, "timestamp,bid,ask_volume") {
		t.Fatalf("missing custom tick header: %s", content)
	}
	if !strings.Contains(content, "2024-01-02T00:00:00Z,100.000,10") {
		t.Fatalf("missing custom tick row: %s", content)
	}
}
