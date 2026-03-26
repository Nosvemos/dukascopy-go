package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDownloadTickSimpleCSV(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-ticks-simple.csv")
	output := runCLI(
		t,
		server.URL,
		"download",
		"--symbol", "xauusd",
		"--granularity", "tick",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:00:02Z",
		"--output", outputPath,
		"--simple",
	)

	if !strings.Contains(output, "wrote 3 ticks") {
		t.Fatalf("unexpected download output: %s", output)
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, "timestamp,bid,ask") {
		t.Fatalf("missing simple tick header: %s", content)
	}
	if strings.Contains(content, "bid_volume") || strings.Contains(content, "ask_volume") {
		t.Fatalf("simple tick output should not include volume columns: %s", content)
	}
}

func TestDownloadTickFullCSV(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-ticks-full.csv")
	output := runCLI(
		t,
		server.URL,
		"download",
		"--symbol", "xauusd",
		"--granularity", "tick",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:00:02Z",
		"--output", outputPath,
		"--full",
	)

	if !strings.Contains(output, "wrote 3 ticks") {
		t.Fatalf("unexpected download output: %s", output)
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, "timestamp,bid,ask,bid_volume,ask_volume") {
		t.Fatalf("missing full tick header: %s", content)
	}
	if !strings.Contains(content, "2024-01-02T00:00:00Z,100.000,100.200,15,10") {
		t.Fatalf("missing expected full tick row: %s", content)
	}
}
