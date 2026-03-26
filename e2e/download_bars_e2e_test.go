package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDownloadMinuteSimpleCSV(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-minute-simple.csv")
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
	if strings.Contains(content, "bid_open") || strings.Contains(content, "ask_open") {
		t.Fatalf("simple output should not include bid/ask columns: %s", content)
	}
	if !strings.Contains(content, "2024-01-02T00:02:00Z,101.250,102.000,100.750,101.500,0.8") {
		t.Fatalf("missing expected simple row: %s", content)
	}
}

func TestDownloadMinuteFullCSV(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-minute-full.csv")
	output := runCLI(
		t,
		server.URL,
		"download",
		"--symbol", "xauusd",
		"--granularity", "minute",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:03:00Z",
		"--output", outputPath,
		"--full",
	)

	if !strings.Contains(output, "wrote 3 bars") {
		t.Fatalf("unexpected download output: %s", output)
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, "timestamp,open,high,low,close,volume,bid_open,bid_high,bid_low,bid_close,ask_open,ask_high,ask_low,ask_close") {
		t.Fatalf("missing full header: %s", content)
	}
	if !strings.Contains(content, "2024-01-02T00:00:00Z,100.100,101.100,99.100,100.600,1.1,100.000,101.000,99.000,100.500,100.200,101.200,99.200,100.700") {
		t.Fatalf("missing expected full row: %s", content)
	}
}
