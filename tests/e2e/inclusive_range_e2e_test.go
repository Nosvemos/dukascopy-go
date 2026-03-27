package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDownloadIncludesInclusiveBarEndTimestamp(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-inclusive-bars.csv")
	output := runCLI(
		t,
		server.URL,
		"download",
		"--symbol", "xauusd",
		"--timeframe", "m1",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:02:00Z",
		"--output", outputPath,
		"--simple",
	)

	if !strings.Contains(output, "wrote 3 bars") {
		t.Fatalf("expected inclusive end bar count, got: %s", output)
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	if !strings.Contains(string(data), "2024-01-02T00:02:00Z,101.250,102.000,100.750,101.500,800") {
		t.Fatalf("expected inclusive end bar row, got: %s", string(data))
	}
}

func TestDownloadAllowsSingleInclusiveTimestamp(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-single-point.csv")
	output := runCLI(
		t,
		server.URL,
		"download",
		"--symbol", "xauusd",
		"--timeframe", "m1",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:00:00Z",
		"--output", outputPath,
		"--simple",
	)

	if !strings.Contains(output, "wrote 1 bars") {
		t.Fatalf("expected single inclusive bar, got: %s", output)
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "2024-01-02T00:00:00Z,100.000,101.000,99.000,100.500,1100") {
		t.Fatalf("expected exact inclusive timestamp row, got: %s", content)
	}
	if strings.Contains(content, "2024-01-02T00:01:00Z") {
		t.Fatalf("did not expect later rows in single inclusive timestamp range, got: %s", content)
	}
}

func TestDownloadIncludesInclusiveTickEndTimestamp(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-inclusive-ticks.csv")
	output := runCLI(
		t,
		server.URL,
		"download",
		"--symbol", "xauusd",
		"--timeframe", "tick",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:00:01Z",
		"--output", outputPath,
		"--simple",
	)

	if !strings.Contains(output, "wrote 3 ticks") {
		t.Fatalf("expected inclusive end tick count, got: %s", output)
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	if !strings.Contains(string(data), "2024-01-02T00:00:01Z,100.300,100.500") {
		t.Fatalf("expected inclusive end tick row, got: %s", string(data))
	}
}
