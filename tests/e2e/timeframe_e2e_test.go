package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDownloadSupportsTimeframeAliasM1(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-m1.csv")
	output := runCLI(
		t,
		server.URL,
		"download",
		"--symbol", "xauusd",
		"--timeframe", "m1",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:03:00Z",
		"--output", outputPath,
		"--simple",
	)

	if !strings.Contains(output, "wrote 3 bars") {
		t.Fatalf("unexpected output: %s", output)
	}
}

func TestDownloadSupportsTimeframeAliasH1(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-h1.csv")
	output := runCLI(
		t,
		server.URL,
		"download",
		"--symbol", "xauusd",
		"--timeframe", "h1",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T02:00:00Z",
		"--output", outputPath,
		"--simple",
	)

	if !strings.Contains(output, "wrote 2 bars") {
		t.Fatalf("unexpected output: %s", output)
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, "2024-01-02T01:00:00Z,101.000,102.000,100.000,101.500,3000") {
		t.Fatalf("unexpected h1 csv content: %s", content)
	}
}

func TestDownloadSupportsAggregatedTimeframeM5(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-m5.csv")
	output := runCLI(
		t,
		server.URL,
		"download",
		"--symbol", "xauusd",
		"--timeframe", "m5",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:03:00Z",
		"--output", outputPath,
		"--simple",
	)

	if !strings.Contains(output, "wrote 1 bars") {
		t.Fatalf("unexpected output: %s", output)
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, "2024-01-02T00:00:00Z,100.000,102.000,99.000,101.500,2800") {
		t.Fatalf("unexpected m5 csv content: %s", content)
	}
}

func TestDownloadSupportsAggregatedTimeframeM3(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-m3.csv")
	output := runCLI(
		t,
		server.URL,
		"download",
		"--symbol", "xauusd",
		"--timeframe", "m3",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:03:00Z",
		"--output", outputPath,
		"--simple",
	)

	if !strings.Contains(output, "wrote 1 bars") {
		t.Fatalf("unexpected output: %s", output)
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, "2024-01-02T00:00:00Z,100.000,102.000,99.000,101.500,2800") {
		t.Fatalf("unexpected m3 csv content: %s", content)
	}
}

func TestDownloadSupportsAggregatedTimeframeH4(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-h4.csv")
	output := runCLI(
		t,
		server.URL,
		"download",
		"--symbol", "xauusd",
		"--timeframe", "h4",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T04:00:00Z",
		"--output", outputPath,
		"--simple",
	)

	if !strings.Contains(output, "wrote 1 bars") {
		t.Fatalf("unexpected output: %s", output)
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, "2024-01-02T00:00:00Z,100.000,104.000,99.000,103.500,14000") {
		t.Fatalf("unexpected h4 csv content: %s", content)
	}
}

func TestDownloadSupportsAggregatedTimeframeW1(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-w1.csv")
	output := runCLI(
		t,
		server.URL,
		"download",
		"--symbol", "xauusd",
		"--timeframe", "w1",
		"--from", "2024-01-01T00:00:00Z",
		"--to", "2024-01-04T00:00:00Z",
		"--output", outputPath,
		"--simple",
	)

	if !strings.Contains(output, "wrote 1 bars") {
		t.Fatalf("unexpected output: %s", output)
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, "2024-01-01T00:00:00Z,100.000,103.000,99.000,102.500,9000") {
		t.Fatalf("unexpected w1 csv content: %s", content)
	}
}

func TestDownloadSupportsAggregatedTimeframeMN1(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-mn1.csv")
	output := runCLI(
		t,
		server.URL,
		"download",
		"--symbol", "xauusd",
		"--timeframe", "mn1",
		"--from", "2024-01-01T00:00:00Z",
		"--to", "2024-01-04T00:00:00Z",
		"--output", outputPath,
		"--simple",
	)

	if !strings.Contains(output, "wrote 1 bars") {
		t.Fatalf("unexpected output: %s", output)
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, "2024-01-01T00:00:00Z,100.000,103.000,99.000,102.500,9000") {
		t.Fatalf("unexpected mn1 csv content: %s", content)
	}
}
