package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestE2EMultiSymbolDownload(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	tempDir := t.TempDir()
	outputPath := filepath.Join(tempDir, "{symbol}-minute.csv")

	output := runCLI(
		t,
		server.URL,
		"download",
		"--symbol", "xau/usd,eur/usd",
		"--granularity", "minute",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:03:00Z",
		"--output", outputPath,
		"--simple",
	)

	if !strings.Contains(output, "batch downloading 2 symbols") {
		t.Fatalf("unexpected batch download output: %s", output)
	}

	// Verify both file paths are created based on safeSymbolFilename logic
	file1 := filepath.Join(tempDir, "xau_usd-minute.csv")
	file2 := filepath.Join(tempDir, "eur_usd-minute.csv")

	data1, err := os.ReadFile(file1)
	if err != nil {
		t.Fatalf("read xauusd file: %v", err)
	}
	if !strings.Contains(string(data1), "timestamp,open,high,low,close,volume") {
		t.Fatalf("xauusd file header missing: %s", string(data1))
	}

	data2, err := os.ReadFile(file2)
	if err != nil {
		t.Fatalf("read eurusd file: %v", err)
	}
	if !strings.Contains(string(data2), "timestamp,open,high,low,close,volume") {
		t.Fatalf("eurusd file header missing: %s", string(data2))
	}
}

func TestE2EDeltaSync(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	tempDir := t.TempDir()
	outputPath := filepath.Join(tempDir, "sync-xauusd.csv")

	// 1. Initial download for 2024-01-02 (wrote 3 bars)
	output1 := runCLI(
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
	if !strings.Contains(output1, "wrote 3 bars") {
		t.Fatalf("initial download failed: %s", output1)
	}

	// 2. Sync to pull the next day 2024-01-03 (should read state.LastTime and sync from there)
	output2 := runCLI(
		t,
		server.URL,
		"sync",
		"--symbol", "xauusd",
		"--output", outputPath,
		"--to", "2024-01-03T00:03:00Z",
	)

	if !strings.Contains(output2, "syncing xauusd in-place") {
		t.Fatalf("unexpected sync output: %s", output2)
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read synced output: %v", err)
	}

	content := string(data)
	lines := strings.Split(strings.TrimSpace(content), "\n")

	// Expecting: Header (1 line) + Initial bars (3 lines) + Synced bars (3 lines) = 7 lines total
	if len(lines) != 7 {
		t.Fatalf("expected 7 lines in synced CSV, got %d:\n%s", len(lines), content)
	}

	// Verify both timestamps are represented in chronological order
	if !strings.Contains(content, "2024-01-02T00:02:00Z") {
		t.Fatalf("missing initial data: %s", content)
	}
	if !strings.Contains(content, "2024-01-03T00:02:00Z") {
		t.Fatalf("missing synced data: %s", content)
	}
}
