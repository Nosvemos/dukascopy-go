package e2e

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStatsCommandPrintsCSVSummary(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-stats.csv")
	runCLI(
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

	output := runCLI(
		t,
		server.URL,
		"stats",
		"--input", outputPath,
	)

	if !strings.Contains(output, "rows:              3") || !strings.Contains(output, "inferred frame:    m1") {
		t.Fatalf("unexpected stats output: %s", output)
	}
}

func TestStatsCommandSupportsJSONAndGzip(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-stats.csv.gz")
	runCLI(
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

	if _, err := os.Stat(outputPath); err != nil {
		t.Fatalf("expected gzip output file: %v", err)
	}

	output := runCLI(
		t,
		server.URL,
		"stats",
		"--input", outputPath,
		"--json",
	)

	var stats struct {
		Compressed        bool   `json:"Compressed"`
		Rows              int    `json:"Rows"`
		InferredTimeframe string `json:"InferredTimeframe"`
	}
	if err := json.Unmarshal([]byte(output), &stats); err != nil {
		t.Fatalf("decode stats json: %v\n%s", err, output)
	}
	if !stats.Compressed || stats.Rows != 3 || stats.InferredTimeframe != "m1" {
		t.Fatalf("unexpected stats json: %s", output)
	}
}
