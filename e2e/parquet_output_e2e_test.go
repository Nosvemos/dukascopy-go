package e2e

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDownloadCanWriteParquetAndStatsCanInspectIt(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd.parquet")
	output := runCLI(
		t,
		server.URL,
		"download",
		"--symbol", "xauusd",
		"--timeframe", "m1",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:03:00Z",
		"--output", outputPath,
		"--full",
	)

	if !strings.Contains(output, "wrote 3 bars") {
		t.Fatalf("unexpected parquet download output: %s", output)
	}
	if _, err := os.Stat(outputPath); err != nil {
		t.Fatalf("expected parquet output file: %v", err)
	}

	statsOutput := runCLI(
		t,
		server.URL,
		"stats",
		"--input", outputPath,
		"--json",
	)

	var stats struct {
		Format            string   `json:"Format"`
		Rows              int      `json:"Rows"`
		Columns           []string `json:"Columns"`
		InferredTimeframe string   `json:"InferredTimeframe"`
	}
	if err := json.Unmarshal([]byte(statsOutput), &stats); err != nil {
		t.Fatalf("decode parquet stats json: %v\n%s", err, statsOutput)
	}
	if stats.Format != "parquet" || stats.Rows != 3 || stats.InferredTimeframe != "m1" {
		t.Fatalf("unexpected parquet stats json: %s", statsOutput)
	}
	if len(stats.Columns) == 0 || stats.Columns[0] != "timestamp" {
		t.Fatalf("expected parquet columns in metadata order, got: %s", statsOutput)
	}
}
