package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPartitionedDownloadCanAssembleParquetOutput(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-partitioned.parquet")
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
	)

	if !strings.Contains(output, "wrote 7 bars") {
		t.Fatalf("unexpected partitioned parquet output: %s", output)
	}
	if _, err := os.Stat(outputPath); err != nil {
		t.Fatalf("expected final parquet output file: %v", err)
	}

	verifyOutput := runCLI(
		t,
		server.URL,
		"manifest", "verify",
		"--output", outputPath,
	)
	if !strings.Contains(verifyOutput, "verified") {
		t.Fatalf("expected manifest verification to pass for parquet output: %s", verifyOutput)
	}
}
