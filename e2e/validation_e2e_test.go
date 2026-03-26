package e2e

import (
	"strings"
	"testing"
)

func TestDownloadRejectsConflictingProfiles(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	output := runCLIExpectError(
		t,
		server.URL,
		"download",
		"--symbol", "xauusd",
		"--granularity", "minute",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:03:00Z",
		"--output", "ignored.csv",
		"--simple",
		"--full",
	)

	if !strings.Contains(output, "--simple and --full cannot be used together") {
		t.Fatalf("unexpected validation output: %s", output)
	}
}

func TestDownloadRejectsCustomColumnsWithProfileFlags(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	output := runCLIExpectError(
		t,
		server.URL,
		"download",
		"--symbol", "xauusd",
		"--timeframe", "m1",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:03:00Z",
		"--output", "ignored.csv",
		"--simple",
		"--custom-columns", "timestamp,open",
	)

	if !strings.Contains(output, "--custom-columns cannot be combined with --simple or --full") {
		t.Fatalf("unexpected validation output: %s", output)
	}
}

func TestDownloadRejectsResumeForParquetOutput(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	output := runCLIExpectError(
		t,
		server.URL,
		"download",
		"--symbol", "xauusd",
		"--timeframe", "m1",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:03:00Z",
		"--output", "ignored.parquet",
		"--simple",
		"--resume",
	)

	if !strings.Contains(output, "--resume is not supported for parquet output") {
		t.Fatalf("unexpected parquet resume validation output: %s", output)
	}
}
