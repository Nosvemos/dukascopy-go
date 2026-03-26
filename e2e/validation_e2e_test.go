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
