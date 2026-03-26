package e2e

import (
	"strings"
	"testing"
)

func TestDownloadCanWriteCSVToStdout(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	output := runCLI(
		t,
		server.URL,
		"download",
		"--symbol", "xauusd",
		"--timeframe", "m1",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:03:00Z",
		"--output", "-",
		"--simple",
	)

	if !strings.HasPrefix(output, "timestamp,open,high,low,close,volume\n") {
		t.Fatalf("expected CSV header on stdout, got: %s", output)
	}
	if strings.Contains(output, "wrote 3 bars") {
		t.Fatalf("expected pure CSV stdout output without summary line, got: %s", output)
	}
	if !strings.Contains(output, "2024-01-02T00:02:00Z,101.250,102.000,100.750,101.500,800") {
		t.Fatalf("expected last CSV row on stdout, got: %s", output)
	}
}
