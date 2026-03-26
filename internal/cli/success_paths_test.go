package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunCommandSuccessPaths(t *testing.T) {
	server := newCLITestServer()
	defer server.Close()
	t.Setenv("DUKASCOPY_API_BASE_URL", server.URL)
	t.Setenv("NO_COLOR", "1")

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	if code := Run([]string{"instruments", "--query", "xauusd"}, &stdout, &stderr); code != 0 {
		t.Fatalf("expected instruments command to succeed, got %d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "XAU-USD") {
		t.Fatalf("unexpected instruments output: %s", stdout.String())
	}

	stdout.Reset()
	if code := Run([]string{"stats", "--input", writeStatsFixture(t)}, &stdout, &stderr); code != 0 {
		t.Fatalf("expected stats command to succeed, got %d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "Stats") {
		t.Fatalf("unexpected stats output: %s", stdout.String())
	}

	stdout.Reset()
	if code := Run([]string{"manifest", "help"}, &stdout, &stderr); code != 0 {
		t.Fatalf("expected manifest help to succeed, got %d stderr=%s", code, stderr.String())
	}
}

func TestRunDownloadMoreSuccessPaths(t *testing.T) {
	server := newCLITestServer()
	defer server.Close()

	dir := t.TempDir()
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	if err := runDownload([]string{
		"--symbol", "xauusd",
		"--timeframe", "m1",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:02:00Z",
		"--output", "-",
		"--simple",
		"--base-url", server.URL,
	}, &stdout, &stderr); err != nil {
		t.Fatalf("stdout download failed: %v", err)
	}
	if !strings.Contains(stdout.String(), "timestamp,open") {
		t.Fatalf("unexpected stdout csv: %s", stdout.String())
	}

	fullPath := filepath.Join(dir, "full.csv")
	stdout.Reset()
	if err := runDownload([]string{
		"--symbol", "xauusd",
		"--timeframe", "m1",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:02:00Z",
		"--output", fullPath,
		"--full",
		"--base-url", server.URL,
	}, &stdout, &stderr); err != nil {
		t.Fatalf("full download failed: %v", err)
	}
	if !strings.Contains(stdout.String(), "wrote") {
		t.Fatalf("unexpected full download output: %s", stdout.String())
	}

	customPath := filepath.Join(dir, "custom.csv")
	stdout.Reset()
	if err := runDownload([]string{
		"--symbol", "xauusd",
		"--timeframe", "m1",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:02:00Z",
		"--output", customPath,
		"--custom-columns", "timestamp,mid_close",
		"--base-url", server.URL,
	}, &stdout, &stderr); err != nil {
		t.Fatalf("custom bar download failed: %v", err)
	}

	tickPath := filepath.Join(dir, "ticks.csv")
	stdout.Reset()
	if err := runDownload([]string{
		"--symbol", "xauusd",
		"--timeframe", "tick",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:00:01Z",
		"--output", tickPath,
		"--custom-columns", "timestamp,bid,ask",
		"--base-url", server.URL,
	}, &stdout, &stderr); err != nil {
		t.Fatalf("tick download failed: %v", err)
	}
	if !strings.Contains(stdout.String(), "ticks") {
		t.Fatalf("unexpected tick output: %s", stdout.String())
	}
}

func writeStatsFixture(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "stats.csv")
	if err := os.WriteFile(path, []byte("timestamp,open\n2024-01-01T00:00:00Z,1\n2024-01-01T00:01:00Z,2\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	return path
}
