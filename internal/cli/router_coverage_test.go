package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunRouterErrorAndSuccessCoverage(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	server := newCLITestServer()
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	if code := Run([]string{"instruments", "--query", "xauusd", "--json", "--base-url", server.URL}, &stdout, &stderr); code != 0 {
		t.Fatalf("expected instruments json command to succeed, got %d", code)
	}
	if !strings.Contains(stdout.String(), "\"code\": \"XAU-USD\"") {
		t.Fatalf("unexpected instruments json output: %s", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	if code := Run([]string{"stats"}, &stdout, &stderr); code != 1 {
		t.Fatalf("expected stats error code 1, got %d", code)
	}
	if !strings.Contains(stderr.String(), "error:") {
		t.Fatalf("unexpected stats stderr: %s", stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	if code := Run([]string{"manifest"}, &stdout, &stderr); code != 1 {
		t.Fatalf("expected manifest error code 1, got %d", code)
	}
	if !strings.Contains(stderr.String(), "manifest subcommand is required") {
		t.Fatalf("unexpected manifest stderr: %s", stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	if code := Run([]string{"download", "--symbol", "xauusd"}, &stdout, &stderr); code != 1 {
		t.Fatalf("expected download validation error code 1, got %d", code)
	}
	if !strings.Contains(stderr.String(), "--from is required") {
		t.Fatalf("unexpected download stderr: %s", stderr.String())
	}

	dir := t.TempDir()
	statsPath := filepath.Join(dir, "stats.csv")
	if err := os.WriteFile(statsPath, []byte("timestamp,open\n2024-01-01T00:00:00Z,1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	stdout.Reset()
	stderr.Reset()
	if code := Run([]string{"stats", "--input", statsPath}, &stdout, &stderr); code != 0 {
		t.Fatalf("expected stats success code 0, got %d", code)
	}
	if !strings.Contains(stdout.String(), "rows:") {
		t.Fatalf("unexpected stats output: %s", stdout.String())
	}
}
