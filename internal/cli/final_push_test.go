package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunAndManifestAdditionalBranches(t *testing.T) {
	t.Setenv("NO_COLOR", "1")

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if code := Run(nil, &stdout, &stderr); code != 2 {
		t.Fatalf("expected empty Run args to return 2, got %d", code)
	}

	badConfig := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(badConfig, []byte("{"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	stdout.Reset()
	stderr.Reset()
	if code := Run([]string{"--config", badConfig, "version"}, &stdout, &stderr); code != 1 {
		t.Fatalf("expected invalid config to return 1, got %d", code)
	}
	if !strings.Contains(stderr.String(), "error:") {
		t.Fatalf("unexpected config stderr: %s", stderr.String())
	}

	stdout.Reset()
	if err := runManifest(nil, &stdout); err == nil {
		t.Fatal("expected empty manifest args error")
	}
	if !strings.Contains(stdout.String(), "manifest commands") {
		t.Fatalf("unexpected manifest usage output: %s", stdout.String())
	}

	if path, err := resolveManifestPath("custom.json", ""); err != nil || path != "custom.json" {
		t.Fatalf("unexpected manifest-only resolution: %q %v", path, err)
	}
	if _, err := resolveManifestPath("", ""); err == nil {
		t.Fatal("expected missing manifest/output error")
	}
	if filepathBase("") != "" {
		t.Fatal("expected empty filepathBase result for empty input")
	}
	if !shouldPruneTopLevelFile("dataset.csv.resume-1.csv", "dataset.csv.manifest.json", "dataset.csv") {
		t.Fatal("expected resume temp file to be pruned")
	}
}

func TestRunDownloadStdoutBranches(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	server := newCLITestServer()
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runDownload([]string{
		"--symbol", "xauusd",
		"--timeframe", "tick",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:01:00Z",
		"--output", "-",
		"--base-url", server.URL,
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("runDownload tick stdout returned error: %v", err)
	}
	if !strings.Contains(stdout.String(), "timestamp,bid,ask") {
		t.Fatalf("unexpected tick stdout output: %s", stdout.String())
	}

	stdout.Reset()
	err = runDownload([]string{
		"--symbol", "xauusd",
		"--timeframe", "m1",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:02:00Z",
		"--output", "-",
		"--full",
		"--base-url", server.URL,
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("runDownload bar stdout returned error: %v", err)
	}
	if !strings.Contains(stdout.String(), "mid_open") || !strings.Contains(stdout.String(), "bid_open") {
		t.Fatalf("unexpected bar stdout output: %s", stdout.String())
	}
}
