package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestManifestInspectPrintsSummary(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-inspect.csv")
	runCLI(
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

	output := runCLI(
		t,
		server.URL,
		"manifest", "inspect",
		"--output", outputPath,
	)

	if !strings.Contains(output, "Manifest") || !strings.Contains(output, "symbol:      xauusd") || !strings.Contains(output, "output rows: 6") {
		t.Fatalf("unexpected inspect output: %s", output)
	}
	if !strings.Contains(output, "Parts") || !strings.Contains(output, "completed") {
		t.Fatalf("expected parts table in inspect output: %s", output)
	}
}

func TestManifestVerifyDetectsTamperedFinalOutput(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-verify.csv")
	runCLI(
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

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	tampered := strings.Replace(string(data), "102.500", "102.501", 1)
	if err := os.WriteFile(outputPath, []byte(tampered), 0o644); err != nil {
		t.Fatalf("tamper output file: %v", err)
	}

	output := runCLIExpectError(
		t,
		server.URL,
		"manifest", "verify",
		"--output", outputPath,
	)

	if !strings.Contains(output, "final") || !strings.Contains(output, "invalid") || !strings.Contains(output, "sha256 mismatch") {
		t.Fatalf("unexpected verify output: %s", output)
	}
}
