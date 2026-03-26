package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDownloadResumeAppendsOnlyMissingBars(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-minute-simple.csv")
	initial := strings.Join([]string{
		"timestamp,open,high,low,close,volume",
		"2024-01-02T00:00:00Z,100.000,101.000,99.000,100.500,1100",
		"2024-01-02T00:01:00Z,100.500,101.250,99.500,100.750,900",
		"",
	}, "\n")
	if err := os.WriteFile(outputPath, []byte(initial), 0o644); err != nil {
		t.Fatalf("write initial csv: %v", err)
	}

	output := runCLI(
		t,
		server.URL,
		"download",
		"--symbol", "xauusd",
		"--timeframe", "m1",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:03:00Z",
		"--output", outputPath,
		"--simple",
		"--resume",
	)

	if !strings.Contains(output, "wrote 1 bars") {
		t.Fatalf("unexpected resume output: %s", output)
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read resumed file: %v", err)
	}

	content := string(data)
	if strings.Count(content, "2024-01-02T00:01:00Z") != 1 {
		t.Fatalf("resume duplicated an existing row: %s", content)
	}
	if !strings.Contains(content, "2024-01-02T00:02:00Z,101.250,102.000,100.750,101.500,800") {
		t.Fatalf("resume did not append the missing row: %s", content)
	}
}

func TestDownloadResumeRejectsHeaderMismatch(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-minute.csv")
	initial := "timestamp,open,high,low,close,volume\n2024-01-02T00:00:00Z,100.000,101.000,99.000,100.500,1100\n"
	if err := os.WriteFile(outputPath, []byte(initial), 0o644); err != nil {
		t.Fatalf("write initial csv: %v", err)
	}

	output := runCLIExpectError(
		t,
		server.URL,
		"download",
		"--symbol", "xauusd",
		"--timeframe", "m1",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:03:00Z",
		"--output", outputPath,
		"--full",
		"--resume",
	)

	if !strings.Contains(output, "existing CSV header does not match") {
		t.Fatalf("unexpected resume validation output: %s", output)
	}
}
