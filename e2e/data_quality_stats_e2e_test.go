package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStatsReportsGapsAndOutOfOrderRows(t *testing.T) {
	inputPath := filepath.Join(t.TempDir(), "quality.csv")
	content := strings.Join([]string{
		"timestamp,open,high,low,close,volume",
		"2024-01-02T00:00:00Z,1,1,1,1,1",
		"2024-01-02T00:01:00Z,1,1,1,1,1",
		"2024-01-02T00:04:00Z,1,1,1,1,1",
		"2024-01-02T00:03:00Z,1,1,1,1,1",
	}, "\n") + "\n"
	if err := os.WriteFile(inputPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write quality csv: %v", err)
	}

	output := runCLI(
		t,
		"",
		"stats",
		"--input", inputPath,
	)

	if !strings.Contains(output, "inferred frame:    m1") {
		t.Fatalf("expected inferred timeframe in stats output: %s", output)
	}
	if !strings.Contains(output, "gap count:         1") || !strings.Contains(output, "missing intervals: 2") {
		t.Fatalf("expected gap details in stats output: %s", output)
	}
	if !strings.Contains(output, "out of order:      1") {
		t.Fatalf("expected out-of-order details in stats output: %s", output)
	}
}
