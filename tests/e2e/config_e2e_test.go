package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConfigFileCanProvideCommandDefaults(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	configPath := filepath.Join(t.TempDir(), "dukascopy.json")
	config := `{
  "base_url": "` + server.URL + `",
  "instruments": {
    "limit": 1
  },
  "download": {
    "timeframe": "m1",
    "simple": true,
    "retries": 0
  }
}`
	if err := os.WriteFile(configPath, []byte(config), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	instrumentOutput := runCLI(
		t,
		"",
		"--config", configPath,
		"instruments",
		"--query", "usd",
	)
	if strings.Count(instrumentOutput, "USD") > 3 {
		t.Fatalf("expected config limit to reduce instrument output, got: %s", instrumentOutput)
	}

	outputPath := filepath.Join(t.TempDir(), "config-download.csv")
	downloadOutput := runCLI(
		t,
		"",
		"download",
		"--config", configPath,
		"--symbol", "xauusd",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:03:00Z",
		"--output", outputPath,
	)
	if !strings.Contains(downloadOutput, "wrote 3 bars") {
		t.Fatalf("unexpected download output with config defaults: %s", downloadOutput)
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	if !strings.HasPrefix(string(data), "timestamp,open,high,low,close,volume\n") {
		t.Fatalf("expected simple schema from config defaults, got: %s", string(data))
	}
}
