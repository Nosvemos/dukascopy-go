package e2e

import (
	"encoding/json"
	"testing"
)

func TestInstrumentsJSONOutput(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	output := runCLI(
		t,
		server.URL,
		"instruments",
		"--query", "xauusd",
		"--json",
	)

	var instruments []map[string]any
	if err := json.Unmarshal([]byte(output), &instruments); err != nil {
		t.Fatalf("decode json output: %v\n%s", err, output)
	}
	if len(instruments) == 0 {
		t.Fatalf("expected at least one instrument: %s", output)
	}
	if instruments[0]["code"] != "XAU-USD" {
		t.Fatalf("unexpected first instrument: %s", output)
	}
}
