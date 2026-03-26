package e2e

import (
	"strings"
	"testing"
)

func TestInstrumentsCommandE2E(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	output := runCLI(t, server.URL, "instruments", "--query", "xauusd", "--limit", "1")
	if !strings.Contains(output, "NAME") || !strings.Contains(output, "CODE") || !strings.Contains(output, "DESCRIPTION") {
		t.Fatalf("missing table header: %s", output)
	}
	if !strings.Contains(output, "XAU/USD") || !strings.Contains(output, "XAU-USD") || !strings.Contains(output, "Gold vs US Dollar") {
		t.Fatalf("unexpected instruments output: %s", output)
	}
}
