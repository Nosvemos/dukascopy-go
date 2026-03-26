package e2e

import (
	"strings"
	"testing"
)

func TestInstrumentsCommandE2E(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	output := runCLI(t, server.URL, "instruments", "--query", "xauusd", "--limit", "1")
	if !strings.Contains(output, "XAU/USD\tXAU-USD\tGold vs US Dollar") {
		t.Fatalf("unexpected instruments output: %s", output)
	}
}
