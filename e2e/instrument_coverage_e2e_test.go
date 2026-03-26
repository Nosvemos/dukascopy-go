package e2e

import (
	"strings"
	"testing"
)

func TestInstrumentsCommandSupportsOtherDukascopySymbols(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	eurOutput := runCLI(t, server.URL, "instruments", "--query", "eurusd", "--limit", "1")
	if !strings.Contains(eurOutput, "EUR/USD") || !strings.Contains(eurOutput, "EUR-USD") || !strings.Contains(eurOutput, "Euro vs US Dollar") {
		t.Fatalf("unexpected EURUSD output: %s", eurOutput)
	}

	btcOutput := runCLI(t, server.URL, "instruments", "--query", "btcusd", "--limit", "1")
	if !strings.Contains(btcOutput, "BTC/USD") || !strings.Contains(btcOutput, "BTC-USD") || !strings.Contains(btcOutput, "Bitcoin vs US Dollar") {
		t.Fatalf("unexpected BTCUSD output: %s", btcOutput)
	}
}
