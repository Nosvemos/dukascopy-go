package e2e

import (
	"strings"
	"testing"
)

func TestInstrumentsCommandSupportsOtherDukascopySymbols(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	eurOutput := runCLI(t, server.URL, "instruments", "--query", "eurusd", "--limit", "1")
	if !strings.Contains(eurOutput, "EUR/USD\tEUR-USD\tEuro vs US Dollar") {
		t.Fatalf("unexpected EURUSD output: %s", eurOutput)
	}

	btcOutput := runCLI(t, server.URL, "instruments", "--query", "btcusd", "--limit", "1")
	if !strings.Contains(btcOutput, "BTC/USD\tBTC-USD\tBitcoin vs US Dollar") {
		t.Fatalf("unexpected BTCUSD output: %s", btcOutput)
	}
}
