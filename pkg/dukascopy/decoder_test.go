package dukascopy

import "testing"

func TestResolveInstrumentSupportsMultipleSymbols(t *testing.T) {
	instruments := []Instrument{
		{Name: "XAU/USD", Code: "XAU-USD", Description: "Gold vs US Dollar"},
		{Name: "EUR/USD", Code: "EUR-USD", Description: "Euro vs US Dollar"},
		{Name: "BTC/USD", Code: "BTC-USD", Description: "Bitcoin vs US Dollar"},
	}

	testCases := []struct {
		input string
		want  string
	}{
		{input: "xauusd", want: "XAU-USD"},
		{input: "eur/usd", want: "EUR-USD"},
		{input: "BTCUSD", want: "BTC-USD"},
	}

	for _, testCase := range testCases {
		got, err := ResolveInstrument(instruments, testCase.input)
		if err != nil {
			t.Fatalf("ResolveInstrument(%q) returned error: %v", testCase.input, err)
		}
		if got.Code != testCase.want {
			t.Fatalf("ResolveInstrument(%q) = %q, want %q", testCase.input, got.Code, testCase.want)
		}
	}
}
