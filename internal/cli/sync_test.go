package cli

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSafeSymbolFilename(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"EUR/USD", "eur_usd"},
		{"USD-JPY", "usd_jpy"},
		{"XAUUSD", "xauusd"},
		{"  GBP/USD  ", "gbp_usd"},
		{"BTC USD", "btc_usd"},
	}

	for _, tc := range tests {
		got := safeSymbolFilename(tc.input)
		if got != tc.expected {
			t.Errorf("safeSymbolFilename(%q) = %q; expected %q", tc.input, got, tc.expected)
		}
	}
}

func TestFormatMultiSymbolOutputPath(t *testing.T) {
	// Create a temp directory for testing directory-based outputs
	tempDir, err := os.MkdirTemp("", "sync_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	tests := []struct {
		outputPath string
		symbol     string
		expected   string
	}{
		// Case 1: Placeholder
		{"./data/ohlc-{symbol}.csv", "EUR/USD", "./data/ohlc-eur_usd.csv"},
		{"./data/{symbol}/data.parquet", "GBP/USD", "./data/gbp_usd/data.parquet"},

		// Case 3: Standard File Name injection
		{"./data/prices.csv", "EUR/USD", "./data/prices-eur_usd.csv"},
		{"./data/prices.csv.gz", "EUR/USD", "./data/prices-eur_usd.csv.gz"},
		{"ohlc.parquet", "XAUUSD", "ohlc-xauusd.parquet"},

		// Case 2: Directory (using our tempDir)
		{tempDir, "EUR/USD", filepath.Join(tempDir, "eur_usd.csv")},
		{tempDir + string(filepath.Separator), "GBP/USD", filepath.Join(tempDir, "gbp_usd.csv")},
	}

	for _, tc := range tests {
		got := formatMultiSymbolOutputPath(tc.outputPath, tc.symbol)
		// Clean paths to avoid slash mismatches on different OS (Windows vs Linux)
		gotClean := filepath.Clean(got)
		expectedClean := filepath.Clean(tc.expected)
		if gotClean != expectedClean {
			t.Errorf("formatMultiSymbolOutputPath(%q, %q) = %q; expected %q", tc.outputPath, tc.symbol, gotClean, expectedClean)
		}
	}
}
