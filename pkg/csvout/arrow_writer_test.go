package csvout

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

func TestIsArrowPath(t *testing.T) {
	testCases := []struct {
		input string
		want  bool
	}{
		{"test.arrow", true},
		{"test.ipc", true},
		{"test.feather", true},
		{"test.csv", false},
		{"test.parquet", false},
	}
	for _, tc := range testCases {
		if got := isArrowPath(tc.input); got != tc.want {
			t.Errorf("isArrowPath(%q) = %v, want %v", tc.input, got, tc.want)
		}
	}
}

func TestWriteBarsAndTicksArrow(t *testing.T) {
	dir := t.TempDir()
	outputPath := filepath.Join(dir, "bars.arrow")

	instrument := dukascopy.Instrument{
		ID:         1,
		Name:       "EUR/USD",
		Code:       "EURUSD",
		PriceScale: 5,
	}
	columns := []string{"timestamp", "open", "high", "low", "close", "volume"}
	bars := []dukascopy.Bar{
		{
			Time:   time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			Open:   1.1000,
			High:   1.1010,
			Low:    1.0990,
			Close:  1.1005,
			Volume: 123.45,
		},
	}

	err := WriteBars(outputPath, instrument, columns, bars, nil, nil)
	if err != nil {
		t.Fatalf("WriteBars failed: %v", err)
	}

	// Verify file exists and has size > 0
	info, err := os.Stat(outputPath)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("expected non-empty output file")
	}

	// Test Ticks
	tickPath := filepath.Join(dir, "ticks.arrow")
	tickColumns := []string{"timestamp", "bid", "ask", "bid_volume", "ask_volume"}
	ticks := []dukascopy.Tick{
		{
			Time:      time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			Bid:       1.1000,
			Ask:       1.1002,
			BidVolume: 1.5,
			AskVolume: 2.5,
		},
	}
	err = WriteTicks(tickPath, instrument, tickColumns, ticks)
	if err != nil {
		t.Fatalf("WriteTicks failed: %v", err)
	}
	info, err = os.Stat(tickPath)
	if err != nil {
		t.Fatalf("failed to stat tick file: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("expected non-empty tick output file")
	}
}

func TestArrowStreamWriter(t *testing.T) {
	dir := t.TempDir()
	outputPath := filepath.Join(dir, "stream.arrow")
	columns := []string{"timestamp", "open", "close"}

	writer, err := CreateArrowStreamWriter(outputPath, columns)
	if err != nil {
		t.Fatalf("CreateArrowStreamWriter failed: %v", err)
	}

	batch := []map[string]any{
		{"timestamp": "2024-01-02T00:00:00Z", "open": 1.100, "close": 1.105},
		{"timestamp": "2024-01-02T00:01:00Z", "open": 1.105, "close": 1.102},
	}

	if err := writer.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	info, err := os.Stat(outputPath)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("expected non-empty output file")
	}
}
