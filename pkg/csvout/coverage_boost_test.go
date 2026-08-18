package csvout

import (
	"bytes"
	"encoding/csv"
	"path/filepath"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

func TestCoverageBoostJSONLAndWriters(t *testing.T) {
	now := time.Now().UTC()
	inst := dukascopy.Instrument{Name: "EURUSD", PriceScale: 5}
	bars := []dukascopy.Bar{
		{Time: now, Open: 1.08, High: 1.09, Low: 1.07, Close: 1.085, Volume: 1000},
		{Time: now.Add(time.Minute), Open: 1.085, High: 1.095, Low: 1.08, Close: 1.09, Volume: 1500},
	}
	ticks := []dukascopy.Tick{
		{Time: now, Bid: 1.08, Ask: 1.0802, BidVolume: 100, AskVolume: 100},
		{Time: now.Add(time.Second), Bid: 1.0805, Ask: 1.0807, BidVolume: 200, AskVolume: 200},
	}

	barCols := []string{"timestamp", "open", "high", "low", "close", "volume"}
	tickCols := []string{"timestamp", "bid", "ask", "bid_volume", "ask_volume"}

	// JSONL writers to buffer
	var barBuf bytes.Buffer
	if err := WriteBarsJSONLToWriter(&barBuf, inst, barCols, bars, nil, nil); err != nil {
		t.Fatalf("WriteBarsJSONLToWriter failed: %v", err)
	}

	var tickBuf bytes.Buffer
	if err := WriteTicksJSONLToWriter(&tickBuf, inst, tickCols, ticks); err != nil {
		t.Fatalf("WriteTicksJSONLToWriter failed: %v", err)
	}

	// JSONL file writers
	dir := t.TempDir()
	barFile := filepath.Join(dir, "bars.jsonl")
	if err := writeBarsJSONL(barFile, inst, barCols, bars, nil, nil); err != nil {
		t.Fatalf("writeBarsJSONL failed: %v", err)
	}

	tickFile := filepath.Join(dir, "ticks.jsonl")
	if err := writeTicksJSONL(tickFile, inst, tickCols, ticks); err != nil {
		t.Fatalf("writeTicksJSONL failed: %v", err)
	}

	// JSONL with Bid/Ask fused columns
	fusedCols := []string{"timestamp", "bid_open", "bid_close", "ask_open", "ask_close"}
	var fusedBuf bytes.Buffer
	if err := WriteBarsJSONLToWriter(&fusedBuf, inst, fusedCols, nil, bars, bars); err != nil {
		t.Fatalf("WriteBarsJSONLToWriter fused failed: %v", err)
	}

	// IsJSONLPath
	if !IsJSONLPath("data.jsonl") || !IsJSONLPath("data.json") {
		t.Errorf("expected true for jsonl/json")
	}
	if IsJSONLPath("data.csv") {
		t.Errorf("expected false for csv")
	}

	// CSV tick rows
	var csvBuf bytes.Buffer
	csvWriter := csv.NewWriter(&csvBuf)
	if err := writeTicksCSVRows(csvWriter, inst, tickCols, ticks, false); err != nil {
		t.Fatalf("writeTicksCSVRows failed: %v", err)
	}
}

func TestCoverageBoostMarketHolidayAndProfiles(t *testing.T) {
	// Easter Sunday and Good Friday
	g1 := goodFriday(2024, time.UTC)
	if g1.Month() != time.March || g1.Day() != 29 {
		t.Errorf("unexpected Good Friday 2024: %v", g1)
	}

	// nth weekday of month
	mlk := nthWeekdayOfMonth(2024, time.January, time.Monday, 3, time.UTC)
	if mlk.Day() != 15 {
		t.Errorf("unexpected MLK Day 2024: %v", mlk)
	}

	// last weekday of month
	memDay := lastWeekdayOfMonth(2024, time.May, time.Monday, time.UTC)
	if memDay.Day() != 27 {
		t.Errorf("unexpected Memorial Day 2024: %v", memDay)
	}

	// FX closure boundaries
	fridayClose := time.Date(2024, 1, 5, 22, 0, 0, 0, time.UTC) // Friday 22:00 UTC
	if !isLikelyFXMarketClosed(fridayClose) {
		t.Errorf("expected FX closed on Friday night")
	}

	nextOpen := nextLikelyFXClosureBoundary(fridayClose)
	if nextOpen.Weekday() != time.Saturday {
		t.Errorf("expected next boundary, got %v", nextOpen)
	}

	// Equity closures
	locNY, _ := time.LoadLocation("America/New_York")
	equityNight := time.Date(2024, 1, 2, 22, 0, 0, 0, locNY)
	if !isLikelyEquityMarketClosed(equityNight) {
		t.Errorf("expected equity closed at 22:00 NY")
	}
	nextEq := nextLikelyEquityClosureBoundary(equityNight)
	if nextEq.IsZero() {
		t.Errorf("expected next equity boundary")
	}
}
