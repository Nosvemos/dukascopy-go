package dukascopy

import (
	"testing"
	"time"
)

func TestFilterInstrumentsAndScoringHelpers(t *testing.T) {
	instruments := []Instrument{
		{Name: "XAU/USD", Code: "XAU-USD", Description: "Gold vs US Dollar"},
		{Name: "EUR/USD", Code: "EUR-USD", Description: "Euro vs US Dollar"},
		{Name: "BTC/USD", Code: "BTC-USD", Description: "Bitcoin vs US Dollar"},
	}

	filtered := FilterInstruments(instruments, "usd", 2)
	if len(filtered) != 2 {
		t.Fatalf("expected 2 instruments, got %d", len(filtered))
	}
	if filtered[0].Code != "BTC-USD" && filtered[0].Code != "EUR-USD" && filtered[0].Code != "XAU-USD" {
		t.Fatalf("unexpected instrument ordering: %+v", filtered)
	}

	if score := scoreInstrument(instruments[0], "xauusd", "XAUUSD"); score <= 0 {
		t.Fatalf("expected positive score, got %d", score)
	}
}

func TestNormalizeGranularityAndSide(t *testing.T) {
	if got := normalizeGranularity("minute"); got != GranularityM1 {
		t.Fatalf("expected minute -> m1, got %q", got)
	}
	if got := normalizeGranularity("monthly"); got != GranularityMN1 {
		t.Fatalf("expected monthly -> mn1, got %q", got)
	}
	if _, err := normalizeSide("weird"); err == nil {
		t.Fatal("expected invalid side error")
	}
}

func TestDecodeAndFilterHelpers(t *testing.T) {
	bars := decodeBars(candlePayload{
		Timestamp:  1704153600000,
		Multiplier: 0.1,
		Open:       100,
		High:       101,
		Low:        99,
		Close:      100.5,
		Shift:      60000,
		Times:      []int64{0, 1},
		Opens:      []float64{0, 1},
		Highs:      []float64{0, 1},
		Lows:       []float64{0, 1},
		Closes:     []float64{0, 1},
		Volumes:    []float64{0.001, 0.002},
	})
	if len(bars) != 2 {
		t.Fatalf("expected 2 bars, got %d", len(bars))
	}
	if bars[0].Volume != 1000 || bars[1].Volume != 2000 {
		t.Fatalf("unexpected bar volumes: %+v", bars)
	}

	ticks := decodeTicks(tickPayload{
		Timestamp:  1704153600000,
		Multiplier: 0.1,
		Ask:        10,
		Bid:        9,
		Times:      []int64{0, 500},
		Asks:       []float64{0, 1},
		Bids:       []float64{0, 1},
		AskVolumes: []float64{2, 3},
		BidVolumes: []float64{4, 5},
	})
	if len(ticks) != 2 {
		t.Fatalf("expected 2 ticks, got %d", len(ticks))
	}

	from := time.UnixMilli(1704153600000).UTC()
	to := from.Add(90 * time.Second)
	if len(filterBars(bars, from, to)) != 2 {
		t.Fatal("expected both bars in filtered range")
	}
	if len(filterTicks(ticks, from, to)) != 2 {
		t.Fatal("expected both ticks in filtered range")
	}
	if !midnightUTC(from).Equal(time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("unexpected midnightUTC result: %s", midnightUTC(from))
	}
	if !hourStartUTC(from.Add(25 * time.Minute)).Equal(from) {
		t.Fatalf("unexpected hourStartUTC result: %s", hourStartUTC(from.Add(25*time.Minute)))
	}
	if got := minLength(5, 4, 9); got != 4 {
		t.Fatalf("expected minLength 4, got %d", got)
	}
}
