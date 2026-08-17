package dukascopy

import (
	"testing"
	"time"
)

func TestSamplingTicksToBars(t *testing.T) {
	now := time.Now().UTC()
	ticks := make([]Tick, 100)
	for i := 0; i < 100; i++ {
		ticks[i] = Tick{
			Time:      now.Add(time.Duration(i) * time.Second),
			Bid:       1.0800 + float64(i)*0.0001,
			Ask:       1.0802 + float64(i)*0.0001,
			BidVolume: 10,
			AskVolume: 10,
		}
	}

	// 1. Tick bars (every 10 ticks = 10 bars)
	tickBars := AggregateTicksToTickBars(ticks, 10, PriceSideBid)
	if len(tickBars) != 10 {
		t.Fatalf("expected 10 tick bars, got %d", len(tickBars))
	}
	if tickBars[0].Open != 1.0800 {
		t.Errorf("expected open 1.0800, got %f", tickBars[0].Open)
	}
	if tickBars[0].Volume != 100 {
		t.Errorf("expected volume 100, got %f", tickBars[0].Volume)
	}

	// 2. Volume bars (every 250 vol = 4 bars)
	volBars := AggregateTicksToVolumeBars(ticks, 250, PriceSideBid)
	if len(volBars) != 4 {
		t.Fatalf("expected 4 volume bars, got %d", len(volBars))
	}

	// 3. Dollar/Notional bars
	dollarBars := AggregateTicksToDollarBars(ticks, 200, PriceSideBid)
	if len(dollarBars) == 0 {
		t.Fatalf("expected dollar bars, got 0")
	}

	// 4. SampleTicksToCustomBars
	parsed, err := ParseBarType("volume")
	if err != nil || parsed != BarTypeVolume {
		t.Fatalf("ParseBarType failed: %v", err)
	}
	customBars, err := SampleTicksToCustomBars(ticks, BarTypeTick, 20, PriceSideBid)
	if err != nil {
		t.Fatalf("SampleTicksToCustomBars failed: %v", err)
	}
	if len(customBars) != 5 {
		t.Fatalf("expected 5 custom bars, got %d", len(customBars))
	}
}
