package dukascopy

import (
	"testing"
	"time"
)

func TestAggregateTicksToBarsBid(t *testing.T) {
	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(2 * time.Minute)
	ticks := []Tick{
		{Time: from.Add(5 * time.Second), Bid: 10, Ask: 11, BidVolume: 2, AskVolume: 3},
		{Time: from.Add(20 * time.Second), Bid: 12, Ask: 13, BidVolume: 1, AskVolume: 2},
		{Time: from.Add(50 * time.Second), Bid: 9, Ask: 10, BidVolume: 4, AskVolume: 1},
		{Time: from.Add(70 * time.Second), Bid: 8, Ask: 9, BidVolume: 5, AskVolume: 1},
	}

	bars, err := AggregateTicksToBars(ticks, GranularityM1, PriceSideBid, from, to)
	if err != nil {
		t.Fatalf("AggregateTicksToBars returned error: %v", err)
	}
	if len(bars) != 2 {
		t.Fatalf("expected 2 bars, got %d", len(bars))
	}

	if bars[0].Open != 10 || bars[0].High != 12 || bars[0].Low != 9 || bars[0].Close != 9 || bars[0].Volume != 7 {
		t.Fatalf("unexpected first bar: %+v", bars[0])
	}
	if bars[1].Open != 8 || bars[1].High != 8 || bars[1].Low != 8 || bars[1].Close != 8 || bars[1].Volume != 5 {
		t.Fatalf("unexpected second bar: %+v", bars[1])
	}
}

func TestAggregateBarsM5(t *testing.T) {
	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(10 * time.Minute)
	source := []Bar{
		{Time: from, Open: 1, High: 3, Low: 1, Close: 2, Volume: 10},
		{Time: from.Add(1 * time.Minute), Open: 2, High: 5, Low: 2, Close: 4, Volume: 11},
		{Time: from.Add(4 * time.Minute), Open: 4, High: 4, Low: 3, Close: 3.5, Volume: 12},
		{Time: from.Add(5 * time.Minute), Open: 3.5, High: 6, Low: 3, Close: 5, Volume: 13},
	}

	bars, err := AggregateBars(source, GranularityM5, from, to)
	if err != nil {
		t.Fatalf("AggregateBars returned error: %v", err)
	}
	if len(bars) != 2 {
		t.Fatalf("expected 2 bars, got %d", len(bars))
	}
	if bars[0].Open != 1 || bars[0].High != 5 || bars[0].Low != 1 || bars[0].Close != 3.5 || bars[0].Volume != 33 {
		t.Fatalf("unexpected first aggregated bar: %+v", bars[0])
	}
	if bars[1].Open != 3.5 || bars[1].High != 6 || bars[1].Low != 3 || bars[1].Close != 5 || bars[1].Volume != 13 {
		t.Fatalf("unexpected second aggregated bar: %+v", bars[1])
	}
}

func TestBucketStartWeeklyAndMonthly(t *testing.T) {
	value := time.Date(2024, 1, 3, 14, 45, 0, 0, time.UTC)

	week, err := bucketStart(value, GranularityW1)
	if err != nil {
		t.Fatalf("bucketStart weekly returned error: %v", err)
	}
	month, err := bucketStart(value, GranularityMN1)
	if err != nil {
		t.Fatalf("bucketStart monthly returned error: %v", err)
	}

	if !week.Equal(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("unexpected week start: %s", week)
	}
	if !month.Equal(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("unexpected month start: %s", month)
	}
}

func TestAggregateTicksToBarsRejectsUnsupportedGranularity(t *testing.T) {
	_, err := AggregateTicksToBars(nil, GranularityTick, PriceSideBid, time.Time{}, time.Now())
	if err == nil {
		t.Fatal("expected unsupported granularity error")
	}
}
