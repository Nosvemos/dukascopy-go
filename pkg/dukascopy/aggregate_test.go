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
		{Time: from, Open: 1, High: 3, Low: 1, Close: 2, Volume: 10, TickCount: 2},
		{Time: from.Add(1 * time.Minute), Open: 2, High: 5, Low: 2, Close: 4, Volume: 11, TickCount: 3},
		{Time: from.Add(4 * time.Minute), Open: 4, High: 4, Low: 3, Close: 3.5, Volume: 12, TickCount: 4},
		{Time: from.Add(5 * time.Minute), Open: 3.5, High: 6, Low: 3, Close: 5, Volume: 13, TickCount: 5},
	}

	bars, err := AggregateBars(source, GranularityM5, from, to)
	if err != nil {
		t.Fatalf("AggregateBars returned error: %v", err)
	}
	if len(bars) != 2 {
		t.Fatalf("expected 2 bars, got %d", len(bars))
	}
	if bars[0].Open != 1 || bars[0].High != 5 || bars[0].Low != 1 || bars[0].Close != 3.5 || bars[0].Volume != 33 || bars[0].TickCount != 9 {
		t.Fatalf("unexpected first aggregated bar: %+v", bars[0])
	}
	if bars[1].Open != 3.5 || bars[1].High != 6 || bars[1].Low != 3 || bars[1].Close != 5 || bars[1].Volume != 13 || bars[1].TickCount != 5 {
		t.Fatalf("unexpected second aggregated bar: %+v", bars[1])
	}
}

func TestCountTicksByIntervalUsesUTCAndCountsRecords(t *testing.T) {
	plusFive := time.FixedZone("UTC+5", 5*60*60)
	ticks := []Tick{
		{Time: time.Date(2024, 1, 1, 0, 0, 1, 0, plusFive)}, // previous UTC day, 19:00
		{Time: time.Date(2024, 1, 1, 0, 0, 5, 0, time.UTC)},
		{Time: time.Date(2024, 1, 1, 0, 0, 59, 999_000_000, time.UTC)},
		{Time: time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC)},
	}

	counts, err := CountTicksByInterval(ticks, GranularityM1)
	if err != nil {
		t.Fatalf("CountTicksByInterval returned error: %v", err)
	}
	want := map[time.Time]int64{
		time.Date(2023, 12, 31, 19, 0, 0, 0, time.UTC): 1,
		time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC):    2,
		time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC):    1,
	}
	if len(counts) != len(want) {
		t.Fatalf("expected %d intervals, got %d: %v", len(want), len(counts), counts)
	}
	for interval, expected := range want {
		if got := counts[interval]; got != expected {
			t.Errorf("count for %s = %d, want %d", interval, got, expected)
		}
	}
}

func TestCountTicksByIntervalRejectsTickGranularity(t *testing.T) {
	if _, err := CountTicksByInterval(nil, GranularityTick); err == nil {
		t.Fatal("expected tick granularity to be rejected")
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
