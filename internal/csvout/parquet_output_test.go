package csvout

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/dukascopy"
)

func TestParquetOutputBranches(t *testing.T) {
	dir := t.TempDir()
	instrument := dukascopy.Instrument{PriceScale: 3}
	bar := dukascopy.Bar{
		Time:   time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		Open:   100,
		High:   101,
		Low:    99,
		Close:  100.5,
		Volume: 2,
	}
	tick := dukascopy.Tick{
		Time:      bar.Time,
		Bid:       100.1,
		Ask:       100.3,
		BidVolume: 11,
		AskVolume: 12,
	}

	barPath := filepath.Join(dir, "bars.parquet")
	if err := WriteBars(barPath, instrument, []string{"timestamp", "open", "high", "low", "close", "volume"}, []dukascopy.Bar{bar}, nil, nil); err != nil {
		t.Fatalf("WriteBars parquet returned error: %v", err)
	}
	barStats, err := InspectCSV(barPath)
	if err != nil {
		t.Fatalf("InspectCSV parquet bars returned error: %v", err)
	}
	if barStats.Format != "parquet" || barStats.Rows != 1 {
		t.Fatalf("unexpected parquet bar stats: %+v", barStats)
	}

	tickPath := filepath.Join(dir, "ticks.parquet")
	if err := WriteTicks(tickPath, instrument, []string{"timestamp", "bid", "ask", "bid_volume", "ask_volume"}, []dukascopy.Tick{tick}); err != nil {
		t.Fatalf("WriteTicks parquet returned error: %v", err)
	}
	tickStats, err := InspectCSV(tickPath)
	if err != nil {
		t.Fatalf("InspectCSV parquet ticks returned error: %v", err)
	}
	if tickStats.Format != "parquet" || tickStats.Rows != 1 {
		t.Fatalf("unexpected parquet tick stats: %+v", tickStats)
	}

	filteredPath := filepath.Join(dir, "filtered.parquet")
	if err := extractRangeFromParquet(barPath, filteredPath, bar.Time.Add(-time.Minute), bar.Time.Add(time.Minute)); err != nil {
		t.Fatalf("extractRangeFromParquet parquet->parquet returned error: %v", err)
	}
	filteredStats, err := InspectCSV(filteredPath)
	if err != nil {
		t.Fatalf("InspectCSV filtered parquet returned error: %v", err)
	}
	if filteredStats.Rows != 1 {
		t.Fatalf("expected 1 filtered parquet row, got %+v", filteredStats)
	}
}
