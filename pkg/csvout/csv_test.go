package csvout

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

func TestCombineBarRowsRejectsMismatchedTimestamps(t *testing.T) {
	bidBars := []dukascopy.Bar{{Time: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)}}
	askBars := []dukascopy.Bar{{Time: time.Date(2024, 1, 2, 0, 1, 0, 0, time.UTC)}}

	_, err := combineBarRows(bidBars, askBars)
	if err == nil {
		t.Fatal("expected timestamp mismatch error")
	}
}

func TestFormatMidPriceKeepsExtraHalfPipPrecision(t *testing.T) {
	got := formatMidPrice((2064.295+2064.652)/2, 3)
	if got != "2064.4735" {
		t.Fatalf("formatMidPrice() = %s, want 2064.4735", got)
	}
}

func TestCustomTimezoneFormatting(t *testing.T) {
	// Restore variables at the end of the test
	origLoc := OutputLocation
	origFormat := OutputTimestampFormat
	defer func() {
		OutputLocation = origLoc
		OutputTimestampFormat = origFormat
	}()

	nyc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("failed to load NYC timezone: %v", err)
	}

	OutputLocation = nyc
	OutputTimestampFormat = "2006-01-02 15:04:05"

	utcTime := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	got := formatTime(utcTime)
	// New York is UTC-4 in May (DST active)
	want := "2026-05-25 08:00:00"
	if got != want {
		t.Errorf("formatTime() in NYC = %q, want %q", got, want)
	}
}

type mockCSVRecordWriter struct {
	records [][]string
}

func (m *mockCSVRecordWriter) Write(record []string) error {
	m.records = append(m.records, record)
	return nil
}

func (m *mockCSVRecordWriter) Flush() {}

func (m *mockCSVRecordWriter) Error() error { return nil }

func TestCustomDelimiterAndHeaderSuppression(t *testing.T) {
	origHide := HideCSVHeader
	defer func() {
		HideCSVHeader = origHide
	}()

	HideCSVHeader = true
	mockWriter := &mockCSVRecordWriter{}
	instrument := dukascopy.Instrument{Code: "EURUSD", PriceScale: 5}
	columns := []string{"timestamp", "open"}
	bars := []dukascopy.Bar{{Time: time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC), Open: 1.0850}}

	err := writeBarsCSVRows(mockWriter, instrument, columns, bars, nil, nil, true)
	if err != nil {
		t.Fatalf("writeBarsCSVRows failed: %v", err)
	}

	// Because HideCSVHeader is true, the columns header should not be written. Only the row should be written!
	if len(mockWriter.records) != 1 {
		t.Fatalf("expected 1 record written (header hidden), got %d", len(mockWriter.records))
	}

	if mockWriter.records[0][1] != "1.08500" {
		t.Errorf("expected open price 1.08500, got %s", mockWriter.records[0][1])
	}
}

func TestCleanDuplicates(t *testing.T) {
	tempPath := filepath.Join(t.TempDir(), "test-cleanup.csv")
	instrument := dukascopy.Instrument{Code: "EURUSD", PriceScale: 5}
	columns := []string{"timestamp", "open"}

	bars := []dukascopy.Bar{
		{Time: time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC), Open: 1.0850},
		{Time: time.Date(2026, 5, 25, 11, 0, 0, 0, time.UTC), Open: 1.0840},
		{Time: time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC), Open: 1.0850},
	}

	err := WriteBars(tempPath, instrument, columns, bars, nil, nil)
	if err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	cleanedCount, err := CleanDuplicates(tempPath)
	if err != nil {
		t.Fatalf("CleanDuplicates failed: %v", err)
	}

	if cleanedCount != 1 {
		t.Errorf("expected 1 duplicate removed, got %d", cleanedCount)
	}

	stats, err := InspectCSV(tempPath)
	if err != nil {
		t.Fatalf("InspectCSV failed: %v", err)
	}

	if stats.Rows != 2 {
		t.Errorf("expected 2 rows after cleanup, got %d", stats.Rows)
	}

	if stats.DuplicateRows != 0 || stats.DuplicateStamps != 0 || stats.OutOfOrderRows != 0 {
		t.Errorf("unexpected anomalies remaining: duplicate_rows=%d duplicate_stamps=%d out_of_order=%d",
			stats.DuplicateRows, stats.DuplicateStamps, stats.OutOfOrderRows)
	}

	if !stats.FirstTimestamp.Equal(time.Date(2026, 5, 25, 11, 0, 0, 0, time.UTC)) {
		t.Errorf("expected first timestamp to be 11:00, got %s", stats.FirstTimestamp)
	}
}

func TestWriteBarsCSVRowsWithForwardGapFilling(t *testing.T) {
	origFill := FillGaps
	defer func() {
		FillGaps = origFill
	}()

	FillGaps = "forward"
	mockWriter := &mockCSVRecordWriter{}
	instrument := dukascopy.Instrument{Code: "EURUSD", PriceScale: 5}
	columns := []string{"timestamp", "open", "close", "volume"}

	// Two bars with a 3-minute gap. Inferred timeframe interval is 1 minute.
	// Gaps at minute 1 and minute 2 should be forward-filled!
	t1 := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 5, 25, 12, 1, 0, 0, time.UTC)
	t3 := time.Date(2026, 5, 25, 12, 2, 0, 0, time.UTC)
	t4 := time.Date(2026, 5, 25, 12, 5, 0, 0, time.UTC)

	bars := []dukascopy.Bar{
		{Time: t1, Open: 1.0850, Close: 1.0860, Volume: 100},
		{Time: t2, Open: 1.0860, Close: 1.0870, Volume: 110},
		{Time: t3, Open: 1.0870, Close: 1.0880, Volume: 120},
		{Time: t4, Open: 1.0890, Close: 1.0900, Volume: 200},
	}

	err := writeBarsCSVRows(mockWriter, instrument, columns, bars, nil, nil, false)
	if err != nil {
		t.Fatalf("writeBarsCSVRows failed: %v", err)
	}

	// We expect 6 bars: t1, t2, t3, t3+1m (synthetic), t3+2m (synthetic), t4
	if len(mockWriter.records) != 6 {
		t.Fatalf("expected 6 records (4 actual + 2 filled), got %d: %+v", len(mockWriter.records), mockWriter.records)
	}

	// Verify first synthetic bar (t3+1m = 12:03) has Close price of t3 (1.08800) and volume 0
	row1 := mockWriter.records[3]
	if row1[0] != "2026-05-25T12:03:00Z" {
		t.Errorf("expected synthetic timestamp 12:03:00, got %s", row1[0])
	}
	if row1[1] != "1.08800" || row1[2] != "1.08800" {
		t.Errorf("expected synthetic price 1.08800, got open=%s close=%s", row1[1], row1[2])
	}
	if row1[3] != "0" {
		t.Errorf("expected synthetic volume 0, got %s", row1[3])
	}

	// Verify second synthetic bar (t3+2m = 12:04) has Close price of t3 (1.08800) and volume 0
	row2 := mockWriter.records[4]
	if row2[0] != "2026-05-25T12:04:00Z" {
		t.Errorf("expected synthetic timestamp 12:04:00, got %s", row2[0])
	}
	if row2[3] != "0" {
		t.Errorf("expected synthetic volume 0, got %s", row2[3])
	}
}
