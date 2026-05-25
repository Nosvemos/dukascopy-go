package csvout

import (
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/dukascopy"
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

