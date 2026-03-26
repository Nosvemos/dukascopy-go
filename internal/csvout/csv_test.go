package csvout

import (
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-data-downloader/internal/dukascopy"
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
