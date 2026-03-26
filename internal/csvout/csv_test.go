package csvout

import (
	"testing"
	"time"

	"dukascopy-data-downloader/internal/dukascopy"
)

func TestCombineBarRowsRejectsMismatchedTimestamps(t *testing.T) {
	bidBars := []dukascopy.Bar{{Time: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)}}
	askBars := []dukascopy.Bar{{Time: time.Date(2024, 1, 2, 0, 1, 0, 0, time.UTC)}}

	_, err := combineBarRows(bidBars, askBars)
	if err == nil {
		t.Fatal("expected timestamp mismatch error")
	}
}
