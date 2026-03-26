package dukascopy

import (
	"context"
	"testing"
	"time"
)

func TestDownloadBarsAdditionalGranularities(t *testing.T) {
	server := newDukascopyTestServer()
	defer server.Close()
	client := NewClient(server.URL, time.Second)
	instrument := Instrument{Code: "XAU-USD"}
	minuteFrom := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	minuteTo := time.Date(2024, 1, 2, 1, 0, 0, 0, time.UTC)
	for _, gran := range []Granularity{GranularityM3, GranularityM5, GranularityM15, GranularityM30} {
		bars, err := client.downloadBars(context.Background(), instrument, PriceSideBid, gran, minuteFrom, minuteTo)
		if err != nil {
			t.Fatalf("downloadBars(%s) returned error: %v", gran, err)
		}
		if len(bars) == 0 {
			t.Fatalf("expected bars for granularity %s", gran)
		}
	}

	dayFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	dayTo := time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC)
	for _, gran := range []Granularity{GranularityW1, GranularityMN1} {
		bars, err := client.downloadBars(context.Background(), instrument, PriceSideBid, gran, dayFrom, dayTo)
		if err != nil {
			t.Fatalf("downloadBars(%s) returned error: %v", gran, err)
		}
		if len(bars) == 0 {
			t.Fatalf("expected bars for granularity %s", gran)
		}
	}
}
