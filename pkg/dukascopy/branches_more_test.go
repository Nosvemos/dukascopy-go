package dukascopy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDecoderAndAggregateRemainingBranches(t *testing.T) {
	instruments := []Instrument{
		{Name: "XAU/USD", Code: "XAU-USD", Description: "Gold vs US Dollar"},
	}
	if _, err := ResolveInstrument(instruments, ""); err == nil {
		t.Fatal("expected empty symbol error")
	}
	if _, err := ResolveInstrument(instruments, "unknown"); err == nil {
		t.Fatal("expected unresolved symbol error")
	}
	if score := scoreInstrument(instruments[0], "Gold", "GOLD"); score <= 0 {
		t.Fatalf("expected description match score, got %d", score)
	}

	aliases := map[Granularity]Granularity{
		"t1":      GranularityTick,
		"min":     GranularityM1,
		"m30":     GranularityM30,
		"hr":      GranularityH1,
		"week":    GranularityW1,
		"monthly": GranularityMN1,
	}
	for input, want := range aliases {
		if got := normalizeGranularity(input); got != want {
			t.Fatalf("normalizeGranularity(%q) = %q, want %q", input, got, want)
		}
	}

	if _, err := bucketStart(time.Now(), Granularity("weird")); err == nil {
		t.Fatal("expected unsupported bucket granularity error")
	}
	if price, volume := tickSideValue(Tick{Bid: 1, Ask: 2, BidVolume: 3, AskVolume: 4}, PriceSideAsk); price != 2 || volume != 4 {
		t.Fatalf("unexpected ask side values: %f %f", price, volume)
	}
}

func TestClientErrorBranches(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/instruments":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"instruments":[{"id":1,"name":"XAU/USD","code":"XAU-USD","description":"Gold","priceScale":3}]}`))
		case "/v1/candles/day/XAU-USD/BID/2024":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"timestamp":1704067200000,"multiplier":1.0,"open":100,"high":101,"low":99,"close":100.5,"shift":86400000,"times":[0],"opens":[0],"highs":[0],"lows":[0],"closes":[0],"volumes":[0.001]}`))
		default:
			http.Error(w, "bad request", http.StatusBadRequest)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL, time.Second)
	if err := client.getJSON(context.Background(), []string{"v1", "bad"}, &map[string]any{}); err == nil {
		t.Fatal("expected non-retryable HTTP error")
	}

	_, err := client.Download(context.Background(), DownloadRequest{
		Symbol:      "xauusd",
		Granularity: GranularityM1,
		Side:        PriceSide("wat"),
		From:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		To:          time.Date(2024, 1, 2, 0, 1, 0, 0, time.UTC),
	})
	if err == nil {
		t.Fatal("expected invalid side error")
	}

	if _, err := client.downloadBars(context.Background(), Instrument{Code: "XAU-USD"}, PriceSideBid, Granularity("wat"), time.Now(), time.Now().Add(time.Hour)); err == nil {
		t.Fatal("expected unsupported bar granularity error")
	}
}
