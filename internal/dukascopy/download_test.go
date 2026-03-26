package dukascopy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestListInstrumentsAndDownloadFlows(t *testing.T) {
	server := newDukascopyTestServer()
	defer server.Close()

	client := NewClient(server.URL, time.Second)
	ctx := context.Background()

	instruments, err := client.ListInstruments(ctx)
	if err != nil {
		t.Fatalf("ListInstruments returned error: %v", err)
	}
	if len(instruments) != 2 || instruments[0].Code != "EUR-USD" || instruments[1].Code != "XAU-USD" {
		t.Fatalf("unexpected instruments: %+v", instruments)
	}

	minuteResult, err := client.Download(ctx, DownloadRequest{
		Symbol:      "xauusd",
		Granularity: GranularityM1,
		Side:        PriceSideBid,
		From:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		To:          time.Date(2024, 1, 2, 0, 3, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("Download minute bars returned error: %v", err)
	}
	if minuteResult.Kind != ResultKindBar || len(minuteResult.Bars) != 3 {
		t.Fatalf("unexpected minute result: %+v", minuteResult)
	}

	hourResult, err := client.Download(ctx, DownloadRequest{
		Symbol:      "xauusd",
		Granularity: GranularityH4,
		Side:        PriceSideBid,
		From:        time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:          time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("Download h4 bars returned error: %v", err)
	}
	if len(hourResult.Bars) != 1 {
		t.Fatalf("expected 1 aggregated h4 bar, got %d", len(hourResult.Bars))
	}

	dayResult, err := client.Download(ctx, DownloadRequest{
		Symbol:      "xauusd",
		Granularity: GranularityD1,
		Side:        PriceSideBid,
		From:        time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:          time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("Download daily bars returned error: %v", err)
	}
	if len(dayResult.Bars) != 2 {
		t.Fatalf("expected 2 daily bars, got %d", len(dayResult.Bars))
	}

	tickResult, err := client.Download(ctx, DownloadRequest{
		Symbol:      "xauusd",
		Granularity: GranularityTick,
		Side:        PriceSideBid,
		From:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		To:          time.Date(2024, 1, 2, 1, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("Download ticks returned error: %v", err)
	}
	if tickResult.Kind != ResultKindTick || len(tickResult.Ticks) != 2 {
		t.Fatalf("unexpected tick result: %+v", tickResult)
	}

	_, askBars, err := client.DownloadBarsForSide(ctx, DownloadRequest{
		Symbol:      "xauusd",
		Granularity: GranularityM1,
		From:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		To:          time.Date(2024, 1, 2, 0, 3, 0, 0, time.UTC),
	}, PriceSideAsk)
	if err != nil {
		t.Fatalf("DownloadBarsForSide returned error: %v", err)
	}
	if len(askBars) != 3 {
		t.Fatalf("expected 3 ask bars, got %d", len(askBars))
	}
}

func TestEmitProgressAndOptionSetters(t *testing.T) {
	client := NewClient("https://example.test", time.Second).
		WithRetries(-1).
		WithBackoff(0).
		WithRateLimit(-1)

	if client.maxRetries != 0 {
		t.Fatalf("expected retries clamp to 0, got %d", client.maxRetries)
	}
	if client.backoff != 500*time.Millisecond {
		t.Fatalf("expected default backoff, got %s", client.backoff)
	}
	if client.rateLimit != 0 {
		t.Fatalf("expected rate limit clamp to 0, got %s", client.rateLimit)
	}

	called := false
	client.WithProgress(func(event ProgressEvent) {
		called = event.Kind == "chunk"
	})
	client.emitProgress(ProgressEvent{Kind: "chunk"})
	if !called {
		t.Fatal("expected progress callback to be invoked")
	}
}

func newDukascopyTestServer() *httptest.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/v1/instruments", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"instruments": []map[string]any{
				{"id": 1, "name": "XAU/USD", "code": "XAU-USD", "description": "Gold vs US Dollar", "priceScale": 3},
				{"id": 2, "name": "EUR/USD", "code": "EUR-USD", "description": "Euro vs US Dollar", "priceScale": 5},
			},
		})
	})

	mux.HandleFunc("/v1/candles/minute/XAU-USD/BID/2024/1/2", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"timestamp":  1704153600000,
			"multiplier": 1.0,
			"open":       100.0,
			"high":       101.0,
			"low":        99.0,
			"close":      100.5,
			"shift":      60000,
			"times":      []int{0, 1, 1},
			"opens":      []float64{0, 1, 1},
			"highs":      []float64{0, 1, 1},
			"lows":       []float64{0, 1, 1},
			"closes":     []float64{0, 1, 1},
			"volumes":    []float64{0.001, 0.002, 0.003},
		})
	})

	mux.HandleFunc("/v1/candles/minute/XAU-USD/ASK/2024/1/2", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"timestamp":  1704153600000,
			"multiplier": 1.0,
			"open":       100.2,
			"high":       101.2,
			"low":        99.2,
			"close":      100.7,
			"shift":      60000,
			"times":      []int{0, 1, 1},
			"opens":      []float64{0, 1, 1},
			"highs":      []float64{0, 1, 1},
			"lows":       []float64{0, 1, 1},
			"closes":     []float64{0, 1, 1},
			"volumes":    []float64{0.001, 0.002, 0.003},
		})
	})

	mux.HandleFunc("/v1/candles/hour/XAU-USD/BID/2024/1", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"timestamp":  1704067200000,
			"multiplier": 1.0,
			"open":       100.0,
			"high":       101.0,
			"low":        99.0,
			"close":      100.5,
			"shift":      3600000,
			"times":      []int{0, 1, 1, 1},
			"opens":      []float64{0, 1, 1, 1},
			"highs":      []float64{0, 1, 1, 1},
			"lows":       []float64{0, 1, 1, 1},
			"closes":     []float64{0, 1, 1, 1},
			"volumes":    []float64{0.001, 0.002, 0.003, 0.004},
		})
	})

	mux.HandleFunc("/v1/ticks/XAU-USD/2024/1/2/0", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"timestamp":  1704153600000,
			"multiplier": 1.0,
			"ask":        100.2,
			"bid":        100.0,
			"times":      []int{0, 500},
			"asks":       []float64{0, 0.1},
			"bids":       []float64{0, 0.1},
			"askVolumes": []float64{10, 20},
			"bidVolumes": []float64{11, 21},
		})
	})

	mux.HandleFunc("/v1/candles/day/XAU-USD/BID/2024", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"timestamp":  1704067200000,
			"multiplier": 1.0,
			"open":       100.0,
			"high":       101.0,
			"low":        99.0,
			"close":      100.5,
			"shift":      86400000,
			"times":      []int{0, 1},
			"opens":      []float64{0, 1},
			"highs":      []float64{0, 1},
			"lows":       []float64{0, 1},
			"closes":     []float64{0, 1},
			"volumes":    []float64{0.001, 0.002},
		})
	})

	return httptest.NewServer(mux)
}

func writeJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}
