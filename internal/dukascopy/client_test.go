package dukascopy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestShouldRetryStatus(t *testing.T) {
	if !shouldRetryStatus(http.StatusTooManyRequests) {
		t.Fatal("expected 429 to be retryable")
	}
	if !shouldRetryStatus(http.StatusBadGateway) {
		t.Fatal("expected 502 to be retryable")
	}
	if shouldRetryStatus(http.StatusBadRequest) {
		t.Fatal("expected 400 to be non-retryable")
	}
}

func TestShouldRetryResponseDetectsWrappedServerErrors(t *testing.T) {
	if !shouldRetryResponse(http.StatusBadRequest, []byte(`{"error":"Internal server error","statusCode":500}`)) {
		t.Fatal("expected wrapped server error body to be retryable")
	}
	if shouldRetryResponse(http.StatusBadRequest, []byte(`{"error":"Bad request","statusCode":400}`)) {
		t.Fatal("expected plain bad request body to stay non-retryable")
	}
}

func TestClientGetJSONRetriesTransientStatus(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			http.Error(w, "slow down", http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"instruments":[{"id":1,"name":"XAU/USD","code":"XAU-USD","description":"Gold","priceScale":3}]}`))
	}))
	defer server.Close()

	client := NewClient(server.URL, time.Second).WithRetries(1).WithBackoff(time.Millisecond)
	var payload instrumentsResponse
	if err := client.getJSON(context.Background(), []string{"v1", "instruments"}, &payload); err != nil {
		t.Fatalf("getJSON returned error: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts)
	}
	if len(payload.Instruments) != 1 || payload.Instruments[0].Code != "XAU-USD" {
		t.Fatalf("unexpected payload: %+v", payload)
	}
}

func TestClientGetJSONRetriesWrappedServerErrorBody(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.Header().Set("Content-Type", "application/json")
		if attempts == 1 {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"Internal server error","message":"Failed to load historical ticks","statusCode":500}`))
			return
		}
		_, _ = w.Write([]byte(`{"instruments":[{"id":1,"name":"XAU/USD","code":"XAU-USD","description":"Gold","priceScale":3}]}`))
	}))
	defer server.Close()

	client := NewClient(server.URL, time.Second).WithRetries(1).WithBackoff(time.Millisecond)
	var payload instrumentsResponse
	if err := client.getJSON(context.Background(), []string{"v1", "instruments"}, &payload); err != nil {
		t.Fatalf("getJSON returned error: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts)
	}
}

func TestWaitForRateLimitHonorsContextCancellation(t *testing.T) {
	client := NewClient("https://example.test", time.Second).WithRateLimit(50 * time.Millisecond)
	client.nextSlot = time.Now().Add(100 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := client.waitForRateLimit(ctx)
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
}

func TestIsCryptoSymbol(t *testing.T) {
	testCases := []struct {
		input string
		want  bool
	}{
		{input: "BTCUSD", want: true},
		{input: "btc/usd", want: true},
		{input: "ETH-USD", want: true},
		{input: "EURUSD", want: false},
		{input: "GBP-USD", want: false},
		{input: "XAUUSD", want: false},
		{input: "", want: false},
	}
	for _, tc := range testCases {
		if got := IsCryptoSymbol(tc.input); got != tc.want {
			t.Errorf("IsCryptoSymbol(%q) = %v, want %v", tc.input, got, tc.want)
		}
	}
}

func TestIsMarketClosed(t *testing.T) {
	if IsMarketClosed("BTCUSD", time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)) {
		t.Fatal("expected crypto to be always open")
	}

	testCases := []struct {
		time time.Time
		want bool
	}{
		{time: time.Date(2026, 5, 22, 21, 59, 0, 0, time.UTC), want: false},
		{time: time.Date(2026, 5, 22, 22, 0, 0, 0, time.UTC), want: true},
		{time: time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC), want: true},
		{time: time.Date(2026, 5, 24, 21, 59, 0, 0, time.UTC), want: true},
		{time: time.Date(2026, 5, 24, 22, 0, 0, 0, time.UTC), want: false},
		{time: time.Date(2026, 5, 25, 10, 0, 0, 0, time.UTC), want: false},
	}

	for _, tc := range testCases {
		if got := IsMarketClosed("EURUSD", tc.time); got != tc.want {
			t.Errorf("IsMarketClosed(EURUSD, %s) = %v, want %v", tc.time.Format(time.RFC3339), got, tc.want)
		}
	}
}

