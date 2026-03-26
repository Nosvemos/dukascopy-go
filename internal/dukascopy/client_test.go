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
