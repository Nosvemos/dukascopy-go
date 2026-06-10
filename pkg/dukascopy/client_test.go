package dukascopy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

	client, err := NewClient(server.URL, time.Second)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	client = client.WithRetries(1).WithBackoff(time.Millisecond)
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

	client, err := NewClient(server.URL, time.Second)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	client = client.WithRetries(1).WithBackoff(time.Millisecond)
	var payload instrumentsResponse
	if err := client.getJSON(context.Background(), []string{"v1", "instruments"}, &payload); err != nil {
		t.Fatalf("getJSON returned error: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts)
	}
}

func TestWaitForRateLimitHonorsContextCancellation(t *testing.T) {
	client, err := NewClient("https://example.test", time.Second)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	client = client.WithRateLimit(50 * time.Millisecond)
	client.nextSlot = time.Now().Add(100 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := client.waitForRateLimit(ctx); err == nil {
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

	// Test Equity symbol market hours (AAPLUSUSD)
	// Open hours on Monday: 09:30 - 16:00 EST.
	// Since May 25, 2026 is a Monday:
	// 09:30 EST on May 25, 2026 is 13:30 UTC.
	// 16:00 EST on May 25, 2026 is 20:00 UTC.
	equityTests := []struct {
		time time.Time
		want bool
	}{
		{time: time.Date(2026, 5, 25, 13, 29, 0, 0, time.UTC), want: true},  // Mon 09:29 EST - closed
		{time: time.Date(2026, 5, 25, 13, 31, 0, 0, time.UTC), want: false}, // Mon 09:31 EST - open
		{time: time.Date(2026, 5, 25, 19, 59, 0, 0, time.UTC), want: false}, // Mon 15:59 EST - open
		{time: time.Date(2026, 5, 25, 20, 01, 0, 0, time.UTC), want: true},  // Mon 16:01 EST - closed
		{time: time.Date(2026, 5, 24, 15, 0, 0, 0, time.UTC), want: true},   // Sun 11:00 EST - closed
	}
	for _, tc := range equityTests {
		if got := IsMarketClosed("AAPLUSUSD", tc.time); got != tc.want {
			t.Errorf("IsMarketClosed(AAPLUSUSD, %s) = %v, want %v", tc.time.Format(time.RFC3339), got, tc.want)
		}
	}
}

func TestProxyPool(t *testing.T) {
	tempFile := filepath.Join(t.TempDir(), "proxies.txt")
	content := "# This is a comment\n127.0.0.1:8080\nsocks5://127.0.0.1:1080\n"
	err := os.WriteFile(tempFile, []byte(content), 0o644)
	if err != nil {
		t.Fatalf("failed to write mock proxy file: %v", err)
	}

	pool := &ProxyPool{}
	if err := pool.LoadFromFile(tempFile); err != nil {
		t.Fatalf("LoadFromFile failed: %v", err)
	}

	req, _ := http.NewRequest("GET", "https://example.com", nil)

	// First proxy: 127.0.0.1:8080 (inferred http://)
	u1, err := pool.GetNextProxy(req)
	if err != nil || u1 == nil || u1.Host != "127.0.0.1:8080" || u1.Scheme != "http" {
		t.Fatalf("unexpected first proxy: %v, err: %v", u1, err)
	}

	// Second proxy: socks5://127.0.0.1:1080
	u2, err := pool.GetNextProxy(req)
	if err != nil || u2 == nil || u2.Host != "127.0.0.1:1080" || u2.Scheme != "socks5" {
		t.Fatalf("unexpected second proxy: %v, err: %v", u2, err)
	}

	// Rotates back to the first proxy
	u3, err := pool.GetNextProxy(req)
	if err != nil || u3 == nil || u3.Host != "127.0.0.1:8080" || u3.Scheme != "http" {
		t.Fatalf("unexpected rotated third proxy: %v, err: %v", u3, err)
	}
}

func TestLocalCache(t *testing.T) {
	origPath := getLocalCacheFilePath()
	setLocalCacheFilePath(filepath.Join(t.TempDir(), "test_cache.json"))
	defer func() {
		setLocalCacheFilePath(origPath)
	}()

	instruments := []Instrument{
		{ID: 1, Name: "EUR/USD", Code: "EURUSD", Description: "Euro vs US Dollar", PriceScale: 5},
	}

	saveLocalCache(instruments)

	cached, ok := loadLocalCache()
	if !ok || len(cached) != 1 || cached[0].Code != "EURUSD" {
		t.Fatalf("failed to load fresh cached instruments: %+v, ok: %t", cached, ok)
	}
}
