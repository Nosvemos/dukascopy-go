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
		{input: "USDTRY", want: false},
		{input: "USD/TRY", want: false},
		{input: "usdthb", want: false},
		{input: "USDTWD", want: false},
		{input: "USDTUSD", want: true},
		{input: "USDT/EUR", want: true},
		{input: "BTCUSD", want: true},
		{input: "DOGE-USD", want: true},
		{input: "USDJPY", want: false},
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
		{time: time.Date(2026, 5, 24, 20, 59, 0, 0, time.UTC), want: true},
		// Sunday 21:00 UTC is open during US daylight saving time.
		{time: time.Date(2026, 5, 24, 21, 0, 0, 0, time.UTC), want: false},
		{time: time.Date(2026, 5, 24, 22, 0, 0, 0, time.UTC), want: false},
		{time: time.Date(2026, 5, 25, 10, 0, 0, 0, time.UTC), want: false},
	}

	for _, tc := range testCases {
		if got := IsMarketClosed("EURUSD", tc.time); got != tc.want {
			t.Errorf("IsMarketClosed(EURUSD, %s) = %v, want %v", tc.time.Format(time.RFC3339), got, tc.want)
		}
	}

	// Stock and index CFDs are not pruned to exchange sessions: on weekdays
	// every hour is fetched (hours without data come back as 404). Only the
	// FX weekend applies.
	for _, symbol := range []string{"AAPLUSUSD", "AAPL.US/USD", "DEUIDXEUR", "USA500.IDX/USD", "JPNIDXJPY"} {
		weekdayHours := []time.Time{
			time.Date(2026, 5, 25, 1, 0, 0, 0, time.UTC),  // Mon, Asia session
			time.Date(2026, 5, 25, 7, 0, 0, 0, time.UTC),  // Mon, Europe open
			time.Date(2026, 5, 25, 13, 0, 0, 0, time.UTC), // Mon 09:00 New York (hour holding the 09:30 open)
			time.Date(2026, 5, 25, 20, 0, 0, 0, time.UTC), // Mon 16:00 New York
		}
		for _, hour := range weekdayHours {
			if IsMarketClosed(symbol, hour) {
				t.Errorf("IsMarketClosed(%s, %s) = true, want false", symbol, hour.Format(time.RFC3339))
			}
		}
		if !IsMarketClosed(symbol, time.Date(2026, 5, 24, 15, 0, 0, 0, time.UTC)) {
			t.Errorf("IsMarketClosed(%s, Sunday 15:00 UTC) = false, want true", symbol)
		}
	}
}

func TestIsMarketClosedFXWeekendBoundaries(t *testing.T) {
	// Winter (no US DST): market opens Sunday 22:00 UTC. Summer: 21:00 UTC.
	// Only hours closed in both regimes are pruned.
	cases := []struct {
		time time.Time
		want bool
	}{
		{time.Date(2026, 1, 9, 21, 0, 0, 0, time.UTC), false}, // Fri 21:00
		{time.Date(2026, 1, 9, 22, 0, 0, 0, time.UTC), true},  // Fri 22:00
		{time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC), true},  // Sat
		{time.Date(2026, 1, 10, 23, 0, 0, 0, time.UTC), true}, // Sat
		{time.Date(2026, 1, 11, 20, 0, 0, 0, time.UTC), true}, // Sun 20:00
		{time.Date(2026, 1, 11, 21, 0, 0, 0, time.UTC), false},
		{time.Date(2026, 7, 12, 21, 0, 0, 0, time.UTC), false}, // Sun 21:00 in DST: open
		{time.Date(2026, 7, 12, 21, 30, 0, 0, time.FixedZone("x", 3600)), true},
	}
	for _, tc := range cases {
		if got := IsMarketClosed("EURUSD", tc.time); got != tc.want {
			t.Errorf("IsMarketClosed(EURUSD, %s) = %v, want %v", tc.time.UTC().Format(time.RFC3339), got, tc.want)
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
