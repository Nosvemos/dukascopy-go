package dukascopy

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ulikunitz/xz/lzma"
)

// --- 1. no equity-session pruning in minute downloads ---

func TestDownloadMinuteBarsFetchesSundayForIndices(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		http.NotFound(w, r)
	}))
	defer server.Close()

	client, err := NewClient(server.URL, time.Second)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	from := time.Date(2024, 1, 6, 0, 0, 0, 0, time.UTC) // Saturday
	to := time.Date(2024, 1, 9, 0, 0, 0, 0, time.UTC)   // through Monday
	if _, err := client.downloadBars(context.Background(), Instrument{Code: "DEUIDXEUR"}, PriceSideBid, GranularityM1, from, to); err != nil {
		t.Fatalf("downloadBars: %v", err)
	}
	joined := strings.Join(paths, "\n")
	if !strings.Contains(joined, "/2024/1/7") {
		t.Fatalf("expected Sunday 2024-01-07 to be requested, got %v", paths)
	}
	if strings.Contains(joined, "/2024/1/6") {
		t.Fatalf("expected Saturday to be skipped, got %v", paths)
	}
}

// --- 4. sampling side normalization and tick bar size ---

func TestSamplingNormalizesSide(t *testing.T) {
	ticks := []Tick{
		{Time: time.Unix(0, 0), Bid: 1, Ask: 2, BidVolume: 10, AskVolume: 20},
		{Time: time.Unix(1, 0), Bid: 3, Ask: 4, BidVolume: 10, AskVolume: 20},
	}
	for _, side := range []PriceSide{"ask", " Ask ", "ASK"} {
		for _, barType := range []BarType{BarTypeTick, BarTypeVolume, BarTypeDollar} {
			bars, err := SampleTicksToCustomBars(ticks, barType, 1, side)
			if err != nil {
				t.Fatalf("SampleTicksToCustomBars(%s, %q): %v", barType, side, err)
			}
			if len(bars) == 0 || bars[0].Open != 2 || bars[0].Volume != 20 {
				t.Fatalf("SampleTicksToCustomBars(%s, %q) used bid prices: %+v", barType, side, bars)
			}
		}
		if bars := AggregateTicksToTickBars(ticks, 1, side); bars[0].Open != 2 {
			t.Fatalf("AggregateTicksToTickBars(%q) used bid: %+v", side, bars)
		}
		if bars := AggregateTicksToVolumeBars(ticks, 1, side); bars[0].Open != 2 {
			t.Fatalf("AggregateTicksToVolumeBars(%q) used bid: %+v", side, bars)
		}
		if bars := AggregateTicksToDollarBars(ticks, 1, side); bars[0].Open != 2 {
			t.Fatalf("AggregateTicksToDollarBars(%q) used bid: %+v", side, bars)
		}
	}
	if _, err := SampleTicksToCustomBars(ticks, BarTypeTick, 1, "sideways"); err == nil {
		t.Fatal("expected invalid side error")
	}
}

func TestSampleTicksRejectsFractionalTickBarBelowOne(t *testing.T) {
	ticks := []Tick{{Time: time.Unix(0, 0), Bid: 1, Ask: 2}}
	for _, size := range []float64{0.5, 0.999, math.NaN()} {
		if _, err := SampleTicksToCustomBars(ticks, BarTypeTick, size, PriceSideBid); err == nil {
			t.Fatalf("expected error for tick bar size %v", size)
		}
	}
	bars, err := SampleTicksToCustomBars(ticks, BarTypeTick, 1.5, PriceSideBid)
	if err != nil || len(bars) != 1 {
		t.Fatalf("expected 1.5 to truncate to 1 tick per bar, got %v %v", bars, err)
	}
	// Volume bars accept fractional sizes.
	if _, err := SampleTicksToCustomBars(ticks, BarTypeVolume, 0.5, PriceSideBid); err != nil {
		t.Fatalf("volume bar size 0.5 rejected: %v", err)
	}
}

// --- 5. worker pool never returns partial data with a nil error ---

func TestDownloadParallelParentCancelReturnsError(t *testing.T) {
	client := &Client{}
	for run := 0; run < 50; run++ {
		ctx, cancel := context.WithCancel(context.Background())
		data, err := downloadParallel(client, ctx, 32, func(ctx context.Context, idx int) ([]int, int64, error) {
			if idx == 0 {
				cancel()
			}
			return []int{idx}, 1, nil
		}, "test", nil)
		cancel()
		if err == nil {
			t.Fatalf("run %d: expected error after parent cancel, got nil with %d rows", run, len(data))
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("run %d: expected context.Canceled, got %v", run, err)
		}
	}
}

func TestDownloadParallelReportsChunkErrorAndRunningRows(t *testing.T) {
	var maxRows atomic.Int64
	client := (&Client{}).WithProgress(func(e ProgressEvent) {
		if int64(e.Rows) > maxRows.Load() {
			maxRows.Store(int64(e.Rows))
		}
	})
	data, err := downloadParallel(client, context.Background(), 10, func(ctx context.Context, idx int) ([]int, int64, error) {
		return []int{idx, idx}, 2, nil
	}, "test", nil)
	if err != nil || len(data) != 20 || maxRows.Load() != 20 {
		t.Fatalf("unexpected result: len=%d rows=%d err=%v", len(data), maxRows.Load(), err)
	}

	boom := errors.New("boom")
	_, err = downloadParallel(client, context.Background(), 10, func(ctx context.Context, idx int) ([]int, int64, error) {
		if idx == 5 {
			return nil, 0, boom
		}
		return []int{idx}, 1, nil
	}, "test", nil)
	if !errors.Is(err, boom) {
		t.Fatalf("expected chunk error, got %v", err)
	}

	// A 404 chunk is empty, not an error.
	data, err = downloadParallel(client, context.Background(), 3, func(ctx context.Context, idx int) ([]int, int64, error) {
		if idx == 1 {
			return nil, 0, &HTTPStatusError{StatusCode: http.StatusNotFound}
		}
		return []int{idx}, 1, nil
	}, "test", nil)
	if err != nil || len(data) != 2 {
		t.Fatalf("expected 404 chunk to be skipped, got %v %v", data, err)
	}
}

// --- 6. only 404 means "no data" ---

func TestOnlyNotFoundIsNoData(t *testing.T) {
	if !errors.Is(&HTTPStatusError{StatusCode: 404}, ErrNoData) {
		t.Fatal("404 should match ErrNoData")
	}
	for _, err := range []error{
		&HTTPStatusError{StatusCode: 400, Body: "bad request"},
		&HTTPStatusError{StatusCode: 500, Body: `{"message":"Failed to load historical ticks"}`},
		errors.New("instrument not found"),
		errors.New("dukascopy api https://x/returned 404"),
	} {
		if isNoDataError(err) {
			t.Fatalf("%v must not be treated as no data", err)
		}
	}
}

func TestDownloadSurfacesNonNotFoundChunkErrors(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		wantOK bool
	}{
		{"404 is empty", http.StatusNotFound, "not found", true},
		{"400 is error", http.StatusBadRequest, `{"error":"Bad request","statusCode":400}`, false},
		{"500 failed to load historical is error", http.StatusInternalServerError, `{"message":"Failed to load historical candles","statusCode":500}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/instruments" {
					writeJSON(w, map[string]any{"instruments": []map[string]any{{"id": 1, "name": "XAU/USD", "code": "XAU-USD", "priceScale": 3}}})
					return
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			client, err := NewClient(server.URL, time.Second)
			if err != nil {
				t.Fatalf("NewClient: %v", err)
			}
			client = client.WithRetries(1).WithBackoff(time.Millisecond)
			res, err := client.Download(context.Background(), DownloadRequest{
				Symbol: "xauusd", Granularity: GranularityM1, Side: PriceSideBid,
				From: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
				To:   time.Date(2024, 1, 2, 0, 3, 0, 0, time.UTC),
			})
			if tc.wantOK {
				if err != nil || len(res.Bars) != 0 {
					t.Fatalf("expected empty result, got %v %v", res.Bars, err)
				}
				return
			}
			var statusErr *HTTPStatusError
			if !errors.As(err, &statusErr) || statusErr.StatusCode != tc.status {
				t.Fatalf("expected HTTPStatusError %d, got %v", tc.status, err)
			}
		})
	}
}

// --- 7. rate adaptation ---

func TestRateAdaptationTreatsNotFoundAsSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, time.Second)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	client = client.WithRateLimit(time.Millisecond)
	for i := 0; i < 5; i++ {
		if err := client.getJSON(context.Background(), []string{"v1", "x"}, &map[string]any{}); !errors.Is(err, ErrNoData) {
			t.Fatalf("expected ErrNoData, got %v", err)
		}
	}
	if client.adaptiveRate != time.Millisecond {
		t.Fatalf("404s should not throttle, adaptive rate = %s", client.adaptiveRate)
	}
}

func TestRateAdaptationNeverLowersThrottledInterval(t *testing.T) {
	client := &Client{}
	for i := 0; i < 6; i++ {
		client.adjustRateLimit(rateThrottled)
	}
	raised := client.adaptiveRate
	if raised <= maxSoftErrorRate {
		t.Fatalf("expected 429s to raise above %s, got %s", maxSoftErrorRate, raised)
	}
	client.adjustRateLimit(rateServerError)
	if client.adaptiveRate < raised {
		t.Fatalf("server error lowered interval from %s to %s", raised, client.adaptiveRate)
	}
	client.adjustRateLimit(rateNeutral)
	if client.adaptiveRate < raised {
		t.Fatalf("4xx lowered interval from %s to %s", raised, client.adaptiveRate)
	}
	client.adjustRateLimit(rateSuccess)
	if client.adaptiveRate != raised-10*time.Millisecond {
		t.Fatalf("success should decay by 10ms, got %s", client.adaptiveRate)
	}
}

// --- 8. retries ---

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cases := map[string]time.Duration{
		"":                              0,
		"3":                             3 * time.Second,
		"0":                             0,
		"-1":                            0,
		"junk":                          0,
		"Thu, 01 Jan 2026 00:00:10 GMT": 10 * time.Second,
		"Wed, 31 Dec 2025 23:59:00 GMT": 0,
	}
	for in, want := range cases {
		if got := parseRetryAfter(in, now); got != want {
			t.Errorf("parseRetryAfter(%q) = %s, want %s", in, got, want)
		}
	}
}

func TestRetryDelayBackoffIsJitteredAndCapped(t *testing.T) {
	client := &Client{backoff: 500 * time.Millisecond}
	for attempt := 0; attempt < 20; attempt++ {
		for i := 0; i < 20; i++ {
			d := client.retryDelay(attempt, 0)
			if d < 0 || d > maxRetryDelay {
				t.Fatalf("attempt %d: delay %s outside [0, %s]", attempt, d, maxRetryDelay)
			}
			if d > backoffCeiling(client.backoff, attempt) {
				t.Fatalf("attempt %d: delay %s above ceiling", attempt, d)
			}
		}
	}
	if got := backoffCeiling(500*time.Millisecond, 2); got != 2*time.Second {
		t.Fatalf("expected exponential ceiling 2s, got %s", got)
	}
	if got := backoffCeiling(500*time.Millisecond, 100); got != maxRetryDelay {
		t.Fatalf("expected capped ceiling, got %s", got)
	}
	if got := client.retryDelay(0, 7*time.Second); got != 7*time.Second {
		t.Fatalf("expected Retry-After to be honored, got %s", got)
	}
	if got := client.retryDelay(0, time.Hour); got != maxRetryAfter {
		t.Fatalf("expected Retry-After cap, got %s", got)
	}
}

func TestRetryHonorsRetryAfterHeader(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "slow down", http.StatusTooManyRequests)
			return
		}
		writeJSON(w, map[string]any{"ok": true})
	}))
	defer server.Close()
	client, err := NewClient(server.URL, 5*time.Second)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	client = client.WithRetries(1).WithBackoff(time.Millisecond)
	start := time.Now()
	if err := client.getJSON(context.Background(), []string{"v1"}, &map[string]any{}); err != nil {
		t.Fatalf("getJSON: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Fatalf("expected to wait for Retry-After (1s), waited %s", elapsed)
	}
}

func TestNoRetryAfterContextCancelled(t *testing.T) {
	client, err := NewClient("https://example.test", time.Second)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	var retries atomic.Int32
	client = client.WithRetries(5).WithBackoff(time.Millisecond).WithProgress(func(e ProgressEvent) {
		if e.Kind == "retry" {
			retries.Add(1)
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int32
	client.httpClient = &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		cancel()
		return nil, &net.OpError{Op: "dial", Err: errors.New("connection reset")}
	})}
	err = client.getJSON(ctx, []string{"v1"}, &map[string]any{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if calls.Load() != 1 || retries.Load() != 0 {
		t.Fatalf("expected no retry after cancel: calls=%d retry events=%d", calls.Load(), retries.Load())
	}
}

func TestRetrySleepIsInterruptible(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "60")
		http.Error(w, "busy", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, time.Second)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	client = client.WithRetries(3)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = client.getJSON(ctx, []string{"v1"}, &map[string]any{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("retry sleep was not interrupted by context")
	}
}

// --- 9. bodies ---

func TestGetRawBytesRejectsOversizedBody(t *testing.T) {
	old := maxRawBodyBytes
	maxRawBodyBytes = 10
	defer func() { maxRawBodyBytes = old }()

	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		_, _ = w.Write(bytes.Repeat([]byte("x"), 20))
	}))
	defer server.Close()
	client, err := NewClient(server.URL, time.Second)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	client = client.WithRetries(2).WithBackoff(time.Millisecond)
	if _, err := client.getRawBytes(context.Background(), []string{"f"}); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("expected size error, got %v", err)
	}
	if attempts.Load() != 1 {
		t.Fatalf("oversized body should not be retried, attempts=%d", attempts.Load())
	}

	maxRawBodyBytes = 20
	if data, err := client.getRawBytes(context.Background(), []string{"f"}); err != nil || len(data) != 20 {
		t.Fatalf("body at the limit should pass, got %d bytes, %v", len(data), err)
	}
}

func TestErrorBodiesAreDrainedForConnectionReuse(t *testing.T) {
	var newConns atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write(bytes.Repeat([]byte("e"), 4096))
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			newConns.Add(1)
		}
	}
	server.Start()
	defer server.Close()

	client, err := NewClient(server.URL, time.Second)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	client = client.WithRetries(0)
	for i := 0; i < 5; i++ {
		var statusErr *HTTPStatusError
		err := client.getJSON(context.Background(), []string{"v1"}, &map[string]any{})
		if !errors.As(err, &statusErr) || statusErr.StatusCode != 400 || len(statusErr.Body) > errorBodyLimit {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if got := newConns.Load(); got != 1 {
		t.Fatalf("expected 1 reused connection, got %d", got)
	}
}

// --- 10. client construction and request errors ---

func TestNewClientValidatesBaseURL(t *testing.T) {
	for _, raw := range []string{"", "jetta.dukascopy.com", "ftp://example.test", "http://", "https:///path"} {
		if _, err := NewClient(raw, time.Second); err == nil {
			t.Errorf("NewClient(%q) should fail", raw)
		}
	}
	client, err := NewClient(" https://example.test/ ", time.Second)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	transport, ok := client.httpClient.Transport.(*http.Transport)
	if !ok {
		t.Fatal("expected *http.Transport")
	}
	if transport.DialContext == nil || transport.TLSHandshakeTimeout == 0 || transport.ResponseHeaderTimeout == 0 {
		t.Fatalf("expected dial, TLS and response-header timeouts, got %+v", transport)
	}
}

func TestBadRequestURLReturnsErrorInsteadOfPanicking(t *testing.T) {
	client := &Client{
		baseURL:    &url.URL{Scheme: "http", Host: "bad host"},
		httpClient: http.DefaultClient,
	}
	if err := client.getJSON(context.Background(), []string{"v1"}, &map[string]any{}); err == nil {
		t.Fatal("expected request build error")
	}
	if _, err := client.getRawBytes(context.Background(), []string{"v1"}); err == nil {
		t.Fatal("expected request build error")
	}
	if err := (&Client{}).getJSON(context.Background(), nil, &map[string]any{}); err == nil {
		t.Fatal("expected error for client without base url")
	}
}

// --- 11. instruments cache ---

func TestInstrumentFallbackIsNeverPersisted(t *testing.T) {
	list, persist, err := buildInstrumentList(nil, errors.New("offline"), true, true)
	if err != nil || persist || len(list) != len(DefaultInstruments) {
		t.Fatalf("fallback: len=%d persist=%v err=%v", len(list), persist, err)
	}
	if _, _, err := buildInstrumentList(nil, errors.New("offline"), false, false); err == nil {
		t.Fatal("expected error when fallback is not allowed")
	}
	list, persist, err = buildInstrumentList([]Instrument{{Name: "Z", Code: "ZZZ"}, {Name: "A", Code: "AAA"}}, nil, true, false)
	if err != nil || !persist || len(list) != 2 || list[0].Name != "A" {
		t.Fatalf("success: %v persist=%v err=%v", list, persist, err)
	}
	list, _, _ = buildInstrumentList([]Instrument{{Name: "EUR/USD", Code: "EUR-USD"}}, nil, true, true)
	if len(list) != len(DefaultInstruments) {
		t.Fatalf("merge should dedupe by compact code: got %d, want %d", len(list), len(DefaultInstruments))
	}
}

func TestListInstrumentsPersistsSynchronouslyAndAtomically(t *testing.T) {
	dir := t.TempDir()
	cachePath := filepath.Join(dir, "sub", "instruments.json")
	setLocalCacheFilePath(cachePath)
	defer setLocalCacheFilePath("")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"instruments": []map[string]any{{"id": 1, "name": "XAU/USD", "code": "XAU-USD", "priceScale": 3}}})
	}))
	defer server.Close()
	client, err := NewClient(server.URL, time.Second)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if _, err := client.WithForceUpdate(true).ListInstruments(context.Background()); err != nil {
		t.Fatalf("ListInstruments: %v", err)
	}
	// No sleep: the write must have completed before ListInstruments returned.
	cached, ok := loadLocalCache()
	if !ok || len(cached) != 1 || cached[0].Code != "XAU-USD" {
		t.Fatalf("cache not written synchronously: %v ok=%v", cached, ok)
	}
	entries, err := os.ReadDir(filepath.Dir(cachePath))
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected only the cache file (no temp leftovers), got %v", entries)
	}

	// A failed fetch never overwrites the cache.
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "down", http.StatusInternalServerError)
	}))
	defer failing.Close()
	before, _ := os.ReadFile(cachePath)
	failClient, _ := NewClient(failing.URL, time.Second)
	if _, err := failClient.WithRetries(0).WithForceUpdate(true).ListInstruments(context.Background()); err == nil {
		t.Fatal("expected error in test mode")
	}
	after, _ := os.ReadFile(cachePath)
	if !bytes.Equal(before, after) {
		t.Fatal("cache changed after failed fetch")
	}
}

func TestGetLocalCachePathHasNoSideEffects(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	setLocalCacheFilePath("")
	path := getLocalCachePath()
	if path == "" {
		t.Skip("no home directory")
	}
	if _, err := os.Stat(filepath.Dir(path)); !os.IsNotExist(err) {
		t.Fatalf("getLocalCachePath created %s (err=%v)", filepath.Dir(path), err)
	}
	if _, ok := loadLocalCache(); ok {
		t.Fatal("unexpected cache hit")
	}
	if _, err := os.Stat(filepath.Dir(path)); !os.IsNotExist(err) {
		t.Fatalf("loadLocalCache created %s", filepath.Dir(path))
	}
}

// --- 12. truncated bi5 streams ---

func compressLZMA(t *testing.T, raw []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := lzma.NewWriter(&buf)
	if err != nil {
		t.Fatalf("lzma.NewWriter: %v", err)
	}
	if _, err := w.Write(raw); err != nil {
		t.Fatalf("lzma write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("lzma close: %v", err)
	}
	return buf.Bytes()
}

func TestBi5TrailingPartialRecordIsAnError(t *testing.T) {
	var ticks bytes.Buffer
	for _, v := range []uint32{100, 100050, 100000, math.Float32bits(1), math.Float32bits(1)} {
		_ = binary.Write(&ticks, binary.BigEndian, v)
	}
	ticks.Write([]byte{0, 0, 1, 0, 0, 1}) // partial second record
	if _, err := DecodeTicksBi5(bytes.NewReader(compressLZMA(t, ticks.Bytes())), time.Time{}, 5); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected wrapped ErrUnexpectedEOF for ticks, got %v", err)
	}

	bars := make([]byte, 24+10)
	if _, err := DecodeBarsBi5(bytes.NewReader(compressLZMA(t, bars)), time.Time{}, 5); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected wrapped ErrUnexpectedEOF for bars, got %v", err)
	}
}

func TestBi5TruncatedCompressedStreamIsAnError(t *testing.T) {
	raw := make([]byte, 20*200)
	for i := range raw {
		raw[i] = byte(rand.IntN(256))
	}
	compressed := compressLZMA(t, raw)
	truncated := compressed[:len(compressed)/2]
	got, err := DecodeTicksBi5(bytes.NewReader(truncated), time.Time{}, 5)
	if err == nil {
		t.Fatalf("expected error for truncated stream, got %d ticks", len(got))
	}
	// The intact stream still decodes cleanly.
	full, err := DecodeTicksBi5(bytes.NewReader(compressed), time.Time{}, 5)
	if err != nil || len(full) != 200 {
		t.Fatalf("intact stream: %d ticks, %v", len(full), err)
	}
}

// --- 13. partial first bucket is kept ---

func TestAggregateKeepsBucketOverlappingFrom(t *testing.T) {
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	from := base.Add(2 * time.Minute)
	to := base.Add(7 * time.Minute)
	source := []Bar{
		{Time: base.Add(1 * time.Minute), Open: 9, High: 99, Low: 9, Close: 9, Volume: 100}, // before from
		{Time: base.Add(2 * time.Minute), Open: 1, High: 2, Low: 1, Close: 2, Volume: 1},
		{Time: base.Add(3 * time.Minute), Open: 2, High: 3, Low: 2, Close: 3, Volume: 1},
		{Time: base.Add(5 * time.Minute), Open: 3, High: 4, Low: 3, Close: 4, Volume: 1},
		{Time: base.Add(7 * time.Minute), Open: 8, High: 8, Low: 8, Close: 8, Volume: 100}, // at to
	}
	bars, err := AggregateBars(source, GranularityM5, from, to)
	if err != nil {
		t.Fatalf("AggregateBars: %v", err)
	}
	if len(bars) != 2 {
		t.Fatalf("expected first (partial) and second bucket, got %+v", bars)
	}
	if !bars[0].Time.Equal(base) || bars[0].Open != 1 || bars[0].High != 3 || bars[0].Volume != 2 {
		t.Fatalf("unexpected first bucket (must hold only in-range bars): %+v", bars[0])
	}

	ticks := []Tick{
		{Time: base.Add(30 * time.Second), Bid: 50, BidVolume: 100}, // before from
		{Time: base.Add(150 * time.Second), Bid: 1, BidVolume: 1},
		{Time: base.Add(330 * time.Second), Bid: 2, BidVolume: 1},
	}
	tickBars, err := AggregateTicksToBars(ticks, GranularityM5, "bid", from, to)
	if err != nil {
		t.Fatalf("AggregateTicksToBars: %v", err)
	}
	if len(tickBars) != 2 || !tickBars[0].Time.Equal(base) || tickBars[0].Open != 1 || tickBars[0].Volume != 1 {
		t.Fatalf("unexpected tick bars: %+v", tickBars)
	}
}

// --- 14. FilterInstruments limits ---

func TestFilterInstrumentsNonPositiveLimitReturnsAll(t *testing.T) {
	instruments := []Instrument{{Name: "EUR/USD", Code: "EURUSD"}, {Name: "GBP/USD", Code: "GBPUSD"}}
	for _, limit := range []int{-1, 0, 5} {
		if got := FilterInstruments(instruments, "usd", limit); len(got) != 2 {
			t.Fatalf("limit %d: got %d", limit, len(got))
		}
		if got := FilterInstruments(instruments, "", limit); len(got) != 2 {
			t.Fatalf("empty query limit %d: got %d", limit, len(got))
		}
	}
	if got := FilterInstruments(instruments, "usd", 1); len(got) != 1 {
		t.Fatalf("limit 1: got %d", len(got))
	}
}

// --- 15. proxies ---

func TestProxyLoaderRejectsBadLinesAndEmptyFiles(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}

	pool := &ProxyPool{}
	if err := pool.LoadFromFile(write("good.txt", "127.0.0.1:8080\nsocks5h://user:secret@10.0.0.1:1080\n")); err != nil {
		t.Fatalf("good file: %v", err)
	}

	err := pool.LoadFromFile(write("bad.txt", "127.0.0.1:8080\nftp://10.0.0.1:21\nhttp://\nhttp://user:secret@%zz\n"))
	if err == nil {
		t.Fatal("expected error for bad lines")
	}
	for _, want := range []string{"line 2", "line 3", "line 4"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q should name %s", err, want)
		}
	}
	if strings.Contains(err.Error(), "secret") {
		t.Fatalf("error leaks credentials: %v", err)
	}

	if err := pool.LoadFromFile(write("empty.txt", "# only comments\n\n")); !errors.Is(err, ErrNoProxies) {
		t.Fatalf("expected ErrNoProxies, got %v", err)
	}

	// The pool keeps the last good list after failed loads.
	req, _ := http.NewRequest(http.MethodGet, "https://example.com", nil)
	first, _ := pool.GetNextProxy(req)
	second, _ := pool.GetNextProxy(req)
	if first == nil || second == nil || first.Host != "127.0.0.1:8080" || second.Scheme != "socks5h" {
		t.Fatalf("pool changed after failed load: %v %v", first, second)
	}
}

// --- 16. outlier cleaning matches the previous implementation exactly ---

func referenceMedian(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)
	mid := len(sorted) / 2
	if len(sorted)%2 == 0 {
		return (sorted[mid-1] + sorted[mid]) / 2.0
	}
	return sorted[mid]
}

func referenceMAD(values []float64, med float64) float64 {
	if len(values) == 0 {
		return 0
	}
	diffs := make([]float64, len(values))
	for i, v := range values {
		diffs[i] = math.Abs(v - med)
	}
	return referenceMedian(diffs)
}

// referenceCleanTickOutliers is the pre-optimization CleanTickOutliers.
func referenceCleanTickOutliers(ticks []Tick, window int, threshold float64) []Tick {
	n := len(ticks)
	if n < window || window < 3 {
		return ticks
	}
	cleaned := make([]Tick, 0, n)
	halfWin := window / 2
	for i := 0; i < n; i++ {
		start := max(i-halfWin, 0)
		end := min(i+halfWin+1, n)
		slice := ticks[start:end]
		prices := make([]float64, len(slice))
		for k, t := range slice {
			prices[k] = (t.Bid + t.Ask) / 2.0
		}
		med := referenceMedian(prices)
		mad := referenceMAD(prices, med)
		diff := math.Abs((ticks[i].Bid+ticks[i].Ask)/2.0 - med)
		cutoff := threshold * mad
		if cutoff == 0 {
			cutoff = med * 0.01
		}
		if diff > cutoff {
			continue
		}
		cleaned = append(cleaned, ticks[i])
	}
	return cleaned
}

// referenceCleanBarOutliers is the pre-optimization CleanBarOutliers.
func referenceCleanBarOutliers(bars []Bar, window int, threshold float64) []Bar {
	n := len(bars)
	if n < window || window < 3 {
		return bars
	}
	cleaned := make([]Bar, 0, n)
	halfWin := window / 2
	for i := 0; i < n; i++ {
		start := max(i-halfWin, 0)
		end := min(i+halfWin+1, n)
		slice := bars[start:end]
		closes := make([]float64, len(slice))
		for k, b := range slice {
			closes[k] = b.Close
		}
		med := referenceMedian(closes)
		mad := referenceMAD(closes, med)
		b := bars[i]
		cutoff := threshold * mad
		if cutoff == 0 {
			cutoff = med * 0.02
		}
		if math.Abs(b.High-med) > cutoff*2 || math.Abs(b.Low-med) > cutoff*2 || math.Abs(b.Close-med) > cutoff {
			if b.High > med+cutoff {
				b.High = math.Max(b.Open, b.Close)
			}
			if b.Low < med-cutoff && med-cutoff > 0 {
				b.Low = math.Min(b.Open, b.Close)
			}
			if math.Abs(b.Close-med) > cutoff {
				continue
			}
		}
		cleaned = append(cleaned, b)
	}
	return cleaned
}

func TestOutlierCleanersMatchReferenceImplementation(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for trial := 0; trial < 200; trial++ {
		n := rng.IntN(300)
		ticks := make([]Tick, n)
		bars := make([]Bar, n)
		price := 1.1
		for i := 0; i < n; i++ {
			price += (rng.Float64() - 0.5) * 0.001
			p := price
			switch rng.IntN(25) {
			case 0:
				p *= 1 + rng.Float64()*5 // spike
			case 1:
				p = price // flat stretch helps produce MAD == 0
			}
			if rng.IntN(8) == 0 && i > 0 {
				p = ticks[i-1].Bid // duplicates
			}
			ticks[i] = Tick{Time: time.Unix(int64(i), 0), Bid: p, Ask: p + 0.0002}
			bars[i] = Bar{Time: time.Unix(int64(i), 0), Open: price, Close: p, High: math.Max(price, p) + rng.Float64()*0.01, Low: math.Min(price, p) - rng.Float64()*0.01}
		}
		window := 3 + rng.IntN(12)
		threshold := 0.5 + rng.Float64()*6

		gotTicks := CleanTickOutliers(ticks, window, threshold)
		wantTicks := referenceCleanTickOutliers(ticks, window, threshold)
		if len(gotTicks) != len(wantTicks) {
			t.Fatalf("trial %d ticks: len %d vs %d", trial, len(gotTicks), len(wantTicks))
		}
		for i := range gotTicks {
			if gotTicks[i] != wantTicks[i] {
				t.Fatalf("trial %d tick %d differs: %+v vs %+v", trial, i, gotTicks[i], wantTicks[i])
			}
		}

		gotBars := CleanBarOutliers(bars, window, threshold)
		wantBars := referenceCleanBarOutliers(bars, window, threshold)
		if len(gotBars) != len(wantBars) {
			t.Fatalf("trial %d bars: len %d vs %d", trial, len(gotBars), len(wantBars))
		}
		for i := range gotBars {
			if gotBars[i] != wantBars[i] {
				t.Fatalf("trial %d bar %d differs: %+v vs %+v", trial, i, gotBars[i], wantBars[i])
			}
		}
	}
}

func TestOutlierCleanersDoNotAllocatePerElement(t *testing.T) {
	ticks := make([]Tick, 1000)
	for i := range ticks {
		ticks[i] = Tick{Bid: 1 + float64(i%7)*0.0001, Ask: 1.0002}
	}
	allocs := testing.AllocsPerRun(5, func() {
		_ = CleanTickOutliers(ticks, 7, 5)
	})
	if allocs > 10 {
		t.Fatalf("expected a constant number of allocations, got %.0f", allocs)
	}
}
