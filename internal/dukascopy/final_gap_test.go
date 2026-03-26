package dukascopy

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAggregateAdditionalBranches(t *testing.T) {
	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(3 * time.Minute)

	t.Run("aggregate ticks rejects invalid side", func(t *testing.T) {
		if _, err := AggregateTicksToBars(nil, GranularityM1, PriceSide("wat"), from, to); err == nil {
			t.Fatal("expected invalid side error")
		}
	})

	t.Run("aggregate ticks ask side and range filtering", func(t *testing.T) {
		ticks := []Tick{
			{Time: from.Add(-time.Second), Bid: 10, Ask: 11, BidVolume: 1, AskVolume: 2},
			{Time: from.Add(10 * time.Second), Bid: 12, Ask: 13, BidVolume: 3, AskVolume: 4},
			{Time: from.Add(50 * time.Second), Bid: 8, Ask: 9, BidVolume: 5, AskVolume: 6},
			{Time: from.Add(70 * time.Second), Bid: 7, Ask: 8, BidVolume: 7, AskVolume: 8},
			{Time: to.Add(time.Second), Bid: 20, Ask: 21, BidVolume: 9, AskVolume: 10},
		}

		bars, err := AggregateTicksToBars(ticks, GranularityM1, PriceSideAsk, from, to)
		if err != nil {
			t.Fatalf("AggregateTicksToBars returned error: %v", err)
		}
		if len(bars) != 2 {
			t.Fatalf("expected 2 ask bars in range, got %d", len(bars))
		}
		if bars[0].Open != 13 || bars[0].High != 13 || bars[0].Low != 9 || bars[0].Close != 9 || bars[0].Volume != 10 {
			t.Fatalf("unexpected first ask bar: %+v", bars[0])
		}
		if bars[1].Open != 8 || bars[1].High != 8 || bars[1].Low != 8 || bars[1].Close != 8 || bars[1].Volume != 8 {
			t.Fatalf("unexpected second ask bar: %+v", bars[1])
		}
	})

	t.Run("aggregate bars pass through and filter", func(t *testing.T) {
		source := []Bar{
			{Time: from.Add(-time.Minute), Open: 1, High: 2, Low: 0.5, Close: 1.5, Volume: 1},
			{Time: from, Open: 2, High: 3, Low: 1.5, Close: 2.5, Volume: 2},
			{Time: from.Add(time.Minute), Open: 3, High: 4, Low: 0.25, Close: 3.5, Volume: 3},
			{Time: to, Open: 4, High: 5, Low: 3.5, Close: 4.5, Volume: 4},
		}

		bars, err := AggregateBars(source, GranularityM1, from, to)
		if err != nil {
			t.Fatalf("AggregateBars returned error: %v", err)
		}
		if len(bars) != 2 {
			t.Fatalf("expected 2 filtered bars, got %d", len(bars))
		}
	})

	t.Run("aggregate bars updates low within bucket", func(t *testing.T) {
		source := []Bar{
			{Time: from, Open: 2, High: 3, Low: 1.5, Close: 2.5, Volume: 2},
			{Time: from.Add(time.Minute), Open: 3, High: 4, Low: 0.25, Close: 3.5, Volume: 3},
		}

		bars, err := AggregateBars(source, GranularityM5, from, to.Add(5*time.Minute))
		if err != nil {
			t.Fatalf("AggregateBars returned error: %v", err)
		}
		if len(bars) != 1 || bars[0].Low != 0.25 {
			t.Fatalf("expected aggregated low update, got %+v", bars)
		}
	})

	t.Run("aggregate bars rejects unsupported granularity", func(t *testing.T) {
		if _, err := AggregateBars(nil, GranularityTick, from, to); err == nil {
			t.Fatal("expected unsupported bar granularity error")
		}
	})
}

func TestBucketStartAndDecoderRemainingBranches(t *testing.T) {
	base := time.Date(2024, 1, 3, 14, 37, 42, 0, time.UTC)

	for granularity, want := range map[Granularity]time.Time{
		GranularityM1:  time.Date(2024, 1, 3, 14, 37, 0, 0, time.UTC),
		GranularityM3:  time.Date(2024, 1, 3, 14, 36, 0, 0, time.UTC),
		GranularityM15: time.Date(2024, 1, 3, 14, 30, 0, 0, time.UTC),
		GranularityM30: time.Date(2024, 1, 3, 14, 30, 0, 0, time.UTC),
		GranularityH1:  time.Date(2024, 1, 3, 14, 0, 0, 0, time.UTC),
		GranularityH4:  time.Date(2024, 1, 3, 12, 0, 0, 0, time.UTC),
		GranularityD1:  time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
	} {
		got, err := bucketStart(base, granularity)
		if err != nil {
			t.Fatalf("bucketStart(%s) returned error: %v", granularity, err)
		}
		if !got.Equal(want) {
			t.Fatalf("bucketStart(%s) = %s, want %s", granularity, got, want)
		}
	}

	t.Run("filter instruments with empty query and zero limit", func(t *testing.T) {
		if got := FilterInstruments([]Instrument{{Code: "XAU-USD"}}, "", 1); got != nil {
			t.Fatalf("expected nil empty-query result, got %+v", got)
		}
		if got := FilterInstruments([]Instrument{{Name: "Gold", Code: "XAU-USD", Description: "Gold vs US Dollar"}}, "gold", 0); len(got) != 0 {
			t.Fatalf("expected zero-limit result, got %+v", got)
		}
		got := FilterInstruments([]Instrument{
			{Name: "Zulu Alpha", Code: "ZZZ-USD"},
			{Name: "Alpha Alpha", Code: "AAA-USD"},
		}, "alpha", 10)
		if len(got) != 2 {
			t.Fatalf("expected limit clamped to candidate count, got %+v", got)
		}
		if got[0].Name != "Alpha Alpha" || got[1].Name != "Zulu Alpha" {
			t.Fatalf("expected stable tie sort by name, got %+v", got)
		}

		got = FilterInstruments([]Instrument{
			{Name: "Gamma", Code: "XAU-USD", Description: "Gold metal"},
			{Name: "Alpha", Code: "USD", Description: "USD alias"},
		}, "usd", 2)
		if len(got) != 2 || got[0].Code != "USD" {
			t.Fatalf("expected higher score to sort first, got %+v", got)
		}
	})

	t.Run("resolve instrument stable sort and scoring branches", func(t *testing.T) {
		instruments := []Instrument{
			{Name: "Zulu Alpha", Code: "ZZZ-USD", Description: "Metal"},
			{Name: "Alpha Alpha", Code: "AAA-USD", Description: "Metal"},
		}
		match, err := ResolveInstrument(instruments, "alpha")
		if err != nil {
			t.Fatalf("ResolveInstrument returned error: %v", err)
		}
		if match.Name != "Alpha Alpha" {
			t.Fatalf("expected stable-name tiebreaker to pick Alpha, got %+v", match)
		}

		cases := []struct {
			raw  string
			want int
		}{
			{raw: "AAA-USD", want: 100},
			{raw: "Alpha Asset", want: 95},
			{raw: "aaausd", want: 90},
			{raw: "alphaasset", want: 85},
			{raw: "asset", want: 65},
			{raw: "example", want: 50},
		}
		instrument := Instrument{Name: "Alpha Asset", Code: "AAA-USD", Description: "Alpha asset example"}
		for _, tc := range cases {
			if got := scoreInstrument(instrument, tc.raw, compactSymbol(tc.raw)); got != tc.want {
				t.Fatalf("scoreInstrument(%q) = %d, want %d", tc.raw, got, tc.want)
			}
		}

		preferred, err := ResolveInstrument([]Instrument{
			{Name: "Alpha Asset", Code: "AAA-USD", Description: "Metal"},
			{Name: "Zulu", Code: "USD", Description: "Metal"},
		}, "usd")
		if err != nil {
			t.Fatalf("ResolveInstrument returned error for score ordering: %v", err)
		}
		if preferred.Code != "USD" {
			t.Fatalf("expected highest score candidate first, got %+v", preferred)
		}
	})

	if got := decodeBars(candlePayload{}); got != nil {
		t.Fatalf("expected nil bars for empty payload, got %+v", got)
	}
	if got := decodeTicks(tickPayload{}); got != nil {
		t.Fatalf("expected nil ticks for empty payload, got %+v", got)
	}
	if got := minLength(); got != 0 {
		t.Fatalf("expected empty minLength to return 0, got %d", got)
	}
}

func TestClientRemainingBranches(t *testing.T) {
	t.Run("download and side helpers return list errors", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "boom", http.StatusInternalServerError)
		}))
		defer server.Close()

		client := NewClient(server.URL, time.Second).WithRetries(0)
		request := DownloadRequest{
			Symbol:      "xauusd",
			Granularity: GranularityM1,
			Side:        PriceSideBid,
			From:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			To:          time.Date(2024, 1, 2, 0, 1, 0, 0, time.UTC),
		}
		if _, err := client.Download(context.Background(), request); err == nil {
			t.Fatal("expected Download list-instruments error")
		}
		if _, _, err := client.DownloadBarsForSide(context.Background(), request, PriceSideBid); err == nil {
			t.Fatal("expected DownloadBarsForSide list-instruments error")
		}
	})

	t.Run("download tick and aggregated bar paths bubble endpoint errors", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/v1/instruments":
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"instruments":[{"id":1,"name":"XAU/USD","code":"XAU-USD","description":"Gold","priceScale":3}]}`))
			default:
				http.Error(w, "missing", http.StatusInternalServerError)
			}
		}))
		defer server.Close()

		client := NewClient(server.URL, time.Second).WithRetries(0)
		if _, err := client.Download(context.Background(), DownloadRequest{
			Symbol:      "xauusd",
			Granularity: GranularityTick,
			Side:        PriceSideBid,
			From:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			To:          time.Date(2024, 1, 2, 1, 0, 0, 0, time.UTC),
		}); err == nil {
			t.Fatal("expected tick download error")
		}
		if _, err := client.Download(context.Background(), DownloadRequest{
			Symbol:      "xauusd",
			Granularity: GranularityM1,
			Side:        PriceSideBid,
			From:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			To:          time.Date(2024, 1, 2, 0, 1, 0, 0, time.UTC),
		}); err == nil {
			t.Fatal("expected direct bar download error")
		}

		instrument := Instrument{Code: "XAU-USD"}
		if _, err := client.downloadBars(context.Background(), instrument, PriceSideBid, GranularityM3, time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), time.Date(2024, 1, 2, 0, 3, 0, 0, time.UTC)); err == nil {
			t.Fatal("expected aggregated minute error")
		}
		if _, err := client.downloadBars(context.Background(), instrument, PriceSideBid, GranularityH1, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC)); err == nil {
			t.Fatal("expected direct hourly error")
		}
		if _, err := client.downloadBars(context.Background(), instrument, PriceSideBid, GranularityH4, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC)); err == nil {
			t.Fatal("expected aggregated hourly error")
		}
		if _, err := client.downloadBars(context.Background(), instrument, PriceSideBid, GranularityMN1, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)); err == nil {
			t.Fatal("expected aggregated daily error")
		}
	})

	t.Run("getJSON handles transport error and canceled retry wait", func(t *testing.T) {
		client := NewClient("https://example.test", time.Second).WithRetries(0)
		client.httpClient = &http.Client{
			Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
				return nil, &net.DNSError{Err: "dial failed"}
			}),
		}
		if err := client.getJSON(context.Background(), []string{"v1", "instruments"}, &map[string]any{}); err == nil {
			t.Fatal("expected transport error")
		}

		retryServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "retry later", http.StatusTooManyRequests)
		}))
		defer retryServer.Close()

		retryClient := NewClient(retryServer.URL, time.Second).WithRetries(1).WithBackoff(50 * time.Millisecond)
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()
		if err := retryClient.getJSON(ctx, []string{"v1", "instruments"}, &map[string]any{}); err == nil {
			t.Fatal("expected canceled retry wait error")
		}

		waitClient := NewClient(retryServer.URL, time.Second).WithRetries(0).WithRateLimit(50 * time.Millisecond)
		waitClient.nextSlot = time.Now().Add(50 * time.Millisecond)
		waitCtx, waitCancel := context.WithCancel(context.Background())
		waitCancel()
		if err := waitClient.getJSON(waitCtx, []string{"v1", "instruments"}, &map[string]any{}); err == nil {
			t.Fatal("expected canceled rate-limit wait error")
		}
	})

	t.Run("waitForRateLimit waits successfully", func(t *testing.T) {
		client := NewClient("https://example.test", time.Second).WithRateLimit(10 * time.Millisecond)
		client.nextSlot = time.Now().Add(5 * time.Millisecond)
		if err := client.waitForRateLimit(context.Background()); err != nil {
			t.Fatalf("waitForRateLimit returned error: %v", err)
		}
	})

	t.Run("download bars for side resolves instrument errors", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"instruments":[{"id":1,"name":"XAU/USD","code":"XAU-USD","description":"Gold","priceScale":3}]}`))
		}))
		defer server.Close()

		client := NewClient(server.URL, time.Second)
		_, _, err := client.DownloadBarsForSide(context.Background(), DownloadRequest{
			Symbol:      "eurusd",
			Granularity: GranularityM1,
			From:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			To:          time.Date(2024, 1, 2, 0, 1, 0, 0, time.UTC),
		}, PriceSideBid)
		if err == nil {
			t.Fatal("expected unresolved symbol error")
		}
	})
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (fn roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}
