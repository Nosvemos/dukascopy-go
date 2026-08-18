package dukascopy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestCoverageBoostSamplingAndOutliers(t *testing.T) {
	// ParseBarType
	for _, valid := range []string{"time", "tick", "ticks", "volume", "vol", "dollar", "value"} {
		bt, err := ParseBarType(valid)
		if err != nil {
			t.Errorf("unexpected error parsing bar type %q: %v", valid, err)
		}
		if bt == "" {
			t.Errorf("empty bar type for %q", valid)
		}
	}
	if _, err := ParseBarType("invalid_type"); err == nil {
		t.Errorf("expected error for invalid bar type")
	}

	// SampleTicksToCustomBars
	now := time.Now().UTC()
	ticks := []Tick{
		{Time: now, Bid: 1.1000, Ask: 1.1002, BidVolume: 100, AskVolume: 100},
		{Time: now.Add(time.Second), Bid: 1.1005, Ask: 1.1007, BidVolume: 200, AskVolume: 200},
		{Time: now.Add(2 * time.Second), Bid: 1.1010, Ask: 1.1012, BidVolume: 300, AskVolume: 300},
	}

	// Tick sampling
	tickBars, err := SampleTicksToCustomBars(ticks, BarTypeTick, 2, PriceSideBid)
	if err != nil || len(tickBars) != 2 {
		t.Fatalf("unexpected tick sampling result: len=%d, err=%v", len(tickBars), err)
	}

	// Volume sampling
	volBars, err := SampleTicksToCustomBars(ticks, BarTypeVolume, 300, PriceSideBid)
	if err != nil || len(volBars) == 0 {
		t.Fatalf("unexpected volume sampling result: len=%d, err=%v", len(volBars), err)
	}

	// Dollar sampling
	dollarBars, err := SampleTicksToCustomBars(ticks, BarTypeDollar, 300, PriceSideBid)
	if err != nil || len(dollarBars) == 0 {
		t.Fatalf("unexpected dollar sampling result: len=%d, err=%v", len(dollarBars), err)
	}

	// Invalid bar size
	if _, err := SampleTicksToCustomBars(ticks, BarTypeTick, 0, PriceSideBid); err == nil {
		t.Errorf("expected error for zero bar size")
	}

	// Invalid bar type sampling
	if _, err := SampleTicksToCustomBars(ticks, BarType("unknown"), 100, PriceSideBid); err == nil {
		t.Errorf("expected error for unknown bar type")
	}

	// EnsureBarOrder
	unorderedBars := []Bar{
		{Time: now.Add(2 * time.Minute), Close: 1.10},
		{Time: now, Close: 1.08},
		{Time: now.Add(-time.Minute), Close: 1.07},
	}
	EnsureBarOrder(unorderedBars)
	if !unorderedBars[1].Time.After(unorderedBars[0].Time) || !unorderedBars[2].Time.After(unorderedBars[1].Time) {
		t.Errorf("bars not ordered properly: %+v", unorderedBars)
	}

	// Empty and single element EnsureBarOrder
	EnsureBarOrder(nil)
	EnsureBarOrder([]Bar{{Time: now}})

	// CleanBarOutliers
	barsWithSpike := []Bar{
		{Time: now, Open: 100, High: 101, Low: 99, Close: 100},
		{Time: now.Add(time.Minute), Open: 100, High: 101, Low: 99, Close: 100},
		{Time: now.Add(2 * time.Minute), Open: 100, High: 101, Low: 99, Close: 100},
		{Time: now.Add(3 * time.Minute), Open: 100, High: 500, Low: 99, Close: 500}, // Outlier spike
		{Time: now.Add(4 * time.Minute), Open: 100, High: 101, Low: 99, Close: 100},
		{Time: now.Add(5 * time.Minute), Open: 100, High: 101, Low: 99, Close: 100},
		{Time: now.Add(6 * time.Minute), Open: 100, High: 101, Low: 99, Close: 100},
	}
	cleanedBars := CleanBarOutliers(barsWithSpike, 5, 3.0)
	if len(cleanedBars) >= len(barsWithSpike) {
		t.Errorf("expected outlier bar to be filtered out, got %d bars", len(cleanedBars))
	}

	// CleanBarOutliers short or window < 3
	if len(CleanBarOutliers(barsWithSpike, 2, 3.0)) != len(barsWithSpike) {
		t.Errorf("expected no filtering when window < 3")
	}
	if len(CleanBarOutliers([]Bar{{Time: now}}, 5, 3.0)) != 1 {
		t.Errorf("expected no filtering for short slice")
	}

	// ParseOutlierConfig error branches
	if _, err := ParseOutlierConfig("median:not_num:3.0"); err == nil {
		t.Errorf("expected error for invalid window")
	}
	if _, err := ParseOutlierConfig("median:5:not_num"); err == nil {
		t.Errorf("expected error for invalid threshold")
	}
	if _, err := ParseOutlierConfig("unknown_strategy"); err == nil {
		t.Errorf("expected error for unknown strategy")
	}

	// Count total functions
	totalB := countTotalBars([][]Bar{{{Time: now}}, {{Time: now}}})
	if totalB != 2 {
		t.Errorf("expected total 2 bars, got %d", totalB)
	}
	totalT := countTotalTicks([][]Tick{{{Time: now}}, {{Time: now}}})
	if totalT != 2 {
		t.Errorf("expected total 2 ticks, got %d", totalT)
	}

	// Instruments cache test
	tmpCache := t.TempDir() + "/inst_cache.json"
	setLocalCacheFilePath(tmpCache)
	defer setLocalCacheFilePath("")

	insts := []Instrument{{Name: "EURUSD", Description: "Euro vs US Dollar"}}
	saveLocalCache(insts)
	loaded, ok := loadLocalCache()
	if !ok || len(loaded) != 1 || loaded[0].Name != "EURUSD" {
		t.Errorf("cache load failed: ok=%v, loaded=%v", ok, loaded)
	}

	// ListInstruments memory cache & local cache
	client, err := NewClient("https://datafeed.dukascopy.com", 10*time.Second)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	client.WithEngine(EngineDatafeed)
	res, err := client.ListInstruments(context.Background())
	if err != nil || len(res) == 0 {
		t.Fatalf("ListInstruments failed: %v", err)
	}

	// Second call (memory cache hit)
	resCached, err := client.ListInstruments(context.Background())
	if err != nil || len(resCached) == 0 {
		t.Fatalf("ListInstruments memory cache hit failed: %v", err)
	}
}

func TestCoverageBoostHTTPRawBytes(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "404") {
			http.NotFound(w, r)
			return
		}
		if strings.Contains(r.URL.Path, "500") {
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte("RAW_DATA_TEST"))
	}))
	defer ts.Close()

	u, _ := url.Parse(ts.URL)
	c := &Client{
		baseURL:    u,
		httpClient: ts.Client(),
		engine:     EngineDatafeed,
		maxRetries: 1,
	}

	// 200 OK
	data, err := c.getRawBytes(context.Background(), []string{"data", "200"})
	if err != nil || string(data) != "RAW_DATA_TEST" {
		t.Errorf("unexpected 200 getRawBytes result: %s, %v", string(data), err)
	}

	// 404 Not Found
	data404, err := c.getRawBytes(context.Background(), []string{"data", "404"})
	if err != nil || data404 != nil {
		t.Errorf("expected nil data for 404, got %v, %v", data404, err)
	}

	// 500 Server Error
	_, err500 := c.getRawBytes(context.Background(), []string{"data", "500"})
	if err500 == nil {
		t.Errorf("expected error for 500 status")
	}
}
