package dukascopy

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ulikunitz/xz/lzma"
)

func TestClientDatafeedEngine(t *testing.T) {
	// Create mock lzma bytes for ticks
	var rawTicks bytes.Buffer
	binary.Write(&rawTicks, binary.BigEndian, uint32(100))               // 100ms
	binary.Write(&rawTicks, binary.BigEndian, uint32(100050))             // ask
	binary.Write(&rawTicks, binary.BigEndian, uint32(100000))             // bid
	binary.Write(&rawTicks, binary.BigEndian, math.Float32bits(1.5))      // ask volume
	binary.Write(&rawTicks, binary.BigEndian, math.Float32bits(1.0))      // bid volume

	var compressedTicks bytes.Buffer
	w1, _ := lzma.NewWriter(&compressedTicks)
	w1.Write(rawTicks.Bytes())
	w1.Close()

	// Create mock lzma bytes for minute candles
	var rawCandles bytes.Buffer
	binary.Write(&rawCandles, binary.BigEndian, uint32(60))               // 60s
	binary.Write(&rawCandles, binary.BigEndian, uint32(100000))            // open
	binary.Write(&rawCandles, binary.BigEndian, uint32(100050))            // close
	binary.Write(&rawCandles, binary.BigEndian, uint32(99990))             // low
	binary.Write(&rawCandles, binary.BigEndian, uint32(100080))            // high
	binary.Write(&rawCandles, binary.BigEndian, math.Float32bits(100.5))   // volume

	var compressedCandles bytes.Buffer
	w2, _ := lzma.NewWriter(&compressedCandles)
	w2.Write(rawCandles.Bytes())
	w2.Close()

	// Start mock http server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/instruments" {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"instruments":[{"id":1,"name":"EUR/USD","code":"EURUSD","description":"EUR/USD","priceScale":5}]}`))
			return
		}
		if r.URL.Path == "/datafeed/EURUSD/2024/00/02/10h_ticks.bi5" {
			w.Write(compressedTicks.Bytes())
			return
		}
		if r.URL.Path == "/datafeed/EURUSD/2024/00/02/BID_candles_min_1.bi5" {
			w.Write(compressedCandles.Bytes())
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := NewClient(server.URL, 5*time.Second).WithEngine(EngineDatafeed)

	// 1. Download ticks
	ctx := context.Background()
	reqTick := DownloadRequest{
		Symbol:      "eurusd",
		Granularity: GranularityTick,
		Side:        PriceSideBid,
		From:        time.Date(2024, 1, 2, 10, 0, 0, 0, time.UTC),
		To:          time.Date(2024, 1, 2, 11, 0, 0, 0, time.UTC),
	}
	resTick, err := client.Download(ctx, reqTick)
	if err != nil {
		t.Fatalf("Download ticks failed: %v", err)
	}
	if len(resTick.Ticks) != 1 {
		t.Fatalf("expected 1 tick, got %d", len(resTick.Ticks))
	}
	if resTick.Ticks[0].Ask != 1.00050 || resTick.Ticks[0].Bid != 1.00000 {
		t.Errorf("unexpected tick prices: %v", resTick.Ticks[0])
	}

	// 2. Download minute bars
	reqBar := DownloadRequest{
		Symbol:      "eurusd",
		Granularity: GranularityM1,
		Side:        PriceSideBid,
		From:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		To:          time.Date(2024, 1, 2, 23, 59, 0, 0, time.UTC),
	}
	resBar, err := client.Download(ctx, reqBar)
	if err != nil {
		t.Fatalf("Download bars failed: %v", err)
	}
	if len(resBar.Bars) != 1 {
		t.Fatalf("expected 1 bar, got %d", len(resBar.Bars))
	}
	if resBar.Bars[0].Open != 1.00000 || resBar.Bars[0].Close != 1.00050 {
		t.Errorf("unexpected bar prices: %v", resBar.Bars[0])
	}
}
