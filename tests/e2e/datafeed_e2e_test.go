package e2e

import (
	"bytes"
	"encoding/binary"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/ulikunitz/xz/lzma"
)

func TestDatafeedDownloadE2E(t *testing.T) {
	// 1. Create mock lzma bytes for ticks
	var rawTicks bytes.Buffer
	binary.Write(&rawTicks, binary.BigEndian, uint32(100))           // 100ms
	binary.Write(&rawTicks, binary.BigEndian, uint32(100050))        // ask
	binary.Write(&rawTicks, binary.BigEndian, uint32(100000))        // bid
	binary.Write(&rawTicks, binary.BigEndian, math.Float32bits(1.5)) // ask volume
	binary.Write(&rawTicks, binary.BigEndian, math.Float32bits(1.0)) // bid volume

	var compressedTicks bytes.Buffer
	w1, _ := lzma.NewWriter(&compressedTicks)
	w1.Write(rawTicks.Bytes())
	w1.Close()

	// 2. Create mock lzma bytes for minute candles
	var rawCandles bytes.Buffer
	binary.Write(&rawCandles, binary.BigEndian, uint32(60))              // 60s
	binary.Write(&rawCandles, binary.BigEndian, uint32(100000))          // open
	binary.Write(&rawCandles, binary.BigEndian, uint32(100050))          // close
	binary.Write(&rawCandles, binary.BigEndian, uint32(99990))           // low
	binary.Write(&rawCandles, binary.BigEndian, uint32(100080))          // high
	binary.Write(&rawCandles, binary.BigEndian, math.Float32bits(100.5)) // volume

	var compressedCandles bytes.Buffer
	w2, _ := lzma.NewWriter(&compressedCandles)
	w2.Write(rawCandles.Bytes())
	w2.Close()

	// Start mock http server
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/instruments", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"instruments":[{"id":1,"name":"EUR/USD","code":"EURUSD","description":"EUR/USD","priceScale":5}]}`))
	})
	mux.HandleFunc("/datafeed/EURUSD/2024/00/02/10h_ticks.bi5", func(w http.ResponseWriter, r *http.Request) {
		w.Write(compressedTicks.Bytes())
	})
	mux.HandleFunc("/datafeed/EURUSD/2024/00/02/BID_candles_min_1.bi5", func(w http.ResponseWriter, r *http.Request) {
		w.Write(compressedCandles.Bytes())
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	// 3. E2E Tick Download with datafeed engine
	tickOutput := filepath.Join(t.TempDir(), "ticks_df.csv")
	runCLI(
		t,
		server.URL,
		"download",
		"--symbol", "eurusd",
		"--timeframe", "tick",
		"--from", "2024-01-02T10:00:00Z",
		"--to", "2024-01-02T11:00:00Z",
		"--output", tickOutput,
		"--simple",
		"--engine", "datafeed",
	)

	tickData, err := os.ReadFile(tickOutput)
	if err != nil {
		t.Fatalf("Failed to read ticks output: %v", err)
	}
	if !bytes.Contains(tickData, []byte("1.0005")) {
		t.Errorf("expected ask price 1.0005 in tick output, got: %s", string(tickData))
	}

	// 4. E2E Minute Candle Download with datafeed engine
	candleOutput := filepath.Join(t.TempDir(), "candles_df.csv")
	runCLI(
		t,
		server.URL,
		"download",
		"--symbol", "eurusd",
		"--timeframe", "m1",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:05:00Z",
		"--output", candleOutput,
		"--simple",
		"--engine", "datafeed",
	)

	candleData, err := os.ReadFile(candleOutput)
	if err != nil {
		t.Fatalf("Failed to read candles output: %v", err)
	}
	if !bytes.Contains(candleData, []byte("1.0005")) {
		t.Errorf("expected close price 1.0005 in candle output, got: %s", string(candleData))
	}
}
