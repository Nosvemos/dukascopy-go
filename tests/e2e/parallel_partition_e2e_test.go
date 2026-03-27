package e2e

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestPartitionedDownloadCanRunInParallel(t *testing.T) {
	var inflight atomic.Int32
	var maxInflight atomic.Int32

	startRequest := func() func() {
		current := inflight.Add(1)
		for {
			previous := maxInflight.Load()
			if current <= previous || maxInflight.CompareAndSwap(previous, current) {
				break
			}
		}
		time.Sleep(150 * time.Millisecond)
		return func() {
			inflight.Add(-1)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/instruments", func(w http.ResponseWriter, r *http.Request) {
		done := startRequest()
		defer done()

		writeJSON(w, map[string]any{
			"instruments": []map[string]any{
				{
					"id":          1,
					"name":        "XAU/USD",
					"code":        "XAU-USD",
					"description": "Gold vs US Dollar",
					"priceScale":  3,
				},
			},
		})
	})
	mux.HandleFunc("/v1/candles/minute/XAU-USD/BID/2024/1/2", func(w http.ResponseWriter, r *http.Request) {
		done := startRequest()
		defer done()

		writeJSON(w, map[string]any{
			"timestamp":  1704153600000,
			"multiplier": 1.0,
			"open":       100.0,
			"high":       101.0,
			"low":        99.0,
			"close":      100.5,
			"shift":      60000,
			"times":      []int{0, 1, 1},
			"opens":      []float64{0, 0.5, 0.75},
			"highs":      []float64{0, 0.25, 0.75},
			"lows":       []float64{0, 0.5, 1.25},
			"closes":     []float64{0, 0.25, 0.75},
			"volumes":    []float64{0.0011, 0.0009, 0.0008},
		})
	})
	mux.HandleFunc("/v1/candles/minute/XAU-USD/BID/2024/1/3", func(w http.ResponseWriter, r *http.Request) {
		done := startRequest()
		defer done()

		writeJSON(w, map[string]any{
			"timestamp":  1704240000000,
			"multiplier": 1.0,
			"open":       102.0,
			"high":       103.0,
			"low":        101.0,
			"close":      102.5,
			"shift":      60000,
			"times":      []int{0, 1, 1},
			"opens":      []float64{0, 0.5, 0.75},
			"highs":      []float64{0, 0.25, 0.75},
			"lows":       []float64{0, 0.5, 1.25},
			"closes":     []float64{0, 0.25, 0.75},
			"volumes":    []float64{0.0014, 0.0012, 0.0010},
		})
	})
	mux.HandleFunc("/v1/candles/minute/XAU-USD/BID/2024/1/4", func(w http.ResponseWriter, r *http.Request) {
		done := startRequest()
		defer done()

		writeJSON(w, map[string]any{
			"timestamp":  1704326400000,
			"multiplier": 1.0,
			"open":       104.0,
			"high":       105.0,
			"low":        103.0,
			"close":      104.5,
			"shift":      60000,
			"times":      []int{0, 1, 1},
			"opens":      []float64{0, 0.5, 0.75},
			"highs":      []float64{0, 0.25, 0.75},
			"lows":       []float64{0, 0.5, 1.25},
			"closes":     []float64{0, 0.25, 0.75},
			"volumes":    []float64{0.0016, 0.0013, 0.0011},
		})
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-parallel.csv")
	output := runCLI(
		t,
		server.URL,
		"download",
		"--symbol", "xauusd",
		"--timeframe", "m1",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-04T00:00:00Z",
		"--output", outputPath,
		"--simple",
		"--partition", "auto",
		"--parallelism", "2",
		"--retries", "0",
	)

	if !strings.Contains(output, "wrote 7 bars") {
		t.Fatalf("unexpected partitioned output: %s", output)
	}
	if maxInflight.Load() < 2 {
		t.Fatalf("expected at least two concurrent HTTP requests, max inflight was %d", maxInflight.Load())
	}
}
