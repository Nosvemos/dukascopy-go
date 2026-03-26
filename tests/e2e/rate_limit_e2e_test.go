package e2e

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestDownloadRateLimitSpacesHTTPRequests(t *testing.T) {
	var (
		mu           sync.Mutex
		requestTimes []time.Time
	)

	recordRequest := func() {
		mu.Lock()
		requestTimes = append(requestTimes, time.Now())
		mu.Unlock()
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/instruments", func(w http.ResponseWriter, r *http.Request) {
		recordRequest()
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
		recordRequest()
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

	server := httptest.NewServer(mux)
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-rate-limit.csv")
	runCLI(
		t,
		server.URL,
		"download",
		"--symbol", "xauusd",
		"--timeframe", "m1",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:03:00Z",
		"--output", outputPath,
		"--simple",
		"--rate-limit", "150ms",
	)

	mu.Lock()
	defer mu.Unlock()
	if len(requestTimes) < 2 {
		t.Fatalf("expected at least two HTTP requests, got %d", len(requestTimes))
	}

	gap := requestTimes[1].Sub(requestTimes[0])
	if gap < 120*time.Millisecond {
		t.Fatalf("expected rate-limited request spacing, got %s", gap)
	}
}
