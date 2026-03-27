package e2e

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestDownloadRetriesTransientFailures(t *testing.T) {
	var attempts atomic.Int32

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/instruments", func(w http.ResponseWriter, r *http.Request) {
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
		if attempts.Add(1) == 1 {
			http.Error(w, "temporary upstream failure", http.StatusBadGateway)
			return
		}
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

	outputPath := filepath.Join(t.TempDir(), "xauusd-minute-simple.csv")
	output := runCLI(
		t,
		server.URL,
		"download",
		"--symbol", "xauusd",
		"--timeframe", "m1",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:03:00Z",
		"--output", outputPath,
		"--simple",
		"--retries", "2",
		"--progress",
	)

	if !strings.Contains(output, "retry 1/3") {
		t.Fatalf("expected retry progress in output, got: %s", output)
	}
	if !strings.Contains(output, "wrote 3 bars") {
		t.Fatalf("unexpected download output: %s", output)
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	if !strings.Contains(string(data), "2024-01-02T00:02:00Z,101.250,102.000,100.750,101.500,800") {
		t.Fatalf("missing expected resumed row: %s", string(data))
	}
}
