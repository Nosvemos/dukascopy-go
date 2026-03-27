package e2e

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestManifestRepairCanRedownloadGaps(t *testing.T) {
	gapServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/instruments":
			writeJSON(w, map[string]any{
				"instruments": []map[string]any{
					{"id": 1, "name": "XAU/USD", "code": "XAU-USD", "description": "Gold vs US Dollar", "priceScale": 3},
				},
			})
		case "/v1/candles/minute/XAU-USD/BID/2024/1/2":
			writeJSON(w, map[string]any{
				"timestamp":  1704153600000,
				"multiplier": 1.0,
				"open":       100.0,
				"high":       101.0,
				"low":        99.0,
				"close":      100.5,
				"shift":      60000,
				"times":      []int{0, 1, 2, 1},
				"opens":      []float64{0, 0.5, 0.75, 1.0},
				"highs":      []float64{0, 0.25, 0.75, 1.0},
				"lows":       []float64{0, 0.5, 1.25, 1.5},
				"closes":     []float64{0, 0.25, 0.75, 1.0},
				"volumes":    []float64{0.0011, 0.0009, 0.0008, 0.0007},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer gapServer.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-gap-repair.csv")
	manifestPath := outputPath + ".manifest.json"

	downloadOutput := runCLI(
		t,
		gapServer.URL,
		"download",
		"--symbol", "xauusd",
		"--timeframe", "m1",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:04:00Z",
		"--output", outputPath,
		"--simple",
		"--partition", "hour",
	)
	if !strings.Contains(downloadOutput, "wrote 4 bars") {
		t.Fatalf("unexpected initial download output: %s", downloadOutput)
	}

	fullServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/instruments":
			writeJSON(w, map[string]any{
				"instruments": []map[string]any{
					{"id": 1, "name": "XAU/USD", "code": "XAU-USD", "description": "Gold vs US Dollar", "priceScale": 3},
				},
			})
		case "/v1/candles/minute/XAU-USD/BID/2024/1/2":
			writeJSON(w, map[string]any{
				"timestamp":  1704153600000,
				"multiplier": 1.0,
				"open":       100.0,
				"high":       101.0,
				"low":        99.0,
				"close":      100.5,
				"shift":      60000,
				"times":      []int{0, 1, 1, 1, 1},
				"opens":      []float64{0, 0.5, 0.75, 1.0, 1.25},
				"highs":      []float64{0, 0.25, 0.75, 1.0, 1.25},
				"lows":       []float64{0, 0.5, 1.25, 1.5, 1.75},
				"closes":     []float64{0, 0.25, 0.75, 1.0, 1.25},
				"volumes":    []float64{0.0011, 0.0009, 0.0008, 0.0007, 0.0006},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer fullServer.Close()

	repairOutput := runCLI(
		t,
		fullServer.URL,
		"manifest", "repair",
		"--manifest", manifestPath,
		"--redownload-gaps",
	)
	if !strings.Contains(repairOutput, "re-downloaded 1 gap part(s)") {
		t.Fatalf("unexpected repair output: %s", repairOutput)
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read repaired output: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "2024-01-02T00:02:00Z,101.250,102.000,100.750,101.500,800") {
		t.Fatalf("expected repaired gap row, got: %s", content)
	}
	if !strings.Contains(content, "2024-01-02T00:04:00Z") {
		t.Fatalf("expected repaired dataset to keep the final row, got: %s", content)
	}
}
