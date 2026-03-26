package e2e

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestPartitionedDownloadRedownloadsTamperedPartFile(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-audit.csv")
	manifestPath := outputPath + ".checkpoint.json"

	runCLI(
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
		"--checkpoint-manifest", manifestPath,
	)

	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var manifest struct {
		Parts []struct {
			File string `json:"file"`
		} `json:"parts"`
	}
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if len(manifest.Parts) == 0 {
		t.Fatalf("expected at least one partition in manifest: %s", string(manifestData))
	}

	partPath := filepath.Join(outputPath+".parts", manifest.Parts[0].File)
	partData, err := os.ReadFile(partPath)
	if err != nil {
		t.Fatalf("read part file: %v", err)
	}
	tampered := strings.Replace(string(partData), "100.500", "100.501", 1)
	if err := os.WriteFile(partPath, []byte(tampered), 0o644); err != nil {
		t.Fatalf("tamper part file: %v", err)
	}

	var dayOneAttempts atomic.Int32
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
		dayOneAttempts.Add(1)
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
	healthyServer := httptest.NewServer(mux)
	defer healthyServer.Close()

	output := runCLI(
		t,
		healthyServer.URL,
		"download",
		"--symbol", "xauusd",
		"--timeframe", "m1",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-04T00:00:00Z",
		"--output", outputPath,
		"--simple",
		"--partition", "auto",
		"--checkpoint-manifest", manifestPath,
	)
	if !strings.Contains(output, "wrote 6 bars") {
		t.Fatalf("unexpected output: %s", output)
	}
	if dayOneAttempts.Load() == 0 {
		t.Fatal("expected tampered partition to be downloaded again")
	}

	finalData, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read final output: %v", err)
	}
	if strings.Contains(string(finalData), "100.501") {
		t.Fatalf("tampered value leaked into final output: %s", string(finalData))
	}
}
