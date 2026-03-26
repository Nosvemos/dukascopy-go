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

func TestPartitionedDownloadCreatesManifestAndAssemblesFinalCSV(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-partitioned.csv")
	manifestPath := outputPath + ".checkpoint.json"

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
		"--checkpoint-manifest", manifestPath,
	)

	if !strings.Contains(output, "wrote 6 bars") {
		t.Fatalf("unexpected partitioned output: %s", output)
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	content := string(data)
	if strings.Count(content, "2024-01-02T") != 3 || strings.Count(content, "2024-01-03T") != 3 {
		t.Fatalf("expected rows from both partition days, got: %s", content)
	}

	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read checkpoint manifest: %v", err)
	}

	var manifest struct {
		Completed   bool `json:"completed"`
		FinalOutput struct {
			Rows   int    `json:"rows"`
			SHA256 string `json:"sha256"`
		} `json:"final_output"`
		Summary struct {
			TotalParts     int `json:"total_parts"`
			CompletedParts int `json:"completed_parts"`
			TotalRows      int `json:"total_rows"`
			OutputRows     int `json:"output_rows"`
		} `json:"summary"`
		Parts []struct {
			Status string `json:"status"`
			Rows   int    `json:"rows"`
		} `json:"parts"`
	}
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatalf("decode checkpoint manifest: %v", err)
	}
	if !manifest.Completed {
		t.Fatalf("expected manifest to be completed: %s", string(manifestData))
	}
	if len(manifest.Parts) != 2 {
		t.Fatalf("expected 2 checkpoint parts, got %d", len(manifest.Parts))
	}
	if manifest.FinalOutput.Rows != 6 || manifest.FinalOutput.SHA256 == "" {
		t.Fatalf("expected final output audit in manifest: %s", string(manifestData))
	}
	if manifest.Summary.TotalParts != 2 || manifest.Summary.CompletedParts != 2 || manifest.Summary.TotalRows != 6 || manifest.Summary.OutputRows != 6 {
		t.Fatalf("expected manifest summary to be populated: %s", string(manifestData))
	}
	for _, part := range manifest.Parts {
		if part.Status != "completed" || part.Rows != 3 {
			t.Fatalf("unexpected part state: %s", string(manifestData))
		}
	}
}

func TestPartitionedDownloadResumesFromCheckpoint(t *testing.T) {
	var dayTwoAttempts atomic.Int32
	var dayOneHealthyAttempts atomic.Int32

	firstMux := http.NewServeMux()
	firstMux.HandleFunc("/v1/instruments", func(w http.ResponseWriter, r *http.Request) {
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
	firstMux.HandleFunc("/v1/candles/minute/XAU-USD/BID/2024/1/2", func(w http.ResponseWriter, r *http.Request) {
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
	firstMux.HandleFunc("/v1/candles/minute/XAU-USD/BID/2024/1/3", func(w http.ResponseWriter, r *http.Request) {
		dayTwoAttempts.Add(1)
		http.Error(w, "simulated upstream outage", http.StatusBadGateway)
	})
	firstServer := httptest.NewServer(firstMux)
	defer firstServer.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-checkpoint.csv")
	manifestPath := outputPath + ".checkpoint.json"

	failedOutput := runCLIExpectError(
		t,
		firstServer.URL,
		"download",
		"--symbol", "xauusd",
		"--timeframe", "m1",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-04T00:00:00Z",
		"--output", outputPath,
		"--simple",
		"--partition", "auto",
		"--checkpoint-manifest", manifestPath,
		"--retries", "0",
	)
	if !strings.Contains(failedOutput, "simulated upstream outage") {
		t.Fatalf("unexpected first failure output: %s", failedOutput)
	}

	secondMux := http.NewServeMux()
	secondMux.HandleFunc("/v1/instruments", func(w http.ResponseWriter, r *http.Request) {
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
	secondMux.HandleFunc("/v1/candles/minute/XAU-USD/BID/2024/1/2", func(w http.ResponseWriter, r *http.Request) {
		dayOneHealthyAttempts.Add(1)
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
	secondMux.HandleFunc("/v1/candles/minute/XAU-USD/BID/2024/1/3", func(w http.ResponseWriter, r *http.Request) {
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
	secondServer := httptest.NewServer(secondMux)
	defer secondServer.Close()

	output := runCLI(
		t,
		secondServer.URL,
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
		t.Fatalf("unexpected resumed output: %s", output)
	}
	if dayOneHealthyAttempts.Load() != 0 {
		t.Fatalf("expected completed day-one partition to be reused, but it was downloaded again %d time(s)", dayOneHealthyAttempts.Load())
	}
	if dayTwoAttempts.Load() == 0 {
		t.Fatalf("expected first run to attempt the failing partition")
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	content := string(data)
	if strings.Count(content, "2024-01-02T") != 3 || strings.Count(content, "2024-01-03T") != 3 {
		t.Fatalf("expected resumed file to contain both days, got: %s", content)
	}
}
