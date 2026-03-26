package e2e

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

var builtCLI string

func TestMain(m *testing.M) {
	root := repoRoot()
	builtCLI = filepath.Join(os.TempDir(), "dukascopy-data-e2e.exe")

	build := exec.Command("go", "build", "-o", builtCLI, "./cmd/dukascopy")
	build.Dir = root
	output, err := build.CombinedOutput()
	if err != nil {
		panic("failed to build CLI: " + err.Error() + "\n" + string(output))
	}

	code := m.Run()
	_ = os.Remove(builtCLI)
	os.Exit(code)
}

func runCLI(t *testing.T, baseURL string, args ...string) string {
	t.Helper()

	cmd := exec.Command(builtCLI, args...)
	cmd.Dir = repoRoot()
	cmd.Env = append(os.Environ(), "DUKASCOPY_API_BASE_URL="+baseURL, "NO_COLOR=1")

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("command failed: %v\n%s", err, string(output))
	}

	return string(output)
}

func runCLIExpectError(t *testing.T, baseURL string, args ...string) string {
	t.Helper()

	cmd := exec.Command(builtCLI, args...)
	cmd.Dir = repoRoot()
	cmd.Env = append(os.Environ(), "DUKASCOPY_API_BASE_URL="+baseURL, "NO_COLOR=1")

	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected command to fail, but it succeeded\n%s", string(output))
	}

	return string(output)
}

func repoRoot() string {
	_, currentFile, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(currentFile), ".."))
}

func newMockServer() *httptest.Server {
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
				{
					"id":          2,
					"name":        "EUR/USD",
					"code":        "EUR-USD",
					"description": "Euro vs US Dollar",
					"priceScale":  5,
				},
				{
					"id":          3,
					"name":        "BTC/USD",
					"code":        "BTC-USD",
					"description": "Bitcoin vs US Dollar",
					"priceScale":  1,
				},
			},
		})
	})

	mux.HandleFunc("/v1/candles/minute/XAU-USD/BID/2024/1/2", func(w http.ResponseWriter, r *http.Request) {
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

	mux.HandleFunc("/v1/candles/minute/XAU-USD/ASK/2024/1/2", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"timestamp":  1704153600000,
			"multiplier": 1.0,
			"open":       100.2,
			"high":       101.2,
			"low":        99.2,
			"close":      100.7,
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

	mux.HandleFunc("/v1/candles/minute/XAU-USD/ASK/2024/1/3", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"timestamp":  1704240000000,
			"multiplier": 1.0,
			"open":       102.2,
			"high":       103.2,
			"low":        101.2,
			"close":      102.7,
			"shift":      60000,
			"times":      []int{0, 1, 1},
			"opens":      []float64{0, 0.5, 0.75},
			"highs":      []float64{0, 0.25, 0.75},
			"lows":       []float64{0, 0.5, 1.25},
			"closes":     []float64{0, 0.25, 0.75},
			"volumes":    []float64{0.0014, 0.0012, 0.0010},
		})
	})

	mux.HandleFunc("/v1/candles/hour/XAU-USD/BID/2024/1", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"timestamp":  1704153600000,
			"multiplier": 1.0,
			"open":       100.0,
			"high":       101.0,
			"low":        99.0,
			"close":      100.5,
			"shift":      3600000,
			"times":      []int{0, 1, 1, 1},
			"opens":      []float64{0, 1, 1, 1},
			"highs":      []float64{0, 1, 1, 1},
			"lows":       []float64{0, 1, 1, 1},
			"closes":     []float64{0, 1, 1, 1},
			"volumes":    []float64{0.002, 0.003, 0.004, 0.005},
		})
	})

	mux.HandleFunc("/v1/candles/day/XAU-USD/BID/2024", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"timestamp":  1704067200000,
			"multiplier": 1.0,
			"open":       100.0,
			"high":       101.0,
			"low":        99.0,
			"close":      100.5,
			"shift":      86400000,
			"times":      []int{0, 1, 1},
			"opens":      []float64{0, 1, 1},
			"highs":      []float64{0, 1, 1},
			"lows":       []float64{0, 1, 1},
			"closes":     []float64{0, 1, 1},
			"volumes":    []float64{0.002, 0.003, 0.004},
		})
	})

	mux.HandleFunc("/v1/ticks/XAU-USD/2024/1/2/0", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"timestamp":  1704153600000,
			"multiplier": 1.0,
			"ask":        100.2,
			"bid":        100.0,
			"times":      []int{0, 500, 500},
			"asks":       []float64{0, 0.1, 0.2},
			"bids":       []float64{0, 0.1, 0.2},
			"askVolumes": []float64{10, 20, 30},
			"bidVolumes": []float64{15, 25, 35},
		})
	})

	return httptest.NewServer(mux)
}

func writeJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}
