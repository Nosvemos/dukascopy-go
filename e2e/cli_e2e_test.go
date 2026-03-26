package e2e

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
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

func TestInstrumentsCommandE2E(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	output := runCLI(t, server.URL, "instruments", "--query", "xauusd", "--limit", "1")
	if !strings.Contains(output, "XAU/USD\tXAU-USD\tGold vs US Dollar") {
		t.Fatalf("unexpected instruments output: %s", output)
	}
}

func TestDownloadMinuteCommandE2E(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "xauusd-minute.csv")
	output := runCLI(
		t,
		server.URL,
		"download",
		"--symbol", "xauusd",
		"--granularity", "minute",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:03:00Z",
		"--output", outputPath,
	)

	if !strings.Contains(output, "wrote 3 bars") {
		t.Fatalf("unexpected download output: %s", output)
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, "instrument,time,open,high,low,close,volume") {
		t.Fatalf("missing csv header: %s", content)
	}
	if !strings.Contains(content, "XAU-USD,2024-01-02T00:00:00Z,100.000,101.000,99.000,100.500,1.1") {
		t.Fatalf("missing first bar: %s", content)
	}
	if !strings.Contains(content, "XAU-USD,2024-01-02T00:02:00Z,101.250,102.000,100.250,101.500,0.8") {
		t.Fatalf("missing last bar: %s", content)
	}
}

func runCLI(t *testing.T, baseURL string, args ...string) string {
	t.Helper()

	cmd := exec.Command(builtCLI, args...)
	cmd.Dir = repoRoot()
	cmd.Env = append(os.Environ(), "DUKASCOPY_API_BASE_URL="+baseURL)

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("command failed: %v\n%s", err, string(output))
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
			"lows":       []float64{0, 0.5, 0.75},
			"closes":     []float64{0, 0.25, 0.75},
			"volumes":    []float64{1.1, 0.9, 0.8},
		})
	})

	return httptest.NewServer(mux)
}

func writeJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}
