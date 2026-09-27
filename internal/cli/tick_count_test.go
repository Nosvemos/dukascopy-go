package cli

import (
	"bytes"
	"context"
	"encoding/csv"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

func TestRunDownloadTickCountAndExplicitColumn(t *testing.T) {
	server := newCLITestServer()
	defer server.Close()

	cases := []struct {
		name               string
		profileArgs        []string
		baselineColumnArgs []string
		enableFlag         bool
	}{
		{name: "simple bars", profileArgs: []string{"--simple"}, enableFlag: true},
		{name: "bid ask bars", profileArgs: []string{"--full"}, enableFlag: true},
		{
			name:               "explicit custom column",
			profileArgs:        []string{"--custom-columns", "timestamp,close,volume,tick_count"},
			baselineColumnArgs: []string{"--custom-columns", "timestamp,close,volume"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			baseArgs := []string{
				"--symbol", "xauusd",
				"--timeframe", "m1",
				"--from", "2024-01-02T00:00:00Z",
				"--to", "2024-01-02T00:02:00Z",
				"--base-url", server.URL,
				"--no-progress",
			}

			baselinePath := filepath.Join(dir, "baseline.csv")
			baselineArgs := append([]string{}, baseArgs...)
			if tc.baselineColumnArgs != nil {
				baselineArgs = append(baselineArgs, tc.baselineColumnArgs...)
			} else {
				baselineArgs = append(baselineArgs, tc.profileArgs...)
			}
			baselineArgs = append(baselineArgs, "--output", baselinePath, "--cache-dir", filepath.Join(dir, "baseline-cache"))
			if err := runDownload(baselineArgs, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
				t.Fatalf("baseline runDownload: %v", err)
			}
			baselineRows := readTickCountCSV(t, baselinePath)

			countedPath := filepath.Join(dir, "counted.csv")
			countedArgs := append([]string{}, baseArgs...)
			countedArgs = append(countedArgs, tc.profileArgs...)
			countedArgs = append(countedArgs, "--output", countedPath, "--cache-dir", filepath.Join(dir, "counted-cache"))
			if tc.enableFlag {
				countedArgs = append(countedArgs, "--tick-count")
			}
			if err := runDownload(countedArgs, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
				t.Fatalf("tick-count runDownload: %v", err)
			}
			countedRows := readTickCountCSV(t, countedPath)

			if len(countedRows) != 3 || len(baselineRows) != 3 {
				t.Fatalf("expected header plus two bars, baseline=%d counted=%d", len(baselineRows), len(countedRows))
			}
			countIndex := csvColumnIndex(t, countedRows[0], "tick_count")
			volumeIndex := csvColumnIndex(t, countedRows[0], "volume")
			baselineVolumeIndex := csvColumnIndex(t, baselineRows[0], "volume")
			if countedRows[1][countIndex] != "2" || countedRows[2][countIndex] != "0" {
				t.Fatalf("unexpected per-minute counts: %q, %q", countedRows[1][countIndex], countedRows[2][countIndex])
			}
			for row := 1; row < len(countedRows); row++ {
				if got, want := countedRows[row][volumeIndex], baselineRows[row][baselineVolumeIndex]; got != want {
					t.Errorf("bar %d volume changed from %q to %q", row, want, got)
				}
			}
		})
	}
}

func TestRunDownloadTickCountRejectsUnsupportedModes(t *testing.T) {
	cases := []struct {
		name     string
		args     []string
		wantText string
	}{
		{
			name:     "raw tick timeframe",
			args:     []string{"--timeframe", "tick", "--from", "2024-01-02T00:00:00Z", "--to", "2024-01-02T00:01:00Z"},
			wantText: "time-based bars",
		},
		{
			name:     "live mode",
			args:     []string{"--live", "--last", "1h"},
			wantText: "historical downloads",
		},
		{
			name:     "custom bar type",
			args:     []string{"--bar-type", "tick", "--bar-size", "10", "--from", "2024-01-02T00:00:00Z", "--to", "2024-01-02T00:01:00Z"},
			wantText: "--bar-type time",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{"--symbol", "xauusd", "--output", filepath.Join(t.TempDir(), "out.csv"), "--tick-count"}
			args = append(args, tc.args...)
			err := runDownload(args, &bytes.Buffer{}, &bytes.Buffer{})
			if err == nil || !strings.Contains(err.Error(), tc.wantText) {
				t.Fatalf("runDownload error = %v, want text %q", err, tc.wantText)
			}
		})
	}
}

func TestRunDownloadTickCountSeparatesLegacyChunkCache(t *testing.T) {
	server := newCLITestServer()
	defer server.Close()
	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "shared-cache")
	baseArgs := []string{
		"--symbol", "xauusd",
		"--timeframe", "m1",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:02:00Z",
		"--simple",
		"--base-url", server.URL,
		"--no-progress",
		"--keep-cache",
		"--cache-dir", cacheDir,
	}

	plainArgs := append([]string{}, baseArgs...)
	plainArgs = append(plainArgs, "--output", filepath.Join(dir, "plain.csv"))
	if err := runDownload(plainArgs, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
		t.Fatalf("plain download: %v", err)
	}

	countedArgs := append([]string{}, baseArgs...)
	countedArgs = append(countedArgs, "--output", filepath.Join(dir, "counted.csv"), "--tick-count")
	if err := runDownload(countedArgs, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
		t.Fatalf("tick-count download after plain cached download: %v", err)
	}
	rows := readTickCountCSV(t, filepath.Join(dir, "counted.csv"))
	countIndex := csvColumnIndex(t, rows[0], "tick_count")
	if rows[1][countIndex] != "2" {
		t.Fatalf("stale plain-bar cache was reused: tick_count=%q", rows[1][countIndex])
	}
}

func TestLoadBidAskBarsTickCountReusesFallbackTicks(t *testing.T) {
	var tickRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/instruments":
			writeCLIJSON(w, map[string]any{"instruments": []map[string]any{{"id": 1, "name": "XAU/USD", "code": "XAU-USD", "description": "Gold", "priceScale": 3}}})
		case "/v1/candles/minute/XAU-USD/BID/2024/1/2", "/v1/candles/minute/XAU-USD/ASK/2024/1/2":
			http.Error(w, "force tick fallback", http.StatusInternalServerError)
		case "/v1/ticks/XAU-USD/2024/1/2/0":
			tickRequests.Add(1)
			writeCLIJSON(w, map[string]any{
				"timestamp":  1704153600000,
				"multiplier": 1.0,
				"ask":        100.2,
				"bid":        100.0,
				"times":      []int{0, 500},
				"asks":       []float64{0, 0.1},
				"bids":       []float64{0, 0.1},
				"askVolumes": []float64{10, 20},
				"bidVolumes": []float64{11, 21},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := dukascopy.NewClient(server.URL, time.Second)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	client = client.WithRetries(0).WithBackoff(time.Millisecond)
	request := dukascopy.DownloadRequest{
		Symbol:      "xauusd",
		Granularity: dukascopy.GranularityM1,
		Side:        dukascopy.PriceSideBid,
		From:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		To:          time.Date(2024, 1, 2, 0, 2, 0, 0, time.UTC),
	}
	instrument, bidBars, askBars, fallbackTicks, usedFallback, err := loadBidAskBarsWithTicks(context.Background(), client, request)
	if err != nil {
		t.Fatalf("loadBidAskBarsWithTicks: %v", err)
	}
	if !usedFallback || len(fallbackTicks) != 2 {
		t.Fatalf("expected the raw fallback tick stream, usedFallback=%v ticks=%d", usedFallback, len(fallbackTicks))
	}
	if err := applyTickCountsForColumns(context.Background(), client, request, []string{"timestamp", "tick_count"}, fallbackTicks, usedFallback, bidBars, askBars); err != nil {
		t.Fatalf("applyTickCountsForColumns: %v", err)
	}
	if len(bidBars) != 1 || len(askBars) != 1 || bidBars[0].TickCount != 2 || askBars[0].TickCount != 2 {
		t.Fatalf("unexpected fallback bar counts: bid=%+v ask=%+v instrument=%+v", bidBars, askBars, instrument)
	}
	if got := tickRequests.Load(); got != 1 {
		t.Fatalf("fallback tick stream was downloaded %d times, want once", got)
	}
}

func readTickCountCSV(t *testing.T, path string) [][]string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open CSV %s: %v", path, err)
	}
	defer file.Close()
	rows, err := csv.NewReader(file).ReadAll()
	if err != nil {
		t.Fatalf("read CSV %s: %v", path, err)
	}
	return rows
}

func csvColumnIndex(t *testing.T, header []string, column string) int {
	t.Helper()
	for i, name := range header {
		if name == column {
			return i
		}
	}
	t.Fatalf("column %q not found in header %v", column, header)
	return -1
}
