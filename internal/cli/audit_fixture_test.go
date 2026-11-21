package cli

import (
	"bytes"
	"encoding/csv"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/csvout"
)

// fakeDukascopy is a local Dukascopy API double that generates data for any
// date: minute candles at configurable minute offsets per day, daily candles
// for every day of a year and ticks for every hour.
type fakeDukascopy struct {
	server *httptest.Server
	// minuteOffsets returns the minute offsets of the candles of one day.
	minuteOffsets func(day time.Time) []int64
	// ticksPerHour is the number of ticks served per hour file.
	ticksPerHour int
	// failStatus, when set, is returned for paths containing failPath.
	failStatus int
	failPath   string

	mu       sync.Mutex
	requests map[string]int
	total    atomic.Int64
}

func newFakeDukascopy(t *testing.T) *fakeDukascopy {
	t.Helper()
	fake := &fakeDukascopy{
		minuteOffsets: func(time.Time) []int64 { return []int64{0, 1, 2} },
		ticksPerHour:  3,
		requests:      make(map[string]int),
	}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.handle))
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *fakeDukascopy) URL() string { return f.server.URL }

// count returns how many requests had a path starting with prefix.
func (f *fakeDukascopy) count(prefix string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for path, hits := range f.requests {
		if strings.HasPrefix(path, prefix) {
			n += hits
		}
	}
	return n
}

func (f *fakeDukascopy) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests[r.URL.Path]++
	f.mu.Unlock()
	f.total.Add(1)

	if f.failStatus != 0 && strings.Contains(r.URL.Path, f.failPath) {
		http.Error(w, "forced failure", f.failStatus)
		return
	}

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	atoi := func(s string) int { v, _ := strconv.Atoi(s); return v }
	switch {
	case r.URL.Path == "/v1/instruments":
		writeCLIJSON(w, map[string]any{"instruments": []map[string]any{
			{"id": 1, "name": "XAU/USD", "code": "XAU-USD", "description": "Gold", "priceScale": 3},
			{"id": 2, "name": "EUR/USD", "code": "EUR-USD", "description": "Euro", "priceScale": 5},
		}})
	case len(parts) == 8 && parts[1] == "candles" && parts[2] == "minute":
		day := time.Date(atoi(parts[5]), time.Month(atoi(parts[6])), atoi(parts[7]), 0, 0, 0, 0, time.UTC)
		offsets := f.minuteOffsets(day)
		times := make([]int64, len(offsets))
		deltas := make([]float64, len(offsets))
		volumes := make([]float64, len(offsets))
		previous := int64(0)
		for i, offset := range offsets {
			times[i] = offset - previous
			previous = offset
			if i > 0 {
				deltas[i] = 0.5
			}
			volumes[i] = 0.001
		}
		base := 100.0 + float64(day.Day())
		if parts[4] == "ASK" {
			base += 0.2
		}
		writeCLIJSON(w, map[string]any{
			"timestamp": day.UnixMilli(), "multiplier": 1.0, "shift": 60000,
			"open": base, "high": base + 1, "low": base - 1, "close": base,
			"times": times, "opens": deltas, "highs": deltas, "lows": deltas, "closes": deltas, "volumes": volumes,
		})
	case len(parts) == 6 && parts[1] == "candles" && parts[2] == "day":
		year := atoi(parts[5])
		start := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
		days := int(time.Date(year+1, 1, 1, 0, 0, 0, 0, time.UTC).Sub(start).Hours() / 24)
		times := make([]int64, days)
		zeros := make([]float64, days)
		volumes := make([]float64, days)
		for i := range times {
			if i > 0 {
				times[i] = 1
			}
			volumes[i] = 0.001
		}
		writeCLIJSON(w, map[string]any{
			"timestamp": start.UnixMilli(), "multiplier": 1.0, "shift": 86400000,
			"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5,
			"times": times, "opens": zeros, "highs": zeros, "lows": zeros, "closes": zeros, "volumes": volumes,
		})
	case len(parts) == 7 && parts[1] == "ticks":
		hour := time.Date(atoi(parts[3]), time.Month(atoi(parts[4])), atoi(parts[5]), atoi(parts[6]), 0, 0, 0, time.UTC)
		n := f.ticksPerHour
		times := make([]int64, n)
		deltas := make([]float64, n)
		volumes := make([]float64, n)
		for i := range times {
			if i > 0 {
				times[i] = 1000
				deltas[i] = 0.1
			}
			volumes[i] = 1
		}
		writeCLIJSON(w, map[string]any{
			"timestamp": hour.UnixMilli(), "multiplier": 1.0, "ask": 100.2, "bid": 100.0,
			"times": times, "asks": deltas, "bids": deltas, "askVolumes": volumes, "bidVolumes": volumes,
		})
	default:
		http.NotFound(w, r)
	}
}

// restoreCSVGlobals restores the csvout package defaults that runDownload
// mutates.
func restoreCSVGlobals(t *testing.T) {
	t.Helper()
	csvout.ConfigMutex.Lock()
	loc, format, delim, hide, fill := csvout.OutputLocation, csvout.OutputTimestampFormat, csvout.CSVDelimiter, csvout.HideCSVHeader, csvout.FillGaps
	csvout.ConfigMutex.Unlock()
	t.Cleanup(func() {
		csvout.ConfigMutex.Lock()
		csvout.OutputLocation, csvout.OutputTimestampFormat, csvout.CSVDelimiter, csvout.HideCSVHeader, csvout.FillGaps = loc, format, delim, hide, fill
		csvout.ConfigMutex.Unlock()
	})
}

// runTestDownload runs the download command against fake with a private
// cache directory.
func runTestDownload(t *testing.T, fake *fakeDukascopy, args ...string) (string, error) {
	t.Helper()
	restoreCSVGlobals(t)
	full := append([]string{"--base-url", fake.URL(), "--retries", "0", "--retry-backoff", "1ms", "--no-progress"}, args...)
	var stdout, stderr bytes.Buffer
	err := runDownload(full, &stdout, &stderr)
	return stdout.String() + stderr.String(), err
}

// readDelimited reads a whole CSV file with the given delimiter.
func readDelimited(t *testing.T, path string, delimiter rune) [][]string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	reader := csv.NewReader(bytes.NewReader(data))
	reader.Comma = delimiter
	reader.FieldsPerRecord = -1
	rows, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("parse %s: %v\n%s", path, err, data)
	}
	return rows
}

// writeChunk writes a chunk file in the internal format.
func writeChunk(t *testing.T, dir string, name string, header string, rows ...string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	content := header + "\n" + strings.Join(rows, "\n")
	if len(rows) > 0 {
		content += "\n"
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write chunk: %v", err)
	}
	return path
}

// listDir returns the names in dir.
func listDir(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}
