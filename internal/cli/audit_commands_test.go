package cli

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/checkpoint"
	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

// treatSideErrorsAsNoData makes a failing bid/ask bar download look like
// "no data" (HTTP 404), the only case loadBidAskBarsWithTicks falls back to
// ticks for.
func treatSideErrorsAsNoData(t *testing.T) {
	t.Helper()
	original := downloadBarsForSide
	downloadBarsForSide = func(ctx context.Context, client *dukascopy.Client, request dukascopy.DownloadRequest, side dukascopy.PriceSide) (dukascopy.Instrument, []dukascopy.Bar, error) {
		instrument, bars, err := original(ctx, client, request, side)
		if err != nil {
			return dukascopy.Instrument{}, nil, &dukascopy.HTTPStatusError{StatusCode: 404, URL: "test"}
		}
		return instrument, bars, nil
	}
	t.Cleanup(func() { downloadBarsForSide = original })
}

func TestLoadBidAskBarsDoesNotFallBackOnServerErrors(t *testing.T) {
	fake := newFakeDukascopy(t)
	fake.failStatus = 500
	fake.failPath = "/ASK/"
	client, err := dukascopy.NewClient(fake.URL(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client = client.WithRetries(0).WithBackoff(time.Millisecond)
	request := dukascopy.DownloadRequest{Symbol: "xauusd", Granularity: dukascopy.GranularityM1, Side: dukascopy.PriceSideBid,
		From: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), To: time.Date(2024, 1, 2, 0, 3, 0, 0, time.UTC)}

	_, _, _, _, usedFallback, err := loadBidAskBarsWithTicks(context.Background(), client, request)
	var statusErr *dukascopy.HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != 500 || usedFallback {
		t.Fatalf("expected the 500 error without fallback, got err=%v fallback=%v", err, usedFallback)
	}
	if ticks := fake.count("/v1/ticks"); ticks != 0 {
		t.Fatalf("a 500 must not trigger a tick download, got %d tick requests", ticks)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, _, _, _, err := loadBidAskBarsWithTicks(ctx, client, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if ticks := fake.count("/v1/ticks"); ticks != 0 {
		t.Fatalf("cancellation must not trigger a tick download, got %d tick requests", ticks)
	}

	// No data (404) still falls back to ticks.
	original := downloadBarsForSide
	t.Cleanup(func() { downloadBarsForSide = original })
	downloadBarsForSide = func(ctx context.Context, client *dukascopy.Client, request dukascopy.DownloadRequest, side dukascopy.PriceSide) (dukascopy.Instrument, []dukascopy.Bar, error) {
		return dukascopy.Instrument{}, nil, dukascopy.ErrNoData
	}
	_, bid, ask, _, usedFallback, err := loadBidAskBarsWithTicks(context.Background(), client, request)
	if err != nil || !usedFallback || len(bid) == 0 || len(ask) == 0 {
		t.Fatalf("expected tick fallback for no data: err=%v fallback=%v bid=%d ask=%d", err, usedFallback, len(bid), len(ask))
	}
}

func TestSyncAcceptsDownloadFlagsAndChecksTimeframe(t *testing.T) {
	fake := newFakeDukascopy(t)
	restoreCSVGlobals(t)
	dir := t.TempDir()

	// An hourly file.
	hourly := filepath.Join(dir, "h1.csv")
	content := "timestamp,open,high,low,close,volume\n" +
		"2024-01-02T00:00:00Z,1,1,1,1,1\n2024-01-02T01:00:00Z,1,1,1,1,1\n2024-01-02T02:00:00Z,1,1,1,1,1\n"
	if err := os.WriteFile(hourly, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	err := runSync([]string{"--symbol", "xauusd", "--output", hourly, "--base-url", fake.URL()}, &out, &out)
	if err == nil || !strings.Contains(err.Error(), "looks like h1") || !strings.Contains(err.Error(), "--timeframe h1") {
		t.Fatalf("expected a timeframe mismatch error, got %v", err)
	}

	// --timeframe is accepted and matches; boolean flags are forwarded
	// without swallowing the next argument.
	err = runSync([]string{"--symbol", "xauusd", "--timeframe", "h1", "--resume", "--no-progress", "--output", hourly,
		"--base-url", fake.URL(), "--to", "2024-01-02T03:00:00Z", "--retries", "0"}, &out, &out)
	if err != nil && strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatalf("sync rejected a download flag: %v", err)
	}
	if err != nil && strings.Contains(err.Error(), "looks like") {
		t.Fatalf("matching timeframe was refused: %v", err)
	}
	if fake.count("/v1/candles/hour") == 0 {
		t.Fatalf("sync did not download hourly candles (err=%v)\n%s", err, out.String())
	}

	// A semicolon file written with a preset layout is detected as well.
	minute := filepath.Join(dir, "m1.csv")
	if err := os.WriteFile(minute, []byte("timestamp;open\n2024.01.02 00:00:00;1\n2024.01.02 00:01:00;1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	interval, ok, err := detectFileInterval(minute, "", "UTC")
	if err != nil || !ok || interval != time.Minute {
		t.Fatalf("detectFileInterval = %s %v %v, want 1m", interval, ok, err)
	}

	forwarded := forwardedFlags(func() *flag.FlagSet {
		fs, _ := newDownloadFlagSet(&out)
		_ = fs.Parse([]string{"--resume", "--timeframe", "h1", "--symbol", "x"})
		return fs
	}(), map[string]bool{"symbol": true})
	if strings.Join(forwarded, " ") != "--resume=true --timeframe=h1" {
		t.Fatalf("unexpected forwarded flags %q", forwarded)
	}
}

func TestWatchLoopReportsFailuresAndStops(t *testing.T) {
	evaluator, err := parseWatchCondition("price > 1")
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	calls := 0
	fetch := func(context.Context, time.Time, time.Time) ([]watchSample, error) {
		calls++
		return nil, errors.New("unknown instrument")
	}
	err = runWatchLoop(context.Background(), watchOptions{symbol: "nope", condition: "price > 1", granularity: dukascopy.GranularityM1, pollInterval: time.Millisecond, maxFailures: 3}, evaluator, fetch, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "3 consecutive download failures") || calls != 3 {
		t.Fatalf("expected to give up after 3 failures, got err=%v calls=%d", err, calls)
	}
	if strings.Count(stderr.String(), "download failed") != 3 {
		t.Fatalf("each failure must be reported on stderr:\n%s", stderr.String())
	}
}

func TestWatchLoopWarmsUpIndicatorsAndWidensLookback(t *testing.T) {
	evaluator, err := parseWatchCondition("rsi:14 < 101")
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	var windows []time.Duration
	fetch := func(_ context.Context, from time.Time, to time.Time) ([]watchSample, error) {
		windows = append(windows, to.Sub(from))
		// Only a few bars inside the first windows (e.g. a weekend).
		n := 5
		if len(windows) >= 3 {
			n = 40
		}
		samples := make([]watchSample, n)
		for i := range samples {
			price := 100 + float64(i%3)
			samples[i] = watchSample{Time: time.Unix(int64(i)*3600, 0), Close: price, Bid: price}
		}
		return samples, nil
	}
	err = runWatchLoop(context.Background(), watchOptions{symbol: "eurusd", condition: "rsi:14 < 101", granularity: dukascopy.GranularityH1, pollInterval: time.Millisecond, maxFailures: 3, once: true}, evaluator, fetch, &stdout, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(stderr.String(), "warming up") != 1 || !strings.Contains(stderr.String(), "needs 15 bars") {
		t.Fatalf("expected one warm-up note:\n%s", stderr.String())
	}
	if strings.Contains(stdout.String(), "NaN") || !strings.Contains(stdout.String(), "ALERT TRIGGERED") {
		t.Fatalf("expected an alert with a real RSI value, got:\n%s", stdout.String())
	}
	if len(windows) < 3 || windows[1] <= windows[0] {
		t.Fatalf("lookback did not widen while warming up: %v", windows)
	}
	if first := initialWatchLookback(dukascopy.GranularityH1, 15); first < 15*time.Hour {
		t.Fatalf("initial h1 lookback %s cannot hold 15 bars", first)
	}
}

func TestWatchConditionsAndValidation(t *testing.T) {
	spread, err := parseWatchCondition("spread > 0.0001")
	if err != nil {
		t.Fatal(err)
	}
	result := spread.Evaluate([]watchSample{{Bid: 1.1000, Ask: 1.1003, Close: 1.1000, HasAsk: true}})
	if !result.ready || !result.triggered || math.Abs(result.value-0.0003) > 1e-9 {
		t.Fatalf("spread evaluated to %+v", result)
	}
	if result := spread.Evaluate([]watchSample{{Bid: 1, Close: 1}}); result.ready {
		t.Fatal("spread without ask data must not evaluate")
	}
	if !spread.needsAsk() {
		t.Fatal("spread needs ask data")
	}

	tickPrice, _ := parseWatchCondition("price < 2")
	if result := tickPrice.Evaluate(samplesFromTicks([]dukascopy.Tick{{Bid: 1.5, Ask: 1.6}})); !result.triggered {
		t.Fatalf("tick price condition did not trigger: %+v", result)
	}
	if err := tickPrice.validateFor(dukascopy.GranularityTick); err != nil {
		t.Fatal(err)
	}
	volume, _ := parseWatchCondition("volume > 1")
	if err := volume.validateFor(dukascopy.GranularityTick); err == nil {
		t.Fatal("volume is not available for ticks")
	}
	if _, err := parseWatchCondition("macd > 1"); err == nil || !strings.Contains(err.Error(), "unknown metric") {
		t.Fatalf("unknown metrics must be rejected, got %v", err)
	}

	var out bytes.Buffer
	for name, args := range map[string][]string{
		"zero poll interval": {"--symbol", "eurusd", "--condition", "price > 1", "--poll-interval", "0s"},
		"bad timeframe":      {"--symbol", "eurusd", "--condition", "price > 1", "--timeframe", "m2"},
		"unknown metric":     {"--symbol", "eurusd", "--condition", "foo > 1"},
		"tick high":          {"--symbol", "eurusd", "--condition", "high > 1", "--timeframe", "tick"},
	} {
		if err := runWatch(args, &out, &out); err == nil {
			t.Fatalf("%s: expected a validation error", name)
		}
	}

	out.Reset()
	printUsage(&out)
	if !strings.Contains(out.String(), "watch ") {
		t.Fatalf("usage does not list watch:\n%s", out.String())
	}
}

func TestConfigProfileIsOneSetting(t *testing.T) {
	fake := newFakeDukascopy(t)
	simple := true
	activeConfig = &appConfig{Download: downloadDefaultsConfig{Simple: &simple}}
	t.Cleanup(func() { activeConfig = nil })

	dir := t.TempDir()
	for _, flagArgs := range [][]string{{"--full"}, {"--fused"}, {"--custom-columns", "timestamp,close"}} {
		output := filepath.Join(dir, strings.TrimPrefix(flagArgs[0], "--")+".csv")
		args := append(append(append([]string{}, threeDayArgs...), "--output", output, "--cache-dir", filepath.Join(dir, "cache")), flagArgs...)
		if logs, err := runTestDownload(t, fake, args...); err != nil {
			t.Fatalf("%v with config simple=true: %v\n%s", flagArgs, err, logs)
		}
	}
	rows := readDelimited(t, filepath.Join(dir, "custom-columns.csv"), ',')
	if strings.Join(rows[0], ",") != "timestamp,close" {
		t.Fatalf("unexpected custom columns header %q", rows[0])
	}
}

func TestRunExitCodes(t *testing.T) {
	var out bytes.Buffer
	if code := Run([]string{"download", "--no-such-flag"}, &out, &out); code != 2 {
		t.Fatalf("flag errors must exit 2, got %d", code)
	}
	if code := Run([]string{"watch", "--poll-interval", "soon"}, &out, &out); code != 2 {
		t.Fatalf("invalid flag values must exit 2, got %d", code)
	}
	if code := Run([]string{"download", "--symbol", "x"}, &out, &out); code != 1 {
		t.Fatalf("runtime errors must exit 1, got %d", code)
	}
	if code := Run([]string{"stats", "--help"}, &out, &out); code != 0 {
		t.Fatalf("--help must exit 0, got %d", code)
	}
	if code := Run([]string{"nope"}, &out, &out); code != 2 {
		t.Fatalf("unknown commands must exit 2, got %d", code)
	}
}

func TestLoadersAcceptPresetFiles(t *testing.T) {
	reader := newDetectedCSVReader(strings.NewReader("timestamp;open;high;low;close;volume\n20240102 000000;1;2;0.5;1.5;10\n"))
	header, err := reader.Read()
	if err != nil || len(header) != 6 {
		t.Fatalf("semicolon header not detected: %q %v", header, err)
	}
	row, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	colIndex := buildColumnIndex(header)
	want := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

	line, err := buildLineProtocol(row, colIndex, "bars", "")
	if err != nil || !strings.HasSuffix(line, " "+strconv.FormatInt(want.UnixMilli(), 10)) {
		t.Fatalf("influx line protocol for a ninjatrader row: %q %v", line, err)
	}

	for _, value := range []string{"2024.01.02 00:00", "2024-01-02 00:00:00", "2024-01-02T00:00:00Z", "1704153600000"} {
		got, err := parseLoadTimestamp(value)
		if err != nil || !got.Equal(want) {
			t.Fatalf("parseLoadTimestamp(%q) = %v, %v", value, got, err)
		}
	}
}

func TestGapRepairReadsPresetParts(t *testing.T) {
	dir := t.TempDir()
	partOne := filepath.Join(dir, "a.csv")
	partTwo := filepath.Join(dir, "b.csv")
	if err := os.WriteFile(partOne, []byte("timestamp;open\n2024.01.02 00:00:00;1\n2024.01.02 00:01:00;1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(partTwo, []byte("timestamp;open\n2024.01.02 00:05:00;1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	indexes, err := detectManifestGapPartIndexes(checkpoint.Manifest{
		PartsDir: dir,
		Symbol:   "XAUUSD",
		Parts:    []checkpoint.ManifestPart{{ID: "a", File: "a.csv"}, {ID: "b", File: "b.csv"}},
	}, time.Minute)
	if err != nil {
		t.Fatalf("preset/semicolon parts must be readable: %v", err)
	}
	if len(indexes) == 0 {
		t.Fatal("expected the 4-minute gap to be detected")
	}
}

func TestRedactURLHidesPassword(t *testing.T) {
	got := redactURL("redis://user:s3cret@localhost:6379/market")
	if strings.Contains(got, "s3cret") || !strings.Contains(got, "localhost:6379") {
		t.Fatalf("redactURL = %q", got)
	}
}

func TestRunSDKDownloadValidatesBeforeDownloading(t *testing.T) {
	var requests atomic.Int64
	fake := newFakeDukascopy(t)
	t.Setenv("DUKASCOPY_API_BASE_URL", fake.URL())
	restoreCSVGlobals(t)
	from := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	valid := SDKDownloadOptions{Symbol: "xauusd", Timeframe: "m1", Side: "bid", From: from, To: from.Add(2 * time.Minute), OutputPath: filepath.Join(t.TempDir(), "out.csv"), Engine: "jetta"}

	cases := map[string]func(*SDKDownloadOptions){
		"to before from": func(o *SDKDownloadOptions) { o.To = o.From.Add(-time.Hour) },
		"empty output":   func(o *SDKDownloadOptions) { o.OutputPath = "" },
		"empty symbol":   func(o *SDKDownloadOptions) { o.Symbol = "" },
		"bad timeframe":  func(o *SDKDownloadOptions) { o.Timeframe = "m2" },
		"bad side":       func(o *SDKDownloadOptions) { o.Side = "mid" },
		"bad fill gaps":  func(o *SDKDownloadOptions) { o.FillGaps = "back" },
		"future from":    func(o *SDKDownloadOptions) { o.From = time.Now().Add(time.Hour); o.To = o.From.Add(time.Hour) },
	}
	for name, mutate := range cases {
		opts := valid
		mutate(&opts)
		if err := RunSDKDownload(context.Background(), opts); err == nil {
			t.Fatalf("%s: expected a validation error", name)
		}
	}
	requests.Store(fake.total.Load())
	if requests.Load() != 0 {
		t.Fatalf("validation must happen before any request, got %d", requests.Load())
	}

	// The base URL comes from DUKASCOPY_API_BASE_URL like the CLI.
	if err := RunSDKDownload(context.Background(), valid); err != nil {
		t.Fatalf("valid SDK download: %v", err)
	}
	if fake.count("/v1/candles/minute") == 0 {
		t.Fatal("SDK download did not use the configured base URL")
	}
	if rows := readDelimited(t, valid.OutputPath, ','); len(rows) != 4 {
		t.Fatalf("expected header + 3 rows, got %q", rows)
	}
}
