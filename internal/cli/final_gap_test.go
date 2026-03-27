package cli

import (
	"bytes"
	"context"
	"flag"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/csvout"
	"github.com/Nosvemos/dukascopy-go/internal/dukascopy"
)

func TestStatsConfigAndInstrumentGapBranches(t *testing.T) {
	t.Run("runStats validation and json output", func(t *testing.T) {
		var out bytes.Buffer
		if err := runStats([]string{"--bad-flag"}, &out); err == nil {
			t.Fatal("expected stats flag parse error")
		}
		if err := runStats(nil, &out); err == nil {
			t.Fatal("expected stats missing input error")
		}
		if err := runStats([]string{"--input", "missing.csv"}, &out); err == nil {
			t.Fatal("expected stats missing file error")
		}

		path := writeCSVFixture(t, "timestamp,open\n2024-01-01T00:00:00Z,1\n")
		out.Reset()
		if err := runStats([]string{"--input", path, "--json"}, &out); err != nil {
			t.Fatalf("runStats json returned error: %v", err)
		}
		if !strings.Contains(out.String(), "\"Format\": \"csv\"") {
			t.Fatalf("unexpected stats json output: %s", out.String())
		}
	})

	t.Run("config helpers", func(t *testing.T) {
		defer func() { activeConfig = nil }()

		if _, _, err := extractConfigPath([]string{"--config"}); err == nil {
			t.Fatal("expected --config missing value error")
		}
		if _, err := readConfigFile("missing.json"); err == nil {
			t.Fatal("expected missing config file error")
		}
		if _, err := loadActiveConfig([]string{"--config"}); err == nil {
			t.Fatal("expected loadActiveConfig parse error")
		}
		path := writeJSONFixture(t, `{"download":`)
		if _, err := loadActiveConfig([]string{"--config", path}); err == nil {
			t.Fatal("expected invalid config decode error")
		}

		activeConfig = &appConfig{
			BaseURL: "https://example.test",
			Download: downloadDefaultsConfig{
				Side:        "ask",
				Retries:     gapIntPtr(7),
				RateLimit:   "bad",
				Parallelism: gapIntPtr(3),
			},
		}
		fs := newTestFlagSet()
		timeframe := "m1"
		side := "bid"
		simpleOutput := false
		fullOutput := false
		customColumns := ""
		live := false
		pollInterval := time.Second
		retries := 1
		retryBackoff := time.Second
		rateLimit := time.Second
		progress := false
		resume := false
		partition := "none"
		parallelism := 1
		checkpointManifest := ""
		baseURL := ""
		if err := applyDownloadConfigDefaults(fs, &timeframe, &side, &simpleOutput, &fullOutput, &customColumns, &live, &pollInterval, &retries, &retryBackoff, &rateLimit, &progress, &resume, &partition, &parallelism, &checkpointManifest, &baseURL); err == nil {
			t.Fatal("expected invalid rate_limit config error")
		}
		if side != "ask" || retries != 7 {
			t.Fatalf("expected side/retries defaults to apply, got side=%s retries=%d", side, retries)
		}
	})

	t.Run("runInstruments validation and lookup branches", func(t *testing.T) {
		var out bytes.Buffer
		if err := runInstruments([]string{"--bad-flag"}, &out); err == nil {
			t.Fatal("expected instruments flag parse error")
		}
		if err := runInstruments(nil, &out); err == nil {
			t.Fatal("expected instruments missing query error")
		}
		if err := runInstruments([]string{"--query", "xauusd", "--limit", "0"}, &out); err == nil {
			t.Fatal("expected instruments bad limit error")
		}

		server := newCLITestServer()
		defer server.Close()
		if err := runInstruments([]string{"--query", "does-not-match", "--base-url", server.URL}, &out); err == nil {
			t.Fatal("expected no instruments found error")
		}
		if err := runInstruments([]string{"--query", "xauusd", "--base-url", "http://127.0.0.1:1"}, &out); err == nil {
			t.Fatal("expected instruments list error")
		}
	})
}

func TestDownloadAndResumeGapBranches(t *testing.T) {
	t.Run("runDownload validation branches", func(t *testing.T) {
		var out bytes.Buffer
		var errBuf bytes.Buffer

		cases := []struct {
			args []string
		}{
			{args: []string{}},
			{args: []string{"--symbol", "xauusd"}},
			{args: []string{"--symbol", "xauusd", "--from", "2024-01-01T00:00:00Z"}},
			{args: []string{"--symbol", "xauusd", "--from", "bad", "--to", "2024-01-01T01:00:00Z", "--output", "out.csv"}},
			{args: []string{"--symbol", "xauusd", "--from", "2024-01-01T00:00:00Z", "--to", "bad", "--output", "out.csv"}},
			{args: []string{"--symbol", "xauusd", "--from", "2024-01-01T01:00:00Z", "--to", "2024-01-01T00:00:00Z", "--output", "out.csv"}},
			{args: []string{"--symbol", "xauusd", "--from", "2024-01-01T00:00:00Z", "--to", "2024-01-01T01:00:00Z", "--output", "out.csv", "--retries", "-1"}},
			{args: []string{"--symbol", "xauusd", "--from", "2024-01-01T00:00:00Z", "--to", "2024-01-01T01:00:00Z", "--output", "out.csv", "--retry-backoff", "0s"}},
			{args: []string{"--symbol", "xauusd", "--from", "2024-01-01T00:00:00Z", "--to", "2024-01-01T01:00:00Z", "--output", "out.csv", "--rate-limit", "-1s"}},
			{args: []string{"--symbol", "xauusd", "--from", "2024-01-01T00:00:00Z", "--to", "2024-01-01T01:00:00Z", "--output", "out.csv", "--parallelism", "0"}},
			{args: []string{"--symbol", "xauusd", "--from", "2024-01-01T00:00:00Z", "--to", "2024-01-01T01:00:00Z", "--output", "out.csv", "--simple", "--full"}},
			{args: []string{"--symbol", "xauusd", "--from", "2024-01-01T00:00:00Z", "--to", "2024-01-01T01:00:00Z", "--output", "out.csv", "--simple", "--custom-columns", "timestamp"}},
			{args: []string{"--symbol", "xauusd", "--from", "2024-01-01T00:00:00Z", "--to", "2024-01-01T01:00:00Z", "--output", "out.csv", "--custom-columns", "wat"}},
			{args: []string{"--symbol", "xauusd", "--timeframe", "tick", "--from", "2024-01-01T00:00:00Z", "--to", "2024-01-01T01:00:00Z", "--output", "out.csv", "--custom-columns", "wat"}},
			{args: []string{"--symbol", "xauusd", "--from", "2024-01-01T00:00:00Z", "--to", "2024-01-01T01:00:00Z", "--output", "out.csv", "--partition", "weird"}},
			{args: []string{"--symbol", "xauusd", "--from", "2024-01-01T00:00:00Z", "--to", "2024-01-01T01:00:00Z", "--output", "-", "--resume"}},
			{args: []string{"--symbol", "xauusd", "--from", "2024-01-01T00:00:00Z", "--to", "2024-01-01T01:00:00Z", "--output", "-", "--partition", "hour"}},
			{args: []string{"--symbol", "xauusd", "--from", "2024-01-01T00:00:00Z", "--to", "2024-01-01T01:00:00Z", "--output", "out.csv", "--parallelism", "2"}},
			{args: []string{"--symbol", "xauusd", "--from", "2024-01-01T00:00:00Z", "--to", "2024-01-01T01:00:00Z", "--output", "out.parquet", "--resume"}},
			{args: []string{"--symbol", "xauusd", "--from", "2024-01-01T00:00:00Z", "--to", "2024-01-01T01:00:00Z", "--output", "-", "--checkpoint-manifest", "state.json"}},
			{args: []string{"--symbol", "xauusd", "--from", "2024-01-01T00:00:00Z", "--to", "2024-01-01T01:00:00Z", "--output", "-", "--checkpoint-manifest", "state.json", "--partition="}},
		}
		for _, tc := range cases {
			if err := runDownload(tc.args, &out, &errBuf); err == nil {
				t.Fatalf("expected runDownload error for args %v", tc.args)
			}
		}

		activeConfig = &appConfig{Download: downloadDefaultsConfig{RateLimit: "bad"}}
		defer func() { activeConfig = nil }()
		if err := runDownload([]string{"--symbol", "xauusd", "--from", "2024-01-01T00:00:00Z", "--to", "2024-01-01T01:00:00Z", "--output", "out.csv"}, &out, &errBuf); err == nil {
			t.Fatal("expected runDownload config default parse error")
		}
	})

	t.Run("loadBidAskBars fallback errors", func(t *testing.T) {
		server := newCLITestServer()
		defer server.Close()
		client := dukascopy.NewClient(server.URL, time.Second)
		request := dukascopy.DownloadRequest{
			Symbol:      "xauusd",
			Granularity: dukascopy.GranularityTick,
			Side:        dukascopy.PriceSideBid,
			From:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			To:          time.Date(2024, 1, 2, 0, 1, 0, 0, time.UTC),
		}
		if _, _, _, err := loadBidAskBars(context.Background(), client, request); err == nil {
			t.Fatal("expected bid/ask fallback aggregation error")
		}

		badClient := dukascopy.NewClient("http://127.0.0.1:1", time.Second).WithRetries(0)
		request.Granularity = dukascopy.GranularityM1
		if _, _, _, err := loadBidAskBars(context.Background(), badClient, request); err == nil {
			t.Fatal("expected fallback download error")
		}
	})

	t.Run("prepareResume and output helpers", func(t *testing.T) {
		request := &dukascopy.DownloadRequest{
			From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
		}
		if _, _, err := prepareResume(true, "ignored.csv", dukascopy.ResultKindTick, nil, []string{"bid"}, request); err == nil {
			t.Fatal("expected prepareResume timestamp requirement error")
		}
		if _, _, err := prepareResume(true, t.TempDir(), dukascopy.ResultKindBar, []string{"timestamp", "open"}, nil, request); err == nil {
			t.Fatal("expected prepareResume inspect error")
		}

		path := writeCSVFixture(t, "timestamp,open\n2024-01-01T00:00:00Z,1\n")
		if _, _, err := prepareResume(true, path, dukascopy.ResultKindBar, []string{"timestamp", "close"}, nil, request); err == nil {
			t.Fatal("expected prepareResume header mismatch error")
		}

		tickPath := writeCSVFixture(t, "timestamp,bid\n2024-01-01T00:30:00Z,1\n")
		request = &dukascopy.DownloadRequest{
			From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
		}
		state, dedupe, err := prepareResume(true, tickPath, dukascopy.ResultKindTick, nil, []string{"timestamp", "bid"}, request)
		if err != nil || state == nil || len(dedupe) == 0 || !request.From.Equal(time.Date(2024, 1, 1, 0, 30, 0, 0, time.UTC)) {
			t.Fatalf("expected tick resume state to adjust request, got state=%+v dedupe=%v err=%v from=%s", state, dedupe, err, request.From)
		}

		instrument := dukascopy.Instrument{PriceScale: 3}
		ticks := []dukascopy.Tick{{Time: time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC), Bid: 1.1}}
		if _, err := writeTickOutput(t.TempDir(), nil, nil, instrument, []string{"timestamp", "bid"}, ticks); err == nil {
			t.Fatal("expected writeTickOutput direct write error")
		}
		parentFile := writeTextFixture(t, "x")
		resumeState := csvoutResumeState()
		if _, err := writeTickOutput(parentFile+"/out.csv", &resumeState, nil, instrument, []string{"timestamp", "bid"}, ticks); err == nil {
			t.Fatal("expected writeTickOutput temp path error")
		}
		path = writeCSVFixture(t, "timestamp,bid\n2024-01-01T00:00:00Z,1\n")
		resumeState = csvoutResumeState()
		if _, err := writeTickOutput(path, &resumeState, nil, instrument, []string{"wat"}, ticks); err == nil {
			t.Fatal("expected writeTickOutput temp write error")
		}

		bars := []dukascopy.Bar{{Time: time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC), Open: 1}}
		if _, err := writeBarOutput(t.TempDir(), nil, nil, instrument, []string{"timestamp", "open"}, bars, nil, nil); err == nil {
			t.Fatal("expected writeBarOutput direct write error")
		}
		if count, err := writeBarOutput(writeCSVFixture(t, "timestamp,spread\n"), nil, nil, instrument, []string{"timestamp", "spread"}, nil, []dukascopy.Bar{{Time: bars[0].Time}}, []dukascopy.Bar{{Time: bars[0].Time}}); err != nil || count != 1 {
			t.Fatalf("expected bid/ask count branch, got %d %v", count, err)
		}
		resumeState = csvoutResumeState()
		if _, err := writeBarOutput(parentFile+"/bar.csv", &resumeState, nil, instrument, []string{"timestamp", "open"}, bars, nil, nil); err == nil {
			t.Fatal("expected writeBarOutput temp path error")
		}
		path = writeCSVFixture(t, "timestamp,open\n2024-01-01T00:00:00Z,1\n")
		resumeState = csvoutResumeState()
		if _, err := writeBarOutput(path, &resumeState, nil, instrument, []string{"wat"}, bars, nil, nil); err == nil {
			t.Fatal("expected writeBarOutput temp write error")
		}

		if _, err := createResumeTempPath(parentFile + "/resume.csv"); err == nil {
			t.Fatal("expected createResumeTempPath error")
		}
	})

	t.Run("runDownload execution branches", func(t *testing.T) {
		server := newCLITestServer()
		defer server.Close()

		var stdout bytes.Buffer
		var stderr bytes.Buffer
		outputPath := t.TempDir() + "/ticks.csv"
		if err := runDownload([]string{
			"--symbol", "xauusd",
			"--granularity", "tick",
			"--from", "2024-01-02T00:00:00Z",
			"--to", "2024-01-02T00:00:02Z",
			"--output", outputPath,
			"--custom-columns", "timestamp,bid",
			"--base-url", server.URL,
			"--progress",
		}, &stdout, &stderr); err != nil {
			t.Fatalf("expected tick download success, got %v", err)
		}

		resumePath := writeFixture(t, "resume.csv", "timestamp,open\n2024-01-02T01:00:00Z,1\n")
		stdout.Reset()
		if err := runDownload([]string{
			"--symbol", "xauusd",
			"--timeframe", "m1",
			"--from", "2024-01-02T00:00:00Z",
			"--to", "2024-01-02T01:00:00Z",
			"--output", resumePath,
			"--custom-columns", "timestamp,open",
			"--base-url", server.URL,
			"--resume",
		}, &stdout, &stderr); err != nil {
			t.Fatalf("expected resume no-op success, got %v", err)
		}
		if !strings.Contains(stdout.String(), "no new rows needed") {
			t.Fatalf("unexpected resume stdout: %s", stdout.String())
		}

		if err := runDownload([]string{
			"--symbol", "xauusd",
			"--timeframe", "m1",
			"--from", "2024-01-02T00:00:00Z",
			"--to", "2024-01-02T00:01:00Z",
			"--output", t.TempDir(),
			"--base-url", server.URL,
			"--resume",
		}, &stdout, &stderr); err == nil {
			t.Fatal("expected resume inspect error")
		}

		if err := runDownload([]string{
			"--symbol", "xauusd",
			"--timeframe", "m1",
			"--from", "2024-01-02T00:00:00Z",
			"--to", "2024-01-02T00:01:00Z",
			"--output", "out.csv",
			"--base-url", "http://127.0.0.1:1",
		}, &stdout, &stderr); err == nil {
			t.Fatal("expected client download error")
		}

		if err := runDownload([]string{
			"--symbol", "xauusd",
			"--timeframe", "tick",
			"--from", "2024-01-02T00:00:00Z",
			"--to", "2024-01-02T00:00:02Z",
			"--output", t.TempDir(),
			"--base-url", server.URL,
		}, &stdout, &stderr); err == nil {
			t.Fatal("expected tick output write error")
		}

		if err := runDownload([]string{
			"--symbol", "xauusd",
			"--timeframe", "m1",
			"--from", "2024-01-02T00:00:00Z",
			"--to", "2024-01-02T00:01:00Z",
			"--output", t.TempDir(),
			"--base-url", server.URL,
			"--full",
		}, &stdout, &stderr); err == nil {
			t.Fatal("expected bid/ask bar output write error")
		}

		if err := runDownload([]string{
			"--symbol", "xauusd",
			"--timeframe", "m1",
			"--from", "2024-01-02T00:00:00Z",
			"--to", "2024-01-02T00:01:00Z",
			"--output", t.TempDir(),
			"--base-url", server.URL,
			"--simple",
		}, &stdout, &stderr); err == nil {
			t.Fatal("expected primary bar output write error")
		}
	})

	t.Run("Run router branches", func(t *testing.T) {
		server := newCLITestServer()
		defer server.Close()

		var stdout bytes.Buffer
		var stderr bytes.Buffer
		if code := Run([]string{"instruments"}, &stdout, &stderr); code != 1 {
			t.Fatalf("expected instruments error exit code, got %d", code)
		}

		outputPath := t.TempDir() + "/run-download.csv"
		if code := Run([]string{
			"download",
			"--symbol", "xauusd",
			"--timeframe", "m1",
			"--from", "2024-01-02T00:00:00Z",
			"--to", "2024-01-02T00:01:00Z",
			"--output", outputPath,
			"--base-url", server.URL,
			"--simple",
		}, &stdout, &stderr); code != 0 {
			t.Fatalf("expected download success exit code, got %d stderr=%s", code, stderr.String())
		}
	})

	t.Run("runDownload partition dispatch", func(t *testing.T) {
		server := newCLITestServer()
		defer server.Close()

		var stdout bytes.Buffer
		var stderr bytes.Buffer
		outputPath := t.TempDir() + "/partitioned.csv"
		if err := runDownload([]string{
			"--symbol", "xauusd",
			"--timeframe", "m1",
			"--from", "2024-01-02T00:00:00Z",
			"--to", "2024-01-02T00:02:00Z",
			"--output", outputPath,
			"--base-url", server.URL,
			"--partition", "hour",
			"--progress",
		}, &stdout, &stderr); err != nil {
			t.Fatalf("expected partitioned runDownload success, got %v", err)
		}
	})
}

func writeCSVFixture(t *testing.T, content string) string {
	t.Helper()
	return writeFixture(t, "data.csv", content)
}

func writeJSONFixture(t *testing.T, content string) string {
	t.Helper()
	return writeFixture(t, "config.json", content)
}

func writeTextFixture(t *testing.T, content string) string {
	t.Helper()
	return writeFixture(t, "file.txt", content)
}

func writeFixture(t *testing.T, name string, content string) string {
	t.Helper()
	path := t.TempDir() + "/" + name
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	return path
}

func newTestFlagSet() *flag.FlagSet {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return fs
}

func gapIntPtr(value int) *int {
	return &value
}

func csvoutResumeState() csvout.ResumeState {
	return csvout.ResumeState{Exists: true, Columns: []string{"timestamp", "open"}}
}
