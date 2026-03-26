package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/checkpoint"
	"github.com/Nosvemos/dukascopy-go/internal/csvout"
	"github.com/Nosvemos/dukascopy-go/internal/dukascopy"
)

func TestRunAndPrintHelpers(t *testing.T) {
	t.Setenv("NO_COLOR", "1")

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if code := Run([]string{"--version"}, &stdout, &stderr); code != 0 {
		t.Fatalf("expected version command to succeed, got %d", code)
	}
	if !strings.Contains(stdout.String(), "dukascopy-go") {
		t.Fatalf("unexpected version output: %s", stdout.String())
	}

	stdout.Reset()
	if code := Run([]string{"--help"}, &stdout, &stderr); code != 0 {
		t.Fatalf("expected help command to succeed, got %d", code)
	}
	if !strings.Contains(stdout.String(), "Commands") {
		t.Fatalf("unexpected help output: %s", stdout.String())
	}

	stderr.Reset()
	if code := Run([]string{"wat"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected unknown command to return 2, got %d", code)
	}
	if !strings.Contains(stderr.String(), "unknown command") {
		t.Fatalf("unexpected stderr: %s", stderr.String())
	}

	if got := maxInt(2, 5); got != 5 {
		t.Fatalf("expected maxInt to return 5, got %d", got)
	}
}

func TestEnvironmentAndFormattingHelpers(t *testing.T) {
	t.Setenv("DUKASCOPY_API_BASE_URL", "https://api.test")
	if got := readBaseURL(); got != "https://api.test" {
		t.Fatalf("unexpected base URL: %q", got)
	}

	t.Setenv("NO_COLOR", "1")
	if got := colorize(colorCyan); got != "" {
		t.Fatalf("expected NO_COLOR to disable color output, got %q", got)
	}

	var buffer bytes.Buffer
	printTimeframes(&buffer)
	if !strings.Contains(buffer.String(), "mn1") {
		t.Fatalf("unexpected timeframe output: %s", buffer.String())
	}

	buffer.Reset()
	printInstrumentTable(&buffer, []dukascopy.Instrument{{Name: "XAU/USD", Code: "XAU-USD", Description: "Gold"}})
	if !strings.Contains(buffer.String(), "XAU-USD") {
		t.Fatalf("unexpected instrument table: %s", buffer.String())
	}
}

func TestPrepareResumeAndWriteHelpers(t *testing.T) {
	dir := t.TempDir()
	outputPath := filepath.Join(dir, "bars.csv")
	content := "timestamp,open\n2024-01-02T00:00:00Z,100.000\n2024-01-02T00:01:00Z,101.000\n"
	if err := os.WriteFile(outputPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	request := &dukascopy.DownloadRequest{
		From: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 2, 0, 3, 0, 0, time.UTC),
	}
	resumeState, dedupe, err := prepareResume(true, outputPath, dukascopy.ResultKindBar, []string{"timestamp", "open"}, nil, request)
	if err != nil {
		t.Fatalf("prepareResume returned error: %v", err)
	}
	if resumeState == nil || !resumeState.Exists || len(dedupe) == 0 {
		t.Fatalf("unexpected resume state: %+v dedupe=%v", resumeState, dedupe)
	}
	if !request.From.Equal(time.Date(2024, 1, 2, 0, 1, 0, 0, time.UTC)) {
		t.Fatalf("expected request.From to move to last timestamp, got %s", request.From)
	}

	instrument := dukascopy.Instrument{PriceScale: 3}
	appended, err := writeBarOutput(
		outputPath,
		resumeState,
		dedupe,
		instrument,
		[]string{"timestamp", "open"},
		[]dukascopy.Bar{
			{Time: time.Date(2024, 1, 2, 0, 1, 0, 0, time.UTC), Open: 101},
			{Time: time.Date(2024, 1, 2, 0, 2, 0, 0, time.UTC), Open: 102},
		},
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("writeBarOutput returned error: %v", err)
	}
	if appended != 1 {
		t.Fatalf("expected 1 appended row, got %d", appended)
	}

	tickPath := filepath.Join(dir, "ticks.csv")
	appended, err = writeTickOutput(tickPath, nil, nil, instrument, []string{"timestamp", "bid"}, []dukascopy.Tick{
		{Time: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), Bid: 100},
	})
	if err != nil {
		t.Fatalf("writeTickOutput returned error: %v", err)
	}
	if appended != 1 {
		t.Fatalf("expected 1 written tick, got %d", appended)
	}

	tempPath, err := createResumeTempPath(filepath.Join(dir, "ticks.csv.gz"))
	if err != nil {
		t.Fatalf("createResumeTempPath returned error: %v", err)
	}
	if !strings.HasSuffix(tempPath, ".csv.gz") {
		t.Fatalf("expected gzip resume temp path, got %q", tempPath)
	}
}

func TestProgressAndManifestHelpers(t *testing.T) {
	t.Setenv("NO_COLOR", "1")

	var buffer bytes.Buffer
	printer := newProgressPrinter(&buffer)
	printer.Print(dukascopy.ProgressEvent{Kind: "chunk", Scope: "minute", Current: 1, Total: 2, Detail: "2024-01-02"})
	printer.Print(dukascopy.ProgressEvent{Kind: "retry", Attempt: 1, MaxAttempt: 3, Detail: "http://example.test"})
	output := buffer.String()
	if !strings.Contains(output, "progress") || !strings.Contains(output, "retry") {
		t.Fatalf("unexpected progress output: %s", output)
	}

	var usage bytes.Buffer
	printManifestUsage(&usage)
	if !strings.Contains(usage.String(), "manifest inspect") {
		t.Fatalf("unexpected manifest usage: %s", usage.String())
	}

	manifestPath, err := resolveManifestPath("", "dataset.csv")
	if err != nil || manifestPath != "dataset.csv.manifest.json" {
		t.Fatalf("unexpected manifest path resolution: %q %v", manifestPath, err)
	}
	if _, err := resolveManifestPath("a.json", "dataset.csv"); err == nil {
		t.Fatal("expected resolveManifestPath conflict error")
	}
	if filepathBase(`c:\temp\file.csv`) != "file.csv" {
		t.Fatal("expected filepathBase to strip parent directories")
	}
	if !shouldPrunePartFile("part.tmp-123.csv") || !shouldPruneTopLevelFile("dataset.csv.tmp-1", "manifest.json", "dataset.csv") {
		t.Fatal("expected prune helpers to match temp files")
	}
	if shouldPruneTopLevelFile("keep.txt", "manifest.json", "dataset.csv") {
		t.Fatal("did not expect non-temp file to be pruned")
	}
	if got := defaultString("", "fallback"); got != "fallback" {
		t.Fatalf("unexpected defaultString fallback: %q", got)
	}
	if got := weekStartForPartition(time.Date(2024, 1, 3, 12, 0, 0, 0, time.UTC)); !got.Equal(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("unexpected week start: %s", got)
	}
	if _, _, err := manifestRange(checkpoint.Manifest{}); err == nil {
		t.Fatal("expected empty manifest range error")
	}
}

func TestRunStatsManifestAndInstruments(t *testing.T) {
	server := newCLITestServer()
	defer server.Close()

	dir := t.TempDir()
	dataPath := filepath.Join(dir, "dataset.csv")
	content := "timestamp,mid_close\n2024-01-01T00:00:00Z,1.1\n2024-01-01T00:01:00Z,1.2\n"
	if err := os.WriteFile(dataPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	partPath := filepath.Join(dir, "part-1.csv")
	if err := os.WriteFile(partPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	partAudit, err := csvout.AuditCSV(partPath)
	if err != nil {
		t.Fatalf("AuditCSV returned error: %v", err)
	}
	outputAudit, err := csvout.AuditCSV(dataPath)
	if err != nil {
		t.Fatalf("AuditCSV returned error: %v", err)
	}

	manifest := checkpoint.Manifest{
		Version:    checkpoint.CurrentManifestVersion,
		OutputPath: dataPath,
		PartsDir:   dir,
		Symbol:     "xauusd",
		Timeframe:  "m1",
		Side:       "BID",
		ResultKind: "bar",
		Columns:    []string{"timestamp", "mid_close"},
		Partition:  "day",
		CreatedAt:  time.Now().UTC(),
		Parts: []checkpoint.ManifestPart{{
			ID:     "part-1",
			Start:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			End:    time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			File:   filepath.Base(partPath),
			Status: "completed",
			Rows:   partAudit.Rows,
			Bytes:  partAudit.Bytes,
			SHA256: partAudit.SHA256,
		}},
		FinalOutput: &checkpoint.ManifestOutput{
			Rows:   outputAudit.Rows,
			Bytes:  outputAudit.Bytes,
			SHA256: outputAudit.SHA256,
		},
	}
	manifestPath := checkpoint.DefaultManifestPath(dataPath)
	if err := checkpoint.Save(manifestPath, manifest); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	var out bytes.Buffer
	if err := runStats([]string{"--input", dataPath}, &out); err != nil {
		t.Fatalf("runStats returned error: %v", err)
	}
	if !strings.Contains(out.String(), "rows:") {
		t.Fatalf("unexpected stats output: %s", out.String())
	}

	out.Reset()
	if err := runManifestInspect([]string{"--output", dataPath}, &out); err != nil {
		t.Fatalf("runManifestInspect returned error: %v", err)
	}
	if !strings.Contains(out.String(), "Manifest") {
		t.Fatalf("unexpected manifest inspect output: %s", out.String())
	}

	out.Reset()
	if err := runManifestVerify([]string{"--output", dataPath}, &out); err != nil {
		t.Fatalf("runManifestVerify returned error: %v", err)
	}
	if !strings.Contains(out.String(), "verified") {
		t.Fatalf("unexpected manifest verify output: %s", out.String())
	}

	t.Setenv("DUKASCOPY_API_BASE_URL", server.URL)
	out.Reset()
	if err := runInstruments([]string{"--query", "xauusd"}, &out); err != nil {
		t.Fatalf("runInstruments returned error: %v", err)
	}
	if !strings.Contains(out.String(), "XAU-USD") {
		t.Fatalf("unexpected instruments output: %s", out.String())
	}
}

func TestLoadBidAskBarsAndManifestUtilityLogic(t *testing.T) {
	server := newCLITestServer()
	defer server.Close()

	client := dukascopy.NewClient(server.URL, time.Second)
	ctx := context.Background()
	request := dukascopy.DownloadRequest{
		Symbol:      "xauusd",
		Granularity: dukascopy.GranularityM1,
		From:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		To:          time.Date(2024, 1, 2, 0, 2, 0, 0, time.UTC),
	}
	instrument, bidBars, askBars, err := loadBidAskBars(ctx, client, request)
	if err != nil {
		t.Fatalf("loadBidAskBars returned error: %v", err)
	}
	if instrument.Code != "XAU-USD" || len(bidBars) != 2 || len(askBars) != 2 {
		t.Fatalf("unexpected bid/ask load result: %+v %d %d", instrument, len(bidBars), len(askBars))
	}

	report := checkpoint.VerificationReport{
		Parts: []checkpoint.FileVerification{{Valid: true}},
	}
	if !shouldRepairFinalOutput(report) {
		t.Fatal("expected repair to be allowed when all parts are valid and final output is missing")
	}
	report.FinalOutput = &checkpoint.FileVerification{Valid: true}
	if shouldRepairFinalOutput(report) {
		t.Fatal("expected no repair when final output is already valid")
	}

	manifest := checkpoint.Manifest{
		Parts: []checkpoint.ManifestPart{
			{Start: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), End: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)},
			{Start: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), End: time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC)},
		},
	}
	from, to, err := manifestRange(manifest)
	if err != nil {
		t.Fatalf("manifestRange returned error: %v", err)
	}
	if !from.Equal(manifest.Parts[0].Start) || !to.Equal(manifest.Parts[1].End) {
		t.Fatalf("unexpected manifest range: %s -> %s", from, to)
	}

	issues, warnings := evaluateDataQuality(csvout.CSVStats{
		DuplicateRows:    1,
		DuplicateStamps:  2,
		OutOfOrderRows:   3,
		GapCount:         1,
		MissingIntervals: 4,
		LargestGap:       "5m0s",
	})
	if len(issues) != 3 || len(warnings) != 1 {
		t.Fatalf("unexpected data quality evaluation: issues=%v warnings=%v", issues, warnings)
	}
}

func TestRunManifestRouterAndLoadBidAskFallback(t *testing.T) {
	var out bytes.Buffer
	if err := runManifest([]string{"help"}, &out); err != nil {
		t.Fatalf("runManifest help returned error: %v", err)
	}
	out.Reset()
	if err := runManifest([]string{"wat"}, &out); err == nil {
		t.Fatal("expected unknown manifest subcommand error")
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/instruments":
			writeCLIJSON(w, map[string]any{"instruments": []map[string]any{{"id": 1, "name": "XAU/USD", "code": "XAU-USD", "description": "Gold", "priceScale": 3}}})
		case "/v1/candles/minute/XAU-USD/BID/2024/1/2":
			writeCLIJSON(w, map[string]any{
				"timestamp":  1704153600000,
				"multiplier": 1.0,
				"open":       100.0,
				"high":       101.0,
				"low":        99.0,
				"close":      100.5,
				"shift":      60000,
				"times":      []int{0, 1},
				"opens":      []float64{0, 1},
				"highs":      []float64{0, 1},
				"lows":       []float64{0, 1},
				"closes":     []float64{0, 1},
				"volumes":    []float64{0.001, 0.002},
			})
		case "/v1/candles/minute/XAU-USD/ASK/2024/1/2":
			http.Error(w, "no ask candles", http.StatusInternalServerError)
		case "/v1/ticks/XAU-USD/2024/1/2/0":
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

	client := dukascopy.NewClient(server.URL, time.Second)
	_, bidBars, askBars, err := loadBidAskBars(context.Background(), client, dukascopy.DownloadRequest{
		Symbol:      "xauusd",
		Granularity: dukascopy.GranularityM1,
		Side:        dukascopy.PriceSideBid,
		From:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		To:          time.Date(2024, 1, 2, 0, 2, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("loadBidAskBars fallback returned error: %v", err)
	}
	if len(bidBars) == 0 || len(askBars) == 0 {
		t.Fatalf("expected fallback bid/ask bars, got %d/%d", len(bidBars), len(askBars))
	}
}

func newCLITestServer() *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/instruments", func(w http.ResponseWriter, r *http.Request) {
		writeCLIJSON(w, map[string]any{
			"instruments": []map[string]any{
				{"id": 1, "name": "XAU/USD", "code": "XAU-USD", "description": "Gold vs US Dollar", "priceScale": 3},
			},
		})
	})
	mux.HandleFunc("/v1/candles/minute/XAU-USD/BID/2024/1/2", func(w http.ResponseWriter, r *http.Request) {
		writeCLIJSON(w, map[string]any{
			"timestamp":  1704153600000,
			"multiplier": 1.0,
			"open":       100.0,
			"high":       101.0,
			"low":        99.0,
			"close":      100.5,
			"shift":      60000,
			"times":      []int{0, 1},
			"opens":      []float64{0, 1},
			"highs":      []float64{0, 1},
			"lows":       []float64{0, 1},
			"closes":     []float64{0, 1},
			"volumes":    []float64{0.001, 0.002},
		})
	})
	mux.HandleFunc("/v1/candles/minute/XAU-USD/ASK/2024/1/2", func(w http.ResponseWriter, r *http.Request) {
		writeCLIJSON(w, map[string]any{
			"timestamp":  1704153600000,
			"multiplier": 1.0,
			"open":       100.2,
			"high":       101.2,
			"low":        99.2,
			"close":      100.7,
			"shift":      60000,
			"times":      []int{0, 1},
			"opens":      []float64{0, 1},
			"highs":      []float64{0, 1},
			"lows":       []float64{0, 1},
			"closes":     []float64{0, 1},
			"volumes":    []float64{0.001, 0.002},
		})
	})
	mux.HandleFunc("/v1/ticks/XAU-USD/2024/1/2/0", func(w http.ResponseWriter, r *http.Request) {
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
	})
	return httptest.NewServer(mux)
}

func writeCLIJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}
