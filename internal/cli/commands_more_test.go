package cli

import (
	"bytes"
	"context"
	"flag"
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

func TestRunDownloadValidationErrors(t *testing.T) {
	testCases := []struct {
		name string
		args []string
	}{
		{name: "missing symbol", args: []string{"--timeframe", "m1", "--from", "2024-01-02T00:00:00Z", "--to", "2024-01-02T00:02:00Z", "--output", "out.csv"}},
		{name: "missing output", args: []string{"--symbol", "xauusd", "--timeframe", "m1", "--from", "2024-01-02T00:00:00Z", "--to", "2024-01-02T00:02:00Z"}},
		{name: "bad from", args: []string{"--symbol", "xauusd", "--timeframe", "m1", "--from", "bad", "--to", "2024-01-02T00:02:00Z", "--output", "out.csv"}},
		{name: "from after to", args: []string{"--symbol", "xauusd", "--timeframe", "m1", "--from", "2024-01-02T00:02:00Z", "--to", "2024-01-02T00:00:00Z", "--output", "out.csv"}},
		{name: "conflicting profile flags", args: []string{"--symbol", "xauusd", "--timeframe", "m1", "--from", "2024-01-02T00:00:00Z", "--to", "2024-01-02T00:02:00Z", "--output", "out.csv", "--simple", "--full"}},
		{name: "custom and simple", args: []string{"--symbol", "xauusd", "--timeframe", "m1", "--from", "2024-01-02T00:00:00Z", "--to", "2024-01-02T00:02:00Z", "--output", "out.csv", "--simple", "--custom-columns", "timestamp"}},
		{name: "parallel without partition", args: []string{"--symbol", "xauusd", "--timeframe", "m1", "--from", "2024-01-02T00:00:00Z", "--to", "2024-01-02T00:02:00Z", "--output", "out.csv", "--parallelism", "2"}},
		{name: "resume to stdout", args: []string{"--symbol", "xauusd", "--timeframe", "m1", "--from", "2024-01-02T00:00:00Z", "--to", "2024-01-02T00:02:00Z", "--output", "-", "--resume"}},
		{name: "live rejects to", args: []string{"--symbol", "xauusd", "--timeframe", "m1", "--from", "2024-01-02T00:00:00Z", "--to", "2024-01-02T00:02:00Z", "--output", "out.csv", "--live"}},
		{name: "live rejects nonpositive poll interval", args: []string{"--symbol", "xauusd", "--timeframe", "m1", "--from", "2024-01-02T00:00:00Z", "--output", "out.csv", "--live", "--poll-interval", "0s"}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := runDownload(testCase.args, &bytes.Buffer{}, &bytes.Buffer{})
			if err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestRunLiveDownloadAppendsAndStops(t *testing.T) {
	t.Setenv("NO_COLOR", "1")

	server := newCLITestServer()
	defer server.Close()

	dir := t.TempDir()
	outputPath := filepath.Join(dir, "live.csv")

	previousNow := liveNow
	previousSleep := liveSleep
	defer func() {
		liveNow = previousNow
		liveSleep = previousSleep
	}()

	liveNow = func() time.Time {
		return time.Date(2024, 1, 2, 0, 2, 30, 0, time.UTC)
	}

	ctx, cancel := context.WithCancel(context.Background())
	liveSleep = func(ctx context.Context, wait time.Duration) error {
		cancel()
		return ctx.Err()
	}

	var stdout bytes.Buffer
	err := runLiveDownload(
		ctx,
		dukascopy.NewClient(server.URL, time.Second),
		&stdout,
		&bytes.Buffer{},
		outputPath,
		outputPath,
		"",
		dukascopy.DownloadRequest{
			Symbol:      "xauusd",
			Granularity: dukascopy.GranularityM1,
			Side:        dukascopy.PriceSideBid,
			From:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		},
		dukascopy.ResultKindBar,
		[]string{"timestamp", "open"},
		nil,
		partitionNone,
		1,
		time.Millisecond,
	)
	if err != nil {
		t.Fatalf("runLiveDownload returned error: %v", err)
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "2024-01-02T00:00:00Z,100.000") || !strings.Contains(content, "2024-01-02T00:01:00Z,101.000") {
		t.Fatalf("unexpected live output content: %s", content)
	}
	if !strings.Contains(stdout.String(), "live wrote 2 bars") || !strings.Contains(stdout.String(), "live stopped") {
		t.Fatalf("unexpected live stdout: %s", stdout.String())
	}
}

func TestRunLiveDownloadStreamsPureCSVToStdout(t *testing.T) {
	t.Setenv("NO_COLOR", "1")

	server := newCLITestServer()
	defer server.Close()

	previousNow := liveNow
	previousSleep := liveSleep
	defer func() {
		liveNow = previousNow
		liveSleep = previousSleep
	}()

	cycle := 0
	liveNow = func() time.Time {
		if cycle == 0 {
			return time.Date(2024, 1, 2, 0, 2, 30, 0, time.UTC)
		}
		return time.Date(2024, 1, 2, 0, 3, 30, 0, time.UTC)
	}

	ctx, cancel := context.WithCancel(context.Background())
	liveSleep = func(ctx context.Context, wait time.Duration) error {
		cycle++
		if cycle >= 2 {
			cancel()
			return ctx.Err()
		}
		return nil
	}

	var stdout bytes.Buffer
	err := runLiveDownload(
		ctx,
		dukascopy.NewClient(server.URL, time.Second),
		&stdout,
		&bytes.Buffer{},
		"-",
		"-",
		"",
		dukascopy.DownloadRequest{
			Symbol:      "xauusd",
			Granularity: dukascopy.GranularityM1,
			Side:        dukascopy.PriceSideBid,
			From:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		},
		dukascopy.ResultKindBar,
		[]string{"timestamp", "open"},
		nil,
		partitionNone,
		1,
		time.Millisecond,
	)
	if err != nil {
		t.Fatalf("runLiveDownload returned error: %v", err)
	}

	output := stdout.String()
	if strings.Count(output, "timestamp,open") != 1 {
		t.Fatalf("expected exactly one CSV header, got: %s", output)
	}
	if strings.Contains(output, "live wrote") || strings.Contains(output, "live stopped") {
		t.Fatalf("expected pure CSV stdout output, got: %s", output)
	}
	if !strings.Contains(output, "2024-01-02T00:00:00Z,100.000") || !strings.Contains(output, "2024-01-02T00:01:00Z,101.000") {
		t.Fatalf("expected first cycle rows on stdout, got: %s", output)
	}
}

func TestRunLiveDownloadStreamsPartitionedCSVToStdoutWithCheckpoint(t *testing.T) {
	t.Setenv("NO_COLOR", "1")

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/instruments", func(w http.ResponseWriter, r *http.Request) {
		writeCLIJSON(w, map[string]any{
			"instruments": []map[string]any{
				{"id": 1, "name": "XAU/USD", "code": "XAU-USD", "description": "Gold vs US Dollar", "priceScale": 3},
			},
		})
	})
	mux.HandleFunc("/v1/candles/hour/XAU-USD/BID/2024/1", func(w http.ResponseWriter, r *http.Request) {
		writeCLIJSON(w, map[string]any{
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
	server := httptest.NewServer(mux)
	defer server.Close()

	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "stream.manifest.json")
	cachePath := defaultLiveStdoutCachePath(manifestPath)

	previousNow := liveNow
	previousSleep := liveSleep
	defer func() {
		liveNow = previousNow
		liveSleep = previousSleep
	}()

	runOnce := func(now time.Time) string {
		ctx, cancel := context.WithCancel(context.Background())
		liveNow = func() time.Time { return now }
		liveSleep = func(ctx context.Context, wait time.Duration) error {
			cancel()
			return ctx.Err()
		}

		var stdout bytes.Buffer
		err := runLiveDownload(
			ctx,
			dukascopy.NewClient(server.URL, time.Second),
			&stdout,
			&bytes.Buffer{},
			"-",
			cachePath,
			manifestPath,
			dukascopy.DownloadRequest{
				Symbol:      "xauusd",
				Granularity: dukascopy.GranularityH1,
				Side:        dukascopy.PriceSideBid,
				From:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			},
			dukascopy.ResultKindBar,
			[]string{"timestamp", "open"},
			nil,
			partitionMonth,
			1,
			time.Millisecond,
		)
		if err != nil {
			t.Fatalf("runLiveDownload() error = %v", err)
		}
		return stdout.String()
	}

	first := runOnce(time.Date(2024, 1, 2, 2, 30, 0, 0, time.UTC))
	second := runOnce(time.Date(2024, 1, 2, 4, 30, 0, 0, time.UTC))

	if strings.Count(first, "timestamp,open") != 1 || strings.Count(second, "timestamp,open") != 1 {
		t.Fatalf("unexpected partitioned stdout headers: first=%q second=%q", first, second)
	}
	if strings.Contains(first, "live wrote") || strings.Contains(second, "live wrote") {
		t.Fatalf("expected pure CSV partition stdout, got first=%q second=%q", first, second)
	}
	if !strings.Contains(first, "2024-01-02T00:00:00Z,100.000") || !strings.Contains(first, "2024-01-02T01:00:00Z,101.000") {
		t.Fatalf("expected first streamed partition rows, got %q", first)
	}
	if strings.Contains(second, "2024-01-02T00:00:00Z,100.000") || strings.Contains(second, "2024-01-02T01:00:00Z,101.000") {
		t.Fatalf("did not expect previously streamed rows in second output, got %q", second)
	}
	if !strings.Contains(second, "2024-01-02T02:00:00Z,102.000") || !strings.Contains(second, "2024-01-02T03:00:00Z,103.000") {
		t.Fatalf("expected second streamed partition rows, got %q", second)
	}

	manifest, err := checkpoint.Load(manifestPath)
	if err != nil {
		t.Fatalf("checkpoint.Load() error = %v", err)
	}
	if manifest.LiveStream == nil || manifest.LiveStream.Rows != 4 || !manifest.LiveStream.LastTimestamp.Equal(time.Date(2024, 1, 2, 3, 0, 0, 0, time.UTC)) {
		t.Fatalf("unexpected live stream manifest state: %+v", manifest.LiveStream)
	}
	if _, err := os.Stat(cachePath); err != nil {
		t.Fatalf("expected stream cache output file: %v", err)
	}
}

func TestRunLiveDownloadWithPartitionManifestAndGzipOutput(t *testing.T) {
	t.Setenv("NO_COLOR", "1")

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/instruments", func(w http.ResponseWriter, r *http.Request) {
		writeCLIJSON(w, map[string]any{
			"instruments": []map[string]any{
				{"id": 1, "name": "XAU/USD", "code": "XAU-USD", "description": "Gold vs US Dollar", "priceScale": 3},
			},
		})
	})
	mux.HandleFunc("/v1/candles/hour/XAU-USD/BID/2024/1", func(w http.ResponseWriter, r *http.Request) {
		writeCLIJSON(w, map[string]any{
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
	server := httptest.NewServer(mux)
	defer server.Close()

	dir := t.TempDir()
	outputPath := filepath.Join(dir, "live.csv.gz")
	manifestPath := filepath.Join(dir, "custom-live.manifest.json")

	previousNow := liveNow
	previousSleep := liveSleep
	defer func() {
		liveNow = previousNow
		liveSleep = previousSleep
	}()

	runLiveOnce := func(now time.Time) string {
		ctx, cancel := context.WithCancel(context.Background())
		liveNow = func() time.Time { return now }
		liveSleep = func(ctx context.Context, wait time.Duration) error {
			cancel()
			return ctx.Err()
		}

		var stdout bytes.Buffer
		err := runLiveDownload(
			ctx,
			dukascopy.NewClient(server.URL, time.Second),
			&stdout,
			&bytes.Buffer{},
			outputPath,
			outputPath,
			manifestPath,
			dukascopy.DownloadRequest{
				Symbol:      "xauusd",
				Granularity: dukascopy.GranularityH1,
				Side:        dukascopy.PriceSideBid,
				From:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			},
			dukascopy.ResultKindBar,
			[]string{"timestamp", "open"},
			nil,
			partitionMonth,
			1,
			time.Millisecond,
		)
		if err != nil {
			t.Fatalf("runLiveDownload() error = %v", err)
		}
		return stdout.String()
	}

	firstOut := runLiveOnce(time.Date(2024, 1, 2, 2, 30, 0, 0, time.UTC))
	secondOut := runLiveOnce(time.Date(2024, 1, 2, 4, 30, 0, 0, time.UTC))

	manifest, err := checkpoint.Load(manifestPath)
	if err != nil {
		t.Fatalf("checkpoint.Load() error = %v", err)
	}
	if manifest.FinalOutput == nil || manifest.FinalOutput.Rows != 4 {
		t.Fatalf("unexpected final output metadata: %+v", manifest.FinalOutput)
	}
	if len(manifest.Parts) != 1 {
		t.Fatalf("expected one live partition entry, got %+v", manifest.Parts)
	}
	if !manifest.Parts[0].End.Equal(time.Date(2024, 1, 2, 3, 0, 0, 1, time.UTC)) {
		t.Fatalf("unexpected live partition end: %s", manifest.Parts[0].End)
	}
	partFiles, err := filepath.Glob(filepath.Join(checkpoint.DefaultPartsDir(outputPath), "*.csv"))
	if err != nil {
		t.Fatalf("Glob() error = %v", err)
	}
	if len(partFiles) != 1 {
		t.Fatalf("expected obsolete live parts to be pruned, got %v", partFiles)
	}

	audit, err := csvout.AuditCSV(outputPath)
	if err != nil {
		t.Fatalf("AuditCSV() error = %v", err)
	}
	if audit.Rows != 4 {
		t.Fatalf("unexpected gzip live output rows: %+v", audit)
	}
	if !strings.Contains(firstOut, "live wrote 2 bars") || !strings.Contains(secondOut, "live wrote 2 bars") {
		t.Fatalf("unexpected live partition stdout: first=%q second=%q", firstOut, secondOut)
	}
}

func TestRunDownloadLiveParquetAutoEnablesPartitioning(t *testing.T) {
	t.Setenv("NO_COLOR", "1")

	server := newCLITestServer()
	defer server.Close()

	previousNow := liveNow
	previousSleep := liveSleep
	defer func() {
		liveNow = previousNow
		liveSleep = previousSleep
	}()

	liveNow = func() time.Time {
		return time.Date(2024, 1, 2, 0, 2, 30, 0, time.UTC)
	}
	liveSleep = func(ctx context.Context, wait time.Duration) error {
		return context.Canceled
	}

	dir := t.TempDir()
	outputPath := filepath.Join(dir, "live.parquet")

	var stdout bytes.Buffer
	err := runDownload([]string{
		"--symbol", "xauusd",
		"--timeframe", "m1",
		"--from", "2024-01-02T00:00:00Z",
		"--output", outputPath,
		"--simple",
		"--live",
		"--base-url", server.URL,
	}, &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("runDownload returned error: %v", err)
	}

	if _, err := os.Stat(outputPath); err != nil {
		t.Fatalf("expected live parquet output file: %v", err)
	}
	manifestPath := checkpoint.DefaultManifestPath(outputPath)
	manifest, err := checkpoint.Load(manifestPath)
	if err != nil {
		t.Fatalf("checkpoint.Load returned error: %v", err)
	}
	if manifest.Partition != partitionDay {
		t.Fatalf("expected auto partition %q, got %q", partitionDay, manifest.Partition)
	}
	if manifest.FinalOutput == nil || manifest.FinalOutput.Rows != 2 {
		t.Fatalf("unexpected live parquet final output metadata: %+v", manifest.FinalOutput)
	}
}

func TestRunDownloadAndPartitionPipeline(t *testing.T) {
	server := newCLITestServer()
	defer server.Close()

	dir := t.TempDir()
	outputPath := filepath.Join(dir, "bars.csv")

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runDownload([]string{
		"--symbol", "xauusd",
		"--timeframe", "m1",
		"--from", "2024-01-02T00:00:00Z",
		"--to", "2024-01-02T00:02:00Z",
		"--output", outputPath,
		"--simple",
		"--base-url", server.URL,
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("runDownload returned error: %v", err)
	}
	if !strings.Contains(stdout.String(), "wrote") {
		t.Fatalf("unexpected download output: %s", stdout.String())
	}

	manifestPath := filepath.Join(dir, "partition.manifest.json")
	partitionOutput := filepath.Join(dir, "partitioned.csv")
	request := dukascopy.DownloadRequest{
		Symbol:      "xauusd",
		Granularity: dukascopy.GranularityM1,
		Side:        dukascopy.PriceSideBid,
		From:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		To:          time.Date(2024, 1, 2, 0, 2, 0, 0, time.UTC),
	}
	err = runPartitionedDownload(
		context.Background(),
		dukascopy.NewClient(server.URL, time.Second),
		&stdout,
		&stderr,
		partitionOutput,
		manifestPath,
		request,
		dukascopy.ResultKindBar,
		[]string{"timestamp", "open"},
		nil,
		partitionHour,
		1,
	)
	if err != nil {
		t.Fatalf("runPartitionedDownload returned error: %v", err)
	}
	if _, err := os.Stat(manifestPath); err != nil {
		t.Fatalf("expected manifest to be created: %v", err)
	}
}

func TestPartitionExecutionHelpers(t *testing.T) {
	server := newCLITestServer()
	defer server.Close()

	dir := t.TempDir()
	partsDir := filepath.Join(dir, "parts")
	if err := os.MkdirAll(partsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll returned error: %v", err)
	}

	request := dukascopy.DownloadRequest{
		Symbol:      "xauusd",
		Granularity: dukascopy.GranularityM1,
		Side:        dukascopy.PriceSideBid,
		From:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		To:          time.Date(2024, 1, 2, 0, 2, 0, 0, time.UTC),
	}
	item := partitionWorkItem{
		Index: 0,
		Partition: downloadPartition{
			ID:    "part-1",
			Start: request.From,
			End:   request.To,
			File:  "part-1.csv",
		},
	}
	client := dukascopy.NewClient(server.URL, time.Second)
	result := runPartitionJob(context.Background(), client, partsDir, 1, item, request, dukascopy.ResultKindBar, []string{"timestamp", "open"}, nil)
	if result.Err != nil || result.RowsWritten == 0 {
		t.Fatalf("unexpected partition job result: %+v", result)
	}

	manifestPath := filepath.Join(dir, "dataset.manifest.json")
	manifest := checkpoint.Manifest{
		Version:    checkpoint.CurrentManifestVersion,
		OutputPath: filepath.Join(dir, "dataset.csv"),
		PartsDir:   partsDir,
		Symbol:     "xauusd",
		Timeframe:  "m1",
		Side:       "BID",
		ResultKind: "bar",
		Columns:    []string{"timestamp", "open"},
		Partition:  "hour",
		Parts: []checkpoint.ManifestPart{{
			ID:    item.Partition.ID,
			Start: item.Partition.Start,
			End:   item.Partition.End,
			File:  item.Partition.File,
		}},
	}
	if err := checkpoint.Save(manifestPath, manifest); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}
	if err := applyPartitionResult(manifestPath, &manifest, result); err != nil {
		t.Fatalf("applyPartitionResult returned error: %v", err)
	}
	if manifest.Parts[0].Status != "completed" {
		t.Fatalf("expected completed partition, got %+v", manifest.Parts[0])
	}

	pending := []partitionWorkItem{item}
	if err := executePartitionDownloads(context.Background(), client, manifestPath, &manifest, pending, partsDir, request, dukascopy.ResultKindBar, []string{"timestamp", "open"}, nil, 2, nil); err != nil {
		t.Fatalf("executePartitionDownloads returned error: %v", err)
	}
}

func TestRunManifestRepairAndPrune(t *testing.T) {
	dir := t.TempDir()
	outputPath := filepath.Join(dir, "dataset.csv")
	partPath := filepath.Join(dir, "part-1.csv")
	content := "timestamp,mid_close\n2024-01-01T00:00:00Z,1.1\n2024-01-01T00:01:00Z,1.2\n"
	if err := os.WriteFile(partPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	audit, err := csvout.AuditCSV(partPath)
	if err != nil {
		t.Fatalf("AuditCSV returned error: %v", err)
	}

	manifest := checkpoint.Manifest{
		Version:    checkpoint.CurrentManifestVersion,
		OutputPath: outputPath,
		PartsDir:   dir,
		Symbol:     "xauusd",
		Timeframe:  "m1",
		Side:       "BID",
		ResultKind: "bar",
		Columns:    []string{"timestamp", "mid_close"},
		Partition:  "day",
		Parts: []checkpoint.ManifestPart{{
			ID:     "part-1",
			Start:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			End:    time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			File:   filepath.Base(partPath),
			Status: "completed",
			Rows:   audit.Rows,
			Bytes:  audit.Bytes,
			SHA256: audit.SHA256,
		}},
	}
	manifestPath := checkpoint.DefaultManifestPath(outputPath)
	if err := checkpoint.Save(manifestPath, manifest); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	var stdout bytes.Buffer
	if err := runManifestRepair([]string{"--output", outputPath}, &stdout); err != nil {
		t.Fatalf("runManifestRepair returned error: %v", err)
	}
	if !strings.Contains(stdout.String(), "verified") {
		t.Fatalf("unexpected repair output: %s", stdout.String())
	}

	orphanPath := filepath.Join(dir, "orphan.tmp-123.csv")
	if err := os.WriteFile(orphanPath, []byte("temp"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	stdout.Reset()
	if err := runManifestPrune([]string{"--output", outputPath}, &stdout); err != nil {
		t.Fatalf("runManifestPrune returned error: %v", err)
	}
	if !strings.Contains(stdout.String(), "removed") {
		t.Fatalf("unexpected prune output: %s", stdout.String())
	}
	if _, err := os.Stat(orphanPath); !os.IsNotExist(err) {
		t.Fatalf("expected orphan temp file to be removed, got err=%v", err)
	}
}

func TestLoadConfigAndInstrumentDefaults(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "dukascopy.json")
	if err := os.WriteFile(configPath, []byte(configExample()), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	args, err := loadActiveConfig([]string{"--config", configPath, "help"})
	if err != nil {
		t.Fatalf("loadActiveConfig returned error: %v", err)
	}
	defer func() {
		activeConfig = nil
	}()
	if len(args) != 1 || args[0] != "help" {
		t.Fatalf("unexpected remaining args: %v", args)
	}

	fs := newFlagSetWithLimitBaseURL()
	limit := fs.Int("limit", 20, "")
	baseURL := fs.String("base-url", "https://default.test", "")
	applyInstrumentConfigDefaults(fs, limit, baseURL)
	if *limit != 5 {
		t.Fatalf("expected config limit to apply, got %d", *limit)
	}
	if *baseURL != "https://jetta.dukascopy.com" {
		t.Fatalf("expected config base URL to apply, got %q", *baseURL)
	}
}

func TestNewDownloadContextHasNoDeadline(t *testing.T) {
	ctx, cancel := newDownloadContext()
	defer cancel()

	if _, ok := ctx.Deadline(); ok {
		t.Fatal("expected download context to have no overall deadline")
	}
}

func newFlagSetWithLimitBaseURL() *flag.FlagSet {
	return flag.NewFlagSet("instruments", flag.ContinueOnError)
}
