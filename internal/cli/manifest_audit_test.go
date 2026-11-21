package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/checkpoint"
	"github.com/Nosvemos/dukascopy-go/pkg/csvout"
	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

func TestDetectManifestGapPartIndexesMarksEmptyMiddlePartition(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{
		"p0.csv": "timestamp,open\n2024-01-02T00:00:00Z,1\n2024-01-02T00:01:00Z,2\n",
		"p1.csv": "timestamp,open\n", // zero rows: the partition actually missing data
		"p2.csv": "timestamp,open\n2024-01-02T00:05:00Z,3\n",
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	indexes, err := detectManifestGapPartIndexes(checkpoint.Manifest{
		PartsDir: dir,
		Parts: []checkpoint.ManifestPart{
			{ID: "p0", File: "p0.csv"},
			{ID: "p1", File: "p1.csv"},
			{ID: "p2", File: "p2.csv"},
		},
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(indexes) != 3 || indexes[0] != 0 || indexes[1] != 1 || indexes[2] != 2 {
		t.Fatalf("expected the gap to cover partitions [0 1 2], got %v", indexes)
	}
}

func TestRebuildManifestFinalOutputRejectsTraversal(t *testing.T) {
	root := t.TempDir()
	partsDir := filepath.Join(root, "out.csv.parts")
	if err := os.MkdirAll(partsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(root, "outside.csv")
	if err := os.WriteFile(outside, []byte("timestamp,open\n2024-01-02T00:00:00Z,1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	manifest := &checkpoint.Manifest{
		Version:    checkpoint.CurrentManifestVersion,
		OutputPath: filepath.Join(root, "out.csv"),
		PartsDir:   partsDir,
		Parts: []checkpoint.ManifestPart{{
			ID:    "p",
			Start: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			End:   time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
			File:  "../outside.csv",
		}},
	}
	if err := rebuildManifestFinalOutput(filepath.Join(root, "m.json"), manifest); err == nil {
		t.Fatal("expected a part outside the parts directory to be refused")
	}
	if _, err := detectManifestGapPartIndexes(*manifest, time.Minute); err == nil {
		t.Fatal("expected gap detection to refuse a part outside the parts directory")
	}
}

func writePruneFixture(t *testing.T, partsDir string, outputPath string) string {
	t.Helper()
	if err := os.MkdirAll(partsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(partsDir, "kept.csv"), []byte("timestamp,open\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	manifestPath := checkpoint.DefaultManifestPath(outputPath)
	if err := checkpoint.Save(manifestPath, checkpoint.Manifest{
		Version:    checkpoint.CurrentManifestVersion,
		OutputPath: outputPath,
		PartsDir:   partsDir,
		Parts:      []checkpoint.ManifestPart{{ID: "kept", File: "kept.csv", Status: "completed"}},
	}); err != nil {
		t.Fatal(err)
	}
	return manifestPath
}

func TestManifestPruneRefusesNonDedicatedPartsDir(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	dir := t.TempDir()
	outputPath := filepath.Join(dir, "dataset.csv")
	if err := os.WriteFile(outputPath, []byte("timestamp,open\n2024-01-02T00:00:00Z,1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(dir, "unrelated.csv")
	if err := os.WriteFile(other, []byte("x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	manifestPath := writePruneFixture(t, dir, outputPath) // parts dir == the output's own directory

	err := runManifestPrune([]string{"--manifest", manifestPath}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "refusing to prune") {
		t.Fatalf("expected prune to refuse a non-dedicated directory, got %v", err)
	}
	for _, path := range []string{outputPath, other} {
		if _, statErr := os.Stat(path); statErr != nil {
			t.Fatalf("%s must not be deleted: %v", path, statErr)
		}
	}
}

func TestManifestPruneDryRunAndRecentTempFiles(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	dir := t.TempDir()
	outputPath := filepath.Join(dir, "dataset.csv")
	partsDir := checkpoint.DefaultPartsDir(outputPath)
	manifestPath := writePruneFixture(t, partsDir, outputPath)

	old := time.Now().Add(-time.Hour)
	orphan := filepath.Join(partsDir, "orphan.csv")
	oldTemp := filepath.Join(partsDir, "x.csv.tmp-old")
	recentTemp := filepath.Join(partsDir, "x.csv.tmp-live")
	for _, path := range []string{orphan, oldTemp, recentTemp} {
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{orphan, oldTemp} {
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatal(err)
		}
	}

	var out bytes.Buffer
	if err := runManifestPrune([]string{"--manifest", manifestPath, "--dry-run"}, &out); err != nil {
		t.Fatalf("dry run: %v", err)
	}
	text := out.String()
	if !strings.Contains(text, "orphan.csv") || !strings.Contains(text, "x.csv.tmp-old") || strings.Contains(text, "x.csv.tmp-live") {
		t.Fatalf("unexpected dry-run listing: %s", text)
	}
	for _, path := range []string{orphan, oldTemp, recentTemp} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("dry run must not delete %s", path)
		}
	}

	out.Reset()
	if err := runManifestPrune([]string{"--manifest", manifestPath}, &out); err != nil {
		t.Fatalf("prune: %v", err)
	}
	for _, path := range []string{orphan, oldTemp} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("expected %s to be pruned", path)
		}
	}
	for _, path := range []string{recentTemp, filepath.Join(partsDir, "kept.csv")} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected %s to be kept: %v", path, err)
		}
	}
}

func TestCleanDuplicatesUpdatesManifestFinalOutput(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	dir := t.TempDir()
	outputPath := filepath.Join(dir, "dataset.csv")
	messy := "timestamp,open,high,low,close,volume\n" +
		"2024-01-01T00:00:00Z,1.0,1.1,0.9,1.0,10.0\n" +
		"2024-01-01T00:00:00Z,1.0,1.1,0.9,1.0,10.0\n" +
		"2024-01-01T00:01:00Z,1.0,1.1,0.9,1.0,10.0\n"
	if err := os.WriteFile(outputPath, []byte(messy), 0o644); err != nil {
		t.Fatal(err)
	}
	audit, err := csvout.AuditCSV(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := checkpoint.DefaultManifestPath(outputPath)
	if err := checkpoint.Save(manifestPath, checkpoint.Manifest{
		Version:     checkpoint.CurrentManifestVersion,
		OutputPath:  outputPath,
		PartsDir:    checkpoint.DefaultPartsDir(outputPath),
		Completed:   true,
		FinalOutput: &checkpoint.ManifestOutput{Rows: audit.Rows, Bytes: audit.Bytes, SHA256: audit.SHA256},
	}); err != nil {
		t.Fatal(err)
	}

	if err := runManifestCleanDuplicates([]string{"--output", outputPath}, &bytes.Buffer{}); err != nil {
		t.Fatalf("clean-duplicates: %v", err)
	}

	report, err := checkpoint.VerifyManifest(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if report.FinalOutput == nil || !report.FinalOutput.Valid {
		t.Fatalf("expected manifest final output to match the cleaned file, got %+v", report.FinalOutput)
	}
	if report.FinalOutput.Rows != 2 {
		t.Fatalf("expected 2 rows after cleaning, got %d", report.FinalOutput.Rows)
	}
}

func TestCleanDuplicatesSurfacesCorruptManifest(t *testing.T) {
	dir := t.TempDir()
	outputPath := filepath.Join(dir, "dataset.csv")
	if err := os.WriteFile(outputPath, []byte("timestamp,open\n2024-01-01T00:00:00Z,1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	corrupt := filepath.Join(dir, "corrupt.json")
	if err := os.WriteFile(corrupt, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := runManifestCleanDuplicates([]string{"--manifest", corrupt}, &bytes.Buffer{}); err == nil {
		t.Fatal("expected a corrupt --manifest to be reported")
	}
	if err := os.WriteFile(checkpoint.DefaultManifestPath(outputPath), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := runManifestCleanDuplicates([]string{"--output", outputPath}, &bytes.Buffer{}); err == nil {
		t.Fatal("expected a corrupt manifest next to the output to be reported")
	}
}

func TestBuildWizardDownloadArgsAddsTo(t *testing.T) {
	now := time.Date(2024, 5, 6, 7, 8, 9, 123, time.UTC)
	args := buildWizardDownloadArgs(" eurusd ", "m1", " 2024-01-01 ", " ./out.csv ", now)
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "--from 2024-01-01") || !strings.Contains(joined, "--to 2024-05-06T07:08:09Z") {
		t.Fatalf("expected --from and --to, got %v", args)
	}
	if !strings.Contains(joined, "--symbol eurusd ") || !strings.Contains(joined, "--output ./out.csv") {
		t.Fatalf("expected trimmed symbol and output, got %q", joined)
	}
	toValue := args[len(args)-1]
	if _, err := ParseFlexibleTime(toValue); err != nil {
		t.Fatalf("--to value %q is not accepted by the download parser: %v", toValue, err)
	}

	last := buildWizardDownloadArgs("eurusd", "m1", "30d", "out.csv", now)
	if strings.Contains(strings.Join(last, " "), "--to") || !strings.Contains(strings.Join(last, " "), "--last 30d") {
		t.Fatalf("duration answers should map to --last only, got %v", last)
	}
}

// ---------------------------------------------------------------------------
// live download loop
// ---------------------------------------------------------------------------

func TestRunLiveDownloadRetriesTransientFailuresWithBackoff(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "instruments") {
			writeCLIJSON(w, map[string]any{"instruments": []map[string]any{
				{"id": 1, "name": "XAU/USD", "code": "XAU-USD", "description": "Gold", "priceScale": 3},
			}})
			return
		}
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
	}))
	defer server.Close()

	previousNow, previousSleep := liveNow, liveSleep
	defer func() { liveNow, liveSleep = previousNow, previousSleep }()
	liveNow = func() time.Time { return time.Date(2024, 1, 2, 0, 2, 30, 0, time.UTC) }
	var waits []time.Duration
	liveSleep = func(ctx context.Context, wait time.Duration) error {
		waits = append(waits, wait)
		return nil
	}

	client, err := dukascopy.NewClient(server.URL, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client = client.WithRetries(0)

	var stderr bytes.Buffer
	err = runLiveDownload(context.Background(), client, io.Discard, &stderr, "-", "-", "",
		dukascopy.DownloadRequest{
			Symbol:      "xauusd",
			Granularity: dukascopy.GranularityM1,
			Side:        dukascopy.PriceSideBid,
			From:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		},
		dukascopy.ResultKindBar, []string{"timestamp", "open"}, nil, partitionNone, 1, time.Second)
	if err == nil || !strings.Contains(err.Error(), "consecutive failures") {
		t.Fatalf("expected to give up after consecutive failures, got %v", err)
	}
	if len(waits) != liveMaxConsecutiveFailures-1 {
		t.Fatalf("expected %d backoff sleeps before giving up, got %d (%v)", liveMaxConsecutiveFailures-1, len(waits), waits)
	}
	for i := 1; i < len(waits); i++ {
		if waits[i] < waits[i-1] {
			t.Fatalf("backoff must not shrink: %v", waits)
		}
	}
	if waits[0] != time.Second || waits[1] != 2*time.Second {
		t.Fatalf("expected exponential backoff from the poll interval, got %v", waits)
	}
	if !strings.Contains(stderr.String(), "retrying") {
		t.Fatalf("expected failures to be logged to stderr, got %q", stderr.String())
	}
}

func TestRunLiveDownloadStopsCleanlyWhenCancelledDuringBackoff(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "down", http.StatusInternalServerError)
	}))
	defer server.Close()

	previousNow, previousSleep := liveNow, liveSleep
	defer func() { liveNow, liveSleep = previousNow, previousSleep }()
	liveNow = func() time.Time { return time.Date(2024, 1, 2, 0, 2, 30, 0, time.UTC) }
	ctx, cancel := context.WithCancel(context.Background())
	liveSleep = func(ctx context.Context, wait time.Duration) error {
		cancel()
		return ctx.Err()
	}

	client, err := dukascopy.NewClient(server.URL, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	err = runLiveDownload(ctx, client.WithRetries(0), io.Discard, io.Discard, "-", "-", "",
		dukascopy.DownloadRequest{
			Symbol:      "xauusd",
			Granularity: dukascopy.GranularityM1,
			Side:        dukascopy.PriceSideBid,
			From:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		},
		dukascopy.ResultKindBar, []string{"timestamp", "open"}, nil, partitionNone, 1, time.Second)
	if err != nil {
		t.Fatalf("expected a clean stop on cancel, got %v", err)
	}
}

func TestLiveFailureBackoffIsCapped(t *testing.T) {
	if got := liveFailureBackoff(time.Second, 1); got != time.Second {
		t.Fatalf("first failure should wait one poll interval, got %s", got)
	}
	if got := liveFailureBackoff(time.Second, 3); got != 4*time.Second {
		t.Fatalf("expected 4s, got %s", got)
	}
	if got := liveFailureBackoff(time.Second, 100); got != liveMaxFailureBackoff {
		t.Fatalf("expected cap %s, got %s", liveMaxFailureBackoff, got)
	}
}

// ---------------------------------------------------------------------------
// live stream
// ---------------------------------------------------------------------------

type fakeLiveDownloader struct {
	requests  []dukascopy.DownloadRequest
	responses []fakeLiveResponse
}

type fakeLiveResponse struct {
	result dukascopy.DownloadResult
	err    error
}

func (f *fakeLiveDownloader) Download(ctx context.Context, request dukascopy.DownloadRequest) (dukascopy.DownloadResult, error) {
	f.requests = append(f.requests, request)
	if len(f.responses) == 0 {
		return dukascopy.DownloadResult{}, nil
	}
	next := f.responses[0]
	f.responses = f.responses[1:]
	return next.result, next.err
}

func newTestStreamer(client liveStreamDownloader, out io.Writer, stderr io.Writer, granularity dukascopy.Granularity, format string) *liveStreamer {
	return &liveStreamer{
		client:         client,
		out:            out,
		stderr:         stderr,
		errLog:         newRateLimitedLogger(stderr, time.Hour),
		symbol:         "eurusd",
		instrumentName: "EURUSD",
		timeframeLabel: strings.ToUpper(string(granularity)),
		side:           "BID",
		granularity:    granularity,
		isTick:         granularity == dukascopy.GranularityTick,
		format:         format,
		pollInterval:   time.Second,
	}
}

func TestLiveStreamTickPollResumesFromLastTick(t *testing.T) {
	lastTick := time.Date(2024, 1, 2, 10, 59, 59, 500_000_000, time.UTC)
	fake := &fakeLiveDownloader{responses: []fakeLiveResponse{
		{result: dukascopy.DownloadResult{Ticks: []dukascopy.Tick{{Time: lastTick, Bid: 1.1, Ask: 1.2}}}},
		{result: dukascopy.DownloadResult{Ticks: []dukascopy.Tick{
			{Time: lastTick, Bid: 1.1, Ask: 1.2},                             // already emitted
			{Time: lastTick.Add(200 * time.Millisecond), Bid: 1.3, Ask: 1.4}, // late tick of the previous hour
		}}},
	}}
	var out bytes.Buffer
	s := newTestStreamer(fake, &out, io.Discard, dukascopy.GranularityTick, "jsonl")

	if err := s.pollTicks(context.Background(), time.Date(2024, 1, 2, 10, 59, 59, 900_000_000, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if err := s.pollTicks(context.Background(), time.Date(2024, 1, 2, 11, 0, 5, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}

	if !fake.requests[0].From.Equal(time.Date(2024, 1, 2, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("first poll should start at the hour, got %s", fake.requests[0].From)
	}
	if !fake.requests[1].From.Equal(lastTick) {
		t.Fatalf("second poll should resume from the last emitted tick %s, got %s", lastTick, fake.requests[1].From)
	}
	if lines := strings.Count(out.String(), "\n"); lines != 2 {
		t.Fatalf("expected 2 distinct ticks emitted, got %d: %s", lines, out.String())
	}
}

func TestLiveStreamBarPollCatchesUpMissedBars(t *testing.T) {
	at := func(minute int) time.Time { return time.Date(2024, 1, 2, 10, minute, 0, 0, time.UTC) }
	fake := &fakeLiveDownloader{responses: []fakeLiveResponse{
		{result: dukascopy.DownloadResult{Bars: []dukascopy.Bar{{Time: at(4), Open: 1}}}},
		{err: errors.New("transient")},
		{result: dukascopy.DownloadResult{Bars: []dukascopy.Bar{
			{Time: at(7), Open: 7}, {Time: at(5), Open: 5}, {Time: at(4), Open: 4}, {Time: at(6), Open: 6}, {Time: at(6), Open: 6},
		}}},
	}}
	var out bytes.Buffer
	s := newTestStreamer(fake, &out, io.Discard, dukascopy.GranularityM1, "csv")

	if err := s.pollBars(context.Background(), time.Date(2024, 1, 2, 10, 5, 30, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if err := s.pollBars(context.Background(), time.Date(2024, 1, 2, 10, 6, 30, 0, time.UTC)); err == nil {
		t.Fatal("expected the transient error to be returned")
	}
	if err := s.pollBars(context.Background(), time.Date(2024, 1, 2, 10, 8, 30, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}

	last := fake.requests[2]
	if !last.From.Equal(at(5)) || !last.To.Equal(at(7).Add(time.Nanosecond)) {
		t.Fatalf("expected catch-up request [10:05, 10:07], got [%s, %s)", last.From, last.To)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	var opens []string
	for _, line := range lines {
		fields := strings.Split(line, ",")
		opens = append(opens, fields[4])
	}
	if strings.Join(opens, " ") != "1 5 6 7" {
		t.Fatalf("expected bars emitted once each, in order, got %v", opens)
	}
}

func TestLiveStreamCSVKeepsFullPrecision(t *testing.T) {
	fake := &fakeLiveDownloader{responses: []fakeLiveResponse{
		{result: dukascopy.DownloadResult{Ticks: []dukascopy.Tick{{
			Time: time.Date(2024, 1, 2, 10, 0, 1, 0, time.UTC), Bid: 1.123456789, Ask: 150.1234567, BidVolume: 0.000123, AskVolume: 2.5,
		}}}},
	}}
	var out bytes.Buffer
	s := newTestStreamer(fake, &out, io.Discard, dukascopy.GranularityTick, "csv")
	if err := s.pollTicks(context.Background(), time.Date(2024, 1, 2, 10, 0, 2, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	line := strings.TrimSpace(out.String())
	if !strings.Contains(line, ",1.123456789,150.1234567,0.000123,2.5") {
		t.Fatalf("expected full-precision prices and volumes, got %q", line)
	}
}

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

func TestLiveStreamStopsOnOutputWriteError(t *testing.T) {
	previousSleep, previousNow := liveStreamSleep, liveStreamNow
	defer func() { liveStreamSleep, liveStreamNow = previousSleep, previousNow }()
	liveStreamNow = func() time.Time { return time.Date(2024, 1, 2, 10, 0, 2, 0, time.UTC) }
	sleeps := 0
	liveStreamSleep = func(ctx context.Context, wait time.Duration) error {
		sleeps++
		if sleeps > 5 {
			return context.Canceled
		}
		return nil
	}

	fake := &fakeLiveDownloader{responses: []fakeLiveResponse{
		{result: dukascopy.DownloadResult{Ticks: []dukascopy.Tick{{Time: time.Date(2024, 1, 2, 10, 0, 1, 0, time.UTC), Bid: 1, Ask: 2}}}},
	}}
	var stderr bytes.Buffer
	s := newTestStreamer(fake, failingWriter{err: errors.New("broken pipe")}, &stderr, dukascopy.GranularityTick, "jsonl")
	s.run(context.Background())

	if sleeps != 0 || len(fake.requests) != 1 {
		t.Fatalf("expected the stream to stop right after the failed write (sleeps=%d requests=%d)", sleeps, len(fake.requests))
	}
	if !strings.Contains(stderr.String(), "output closed") {
		t.Fatalf("expected a clean stop message, got %q", stderr.String())
	}
}

func TestLiveStreamLogsDownloadErrorsRateLimitedAndBacksOff(t *testing.T) {
	previousSleep, previousNow := liveStreamSleep, liveStreamNow
	defer func() { liveStreamSleep, liveStreamNow = previousSleep, previousNow }()
	liveStreamNow = func() time.Time { return time.Date(2024, 1, 2, 10, 0, 2, 0, time.UTC) }
	var waits []time.Duration
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	liveStreamSleep = func(ctx context.Context, wait time.Duration) error {
		waits = append(waits, wait)
		if len(waits) == 4 {
			cancel()
			return context.Canceled
		}
		return nil
	}

	fake := &fakeLiveDownloader{responses: []fakeLiveResponse{
		{err: errors.New("http 503")},
		{err: errors.New("http 503")},
		{err: errors.New("http 503")},
		{result: dukascopy.DownloadResult{}},
	}}
	var stderr bytes.Buffer
	s := newTestStreamer(fake, io.Discard, &stderr, dukascopy.GranularityTick, "jsonl")
	s.run(ctx)

	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, time.Second}
	if len(waits) != len(want) {
		t.Fatalf("unexpected waits %v", waits)
	}
	for i := range want {
		if waits[i] != want[i] {
			t.Fatalf("expected waits %v, got %v", want, waits)
		}
	}
	if got := strings.Count(stderr.String(), "download failed"); got != 1 {
		t.Fatalf("expected repeated errors to be rate-limited to one line, got %d: %q", got, stderr.String())
	}
}

func TestLiveStreamLogsPublisherErrors(t *testing.T) {
	fake := &fakeLiveDownloader{responses: []fakeLiveResponse{
		{result: dukascopy.DownloadResult{Ticks: []dukascopy.Tick{{Time: time.Date(2024, 1, 2, 10, 0, 1, 0, time.UTC), Bid: 1, Ask: 2}}}},
	}}
	var stderr bytes.Buffer
	s := newTestStreamer(fake, io.Discard, &stderr, dukascopy.GranularityTick, "jsonl")
	s.publishers = []livePublisher{{name: "redis", publish: func(context.Context, []byte) error { return errors.New("connection refused") }}}
	if err := s.pollTicks(context.Background(), time.Date(2024, 1, 2, 10, 0, 2, 0, time.UTC)); err != nil {
		t.Fatalf("publisher errors must not stop the stream: %v", err)
	}
	if !strings.Contains(stderr.String(), "redis publish failed") {
		t.Fatalf("expected publisher error on stderr, got %q", stderr.String())
	}
}
