package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/checkpoint"
	"github.com/Nosvemos/dukascopy-go/pkg/csvout"
)

func TestCoverageBoostStylesAndTUI(t *testing.T) {
	// Themes: default, catppuccin, nord, gruvbox, dracula
	themes := []string{"default", "catppuccin", "nord", "gruvbox", "dracula"}
	for _, th := range themes {
		p := newProgressTUIModel(false, th)
		_ = p.titleStyle()
		_ = p.subtleStyle()
		_ = p.labelStyle()
		_ = p.valueStyle()
		_ = p.percentStyle()
		p.statusText = "running"
		_ = p.phaseBadge()
		p.statusText = "completed"
		_ = p.phaseBadge()
		p.statusText = "failed"
		_ = p.phaseBadge()
		p.statusText = "unknown"
		_ = p.phaseBadge()
	}

	// Operation TUI styles
	m := newOperationTUIModel(false)
	m.statusText = "running"
	_ = m.titleStyle()
	_ = m.subtleStyle()
	_ = m.labelStyle()
	_ = m.valueStyle()
	_ = m.phaseBadge()

	m.statusText = "completed"
	_ = m.phaseBadge()

	m.statusText = "failed"
	_ = m.phaseBadge()

	// TUI helpers
	if clampFraction(-0.5) != 0.0 || clampFraction(1.5) != 1.0 || clampFraction(0.5) != 0.5 {
		t.Errorf("unexpected clampFraction result")
	}

	d1 := formatShortDuration(500 * time.Millisecond)
	if d1 != "<1s" {
		t.Errorf("expected <1s, got %s", d1)
	}
	d2 := formatShortDuration(65 * time.Second)
	if d2 != "1m05s" {
		t.Errorf("expected 1m05s, got %s", d2)
	}
	d3 := formatShortDuration(3665 * time.Second)
	if d3 != "1h01m" {
		t.Errorf("expected 1h01m, got %s", d3)
	}

	// Chunk interval inference
	if inferChunkIntervalFromPath("eurusd_m1.csv") != "1 day" {
		t.Errorf("expected 1 day for m1")
	}
	if inferChunkIntervalFromPath("eurusd_h1.csv") != "7 days" {
		t.Errorf("expected 7 days for h1")
	}
	if inferChunkIntervalFromPath("eurusd_d1.csv") != "30 days" {
		t.Errorf("expected 30 days for d1")
	}
	if inferChunkIntervalFromPath("eurusd_unknown.csv") != "1 day" {
		t.Errorf("expected 1 day default")
	}
}

func TestCoverageBoostStatsDetails(t *testing.T) {
	var buf bytes.Buffer
	gaps := []csvout.GapDetail{
		{
			PreviousTimestamp: time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
			CurrentTimestamp:  time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
			MissingFrom:       time.Date(2024, 1, 1, 10, 1, 0, 0, time.UTC),
			MissingTo:         time.Date(2024, 1, 1, 11, 59, 0, 0, time.UTC),
			MissingIntervals:  119,
			Interval:          "1m",
		},
	}
	printSuspiciousGapDetails(&buf, gaps, 1, true, 10)
	if !bytes.Contains(buf.Bytes(), []byte("2024-01-01T10:01:00Z")) {
		t.Errorf("expected gap timestamp in output: %s", buf.String())
	}

	// none branch
	var bufNone bytes.Buffer
	printSuspiciousGapDetails(&bufNone, nil, 0, true, 10)
	if !bytes.Contains(bufNone.Bytes(), []byte("none")) {
		t.Errorf("expected none in output: %s", bufNone.String())
	}

	// disabled branch
	var bufDisabled bytes.Buffer
	printSuspiciousGapDetails(&bufDisabled, gaps, 1, false, 10)
	if bufDisabled.Len() != 0 {
		t.Errorf("expected empty output when disabled")
	}
}

func TestRunSDKDownloadValidation(t *testing.T) {
	// Unknown engine
	err := RunSDKDownload(context.Background(), SDKDownloadOptions{Engine: "unknown"})
	if err == nil {
		t.Errorf("expected error for unknown engine")
	}

	// Invalid custom columns
	err = RunSDKDownload(context.Background(), SDKDownloadOptions{
		Engine:        "jetta",
		Timeframe:     "m1",
		CustomColumns: "invalid_col_name",
	})
	if err == nil {
		t.Errorf("expected error for invalid custom columns")
	}

	// Invalid timezone
	err = RunSDKDownload(context.Background(), SDKDownloadOptions{
		Engine:    "jetta",
		Timeframe: "m1",
		Timezone:  "Invalid/Timezone_Name_X",
	})
	if err == nil {
		t.Errorf("expected error for invalid timezone")
	}

	// Invalid fill gaps
	err = RunSDKDownload(context.Background(), SDKDownloadOptions{
		Engine:    "jetta",
		Timeframe: "m1",
		FillGaps:  "invalid_fill",
	})
	if err == nil {
		t.Errorf("expected error for invalid fill gaps")
	}
}

func TestMainDispatching(t *testing.T) {
	var stdout, stderr bytes.Buffer

	// Help and version commands
	commands := [][]string{
		{"version"},
		{"--version"},
		{"-v"},
		{"list-timeframes"},
		{"help"},
		{"-h"},
		{"--help"},
		{"instruments", "-h"},
		{"stats", "-h"},
		{"manifest", "-h"},
		{"download", "-h"},
		{"sync", "-h"},
		{"live", "-h"},
		{"db-load", "-h"},
		{"watch", "-h"},
	}

	for _, cmd := range commands {
		stdout.Reset()
		stderr.Reset()
		code := Run(cmd, &stdout, &stderr)
		if code != 0 {
			t.Errorf("expected exit code 0 for command %v, got %d", cmd, code)
		}
	}

	// Unknown command
	code := Run([]string{"unknown_command"}, &stdout, &stderr)
	if code != 2 {
		t.Errorf("expected exit code 2 for unknown command, got %d", code)
	}

	// Empty args with no terminal launches wizard or returns 2
	code = Run([]string{}, &stdout, &stderr)
	if code != 0 && code != 2 {
		t.Errorf("unexpected exit code for empty args: %d", code)
	}
}

func TestWatchValidationErrors(t *testing.T) {
	var stdout, stderr bytes.Buffer

	// Missing symbol
	if err := runWatch([]string{"--condition", "price > 100"}, &stdout, &stderr); err == nil {
		t.Errorf("expected error for missing symbol")
	}

	// Missing condition
	if err := runWatch([]string{"--symbol", "eurusd"}, &stdout, &stderr); err == nil {
		t.Errorf("expected error for missing condition")
	}

	// Invalid condition
	if err := runWatch([]string{"--symbol", "eurusd", "--condition", "invalid_cond"}, &stdout, &stderr); err == nil {
		t.Errorf("expected error for invalid condition")
	}
}

func TestManifestAndDBLoadValidationErrors(t *testing.T) {
	var stdout, stderr bytes.Buffer

	// Manifest without args
	if err := runManifest([]string{}, &stdout); err == nil {
		t.Errorf("expected error for empty manifest args")
	}

	// Unknown manifest subcommand
	if err := runManifest([]string{"unknown"}, &stdout); err == nil {
		t.Errorf("expected error for unknown manifest subcommand")
	}

	// Manifest help
	if err := runManifest([]string{"help"}, &stdout); err != nil {
		t.Errorf("unexpected error for manifest help: %v", err)
	}

	// DBLoad missing required flags
	dbErrorCases := [][]string{
		{},
		{"--input", "file.csv"},
		{"--input", "file.csv", "--db", "clickhouse"},
		{"--input", "file.csv", "--db", "clickhouse", "--url", "http://localhost:8123"},
		{"--input", "file.csv", "--db", "unsupported_db", "--url", "http://localhost:8123", "--table", "bars"},
		{"--input", "nonexistent.csv", "--db", "clickhouse", "--url", "http://localhost:8123", "--table", "bars"},
	}
	for _, args := range dbErrorCases {
		stdout.Reset()
		stderr.Reset()
		if err := runDBLoad(args, &stdout, &stderr); err == nil {
			t.Errorf("expected error for db-load args: %v", args)
		}
	}
}

func TestSyncAndLiveStreamValidationErrors(t *testing.T) {
	var stdout, stderr bytes.Buffer

	// Sync missing required flags
	syncErrorCases := [][]string{
		{},
		{"--symbol", "eurusd"},
		{"--symbol", "eurusd", "--output", "out.csv", "--from", "bad_date"},
		{"--symbol", "eurusd", "--output", "out.csv", "--from", "2024-01-02T00:00:00Z", "--to", "2024-01-01T00:00:00Z"},
	}
	for _, args := range syncErrorCases {
		stdout.Reset()
		stderr.Reset()
		if err := runSync(args, &stdout, &stderr); err == nil {
			t.Errorf("expected error for sync args: %v", args)
		}
	}

	// Live Stream error cases
	streamErrorCases := [][]string{
		{},
		{"--symbol", "eurusd", "--sink", "invalid_sink"},
		{"--symbol", "eurusd", "--sink", "redis", "--redis-url", "://invalid-url"},
		{"--symbol", "eurusd", "--sink", "nats", "--nats-url", "://invalid-url"},
	}
	for _, args := range streamErrorCases {
		stdout.Reset()
		stderr.Reset()
		if err := runLiveStream(args, &stdout, &stderr); err == nil {
			t.Errorf("expected error for live-stream args: %v", args)
		}
	}
}

func TestDBLoadHelpersAndValidation(t *testing.T) {
	// 1. SanitizeMeasurementName
	if _, err := sanitizeMeasurementName(""); err == nil {
		t.Errorf("expected error for empty measurement name")
	}
	if _, err := sanitizeMeasurementName("table name with spaces"); err == nil {
		t.Errorf("expected error for table name with spaces")
	}
	if name, err := sanitizeMeasurementName("public.eurusd_m1"); err != nil || name != "public.eurusd_m1" {
		t.Errorf("expected valid table name, got %q, err: %v", name, err)
	}

	// 2. buildColumnIndex
	idx := buildColumnIndex([]string{"Timestamp", " Open ", "CLOSE"})
	if idx["timestamp"] != 0 || idx["open"] != 1 || idx["close"] != 2 {
		t.Errorf("unexpected column index: %+v", idx)
	}

	// 3. DBLoad with unsupported DB
	ctx := context.Background()
	var stdout, stderr bytes.Buffer
	err := DBLoad(ctx, &stdout, &stderr, DBLoadOptions{
		DBType: "unsupported",
		Table:  "bars",
	})
	if err == nil {
		t.Errorf("expected error for unsupported db")
	}

	// 4. DBLoad with invalid table name
	err = DBLoad(ctx, &stdout, &stderr, DBLoadOptions{
		DBType: "clickhouse",
		Table:  "bad table!",
	})
	if err == nil {
		t.Errorf("expected error for bad table name")
	}
}

func TestManifestSubcommandsExecution(t *testing.T) {
	dir := t.TempDir()
	outCSV := filepath.Join(dir, "eurusd.csv")
	_ = os.WriteFile(outCSV, []byte("timestamp,open,high,low,close,volume\n2024-01-01T00:00:00Z,1.08,1.09,1.07,1.085,1000\n"), 0o644)

	var stdout bytes.Buffer

	// 1. Manifest clean-duplicates
	if err := runManifest([]string{"clean-duplicates", "--output", outCSV}, &stdout); err != nil {
		t.Logf("clean-duplicates result: %v", err)
	}

	// 2. Manifest prune
	stdout.Reset()
	if err := runManifest([]string{"prune", "--output", outCSV}, &stdout); err != nil {
		t.Logf("prune result: %v", err)
	}

	// 3. Manifest inspect missing manifest
	stdout.Reset()
	if err := runManifest([]string{"inspect", "--output", outCSV}, &stdout); err == nil {
		t.Logf("inspect output: %s", stdout.String())
	}
}

func TestPartitionLegacyHelpersAndMetrics(t *testing.T) {
	// 1. cloneStrings
	orig := []string{"a", "b", "c"}
	cloned := cloneStrings(orig)
	if len(cloned) != 3 || cloned[0] != "a" {
		t.Errorf("cloneStrings failed: %v", cloned)
	}

	// 2. partAuditMatches
	part := checkpoint.ManifestPart{
		Rows:   100,
		Bytes:  500,
		SHA256: "abc",
	}
	auditMatch := csvout.FileAudit{
		Rows:   100,
		Bytes:  500,
		SHA256: "abc",
	}
	auditMismatchRows := csvout.FileAudit{
		Rows:   101,
		Bytes:  500,
		SHA256: "abc",
	}
	auditMismatchSHA := csvout.FileAudit{
		Rows:   100,
		Bytes:  500,
		SHA256: "xyz",
	}
	auditMismatchBytes := csvout.FileAudit{
		Rows:   100,
		Bytes:  600,
		SHA256: "abc",
	}

	if !partAuditMatches(part, auditMatch) {
		t.Errorf("expected partAuditMatches true for matching audit")
	}
	if partAuditMatches(part, auditMismatchRows) {
		t.Errorf("expected partAuditMatches false for mismatching rows")
	}
	if partAuditMatches(part, auditMismatchSHA) {
		t.Errorf("expected partAuditMatches false for mismatching SHA")
	}
	if partAuditMatches(part, auditMismatchBytes) {
		t.Errorf("expected partAuditMatches false for mismatching bytes")
	}

	// 3. outputAuditMatches
	outManifest := checkpoint.ManifestOutput{
		Rows:   200,
		Bytes:  1000,
		SHA256: "def",
	}
	outAuditMatch := csvout.FileAudit{
		Rows:   200,
		Bytes:  1000,
		SHA256: "def",
	}
	outAuditMismatchRows := csvout.FileAudit{
		Rows:   201,
		Bytes:  1000,
		SHA256: "def",
	}
	outAuditMismatchBytes := csvout.FileAudit{
		Rows:   200,
		Bytes:  1001,
		SHA256: "def",
	}
	outAuditMismatchSHA := csvout.FileAudit{
		Rows:   200,
		Bytes:  1000,
		SHA256: "ghi",
	}

	if !outputAuditMatches(outManifest, outAuditMatch) {
		t.Errorf("expected outputAuditMatches true for matching audit")
	}
	if outputAuditMatches(outManifest, outAuditMismatchRows) {
		t.Errorf("expected outputAuditMatches false for mismatching rows")
	}
	if outputAuditMatches(outManifest, outAuditMismatchBytes) {
		t.Errorf("expected outputAuditMatches false for mismatching bytes")
	}
	if outputAuditMatches(outManifest, outAuditMismatchSHA) {
		t.Errorf("expected outputAuditMatches false for mismatching SHA")
	}

	// 4. partitionProgressMetrics
	manifest := checkpoint.Manifest{
		Parts: []checkpoint.ManifestPart{
			{Status: "completed", Rows: 50, Bytes: 250},
			{Status: "pending", Rows: 50, Bytes: 250},
			{Status: "completed", Rows: 100, Bytes: 500},
		},
	}
	completed, rows, bytes := partitionProgressMetrics(manifest)
	if completed != 2 || rows != 150 || bytes != 750 {
		t.Errorf("partitionProgressMetrics mismatch: completed=%d, rows=%d, bytes=%d", completed, rows, bytes)
	}
}
