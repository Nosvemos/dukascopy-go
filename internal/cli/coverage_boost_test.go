package cli

import (
	"bytes"
	"context"
	"testing"
	"time"

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
