package cli

import (
	"strings"
	"testing"
	"time"
)

func TestProgressTUIFormatHelpers(t *testing.T) {
	cases := []struct {
		val      int
		expected string
	}{
		{-1, ""},
		{0, "0"},
		{5, "5"},
		{100, "100"},
		{1000, "1,000"},
		{1234567, "1,234,567"},
	}

	for _, tc := range cases {
		got := formatCount(tc.val)
		if got != tc.expected {
			t.Errorf("formatCount(%d) = %q, expected %q", tc.val, got, tc.expected)
		}
	}

	byteCases := []struct {
		val      int64
		expected string
	}{
		{-1, ""},
		{0, "0 B"},
		{500, "500 B"},
		{1024, "1.00 KB"},
		{1500, "1.46 KB"},
		{1024 * 1024, "1.00 MB"},
		{100 * 1024 * 1024, "100 MB"},
		{1024 * 1024 * 1024, "1.00 GB"},
	}

	for _, tc := range byteCases {
		got := formatByteCount(tc.val)
		if got != tc.expected {
			t.Errorf("formatByteCount(%d) = %q, expected %q", tc.val, got, tc.expected)
		}
	}
}

func TestProgressTUIModelCalculations(t *testing.T) {
	model := progressTUIModel{
		noColor:        true,
		partitionTotal: 10,
		partitionCompleted: 3,
		chunkTotal:     5,
		chunkCurrent:   2,
		chunkScope:     "minute",
		chunkDetail:    "2026-05-25",
		completedRows:  1000,
		completedBytes: 50000,
	}

	frac := model.progressFraction()
	if frac != 0.3 {
		t.Errorf("expected fraction 0.3, got %f", frac)
	}

	model.partitionTotal = 0
	frac2 := model.progressFraction()
	if frac2 != 0.4 {
		t.Errorf("expected fraction 0.4, got %f", frac2)
	}

	summary := model.partitionSummary()
	if summary != "-" {
		t.Errorf("expected '-' partitionSummary when total <= 0, got %q", summary)
	}

	model.partitionTotal = 10
	summary2 := model.partitionSummary()
	if !strings.Contains(summary2, "3/10") {
		t.Errorf("expected '3/10' partitionSummary, got %q", summary2)
	}

	chunkSum := model.chunkSummary()
	if !strings.Contains(chunkSum, "minute 2/5") {
		t.Errorf("expected 'minute 2/5' chunkSummary, got %q", chunkSum)
	}

	// Throughput snapshots
	model.throughputStartedAt = time.Now().Add(-10 * time.Second)
	model.completedRows = 2000
	model.completedBytes = 100000
	speed := model.speedText()
	if !strings.Contains(speed, "rows/s") {
		t.Errorf("expected rows/s in speedText, got %q", speed)
	}

	eta := model.etaText()
	if eta == "" {
		t.Errorf("expected non-empty etaText, got %q", eta)
	}
}

func TestProgressViewRendersFine(t *testing.T) {
	model := newProgressTUIModel(true, "")
	model.symbol = "EURUSD"
	model.timeframe = "m1"
	model.side = "BID"
	model.width = 80
	model.height = 20

	view := model.View()
	if !strings.Contains(view, "DUKASCOPY-GO") {
		t.Errorf("expected 'DUKASCOPY-GO' in view, got: %s", view)
	}
}
