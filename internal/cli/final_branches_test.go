package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/checkpoint"
	"github.com/Nosvemos/dukascopy-go/internal/csvout"
	"github.com/Nosvemos/dukascopy-go/internal/dukascopy"
)

func TestRunNoArgsAndHelpers(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if code := Run(nil, &stdout, &stderr); code != 2 {
		t.Fatalf("expected empty args to return 2, got %d", code)
	}
	if got := maxInt(5, 2); got != 5 {
		t.Fatalf("unexpected maxInt result: %d", got)
	}
}

func TestRunManifestRepairNoOpAndPruneMissingDir(t *testing.T) {
	dir := t.TempDir()
	outputPath := filepath.Join(dir, "dataset.csv")
	content := "timestamp,mid_close\n2024-01-01T00:00:00Z,1.1\n"
	if err := os.WriteFile(outputPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	audit, err := csvout.AuditCSV(outputPath)
	if err != nil {
		t.Fatalf("AuditCSV returned error: %v", err)
	}
	manifest := checkpoint.Manifest{
		Version:    checkpoint.CurrentManifestVersion,
		OutputPath: outputPath,
		PartsDir:   filepath.Join(dir, "missing-parts"),
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
			File:   "part-1.csv",
			Status: "completed",
			Rows:   audit.Rows,
		}},
		Completed:   true,
		FinalOutput: &checkpoint.ManifestOutput{Rows: audit.Rows, Bytes: audit.Bytes, SHA256: audit.SHA256},
	}
	manifestPath := checkpoint.DefaultManifestPath(outputPath)
	if err := checkpoint.Save(manifestPath, manifest); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	var out bytes.Buffer
	if err := runManifestRepair([]string{"--output", outputPath}, &out); err != nil {
		t.Fatalf("runManifestRepair no-op returned error: %v", err)
	}
	if err := runManifestPrune([]string{"--output", outputPath}, &out); err != nil {
		t.Fatalf("runManifestPrune missing-dir returned error: %v", err)
	}
}

func TestPrepareResumeNoExistAndWriteBarResumeError(t *testing.T) {
	request := &dukascopy.DownloadRequest{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}
	state, dedupe, err := prepareResume(true, filepath.Join(t.TempDir(), "missing.csv"), dukascopy.ResultKindBar, []string{"timestamp", "open"}, nil, request)
	if err != nil || state != nil || dedupe != nil {
		t.Fatalf("expected missing resume target to no-op, got state=%v dedupe=%v err=%v", state, dedupe, err)
	}

	path := filepath.Join(t.TempDir(), "existing.csv")
	if err := os.WriteFile(path, []byte("timestamp,open\n2024-01-01T00:00:00Z,1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	resumeState := &csvout.ResumeState{Exists: true, Columns: []string{"timestamp", "open"}}
	if _, err := writeBarOutput(path, resumeState, []string{"not-found"}, dukascopy.Instrument{PriceScale: 0}, []string{"timestamp", "open"}, []dukascopy.Bar{{Time: time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC), Open: 2}}, nil, nil); err == nil {
		t.Fatal("expected resume merge error")
	}
}
