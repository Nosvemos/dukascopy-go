package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/buildinfo"
	"github.com/Nosvemos/dukascopy-go/internal/checkpoint"
	"github.com/Nosvemos/dukascopy-go/internal/csvout"
)

func TestRunInstrumentsAndStatsErrorBranches(t *testing.T) {
	server := newCLITestServer()
	defer server.Close()

	if err := runInstruments([]string{"--query", "xauusd", "--limit", "0", "--base-url", server.URL}, &bytes.Buffer{}); err == nil {
		t.Fatal("expected invalid limit error")
	}
	if err := runInstruments([]string{"--query", "unknown", "--base-url", server.URL}, &bytes.Buffer{}); err == nil {
		t.Fatal("expected no instruments found error")
	}

	if err := runStats([]string{}, &bytes.Buffer{}); err == nil {
		t.Fatal("expected missing input error")
	}
}

func TestPrintVersionAndRunSwitchBranches(t *testing.T) {
	previousVersion := buildinfo.Version
	previousCommit := buildinfo.Commit
	previousDate := buildinfo.Date
	defer func() {
		buildinfo.Version = previousVersion
		buildinfo.Commit = previousCommit
		buildinfo.Date = previousDate
	}()

	buildinfo.Version = "v1.0.0"
	buildinfo.Commit = "abc123"
	buildinfo.Date = "2026-03-26"

	var out bytes.Buffer
	printVersion(&out)
	if !strings.Contains(out.String(), "commit: abc123") || !strings.Contains(out.String(), "date:   2026-03-26") {
		t.Fatalf("unexpected version output: %s", out.String())
	}

	out.Reset()
	if code := Run([]string{"list-timeframes"}, &out, &bytes.Buffer{}); code != 0 {
		t.Fatalf("expected list-timeframes to succeed, got %d", code)
	}
}

func TestRunManifestVerifyJSONFailureBranch(t *testing.T) {
	dir := t.TempDir()
	outputPath := filepath.Join(dir, "dataset.csv")
	partPath := filepath.Join(dir, "part.csv")
	content := "timestamp,mid_close\n2024-01-01T00:00:00Z,1.1\n2024-01-01T00:00:00Z,1.1\n"
	if err := os.WriteFile(outputPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := os.WriteFile(partPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	audit, _ := csvout.AuditCSV(partPath)
	outAudit, _ := csvout.AuditCSV(outputPath)
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
		FinalOutput: &checkpoint.ManifestOutput{Rows: outAudit.Rows, Bytes: outAudit.Bytes, SHA256: outAudit.SHA256},
	}
	if err := checkpoint.Save(checkpoint.DefaultManifestPath(outputPath), manifest); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	if err := runManifestVerify([]string{"--output", outputPath, "--json", "--check-data-quality"}, &bytes.Buffer{}); err == nil {
		t.Fatal("expected data-quality verification failure")
	}
}
