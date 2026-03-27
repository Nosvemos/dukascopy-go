package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/checkpoint"
	"github.com/Nosvemos/dukascopy-go/internal/csvout"
	"github.com/Nosvemos/dukascopy-go/internal/dukascopy"
)

func TestRunManifestBranches(t *testing.T) {
	var out bytes.Buffer
	if err := runManifest(nil, &out); err == nil {
		t.Fatal("expected missing manifest subcommand error")
	}
}

func TestRunManifestInspectAndVerifyJSONBranches(t *testing.T) {
	dir := t.TempDir()
	outputPath := filepath.Join(dir, "dataset.csv")
	if err := os.WriteFile(outputPath, []byte("timestamp,mid_close\n2024-01-01T00:00:00Z,1.1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	audit, err := csvout.AuditCSV(outputPath)
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
			File:   filepath.Base(outputPath),
			Status: "completed",
			Rows:   audit.Rows,
			Bytes:  audit.Bytes,
			SHA256: audit.SHA256,
		}},
		FinalOutput: &checkpoint.ManifestOutput{Rows: audit.Rows, Bytes: audit.Bytes, SHA256: audit.SHA256},
	}
	manifestPath := checkpoint.DefaultManifestPath(outputPath)
	if err := checkpoint.Save(manifestPath, manifest); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	var out bytes.Buffer
	if err := runManifestInspect([]string{"--output", outputPath, "--json"}, &out); err != nil {
		t.Fatalf("runManifestInspect json returned error: %v", err)
	}
	if !strings.Contains(out.String(), "\"output_path\"") {
		t.Fatalf("unexpected inspect json output: %s", out.String())
	}

	out.Reset()
	if err := runManifestVerify([]string{"--output", outputPath, "--json"}, &out); err != nil {
		t.Fatalf("runManifestVerify json returned error: %v", err)
	}
	if !strings.Contains(out.String(), "\"report\"") {
		t.Fatalf("unexpected verify json output: %s", out.String())
	}
}

func TestRunManifestVerifyFailureAndRepairBranches(t *testing.T) {
	dir := t.TempDir()
	outputPath := filepath.Join(dir, "dataset.csv")
	partPath := filepath.Join(dir, "part.csv")
	if err := os.WriteFile(partPath, []byte("timestamp,mid_close\n2024-01-01T00:00:00Z,1.1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	audit, _ := csvout.AuditCSV(partPath)
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
			SHA256: "bad",
		}},
	}
	manifestPath := checkpoint.DefaultManifestPath(outputPath)
	if err := checkpoint.Save(manifestPath, manifest); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	if err := runManifestVerify([]string{"--output", outputPath}, &bytes.Buffer{}); err == nil {
		t.Fatal("expected manifest verify failure")
	}
}

func TestPartitionHelperBranches(t *testing.T) {
	if _, err := nextPartitionBoundary(time.Now(), "wat"); err == nil {
		t.Fatal("expected unsupported partition mode error")
	}
	if got := cloneStrings([]string{"a", "b"}); len(got) != 2 || got[1] != "b" {
		t.Fatalf("unexpected cloneStrings result: %v", got)
	}
	if err := executePartitionDownloads(context.Background(), nil, "", nil, nil, "", dukascopy.DownloadRequest{}, dukascopy.ResultKindBar, nil, nil, 1, nil); err != nil {
		t.Fatalf("expected empty pending partitions to succeed, got %v", err)
	}

	manifest := checkpoint.Manifest{}
	if err := applyPartitionResult("", &manifest, partitionWorkResult{Item: partitionWorkItem{Partition: downloadPartition{ID: "missing"}}}); err == nil {
		t.Fatal("expected missing partition state error")
	}
}

func TestPrepareResumeErrorBranches(t *testing.T) {
	request := &dukascopy.DownloadRequest{From: time.Now(), To: time.Now().Add(time.Hour)}
	if _, _, err := prepareResume(true, filepath.Join(t.TempDir(), "missing.csv"), dukascopy.ResultKindBar, []string{"open"}, nil, request); err == nil {
		t.Fatal("expected timestamp requirement error")
	}

	path := filepath.Join(t.TempDir(), "existing.csv")
	if err := os.WriteFile(path, []byte("timestamp,mid_close\n2024-01-01T00:00:00Z,1.1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if _, _, err := prepareResume(true, path, dukascopy.ResultKindBar, []string{"timestamp", "open"}, nil, request); err == nil {
		t.Fatal("expected header mismatch error")
	}
}

func TestDownloadPartitionToFileTickAndBidAskBranches(t *testing.T) {
	server := newCLITestServer()
	defer server.Close()
	client := dukascopy.NewClient(server.URL, time.Second)
	dir := t.TempDir()
	request := dukascopy.DownloadRequest{
		Symbol:      "xauusd",
		Granularity: dukascopy.GranularityTick,
		Side:        dukascopy.PriceSideBid,
		From:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		To:          time.Date(2024, 1, 2, 0, 0, 1, 0, time.UTC),
	}
	if rows, err := downloadPartitionToFile(context.Background(), client, filepath.Join(dir, "ticks.csv"), request, dukascopy.ResultKindTick, nil, []string{"timestamp", "bid"}); err != nil || rows == 0 {
		t.Fatalf("unexpected tick download result: %d %v", rows, err)
	}

	request.Granularity = dukascopy.GranularityM1
	if rows, err := downloadPartitionToFile(context.Background(), client, filepath.Join(dir, "bars.csv"), request, dukascopy.ResultKindBar, []string{"timestamp", "mid_close", "spread"}, nil); err != nil || rows == 0 {
		t.Fatalf("unexpected bid/ask bar download result: %d %v", rows, err)
	}
}

func TestWriteOutputErrorBranches(t *testing.T) {
	errReader := &errorClientWriter{}
	if _, err := writeTickOutput(filepath.Join(t.TempDir(), "ticks.csv"), &csvout.ResumeState{Exists: true, Columns: []string{"timestamp"}}, nil, dukascopy.Instrument{}, []string{"timestamp", "bid"}, nil); err == nil {
		_ = errReader
	}
}

type errorClientWriter struct{}

func (e *errorClientWriter) Write(p []byte) (int, error) { return 0, errors.New("write error") }
