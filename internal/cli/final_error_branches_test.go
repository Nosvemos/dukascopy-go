package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/checkpoint"
	"github.com/Nosvemos/dukascopy-go/internal/csvout"
	"github.com/Nosvemos/dukascopy-go/internal/dukascopy"
)

func TestManifestFailureAndPruneBranches(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	dir := t.TempDir()
	outputDir := filepath.Join(dir, "out")
	manifestDir := filepath.Join(dir, "meta")
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		t.Fatalf("MkdirAll returned error: %v", err)
	}
	if err := os.MkdirAll(manifestDir, 0o755); err != nil {
		t.Fatalf("MkdirAll returned error: %v", err)
	}

	outputPath := filepath.Join(outputDir, "dataset.csv")
	partPath := filepath.Join(dir, "part.csv")
	if err := os.WriteFile(outputPath, []byte("timestamp,mid_close\n2024-01-01T00:00:00Z,1.1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := os.WriteFile(partPath, []byte("timestamp,mid_close\n2024-01-01T00:00:00Z,1.1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	audit, err := csvout.AuditCSV(partPath)
	if err != nil {
		t.Fatalf("AuditCSV returned error: %v", err)
	}

	manifestPath := filepath.Join(manifestDir, "dataset.csv.manifest.json")
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
			Status: "failed",
			Rows:   audit.Rows,
			Bytes:  audit.Bytes,
			SHA256: audit.SHA256,
		}},
		FinalOutput: &checkpoint.ManifestOutput{
			Rows:   audit.Rows,
			Bytes:  audit.Bytes,
			SHA256: audit.SHA256,
		},
	}
	if err := checkpoint.Save(manifestPath, manifest); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	var out bytes.Buffer
	if err := runManifestVerify([]string{"--manifest", manifestPath}, &out); err == nil {
		t.Fatal("expected plain manifest verify failure")
	}
	if !strings.Contains(out.String(), "invalid") {
		t.Fatalf("unexpected manifest verify output: %s", out.String())
	}

	manifestTemp := filepath.Join(manifestDir, filepath.Base(manifestPath)+".tmp-1")
	outputTemp := filepath.Join(outputDir, filepath.Base(outputPath)+".tmp-1")
	outputResume := filepath.Join(outputDir, filepath.Base(outputPath)+".resume-1.csv")
	for _, path := range []string{manifestTemp, outputTemp, outputResume} {
		if err := os.WriteFile(path, []byte("tmp"), 0o644); err != nil {
			t.Fatalf("WriteFile returned error: %v", err)
		}
	}

	out.Reset()
	if err := runManifestPrune([]string{"--manifest", manifestPath}, &out); err != nil {
		t.Fatalf("runManifestPrune returned error: %v", err)
	}
	if !strings.Contains(out.String(), "removed") {
		t.Fatalf("unexpected prune output: %s", out.String())
	}
	for _, path := range []string{manifestTemp, outputTemp, outputResume} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("expected temp file %s to be pruned, got err=%v", path, err)
		}
	}
}

func TestPartitionAndDownloadAdditionalBranches(t *testing.T) {
	server := newCLITestServer()
	defer server.Close()

	now := time.Now()

	if _, err := normalizePartition("auto", dukascopy.Granularity("odd")); err == nil {
		t.Fatal("expected unsupported auto partition error")
	}
	if _, err := normalizePartition("weird", dukascopy.GranularityM1); err == nil {
		t.Fatal("expected unsupported partition value error")
	}
	if _, err := buildPartitions(now, now, partitionDay); err == nil {
		t.Fatal("expected empty partition range error")
	}
	if _, err := nextPartitionBoundary(now, "odd"); err == nil {
		t.Fatal("expected unsupported boundary mode error")
	}

	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "dataset.manifest.json")
	request := dukascopy.DownloadRequest{
		Symbol:      "xauusd",
		Granularity: dukascopy.GranularityM1,
		Side:        dukascopy.PriceSideBid,
		From:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		To:          time.Date(2024, 1, 2, 1, 0, 0, 0, time.UTC),
	}
	badManifest := checkpoint.Manifest{
		Version:    checkpoint.CurrentManifestVersion,
		OutputPath: filepath.Join(dir, "dataset.csv"),
		PartsDir:   checkpoint.DefaultPartsDir(filepath.Join(dir, "dataset.csv")),
		Symbol:     "eurusd",
		Timeframe:  "m1",
		Side:       "BID",
		ResultKind: "bar",
		Columns:    []string{"timestamp", "open"},
		Partition:  partitionHour,
		Parts: []checkpoint.ManifestPart{{
			ID:    "20240102T000000Z_20240102T010000Z",
			Start: request.From,
			End:   request.To,
			File:  "20240102T000000Z_20240102T010000Z.csv",
			Status:"pending",
		}},
	}
	if err := checkpoint.Save(manifestPath, badManifest); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}
	if err := runPartitionedDownload(context.Background(), dukascopy.NewClient(server.URL, time.Second), &bytes.Buffer{}, &bytes.Buffer{}, filepath.Join(dir, "dataset.csv"), manifestPath, request, dukascopy.ResultKindBar, []string{"timestamp", "open"}, nil, partitionHour, 1); err == nil {
		t.Fatal("expected incompatible manifest error")
	}

	partsDir := filepath.Join(dir, "parts")
	if err := os.MkdirAll(partsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll returned error: %v", err)
	}
	client := dukascopy.NewClient(server.URL, time.Second)
	rows, err := downloadPartitionToFile(context.Background(), client, filepath.Join(partsDir, "primary.csv"), request, dukascopy.ResultKindBar, []string{"timestamp", "open"}, nil)
	if err != nil {
		t.Fatalf("downloadPartitionToFile primary branch returned error: %v", err)
	}
	if rows == 0 {
		t.Fatal("expected primary bar rows to be written")
	}

	manifest := checkpoint.Manifest{
		Version:    checkpoint.CurrentManifestVersion,
		OutputPath: filepath.Join(dir, "parallel.csv"),
		PartsDir:   partsDir,
		Symbol:     "xauusd",
		Timeframe:  "m1",
		Side:       "BID",
		ResultKind: "bar",
		Columns:    []string{"timestamp", "open"},
		Partition:  partitionHour,
		Parts: []checkpoint.ManifestPart{{
			ID:    "known",
			Start: request.From,
			End:   request.To,
			File:  "known.csv",
			Status:"pending",
		}},
	}
	if err := checkpoint.Save(manifestPath, manifest); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	err = executePartitionDownloads(context.Background(), client, manifestPath, &manifest, []partitionWorkItem{{
		Index: 0,
		Partition: downloadPartition{
			ID:    "missing",
			Start: request.From,
			End:   request.To,
			File:  "missing.csv",
		},
	}}, partsDir, request, dukascopy.ResultKindBar, []string{"timestamp", "open"}, nil, 2)
	if err == nil {
		t.Fatal("expected executePartitionDownloads missing-part error")
	}

	if partAuditMatches(checkpoint.ManifestPart{Rows: 1, Bytes: 10, SHA256: "a"}, csvout.FileAudit{Rows: 1, Bytes: 11, SHA256: "a"}) {
		t.Fatal("expected partAuditMatches byte mismatch")
	}
	if partAuditMatches(checkpoint.ManifestPart{Rows: 1, Bytes: 10, SHA256: "a"}, csvout.FileAudit{Rows: 1, Bytes: 10, SHA256: "b"}) {
		t.Fatal("expected partAuditMatches sha mismatch")
	}
	if outputAuditMatches(checkpoint.ManifestOutput{Rows: 1, Bytes: 10, SHA256: "a"}, csvout.FileAudit{Rows: 1, Bytes: 11, SHA256: "a"}) {
		t.Fatal("expected outputAuditMatches byte mismatch")
	}
	if outputAuditMatches(checkpoint.ManifestOutput{Rows: 1, Bytes: 10, SHA256: "a"}, csvout.FileAudit{Rows: 1, Bytes: 10, SHA256: "b"}) {
		t.Fatal("expected outputAuditMatches sha mismatch")
	}
}
