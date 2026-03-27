package cli

import (
	"bytes"
	"context"
	"flag"
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
