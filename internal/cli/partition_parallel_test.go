package cli

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/checkpoint"
	"github.com/Nosvemos/dukascopy-go/internal/dukascopy"
)

func TestPartitionedDownloadParallelReuseAndReassemble(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/instruments":
			writeCLIJSON(w, map[string]any{
				"instruments": []map[string]any{
					{"id": 1, "name": "XAU/USD", "code": "XAU-USD", "description": "Gold vs US Dollar", "priceScale": 3},
				},
			})
		case "/v1/candles/minute/XAU-USD/BID/2024/1/2":
			writeCLIJSON(w, map[string]any{
				"timestamp":  1704153600000,
				"multiplier": 1.0,
				"open":       100.0,
				"high":       101.0,
				"low":        99.0,
				"close":      100.5,
				"shift":      60000,
				"times":      []int{0, 60},
				"opens":      []float64{0, 1},
				"highs":      []float64{0, 1},
				"lows":       []float64{0, 1},
				"closes":     []float64{0, 1},
				"volumes":    []float64{0.001, 0.002},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	dir := t.TempDir()
	outputPath := filepath.Join(dir, "parallel.csv")
	manifestPath := checkpoint.DefaultManifestPath(outputPath)
	request := dukascopy.DownloadRequest{
		Symbol:      "xauusd",
		Granularity: dukascopy.GranularityM1,
		Side:        dukascopy.PriceSideBid,
		From:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		To:          time.Date(2024, 1, 2, 2, 0, 0, 0, time.UTC),
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	client := dukascopy.NewClient(server.URL, time.Second)
	if err := runPartitionedDownload(
		context.Background(),
		client,
		&stdout,
		&stderr,
		outputPath,
		manifestPath,
		request,
		dukascopy.ResultKindBar,
		[]string{"timestamp", "open"},
		nil,
		partitionHour,
		2,
	); err != nil {
		t.Fatalf("runPartitionedDownload parallel returned error: %v", err)
	}
	if !strings.Contains(stdout.String(), "wrote") {
		t.Fatalf("unexpected partition stdout: %s", stdout.String())
	}

	manifest, err := checkpoint.Load(manifestPath)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if len(manifest.Parts) != 2 || !manifest.Completed {
		t.Fatalf("unexpected manifest after partition download: %+v", manifest)
	}

	manifest.FinalOutput.SHA256 = "mismatch"
	if err := checkpoint.Save(manifestPath, manifest); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	stdout.Reset()
	stderr.Reset()
	if err := runPartitionedDownload(
		context.Background(),
		client,
		&stdout,
		&stderr,
		outputPath,
		manifestPath,
		request,
		dukascopy.ResultKindBar,
		[]string{"timestamp", "open"},
		nil,
		partitionHour,
		2,
	); err != nil {
		t.Fatalf("runPartitionedDownload reassemble returned error: %v", err)
	}
	if !strings.Contains(stderr.String(), "re-assembling") {
		t.Fatalf("expected reassemble message, got: %s", stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	if err := runPartitionedDownload(
		context.Background(),
		client,
		&stdout,
		&stderr,
		outputPath,
		manifestPath,
		request,
		dukascopy.ResultKindBar,
		[]string{"timestamp", "open"},
		nil,
		partitionHour,
		2,
	); err != nil {
		t.Fatalf("runPartitionedDownload reuse returned error: %v", err)
	}
	if !strings.Contains(stderr.String(), "final output verified") {
		t.Fatalf("expected verified output reuse message, got: %s", stderr.String())
	}
}

func TestApplyPartitionResultFailureAndMissingPartBranches(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "dataset.manifest.json")
	manifest := checkpoint.Manifest{
		Version:    checkpoint.CurrentManifestVersion,
		OutputPath: filepath.Join(dir, "dataset.csv"),
		PartsDir:   filepath.Join(dir, "parts"),
		Symbol:     "xauusd",
		Timeframe:  "m1",
		Side:       "BID",
		ResultKind: "bar",
		Columns:    []string{"timestamp", "open"},
		Partition:  partitionHour,
		Parts: []checkpoint.ManifestPart{{
			ID:   "known",
			File: "known.csv",
		}},
	}
	if err := checkpoint.Save(manifestPath, manifest); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	err := applyPartitionResult(manifestPath, &manifest, partitionWorkResult{
		Item: partitionWorkItem{Partition: downloadPartition{ID: "known"}},
		Err:  context.Canceled,
	})
	if err != nil {
		t.Fatalf("applyPartitionResult failure branch returned error: %v", err)
	}
	if manifest.Parts[0].Status != "failed" || manifest.Parts[0].Error == "" {
		t.Fatalf("expected failed partition status, got %+v", manifest.Parts[0])
	}

	err = applyPartitionResult(manifestPath, &manifest, partitionWorkResult{
		Item: partitionWorkItem{Partition: downloadPartition{ID: "missing"}},
	})
	if err == nil {
		t.Fatal("expected missing partition error")
	}
}
