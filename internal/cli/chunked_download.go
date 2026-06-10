package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/checkpoint"
	"github.com/Nosvemos/dukascopy-go/pkg/csvout"
	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

// runChunkedDownload orchestrates the low-memory download process.
// It generates chunk ranges, downloads them in parallel to a local cache,
// and then merges them sequentially (streaming) to the final output file(s).
func runChunkedDownload(
	ctx context.Context,
	client *dukascopy.Client,
	stdout io.Writer,
	stderr io.Writer,
	outputPath string,
	checkpointManifest string,
	request dukascopy.DownloadRequest,
	resultKind dukascopy.ResultKind,
	barColumns []string,
	tickColumns []string,
	partitionMode string,
	parallelism int,
	cacheDir string,
	keepCache bool,
	resumeState *csvout.ResumeState,
	dedupeRecord []string,
	hive bool,
) (int, error) {
	progress, _ := stderr.(*progressPrinter)

	// Determine cache directory and manifest path
	var manifestPath string
	var targetCacheDir string
	if partitionMode != partitionNone {
		manifestPath = checkpointManifest
		if manifestPath == "" {
			manifestPath = checkpoint.DefaultManifestPath(outputPath)
		}
		targetCacheDir = checkpoint.DefaultPartsDir(outputPath)
	} else {
		if cacheDir == "" {
			cacheDir = "./.dukascopy_cache"
		}
		symSafe := safeSymbolFilename(request.Symbol)
		tfSafe := strings.ToLower(string(request.Granularity))
		sideSafe := strings.ToLower(string(request.Side))
		targetCacheDir = filepath.Join(cacheDir, fmt.Sprintf("%s_%s_%s", symSafe, tfSafe, sideSafe))
	}

	if err := os.MkdirAll(targetCacheDir, 0o755); err != nil {
		return 0, fmt.Errorf("failed to create cache directory %s: %w", targetCacheDir, err)
	}

	// 1. Determine chunk size based on timeframe
	chunkMode := partitionDay
	switch dukascopy.NormalizeGranularity(request.Granularity) {
	case dukascopy.GranularityTick:
		chunkMode = partitionHour
	case dukascopy.GranularityM1, dukascopy.GranularityM3, dukascopy.GranularityM5, dukascopy.GranularityM15, dukascopy.GranularityM30:
		chunkMode = partitionDay
	case dukascopy.GranularityH1, dukascopy.GranularityH4:
		chunkMode = partitionMonth
	case dukascopy.GranularityD1, dukascopy.GranularityW1, dukascopy.GranularityMN1:
		chunkMode = partitionYear
	}

	// Generate all chunk boundaries
	chunks, err := buildPartitions(request.From, request.To, chunkMode)
	if err != nil {
		return 0, err
	}

	if len(chunks) == 0 {
		fmt.Fprintln(stdout, "No data range to download.")
		return 0, nil
	}

	// Build and prepare manifest if in partition mode
	var manifest checkpoint.Manifest
	if partitionMode != partitionNone {
		columns := barColumns
		if resultKind == dukascopy.ResultKindTick {
			columns = tickColumns
		}
		expected := checkpoint.Manifest{
			Version:    checkpoint.CurrentManifestVersion,
			OutputPath: outputPath,
			PartsDir:   targetCacheDir,
			Symbol:     strings.TrimSpace(request.Symbol),
			Timeframe:  string(request.Granularity),
			Side:       string(request.Side),
			ResultKind: string(resultKind),
			Columns:    cloneStrings(columns),
			Partition:  partitionMode,
			CreatedAt:  time.Now().UTC(),
			Parts:      make([]checkpoint.ManifestPart, 0, len(chunks)),
		}
		for _, part := range chunks {
			expected.Parts = append(expected.Parts, checkpoint.ManifestPart{
				ID:     part.ID,
				Start:  part.Start,
				End:    part.End,
				File:   part.File,
				Status: "pending",
			})
		}

		manifest = expected
		existing, err := checkpoint.Load(manifestPath)
		if err == nil {
			if err := checkpoint.ValidateCompatibility(existing, expected); err != nil {
				return 0, err
			}
			manifest = existing
		} else if !os.IsNotExist(err) {
			return 0, err
		}

		if err := checkpoint.Save(manifestPath, manifest); err != nil {
			return 0, err
		}
	}

	// Prepare pending items
	var pending []partitionWorkItem
	completedCount := 0
	var completedBytes int64
	var completedRows int

	if progress != nil {
		progress.SetStatus("scanning cache")
	}

	for index, part := range chunks {
		var partState *checkpoint.ManifestPart
		if partitionMode != partitionNone {
			partState = checkpoint.FindPart(&manifest, part.ID)
		}

		partPath := filepath.Join(targetCacheDir, part.File)
		if partState != nil && partState.Status == "completed" {
			if _, err := os.Stat(partPath); err == nil {
				audit, auditErr := csvout.AuditCSV(partPath)
				if auditErr == nil && partAuditMatches(*partState, audit) {
					completedCount++
					completedBytes += audit.Bytes
					completedRows += partState.Rows
					continue
				}
			}
		} else if partitionMode == partitionNone {
			if info, err := os.Stat(partPath); err == nil && info.Size() > 0 {
				completedCount++
				completedBytes += info.Size()
				audit, auditErr := csvout.AuditCSV(partPath)
				if auditErr == nil {
					completedRows += audit.Rows
				}
				continue
			}
		}

		pending = append(pending, partitionWorkItem{
			Index:     index,
			Partition: part,
		})
	}

	if progress != nil {
		progress.SetPartitionMetrics(len(chunks), completedCount, completedRows, completedBytes)
		progress.SetStatus("downloading")
	}

	// 2. Download pending chunks in parallel using worker pool
	if len(pending) > 0 {
		if partitionMode != partitionNone {
			manifest.Completed = false
			manifest.FinalOutput = nil
			if err := checkpoint.Save(manifestPath, manifest); err != nil {
				return 0, err
			}
		}

		if parallelism < 1 {
			parallelism = 1
		}
		if parallelism > len(pending) {
			parallelism = len(pending)
		}

		childCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		jobs := make(chan partitionWorkItem, len(pending))
		results := make(chan partitionWorkResult, len(pending))

		var wg sync.WaitGroup
		for workerIndex := 0; workerIndex < parallelism; workerIndex++ {
			workerID := workerIndex + 1
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for item := range jobs {
					if progress != nil {
						progress.PartitionStarted(workerID, item.Partition)
					}
					result := downloadChunk(childCtx, client, targetCacheDir, workerID, item, request, resultKind, barColumns, tickColumns)
					if progress != nil {
						progress.PartitionFinished(result)
					}
					select {
					case <-childCtx.Done():
						return
					case results <- result:
					}
				}
			}(workerID)
		}

		for _, item := range pending {
			jobs <- item
		}
		close(jobs)

		go func() {
			wg.Wait()
			close(results)
		}()

		var firstErr error
		for result := range results {
			if partitionMode != partitionNone {
				if err := applyPartitionResult(manifestPath, &manifest, result); err != nil && firstErr == nil {
					firstErr = err
					cancel()
					continue
				}
			}
			if result.Err != nil && firstErr == nil {
				firstErr = result.Err
				cancel() // cancel other workers on first failure
			}
		}

		if firstErr != nil {
			return 0, firstErr
		}
	}

	// 3. Final Merge & Partitioning Stage
	if progress != nil {
		progress.SetStatus("merging chunks")
	} else if outputPath != "-" {
		fmt.Fprintf(stderr, "Merging %d chunks...\n", len(chunks))
	}

	partPaths := make([]string, len(chunks))
	for i, part := range chunks {
		partPaths[i] = filepath.Join(targetCacheDir, part.File)
	}

	mergePath := outputPath
	isResume := resumeState != nil && resumeState.Exists
	if isResume {
		mergePath = outputPath + ".resume-tmp"
	}

	totalRows, err := mergeChunks(stdout, mergePath, partPaths, request.From, request.To, partitionMode, resultKind, barColumns, tickColumns, hive)
	if err != nil {
		if isResume {
			_ = os.Remove(mergePath)
		}
		return 0, fmt.Errorf("merge failed: %w", err)
	}

	if isResume {
		appendedRows, err := csvout.MergeResumeCSV(outputPath, mergePath, dedupeRecord)
		_ = os.Remove(mergePath)
		if err != nil {
			return 0, fmt.Errorf("resume merge failed: %w", err)
		}
		totalRows = appendedRows
	}

	// Save final output metadata to manifest if in partition mode
	if partitionMode != partitionNone {
		outputAudit, err := csvout.AuditCSV(outputPath)
		if err != nil {
			return 0, err
		}
		manifest.Completed = true
		manifest.FinalOutput = &checkpoint.ManifestOutput{
			Rows:      outputAudit.Rows,
			Bytes:     outputAudit.Bytes,
			SHA256:    outputAudit.SHA256,
			UpdatedAt: time.Now().UTC(),
		}
		if err := checkpoint.Save(manifestPath, manifest); err != nil {
			return 0, err
		}
	}

	// 4. Cleanup cache
	if !keepCache {
		if progress != nil {
			progress.SetStatus("cleaning up")
		}
		_ = os.RemoveAll(targetCacheDir)
		// Clean up parent cacheDir if empty
		if partitionMode == partitionNone {
			if entries, err := os.ReadDir(cacheDir); err == nil && len(entries) == 0 {
				_ = os.Remove(cacheDir)
			}
		}
	}

	if progress != nil {
		progress.SetStatus("completed")
	}

	return totalRows, nil
}
