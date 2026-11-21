package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
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
	"github.com/Nosvemos/dukascopy-go/pkg/features"
)

// chunkedDownloadOptions configures runChunkedDownload.
type chunkedDownloadOptions struct {
	OutputPath         string
	CheckpointManifest string
	Request            dukascopy.DownloadRequest
	ResultKind         dukascopy.ResultKind
	BarColumns         []string
	TickColumns        []string
	PartitionMode      string
	Parallelism        int
	CacheDir           string
	KeepCache          bool
	ResumeState        *csvout.ResumeState
	DedupeRecord       []string
	Hive               bool
	BarType            dukascopy.BarType
	BarSize            float64
	OutlierCfg         dukascopy.OutlierConfig
	// OutlierSpec and Engine only feed the chunk cache key.
	OutlierSpec  string
	Engine       string
	FeatureSpecs []features.FeatureSpec
	// OutputConfig is the user's output format; nil means csvout.DefaultConfig().
	OutputConfig *csvout.Config
}

func (o chunkedDownloadOptions) customBars() bool {
	return o.BarType != "" && o.BarType != dukascopy.BarTypeTime
}

// outputColumns are the columns of the final output.
func (o chunkedDownloadOptions) outputColumns() []string {
	if o.ResultKind == dukascopy.ResultKindTick {
		return o.TickColumns
	}
	return o.BarColumns
}

// chunkColumns are the columns of the cached chunk files.
func (o chunkedDownloadOptions) chunkColumns() []string {
	if o.customBars() {
		return customBarTickColumns
	}
	return o.outputColumns()
}

// runChunkedDownload orchestrates the low-memory download process.
// It generates chunk ranges, downloads them in parallel to a local cache,
// and then merges them sequentially (streaming) to the final output file(s).
func runChunkedDownload(
	ctx context.Context,
	client *dukascopy.Client,
	stdout io.Writer,
	stderr io.Writer,
	opts chunkedDownloadOptions,
) (int, error) {
	progress, _ := stderr.(*progressPrinter)
	request := opts.Request
	outputPath := opts.OutputPath
	partitionMode := opts.PartitionMode
	chunkColumns := opts.chunkColumns()
	outCfg := opts.OutputConfig
	if outCfg == nil {
		outCfg = csvout.DefaultConfig()
	}

	cacheDir := opts.CacheDir
	if cacheDir == "" {
		cacheDir = "./.dukascopy_cache"
	}

	// Determine cache directory and manifest path
	var manifestPath string
	var targetCacheDir string
	if partitionMode != partitionNone {
		manifestPath = opts.CheckpointManifest
		if manifestPath == "" {
			manifestPath = checkpoint.DefaultManifestPath(outputPath)
		}
		targetCacheDir = checkpoint.DefaultPartsDir(outputPath)
	} else {
		symSafe := safeSymbolFilename(request.Symbol)
		tfSafe := strings.ToLower(string(request.Granularity))
		sideSafe := strings.ToLower(string(request.Side))
		key := chunkCacheKey(chunkCacheSettings{
			Symbol:        request.Symbol,
			Timeframe:     string(request.Granularity),
			Side:          string(request.Side),
			ResultKind:    string(opts.ResultKind),
			Columns:       chunkColumns,
			BarType:       string(opts.BarType),
			CleanOutliers: opts.OutlierSpec,
			Engine:        opts.Engine,
			FillGaps:      outCfg.FillGaps,
		})
		targetCacheDir = filepath.Join(cacheDir, fmt.Sprintf("%s_%s_%s_%s", symSafe, tfSafe, sideSafe, key))
	}

	if err := os.MkdirAll(targetCacheDir, 0o755); err != nil {
		return 0, fmt.Errorf("failed to create cache directory %s: %w", targetCacheDir, err)
	}

	chunks, err := buildChunkRanges(request.From, request.To, request.Granularity)
	if err != nil {
		return 0, err
	}
	if len(chunks) == 0 {
		fmt.Fprintln(stdout, "No data range to download.")
		return 0, nil
	}

	// Build and prepare manifest if in partition mode
	var manifest checkpoint.Manifest
	var saver *manifestSaver
	if partitionMode != partitionNone {
		expected := checkpoint.Manifest{
			Version:    checkpoint.CurrentManifestVersion,
			OutputPath: outputPath,
			PartsDir:   targetCacheDir,
			Symbol:     strings.TrimSpace(request.Symbol),
			Timeframe:  string(request.Granularity),
			Side:       string(request.Side),
			ResultKind: string(opts.ResultKind),
			Columns:    cloneStrings(opts.outputColumns()),
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

		saver = newManifestSaver(manifestPath, &manifest)
		if err := saver.saveNow(); err != nil {
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
		partPath := filepath.Join(targetCacheDir, part.File)
		if saver != nil {
			partState := saver.part(part.ID)
			if partState != nil && partState.Status == "completed" && chunkHeaderMatches(partPath, chunkColumns) {
				audit, auditErr := auditChunkFile(partPath)
				if auditErr == nil && partAuditMatches(*partState, audit) {
					completedCount++
					completedBytes += audit.Bytes
					completedRows += audit.Rows
					continue
				}
			}
		} else if info, err := os.Stat(partPath); err == nil && info.Size() > 0 && chunkHeaderMatches(partPath, chunkColumns) {
			if audit, auditErr := auditChunkFile(partPath); auditErr == nil {
				completedCount++
				completedBytes += audit.Bytes
				completedRows += audit.Rows
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

	var instrument dukascopy.Instrument
	haveInstrument := false

	// Download pending chunks in parallel using worker pool
	if len(pending) > 0 {
		if saver != nil {
			manifest.Completed = false
			manifest.FinalOutput = nil
			if err := saver.saveNow(); err != nil {
				return 0, err
			}
		}

		parallelism := opts.Parallelism
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
					if childCtx.Err() != nil {
						return
					}
					if progress != nil {
						progress.PartitionStarted(workerID, item.Partition)
					}
					result := downloadChunk(
						childCtx,
						client,
						targetCacheDir,
						workerID,
						item,
						request,
						opts.ResultKind,
						opts.BarColumns,
						opts.TickColumns,
						opts.BarType,
						opts.OutlierCfg,
					)
					if progress != nil {
						progress.PartitionFinished(result)
					}
					// results is buffered for every pending item, so this never blocks.
					results <- result
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
			if result.Err != nil && firstErr == nil {
				firstErr = result.Err
				cancel()
			}
			if result.Err == nil && !haveInstrument && result.Instrument.Code != "" {
				instrument = result.Instrument
				haveInstrument = true
			}
			if saver == nil {
				continue
			}
			manifestPart := saver.part(result.Item.Partition.ID)
			if manifestPart == nil {
				continue
			}
			if result.Err != nil {
				if errors.Is(result.Err, context.Canceled) {
					continue
				}
				manifestPart.Status = "failed"
				manifestPart.Rows = 0
				manifestPart.Bytes = 0
				manifestPart.SHA256 = ""
				manifestPart.Error = result.Err.Error()
			} else {
				manifestPart.Status = "completed"
				// Store the audited row count: resume compares it with a fresh
				// audit of the part file (partAuditMatches).
				manifestPart.Rows = result.Audit.Rows
				manifestPart.Bytes = result.Audit.Bytes
				manifestPart.SHA256 = result.Audit.SHA256
				manifestPart.Error = ""
			}
			manifestPart.UpdatedAt = time.Now().UTC()
			if err := saver.changed(); err != nil && firstErr == nil {
				firstErr = err
				cancel()
			}
		}

		if saver != nil {
			if err := saver.flush(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if firstErr != nil {
			return 0, firstErr
		}
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	// Assemble/Merge chunks
	if progress != nil {
		progress.SetStatus("merging chunks")
	}

	partPaths := make([]string, len(chunks))
	for i, part := range chunks {
		partPaths[i] = filepath.Join(targetCacheDir, part.File)
	}

	if (opts.customBars() || len(opts.FeatureSpecs) > 0) && !haveInstrument {
		instrument, err = resolveInstrument(ctx, client, request.Symbol)
		if err != nil {
			return 0, err
		}
	}

	newSource := func() (mergeSource, error) {
		if !opts.customBars() {
			return nil, nil
		}
		return newCustomBarSource(partPaths, request.From, request.To, opts.BarType, opts.BarSize, request.Side, instrument, opts.BarColumns)
	}

	mergeOpts := mergeOptions{
		outputPath:    outputPath,
		partPaths:     partPaths,
		columns:       opts.outputColumns(),
		from:          request.From,
		to:            request.To,
		partitionMode: partitionMode,
		hive:          opts.Hive,
		filterFrom:    opts.ResultKind == dukascopy.ResultKindTick,
		strictTimes:   opts.ResultKind == dukascopy.ResultKindBar && !opts.customBars(),
		outCfg:        outCfg,
	}

	var totalRows int
	if len(opts.FeatureSpecs) > 0 {
		totalRows, err = writeFeatureOutput(ctx, stdout, mergeOpts, newSource, targetCacheDir, instrument, opts.FeatureSpecs)
		if err != nil {
			return 0, err
		}
	} else {
		isResume := opts.ResumeState != nil && opts.ResumeState.Exists
		if isResume {
			// The resume temp is merged into the existing file by
			// MergeResumeCSV, which expects a header row.
			mergeOpts.outputPath = outputPath + ".resume-tmp"
			resumeCfg := *outCfg
			resumeCfg.HideHeader = false
			mergeOpts.outCfg = &resumeCfg
		}
		source, err := newSource()
		if err != nil {
			return 0, err
		}
		mergeOpts.source = source

		totalRows, err = mergeChunks(ctx, stdout, mergeOpts)
		if err != nil {
			if isResume {
				_ = os.Remove(mergeOpts.outputPath)
			}
			return 0, fmt.Errorf("merge failed: %w", err)
		}

		if isResume {
			appendedRows, err := csvout.MergeResumeCSV(outputPath, mergeOpts.outputPath, opts.DedupeRecord)
			_ = os.Remove(mergeOpts.outputPath)
			if err != nil {
				return 0, fmt.Errorf("resume merge failed: %w", err)
			}
			totalRows = appendedRows
		}
	}

	// Save final output metadata to manifest if in partition mode
	if saver != nil {
		outputAudit, err := auditFinalOutput(outputPath, totalRows)
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
		if err := saver.saveNow(); err != nil {
			return 0, err
		}
	}

	// Cleanup cache
	if !opts.KeepCache {
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

// writeFeatureOutput merges the chunks into an internal-format temp file,
// computes the requested features once over the whole continuous series (so
// indicator warm-up happens once, not per chunk) and writes the final output
// with the user's format. It returns the number of rows written.
func writeFeatureOutput(
	ctx context.Context,
	stdout io.Writer,
	mergeOpts mergeOptions,
	newSource func() (mergeSource, error),
	workDir string,
	instrument dukascopy.Instrument,
	specs []features.FeatureSpec,
) (int, error) {
	finalPath := mergeOpts.outputPath
	userCfg := mergeOpts.outCfg

	internalPath := filepath.Join(workDir, "features-input.csv")
	defer os.Remove(internalPath)

	source, err := newSource()
	if err != nil {
		return 0, err
	}
	mergeOpts.source = source
	mergeOpts.outputPath = internalPath
	mergeOpts.outCfg = chunkConfig()
	mergeOpts.partitionMode = partitionNone
	mergeOpts.hive = false
	if _, err := mergeChunks(ctx, stdout, mergeOpts); err != nil {
		return 0, fmt.Errorf("merge failed: %w", err)
	}

	bars, baseColumns, err := chunkConfig().ReadBarsFromCSV(internalPath)
	if err != nil {
		return 0, fmt.Errorf("read merged bars for --features: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	featureNames, featureRows, err := features.ComputeBarFeatures(bars, specs)
	if err != nil {
		return 0, fmt.Errorf("compute --features: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := userCfg.WriteBarsWithFeaturesAtomic(finalPath, instrument, baseColumns, bars, featureNames, featureRows); err != nil {
		return 0, fmt.Errorf("write --features output: %w", err)
	}
	return len(bars), nil
}

// resolveInstrument looks up symbol in the instrument catalog.
func resolveInstrument(ctx context.Context, client *dukascopy.Client, symbol string) (dukascopy.Instrument, error) {
	instruments, err := client.ListInstruments(ctx)
	if err != nil {
		return dukascopy.Instrument{}, err
	}
	return dukascopy.ResolveInstrument(instruments, symbol)
}

// auditFinalOutput audits a final output for the checkpoint manifest. CSV and
// Parquet files are audited by csvout; other formats are hashed and use the
// merged row count.
func auditFinalOutput(path string, rows int) (csvout.FileAudit, error) {
	switch outputKindForPath(path) {
	case outputCSV, outputParquet:
		return csvout.AuditCSV(path)
	}
	file, err := os.Open(path)
	if err != nil {
		return csvout.FileAudit{}, err
	}
	defer file.Close()
	hasher := sha256.New()
	size, err := io.Copy(hasher, file)
	if err != nil {
		return csvout.FileAudit{}, err
	}
	return csvout.FileAudit{Rows: rows, Bytes: size, SHA256: hex.EncodeToString(hasher.Sum(nil))}, nil
}
