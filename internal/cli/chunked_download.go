package cli

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
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

	totalRows, err := mergeChunks(stdout, mergePath, partPaths, request.From, request.To, partitionMode, resultKind, barColumns, tickColumns)
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

// downloadChunk downloads a single chunk and flushes it to a temporary part file,
// then renames it atomically to mark it completed.
func downloadChunk(
	ctx context.Context,
	client *dukascopy.Client,
	targetCacheDir string,
	worker int,
	item partitionWorkItem,
	request dukascopy.DownloadRequest,
	resultKind dukascopy.ResultKind,
	barColumns []string,
	tickColumns []string,
) partitionWorkResult {
	partPath := filepath.Join(targetCacheDir, item.Partition.File)
	tempPath := partPath + ".part"

	partRequest := request
	partRequest.From = item.Partition.Start
	partRequest.To = item.Partition.End

	cfg := csvout.DefaultConfig()

	var rowsWritten int
	var err error

	result, err := client.Download(ctx, partRequest)
	if err == nil {
		if resultKind == dukascopy.ResultKindTick {
			err = cfg.WriteTicksAtomic(tempPath, result.Instrument, tickColumns, result.Ticks)
			rowsWritten = len(result.Ticks)
		} else {
			if csvout.BarColumnsNeedBidAsk(barColumns) {
				var instrument dukascopy.Instrument
				var bidBars, askBars []dukascopy.Bar
				instrument, bidBars, askBars, err = loadBidAskBars(ctx, client, partRequest)
				if err == nil {
					err = cfg.WriteBarsAtomic(tempPath, instrument, barColumns, nil, bidBars, askBars)
					rowsWritten = len(bidBars)
				}
			} else {
				err = cfg.WriteBarsAtomic(tempPath, result.Instrument, barColumns, result.Bars, nil, nil)
				rowsWritten = len(result.Bars)
			}
		}
	}

	if err != nil {
		_ = os.Remove(tempPath)
		return partitionWorkResult{
			Item:   item,
			Worker: worker,
			Err:    err,
		}
	}

	// Atomic rename to finalize chunk file
	if err := os.Rename(tempPath, partPath); err != nil {
		_ = os.Remove(tempPath)
		return partitionWorkResult{
			Item:   item,
			Worker: worker,
			Err:    fmt.Errorf("failed to finalize chunk file: %w", err),
		}
	}

	audit, err := csvout.AuditCSV(partPath)
	if err != nil {
		return partitionWorkResult{
			Item:   item,
			Worker: worker,
			Err:    err,
		}
	}

	return partitionWorkResult{
		Item:        item,
		Worker:      worker,
		RowsWritten: rowsWritten,
		Audit:       audit,
	}
}

// mergeChunks streams all records from chunk files sequentially to the output path(s).
func mergeChunks(
	stdout io.Writer,
	outputPath string,
	partPaths []string,
	from time.Time,
	to time.Time,
	partitionMode string,
	resultKind dukascopy.ResultKind,
	barColumns []string,
	tickColumns []string,
) (int, error) {
	columns := barColumns
	if resultKind == dukascopy.ResultKindTick {
		columns = tickColumns
	}

	isParquet := strings.HasSuffix(strings.ToLower(outputPath), ".parquet")
	isGzip := strings.HasSuffix(strings.ToLower(outputPath), ".gz")

	// Determine if we need to route to partitioned output files
	isPartitionedOutput := partitionMode != "" && partitionMode != partitionNone

	var currentPartitionKey string

	var mainCsvFileWriter *os.File
	var mainGzipWriter *gzip.Writer
	var mainCsvWriter *csv.Writer
	var mainParquetWriter *csvout.ParquetStreamWriter

	var partCsvFileWriter *os.File
	var partGzipWriter *gzip.Writer
	var partCsvWriter *csv.Writer
	var partParquetWriter *csvout.ParquetStreamWriter

	closeMainWriter := func() error {
		var errs []string
		if mainParquetWriter != nil {
			if err := mainParquetWriter.Close(); err != nil {
				errs = append(errs, err.Error())
			}
			mainParquetWriter = nil
		}
		if mainCsvWriter != nil {
			mainCsvWriter.Flush()
			if err := mainCsvWriter.Error(); err != nil {
				errs = append(errs, err.Error())
			}
			mainCsvWriter = nil
		}
		if mainGzipWriter != nil {
			if err := mainGzipWriter.Close(); err != nil {
				errs = append(errs, err.Error())
			}
			mainGzipWriter = nil
		}
		if mainCsvFileWriter != nil {
			if err := mainCsvFileWriter.Close(); err != nil {
				errs = append(errs, err.Error())
			}
			mainCsvFileWriter = nil
		}
		if len(errs) > 0 {
			return errors.New(strings.Join(errs, "; "))
		}
		return nil
	}
	defer closeMainWriter()

	closePartWriter := func() error {
		var errs []string
		if partParquetWriter != nil {
			if err := partParquetWriter.Close(); err != nil {
				errs = append(errs, err.Error())
			}
			partParquetWriter = nil
		}
		if partCsvWriter != nil {
			partCsvWriter.Flush()
			if err := partCsvWriter.Error(); err != nil {
				errs = append(errs, err.Error())
			}
			partCsvWriter = nil
		}
		if partGzipWriter != nil {
			if err := partGzipWriter.Close(); err != nil {
				errs = append(errs, err.Error())
			}
			partGzipWriter = nil
		}
		if partCsvFileWriter != nil {
			if err := partCsvFileWriter.Close(); err != nil {
				errs = append(errs, err.Error())
			}
			partCsvFileWriter = nil
		}
		if len(errs) > 0 {
			return errors.New(strings.Join(errs, "; "))
		}
		return nil
	}
	defer closePartWriter()

	initMainWriter := func() error {
		targetPath := outputPath
		if isParquet {
			var err error
			mainParquetWriter, err = csvout.CreateParquetStreamWriter(targetPath, columns)
			if err != nil {
				return err
			}
		} else {
			var w io.Writer
			if targetPath == "-" {
				w = stdout
			} else {
				if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
					return err
				}
				var err error
				mainCsvFileWriter, err = os.Create(targetPath)
				if err != nil {
					return err
				}
				w = mainCsvFileWriter
			}
			if isGzip {
				mainGzipWriter = gzip.NewWriter(w)
				w = mainGzipWriter
			}
			mainCsvWriter = csv.NewWriter(w)
			mainCsvWriter.Comma = csvout.CSVDelimiter

			if !csvout.HideCSVHeader {
				if err := mainCsvWriter.Write(columns); err != nil {
					return err
				}
			}
		}
		return nil
	}

	initPartWriter := func(key string) error {
		if err := closePartWriter(); err != nil {
			return err
		}

		targetPath := getPartitionOutputPath(outputPath, key)
		if isParquet {
			var err error
			partParquetWriter, err = csvout.CreateParquetStreamWriter(targetPath, columns)
			if err != nil {
				return err
			}
		} else {
			if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
				return err
			}
			var err error
			partCsvFileWriter, err = os.Create(targetPath)
			if err != nil {
				return err
			}
			var w io.Writer = partCsvFileWriter
			if isGzip {
				partGzipWriter = gzip.NewWriter(w)
				w = partGzipWriter
			}
			partCsvWriter = csv.NewWriter(w)
			partCsvWriter.Comma = csvout.CSVDelimiter

			if !csvout.HideCSVHeader {
				if err := partCsvWriter.Write(columns); err != nil {
					return err
				}
			}
		}
		return nil
	}

	// Always initialize the main writer
	if err := initMainWriter(); err != nil {
		return 0, err
	}

	timestampIndex := indexOfColumn(columns, "timestamp")
	if timestampIndex < 0 {
		return 0, fmt.Errorf("missing timestamp column")
	}

	cfg := csvout.DefaultConfig()
	cfg.HideHeader = true

	parquetBatchSize := 50000
	mainParquetBatch := make([]map[string]any, 0, parquetBatchSize)
	partParquetBatch := make([]map[string]any, 0, parquetBatchSize)
	totalRowsWritten := 0

	for _, partPath := range partPaths {
		file, err := os.Open(partPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue // skip missing chunks (should not happen if download succeeded)
			}
			return 0, err
		}

		reader := csv.NewReader(file)
		reader.Comma = csvout.CSVDelimiter
		reader.FieldsPerRecord = -1 // flexible

		isHeader := true
		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				file.Close()
				return 0, err
			}
			if len(record) == 0 {
				continue
			}
			if isHeader {
				isHeader = false
				continue // skip header row in chunk file
			}

			// Parse timestamp to verify ranges and route partitions
			tsVal := record[timestampIndex]
			timestamp, err := time.Parse(time.RFC3339Nano, tsVal)
			if err != nil {
				// Fallback to other layouts
				timestamp, err = time.Parse(time.RFC3339, tsVal)
			}
			if err != nil {
				file.Close()
				return 0, fmt.Errorf("failed to parse timestamp %q: %w", tsVal, err)
			}

			timestamp = timestamp.UTC()
			if timestamp.Before(from) || !timestamp.Before(to) {
				continue
			}

			// Partition routing
			if isPartitionedOutput {
				key := getPartitionKey(timestamp, partitionMode)
				if key != currentPartitionKey {
					// Flush current parquet batch before switching partitions
					if isParquet && len(partParquetBatch) > 0 {
						if err := partParquetWriter.WriteBatch(partParquetBatch); err != nil {
							file.Close()
							return 0, err
						}
						partParquetBatch = partParquetBatch[:0]
					}

					currentPartitionKey = key
					if err := initPartWriter(key); err != nil {
						file.Close()
						return 0, err
					}
				}
			}

			// Write record
			if isParquet {
				row := make(map[string]any, len(columns))
				for index, colName := range columns {
					if index >= len(record) {
						file.Close()
						return 0, fmt.Errorf("malformed CSV row in chunk %s", partPath)
					}
					val, err := parquetValueForColumn(colName, record[index])
					if err != nil {
						file.Close()
						return 0, err
					}
					row[colName] = val
				}
				mainParquetBatch = append(mainParquetBatch, row)
				totalRowsWritten++
				if len(mainParquetBatch) >= parquetBatchSize {
					if err := mainParquetWriter.WriteBatch(mainParquetBatch); err != nil {
						file.Close()
						return 0, err
					}
					mainParquetBatch = mainParquetBatch[:0]
				}

				if isPartitionedOutput {
					partParquetBatch = append(partParquetBatch, row)
					if len(partParquetBatch) >= parquetBatchSize {
						if err := partParquetWriter.WriteBatch(partParquetBatch); err != nil {
							file.Close()
							return 0, err
						}
						partParquetBatch = partParquetBatch[:0]
					}
				}
			} else {
				if err := mainCsvWriter.Write(record); err != nil {
					file.Close()
					return 0, err
				}
				totalRowsWritten++

				if isPartitionedOutput {
					if err := partCsvWriter.Write(record); err != nil {
						file.Close()
						return 0, err
					}
				}
			}
		}

		file.Close()
	}

	// Flush remaining parquet records
	if isParquet {
		if len(mainParquetBatch) > 0 {
			if err := mainParquetWriter.WriteBatch(mainParquetBatch); err != nil {
				return 0, err
			}
		}
		if isPartitionedOutput && len(partParquetBatch) > 0 {
			if err := partParquetWriter.WriteBatch(partParquetBatch); err != nil {
				return 0, err
			}
		}
	}

	return totalRowsWritten, nil
}

func getPartitionKey(t time.Time, mode string) string {
	t = t.UTC()
	switch mode {
	case partitionHour:
		return t.Format("20060102T15")
	case partitionDay:
		return t.Format("20060102")
	case partitionWeek:
		year, week := t.ISOWeek()
		return fmt.Sprintf("%04dW%02d", year, week)
	case partitionMonth:
		return t.Format("200601")
	case partitionYear:
		return t.Format("2006")
	default:
		return ""
	}
}

func getPartitionOutputPath(outputPath string, key string) string {
	ext := filepath.Ext(outputPath)
	base := strings.TrimSuffix(outputPath, ext)
	if ext == ".gz" && strings.HasSuffix(strings.ToLower(base), ".csv") {
		ext = ".csv.gz"
		base = strings.TrimSuffix(base, ".csv")
	}
	return fmt.Sprintf("%s_%s%s", base, key, ext)
}

func parquetValueForColumn(column string, value string) (any, error) {
	if strings.EqualFold(strings.TrimSpace(column), "timestamp") {
		return value, nil
	}
	number, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return nil, fmt.Errorf("parse parquet numeric value for column %q: %w", column, err)
	}
	return number, nil
}

func indexOfColumn(columns []string, name string) int {
	for i, col := range columns {
		if strings.EqualFold(strings.TrimSpace(col), name) {
			return i
		}
	}
	return -1
}
