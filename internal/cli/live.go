package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/checkpoint"
	"github.com/Nosvemos/dukascopy-go/internal/csvout"
	"github.com/Nosvemos/dukascopy-go/internal/dukascopy"
)

var (
	liveNow = func() time.Time {
		return time.Now().UTC()
	}
	liveSleep = sleepWithContext
)

func runLiveDownload(
	ctx context.Context,
	client *dukascopy.Client,
	stdout io.Writer,
	stderr io.Writer,
	outputPath string,
	storageOutputPath string,
	manifestPath string,
	request dukascopy.DownloadRequest,
	resultKind dukascopy.ResultKind,
	barColumns []string,
	tickColumns []string,
	partitionMode string,
	parallelism int,
	pollInterval time.Duration,
) error {
	progress, _ := stderr.(*progressPrinter)
	outputToStdout := strings.TrimSpace(outputPath) == "-"
	stdoutHeaderWritten := false
	label := "bars"
	if resultKind == dukascopy.ResultKindTick {
		label = "ticks"
	}

	for {
		if err := ctx.Err(); err != nil {
			return finishLiveDownload(stdout, progress, outputPath, err)
		}

		upperInclusive, err := liveUpperInclusive(request.Granularity, liveNow())
		if err != nil {
			return err
		}

		cycleRequest := request
		cycleRequest.To = inclusiveDownloadEnd(upperInclusive)

		if cycleRequest.From.Before(cycleRequest.To) {
			var appended int
			if partitionMode != partitionNone {
				if outputToStdout {
					appended, stdoutHeaderWritten, err = runLivePartitionStdoutCycle(
						ctx,
						client,
						stdout,
						stderr,
						stdoutHeaderWritten,
						storageOutputPath,
						manifestPath,
						cycleRequest,
						resultKind,
						barColumns,
						tickColumns,
						partitionMode,
						parallelism,
					)
				} else {
					appended, err = runLivePartitionCycle(
						ctx,
						client,
						stderr,
						storageOutputPath,
						manifestPath,
						cycleRequest,
						resultKind,
						barColumns,
						tickColumns,
						partitionMode,
						parallelism,
					)
				}
			} else if outputToStdout {
				if progress != nil {
					progress.SetStatus(fmt.Sprintf(
						"live stream %s -> %s",
						cycleRequest.From.UTC().Format(time.RFC3339),
						upperInclusive.UTC().Format(time.RFC3339),
					))
				}
				appended, stdoutHeaderWritten, err = runLiveStdoutCycle(
					ctx,
					client,
					stdout,
					stdoutHeaderWritten,
					cycleRequest,
					resultKind,
					barColumns,
					tickColumns,
				)
				if err == nil {
					request.From = cycleRequest.To
				}
			} else {
				resumeState, dedupeRecord, resumeErr := prepareResume(true, outputPath, resultKind, barColumns, tickColumns, &cycleRequest)
				if resumeErr != nil {
					return resumeErr
				}
				if progress != nil {
					progress.SetStatus(fmt.Sprintf(
						"live sync %s -> %s",
						cycleRequest.From.UTC().Format(time.RFC3339),
						upperInclusive.UTC().Format(time.RFC3339),
					))
				}

				appended, err = runSingleDownload(ctx, client, stdout, outputPath, false, resumeState, dedupeRecord, cycleRequest, resultKind, barColumns, tickColumns)
			}
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return finishLiveDownload(stdout, progress, outputPath, err)
				}
				return err
			}
			if appended > 0 && !outputToStdout {
				fmt.Fprintf(
					stdout,
					"%slive%s wrote %d %s through %s to %s\n",
					colorize(colorCyan),
					colorize(colorReset),
					appended,
					label,
					upperInclusive.UTC().Format(time.RFC3339),
					outputPath,
				)
			}
		} else if progress != nil {
			progress.SetStatus("live waiting for next completed interval")
		}

		if err := liveSleep(ctx, pollInterval); err != nil {
			return finishLiveDownload(stdout, progress, outputPath, err)
		}
	}
}

func finishLiveDownload(stdout io.Writer, progress *progressPrinter, outputPath string, err error) error {
	if progress != nil {
		progress.SetStatus("live stopped")
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	if strings.TrimSpace(outputPath) == "-" {
		return nil
	}
	fmt.Fprintf(stdout, "%slive%s stopped for %s\n", colorize(colorCyan), colorize(colorReset), outputPath)
	return nil
}

func runLiveStdoutCycle(
	ctx context.Context,
	client *dukascopy.Client,
	stdout io.Writer,
	headerWritten bool,
	request dukascopy.DownloadRequest,
	resultKind dukascopy.ResultKind,
	barColumns []string,
	tickColumns []string,
) (int, bool, error) {
	result, err := client.Download(ctx, request)
	if err != nil {
		return 0, headerWritten, err
	}

	includeHeader := !headerWritten
	if result.Kind == dukascopy.ResultKindTick {
		if includeHeader {
			if err := csvout.WriteTicksToWriter(stdout, result.Instrument, tickColumns, result.Ticks); err != nil {
				return 0, headerWritten, err
			}
		} else {
			if err := csvout.WriteTicksRowsToWriter(stdout, result.Instrument, tickColumns, result.Ticks); err != nil {
				return 0, headerWritten, err
			}
		}
		return len(result.Ticks), true, nil
	}

	if csvout.BarColumnsNeedBidAsk(barColumns) {
		instrument, bidBars, askBars, err := loadBidAskBars(ctx, client, request)
		if err != nil {
			return 0, headerWritten, err
		}
		if includeHeader {
			if err := csvout.WriteBarsToWriter(stdout, instrument, barColumns, nil, bidBars, askBars); err != nil {
				return 0, headerWritten, err
			}
		} else {
			if err := csvout.WriteBarsRowsToWriter(stdout, instrument, barColumns, nil, bidBars, askBars); err != nil {
				return 0, headerWritten, err
			}
		}
		return len(bidBars), true, nil
	}

	if includeHeader {
		if err := csvout.WriteBarsToWriter(stdout, result.Instrument, barColumns, result.Bars, nil, nil); err != nil {
			return 0, headerWritten, err
		}
	} else {
		if err := csvout.WriteBarsRowsToWriter(stdout, result.Instrument, barColumns, result.Bars, nil, nil); err != nil {
			return 0, headerWritten, err
		}
	}
	return len(result.Bars), true, nil
}

func sleepWithContext(ctx context.Context, wait time.Duration) error {
	if wait <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func liveUpperInclusive(granularity dukascopy.Granularity, now time.Time) (time.Time, error) {
	now = now.UTC()
	switch dukascopy.NormalizeGranularity(granularity) {
	case dukascopy.GranularityTick:
		return now, nil
	case dukascopy.GranularityM1:
		return now.Truncate(time.Minute).Add(-time.Minute), nil
	case dukascopy.GranularityM3:
		return now.Truncate(3 * time.Minute).Add(-3 * time.Minute), nil
	case dukascopy.GranularityM5:
		return now.Truncate(5 * time.Minute).Add(-5 * time.Minute), nil
	case dukascopy.GranularityM15:
		return now.Truncate(15 * time.Minute).Add(-15 * time.Minute), nil
	case dukascopy.GranularityM30:
		return now.Truncate(30 * time.Minute).Add(-30 * time.Minute), nil
	case dukascopy.GranularityH1:
		return now.Truncate(time.Hour).Add(-time.Hour), nil
	case dukascopy.GranularityH4:
		return now.Truncate(4 * time.Hour).Add(-4 * time.Hour), nil
	case dukascopy.GranularityD1:
		start := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		return start.AddDate(0, 0, -1), nil
	case dukascopy.GranularityW1:
		return weekStartForPartition(now).AddDate(0, 0, -7), nil
	case dukascopy.GranularityMN1:
		return time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, -1, 0), nil
	default:
		return time.Time{}, fmt.Errorf("unsupported --live timeframe %q", granularity)
	}
}

func runLivePartitionCycle(
	ctx context.Context,
	client *dukascopy.Client,
	stderr io.Writer,
	outputPath string,
	manifestPath string,
	request dukascopy.DownloadRequest,
	resultKind dukascopy.ResultKind,
	barColumns []string,
	tickColumns []string,
	partitionMode string,
	parallelism int,
) (int, error) {
	columns := barColumns
	if resultKind == dukascopy.ResultKindTick {
		columns = tickColumns
	}

	previousRows, err := reconcileLivePartitionManifest(outputPath, manifestPath, request, resultKind, columns, partitionMode)
	if err != nil {
		return 0, err
	}

	if err := runPartitionedDownload(
		ctx,
		client,
		io.Discard,
		stderr,
		outputPath,
		manifestPath,
		request,
		resultKind,
		barColumns,
		tickColumns,
		partitionMode,
		parallelism,
	); err != nil {
		return 0, err
	}

	manifest, err := checkpoint.Load(manifestPath)
	if err != nil {
		return 0, err
	}
	if manifest.FinalOutput == nil {
		return 0, nil
	}
	if manifest.FinalOutput.Rows <= previousRows {
		return 0, nil
	}
	return manifest.FinalOutput.Rows - previousRows, nil
}

func runLivePartitionStdoutCycle(
	ctx context.Context,
	client *dukascopy.Client,
	stdout io.Writer,
	stderr io.Writer,
	headerWritten bool,
	cacheOutputPath string,
	manifestPath string,
	request dukascopy.DownloadRequest,
	resultKind dukascopy.ResultKind,
	barColumns []string,
	tickColumns []string,
	partitionMode string,
	parallelism int,
) (int, bool, error) {
	if _, err := runLivePartitionCycle(
		ctx,
		client,
		stderr,
		cacheOutputPath,
		manifestPath,
		request,
		resultKind,
		barColumns,
		tickColumns,
		partitionMode,
		parallelism,
	); err != nil {
		return 0, headerWritten, err
	}

	manifest, err := checkpoint.Load(manifestPath)
	if err != nil {
		return 0, headerWritten, err
	}

	lastTimestamp := time.Time{}
	streamedRows := 0
	if manifest.LiveStream != nil {
		lastTimestamp = manifest.LiveStream.LastTimestamp.UTC()
		streamedRows = manifest.LiveStream.Rows
	}

	rows, streamedUntil, err := csvout.StreamCSVRowsAfter(cacheOutputPath, stdout, lastTimestamp, !headerWritten)
	if err != nil {
		return 0, headerWritten, err
	}
	if rows == 0 {
		return 0, headerWritten, nil
	}

	manifest.LiveStream = &checkpoint.LiveStream{
		Rows:          streamedRows + rows,
		LastTimestamp: streamedUntil.UTC(),
		UpdatedAt:     time.Now().UTC(),
	}
	if err := checkpoint.Save(manifestPath, manifest); err != nil {
		return 0, headerWritten, err
	}
	return rows, true, nil
}

func reconcileLivePartitionManifest(
	outputPath string,
	manifestPath string,
	request dukascopy.DownloadRequest,
	resultKind dukascopy.ResultKind,
	columns []string,
	partitionMode string,
) (int, error) {
	partsDir := checkpoint.DefaultPartsDir(outputPath)
	partitions, err := buildPartitions(request.From, request.To, partitionMode)
	if err != nil {
		return 0, err
	}

	expected := checkpoint.Manifest{
		Version:    checkpoint.CurrentManifestVersion,
		OutputPath: outputPath,
		PartsDir:   partsDir,
		Symbol:     strings.TrimSpace(request.Symbol),
		Timeframe:  string(request.Granularity),
		Side:       string(request.Side),
		ResultKind: string(resultKind),
		Columns:    cloneStrings(columns),
		Partition:  partitionMode,
		CreatedAt:  time.Now().UTC(),
		Parts:      make([]checkpoint.ManifestPart, 0, len(partitions)),
	}
	for _, part := range partitions {
		expected.Parts = append(expected.Parts, checkpoint.ManifestPart{
			ID:     part.ID,
			Start:  part.Start,
			End:    part.End,
			File:   part.File,
			Status: "pending",
		})
	}

	manifest := expected
	previousRows := 0
	existing, err := checkpoint.Load(manifestPath)
	if err == nil {
		if err := validateLiveManifestBase(existing, expected); err != nil {
			return 0, err
		}
		if existing.FinalOutput != nil {
			previousRows = existing.FinalOutput.Rows
		}
		var obsolete []checkpoint.ManifestPart
		manifest, obsolete, err = mergeLiveManifest(existing, expected)
		if err != nil {
			return 0, err
		}
		if err := pruneObsoleteLiveParts(expected.PartsDir, obsolete); err != nil {
			return 0, err
		}
	} else if !os.IsNotExist(err) {
		return 0, err
	}

	if err := os.MkdirAll(partsDir, 0o755); err != nil {
		return 0, err
	}
	if err := checkpoint.Save(manifestPath, manifest); err != nil {
		return 0, err
	}

	return previousRows, nil
}

func validateLiveManifestBase(existing checkpoint.Manifest, expected checkpoint.Manifest) error {
	switch {
	case existing.OutputPath != expected.OutputPath:
		return fmt.Errorf("checkpoint manifest output path %q does not match requested output %q", existing.OutputPath, expected.OutputPath)
	case existing.Symbol != expected.Symbol:
		return fmt.Errorf("checkpoint manifest symbol %q does not match requested symbol %q", existing.Symbol, expected.Symbol)
	case existing.Timeframe != expected.Timeframe:
		return fmt.Errorf("checkpoint manifest timeframe %q does not match requested timeframe %q", existing.Timeframe, expected.Timeframe)
	case existing.Side != expected.Side:
		return fmt.Errorf("checkpoint manifest side %q does not match requested side %q", existing.Side, expected.Side)
	case existing.ResultKind != expected.ResultKind:
		return fmt.Errorf("checkpoint manifest result kind %q does not match requested result kind %q", existing.ResultKind, expected.ResultKind)
	case existing.Partition != expected.Partition:
		return fmt.Errorf("checkpoint manifest partition %q does not match requested partition %q", existing.Partition, expected.Partition)
	case existing.PartsDir != expected.PartsDir:
		return fmt.Errorf("checkpoint manifest parts dir %q does not match requested parts dir %q", existing.PartsDir, expected.PartsDir)
	}

	if len(existing.Columns) != len(expected.Columns) {
		return fmt.Errorf("checkpoint manifest columns do not match the selected columns")
	}
	for index := range existing.Columns {
		if existing.Columns[index] != expected.Columns[index] {
			return fmt.Errorf("checkpoint manifest columns do not match the selected columns")
		}
	}

	return nil
}

func defaultLiveStdoutManifestPath(symbol string, granularity dukascopy.Granularity) string {
	sanitized := strings.ToLower(strings.TrimSpace(symbol))
	replacer := strings.NewReplacer("/", "-", "\\", "-", " ", "-", "_", "-", ".", "-", ":", "-")
	sanitized = replacer.Replace(sanitized)
	if sanitized == "" {
		sanitized = "stream"
	}
	return fmt.Sprintf("dukascopy-live-%s-%s.manifest.json", sanitized, strings.ToLower(strings.TrimSpace(string(granularity))))
}

func defaultLiveStdoutCachePath(manifestPath string) string {
	base := strings.TrimSuffix(manifestPath, filepath.Ext(manifestPath))
	return base + ".stream-cache.csv"
}

func mergeLiveManifest(existing checkpoint.Manifest, expected checkpoint.Manifest) (checkpoint.Manifest, []checkpoint.ManifestPart, error) {
	if len(expected.Parts) < len(existing.Parts) {
		for index := range expected.Parts {
			if !sameManifestPartIdentity(existing.Parts[index], expected.Parts[index]) {
				return checkpoint.Manifest{}, nil, fmt.Errorf("live checkpoint manifest partitions moved backwards")
			}
		}
		return checkpoint.Manifest{}, nil, fmt.Errorf("live checkpoint manifest partitions moved backwards")
	}

	prefixLen := 0
	for prefixLen < len(existing.Parts) && prefixLen < len(expected.Parts) {
		if !sameManifestPartIdentity(existing.Parts[prefixLen], expected.Parts[prefixLen]) {
			break
		}
		prefixLen++
	}

	merged := existing
	merged.Version = expected.Version
	merged.OutputPath = expected.OutputPath
	merged.PartsDir = expected.PartsDir
	merged.Symbol = expected.Symbol
	merged.Timeframe = expected.Timeframe
	merged.Side = expected.Side
	merged.ResultKind = expected.ResultKind
	merged.Columns = cloneStrings(expected.Columns)
	merged.Partition = expected.Partition
	if merged.CreatedAt.IsZero() {
		merged.CreatedAt = expected.CreatedAt
	}
	merged.Parts = append([]checkpoint.ManifestPart{}, existing.Parts[:prefixLen]...)
	for _, part := range expected.Parts[prefixLen:] {
		merged.Parts = append(merged.Parts, part)
	}
	obsolete := append([]checkpoint.ManifestPart{}, existing.Parts[prefixLen:]...)

	if prefixLen != len(existing.Parts) || len(existing.Parts) != len(expected.Parts) {
		merged.Completed = false
		merged.FinalOutput = nil
	}

	return merged, obsolete, nil
}

func sameManifestPartIdentity(left checkpoint.ManifestPart, right checkpoint.ManifestPart) bool {
	return left.ID == right.ID && left.File == right.File && left.Start.Equal(right.Start) && left.End.Equal(right.End)
}

func pruneObsoleteLiveParts(partsDir string, obsolete []checkpoint.ManifestPart) error {
	for _, part := range obsolete {
		partPath := filepath.Join(partsDir, part.File)
		if filepath.Dir(partPath) != filepath.Clean(partsDir) {
			return fmt.Errorf("refusing to prune live partition outside parts dir: %s", partPath)
		}
		if err := os.Remove(partPath); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func validateLiveOptions(
	live bool,
	outputPath string,
	partition string,
	checkpointManifest string,
	barColumns []string,
	tickColumns []string,
	resultKind dukascopy.ResultKind,
) error {
	if !live {
		return nil
	}

	outputPath = strings.TrimSpace(outputPath)
	if outputPath == "-" {
		if strings.TrimSpace(checkpointManifest) != "" && strings.TrimSpace(partition) == partitionNone {
			return errors.New("--live stdout requires --partition when used with --checkpoint-manifest")
		}
		return nil
	}

	lowerOutput := strings.ToLower(outputPath)
	if strings.HasSuffix(lowerOutput, ".parquet") && strings.TrimSpace(partition) == partitionNone {
		return errors.New("--live parquet output requires --partition or partition auto-selection")
	}
	if !strings.HasSuffix(lowerOutput, ".csv") && !strings.HasSuffix(lowerOutput, ".csv.gz") && !strings.HasSuffix(lowerOutput, ".parquet") {
		return errors.New("--live currently supports only .csv, .csv.gz, and .parquet output")
	}

	columns := barColumns
	if resultKind == dukascopy.ResultKindTick {
		columns = tickColumns
	}
	if !csvout.ColumnsContainTimestamp(columns) {
		return errors.New("--live requires the selected columns to include timestamp")
	}
	if strings.TrimSpace(checkpointManifest) != "" && strings.TrimSpace(partition) == partitionNone {
		return errors.New("--checkpoint-manifest requires --partition in --live mode")
	}

	return nil
}
