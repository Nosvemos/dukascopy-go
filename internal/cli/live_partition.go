package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/checkpoint"
	"github.com/Nosvemos/dukascopy-go/pkg/csvout"
	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

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
