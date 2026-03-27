package cli

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/checkpoint"
	"github.com/Nosvemos/dukascopy-go/internal/csvout"
	"github.com/Nosvemos/dukascopy-go/internal/dukascopy"
)

type gapRedownloadOptions struct {
	BaseURL      string
	Retries      int
	RetryBackoff time.Duration
	RateLimit    time.Duration
}

func redownloadManifestGaps(manifestPath string, manifest *checkpoint.Manifest, stdout io.Writer, options gapRedownloadOptions) (int, error) {
	stats, err := csvout.InspectCSV(manifest.OutputPath)
	if err != nil {
		return 0, err
	}
	if stats.GapCount == 0 {
		return 0, nil
	}
	if stats.OutOfOrderRows > 0 || stats.DuplicateStamps > 0 {
		return 0, fmt.Errorf("cannot redownload gaps while dataset has duplicate/out-of-order timestamps")
	}
	expectedInterval, err := time.ParseDuration(stats.ExpectedInterval)
	if err != nil || expectedInterval <= 0 {
		return 0, fmt.Errorf("could not infer expected interval for %s", manifest.OutputPath)
	}

	partIndexes, err := detectManifestGapPartIndexes(*manifest, expectedInterval)
	if err != nil {
		return 0, err
	}
	if len(partIndexes) == 0 {
		return 0, nil
	}

	client := dukascopy.NewClient(options.BaseURL, defaultHTTPTimeout).
		WithRetries(options.Retries).
		WithBackoff(options.RetryBackoff).
		WithRateLimit(options.RateLimit)

	request, resultKind, barColumns, tickColumns, err := manifestDownloadParameters(*manifest)
	if err != nil {
		return 0, err
	}

	for _, index := range partIndexes {
		part := manifest.Parts[index]
		fmt.Fprintf(stdout, "%srepair%s re-download gap part %s\n", colorize(colorYellow), colorize(colorReset), part.ID)
		result := runPartitionJob(
			context.Background(),
			client,
			manifest.PartsDir,
			1,
			partitionWorkItem{
				Index: index,
				Partition: downloadPartition{
					ID:    part.ID,
					Start: part.Start,
					End:   part.End,
					File:  part.File,
				},
			},
			request,
			resultKind,
			barColumns,
			tickColumns,
		)
		if err := applyPartitionResult(manifestPath, manifest, result); err != nil {
			return 0, err
		}
		if result.Err != nil {
			return 0, result.Err
		}
	}

	if err := rebuildManifestFinalOutput(manifestPath, manifest); err != nil {
		return 0, err
	}

	return len(partIndexes), nil
}

func detectManifestGapPartIndexes(manifest checkpoint.Manifest, expectedInterval time.Duration) ([]int, error) {
	if expectedInterval <= 0 {
		return nil, fmt.Errorf("expected interval must be greater than 0")
	}

	indexes := make(map[int]struct{})
	var previousTimestamp time.Time
	previousPartIndex := -1

	for index, part := range manifest.Parts {
		partPath := filepath.Join(manifest.PartsDir, part.File)
		file, err := os.Open(partPath)
		if err != nil {
			return nil, err
		}

		reader := csv.NewReader(file)
		header, err := reader.Read()
		if err != nil {
			file.Close()
			return nil, err
		}
		timestampIndex := indexOfString(header, "timestamp")
		if timestampIndex < 0 {
			file.Close()
			return nil, fmt.Errorf("partition file %s does not contain a timestamp column", partPath)
		}

		for {
			record, readErr := reader.Read()
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				file.Close()
				return nil, readErr
			}
			if timestampIndex >= len(record) {
				file.Close()
				return nil, fmt.Errorf("partition file %s contains a malformed row", partPath)
			}

			timestamp, err := time.Parse(time.RFC3339Nano, record[timestampIndex])
			if err != nil {
				file.Close()
				return nil, fmt.Errorf("parse partition timestamp %q: %w", record[timestampIndex], err)
			}
			timestamp = timestamp.UTC()

			if !previousTimestamp.IsZero() {
				delta := timestamp.Sub(previousTimestamp)
				if delta > expectedInterval {
					if previousPartIndex >= 0 {
						indexes[previousPartIndex] = struct{}{}
					}
					indexes[index] = struct{}{}
				}
			}

			previousTimestamp = timestamp
			previousPartIndex = index
		}

		if err := file.Close(); err != nil {
			return nil, err
		}
	}

	if len(indexes) == 0 {
		return nil, nil
	}

	result := make([]int, 0, len(indexes))
	for index := range indexes {
		result = append(result, index)
	}
	sort.Ints(result)
	return result, nil
}

func manifestDownloadParameters(manifest checkpoint.Manifest) (dukascopy.DownloadRequest, dukascopy.ResultKind, []string, []string, error) {
	request := dukascopy.DownloadRequest{
		Symbol:      manifest.Symbol,
		Granularity: dukascopy.Granularity(manifest.Timeframe),
		Side:        dukascopy.PriceSide(manifest.Side),
	}

	resultKind := dukascopy.ResultKind(manifest.ResultKind)
	switch resultKind {
	case dukascopy.ResultKindBar:
		return request, resultKind, cloneStrings(manifest.Columns), nil, nil
	case dukascopy.ResultKindTick:
		return request, resultKind, nil, cloneStrings(manifest.Columns), nil
	default:
		return dukascopy.DownloadRequest{}, "", nil, nil, fmt.Errorf("unsupported manifest result kind %q", manifest.ResultKind)
	}
}

func rebuildManifestFinalOutput(manifestPath string, manifest *checkpoint.Manifest) error {
	partPaths := make([]string, 0, len(manifest.Parts))
	for _, part := range manifest.Parts {
		partPaths = append(partPaths, filepath.Join(manifest.PartsDir, part.File))
	}

	from, to, err := manifestRange(*manifest)
	if err != nil {
		return err
	}
	if err := csvout.AssembleCSVFromParts(manifest.OutputPath, partPaths, from, to); err != nil {
		return err
	}
	audit, err := csvout.AuditCSV(manifest.OutputPath)
	if err != nil {
		return err
	}

	manifest.Completed = true
	manifest.FinalOutput = &checkpoint.ManifestOutput{
		Rows:      audit.Rows,
		Bytes:     audit.Bytes,
		SHA256:    audit.SHA256,
		UpdatedAt: time.Now().UTC(),
	}
	return checkpoint.Save(manifestPath, *manifest)
}

func indexOfString(values []string, target string) int {
	for index, value := range values {
		if value == target {
			return index
		}
	}
	return -1
}
