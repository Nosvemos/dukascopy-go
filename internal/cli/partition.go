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

	"github.com/Nosvemos/dukascopy-data-downloader/internal/checkpoint"
	"github.com/Nosvemos/dukascopy-data-downloader/internal/csvout"
	"github.com/Nosvemos/dukascopy-data-downloader/internal/dukascopy"
)

const (
	partitionNone  = "none"
	partitionAuto  = "auto"
	partitionHour  = "hour"
	partitionDay   = "day"
	partitionWeek  = "week"
	partitionMonth = "month"
	partitionYear  = "year"
)

type downloadPartition struct {
	ID    string
	Start time.Time
	End   time.Time
	File  string
}

func normalizePartition(value string, granularity dukascopy.Granularity) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(value))
	if normalized == "" || normalized == partitionNone {
		return partitionNone, nil
	}
	if normalized == partitionAuto {
		switch granularity {
		case dukascopy.GranularityTick:
			return partitionHour, nil
		case dukascopy.GranularityM1, dukascopy.GranularityM3, dukascopy.GranularityM5, dukascopy.GranularityM15, dukascopy.GranularityM30:
			return partitionDay, nil
		case dukascopy.GranularityH1, dukascopy.GranularityH4, dukascopy.GranularityMN1:
			return partitionMonth, nil
		case dukascopy.GranularityD1:
			return partitionYear, nil
		case dukascopy.GranularityW1:
			return partitionWeek, nil
		default:
			return "", fmt.Errorf("unsupported auto partition mode for timeframe %q", granularity)
		}
	}

	switch normalized {
	case partitionHour, partitionDay, partitionWeek, partitionMonth, partitionYear:
		return normalized, nil
	default:
		return "", fmt.Errorf("unsupported --partition value %q", value)
	}
}

func buildPartitions(from time.Time, to time.Time, mode string) ([]downloadPartition, error) {
	if !from.Before(to) {
		return nil, errors.New("partition range must be non-empty")
	}

	current := from.UTC()
	to = to.UTC()
	partitions := make([]downloadPartition, 0)
	for current.Before(to) {
		next, err := nextPartitionBoundary(current, mode)
		if err != nil {
			return nil, err
		}
		if !next.After(current) {
			return nil, fmt.Errorf("partition mode %q produced a non-increasing boundary at %s", mode, current.Format(time.RFC3339))
		}

		end := next
		if end.After(to) {
			end = to
		}
		partitions = append(partitions, downloadPartition{
			ID:    partitionID(current, end),
			Start: current,
			End:   end,
			File:  partitionFileName(current, end),
		})
		current = end
	}

	return partitions, nil
}

func nextPartitionBoundary(value time.Time, mode string) (time.Time, error) {
	value = value.UTC()
	switch mode {
	case partitionHour:
		return value.Truncate(time.Hour).Add(time.Hour), nil
	case partitionDay:
		start := time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC)
		return start.AddDate(0, 0, 1), nil
	case partitionWeek:
		return weekStartForPartition(value).AddDate(0, 0, 7), nil
	case partitionMonth:
		return time.Date(value.Year(), value.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, 1, 0), nil
	case partitionYear:
		return time.Date(value.Year()+1, 1, 1, 0, 0, 0, 0, time.UTC), nil
	default:
		return time.Time{}, fmt.Errorf("unsupported partition mode %q", mode)
	}
}

func weekStartForPartition(value time.Time) time.Time {
	value = time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC)
	offset := (int(value.Weekday()) + 6) % 7
	return value.AddDate(0, 0, -offset)
}

func partitionID(start time.Time, end time.Time) string {
	return partitionStamp(start) + "_" + partitionStamp(end)
}

func partitionFileName(start time.Time, end time.Time) string {
	return partitionID(start, end) + ".csv"
}

func partitionStamp(value time.Time) string {
	return value.UTC().Format("20060102T150405Z")
}

func runPartitionedDownload(
	ctx context.Context,
	client *dukascopy.Client,
	stdout io.Writer,
	stderr io.Writer,
	outputPath string,
	manifestPath string,
	request dukascopy.DownloadRequest,
	resultKind dukascopy.ResultKind,
	barColumns []string,
	tickColumns []string,
	partitionMode string,
) error {
	columns := barColumns
	if resultKind == dukascopy.ResultKindTick {
		columns = tickColumns
	}

	partsDir := checkpoint.DefaultPartsDir(outputPath)
	partitions, err := buildPartitions(request.From, request.To, partitionMode)
	if err != nil {
		return err
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
	existing, err := checkpoint.Load(manifestPath)
	if err == nil {
		if err := checkpoint.ValidateCompatibility(existing, expected); err != nil {
			return err
		}
		manifest = existing
	} else if !os.IsNotExist(err) {
		return err
	}

	if err := os.MkdirAll(partsDir, 0o755); err != nil {
		return err
	}
	if err := checkpoint.Save(manifestPath, manifest); err != nil {
		return err
	}

	totalRows := 0
	for index, part := range partitions {
		partState := checkpoint.FindPart(&manifest, part.ID)
		if partState == nil {
			return fmt.Errorf("partition %s is missing from checkpoint manifest", part.ID)
		}

		partPath := filepath.Join(partsDir, part.File)
		if partState.Status == "completed" {
			if _, err := os.Stat(partPath); err == nil {
				audit, auditErr := csvout.AuditCSV(partPath)
				if auditErr == nil && partAuditMatches(*partState, audit) {
					totalRows += audit.Rows
					if stderr != nil {
						fmt.Fprintf(stderr, "%scheckpoint%s [%d/%d] reuse %s\n", colorize(colorYellow), colorize(colorReset), index+1, len(partitions), part.ID)
					}
					if partState.SHA256 == "" || partState.Bytes == 0 {
						partState.Rows = audit.Rows
						partState.Bytes = audit.Bytes
						partState.SHA256 = audit.SHA256
						partState.UpdatedAt = time.Now().UTC()
						if err := checkpoint.Save(manifestPath, manifest); err != nil {
							return err
						}
					}
					continue
				}

				if stderr != nil {
					fmt.Fprintf(stderr, "%saudit%s [%d/%d] re-download %s\n", colorize(colorYellow), colorize(colorReset), index+1, len(partitions), part.ID)
				}
			}
		}

		if stderr != nil {
			fmt.Fprintf(stderr, "%spartition%s [%d/%d] %s -> %s\n", colorize(colorCyan), colorize(colorReset), index+1, len(partitions), part.Start.Format(time.RFC3339), part.End.Format(time.RFC3339))
		}

		partState.Status = "running"
		partState.Error = ""
		partState.UpdatedAt = time.Now().UTC()
		if err := checkpoint.Save(manifestPath, manifest); err != nil {
			return err
		}

		partRequest := request
		partRequest.From = part.Start
		partRequest.To = part.End

		rowsWritten, err := downloadPartitionToFile(ctx, client, partPath, partRequest, resultKind, barColumns, tickColumns)
		if err != nil {
			partState.Status = "failed"
			partState.Error = err.Error()
			partState.UpdatedAt = time.Now().UTC()
			_ = checkpoint.Save(manifestPath, manifest)
			return err
		}

		audit, err := csvout.AuditCSV(partPath)
		if err != nil {
			partState.Status = "failed"
			partState.Error = err.Error()
			partState.UpdatedAt = time.Now().UTC()
			_ = checkpoint.Save(manifestPath, manifest)
			return err
		}
		if audit.Rows != rowsWritten {
			err = fmt.Errorf("partition %s row audit mismatch: wrote %d rows but file contains %d", part.ID, rowsWritten, audit.Rows)
			partState.Status = "failed"
			partState.Error = err.Error()
			partState.UpdatedAt = time.Now().UTC()
			_ = checkpoint.Save(manifestPath, manifest)
			return err
		}

		partState.Status = "completed"
		partState.Rows = audit.Rows
		partState.Bytes = audit.Bytes
		partState.SHA256 = audit.SHA256
		partState.Error = ""
		partState.UpdatedAt = time.Now().UTC()
		totalRows += audit.Rows
		if err := checkpoint.Save(manifestPath, manifest); err != nil {
			return err
		}
	}

	partPaths := make([]string, 0, len(manifest.Parts))
	for _, part := range manifest.Parts {
		if part.Status != "completed" {
			return fmt.Errorf("cannot assemble final CSV because partition %s is not completed", part.ID)
		}
		partPaths = append(partPaths, filepath.Join(partsDir, part.File))
	}

	if manifest.Completed && manifest.FinalOutput != nil {
		outputAudit, auditErr := csvout.AuditCSV(outputPath)
		if auditErr == nil && outputAuditMatches(*manifest.FinalOutput, outputAudit) {
			if stderr != nil {
				fmt.Fprintf(stderr, "%scheckpoint%s final output verified %s\n", colorize(colorYellow), colorize(colorReset), outputPath)
			}
			label := "bars"
			if resultKind == dukascopy.ResultKindTick {
				label = "ticks"
			}
			fmt.Fprintf(stdout, "%swrote%s %d %s to %s\n", colorize(colorGreen), colorize(colorReset), manifest.Summary.TotalRows, label, outputPath)
			return nil
		}
		if stderr != nil {
			fmt.Fprintf(stderr, "%saudit%s final output mismatch, re-assembling %s\n", colorize(colorYellow), colorize(colorReset), outputPath)
		}
	}

	if stderr != nil {
		fmt.Fprintf(stderr, "%sassemble%s %d partition files into %s\n", colorize(colorCyan), colorize(colorReset), len(partPaths), outputPath)
	}
	if err := csvout.AssembleCSVFromParts(outputPath, partPaths, request.From, request.To); err != nil {
		return err
	}

	outputAudit, err := csvout.AuditCSV(outputPath)
	if err != nil {
		return err
	}

	manifest.Completed = true
	manifest.FinalOutput = &checkpoint.ManifestOutput{
		Rows:      outputAudit.Rows,
		Bytes:     outputAudit.Bytes,
		SHA256:    outputAudit.SHA256,
		UpdatedAt: time.Now().UTC(),
	}
	if err := checkpoint.Save(manifestPath, manifest); err != nil {
		return err
	}

	label := "bars"
	if resultKind == dukascopy.ResultKindTick {
		label = "ticks"
	}
	fmt.Fprintf(stdout, "%swrote%s %d %s to %s\n", colorize(colorGreen), colorize(colorReset), outputAudit.Rows, label, outputPath)
	return nil
}

func downloadPartitionToFile(
	ctx context.Context,
	client *dukascopy.Client,
	partPath string,
	request dukascopy.DownloadRequest,
	resultKind dukascopy.ResultKind,
	barColumns []string,
	tickColumns []string,
) (int, error) {
	result, err := client.Download(ctx, request)
	if err != nil {
		return 0, err
	}

	if resultKind == dukascopy.ResultKindTick {
		if err := csvout.WriteTicksAtomic(partPath, result.Instrument, tickColumns, result.Ticks); err != nil {
			return 0, err
		}
		return len(result.Ticks), nil
	}

	if csvout.BarColumnsNeedBidAsk(barColumns) {
		instrument, bidBars, askBars, err := loadBidAskBars(ctx, client, request)
		if err != nil {
			return 0, err
		}
		if err := csvout.WriteBarsAtomic(partPath, instrument, barColumns, nil, bidBars, askBars); err != nil {
			return 0, err
		}
		return len(bidBars), nil
	}

	if err := csvout.WriteBarsAtomic(partPath, result.Instrument, barColumns, result.Bars, nil, nil); err != nil {
		return 0, err
	}
	return len(result.Bars), nil
}

func cloneStrings(values []string) []string {
	cloned := make([]string, len(values))
	copy(cloned, values)
	return cloned
}

func partAuditMatches(part checkpoint.ManifestPart, audit csvout.FileAudit) bool {
	if part.Rows != audit.Rows {
		return false
	}
	if part.SHA256 != "" && part.SHA256 != audit.SHA256 {
		return false
	}
	if part.Bytes != 0 && part.Bytes != audit.Bytes {
		return false
	}
	return true
}

func outputAuditMatches(output checkpoint.ManifestOutput, audit csvout.FileAudit) bool {
	if output.Rows != audit.Rows {
		return false
	}
	if output.Bytes != 0 && output.Bytes != audit.Bytes {
		return false
	}
	if output.SHA256 != "" && output.SHA256 != audit.SHA256 {
		return false
	}
	return true
}
