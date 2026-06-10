package cli

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/csvout"
	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
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

type partitionWorkItem struct {
	Index     int
	Partition downloadPartition
}

type partitionWorkResult struct {
	Item        partitionWorkItem
	Worker      int
	RowsWritten int
	Audit       csvout.FileAudit
	Err         error
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
