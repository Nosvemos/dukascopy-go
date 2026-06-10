package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/csvout"
	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

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
