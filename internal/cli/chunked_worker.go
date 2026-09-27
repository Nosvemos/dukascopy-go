package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/csvout"
	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
	"github.com/Nosvemos/dukascopy-go/pkg/features"
)

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
	barType dukascopy.BarType,
	barSize float64,
	outlierCfg dukascopy.OutlierConfig,
	featureSpecs []features.FeatureSpec,
) partitionWorkResult {
	partPath := filepath.Join(targetCacheDir, item.Partition.File)
	tempPath := partPath + ".part"

	partRequest := request
	partRequest.From = item.Partition.Start
	partRequest.To = item.Partition.End

	cfg := csvout.DefaultConfig()

	var rowsWritten int
	var err error

	if barType != "" && barType != dukascopy.BarTypeTime {
		// Sample custom bars from tick stream
		tickReq := partRequest
		tickReq.Granularity = dukascopy.GranularityTick
		var result dukascopy.DownloadResult
		result, err = client.Download(ctx, tickReq)
		if err == nil {
			ticks := result.Ticks
			if outlierCfg.Enabled {
				ticks = dukascopy.CleanTickOutliers(ticks, outlierCfg.Window, outlierCfg.Threshold)
			}
			var bars []dukascopy.Bar
			bars, err = dukascopy.SampleTicksToCustomBars(ticks, barType, barSize, partRequest.Side)
			if err == nil {
				if len(featureSpecs) > 0 {
					featNames, featRows, featErr := features.ComputeBarFeatures(bars, featureSpecs)
					if featErr == nil {
						err = cfg.WriteBarsWithFeaturesAtomic(tempPath, result.Instrument, barColumns, bars, featNames, featRows)
					} else {
						err = cfg.WriteBarsAtomic(tempPath, result.Instrument, barColumns, bars, nil, nil)
					}
				} else {
					err = cfg.WriteBarsAtomic(tempPath, result.Instrument, barColumns, bars, nil, nil)
				}
				rowsWritten = len(bars)
			}
		}
	} else if resultKind == dukascopy.ResultKindTick {
		var result dukascopy.DownloadResult
		result, err = client.Download(ctx, partRequest)
		if err == nil {
			ticks := result.Ticks
			if outlierCfg.Enabled {
				ticks = dukascopy.CleanTickOutliers(ticks, outlierCfg.Window, outlierCfg.Threshold)
			}
			err = cfg.WriteTicksAtomic(tempPath, result.Instrument, tickColumns, ticks)
			rowsWritten = len(ticks)
		}
	} else if csvout.BarColumnsNeedBidAsk(barColumns) {
		var instrument dukascopy.Instrument
		var bidBars, askBars []dukascopy.Bar
		var fallbackTicks []dukascopy.Tick
		var usedTickFallback bool
		instrument, bidBars, askBars, fallbackTicks, usedTickFallback, err = loadBidAskBarsWithTicks(ctx, client, partRequest)
		if err == nil {
			err = applyTickCountsForColumns(ctx, client, partRequest, barColumns, fallbackTicks, usedTickFallback, bidBars, askBars)
		}
		if err == nil {
			if outlierCfg.Enabled {
				bidBars = dukascopy.CleanBarOutliers(bidBars, outlierCfg.Window, outlierCfg.Threshold)
				askBars = dukascopy.CleanBarOutliers(askBars, outlierCfg.Window, outlierCfg.Threshold)
			}
			err = cfg.WriteBarsAtomic(tempPath, instrument, barColumns, nil, bidBars, askBars)
			rowsWritten = len(bidBars)
		}
	} else {
		var result dukascopy.DownloadResult
		result, err = client.Download(ctx, partRequest)
		if err == nil {
			bars := result.Bars
			err = applyTickCountsForColumns(ctx, client, partRequest, barColumns, nil, false, bars)
			if err == nil {
				if outlierCfg.Enabled {
					bars = dukascopy.CleanBarOutliers(bars, outlierCfg.Window, outlierCfg.Threshold)
				}
				err = cfg.WriteBarsAtomic(tempPath, result.Instrument, barColumns, bars, nil, nil)
				rowsWritten = len(bars)
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

func downloadTickCounts(ctx context.Context, client *dukascopy.Client, request dukascopy.DownloadRequest) (map[time.Time]int64, error) {
	tickRequest := request
	tickRequest.Granularity = dukascopy.GranularityTick
	result, err := client.Download(ctx, tickRequest)
	if err != nil {
		return nil, fmt.Errorf("download ticks for --tick-count: %w", err)
	}
	return dukascopy.CountTicksByInterval(result.Ticks, request.Granularity)
}

func applyTickCountsForColumns(ctx context.Context, client *dukascopy.Client, request dukascopy.DownloadRequest, columns []string, ticks []dukascopy.Tick, ticksAvailable bool, barSets ...[]dukascopy.Bar) error {
	if !containsString(columns, "tick_count") {
		return nil
	}
	var counts map[time.Time]int64
	var err error
	if ticksAvailable {
		counts, err = dukascopy.CountTicksByInterval(ticks, request.Granularity)
	} else {
		counts, err = downloadTickCounts(ctx, client, request)
	}
	if err != nil {
		return err
	}
	for _, bars := range barSets {
		applyTickCounts(bars, counts)
	}
	return nil
}

func applyTickCounts(bars []dukascopy.Bar, counts map[time.Time]int64) {
	for index := range bars {
		bars[index].TickCount = counts[bars[index].Time.UTC()]
	}
}
