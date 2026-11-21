package cli

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/csvout"
	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

// downloadChunk downloads a single chunk and writes it atomically, in the
// fixed internal chunk format (see chunkConfig), to its cache file.
//
// For --bar-type tick/volume/dollar the chunk holds the raw (optionally
// outlier-cleaned) ticks in customBarTickColumns; bars are sampled from the
// whole tick stream at merge time so no bar is cut at a chunk boundary.
// Features are never computed here: they are computed once over the merged
// series (see runChunkedDownload).
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
	outlierCfg dukascopy.OutlierConfig,
) partitionWorkResult {
	partPath := filepath.Join(targetCacheDir, item.Partition.File)

	partRequest := request
	partRequest.From = item.Partition.Start
	partRequest.To = item.Partition.End

	cfg := chunkConfig()

	var rowsWritten int
	var instrument dukascopy.Instrument
	var err error

	if barType != "" && barType != dukascopy.BarTypeTime {
		tickReq := partRequest
		tickReq.Granularity = dukascopy.GranularityTick
		var result dukascopy.DownloadResult
		result, err = client.Download(ctx, tickReq)
		if err == nil {
			instrument = result.Instrument
			ticks := result.Ticks
			if outlierCfg.Enabled {
				ticks = dukascopy.CleanTickOutliers(ticks, outlierCfg.Window, outlierCfg.Threshold)
			}
			err = cfg.WriteTicksAtomic(partPath, instrument, customBarTickColumns, ticks)
			rowsWritten = len(ticks)
		}
	} else if resultKind == dukascopy.ResultKindTick {
		var result dukascopy.DownloadResult
		result, err = client.Download(ctx, partRequest)
		if err == nil {
			instrument = result.Instrument
			ticks := result.Ticks
			if outlierCfg.Enabled {
				ticks = dukascopy.CleanTickOutliers(ticks, outlierCfg.Window, outlierCfg.Threshold)
			}
			err = cfg.WriteTicksAtomic(partPath, instrument, tickColumns, ticks)
			rowsWritten = len(ticks)
		}
	} else if csvout.BarColumnsNeedBidAsk(barColumns) {
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
			err = cfg.WriteBarsAtomic(partPath, instrument, barColumns, nil, bidBars, askBars)
			rowsWritten = len(bidBars)
		}
	} else {
		var result dukascopy.DownloadResult
		result, err = client.Download(ctx, partRequest)
		if err == nil {
			instrument = result.Instrument
			bars := result.Bars
			err = applyTickCountsForColumns(ctx, client, partRequest, barColumns, nil, false, bars)
			if err == nil {
				if outlierCfg.Enabled {
					bars = dukascopy.CleanBarOutliers(bars, outlierCfg.Window, outlierCfg.Threshold)
				}
				err = cfg.WriteBarsAtomic(partPath, instrument, barColumns, bars, nil, nil)
				rowsWritten = len(bars)
			}
		}
	}

	if err != nil {
		return partitionWorkResult{Item: item, Worker: worker, Err: err}
	}

	audit, err := auditChunkFile(partPath)
	if err != nil {
		return partitionWorkResult{Item: item, Worker: worker, Err: fmt.Errorf("audit chunk %s: %w", partPath, err)}
	}

	return partitionWorkResult{
		Item:        item,
		Worker:      worker,
		RowsWritten: rowsWritten,
		Audit:       audit,
		Instrument:  instrument,
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
