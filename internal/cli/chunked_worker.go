package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Nosvemos/dukascopy-go/pkg/csvout"
	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
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
