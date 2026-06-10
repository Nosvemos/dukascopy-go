package cli

import (
	"context"
	"io"

	"github.com/Nosvemos/dukascopy-go/pkg/csvout"
	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

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
