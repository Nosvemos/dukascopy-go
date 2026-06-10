package cli

import (
	"context"
	"io"

	"github.com/Nosvemos/dukascopy-go/pkg/csvout"
	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

func runSingleDownload(
	ctx context.Context,
	client *dukascopy.Client,
	stdout io.Writer,
	outputPath string,
	outputToStdout bool,
	resumeState *csvout.ResumeState,
	dedupeRecord []string,
	request dukascopy.DownloadRequest,
	resultKind dukascopy.ResultKind,
	barColumns []string,
	tickColumns []string,
) (int, error) {
	result, err := client.Download(ctx, request)
	if err != nil {
		return 0, err
	}

	if result.Kind == dukascopy.ResultKindTick {
		if outputToStdout {
			return len(result.Ticks), csvout.WriteTicksToWriter(stdout, result.Instrument, tickColumns, result.Ticks)
		}
		return writeTickOutput(outputPath, resumeState, dedupeRecord, result.Instrument, tickColumns, result.Ticks)
	}

	if csvout.BarColumnsNeedBidAsk(barColumns) {
		instrument, bidBars, askBars, err := loadBidAskBars(ctx, client, request)
		if err != nil {
			return 0, err
		}
		if outputToStdout {
			return len(bidBars), csvout.WriteBarsToWriter(stdout, instrument, barColumns, nil, bidBars, askBars)
		}
		return writeBarOutput(outputPath, resumeState, dedupeRecord, instrument, barColumns, nil, bidBars, askBars)
	}

	if outputToStdout {
		return len(result.Bars), csvout.WriteBarsToWriter(stdout, result.Instrument, barColumns, result.Bars, nil, nil)
	}
	return writeBarOutput(outputPath, resumeState, dedupeRecord, result.Instrument, barColumns, result.Bars, nil, nil)
}

func loadBidAskBars(ctx context.Context, client *dukascopy.Client, request dukascopy.DownloadRequest) (dukascopy.Instrument, []dukascopy.Bar, []dukascopy.Bar, error) {
	instrument, bidBars, bidErr := client.DownloadBarsForSide(ctx, request, dukascopy.PriceSideBid)
	if bidErr == nil {
		_, askBars, askErr := client.DownloadBarsForSide(ctx, request, dukascopy.PriceSideAsk)
		if askErr == nil {
			return instrument, bidBars, askBars, nil
		}
	}

	tickRequest := request
	tickRequest.Granularity = dukascopy.GranularityTick
	tickResult, err := client.Download(ctx, tickRequest)
	if err != nil {
		return dukascopy.Instrument{}, nil, nil, err
	}

	bidBars, err = dukascopy.AggregateTicksToBars(tickResult.Ticks, request.Granularity, dukascopy.PriceSideBid, request.From, request.To)
	if err != nil {
		return dukascopy.Instrument{}, nil, nil, err
	}
	askBars, err := dukascopy.AggregateTicksToBars(tickResult.Ticks, request.Granularity, dukascopy.PriceSideAsk, request.From, request.To)
	if err != nil {
		return dukascopy.Instrument{}, nil, nil, err
	}
	return tickResult.Instrument, bidBars, askBars, nil
}
