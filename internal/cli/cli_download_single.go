package cli

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"

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
	if resultKind == dukascopy.ResultKindTick {
		result, err := client.Download(ctx, request)
		if err != nil {
			return 0, err
		}
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

	result, err := client.Download(ctx, request)
	if err != nil {
		return 0, err
	}
	if outputToStdout {
		return len(result.Bars), csvout.WriteBarsToWriter(stdout, result.Instrument, barColumns, result.Bars, nil, nil)
	}
	return writeBarOutput(outputPath, resumeState, dedupeRecord, result.Instrument, barColumns, result.Bars, nil, nil)
}

func loadBidAskBars(ctx context.Context, client *dukascopy.Client, request dukascopy.DownloadRequest) (dukascopy.Instrument, []dukascopy.Bar, []dukascopy.Bar, error) {
	instrument, bidBars, askBars, _, _, err := loadBidAskBarsWithTicks(ctx, client, request)
	return instrument, bidBars, askBars, err
}

// downloadBarsForSide is client.DownloadBarsForSide; tests replace it.
var downloadBarsForSide = func(ctx context.Context, client *dukascopy.Client, request dukascopy.DownloadRequest, side dukascopy.PriceSide) (dukascopy.Instrument, []dukascopy.Bar, error) {
	return client.DownloadBarsForSide(ctx, request, side)
}

// isNoDataError reports whether err means Dukascopy has no bar data for the
// request (HTTP 404), the only case where rebuilding bars from ticks can help.
func isNoDataError(err error) bool {
	if errors.Is(err, dukascopy.ErrNoData) {
		return true
	}
	var statusErr *dukascopy.HTTPStatusError
	return errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusNotFound
}

// loadBidAskBarsWithTicks downloads bid and ask bars concurrently. When one
// side fails the other request is cancelled. It falls back to aggregating
// bars from a tick download only when the failure is "no data" (HTTP 404);
// cancellation and every other error (bad request, exhausted 5xx retries,
// network failures) are returned as is.
func loadBidAskBarsWithTicks(ctx context.Context, client *dukascopy.Client, request dukascopy.DownloadRequest) (dukascopy.Instrument, []dukascopy.Bar, []dukascopy.Bar, []dukascopy.Tick, bool, error) {
	var bidBars, askBars []dukascopy.Bar
	var instrument dukascopy.Instrument
	var bidErr, askErr error

	sideCtx, cancelSides := context.WithCancel(ctx)
	defer cancelSides()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		var inst dukascopy.Instrument
		inst, bidBars, bidErr = downloadBarsForSide(sideCtx, client, request, dukascopy.PriceSideBid)
		if bidErr == nil {
			instrument = inst
		} else if !isNoDataError(bidErr) {
			cancelSides()
		}
	}()

	go func() {
		defer wg.Done()
		_, askBars, askErr = downloadBarsForSide(sideCtx, client, request, dukascopy.PriceSideAsk)
		if askErr != nil && !isNoDataError(askErr) {
			cancelSides()
		}
	}()

	wg.Wait()

	if bidErr == nil && askErr == nil {
		return instrument, bidBars, askBars, nil, false, nil
	}
	if err := ctx.Err(); err != nil {
		return dukascopy.Instrument{}, nil, nil, nil, false, err
	}
	// Report the error that caused the failure, not the sibling's
	// cancellation.
	for _, sideErr := range []error{bidErr, askErr} {
		if sideErr != nil && !isNoDataError(sideErr) && !errors.Is(sideErr, context.Canceled) {
			return dukascopy.Instrument{}, nil, nil, nil, false, sideErr
		}
	}
	for _, sideErr := range []error{bidErr, askErr} {
		if sideErr != nil && !isNoDataError(sideErr) {
			return dukascopy.Instrument{}, nil, nil, nil, false, sideErr
		}
	}

	tickRequest := request
	tickRequest.Granularity = dukascopy.GranularityTick
	tickResult, err := client.Download(ctx, tickRequest)
	if err != nil {
		return dukascopy.Instrument{}, nil, nil, nil, false, err
	}

	bidBars, err = dukascopy.AggregateTicksToBars(tickResult.Ticks, request.Granularity, dukascopy.PriceSideBid, request.From, request.To)
	if err != nil {
		return dukascopy.Instrument{}, nil, nil, nil, false, err
	}
	askBars, err = dukascopy.AggregateTicksToBars(tickResult.Ticks, request.Granularity, dukascopy.PriceSideAsk, request.From, request.To)
	if err != nil {
		return dukascopy.Instrument{}, nil, nil, nil, false, err
	}
	return tickResult.Instrument, bidBars, askBars, tickResult.Ticks, true, nil
}
