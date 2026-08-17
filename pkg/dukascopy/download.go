package dukascopy

import (
	"context"
)

func countTotalBars(chunks [][]Bar) int {
	total := 0
	for _, chunk := range chunks {
		total += len(chunk)
	}
	return total
}

func countTotalTicks(chunks [][]Tick) int {
	total := 0
	for _, chunk := range chunks {
		total += len(chunk)
	}
	return total
}

func (c *Client) resolveInstrumentForSymbol(ctx context.Context, symbol string) (Instrument, error) {
	c.cacheMu.RLock()
	if inst, ok := c.resolvedSymbols[symbol]; ok {
		c.cacheMu.RUnlock()
		return inst, nil
	}
	c.cacheMu.RUnlock()

	instruments, err := c.ListInstruments(ctx)
	if err != nil {
		return Instrument{}, err
	}

	instrument, err := ResolveInstrument(instruments, symbol)
	if err != nil {
		return Instrument{}, err
	}

	c.cacheMu.Lock()
	if c.resolvedSymbols == nil {
		c.resolvedSymbols = make(map[string]Instrument)
	}
	c.resolvedSymbols[symbol] = instrument
	c.cacheMu.Unlock()

	return instrument, nil
}

func (c *Client) Download(ctx context.Context, request DownloadRequest) (DownloadResult, error) {
	instrument, err := c.resolveInstrumentForSymbol(ctx, request.Symbol)
	if err != nil {
		return DownloadResult{}, err
	}

	side, err := normalizeSide(request.Side)
	if err != nil {
		return DownloadResult{}, err
	}

	switch normalizeGranularity(request.Granularity) {
	case GranularityTick:
		ticks, err := c.downloadTicks(ctx, instrument, request.From, request.To)
		if err != nil {
			return DownloadResult{}, err
		}
		return DownloadResult{Kind: ResultKindTick, Instrument: instrument, Ticks: ticks}, nil
	default:
		bars, err := c.downloadBars(ctx, instrument, side, request.Granularity, request.From, request.To)
		if err != nil {
			return DownloadResult{}, err
		}
		return DownloadResult{Kind: ResultKindBar, Instrument: instrument, Bars: bars}, nil
	}
}

func (c *Client) DownloadBarsForSide(ctx context.Context, request DownloadRequest, side PriceSide) (Instrument, []Bar, error) {
	instrument, err := c.resolveInstrumentForSymbol(ctx, request.Symbol)
	if err != nil {
		return Instrument{}, nil, err
	}

	normalizedSide, err := normalizeSide(side)
	if err != nil {
		return Instrument{}, nil, err
	}

	bars, err := c.downloadBars(ctx, instrument, normalizedSide, request.Granularity, request.From, request.To)
	return instrument, bars, err
}
