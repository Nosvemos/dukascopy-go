package dukascopy

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"time"
)

func (c *Client) downloadTicks(ctx context.Context, instrument Instrument, from time.Time, to time.Time) ([]Tick, error) {
	hours := make([]time.Time, 0)
	for current := hourStartUTC(from); current.Before(to); current = current.Add(time.Hour) {
		if IsMarketClosed(instrument.Code, current) {
			continue
		}
		hours = append(hours, current)
	}

	return downloadParallel(c, ctx, len(hours), func(ctx context.Context, idx int) ([]Tick, int64, error) {
		current := hours[idx]
		return c.fetchTickChunk(ctx, instrument, current, from, to)
	}, "tick", func(idx int) string {
		return hours[idx].Format(time.RFC3339)
	})
}

// fetchTickChunk downloads a single tick chunk for a given hour using either the datafeed or jetta engine.
func (c *Client) fetchTickChunk(
	ctx context.Context,
	instrument Instrument,
	current time.Time,
	from time.Time,
	to time.Time,
) ([]Tick, int64, error) {
	if c.engine == EngineDatafeed {
		symbolClean := formatDatafeedSymbol(instrument.Code)
		monthStr := fmt.Sprintf("%02d", int(current.Month())-1)
		dayStr := fmt.Sprintf("%02d", current.Day())
		hourStr := fmt.Sprintf("%02dh_ticks.bi5", current.Hour())
		segments := []string{
			"datafeed", symbolClean,
			strconv.Itoa(current.Year()),
			monthStr,
			dayStr,
			hourStr,
		}
		bytesData, err := c.getRawBytes(ctx, segments)
		if err != nil || len(bytesData) == 0 {
			return nil, 0, err
		}
		decoded, err := DecodeTicksBi5(bytes.NewReader(bytesData), current, instrument.PriceScale)
		if err != nil {
			return nil, 0, err
		}
		return filterTicks(decoded, from, to), int64(len(bytesData)), nil
	}

	var payload tickPayload
	n, err := c.getJSONWithBytes(ctx, []string{
		"v1", "ticks", instrument.Code,
		strconv.Itoa(current.Year()),
		strconv.Itoa(int(current.Month())),
		strconv.Itoa(current.Day()),
		strconv.Itoa(current.Hour()),
	}, &payload)
	if err != nil {
		return nil, 0, err
	}
	return filterTicks(decodeTicks(payload), from, to), n, nil
}
