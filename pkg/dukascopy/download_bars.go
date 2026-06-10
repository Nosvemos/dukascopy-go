package dukascopy

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"
)

func (c *Client) downloadBars(ctx context.Context, instrument Instrument, side PriceSide, granularity Granularity, from time.Time, to time.Time) ([]Bar, error) {
	switch normalizeGranularity(granularity) {
	case GranularityM1:
		return c.downloadMinuteBars(ctx, instrument, side, from, to)
	case GranularityM3, GranularityM5, GranularityM15, GranularityM30:
		minuteBars, err := c.downloadMinuteBars(ctx, instrument, side, from, to)
		if err != nil {
			return nil, err
		}
		return AggregateBars(minuteBars, granularity, from, to)
	case GranularityH1:
		return c.downloadHourlyBars(ctx, instrument, side, from, to)
	case GranularityH4:
		hourlyBars, err := c.downloadHourlyBars(ctx, instrument, side, from, to)
		if err != nil {
			return nil, err
		}
		return AggregateBars(hourlyBars, granularity, from, to)
	case GranularityD1:
		return c.downloadDailyBars(ctx, instrument, side, from, to)
	case GranularityW1, GranularityMN1:
		dailyBars, err := c.downloadDailyBars(ctx, instrument, side, from, to)
		if err != nil {
			return nil, err
		}
		return AggregateBars(dailyBars, granularity, from, to)
	default:
		return nil, fmt.Errorf("unsupported bar granularity %q", granularity)
	}
}

func (c *Client) downloadMinuteBars(ctx context.Context, instrument Instrument, side PriceSide, from time.Time, to time.Time) ([]Bar, error) {
	var all []Bar
	days := make([]time.Time, 0)
	for current := midnightUTC(from); current.Before(to); current = current.AddDate(0, 0, 1) {
		if !IsCryptoSymbol(instrument.Code) && current.UTC().Weekday() == time.Saturday {
			continue
		}
		days = append(days, current)
	}

	if len(days) == 0 {
		return nil, nil
	}

	type taskResult struct {
		index int
		bars  []Bar
		bytes int64
		err   error
	}

	tasks := make(chan int, len(days))
	results := make(chan taskResult, len(days))

	workersCount := 8
	if len(days) < workersCount {
		workersCount = len(days)
	}
	if workersCount < 1 {
		workersCount = 1
	}

	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < workersCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range tasks {
				current := days[idx]
				var chunkBars []Bar
				var chunkBytes int64
				var err error

				if c.engine == EngineDatafeed {
					symbolClean := formatDatafeedSymbol(instrument.Code)
					monthStr := fmt.Sprintf("%02d", int(current.Month())-1)
					dayStr := fmt.Sprintf("%02d", current.Day())
					segments := []string{
						"datafeed", symbolClean,
						fmt.Sprintf("%d", current.Year()),
						monthStr,
						dayStr,
						string(side) + "_candles_min_1.bi5",
					}
					var bytesData []byte
					bytesData, err = c.getRawBytes(childCtx, segments)
					if err == nil && len(bytesData) > 0 {
						chunkBytes = int64(len(bytesData))
						var decoded []Bar
						decoded, err = DecodeBarsBi5(bytes.NewReader(bytesData), current, instrument.PriceScale)
						if err == nil {
							chunkBars = filterBars(decoded, from, to)
						}
					}
				} else {
					var payload candlePayload
					var n int64
					n, err = c.getJSONWithBytes(childCtx, []string{
						"v1", "candles", "minute", instrument.Code, string(side),
						fmt.Sprintf("%d", current.Year()),
						fmt.Sprintf("%d", int(current.Month())),
						fmt.Sprintf("%d", current.Day()),
					}, &payload)
					if err == nil {
						chunkBytes = n
						chunkBars = filterBars(decodeBars(payload), from, to)
					}
				}

				if err != nil && isNoDataError(err) {
					err = nil
				}

				select {
				case <-childCtx.Done():
					return
				case results <- taskResult{index: idx, bars: chunkBars, bytes: chunkBytes, err: err}:
				}
			}
		}()
	}

	for i := 0; i < len(days); i++ {
		tasks <- i
	}
	close(tasks)

	go func() {
		wg.Wait()
		close(results)
	}()

	var totalBytes int64
	var completedCount int
	var firstErr error

	chunksData := make([][]Bar, len(days))
	for res := range results {
		if res.err != nil && firstErr == nil {
			firstErr = res.err
			cancel()
		}
		if firstErr == nil {
			chunksData[res.index] = res.bars
			totalBytes += res.bytes
		}
		completedCount++
		c.emitProgress(ProgressEvent{
			Kind:    "chunk",
			Scope:   "minute",
			Current: completedCount,
			Total:   len(days),
			Detail:  days[res.index].Format("2006-01-02"),
			Rows:    countTotalBars(chunksData),
			Bytes:   totalBytes,
		})
	}

	if firstErr != nil {
		return nil, firstErr
	}

	c.emitProgress(ProgressEvent{
		Kind:    "chunk",
		Scope:   "minute",
		Current: len(days),
		Total:   len(days),
		Detail:  "completed",
		Rows:    countTotalBars(chunksData),
		Bytes:   totalBytes,
	})

	for _, chunk := range chunksData {
		all = append(all, chunk...)
	}

	return all, nil
}

func (c *Client) downloadHourlyBars(ctx context.Context, instrument Instrument, side PriceSide, from time.Time, to time.Time) ([]Bar, error) {
	var all []Bar
	months := make([]time.Time, 0)
	for current := monthStartUTC(from); current.Before(to); current = current.AddDate(0, 1, 0) {
		months = append(months, current)
	}

	if len(months) == 0 {
		return nil, nil
	}

	type taskResult struct {
		index int
		bars  []Bar
		bytes int64
		err   error
	}

	tasks := make(chan int, len(months))
	results := make(chan taskResult, len(months))

	workersCount := 8
	if len(months) < workersCount {
		workersCount = len(months)
	}
	if workersCount < 1 {
		workersCount = 1
	}

	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < workersCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range tasks {
				current := months[idx]
				var chunkBars []Bar
				var chunkBytes int64
				var err error

				var payload candlePayload
				var n int64
				n, err = c.getJSONWithBytes(childCtx, []string{
					"v1", "candles", "hour", instrument.Code, string(side),
					fmt.Sprintf("%d", current.Year()),
					fmt.Sprintf("%d", int(current.Month())),
				}, &payload)
				if err == nil {
					chunkBytes = n
					chunkBars = filterBars(decodeBars(payload), from, to)
				}

				if err != nil && isNoDataError(err) {
					err = nil
				}

				select {
				case <-childCtx.Done():
					return
				case results <- taskResult{index: idx, bars: chunkBars, bytes: chunkBytes, err: err}:
				}
			}
		}()
	}

	for i := 0; i < len(months); i++ {
		tasks <- i
	}
	close(tasks)

	go func() {
		wg.Wait()
		close(results)
	}()

	var totalBytes int64
	var completedCount int
	var firstErr error

	chunksData := make([][]Bar, len(months))
	for res := range results {
		if res.err != nil && firstErr == nil {
			firstErr = res.err
			cancel()
		}
		if firstErr == nil {
			chunksData[res.index] = res.bars
			totalBytes += res.bytes
		}
		completedCount++
		c.emitProgress(ProgressEvent{
			Kind:    "chunk",
			Scope:   "hour",
			Current: completedCount,
			Total:   len(months),
			Detail:  months[res.index].Format("2006-01"),
			Rows:    countTotalBars(chunksData),
			Bytes:   totalBytes,
		})
	}

	if firstErr != nil {
		return nil, firstErr
	}

	c.emitProgress(ProgressEvent{
		Kind:    "chunk",
		Scope:   "hour",
		Current: len(months),
		Total:   len(months),
		Detail:  "completed",
		Rows:    countTotalBars(chunksData),
		Bytes:   totalBytes,
	})

	for _, chunk := range chunksData {
		all = append(all, chunk...)
	}

	return all, nil
}

func (c *Client) downloadDailyBars(ctx context.Context, instrument Instrument, side PriceSide, from time.Time, to time.Time) ([]Bar, error) {
	var all []Bar
	years := make([]int, 0)
	for current := from.UTC().Year(); current <= to.UTC().Year(); current++ {
		years = append(years, current)
	}

	if len(years) == 0 {
		return nil, nil
	}

	type taskResult struct {
		index int
		bars  []Bar
		bytes int64
		err   error
	}

	tasks := make(chan int, len(years))
	results := make(chan taskResult, len(years))

	workersCount := 8
	if len(years) < workersCount {
		workersCount = len(years)
	}
	if workersCount < 1 {
		workersCount = 1
	}

	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < workersCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range tasks {
				year := years[idx]
				var chunkBars []Bar
				var chunkBytes int64
				var err error

				var payload candlePayload
				var n int64
				n, err = c.getJSONWithBytes(childCtx, []string{
					"v1", "candles", "day", instrument.Code, string(side),
					fmt.Sprintf("%d", year),
				}, &payload)
				if err == nil {
					chunkBytes = n
					chunkBars = filterBars(decodeBars(payload), from, to)
				}

				if err != nil && isNoDataError(err) {
					err = nil
				}

				select {
				case <-childCtx.Done():
					return
				case results <- taskResult{index: idx, bars: chunkBars, bytes: chunkBytes, err: err}:
				}
			}
		}()
	}

	for i := 0; i < len(years); i++ {
		tasks <- i
	}
	close(tasks)

	go func() {
		wg.Wait()
		close(results)
	}()

	var totalBytes int64
	var completedCount int
	var firstErr error

	chunksData := make([][]Bar, len(years))
	for res := range results {
		if res.err != nil && firstErr == nil {
			firstErr = res.err
			cancel()
		}
		if firstErr == nil {
			chunksData[res.index] = res.bars
			totalBytes += res.bytes
		}
		completedCount++
		c.emitProgress(ProgressEvent{
			Kind:    "chunk",
			Scope:   "day",
			Current: completedCount,
			Total:   len(years),
			Detail:  fmt.Sprintf("%d", years[res.index]),
			Rows:    countTotalBars(chunksData),
			Bytes:   totalBytes,
		})
	}

	if firstErr != nil {
		return nil, firstErr
	}

	c.emitProgress(ProgressEvent{
		Kind:    "chunk",
		Scope:   "day",
		Current: len(years),
		Total:   len(years),
		Detail:  "completed",
		Rows:    countTotalBars(chunksData),
		Bytes:   totalBytes,
	})

	for _, chunk := range chunksData {
		all = append(all, chunk...)
	}

	return all, nil
}
