package dukascopy

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"
)

func (c *Client) downloadTicks(ctx context.Context, instrument Instrument, from time.Time, to time.Time) ([]Tick, error) {
	var all []Tick
	hours := make([]time.Time, 0)
	for current := hourStartUTC(from); current.Before(to); current = current.Add(time.Hour) {
		if IsMarketClosed(instrument.Code, current) {
			continue
		}
		hours = append(hours, current)
	}

	if len(hours) == 0 {
		return nil, nil
	}

	type taskResult struct {
		index int
		ticks []Tick
		bytes int64
		err   error
	}

	tasks := make(chan int, len(hours))
	results := make(chan taskResult, len(hours))

	workersCount := 8
	if len(hours) < workersCount {
		workersCount = len(hours)
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
				current := hours[idx]
				var chunkTicks []Tick
				var chunkBytes int64
				var err error

				if c.engine == EngineDatafeed {
					symbolClean := formatDatafeedSymbol(instrument.Code)
					monthStr := fmt.Sprintf("%02d", int(current.Month())-1)
					dayStr := fmt.Sprintf("%02d", current.Day())
					hourStr := fmt.Sprintf("%02dh_ticks.bi5", current.Hour())
					segments := []string{
						"datafeed", symbolClean,
						fmt.Sprintf("%d", current.Year()),
						monthStr,
						dayStr,
						hourStr,
					}
					var bytesData []byte
					bytesData, err = c.getRawBytes(childCtx, segments)
					if err == nil && len(bytesData) > 0 {
						chunkBytes = int64(len(bytesData))
						var decoded []Tick
						decoded, err = DecodeTicksBi5(bytes.NewReader(bytesData), current, instrument.PriceScale)
						if err == nil {
							chunkTicks = filterTicks(decoded, from, to)
						}
					}
				} else {
					var payload tickPayload
					var n int64
					n, err = c.getJSONWithBytes(childCtx, []string{
						"v1", "ticks", instrument.Code,
						fmt.Sprintf("%d", current.Year()),
						fmt.Sprintf("%d", int(current.Month())),
						fmt.Sprintf("%d", current.Day()),
						fmt.Sprintf("%d", current.Hour()),
					}, &payload)
					if err == nil {
						chunkBytes = n
						chunkTicks = filterTicks(decodeTicks(payload), from, to)
					}
				}

				if err != nil && isNoDataError(err) {
					err = nil
				}

				select {
				case <-childCtx.Done():
					return
				case results <- taskResult{index: idx, ticks: chunkTicks, bytes: chunkBytes, err: err}:
				}
			}
		}()
	}

	for i := 0; i < len(hours); i++ {
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

	chunksData := make([][]Tick, len(hours))
	for res := range results {
		if res.err != nil && firstErr == nil {
			firstErr = res.err
			cancel()
		}
		if firstErr == nil {
			chunksData[res.index] = res.ticks
			totalBytes += res.bytes
		}
		completedCount++
		c.emitProgress(ProgressEvent{
			Kind:    "chunk",
			Scope:   "tick",
			Current: completedCount,
			Total:   len(hours),
			Detail:  hours[res.index].Format(time.RFC3339),
			Rows:    countTotalTicks(chunksData),
			Bytes:   totalBytes,
		})
	}

	if firstErr != nil {
		return nil, firstErr
	}

	c.emitProgress(ProgressEvent{
		Kind:    "chunk",
		Scope:   "tick",
		Current: len(hours),
		Total:   len(hours),
		Detail:  "completed",
		Rows:    countTotalTicks(chunksData),
		Bytes:   totalBytes,
	})

	for _, chunk := range chunksData {
		all = append(all, chunk...)
	}

	return all, nil
}
