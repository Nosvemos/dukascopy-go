package dukascopy

import (
	"context"
	"fmt"
	"sync"
)

// downloadTask represents a single chunk of work completed by a download worker.
type downloadTask[T any] struct {
	index int
	data  []T
	bytes int64
	err   error
}

// downloadParallel is the generic worker pool engine for all download types (bars and ticks).
// It fans out chunkCount tasks across up to 8 workers, collects results in index order,
// cancels on first error, emits progress, and concatenates the final result.
//
// A chunk whose fetch reports ErrNoData (HTTP 404) is treated as empty. The
// function never returns partial data with a nil error: it fails with the
// first chunk error, with ctx.Err() when the parent context is done, or
// when fewer chunks than expected completed.
func downloadParallel[T any](
	c *Client,
	ctx context.Context,
	chunkCount int,
	fetch func(ctx context.Context, idx int) ([]T, int64, error),
	scope string,
	label func(idx int) string,
) ([]T, error) {
	if chunkCount == 0 {
		return nil, nil
	}

	tasks := make(chan int, chunkCount)
	// Buffered to chunkCount so every worker can always deliver its result
	// without blocking, even after cancellation.
	results := make(chan downloadTask[T], chunkCount)

	workersCount := 8
	if chunkCount < workersCount {
		workersCount = chunkCount
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
				if err := childCtx.Err(); err != nil {
					results <- downloadTask[T]{index: idx, err: err}
					continue
				}
				data, n, err := fetch(childCtx, idx)
				if err != nil && isNoDataError(err) {
					data, n, err = nil, 0, nil
				}
				results <- downloadTask[T]{index: idx, data: data, bytes: n, err: err}
			}
		}()
	}

	for i := 0; i < chunkCount; i++ {
		tasks <- i
	}
	close(tasks)

	go func() {
		wg.Wait()
		close(results)
	}()

	var totalBytes int64
	var totalRows int
	var completedCount int
	var firstErr error

	chunksData := make([][]T, chunkCount)
	for res := range results {
		if res.err != nil && firstErr == nil {
			firstErr = res.err
			cancel()
		}
		if firstErr == nil {
			chunksData[res.index] = res.data
			totalBytes += res.bytes
			totalRows += len(res.data)
		}
		completedCount++

		detail := ""
		if label != nil {
			detail = label(res.index)
		}
		c.emitProgress(ProgressEvent{
			Kind:    "chunk",
			Scope:   scope,
			Current: completedCount,
			Total:   chunkCount,
			Detail:  detail,
			Rows:    totalRows,
			Bytes:   totalBytes,
		})
	}

	if firstErr != nil {
		return nil, firstErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if completedCount != chunkCount {
		return nil, fmt.Errorf("dukascopy: %s download completed %d of %d chunks", scope, completedCount, chunkCount)
	}

	c.emitProgress(ProgressEvent{
		Kind:    "chunk",
		Scope:   scope,
		Current: chunkCount,
		Total:   chunkCount,
		Detail:  "completed",
		Rows:    totalRows,
		Bytes:   totalBytes,
	})

	all := make([]T, 0, totalRows)
	for _, chunk := range chunksData {
		all = append(all, chunk...)
	}
	return all, nil
}

// countTotal sums the lengths of all chunks in a [][]T slice.
func countTotal[T any](chunks [][]T) int {
	total := 0
	for _, chunk := range chunks {
		total += len(chunk)
	}
	return total
}
