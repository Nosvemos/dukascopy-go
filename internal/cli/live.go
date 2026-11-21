package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

var (
	liveNow = func() time.Time {
		return time.Now().UTC()
	}
	liveSleep = sleepWithContext
)

const (
	// liveMaxConsecutiveFailures is how many cycles in a row may fail (for
	// example on transient HTTP errors) before the live loop gives up.
	liveMaxConsecutiveFailures = 10
	// liveMaxFailureBackoff caps the exponential backoff between failed cycles.
	liveMaxFailureBackoff = 5 * time.Minute
)

// liveFailureBackoff returns pollInterval * 2^(failures-1), capped at
// liveMaxFailureBackoff.
func liveFailureBackoff(pollInterval time.Duration, failures int) time.Duration {
	if pollInterval <= 0 {
		pollInterval = time.Second
	}
	backoff := pollInterval
	for i := 1; i < failures; i++ {
		if backoff >= liveMaxFailureBackoff/2 {
			return liveMaxFailureBackoff
		}
		backoff *= 2
	}
	if backoff > liveMaxFailureBackoff {
		return liveMaxFailureBackoff
	}
	return backoff
}

func runLiveDownload(
	ctx context.Context,
	client *dukascopy.Client,
	stdout io.Writer,
	stderr io.Writer,
	outputPath string,
	storageOutputPath string,
	manifestPath string,
	request dukascopy.DownloadRequest,
	resultKind dukascopy.ResultKind,
	barColumns []string,
	tickColumns []string,
	partitionMode string,
	parallelism int,
	pollInterval time.Duration,
) error {
	progress, _ := stderr.(*progressPrinter)
	outputToStdout := strings.TrimSpace(outputPath) == "-"
	stdoutHeaderWritten := false
	label := "bars"
	if resultKind == dukascopy.ResultKindTick {
		label = "ticks"
	}
	consecutiveFailures := 0

	for {
		if err := ctx.Err(); err != nil {
			return finishLiveDownload(stdout, progress, outputPath, err)
		}

		upperInclusive, err := liveUpperInclusive(request.Granularity, liveNow())
		if err != nil {
			return err
		}

		cycleRequest := request
		cycleRequest.To = inclusiveDownloadEnd(upperInclusive)

		if cycleRequest.From.Before(cycleRequest.To) {
			var appended int
			if partitionMode != partitionNone {
				if outputToStdout {
					appended, stdoutHeaderWritten, err = runLivePartitionStdoutCycle(
						ctx,
						client,
						stdout,
						stderr,
						stdoutHeaderWritten,
						storageOutputPath,
						manifestPath,
						cycleRequest,
						resultKind,
						barColumns,
						tickColumns,
						partitionMode,
						parallelism,
					)
				} else {
					appended, err = runLivePartitionCycle(
						ctx,
						client,
						stderr,
						storageOutputPath,
						manifestPath,
						cycleRequest,
						resultKind,
						barColumns,
						tickColumns,
						partitionMode,
						parallelism,
					)
				}
			} else if outputToStdout {
				if progress != nil {
					progress.SetStatus(fmt.Sprintf(
						"live stream %s -> %s",
						cycleRequest.From.UTC().Format(time.RFC3339),
						upperInclusive.UTC().Format(time.RFC3339),
					))
				}
				appended, stdoutHeaderWritten, err = runLiveStdoutCycle(
					ctx,
					client,
					stdout,
					stdoutHeaderWritten,
					cycleRequest,
					resultKind,
					barColumns,
					tickColumns,
				)
				if err == nil {
					request.From = cycleRequest.To
				}
			} else {
				resumeState, dedupeRecord, resumeErr := prepareResume(true, outputPath, resultKind, barColumns, tickColumns, &cycleRequest)
				if resumeErr != nil {
					return resumeErr
				}
				if progress != nil {
					progress.SetStatus(fmt.Sprintf(
						"live sync %s -> %s",
						cycleRequest.From.UTC().Format(time.RFC3339),
						upperInclusive.UTC().Format(time.RFC3339),
					))
				}

				appended, err = runSingleDownload(ctx, client, stdout, outputPath, false, resumeState, dedupeRecord, cycleRequest, resultKind, barColumns, tickColumns)
			}
			if err != nil {
				if errors.Is(err, context.Canceled) || ctx.Err() != nil {
					return finishLiveDownload(stdout, progress, outputPath, context.Canceled)
				}
				consecutiveFailures++
				if consecutiveFailures >= liveMaxConsecutiveFailures {
					return fmt.Errorf("live download giving up after %d consecutive failures: %w", consecutiveFailures, err)
				}
				backoff := liveFailureBackoff(pollInterval, consecutiveFailures)
				fmt.Fprintf(stderr, "%slive%s cycle failed (%d/%d consecutive), retrying in %s: %v\n",
					colorize(colorYellow), colorize(colorReset), consecutiveFailures, liveMaxConsecutiveFailures, backoff, err)
				if progress != nil {
					progress.SetStatus(fmt.Sprintf("live retry %d/%d in %s", consecutiveFailures, liveMaxConsecutiveFailures, backoff))
				}
				if err := liveSleep(ctx, backoff); err != nil {
					return finishLiveDownload(stdout, progress, outputPath, err)
				}
				continue
			}
			consecutiveFailures = 0
			if appended > 0 && !outputToStdout {
				fmt.Fprintf(
					stdout,
					"%slive%s wrote %d %s through %s to %s\n",
					colorize(colorCyan),
					colorize(colorReset),
					appended,
					label,
					upperInclusive.UTC().Format(time.RFC3339),
					outputPath,
				)
			}
		} else if progress != nil {
			progress.SetStatus("live waiting for next completed interval")
		}

		if err := liveSleep(ctx, pollInterval); err != nil {
			return finishLiveDownload(stdout, progress, outputPath, err)
		}
	}
}

func finishLiveDownload(stdout io.Writer, progress *progressPrinter, outputPath string, err error) error {
	if progress != nil {
		progress.SetStatus("live stopped")
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	if strings.TrimSpace(outputPath) == "-" {
		return nil
	}
	fmt.Fprintf(stdout, "%slive%s stopped for %s\n", colorize(colorCyan), colorize(colorReset), outputPath)
	return nil
}
