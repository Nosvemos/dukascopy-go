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
				if errors.Is(err, context.Canceled) {
					return finishLiveDownload(stdout, progress, outputPath, err)
				}
				return err
			}
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
