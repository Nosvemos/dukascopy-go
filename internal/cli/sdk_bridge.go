package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/checkpoint"
	"github.com/Nosvemos/dukascopy-go/pkg/cloud"
	"github.com/Nosvemos/dukascopy-go/pkg/csvout"
	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

var sdkDownloadMutex sync.Mutex

type SDKDownloadOptions struct {
	Symbol        string
	Timeframe     string
	Side          string
	From          time.Time
	To            time.Time
	OutputPath    string
	Engine        string
	PriceScale    int
	Profile       string
	Timezone      string
	CustomColumns string
	Resume        bool
	Parallelism   int
	Partition     string
	FillGaps      string
	Hive          bool
}

// RunSDKDownload coordinates thread-safe download using the low-memory chunked
// engine. It validates every option before any network access and resolves
// the base URL, end-time clamping and cache retention the same way the CLI
// download command does. PriceScale is ignored: the instrument catalog
// supplies the price scale.
func RunSDKDownload(ctx context.Context, opts SDKDownloadOptions) error {
	sdkDownloadMutex.Lock()
	defer sdkDownloadMutex.Unlock()

	engineName := strings.ToLower(strings.TrimSpace(opts.Engine))
	if engineName == "" {
		engineName = string(dukascopy.EngineJetta)
	}
	engineVal := dukascopy.Engine(engineName)
	if engineVal != dukascopy.EngineJetta && engineVal != dukascopy.EngineDatafeed {
		return fmt.Errorf("unknown engine %q (supported: jetta, datafeed)", opts.Engine)
	}
	if strings.TrimSpace(opts.Symbol) == "" {
		return errors.New("symbol is required")
	}
	if strings.TrimSpace(opts.OutputPath) == "" {
		return errors.New("output path is required")
	}
	if strings.TrimSpace(opts.OutputPath) == "-" {
		return errors.New("output path \"-\" (stdout) is not supported by the SDK")
	}
	if cloud.IsCloudPath(opts.OutputPath) {
		return errors.New("cloud (s3://) outputs are not supported by the SDK; download locally and upload")
	}
	if opts.From.IsZero() || opts.To.IsZero() {
		return errors.New("from and to are required")
	}
	if opts.To.Before(opts.From) {
		return fmt.Errorf("to (%s) must be the same as or later than from (%s)", opts.To.UTC().Format(time.RFC3339), opts.From.UTC().Format(time.RFC3339))
	}
	now := time.Now().UTC()
	if opts.From.After(now) {
		return fmt.Errorf("from %s is in the future", opts.From.UTC().Format(time.RFC3339))
	}
	timeframeValue := strings.TrimSpace(opts.Timeframe)
	if timeframeValue == "" {
		timeframeValue = string(dukascopy.GranularityM1)
	}
	normalizedTimeframe, err := validateTimeframe(timeframeValue)
	if err != nil {
		return err
	}
	sideValue := opts.Side
	if strings.TrimSpace(sideValue) == "" {
		sideValue = string(dukascopy.PriceSideBid)
	}
	side, err := validateSide(sideValue)
	if err != nil {
		return err
	}
	fillGaps, err := validateFillGaps(opts.FillGaps)
	if err != nil {
		return err
	}
	parallelism := opts.Parallelism
	if parallelism < 1 {
		parallelism = 1
	}
	if err := validateParallelism(parallelism); err != nil {
		return err
	}

	profileVal := csvout.ProfileSimple
	switch strings.ToLower(strings.TrimSpace(opts.Profile)) {
	case "full":
		profileVal = csvout.ProfileFull
	case "fused":
		profileVal = csvout.ProfileFused
	default:
		profileVal = csvout.ProfileSimple
	}

	barColumns := csvout.BarColumnsForProfile(profileVal)
	tickColumns := csvout.TickColumnsForProfile(profileVal)

	if opts.CustomColumns != "" {
		if normalizedTimeframe == dukascopy.GranularityTick {
			tickColumns, err = csvout.ParseTickColumns(opts.CustomColumns)
		} else {
			barColumns, err = csvout.ParseBarColumns(opts.CustomColumns)
		}
		if err != nil {
			return err
		}
	}

	loc, err := loadOutputLocation(opts.Timezone)
	if err != nil {
		return err
	}

	resultKind := dukascopy.ResultKindBar
	if normalizedTimeframe == dukascopy.GranularityTick {
		resultKind = dukascopy.ResultKindTick
	}

	normalizedPartition, err := normalizePartition(strings.TrimSpace(opts.Partition), normalizedTimeframe)
	if err != nil {
		return err
	}
	if opts.Hive && normalizedPartition == partitionNone {
		return errors.New("hive requires a partition mode")
	}
	if opts.Resume && normalizedPartition != partitionNone {
		return errors.New("resume cannot be combined with a partition mode; partitioned downloads resume from their checkpoint manifest automatically")
	}

	// Apply configuration parameters to package-level globals safely under mutex
	csvout.ConfigMutex.Lock()
	csvout.OutputLocation = loc
	csvout.OutputTimestampFormat = time.RFC3339Nano
	csvout.CSVDelimiter = ','
	csvout.HideCSVHeader = false
	csvout.FillGaps = fillGaps
	csvout.ConfigMutex.Unlock()
	outputConfig := csvout.DefaultConfig()

	request := dukascopy.DownloadRequest{
		Symbol:      opts.Symbol,
		Granularity: normalizedTimeframe,
		Side:        side,
		From:        opts.From.UTC(),
		To:          inclusiveDownloadEnd(clampDownloadEnd(opts.To.UTC(), now)),
	}

	manifestPath := checkpoint.DefaultManifestPath(opts.OutputPath)
	resumeState, dedupeRecord, err := prepareResume(opts.Resume, opts.OutputPath, resultKind, barColumns, tickColumns, &request)
	if err != nil {
		return err
	}

	if !request.From.Before(request.To) {
		// Resume found the existing file already up to date.
		return nil
	}

	baseURL := readBaseURL()
	client, err := dukascopy.NewClient(baseURL, defaultHTTPTimeout)
	if err != nil {
		return err
	}
	client = client.
		WithEngine(engineVal).
		WithRetries(3).
		WithBackoff(500 * time.Millisecond).
		WithRateLimit(0)

	_, err = runChunkedDownload(ctx, client, os.Stdout, os.Stderr, chunkedDownloadOptions{
		OutputPath:         opts.OutputPath,
		CheckpointManifest: manifestPath,
		Request:            request,
		ResultKind:         resultKind,
		BarColumns:         barColumns,
		TickColumns:        tickColumns,
		PartitionMode:      normalizedPartition,
		Parallelism:        parallelism,
		CacheDir:           "./.dukascopy_cache",
		// Like the CLI: partitioned downloads keep their parts (the checkpoint).
		KeepCache:    normalizedPartition != partitionNone,
		ResumeState:  resumeState,
		DedupeRecord: dedupeRecord,
		Hive:         opts.Hive,
		BarType:      dukascopy.BarTypeTime,
		Engine:       string(engineVal),
		OutputConfig: outputConfig,
	})
	return err
}
