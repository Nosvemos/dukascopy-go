package cli

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/checkpoint"
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

// RunSDKDownload coordinates thread-safe download using the low-memory chunked engine.
func RunSDKDownload(ctx context.Context, opts SDKDownloadOptions) error {
	sdkDownloadMutex.Lock()
	defer sdkDownloadMutex.Unlock()

	engineVal := dukascopy.Engine(strings.ToLower(strings.TrimSpace(opts.Engine)))
	if engineVal != dukascopy.EngineJetta && engineVal != dukascopy.EngineDatafeed {
		return fmt.Errorf("unknown engine %q (supported: jetta, datafeed)", opts.Engine)
	}

	timeframeValue := strings.TrimSpace(opts.Timeframe)
	normalizedTimeframe := dukascopy.NormalizeGranularity(dukascopy.Granularity(timeframeValue))

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
		var err error
		if normalizedTimeframe == dukascopy.GranularityTick {
			tickColumns, err = csvout.ParseTickColumns(opts.CustomColumns)
		} else {
			barColumns, err = csvout.ParseBarColumns(opts.CustomColumns)
		}
		if err != nil {
			return err
		}
	}

	var loc *time.Location
	tz := strings.TrimSpace(opts.Timezone)
	if strings.ToLower(tz) == "eet" {
		var err error
		loc, err = time.LoadLocation("Europe/Athens")
		if err != nil {
			loc = time.FixedZone("EET", 2*60*60)
		}
	} else if strings.ToLower(tz) == "est" {
		var err error
		loc, err = time.LoadLocation("America/New_York")
		if err != nil {
			loc = time.FixedZone("EST", -5*60*60)
		}
	} else if tz != "" && strings.ToUpper(tz) != "UTC" {
		var err error
		loc, err = time.LoadLocation(tz)
		if err != nil {
			return fmt.Errorf("invalid timezone %q: %w", tz, err)
		}
	} else {
		loc = time.UTC
	}

	// Apply configuration parameters to package-level globals safely under mutex
	csvout.ConfigMutex.Lock()
	csvout.OutputLocation = loc
	csvout.OutputTimestampFormat = time.RFC3339Nano
	csvout.CSVDelimiter = ','
	csvout.HideCSVHeader = false
	csvout.FillGaps = strings.ToLower(strings.TrimSpace(opts.FillGaps))
	csvout.ConfigMutex.Unlock()

	request := dukascopy.DownloadRequest{
		Symbol:      opts.Symbol,
		Granularity: normalizedTimeframe,
		Side:        dukascopy.PriceSide(strings.ToUpper(opts.Side)),
		From:        opts.From.UTC(),
		To:          inclusiveDownloadEnd(opts.To.UTC()),
	}

	resultKind := dukascopy.ResultKindBar
	if normalizedTimeframe == dukascopy.GranularityTick {
		resultKind = dukascopy.ResultKindTick
	}

	partitionValue := strings.TrimSpace(opts.Partition)
	normalizedPartition, err := normalizePartition(partitionValue, normalizedTimeframe)
	if err != nil {
		return err
	}

	manifestPath := checkpoint.DefaultManifestPath(opts.OutputPath)
	resumeState, dedupeRecord, err := prepareResume(opts.Resume, opts.OutputPath, resultKind, barColumns, tickColumns, &request)
	if err != nil {
		return err
	}

	if !request.From.Before(request.To) {
		return nil
	}

	client, err := dukascopy.NewClient("https://jetta.dukascopy.com", 30*time.Second)
	if err != nil {
		return err
	}
	client = client.
		WithEngine(engineVal).
		WithRetries(3).
		WithBackoff(500 * time.Millisecond).
		WithRateLimit(0)

	_, err = runChunkedDownload(
		ctx,
		client,
		os.Stdout,
		os.Stderr,
		opts.OutputPath,
		manifestPath,
		request,
		resultKind,
		barColumns,
		tickColumns,
		normalizedPartition,
		opts.Parallelism,
		"./.dukascopy_cache",
		false,
		resumeState,
		dedupeRecord,
		opts.Hive,
	)
	return err
}
