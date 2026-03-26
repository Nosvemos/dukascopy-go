package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"dukascopy-data-downloader/internal/csvout"
	"dukascopy-data-downloader/internal/dukascopy"
)

const (
	defaultBaseURL = "https://jetta.dukascopy.com"
	defaultTimeout = 30 * time.Second
)

func Run(args []string, stdout io.Writer, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stderr)
		return 2
	}

	switch args[0] {
	case "instruments":
		if err := runInstruments(args[1:], stdout); err != nil {
			fmt.Fprintf(stderr, "error: %v\n", err)
			return 1
		}
		return 0
	case "download":
		if err := runDownload(args[1:], stdout); err != nil {
			fmt.Fprintf(stderr, "error: %v\n", err)
			return 1
		}
		return 0
	case "help", "-h", "--help":
		printUsage(stdout)
		return 0
	default:
		fmt.Fprintf(stderr, "error: unknown command %q\n\n", args[0])
		printUsage(stderr)
		return 2
	}
}

func printUsage(w io.Writer) {
	fmt.Fprint(w, `dukascopy-data commands:
  instruments  Search Dukascopy instruments
  download     Download historical data and save it as CSV

examples:
  dukascopy-data instruments --query xauusd
  dukascopy-data download --symbol xauusd --granularity minute --from 2024-01-02T00:00:00Z --to 2024-01-02T01:00:00Z --output ./data/xauusd.csv --simple
  dukascopy-data download --symbol xauusd --granularity hour --from 2024-01-01T00:00:00Z --to 2024-01-02T00:00:00Z --output ./data/xauusd-full.csv --full
`)
}

func runInstruments(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("instruments", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	query := fs.String("query", "", "instrument search text such as xauusd or eur/usd")
	limit := fs.Int("limit", 20, "maximum number of rows to print")
	baseURL := fs.String("base-url", readBaseURL(), "Dukascopy API base URL")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*query) == "" {
		return errors.New("--query is required")
	}
	if *limit <= 0 {
		return errors.New("--limit must be greater than 0")
	}

	client := dukascopy.NewClient(*baseURL, defaultTimeout)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	instruments, err := client.ListInstruments(ctx)
	if err != nil {
		return err
	}

	matches := dukascopy.FilterInstruments(instruments, *query, *limit)
	if len(matches) == 0 {
		return fmt.Errorf("no instruments found for %q", *query)
	}

	for _, instrument := range matches {
		fmt.Fprintf(stdout, "%s\t%s\t%s\n", instrument.Name, instrument.Code, instrument.Description)
	}

	return nil
}

func runDownload(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("download", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	symbol := fs.String("symbol", "", "instrument symbol such as xauusd or eur/usd")
	granularity := fs.String("granularity", "minute", "tick, minute, hour, or day")
	side := fs.String("side", "bid", "bid or ask")
	simpleOutput := fs.Bool("simple", false, "write the reduced CSV column set")
	fullOutput := fs.Bool("full", false, "write the full CSV column set with bid/ask columns")
	fromValue := fs.String("from", "", "inclusive RFC3339 start timestamp")
	toValue := fs.String("to", "", "exclusive RFC3339 end timestamp")
	outputPath := fs.String("output", "", "target CSV path")
	baseURL := fs.String("base-url", readBaseURL(), "Dukascopy API base URL")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if strings.TrimSpace(*symbol) == "" {
		return errors.New("--symbol is required")
	}
	if strings.TrimSpace(*fromValue) == "" {
		return errors.New("--from is required")
	}
	if strings.TrimSpace(*toValue) == "" {
		return errors.New("--to is required")
	}
	if strings.TrimSpace(*outputPath) == "" {
		return errors.New("--output is required")
	}

	from, err := time.Parse(time.RFC3339, *fromValue)
	if err != nil {
		return fmt.Errorf("--from must be RFC3339: %w", err)
	}

	to, err := time.Parse(time.RFC3339, *toValue)
	if err != nil {
		return fmt.Errorf("--to must be RFC3339: %w", err)
	}

	if !from.Before(to) {
		return errors.New("--from must be earlier than --to")
	}
	if *simpleOutput && *fullOutput {
		return errors.New("--simple and --full cannot be used together")
	}

	profile := csvout.ProfileSimple
	if *fullOutput {
		profile = csvout.ProfileFull
	}

	request := dukascopy.DownloadRequest{
		Symbol:      *symbol,
		Granularity: dukascopy.Granularity(*granularity),
		Side:        dukascopy.PriceSide(*side),
		From:        from.UTC(),
		To:          to.UTC(),
	}

	client := dukascopy.NewClient(*baseURL, defaultTimeout)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	result, err := client.Download(ctx, request)
	if err != nil {
		return err
	}

	if result.Kind == dukascopy.ResultKindTick {
		if err := csvout.WriteTicks(*outputPath, result.Instrument, profile, result.Ticks); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "wrote %d ticks to %s\n", len(result.Ticks), *outputPath)
		return nil
	}

	if profile == csvout.ProfileFull {
		instrument, bidBars, err := client.DownloadBarsForSide(ctx, request, dukascopy.PriceSideBid)
		if err == nil {
			_, askBars, askErr := client.DownloadBarsForSide(ctx, request, dukascopy.PriceSideAsk)
			if askErr == nil {
				if err := csvout.WriteBars(*outputPath, instrument, profile, nil, bidBars, askBars); err != nil {
					return err
				}
				fmt.Fprintf(stdout, "wrote %d bars to %s\n", len(bidBars), *outputPath)
				return nil
			}
		}

		tickRequest := request
		tickRequest.Granularity = dukascopy.GranularityTick
		tickResult, err := client.Download(ctx, tickRequest)
		if err != nil {
			return err
		}

		bidBars, err = dukascopy.AggregateTicksToBars(tickResult.Ticks, request.Granularity, dukascopy.PriceSideBid, request.From, request.To)
		if err != nil {
			return err
		}
		askBars, err := dukascopy.AggregateTicksToBars(tickResult.Ticks, request.Granularity, dukascopy.PriceSideAsk, request.From, request.To)
		if err != nil {
			return err
		}
		if err := csvout.WriteBars(*outputPath, tickResult.Instrument, profile, nil, bidBars, askBars); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "wrote %d bars to %s\n", len(bidBars), *outputPath)
		return nil
	}

	if err := csvout.WriteBars(*outputPath, result.Instrument, profile, result.Bars, nil, nil); err != nil {
		return err
	}

	fmt.Fprintf(stdout, "wrote %d bars to %s\n", len(result.Bars), *outputPath)
	return nil
}

func readBaseURL() string {
	if value := strings.TrimSpace(os.Getenv("DUKASCOPY_API_BASE_URL")); value != "" {
		return value
	}
	return defaultBaseURL
}
