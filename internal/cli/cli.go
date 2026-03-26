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
	colorReset     = "\033[0m"
	colorBold      = "\033[1m"
	colorRed       = "\033[31m"
	colorGreen     = "\033[32m"
	colorCyan      = "\033[36m"
	colorYellow    = "\033[33m"
)

func Run(args []string, stdout io.Writer, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stderr)
		return 2
	}

	switch args[0] {
	case "list-timeframes", "--list-timeframes":
		printTimeframes(stdout)
		return 0
	case "instruments":
		if err := runInstruments(args[1:], stdout); err != nil {
			fmt.Fprintf(stderr, "%serror:%s %v\n", colorize(colorRed), colorize(colorReset), err)
			return 1
		}
		return 0
	case "download":
		if err := runDownload(args[1:], stdout); err != nil {
			fmt.Fprintf(stderr, "%serror:%s %v\n", colorize(colorRed), colorize(colorReset), err)
			return 1
		}
		return 0
	case "help", "-h", "--help":
		printUsage(stdout)
		return 0
	default:
		fmt.Fprintf(stderr, "%serror:%s unknown command %q\n\n", colorize(colorRed), colorize(colorReset), args[0])
		printUsage(stderr)
		return 2
	}
}

func printUsage(w io.Writer) {
	fmt.Fprintf(w, "%sdukascopy-data%s\n\n", colorize(colorBold), colorize(colorReset))
	fmt.Fprintf(w, "%sCommands%s\n", colorize(colorCyan), colorize(colorReset))
	fmt.Fprint(w, `  instruments  Search Dukascopy instruments
  download     Download historical data and save it as CSV
  list-timeframes  Print supported timeframe values

examples:
  dukascopy-data instruments --query xauusd
  dukascopy-data --list-timeframes
  dukascopy-data download --symbol xauusd --timeframe m1 --from 2024-01-02T00:00:00Z --to 2024-01-02T01:00:00Z --output ./data/xauusd.csv --simple
  dukascopy-data download --symbol xauusd --timeframe h1 --from 2024-01-01T00:00:00Z --to 2024-01-02T00:00:00Z --output ./data/xauusd-full.csv --full
  dukascopy-data download --symbol xauusd --timeframe m1 --from 2024-01-02T00:00:00Z --to 2024-01-02T01:00:00Z --output ./data/xauusd-custom.csv --custom-columns timestamp,bid_open,ask_open,volume
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

	printInstrumentTable(stdout, matches)
	return nil
}

func runDownload(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("download", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	symbol := fs.String("symbol", "", "instrument symbol such as xauusd or eur/usd")
	timeframe := fs.String("timeframe", "m1", "tick, m1, m3, m5, m15, m30, h1, h4, d1, w1, mn1")
	granularity := fs.String("granularity", "", "deprecated alias for --timeframe")
	side := fs.String("side", "bid", "bid or ask")
	simpleOutput := fs.Bool("simple", false, "write the reduced CSV column set")
	fullOutput := fs.Bool("full", false, "write the full CSV column set with bid/ask columns")
	customColumns := fs.String("custom-columns", "", "comma-separated CSV column list")
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
	if strings.TrimSpace(*customColumns) != "" && (*simpleOutput || *fullOutput) {
		return errors.New("--custom-columns cannot be combined with --simple or --full")
	}

	timeframeValue := strings.TrimSpace(*timeframe)
	if strings.TrimSpace(*granularity) != "" {
		timeframeValue = strings.TrimSpace(*granularity)
	}

	normalizedTimeframe := dukascopy.Granularity(timeframeValue)
	profile := csvout.ProfileSimple
	if *fullOutput {
		profile = csvout.ProfileFull
	}

	barColumns := csvout.BarColumnsForProfile(profile)
	tickColumns := csvout.TickColumnsForProfile(profile)

	request := dukascopy.DownloadRequest{
		Symbol:      *symbol,
		Granularity: normalizedTimeframe,
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

	if strings.TrimSpace(*customColumns) != "" {
		if result.Kind == dukascopy.ResultKindTick {
			tickColumns, err = csvout.ParseTickColumns(*customColumns)
			if err != nil {
				return err
			}
		} else {
			barColumns, err = csvout.ParseBarColumns(*customColumns)
			if err != nil {
				return err
			}
		}
	}

	if result.Kind == dukascopy.ResultKindTick {
		if err := csvout.WriteTicks(*outputPath, result.Instrument, tickColumns, result.Ticks); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "%swrote%s %d ticks to %s\n", colorize(colorGreen), colorize(colorReset), len(result.Ticks), *outputPath)
		return nil
	}

	if csvout.BarColumnsNeedBidAsk(barColumns) {
		instrument, bidBars, askBars, err := loadBidAskBars(ctx, client, request)
		if err != nil {
			return err
		}
		if err := csvout.WriteBars(*outputPath, instrument, barColumns, nil, bidBars, askBars); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "%swrote%s %d bars to %s\n", colorize(colorGreen), colorize(colorReset), len(bidBars), *outputPath)
		return nil
	}

	if err := csvout.WriteBars(*outputPath, result.Instrument, barColumns, result.Bars, nil, nil); err != nil {
		return err
	}

	fmt.Fprintf(stdout, "%swrote%s %d bars to %s\n", colorize(colorGreen), colorize(colorReset), len(result.Bars), *outputPath)
	return nil
}

func loadBidAskBars(ctx context.Context, client *dukascopy.Client, request dukascopy.DownloadRequest) (dukascopy.Instrument, []dukascopy.Bar, []dukascopy.Bar, error) {
	instrument, bidBars, bidErr := client.DownloadBarsForSide(ctx, request, dukascopy.PriceSideBid)
	if bidErr == nil {
		_, askBars, askErr := client.DownloadBarsForSide(ctx, request, dukascopy.PriceSideAsk)
		if askErr == nil {
			return instrument, bidBars, askBars, nil
		}
	}

	tickRequest := request
	tickRequest.Granularity = dukascopy.GranularityTick
	tickResult, err := client.Download(ctx, tickRequest)
	if err != nil {
		return dukascopy.Instrument{}, nil, nil, err
	}

	bidBars, err = dukascopy.AggregateTicksToBars(tickResult.Ticks, request.Granularity, dukascopy.PriceSideBid, request.From, request.To)
	if err != nil {
		return dukascopy.Instrument{}, nil, nil, err
	}
	askBars, err := dukascopy.AggregateTicksToBars(tickResult.Ticks, request.Granularity, dukascopy.PriceSideAsk, request.From, request.To)
	if err != nil {
		return dukascopy.Instrument{}, nil, nil, err
	}
	return tickResult.Instrument, bidBars, askBars, nil
}

func readBaseURL() string {
	if value := strings.TrimSpace(os.Getenv("DUKASCOPY_API_BASE_URL")); value != "" {
		return value
	}
	return defaultBaseURL
}

func colorize(code string) string {
	if strings.TrimSpace(os.Getenv("NO_COLOR")) != "" {
		return ""
	}
	return code
}

func printTimeframes(w io.Writer) {
	fmt.Fprintf(w, "%sSupported timeframes%s\n", colorize(colorCyan), colorize(colorReset))
	fmt.Fprint(w, `  tick  raw tick quotes
  m1    native 1-minute bars
  m3    aggregated from m1
  m5    aggregated from m1
  m15   aggregated from m1
  m30   aggregated from m1
  h1    native 1-hour bars
  h4    aggregated from h1
  d1    native 1-day bars
  w1    aggregated from d1
  mn1   aggregated from d1
`)
}

func printInstrumentTable(w io.Writer, instruments []dukascopy.Instrument) {
	nameWidth := maxStringWidth("NAME", instrumentFieldLengths(instruments, func(instrument dukascopy.Instrument) string {
		return instrument.Name
	}))
	codeWidth := maxStringWidth("CODE", instrumentFieldLengths(instruments, func(instrument dukascopy.Instrument) string {
		return instrument.Code
	}))

	fmt.Fprintf(
		w,
		"%s%-*s  %-*s  %s%s\n",
		colorize(colorCyan),
		nameWidth,
		"NAME",
		codeWidth,
		"CODE",
		"DESCRIPTION",
		colorize(colorReset),
	)

	fmt.Fprintf(
		w,
		"%s%s  %s  %s%s\n",
		colorize(colorYellow),
		strings.Repeat("-", nameWidth),
		strings.Repeat("-", codeWidth),
		strings.Repeat("-", maxInt(11, 24)),
		colorize(colorReset),
	)

	for _, instrument := range instruments {
		fmt.Fprintf(
			w,
			"%-*s  %-*s  %s\n",
			nameWidth,
			instrument.Name,
			codeWidth,
			instrument.Code,
			instrument.Description,
		)
	}
}

func instrumentFieldLengths(instruments []dukascopy.Instrument, selector func(dukascopy.Instrument) string) []int {
	lengths := make([]int, 0, len(instruments))
	for _, instrument := range instruments {
		lengths = append(lengths, len(selector(instrument)))
	}
	return lengths
}

func maxStringWidth(defaultLabel string, lengths []int) int {
	maxWidth := len(defaultLabel)
	for _, length := range lengths {
		if length > maxWidth {
			maxWidth = length
		}
	}
	return maxWidth
}

func maxInt(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
