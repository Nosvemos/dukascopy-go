package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"

	_ "time/tzdata"
)

const (
	defaultBaseURL     = "https://jetta.dukascopy.com"
	defaultHTTPTimeout = 30 * time.Second
	colorReset         = "\033[0m"
	colorBold          = "\033[1m"
	colorRed           = "\033[31m"
	colorGreen         = "\033[32m"
	colorCyan          = "\033[36m"
	colorYellow        = "\033[33m"
)

func Run(args []string, stdout io.Writer, stderr io.Writer) int {
	loadedArgs, err := loadActiveConfig(args)
	if err != nil {
		fmt.Fprintf(stderr, "%serror:%s %v\n", colorize(colorRed), colorize(colorReset), err)
		return 1
	}
	args = loadedArgs

	if len(args) == 0 {
		if isInteractiveTerminal(stdout) {
			return runWizard(stdout, stderr)
		}
		printUsage(stderr)
		return 2
	}

	switch args[0] {
	case "version", "--version", "-v":
		printVersion(stdout)
		return 0
	case "list-timeframes", "--list-timeframes":
		printTimeframes(stdout)
		return 0
	case "instruments":
		if err := runInstruments(args[1:], stdout); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return 0
			}
			fmt.Fprintf(stderr, "%serror:%s %v\n", colorize(colorRed), colorize(colorReset), err)
			return 1
		}
		return 0
	case "stats":
		if err := runStats(args[1:], stdout); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return 0
			}
			fmt.Fprintf(stderr, "%serror:%s %v\n", colorize(colorRed), colorize(colorReset), err)
			return 1
		}
		return 0
	case "manifest":
		if err := runManifest(args[1:], stdout); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return 0
			}
			fmt.Fprintf(stderr, "%serror:%s %v\n", colorize(colorRed), colorize(colorReset), err)
			return 1
		}
		return 0
	case "download":
		if err := runDownload(args[1:], stdout, stderr); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return 0
			}
			fmt.Fprintf(stderr, "%serror:%s %v\n", colorize(colorRed), colorize(colorReset), err)
			return 1
		}
		return 0
	case "sync":
		if err := runSync(args[1:], stdout, stderr); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return 0
			}
			fmt.Fprintf(stderr, "%serror:%s %v\n", colorize(colorRed), colorize(colorReset), err)
			return 1
		}
		return 0
	case "live":
		if err := runLiveStream(args[1:], stdout, stderr); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return 0
			}
			fmt.Fprintf(stderr, "%serror:%s %v\n", colorize(colorRed), colorize(colorReset), err)
			return 1
		}
		return 0
	case "db-load":
		if err := runDBLoad(args[1:], stdout, stderr); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return 0
			}
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

func runInstruments(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("instruments", flag.ContinueOnError)
	fs.SetOutput(stdout)
	fs.Usage = func() {
		fmt.Fprintf(stdout, "%sinstruments:%s Search offline/online Dukascopy instruments catalog\n\n", colorize(colorCyan), colorize(colorReset))
		fmt.Fprint(stdout, "Usage:\n  dukascopy-go instruments [options]\n\nOptions:\n")
		fs.PrintDefaults()
		fmt.Fprint(stdout, "\nExamples:\n  dukascopy-go instruments\n  dukascopy-go instruments --query eur/usd\n  dukascopy-go instruments --query try --limit -1\n  dukascopy-go instruments --query gold --update\n")
	}

	query := fs.String("query", "", "instrument search text such as xauusd or eur/usd")
	limit := fs.Int("limit", 20, "maximum number of rows to print (set to -1 for unlimited)")
	jsonOutput := fs.Bool("json", false, "print matching instruments as JSON")
	baseURL := fs.String("base-url", readBaseURL(), "Dukascopy API base URL")
	update := fs.Bool("update", false, "force update the local instruments cache from online catalog")
	if err := fs.Parse(args); err != nil {
		return err
	}
	applyInstrumentConfigDefaults(fs, limit, baseURL)
	if *limit <= 0 && *limit != -1 {
		return errors.New("--limit must be greater than 0 (or set to -1 for unlimited)")
	}

	client, err := dukascopy.NewClient(*baseURL, defaultHTTPTimeout)
	if err != nil {
		return err
	}
	client = client.WithForceUpdate(*update)
	ctx, cancel := context.WithTimeout(context.Background(), defaultHTTPTimeout)
	defer cancel()

	instruments, err := client.ListInstruments(ctx)
	if err != nil {
		return err
	}

	limitVal := *limit
	if limitVal == -1 {
		limitVal = 99999
	}

	allMatches := dukascopy.FilterInstruments(instruments, *query, 99999)
	matches := dukascopy.FilterInstruments(instruments, *query, limitVal)
	if len(matches) == 0 {
		return fmt.Errorf("no instruments found for %q", *query)
	}

	if *jsonOutput {
		data, err := json.MarshalIndent(matches, "", "  ")
		if err != nil {
			return err
		}
		fmt.Fprintln(stdout, string(data))
		return nil
	}

	printInstrumentTable(stdout, matches)

	if *limit > 0 && len(allMatches) > *limit {
		fmt.Fprintf(stdout, "\n%sShowing %d of %d matching instruments. Use --limit -1 to show all.%s\n", colorize(colorYellow), *limit, len(allMatches), colorize(colorReset))
	}
	return nil
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
