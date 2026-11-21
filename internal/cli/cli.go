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
	case "help", "-h", "--help":
		printUsage(stdout)
		return 0
	}

	command, ok := subcommands[args[0]]
	if !ok {
		fmt.Fprintf(stderr, "%serror:%s unknown command %q\n\n", colorize(colorRed), colorize(colorReset), args[0])
		printUsage(stderr)
		return 2
	}
	return exitCodeFor(command(args[1:], stdout, stderr), stderr)
}

// subcommandFunc runs one subcommand with its arguments.
type subcommandFunc func(args []string, stdout io.Writer, stderr io.Writer) error

// subcommands maps each command name to its implementation.
var subcommands = map[string]subcommandFunc{
	"instruments": func(args []string, stdout io.Writer, _ io.Writer) error { return runInstruments(args, stdout) },
	"stats":       func(args []string, stdout io.Writer, _ io.Writer) error { return runStats(args, stdout) },
	"manifest":    func(args []string, stdout io.Writer, _ io.Writer) error { return runManifest(args, stdout) },
	"download":    runDownload,
	"sync":        runSync,
	"live":        runLiveStream,
	"db-load":     runDBLoad,
	"watch":       runWatch,
}

// exitCodeFor reports err and maps it to the process exit code: 0 for
// success and --help, 2 for command-line (flag) errors, 1 otherwise.
func exitCodeFor(err error, stderr io.Writer) int {
	if err == nil || errors.Is(err, flag.ErrHelp) {
		return 0
	}
	fmt.Fprintf(stderr, "%serror:%s %v\n", colorize(colorRed), colorize(colorReset), err)
	if isFlagParseError(err) {
		return 2
	}
	return 1
}

// flagParseErrorPrefixes are the messages the flag package produces when
// command-line parsing fails.
var flagParseErrorPrefixes = []string{
	"flag provided but not defined",
	"bad flag syntax",
	"flag needs an argument",
	"invalid boolean value",
	"invalid boolean flag",
	"invalid value",
}

func isFlagParseError(err error) bool {
	message := err.Error()
	for _, prefix := range flagParseErrorPrefixes {
		if strings.HasPrefix(message, prefix) {
			return true
		}
	}
	return false
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
