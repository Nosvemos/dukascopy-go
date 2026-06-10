package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/csvout"
)

func runSync(args []string, stdout io.Writer, stderr io.Writer) error {
	fs := flag.NewFlagSet("sync", flag.ContinueOnError)
	fs.SetOutput(stdout)
	fs.Usage = func() {
		fmt.Fprintf(stdout, "%ssync:%s Sync an existing dataset file up to the present moment\n\n", colorize(colorCyan), colorize(colorReset))
		fmt.Fprint(stdout, "Usage:\n  dukascopy-go sync [options]\n\nOptions:\n")
		fs.PrintDefaults()
		fmt.Fprint(stdout, "\nExamples:\n  dukascopy-go sync --symbol eurusd --output ./eurusd_m1.csv\n  dukascopy-go sync --symbol eurusd,gbpusd --output ./data/\n  dukascopy-go sync --symbol eurusd --from 2024-01-01 --output ./eurusd_m1.csv\n")
	}

	symbol := fs.String("symbol", "", "instrument symbol such as xauusd or eur/usd")
	outputPath := fs.String("output", "", "target CSV path")
	fromValue := fs.String("from", "", "fallback inclusive start timestamp if file does not exist")
	lastValue := fs.String("last", "", "fallback duration (e.g. 30d, 1y) if file does not exist")
	toValue := fs.String("to", "", "custom end timestamp (defaults to now)")

	// Parse flags so we can split args easily or forward them
	if err := fs.Parse(args); err != nil {
		return err
	}

	if strings.TrimSpace(*symbol) == "" {
		return errors.New("--symbol is required")
	}
	if strings.TrimSpace(*outputPath) == "" {
		return errors.New("--output is required")
	}

	// Support multi-symbol batch sync out of the box!
	if strings.Contains(*symbol, ",") {
		symbols := strings.Split(*symbol, ",")
		fmt.Fprintf(stdout, "%sbatch%s syncing %d symbols...\n", colorize(colorCyan), colorize(colorReset), len(symbols))
		for _, sym := range symbols {
			sym = strings.TrimSpace(sym)
			if sym == "" {
				continue
			}

			formattedOutput := formatMultiSymbolOutputPath(*outputPath, sym)

			// Construct nextArgs replacing symbol and output
			var nextArgs []string
			for i := 0; i < len(args); {
				arg := args[i]
				if arg == "--symbol" || arg == "-symbol" || arg == "--output" || arg == "-output" {
					i += 2
				} else if strings.HasPrefix(arg, "--symbol=") || strings.HasPrefix(arg, "-symbol=") || strings.HasPrefix(arg, "--output=") || strings.HasPrefix(arg, "-output=") {
					i += 1
				} else {
					nextArgs = append(nextArgs, arg)
					i++
				}
			}
			nextArgs = append(nextArgs, "--symbol", sym, "--output", formattedOutput)

			fmt.Fprintf(stdout, "%sbatch%s starting sync for %s -> %s\n", colorize(colorCyan), colorize(colorReset), sym, formattedOutput)
			if err := runSync(nextArgs, stdout, stderr); err != nil {
				return fmt.Errorf("sync for symbol %s failed: %w", sym, err)
			}
		}
		return nil
	}

	// Single symbol sync!
	targetPath := *outputPath

	// Check if the target file exists
	_, statErr := os.Stat(targetPath)
	exists := statErr == nil

	var from, to time.Time
	var resume bool

	if exists {
		if strings.HasSuffix(strings.ToLower(targetPath), ".parquet") {
			return errors.New("syncing parquet files is not supported; use partition mode for durable long-range parquet downloads")
		}

		// Target file exists! Inspect it to get the last timestamp
		state, err := csvout.InspectExistingCSV(targetPath)
		if err != nil {
			if os.IsNotExist(err) {
				exists = false
			} else {
				return fmt.Errorf("inspect existing CSV %s failed: %w", targetPath, err)
			}
		}

		if exists {
			if !state.HasRows {
				// If file has header but no rows, we treat it as starting from fallback --from or --last
				if strings.TrimSpace(*fromValue) == "" && strings.TrimSpace(*lastValue) == "" {
					return fmt.Errorf("target file %s exists but is empty; please provide fallback --from or --last to initialize", targetPath)
				}
			} else {
				from = state.LastTime
				resume = true
			}
		}
	}

	// If it doesn't exist or is empty and we have fallback
	if !exists || from.IsZero() {
		if strings.TrimSpace(*fromValue) == "" && strings.TrimSpace(*lastValue) == "" {
			return fmt.Errorf("target file %s does not exist; please provide fallback --from or --last to initialize", targetPath)
		}
	}

	// Determine custom end timestamp or default to now
	to = time.Now().UTC()
	if strings.TrimSpace(*toValue) != "" {
		parsedTo, err := ParseFlexibleTime(*toValue)
		if err != nil {
			return fmt.Errorf("--to %w", err)
		}
		to = parsedTo.UTC()
	}

	// Route to runDownload by synthesizing the argument slice
	var downloadArgs []string

	// Forward all flags that were passed, but filter out symbol, output, from, last, to
	for i := 0; i < len(args); {
		arg := args[i]
		if arg == "--symbol" || arg == "-symbol" ||
			arg == "--output" || arg == "-output" ||
			arg == "--from" || arg == "-from" ||
			arg == "--last" || arg == "-last" ||
			arg == "--to" || arg == "-to" ||
			arg == "--resume" || arg == "-resume" {
			i += 2
		} else if strings.HasPrefix(arg, "--symbol=") || strings.HasPrefix(arg, "-symbol=") ||
			strings.HasPrefix(arg, "--output=") || strings.HasPrefix(arg, "-output=") ||
			strings.HasPrefix(arg, "--from=") || strings.HasPrefix(arg, "-from=") ||
			strings.HasPrefix(arg, "--last=") || strings.HasPrefix(arg, "-last=") ||
			strings.HasPrefix(arg, "--to=") || strings.HasPrefix(arg, "-to=") ||
			strings.HasPrefix(arg, "--resume=") || strings.HasPrefix(arg, "-resume=") {
			i += 1
		} else {
			downloadArgs = append(downloadArgs, arg)
			i++
		}
	}

	// Append output and symbol
	downloadArgs = append(downloadArgs, "--symbol", *symbol, "--output", targetPath)

	// Append resume if true
	if resume {
		downloadArgs = append(downloadArgs, "--resume")
		fromStr := from.Format(time.RFC3339Nano)
		downloadArgs = append(downloadArgs, "--from", fromStr)
	} else {
		// Use fallback start parameters
		if strings.TrimSpace(*fromValue) != "" {
			downloadArgs = append(downloadArgs, "--from", *fromValue)
		} else if strings.TrimSpace(*lastValue) != "" {
			downloadArgs = append(downloadArgs, "--last", *lastValue)
		}
	}

	// Append custom/default end timestamp
	downloadArgs = append(downloadArgs, "--to", to.Format(time.RFC3339Nano))

	fromLabel := "none (fresh)"
	if !from.IsZero() {
		fromLabel = from.Format(time.RFC3339Nano)
	}
	fmt.Fprintf(stdout, "%ssyncing%s %s in-place starting from: %s -> to: %s\n", colorize(colorCyan), colorize(colorReset), *symbol, fromLabel, to.Format(time.RFC3339Nano))

	// Invoke standard download workflow!
	return runDownload(downloadArgs, stdout, stderr)
}
