package cli

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/csvout"
)

func runStats(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("stats", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	inputPath := fs.String("input", "", "CSV, CSV.GZ, or Parquet file path")
	jsonOutput := fs.Bool("json", false, "print stats as JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*inputPath) == "" {
		return errors.New("--input is required")
	}

	var printer *operationPrinter
	if !*jsonOutput && isInteractiveTerminal(stdout) {
		printer = newOperationPrinter(stdout)
		printer.SetCommand("stats", *inputPath)
		printer.SetStatus("scanning dataset")
		printer.SetPhase("inspect csv")
		defer printer.Finish()
	}

	stats, err := csvout.InspectCSV(*inputPath)
	if err != nil {
		if printer != nil {
			printer.SetStatus("failed")
		}
		return err
	}
	if printer != nil {
		printer.SetMetric("format", stats.Format)
		printer.SetMetric("rows", formatCount(stats.Rows))
		if stats.HasTimestamp {
			printer.SetMetric("range", stats.FirstTimestamp.Format(time.RFC3339)+" -> "+stats.LastTimestamp.Format(time.RFC3339))
			printer.SetMetric("suspicious", formatCount(stats.SuspiciousGapCount))
		}
		printer.SetStatus("summary ready")
		printer.Finish()
		printer = nil
	}

	if *jsonOutput {
		data, err := json.MarshalIndent(stats, "", "  ")
		if err != nil {
			return err
		}
		fmt.Fprintln(stdout, string(data))
		return nil
	}

	fmt.Fprintf(stdout, "%sStats%s\n", colorize(colorCyan), colorize(colorReset))
	fmt.Fprintf(stdout, "path:              %s\n", stats.Path)
	fmt.Fprintf(stdout, "format:            %s\n", stats.Format)
	fmt.Fprintf(stdout, "compressed:        %t\n", stats.Compressed)
	fmt.Fprintf(stdout, "rows:              %d\n", stats.Rows)
	fmt.Fprintf(stdout, "columns:           %s\n", strings.Join(stats.Columns, ","))
	fmt.Fprintf(stdout, "has timestamp:     %t\n", stats.HasTimestamp)
	if stats.HasTimestamp {
		fmt.Fprintf(stdout, "first timestamp:   %s\n", stats.FirstTimestamp.Format("2006-01-02T15:04:05.999999999Z07:00"))
		fmt.Fprintf(stdout, "last timestamp:    %s\n", stats.LastTimestamp.Format("2006-01-02T15:04:05.999999999Z07:00"))
		fmt.Fprintf(stdout, "inferred frame:    %s\n", stats.InferredTimeframe)
		fmt.Fprintf(stdout, "expected interval: %s\n", defaultString(stats.ExpectedInterval, "unknown"))
		fmt.Fprintf(stdout, "gap count:         %d\n", stats.GapCount)
		fmt.Fprintf(stdout, "missing intervals: %d\n", stats.MissingIntervals)
		fmt.Fprintf(stdout, "largest gap:       %s\n", defaultString(stats.LargestGap, "none"))
		fmt.Fprintf(stdout, "expected gaps:     %d\n", stats.ExpectedGapCount)
		fmt.Fprintf(stdout, "expected missing:  %d\n", stats.ExpectedMissingIntervals)
		fmt.Fprintf(stdout, "expected largest:  %s\n", defaultString(stats.ExpectedLargestGap, "none"))
		fmt.Fprintf(stdout, "suspicious gaps:   %d\n", stats.SuspiciousGapCount)
		fmt.Fprintf(stdout, "suspicious miss:   %d\n", stats.SuspiciousMissingIntervals)
		fmt.Fprintf(stdout, "suspicious large:  %s\n", defaultString(stats.SuspiciousLargestGap, "none"))
	}
	fmt.Fprintf(stdout, "duplicate rows:    %d\n", stats.DuplicateRows)
	fmt.Fprintf(stdout, "duplicate stamps:  %d\n", stats.DuplicateStamps)
	fmt.Fprintf(stdout, "out of order:      %d\n", stats.OutOfOrderRows)
	return nil
}

func defaultString(value string, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
