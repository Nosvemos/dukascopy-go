package cli

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/Nosvemos/dukascopy-go/internal/checkpoint"
)

func runManifestInspect(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("manifest inspect", flag.ContinueOnError)
	fs.SetOutput(stdout)
	fs.Usage = func() {
		fmt.Fprintf(stdout, "%smanifest inspect:%s Show checkpoint manifest summary\n\nUsage:\n  dukascopy-go manifest inspect [options]\n\nOptions:\n", colorize(colorCyan), colorize(colorReset))
		fs.PrintDefaults()
	}

	manifestPath := fs.String("manifest", "", "checkpoint manifest path")
	outputPath := fs.String("output", "", "output CSV path used to derive <output>.manifest.json")
	jsonOutput := fs.Bool("json", false, "print the raw manifest as JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}

	path, err := resolveManifestPath(strings.TrimSpace(*manifestPath), strings.TrimSpace(*outputPath))
	if err != nil {
		return err
	}

	var printer *operationPrinter
	if !*jsonOutput && isInteractiveTerminal(stdout) {
		printer = newOperationPrinter(stdout)
		printer.SetCommand("manifest inspect", path)
		printer.SetStatus("loading manifest")
		printer.SetPhase("checkpoint load")
		defer printer.Finish()
	}

	manifest, err := checkpoint.Load(path)
	if err != nil {
		if printer != nil {
			printer.SetStatus("failed")
		}
		return err
	}
	if printer != nil {
		printer.SetMetric("parts", formatCount(len(manifest.Parts)))
		printer.SetMetric("partition", defaultString(manifest.Partition, "-"))
		printer.SetStatus("summary ready")
		printer.Finish()
		printer = nil
	}

	if *jsonOutput {
		data, err := json.MarshalIndent(manifest, "", "  ")
		if err != nil {
			return err
		}
		fmt.Fprintln(stdout, string(data))
		return nil
	}

	fmt.Fprintf(stdout, "%sManifest%s\n", colorize(colorCyan), colorize(colorReset))
	fmt.Fprintf(stdout, "path:        %s\n", path)
	fmt.Fprintf(stdout, "output:      %s\n", manifest.OutputPath)
	fmt.Fprintf(stdout, "symbol:      %s\n", manifest.Symbol)
	fmt.Fprintf(stdout, "timeframe:   %s\n", manifest.Timeframe)
	fmt.Fprintf(stdout, "partition:   %s\n", manifest.Partition)
	fmt.Fprintf(stdout, "completed:   %t\n", manifest.Completed)
	fmt.Fprintf(stdout, "parts:       %d total, %d completed, %d failed, %d pending, %d running\n",
		manifest.Summary.TotalParts,
		manifest.Summary.CompletedParts,
		manifest.Summary.FailedParts,
		manifest.Summary.PendingParts,
		manifest.Summary.RunningParts,
	)
	fmt.Fprintf(stdout, "part rows:   %d\n", manifest.Summary.TotalRows)
	if manifest.FinalOutput != nil {
		fmt.Fprintf(stdout, "output rows: %d\n", manifest.FinalOutput.Rows)
		fmt.Fprintf(stdout, "output sha:  %s\n", manifest.FinalOutput.SHA256)
	}

	printManifestParts(stdout, manifest)
	return nil
}

func resolveManifestPath(manifestPath string, outputPath string) (string, error) {
	switch {
	case manifestPath != "" && outputPath != "":
		return "", errors.New("--manifest and --output cannot be used together")
	case manifestPath != "":
		return manifestPath, nil
	case outputPath != "":
		return checkpoint.DefaultManifestPath(outputPath), nil
	default:
		return "", errors.New("either --manifest or --output is required")
	}
}

func printManifestParts(w io.Writer, manifest checkpoint.Manifest) {
	fmt.Fprintf(w, "\n%sParts%s\n", colorize(colorCyan), colorize(colorReset))
	fmt.Fprintf(w, "%-26s %-10s %-6s %s\n", "ID", "STATUS", "ROWS", "FILE")
	for _, part := range manifest.Parts {
		fmt.Fprintf(w, "%-26s %-10s %-6d %s\n", part.ID, part.Status, part.Rows, part.File)
	}
}
