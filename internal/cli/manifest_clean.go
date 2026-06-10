package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/Nosvemos/dukascopy-go/internal/checkpoint"
	"github.com/Nosvemos/dukascopy-go/pkg/csvout"
)

func runManifestCleanDuplicates(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("manifest clean-duplicates", flag.ContinueOnError)
	fs.SetOutput(stdout)
	fs.Usage = func() {
		fmt.Fprintf(stdout, "%smanifest clean-duplicates:%s Clean duplicate rows and sort chronologically\n\nUsage:\n  dukascopy-go manifest clean-duplicates [options]\n\nOptions:\n", colorize(colorCyan), colorize(colorReset))
		fs.PrintDefaults()
	}

	manifestPath := fs.String("manifest", "", "checkpoint manifest path")
	outputPath := fs.String("output", "", "output CSV/Parquet path to clean")
	if err := fs.Parse(args); err != nil {
		return err
	}

	targetPath := strings.TrimSpace(*outputPath)
	if targetPath == "" && strings.TrimSpace(*manifestPath) != "" {
		manifest, err := checkpoint.Load(strings.TrimSpace(*manifestPath))
		if err == nil {
			targetPath = manifest.OutputPath
		}
	}
	if targetPath == "" {
		return errors.New("either --output or a valid --manifest is required to clean duplicates")
	}

	stats, err := csvout.InspectCSV(targetPath)
	if err != nil {
		return err
	}

	if stats.DuplicateRows == 0 && stats.DuplicateStamps == 0 && stats.OutOfOrderRows == 0 {
		fmt.Fprintf(stdout, "%sclean%s no duplicate or out-of-order rows detected in %s\n", colorize(colorGreen), colorize(colorReset), targetPath)
		return nil
	}

	fmt.Fprintf(stdout, "%sclean%s removing duplicates and re-ordering rows for %s...\n", colorize(colorCyan), colorize(colorReset), targetPath)

	cleanedCount, err := csvout.CleanDuplicates(targetPath)
	if err != nil {
		return err
	}

	fmt.Fprintf(stdout, "%sclean%s successfully removed %d duplicate/anomaly rows in %s\n", colorize(colorGreen), colorize(colorReset), cleanedCount, targetPath)
	return nil
}
