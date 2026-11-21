package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

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
	explicitManifest := strings.TrimSpace(*manifestPath)

	// Load the manifest (if any) before touching the output so a corrupt
	// manifest aborts the command instead of leaving stale metadata behind.
	var (
		manifest      checkpoint.Manifest
		manifestFile  string
		manifestFound bool
	)
	if explicitManifest != "" {
		loaded, err := checkpoint.Load(explicitManifest)
		if err != nil {
			return fmt.Errorf("load checkpoint manifest %s: %w", explicitManifest, err)
		}
		manifest, manifestFile, manifestFound = loaded, explicitManifest, true
		if targetPath == "" {
			targetPath = manifest.OutputPath
		}
	} else if targetPath != "" {
		candidate := checkpoint.DefaultManifestPath(targetPath)
		loaded, err := checkpoint.Load(candidate)
		switch {
		case err == nil:
			manifest, manifestFile, manifestFound = loaded, candidate, true
		case errors.Is(err, os.ErrNotExist):
			// no manifest next to the output: nothing to keep in sync
		default:
			return fmt.Errorf("load checkpoint manifest %s: %w", candidate, err)
		}
	}
	if targetPath == "" {
		return errors.New("either --output or a valid --manifest is required to clean duplicates")
	}
	if manifestFound && filepath.Clean(manifest.OutputPath) != filepath.Clean(targetPath) {
		fmt.Fprintf(stdout, "%sclean%s warning: manifest %s tracks %s, not %s; manifest left unchanged\n", colorize(colorYellow), colorize(colorReset), manifestFile, manifest.OutputPath, targetPath)
		manifestFound = false
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

	if manifestFound {
		audit, err := csvout.AuditCSV(targetPath)
		if err != nil {
			return fmt.Errorf("re-audit cleaned output: %w", err)
		}
		manifest.FinalOutput = &checkpoint.ManifestOutput{
			Rows:      audit.Rows,
			Bytes:     audit.Bytes,
			SHA256:    audit.SHA256,
			UpdatedAt: time.Now().UTC(),
		}
		if err := checkpoint.Save(manifestFile, manifest); err != nil {
			return fmt.Errorf("update checkpoint manifest %s: %w", manifestFile, err)
		}
		fmt.Fprintf(stdout, "%sclean%s updated final output record in %s\n", colorize(colorGreen), colorize(colorReset), manifestFile)
	}
	return nil
}
