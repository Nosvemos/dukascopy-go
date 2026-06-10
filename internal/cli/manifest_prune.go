package cli

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/Nosvemos/dukascopy-go/internal/checkpoint"
)

func runManifestPrune(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("manifest prune", flag.ContinueOnError)
	fs.SetOutput(stdout)
	fs.Usage = func() {
		fmt.Fprintf(stdout, "%smanifest prune:%s Remove obsolete temp files and orphan partition files\n\nUsage:\n  dukascopy-go manifest prune [options]\n\nOptions:\n", colorize(colorCyan), colorize(colorReset))
		fs.PrintDefaults()
	}

	manifestPath := fs.String("manifest", "", "checkpoint manifest path")
	outputPath := fs.String("output", "", "output CSV path used to derive <output>.manifest.json")
	if err := fs.Parse(args); err != nil {
		return err
	}

	path, err := resolveManifestPath(strings.TrimSpace(*manifestPath), strings.TrimSpace(*outputPath))
	if err != nil {
		return err
	}

	var printer *operationPrinter
	if isInteractiveTerminal(stdout) {
		printer = newOperationPrinter(stdout)
		printer.SetCommand("manifest prune", path)
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
		printer.SetStatus("pruning files")
		printer.SetPhase("scan parts and temp files")
	}

	removed := 0

	partFiles := make(map[string]struct{}, len(manifest.Parts))
	for _, part := range manifest.Parts {
		partFiles[part.File] = struct{}{}
	}

	partEntries, err := os.ReadDir(manifest.PartsDir)
	if err == nil {
		for _, entry := range partEntries {
			if entry.IsDir() {
				continue
			}

			name := entry.Name()
			if _, ok := partFiles[name]; ok {
				continue
			}
			if !shouldPrunePartFile(name) {
				continue
			}

			if err := os.Remove(filepath.Join(manifest.PartsDir, name)); err != nil && !os.IsNotExist(err) {
				if printer != nil {
					printer.SetStatus("failed")
				}
				return err
			}
			removed++
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	manifestDir := filepath.Dir(path)
	manifestBase := filepath.Base(path)
	outputDir := filepath.Dir(manifest.OutputPath)
	outputBase := filepath.Base(manifest.OutputPath)

	pruneDirs := []struct {
		dir  string
		keep map[string]struct{}
	}{
		{dir: manifestDir},
	}
	if outputDir != manifestDir {
		pruneDirs = append(pruneDirs, struct {
			dir  string
			keep map[string]struct{}
		}{dir: outputDir})
	}

	for _, item := range pruneDirs {
		entries, err := os.ReadDir(item.dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}

		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name := entry.Name()
			if shouldPruneTopLevelFile(name, manifestBase, outputBase) {
				if err := os.Remove(filepath.Join(item.dir, name)); err != nil && !os.IsNotExist(err) {
					if printer != nil {
						printer.SetStatus("failed")
					}
					return err
				}
				removed++
			}
		}
	}
	if printer != nil {
		printer.SetMetric("removed", formatCount(removed))
		printer.SetStatus("prune complete")
		printer.Finish()
		printer = nil
	}

	fmt.Fprintf(stdout, "%sprune%s removed %d obsolete file(s)\n", colorize(colorGreen), colorize(colorReset), removed)
	return nil
}

func shouldPrunePartFile(name string) bool {
	return strings.HasSuffix(name, ".csv") || strings.Contains(name, ".tmp-")
}

func shouldPruneTopLevelFile(name string, manifestBase string, outputBase string) bool {
	switch {
	case strings.HasPrefix(name, manifestBase+".tmp-"):
		return true
	case strings.HasPrefix(name, outputBase+".tmp-"):
		return true
	case strings.HasPrefix(name, outputBase+".resume-") && strings.HasSuffix(name, ".csv"):
		return true
	default:
		return false
	}
}
