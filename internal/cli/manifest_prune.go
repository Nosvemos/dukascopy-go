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
	dryRun := fs.Bool("dry-run", false, "print the files that would be removed without deleting anything")
	if err := fs.Parse(args); err != nil {
		return err
	}

	path, err := resolveManifestPath(strings.TrimSpace(*manifestPath), strings.TrimSpace(*outputPath))
	if err != nil {
		return err
	}

	var printer *operationPrinter
	if isInteractiveTerminal(stdout) && !*dryRun {
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

	if err := validatePrunePartsDir(manifest); err != nil {
		if printer != nil {
			printer.SetStatus("failed")
		}
		return err
	}

	removed := 0
	now := pruneNow()
	// remove deletes one file, or only reports it in --dry-run mode. Temp files
	// modified recently may belong to a download running in another process and
	// are left alone.
	remove := func(filePath string, info os.FileInfo) error {
		if isRecentTempFile(filepath.Base(filePath), info, now) {
			return nil
		}
		if *dryRun {
			fmt.Fprintf(stdout, "would remove %s\n", filePath)
			removed++
			return nil
		}
		if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
			if printer != nil {
				printer.SetStatus("failed")
			}
			return err
		}
		removed++
		return nil
	}

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

			info, infoErr := entry.Info()
			if infoErr != nil {
				if os.IsNotExist(infoErr) {
					continue
				}
				return infoErr
			}
			if err := remove(filepath.Join(manifest.PartsDir, name), info); err != nil {
				return err
			}
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
				info, infoErr := entry.Info()
				if infoErr != nil {
					if os.IsNotExist(infoErr) {
						continue
					}
					return infoErr
				}
				if err := remove(filepath.Join(item.dir, name), info); err != nil {
					return err
				}
			}
		}
	}
	if printer != nil {
		printer.SetMetric("removed", formatCount(removed))
		printer.SetStatus("prune complete")
		printer.Finish()
		printer = nil
	}

	if *dryRun {
		fmt.Fprintf(stdout, "%sprune%s dry run: %d obsolete file(s) would be removed\n", colorize(colorGreen), colorize(colorReset), removed)
		return nil
	}
	fmt.Fprintf(stdout, "%sprune%s removed %d obsolete file(s)\n", colorize(colorGreen), colorize(colorReset), removed)
	return nil
}

// pruneTempMinAge is how old a ".tmp-" file must be before prune deletes it;
// younger temp files may belong to a download running in another process.
const pruneTempMinAge = 10 * time.Minute

var pruneNow = time.Now

func isRecentTempFile(name string, info os.FileInfo, now time.Time) bool {
	if !strings.Contains(name, ".tmp-") || info == nil {
		return false
	}
	return now.Sub(info.ModTime()) < pruneTempMinAge
}

// validatePrunePartsDir refuses to prune a directory that is not clearly the
// manifest's dedicated parts directory, since every unlisted .csv/.parquet in
// it gets deleted.
func validatePrunePartsDir(manifest checkpoint.Manifest) error {
	partsDir := strings.TrimSpace(manifest.PartsDir)
	if partsDir == "" {
		return errors.New("refusing to prune: manifest has no parts_dir")
	}
	cleaned := filepath.Clean(partsDir)
	if strings.TrimSpace(manifest.OutputPath) != "" && cleaned == filepath.Clean(checkpoint.DefaultPartsDir(manifest.OutputPath)) {
		return nil
	}
	if strings.HasSuffix(strings.ToLower(filepath.Base(cleaned)), ".parts") {
		return nil
	}
	return fmt.Errorf("refusing to prune %q: it is not the dedicated parts directory (expected %q or a directory name ending in .parts)", partsDir, checkpoint.DefaultPartsDir(manifest.OutputPath))
}

func shouldPrunePartFile(name string) bool {
	lower := strings.ToLower(name)
	return strings.HasSuffix(lower, ".csv") || strings.HasSuffix(lower, ".parquet") || strings.Contains(name, ".tmp-")
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
