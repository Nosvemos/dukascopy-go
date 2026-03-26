package cli

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Nosvemos/dukascopy-data-downloader/internal/checkpoint"
	"github.com/Nosvemos/dukascopy-data-downloader/internal/csvout"
)

func runManifest(args []string, stdout io.Writer) error {
	if len(args) == 0 {
		printManifestUsage(stdout)
		return errors.New("manifest subcommand is required")
	}

	switch args[0] {
	case "inspect":
		return runManifestInspect(args[1:], stdout)
	case "prune":
		return runManifestPrune(args[1:], stdout)
	case "repair":
		return runManifestRepair(args[1:], stdout)
	case "verify":
		return runManifestVerify(args[1:], stdout)
	case "help", "-h", "--help":
		printManifestUsage(stdout)
		return nil
	default:
		printManifestUsage(stdout)
		return fmt.Errorf("unknown manifest subcommand %q", args[0])
	}
}

func runManifestInspect(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("manifest inspect", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

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

	manifest, err := checkpoint.Load(path)
	if err != nil {
		return err
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

func runManifestVerify(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("manifest verify", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	manifestPath := fs.String("manifest", "", "checkpoint manifest path")
	outputPath := fs.String("output", "", "output CSV path used to derive <output>.manifest.json")
	jsonOutput := fs.Bool("json", false, "print the verification report as JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}

	path, err := resolveManifestPath(strings.TrimSpace(*manifestPath), strings.TrimSpace(*outputPath))
	if err != nil {
		return err
	}

	report, err := checkpoint.VerifyManifest(path)
	if err != nil {
		return err
	}

	if *jsonOutput {
		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return err
		}
		fmt.Fprintln(stdout, string(data))
		if !report.Valid {
			return errors.New("manifest verification failed")
		}
		return nil
	}

	fmt.Fprintf(stdout, "%sVerify%s %s\n", colorize(colorCyan), colorize(colorReset), path)
	for _, part := range report.Parts {
		status := colorize(colorGreen) + "ok" + colorize(colorReset)
		if !part.Valid {
			status = colorize(colorRed) + "invalid" + colorize(colorReset)
		}
		fmt.Fprintf(stdout, "part  %-24s %s", part.Label, status)
		if part.Problem != "" {
			fmt.Fprintf(stdout, "  %s", part.Problem)
		}
		fmt.Fprintln(stdout)
	}
	if report.FinalOutput != nil {
		status := colorize(colorGreen) + "ok" + colorize(colorReset)
		if !report.FinalOutput.Valid {
			status = colorize(colorRed) + "invalid" + colorize(colorReset)
		}
		fmt.Fprintf(stdout, "final %-24s %s", filepathBase(report.FinalOutput.Path), status)
		if report.FinalOutput.Problem != "" {
			fmt.Fprintf(stdout, "  %s", report.FinalOutput.Problem)
		}
		fmt.Fprintln(stdout)
	}

	if !report.Valid {
		return errors.New("manifest verification failed")
	}

	fmt.Fprintf(stdout, "%sverified%s manifest is consistent\n", colorize(colorGreen), colorize(colorReset))
	return nil
}

func runManifestRepair(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("manifest repair", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	manifestPath := fs.String("manifest", "", "checkpoint manifest path")
	outputPath := fs.String("output", "", "output CSV path used to derive <output>.manifest.json")
	if err := fs.Parse(args); err != nil {
		return err
	}

	path, err := resolveManifestPath(strings.TrimSpace(*manifestPath), strings.TrimSpace(*outputPath))
	if err != nil {
		return err
	}

	manifest, err := checkpoint.Load(path)
	if err != nil {
		return err
	}

	report, err := checkpoint.VerifyManifest(path)
	if err != nil {
		return err
	}

	repairedParts := 0
	repairedOutput := false

	if report.FinalOutput != nil && report.FinalOutput.Valid {
		for _, partResult := range report.Parts {
			if partResult.Valid {
				continue
			}
			partMeta := checkpoint.FindPart(&manifest, partResult.Label)
			if partMeta == nil {
				return fmt.Errorf("manifest part %s was not found", partResult.Label)
			}

			partPath := filepath.Join(manifest.PartsDir, partMeta.File)
			if err := csvout.ExtractCSVRange(manifest.OutputPath, partPath, partMeta.Start, partMeta.End); err != nil {
				return err
			}
			audit, err := csvout.AuditCSV(partPath)
			if err != nil {
				return err
			}

			partMeta.Status = "completed"
			partMeta.Rows = audit.Rows
			partMeta.Bytes = audit.Bytes
			partMeta.SHA256 = audit.SHA256
			partMeta.Error = ""
			partMeta.UpdatedAt = time.Now().UTC()
			repairedParts++
		}
	}

	if repairedParts > 0 {
		if err := checkpoint.Save(path, manifest); err != nil {
			return err
		}
		report, err = checkpoint.VerifyManifest(path)
		if err != nil {
			return err
		}
	}

	if shouldRepairFinalOutput(report) {
		partPaths := make([]string, 0, len(manifest.Parts))
		for _, part := range manifest.Parts {
			partPaths = append(partPaths, filepath.Join(manifest.PartsDir, part.File))
		}

		from, to, err := manifestRange(manifest)
		if err != nil {
			return err
		}
		if err := csvout.AssembleCSVFromParts(manifest.OutputPath, partPaths, from, to); err != nil {
			return err
		}
		audit, err := csvout.AuditCSV(manifest.OutputPath)
		if err != nil {
			return err
		}

		manifest.Completed = true
		manifest.FinalOutput = &checkpoint.ManifestOutput{
			Rows:      audit.Rows,
			Bytes:     audit.Bytes,
			SHA256:    audit.SHA256,
			UpdatedAt: time.Now().UTC(),
		}
		if err := checkpoint.Save(path, manifest); err != nil {
			return err
		}
		repairedOutput = true
	}

	report, err = checkpoint.VerifyManifest(path)
	if err != nil {
		return err
	}

	if repairedParts == 0 && !repairedOutput && report.Valid {
		fmt.Fprintf(stdout, "%srepair%s nothing to do\n", colorize(colorGreen), colorize(colorReset))
		return nil
	}

	fmt.Fprintf(stdout, "%srepair%s repaired %d part(s)", colorize(colorGreen), colorize(colorReset), repairedParts)
	if repairedOutput {
		fmt.Fprint(stdout, " and rebuilt final output")
	}
	fmt.Fprintln(stdout)

	if !report.Valid {
		return errors.New("manifest repair could not fully restore the dataset")
	}

	fmt.Fprintf(stdout, "%sverified%s manifest is consistent after repair\n", colorize(colorGreen), colorize(colorReset))
	return nil
}

func runManifestPrune(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("manifest prune", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	manifestPath := fs.String("manifest", "", "checkpoint manifest path")
	outputPath := fs.String("output", "", "output CSV path used to derive <output>.manifest.json")
	if err := fs.Parse(args); err != nil {
		return err
	}

	path, err := resolveManifestPath(strings.TrimSpace(*manifestPath), strings.TrimSpace(*outputPath))
	if err != nil {
		return err
	}

	manifest, err := checkpoint.Load(path)
	if err != nil {
		return err
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
					return err
				}
				removed++
			}
		}
	}

	fmt.Fprintf(stdout, "%sprune%s removed %d obsolete file(s)\n", colorize(colorGreen), colorize(colorReset), removed)
	return nil
}

func printManifestUsage(w io.Writer) {
	fmt.Fprint(w, `manifest commands:
  manifest inspect  Show a checkpoint manifest summary
  manifest prune    Remove obsolete temp files and orphan partition files
  manifest repair   Repair part files or the final CSV from existing valid files
  manifest verify   Audit part files and the final CSV against the manifest

examples:
  dukascopy-data manifest inspect --output ./data/xauusd.csv
  dukascopy-data manifest prune --output ./data/xauusd.csv
  dukascopy-data manifest repair --output ./data/xauusd.csv
  dukascopy-data manifest verify --manifest ./data/xauusd.csv.manifest.json
`)
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

func filepathBase(path string) string {
	parts := strings.FieldsFunc(path, func(r rune) bool {
		return r == '/' || r == '\\'
	})
	if len(parts) == 0 {
		return path
	}
	return parts[len(parts)-1]
}

func shouldRepairFinalOutput(report checkpoint.VerificationReport) bool {
	if report.FinalOutput != nil && report.FinalOutput.Valid {
		return false
	}
	for _, part := range report.Parts {
		if !part.Valid {
			return false
		}
	}
	return true
}

func manifestRange(manifest checkpoint.Manifest) (time.Time, time.Time, error) {
	if len(manifest.Parts) == 0 {
		return time.Time{}, time.Time{}, errors.New("manifest does not contain any partitions")
	}
	return manifest.Parts[0].Start, manifest.Parts[len(manifest.Parts)-1].End, nil
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
