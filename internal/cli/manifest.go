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

	"github.com/Nosvemos/dukascopy-go/internal/checkpoint"
	"github.com/Nosvemos/dukascopy-go/internal/csvout"
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

func runManifestVerify(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("manifest verify", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	manifestPath := fs.String("manifest", "", "checkpoint manifest path")
	outputPath := fs.String("output", "", "output CSV path used to derive <output>.manifest.json")
	jsonOutput := fs.Bool("json", false, "print the verification report as JSON")
	checkDataQuality := fs.Bool("check-data-quality", false, "inspect the final CSV for duplicates, out-of-order rows, and gaps")
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
		printer.SetCommand("manifest verify", path)
		printer.SetStatus("verifying manifest")
		printer.SetPhase("hash and row audit")
		defer printer.Finish()
	}

	report, err := checkpoint.VerifyManifest(path)
	if err != nil {
		if printer != nil {
			printer.SetStatus("failed")
		}
		return err
	}
	if printer != nil {
		printer.SetMetric("parts", fmt.Sprintf("%d total", len(report.Parts)))
		if report.FinalOutput != nil {
			printer.SetMetric("final", filepathBase(report.FinalOutput.Path))
		}
	}

	var outputStats *csvout.CSVStats
	var dataQualityIssues []string
	var dataQualityWarnings []string
	if *checkDataQuality && report.FinalOutput != nil && report.FinalOutput.Valid {
		if printer != nil {
			printer.SetStatus("scanning final output")
			printer.SetPhase("data quality audit")
		}
		stats, err := csvout.InspectCSV(report.FinalOutput.Path)
		if err != nil {
			if printer != nil {
				printer.SetStatus("failed")
			}
			return err
		}
		outputStats = &stats
		dataQualityIssues, dataQualityWarnings = evaluateDataQuality(stats)
		if printer != nil {
			printer.SetMetric("suspicious", formatCount(stats.SuspiciousGapCount))
			printer.SetMetric("duplicates", formatCount(stats.DuplicateRows+stats.DuplicateStamps))
		}
	}

	if *jsonOutput {
		payload := struct {
			Report              checkpoint.VerificationReport `json:"report"`
			OutputStats         *csvout.CSVStats              `json:"output_stats,omitempty"`
			DataQualityIssues   []string                      `json:"data_quality_issues,omitempty"`
			DataQualityWarnings []string                      `json:"data_quality_warnings,omitempty"`
		}{
			Report:              report,
			OutputStats:         outputStats,
			DataQualityIssues:   dataQualityIssues,
			DataQualityWarnings: dataQualityWarnings,
		}
		data, err := json.MarshalIndent(payload, "", "  ")
		if err != nil {
			return err
		}
		fmt.Fprintln(stdout, string(data))
		if !report.Valid || len(dataQualityIssues) > 0 {
			return errors.New("manifest verification failed")
		}
		return nil
	}
	if printer != nil {
		if !report.Valid || len(dataQualityIssues) > 0 {
			printer.SetStatus("invalid")
		} else {
			printer.SetStatus("verified")
		}
		printer.Finish()
		printer = nil
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
	if outputStats != nil {
		fmt.Fprintf(
			stdout,
			"quality inferred=%s duplicate_rows=%d duplicate_stamps=%d out_of_order=%d gaps=%d missing_intervals=%d expected_gaps=%d expected_missing=%d suspicious_gaps=%d suspicious_missing=%d\n",
			outputStats.InferredTimeframe,
			outputStats.DuplicateRows,
			outputStats.DuplicateStamps,
			outputStats.OutOfOrderRows,
			outputStats.GapCount,
			outputStats.MissingIntervals,
			outputStats.ExpectedGapCount,
			outputStats.ExpectedMissingIntervals,
			outputStats.SuspiciousGapCount,
			outputStats.SuspiciousMissingIntervals,
		)
		for _, warning := range dataQualityWarnings {
			fmt.Fprintf(stdout, "warning %-22s %s\n", "data-quality", warning)
		}
		for _, issue := range dataQualityIssues {
			fmt.Fprintf(stdout, "quality %-22s %s\n", "invalid", issue)
		}
	}

	if !report.Valid || len(dataQualityIssues) > 0 {
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
	redownloadGaps := fs.Bool("redownload-gaps", false, "re-download partition files that intersect detected timestamp gaps")
	baseURL := fs.String("base-url", readBaseURL(), "Dukascopy API base URL used when --redownload-gaps is enabled")
	retries := fs.Int("retries", 3, "retry count for transient HTTP errors when --redownload-gaps is enabled")
	retryBackoff := fs.Duration("retry-backoff", 500*time.Millisecond, "base retry backoff used when --redownload-gaps is enabled")
	rateLimit := fs.Duration("rate-limit", 0, "minimum delay between HTTP requests when --redownload-gaps is enabled")
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
		printer.SetCommand("manifest repair", path)
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
		printer.SetStatus("verifying manifest")
		printer.SetPhase("integrity scan")
	}

	report, err := checkpoint.VerifyManifest(path)
	if err != nil {
		if printer != nil {
			printer.SetStatus("failed")
		}
		return err
	}

	repairedParts := 0
	repairedOutput := false

	if report.FinalOutput != nil && report.FinalOutput.Valid {
		if printer != nil {
			printer.SetStatus("repairing parts")
			printer.SetPhase("extract from final output")
		}
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
				if printer != nil {
					printer.SetStatus("failed")
				}
				return err
			}
			audit, err := csvout.AuditCSV(partPath)
			if err != nil {
				if printer != nil {
					printer.SetStatus("failed")
				}
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
		if printer != nil {
			printer.SetMetric("repaired", formatCount(repairedParts))
			printer.SetStatus("saving manifest")
			printer.SetPhase("checkpoint update")
		}
		if err := checkpoint.Save(path, manifest); err != nil {
			if printer != nil {
				printer.SetStatus("failed")
			}
			return err
		}
		report, err = checkpoint.VerifyManifest(path)
		if err != nil {
			if printer != nil {
				printer.SetStatus("failed")
			}
			return err
		}
	}

	if shouldRepairFinalOutput(report) {
		if printer != nil {
			printer.SetStatus("rebuilding output")
			printer.SetPhase("assemble final csv")
		}
		if err := rebuildManifestFinalOutput(path, &manifest); err != nil {
			if printer != nil {
				printer.SetStatus("failed")
			}
			return err
		}
		repairedOutput = true
	}

	redownloadedGapParts := 0
	if *redownloadGaps {
		logWriter := stdout
		if printer != nil {
			printer.SetStatus("re-downloading gaps")
			printer.SetPhase("refresh intersecting partitions")
			logWriter = printer
		}
		count, err := redownloadManifestGaps(path, &manifest, logWriter, gapRedownloadOptions{
			BaseURL:      *baseURL,
			Retries:      *retries,
			RetryBackoff: *retryBackoff,
			RateLimit:    *rateLimit,
		})
		if err != nil {
			if printer != nil {
				printer.SetStatus("failed")
			}
			return err
		}
		redownloadedGapParts = count
		if count > 0 {
			repairedOutput = true
		}
		if printer != nil {
			printer.SetMetric("gap parts", formatCount(count))
		}
	}

	if printer != nil {
		printer.SetStatus("final verification")
		printer.SetPhase("manifest re-check")
	}
	report, err = checkpoint.VerifyManifest(path)
	if err != nil {
		if printer != nil {
			printer.SetStatus("failed")
		}
		return err
	}

	if repairedParts == 0 && redownloadedGapParts == 0 && !repairedOutput && report.Valid {
		if printer != nil {
			printer.SetStatus("nothing to do")
			printer.Finish()
			printer = nil
		}
		fmt.Fprintf(stdout, "%srepair%s nothing to do\n", colorize(colorGreen), colorize(colorReset))
		return nil
	}
	if printer != nil {
		printer.SetMetric("repaired", formatCount(repairedParts))
		printer.SetMetric("gap parts", formatCount(redownloadedGapParts))
		printer.SetMetric("rebuilt output", boolLabel(repairedOutput))
		if !report.Valid {
			printer.SetStatus("invalid")
		} else {
			printer.SetStatus("repair complete")
		}
		printer.Finish()
		printer = nil
	}

	fmt.Fprintf(stdout, "%srepair%s repaired %d part(s)", colorize(colorGreen), colorize(colorReset), repairedParts)
	if redownloadedGapParts > 0 {
		fmt.Fprintf(stdout, ", re-downloaded %d gap part(s)", redownloadedGapParts)
	}
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

func printManifestUsage(w io.Writer) {
	fmt.Fprint(w, `manifest commands:
  manifest inspect  Show a checkpoint manifest summary
  manifest prune    Remove obsolete temp files and orphan partition files
  manifest repair   Repair part files, rebuild final output, or re-download gap partitions
  manifest verify   Audit part files and the final CSV against the manifest

examples:
  dukascopy-go manifest inspect --output ./data/xauusd.csv
  dukascopy-go manifest prune --output ./data/xauusd.csv
  dukascopy-go manifest repair --output ./data/xauusd.csv
  dukascopy-go manifest repair --output ./data/xauusd.csv --redownload-gaps
  dukascopy-go manifest verify --manifest ./data/xauusd.csv.manifest.json
  dukascopy-go manifest verify --output ./data/xauusd.csv --check-data-quality
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

func boolLabel(value bool) string {
	if value {
		return "yes"
	}
	return "no"
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

func evaluateDataQuality(stats csvout.CSVStats) ([]string, []string) {
	issues := make([]string, 0)
	warnings := make([]string, 0)

	if stats.DuplicateRows > 0 {
		issues = append(issues, fmt.Sprintf("duplicate rows detected: %d", stats.DuplicateRows))
	}
	if stats.DuplicateStamps > 0 {
		issues = append(issues, fmt.Sprintf("duplicate timestamps detected: %d", stats.DuplicateStamps))
	}
	if stats.OutOfOrderRows > 0 {
		issues = append(issues, fmt.Sprintf("out-of-order rows detected: %d", stats.OutOfOrderRows))
	}
	if stats.ExpectedGapCount > 0 {
		warning := fmt.Sprintf("expected gaps: %d gap(s), %d missing interval(s)", stats.ExpectedGapCount, stats.ExpectedMissingIntervals)
		if strings.TrimSpace(stats.ExpectedLargestGap) != "" {
			warning += ", largest gap " + stats.ExpectedLargestGap
		}
		warnings = append(warnings, warning)
	}
	if stats.SuspiciousGapCount > 0 {
		warning := fmt.Sprintf("suspicious gaps: %d gap(s), %d missing interval(s)", stats.SuspiciousGapCount, stats.SuspiciousMissingIntervals)
		if strings.TrimSpace(stats.SuspiciousLargestGap) != "" {
			warning += ", largest gap " + stats.SuspiciousLargestGap
		}
		warnings = append(warnings, warning)
	} else if stats.GapCount > 0 && stats.ExpectedGapCount == stats.GapCount {
		warnings = append(warnings, "no suspicious gaps detected")
	}
	return issues, warnings
}
