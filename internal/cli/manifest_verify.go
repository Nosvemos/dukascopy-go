package cli

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/Nosvemos/dukascopy-go/internal/checkpoint"
	"github.com/Nosvemos/dukascopy-go/pkg/csvout"
)

func runManifestVerify(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("manifest verify", flag.ContinueOnError)
	fs.SetOutput(stdout)
	fs.Usage = func() {
		fmt.Fprintf(stdout, "%smanifest verify:%s Audit part files and final CSV against the manifest\n\nUsage:\n  dukascopy-go manifest verify [options]\n\nOptions:\n", colorize(colorCyan), colorize(colorReset))
		fs.PrintDefaults()
	}

	manifestPath := fs.String("manifest", "", "checkpoint manifest path")
	outputPath := fs.String("output", "", "output CSV path used to derive <output>.manifest.json")
	jsonOutput := fs.Bool("json", false, "print the verification report as JSON")
	checkDataQuality := fs.Bool("check-data-quality", false, "inspect the final CSV for duplicates, out-of-order rows, and gaps")
	showSuspiciousGaps := fs.Bool("show-suspicious-gaps", false, "print suspicious gap ranges after data-quality checks")
	suspiciousGapLimit := fs.Int("suspicious-gap-limit", 20, "maximum number of suspicious gap ranges to print when --show-suspicious-gaps is enabled; 0 means all")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *suspiciousGapLimit < 0 {
		return errors.New("--suspicious-gap-limit must be 0 or greater")
	}
	if *showSuspiciousGaps {
		*checkDataQuality = true
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

	manifest, err := checkpoint.Load(path)
	if err != nil {
		if printer != nil {
			printer.SetStatus("failed")
		}
		return err
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
		stats, err := csvout.InspectCSVWithOptions(report.FinalOutput.Path, csvout.InspectOptions{
			Symbol:                  manifest.Symbol,
			IncludeSuspiciousGaps:   *showSuspiciousGaps,
			MaxSuspiciousGapDetails: *suspiciousGapLimit,
		})
		if err != nil {
			if printer != nil {
				printer.SetStatus("failed")
			}
			return err
		}
		outputStats = &stats
		dataQualityIssues, dataQualityWarnings = evaluateDataQuality(stats)
		if printer != nil {
			printer.SetMetric("profile", stats.GapProfile)
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
			"quality inferred=%s profile=%s duplicate_rows=%d duplicate_stamps=%d out_of_order=%d gaps=%d missing_intervals=%d expected_gaps=%d expected_missing=%d suspicious_gaps=%d suspicious_missing=%d\n",
			outputStats.InferredTimeframe,
			outputStats.GapProfile,
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
		printSuspiciousGapDetails(stdout, outputStats.SuspiciousGaps, outputStats.SuspiciousGapCount, *showSuspiciousGaps, *suspiciousGapLimit)
	}

	if !report.Valid || len(dataQualityIssues) > 0 {
		return errors.New("manifest verification failed")
	}

	fmt.Fprintf(stdout, "%sverified%s manifest is consistent\n", colorize(colorGreen), colorize(colorReset))
	return nil
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
