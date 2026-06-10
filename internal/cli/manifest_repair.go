package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/checkpoint"
	"github.com/Nosvemos/dukascopy-go/pkg/csvout"
)

func runManifestRepair(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("manifest repair", flag.ContinueOnError)
	fs.SetOutput(stdout)
	fs.Usage = func() {
		fmt.Fprintf(stdout, "%smanifest repair:%s Repair part files, rebuild final output, or re-download gaps\n\nUsage:\n  dukascopy-go manifest repair [options]\n\nOptions:\n", colorize(colorCyan), colorize(colorReset))
		fs.PrintDefaults()
	}

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

func boolLabel(value bool) string {
	if value {
		return "yes"
	}
	return "no"
}
