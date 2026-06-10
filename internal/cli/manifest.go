package cli

import (
	"errors"
	"fmt"
	"io"
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
	case "clean-duplicates":
		return runManifestCleanDuplicates(args[1:], stdout)
	case "help", "-h", "--help":
		printManifestUsage(stdout)
		return nil
	default:
		printManifestUsage(stdout)
		return fmt.Errorf("unknown manifest subcommand %q", args[0])
	}
}

func printManifestUsage(w io.Writer) {
	fmt.Fprint(w, `manifest commands:
  manifest inspect          Show a checkpoint manifest summary
  manifest prune            Remove obsolete temp files and orphan partition files
  manifest repair           Repair part files, rebuild final output, or re-download gap partitions
  manifest verify           Audit part files and the final CSV against the manifest
  manifest clean-duplicates Clean duplicate rows and sort chronologically

examples:
  dukascopy-go manifest inspect --output ./data/xauusd.csv
  dukascopy-go manifest prune --output ./data/xauusd.csv
  dukascopy-go manifest repair --output ./data/xauusd.csv
  dukascopy-go manifest repair --output ./data/xauusd.csv --redownload-gaps
  dukascopy-go manifest verify --manifest ./data/xauusd.csv.manifest.json
  dukascopy-go manifest verify --output ./data/xauusd.csv --check-data-quality
  dukascopy-go manifest clean-duplicates --output ./data/xauusd.csv
`)
}
