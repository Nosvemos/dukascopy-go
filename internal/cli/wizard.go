package cli

import (
	"fmt"
	"io"
	"strings"

	"github.com/charmbracelet/huh"
)

func runWizard(stdout io.Writer, stderr io.Writer) int {
	var action string
	var symbol string
	var timeframe string
	var lookback string
	var output string

	form := huh.NewForm(
		huh.NewGroup(
			huh.NewSelect[string]().
				Title("What do you want to do?").
				Options(
					huh.NewOption("📉 Download Historical Data", "download"),
					huh.NewOption("📈 Stream Live Data", "live"),
					huh.NewOption("🔍 Search Instruments", "instruments"),
				).
				Value(&action),
		),
	)

	err := form.Run()
	if err != nil {
		fmt.Fprintf(stderr, "Wizard cancelled.\n")
		return 1
	}

	if action == "instruments" {
		var query string
		err = huh.NewInput().Title("Search query (e.g. xauusd, or leave empty for all):").Value(&query).Run()
		if err != nil {
			return 1
		}

		args := []string{"instruments"}
		if strings.TrimSpace(query) != "" {
			args = append(args, "--query", strings.TrimSpace(query))
		}
		return Run(args, stdout, stderr)
	}

	form2 := huh.NewForm(
		huh.NewGroup(
			huh.NewInput().
				Title("Instrument Symbol (e.g. eurusd, xauusd):").
				Value(&symbol).
				Validate(func(s string) error {
					if strings.TrimSpace(s) == "" {
						return fmt.Errorf("symbol is required")
					}
					return nil
				}),

			huh.NewSelect[string]().
				Title("Timeframe:").
				Options(
					huh.NewOption("Tick (Raw Quotes)", "tick"),
					huh.NewOption("1 Minute (m1)", "m1"),
					huh.NewOption("1 Hour (h1)", "h1"),
					huh.NewOption("1 Day (d1)", "d1"),
					huh.NewOption("1 Month (mn1)", "mn1"),
				).
				Value(&timeframe),
		),
	)

	if err := form2.Run(); err != nil {
		return 1
	}

	if action == "live" {
		err = huh.NewSelect[string]().Title("Output Format:").Options(
			huh.NewOption("JSON Lines (.jsonl)", "jsonl"),
			huh.NewOption("CSV (.csv)", "csv"),
		).Value(&output).Run()
		if err != nil {
			return 1
		}

		args := []string{"live", "--symbol", symbol, "--timeframe", timeframe, "--format", output}
		fmt.Fprintf(stderr, "\nRunning: dukascopy-go %s\n\n", strings.Join(args, " "))
		return Run(args, stdout, stderr)
	}

	// Download flow
	err = huh.NewForm(
		huh.NewGroup(
			huh.NewInput().
				Title("Duration (e.g. 30d, 1y) OR exact Start Date (YYYY-MM-DD):").
				Value(&lookback).
				Validate(func(s string) error {
					if strings.TrimSpace(s) == "" {
						return fmt.Errorf("this field is required")
					}
					return nil
				}),
			huh.NewInput().
				Title("Output File (e.g. ./data.csv or ./data.parquet):").
				Value(&output).
				Validate(func(s string) error {
					if strings.TrimSpace(s) == "" {
						return fmt.Errorf("output path is required")
					}
					return nil
				}),
		),
	).Run()

	if err != nil {
		return 1
	}

	args := []string{"download", "--symbol", symbol, "--timeframe", timeframe, "--output", output}
	lookback = strings.TrimSpace(lookback)

	if strings.HasSuffix(lookback, "d") || strings.HasSuffix(lookback, "y") || strings.HasSuffix(lookback, "mo") || strings.HasSuffix(lookback, "w") || strings.HasSuffix(lookback, "h") {
		args = append(args, "--last", lookback)
	} else {
		args = append(args, "--from", lookback)
	}

	fmt.Fprintf(stderr, "\nRunning: dukascopy-go %s\n\n", strings.Join(args, " "))
	return Run(args, stdout, stderr)
}
