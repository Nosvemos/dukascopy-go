package main

import (
	"os"

	"dukascopy-data-downloader/internal/cli"
)

func main() {
	os.Exit(cli.Run(os.Args[1:], os.Stdout, os.Stderr))
}
