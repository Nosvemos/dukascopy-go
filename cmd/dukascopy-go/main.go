package main

import (
	"os"

	"github.com/Nosvemos/dukascopy-go/internal/cli"
)

func main() {
	os.Exit(cli.Run(os.Args[1:], os.Stdout, os.Stderr))
}
