package e2e

import (
	"strings"
	"testing"
)

func TestVersionFlag(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	output := runCLI(t, server.URL, "--version")
	if !strings.Contains(output, "dukascopy-go") {
		t.Fatalf("unexpected version output: %s", output)
	}
}
