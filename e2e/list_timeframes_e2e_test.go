package e2e

import (
	"strings"
	"testing"
)

func TestListTimeframesCommand(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	output := runCLI(t, server.URL, "--list-timeframes")
	for _, expected := range []string{"tick", "m1", "m3", "m5", "h4", "w1", "mn1"} {
		if !strings.Contains(output, expected) {
			t.Fatalf("missing timeframe %q in output: %s", expected, output)
		}
	}
}
