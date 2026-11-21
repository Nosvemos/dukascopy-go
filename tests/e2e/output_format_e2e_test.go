package e2e

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestDownloadOutputFormatsAcrossChunks downloads three day chunks (9 m1
// bars) with presets and custom formats: chunks use one internal format, and
// the user's format, time zone, delimiter and header setting are applied only
// to the final file.
func TestDownloadOutputFormatsAcrossChunks(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	cases := []struct {
		name      string
		args      []string
		delimiter rune
		header    bool
		first     string
		last      string
	}{
		{name: "mt4", args: []string{"--preset", "mt4"}, delimiter: ',', first: "2024.01.02 00:00", last: "2024.01.04 00:02"},
		{name: "ninjatrader", args: []string{"--preset", "ninjatrader"}, delimiter: ';', first: "20240102 000000", last: "20240104 000200"},
		{name: "backtrader", args: []string{"--preset", "backtrader"}, delimiter: ',', header: true, first: "2024-01-02 00:00:00", last: "2024-01-04 00:02:00"},
		{name: "no-header", args: []string{"--no-header"}, delimiter: ',', first: "2024-01-02T00:00:00Z", last: "2024-01-04T00:02:00Z"},
		{name: "format-timezone", args: []string{"--timestamp-format", "2006-01-02 15:04", "--timezone", "America/New_York"}, delimiter: ',', header: true, first: "2024-01-01 19:00", last: "2024-01-03 19:02"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			output := filepath.Join(dir, "out.csv")
			args := []string{
				"download", "--symbol", "xauusd", "--timeframe", "m1",
				"--from", "2024-01-02T00:00:00Z", "--to", "2024-01-04T00:02:00Z",
				"--output", output, "--cache-dir", filepath.Join(dir, "cache"),
			}
			runCLI(t, server.URL, append(args, tc.args...)...)

			file, err := os.Open(output)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			reader := csv.NewReader(file)
			reader.Comma = tc.delimiter
			rows, err := reader.ReadAll()
			if err != nil {
				t.Fatalf("parse output: %v", err)
			}
			if tc.header {
				if rows[0][0] != "timestamp" {
					t.Fatalf("expected a header, got %q", rows[0])
				}
				rows = rows[1:]
			} else if strings.EqualFold(rows[0][0], "timestamp") {
				t.Fatalf("expected no header, got %q", rows[0])
			}
			if len(rows) != 9 {
				t.Fatalf("expected 9 rows, got %d: %q", len(rows), rows)
			}
			if rows[0][0] != tc.first || rows[len(rows)-1][0] != tc.last {
				t.Fatalf("first/last = %q/%q, want %q/%q", rows[0][0], rows[len(rows)-1][0], tc.first, tc.last)
			}
		})
	}
}
