package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSyncValidationErrors(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "missing symbol",
			args:    []string{"--output", "test.csv"},
			wantErr: "--symbol is required",
		},
		{
			name:    "missing output",
			args:    []string{"--symbol", "EURUSD"},
			wantErr: "--output is required",
		},
		{
			name:    "missing fallback file and params",
			args:    []string{"--symbol", "EURUSD", "--output", "doesnotexist.csv"},
			wantErr: "does not exist; please provide fallback --from or --last to initialize",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := runSync(tc.args, &bytes.Buffer{}, &bytes.Buffer{})
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("expected error containing %q, got %q", tc.wantErr, err.Error())
			}
		})
	}
}

func TestSyncHelpRequested(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runSync([]string{"--help"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected flag.ErrHelp error")
	}
	if !strings.Contains(stdout.String(), "sync:") || !strings.Contains(stdout.String(), "Usage:") {
		t.Errorf("expected subcommand usage text on stdout, got: %s", stdout.String())
	}
}

func TestSyncWithEmptyExistingCSV(t *testing.T) {
	dir := t.TempDir()
	outputPath := filepath.Join(dir, "empty.csv")

	// Create an empty file
	f, err := os.Create(outputPath)
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	f.Close()

	// Try running sync without fallback should fail
	err = runSync([]string{"--symbol", "EURUSD", "--output", outputPath}, &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected error because file is empty and no fallback --from/--last provided")
	}
	if !strings.Contains(err.Error(), "exists but is empty; please provide fallback --from or --last to initialize") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSyncWithParquetError(t *testing.T) {
	dir := t.TempDir()
	outputPath := filepath.Join(dir, "test.parquet")

	f, err := os.Create(outputPath)
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	f.Close()

	err = runSync([]string{"--symbol", "EURUSD", "--output", outputPath}, &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected error because syncing parquet is not supported")
	}
	if !strings.Contains(err.Error(), "syncing parquet files is not supported") {
		t.Errorf("unexpected error: %v", err)
	}
}
