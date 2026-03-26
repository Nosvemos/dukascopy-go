package checkpoint

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type stubTempFile struct {
	name     string
	writeErr error
	closeErr error
}

func (s *stubTempFile) Write(p []byte) (int, error) {
	if s.writeErr != nil {
		return 0, s.writeErr
	}
	return len(p), nil
}

func (s *stubTempFile) Close() error {
	return s.closeErr
}

func (s *stubTempFile) Name() string {
	return s.name
}

func withManifestHooks() func() {
	originalMkdirAll := mkdirAllFn
	originalMarshalIndent := marshalIndentFn
	originalCreateTemp := createTempFn
	originalStat := statFn
	originalRemove := removeFn
	originalRename := renameFn

	return func() {
		mkdirAllFn = originalMkdirAll
		marshalIndentFn = originalMarshalIndent
		createTempFn = originalCreateTemp
		statFn = originalStat
		removeFn = originalRemove
		renameFn = originalRename
	}
}

func TestSaveGapBranches(t *testing.T) {
	restore := withManifestHooks()
	defer restore()

	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.json")
	manifest := Manifest{Version: CurrentManifestVersion}

	marshalIndentFn = func(any, string, string) ([]byte, error) {
		return nil, errors.New("marshal")
	}
	if err := Save(path, manifest); err == nil {
		t.Fatal("expected marshal error")
	}

	marshalIndentFn = func(v any, prefix string, indent string) ([]byte, error) {
		return []byte("{}"), nil
	}
	createTempFn = func(string, string) (tempWriteFile, error) {
		return nil, errors.New("create temp")
	}
	if err := Save(path, manifest); err == nil {
		t.Fatal("expected create temp error")
	}

	createTempFn = func(string, string) (tempWriteFile, error) {
		return &stubTempFile{name: filepath.Join(dir, "write.tmp"), writeErr: errors.New("write")}, nil
	}
	if err := Save(path, manifest); err == nil {
		t.Fatal("expected write error")
	}

	createTempFn = func(string, string) (tempWriteFile, error) {
		return &stubTempFile{name: filepath.Join(dir, "close.tmp"), closeErr: errors.New("close")}, nil
	}
	if err := Save(path, manifest); err == nil {
		t.Fatal("expected close error")
	}

	createTempFn = func(string, string) (tempWriteFile, error) {
		return &stubTempFile{name: filepath.Join(dir, "remove.tmp")}, nil
	}
	statFn = func(string) (os.FileInfo, error) {
		return os.Stat(dir)
	}
	removeFn = func(string) error {
		return errors.New("remove")
	}
	if err := Save(path, manifest); err == nil {
		t.Fatal("expected remove error")
	}

	removeFn = os.Remove
	statFn = func(string) (os.FileInfo, error) {
		return nil, os.ErrNotExist
	}
	renameFn = func(string, string) error {
		return errors.New("rename")
	}
	if err := Save(path, manifest); err == nil {
		t.Fatal("expected rename error")
	}
}

func TestValidateCompatibilityAndVerifyManifestGapBranches(t *testing.T) {
	base := Manifest{
		Version:    CurrentManifestVersion,
		OutputPath: "dataset.csv",
		PartsDir:   "dataset.parts",
		Symbol:     "xauusd",
		Timeframe:  "m1",
		Side:       "BID",
		ResultKind: "bar",
		Columns:    []string{"timestamp", "open"},
		Partition:  "day",
		Parts: []ManifestPart{{
			ID:    "part-1",
			Start: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			End:   time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			File:  "part-1.csv",
		}},
	}

	mismatch := base
	mismatch.Columns = []string{"timestamp"}
	if err := ValidateCompatibility(base, mismatch); err == nil {
		t.Fatal("expected column-length mismatch error")
	}

	dir := t.TempDir()
	outputPath := filepath.Join(dir, "output.csv")
	if err := os.WriteFile(outputPath, []byte("timestamp,open\n2024-01-01T00:00:00Z,1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	manifestPath := filepath.Join(dir, "manifest.json")
	manifest := base
	manifest.OutputPath = outputPath
	manifest.PartsDir = dir
	manifest.FinalOutput = &ManifestOutput{Rows: 999}
	if err := Save(manifestPath, manifest); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	report, err := VerifyManifest(manifestPath)
	if err != nil {
		t.Fatalf("VerifyManifest returned error: %v", err)
	}
	if report.Valid {
		t.Fatalf("expected invalid report, got %+v", report)
	}
	if report.FinalOutput == nil || report.FinalOutput.Valid {
		t.Fatalf("expected invalid final output verification, got %+v", report.FinalOutput)
	}
}
