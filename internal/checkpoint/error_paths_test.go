package checkpoint

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadAndSaveErrorPaths(t *testing.T) {
	dir := t.TempDir()
	badJSON := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(badJSON, []byte("{"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if _, err := Load(badJSON); err == nil {
		t.Fatal("expected invalid json load error")
	}

	parentFile := filepath.Join(dir, "parent-file")
	if err := os.WriteFile(parentFile, []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := Save(filepath.Join(parentFile, "child.json"), Manifest{}); err == nil {
		t.Fatal("expected save error when parent path is a file")
	}
}

func TestVerifyOutputMismatchBranches(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.csv")
	if err := os.WriteFile(path, []byte("timestamp,mid_close\n2024-01-01T00:00:00Z,1.1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	if result := verifyOutput(ManifestOutput{Rows: 1, Bytes: 999}, path); result.Valid {
		t.Fatal("expected byte mismatch verification failure")
	}
	if result := verifyOutput(ManifestOutput{Rows: 1, SHA256: "bad"}, path); result.Valid {
		t.Fatal("expected sha mismatch verification failure")
	}
}
