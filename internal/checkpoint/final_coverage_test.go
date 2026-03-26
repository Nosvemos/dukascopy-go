package checkpoint

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Nosvemos/dukascopy-go/internal/csvout"
)

func TestLoadSaveAndVerifyFinalBranches(t *testing.T) {
	dir := t.TempDir()

	badJSONPath := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(badJSONPath, []byte("{"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if _, err := Load(badJSONPath); err == nil {
		t.Fatal("expected invalid JSON load error")
	}

	blockedParent := filepath.Join(dir, "blocked")
	if err := os.WriteFile(blockedParent, []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := Save(filepath.Join(blockedParent, "manifest.json"), Manifest{Version: CurrentManifestVersion}); err == nil {
		t.Fatal("expected Save to fail when parent path is a file")
	}

	occupiedPath := filepath.Join(dir, "occupied.json")
	if err := os.MkdirAll(occupiedPath, 0o755); err != nil {
		t.Fatalf("MkdirAll returned error: %v", err)
	}
	if err := os.WriteFile(filepath.Join(occupiedPath, "child.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := Save(occupiedPath, Manifest{Version: CurrentManifestVersion}); err == nil {
		t.Fatal("expected Save to fail when replacing a non-empty directory")
	}

	partPath := filepath.Join(dir, "part.csv")
	if err := os.WriteFile(partPath, []byte("timestamp,open\n2024-01-01T00:00:00Z,1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	audit, err := csvout.AuditCSV(partPath)
	if err != nil {
		t.Fatalf("AuditCSV returned error: %v", err)
	}

	part := ManifestPart{
		ID:     "p1",
		Status: "completed",
		Rows:   audit.Rows,
		Bytes:  audit.Bytes + 1,
		SHA256: audit.SHA256,
	}
	if result := verifyPart(part, partPath); result.Valid || !strings.Contains(result.Problem, "byte mismatch") {
		t.Fatalf("expected byte mismatch verification result, got %+v", result)
	}

	part.Bytes = audit.Bytes
	part.SHA256 = "bad"
	if result := verifyPart(part, partPath); result.Valid || !strings.Contains(result.Problem, "sha256 mismatch") {
		t.Fatalf("expected sha mismatch verification result, got %+v", result)
	}

	output := ManifestOutput{
		Rows:   audit.Rows,
		Bytes:  audit.Bytes + 1,
		SHA256: audit.SHA256,
	}
	if result := verifyOutput(output, partPath); result.Valid || !strings.Contains(result.Problem, "byte mismatch") {
		t.Fatalf("expected output byte mismatch verification result, got %+v", result)
	}

	output.Bytes = audit.Bytes
	output.SHA256 = "bad"
	if result := verifyOutput(output, partPath); result.Valid || !strings.Contains(result.Problem, "sha256 mismatch") {
		t.Fatalf("expected output sha mismatch verification result, got %+v", result)
	}
}
