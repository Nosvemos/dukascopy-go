package checkpoint

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestSaveKeepsPreviousManifestWhenRenameFails(t *testing.T) {
	restore := withManifestHooks()
	defer restore()

	dir := t.TempDir()
	path := filepath.Join(dir, "data.csv.manifest.json")
	original := Manifest{Version: CurrentManifestVersion, Symbol: "eurusd"}
	if err := Save(path, original); err != nil {
		t.Fatalf("initial Save: %v", err)
	}

	renameFn = func(string, string) error { return errors.New("simulated crash before rename") }
	updated := original
	updated.Symbol = "xauusd"
	if err := Save(path, updated); err == nil {
		t.Fatal("expected rename failure")
	}

	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("previous manifest must survive a failed save, Load: %v", err)
	}
	if loaded.Symbol != "eurusd" {
		t.Fatalf("expected previous manifest contents, got symbol %q", loaded.Symbol)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), ".tmp-") {
			t.Fatalf("temp file left behind: %s", entry.Name())
		}
	}
}

func TestSaveReplacesExistingManifestWithMode0644(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "m.json")
	if err := Save(path, Manifest{Version: CurrentManifestVersion, Symbol: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := Save(path, Manifest{Version: CurrentManifestVersion, Symbol: "b"}); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Symbol != "b" {
		t.Fatalf("expected replaced manifest, got %q", loaded.Symbol)
	}
	if runtime.GOOS == "windows" {
		return // POSIX permission bits are not meaningful on Windows
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o644 {
		t.Fatalf("expected mode 0644, got %v", info.Mode().Perm())
	}
}

func TestPartPathRejectsEscapes(t *testing.T) {
	partsDir := filepath.Join(t.TempDir(), "out.csv.parts")
	manifest := &Manifest{PartsDir: partsDir}

	bad := []string{
		"",
		".",
		"..",
		"../evil.csv",
		"..\\evil.csv",
		"sub/part.csv",
		"sub\\part.csv",
		filepath.Join(t.TempDir(), "abs.csv"),
		"/etc/passwd",
	}
	if runtime.GOOS == "windows" {
		bad = append(bad, "C:evil.csv", `C:\evil.csv`)
	}
	for _, name := range bad {
		if got, err := PartPath(manifest, ManifestPart{ID: "p", File: name}); err == nil {
			t.Errorf("PartPath(%q) = %q, expected error", name, got)
		}
	}

	got, err := PartPath(manifest, ManifestPart{ID: "p", File: "20240101.csv"})
	if err != nil {
		t.Fatalf("PartPath valid name: %v", err)
	}
	if got != filepath.Join(partsDir, "20240101.csv") {
		t.Fatalf("unexpected part path %q", got)
	}
}

func TestLoadRejectsTraversingPartFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "m.json")
	body := `{"version":1,"parts_dir":"` + filepath.ToSlash(dir) + `","parts":[{"id":"p","file":"../../evil.csv"}]}`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected Load to reject a part file outside the parts directory")
	}
	if _, err := VerifyManifest(path); err == nil {
		t.Fatal("expected VerifyManifest to reject a part file outside the parts directory")
	}
}

func TestVerifyPartExistingButUnreadableIsNotMissing(t *testing.T) {
	dir := t.TempDir()
	partPath := filepath.Join(dir, "part.csv.gz")
	if err := os.WriteFile(partPath, []byte("this is not gzip"), 0o644); err != nil {
		t.Fatal(err)
	}
	result := verifyPart(ManifestPart{ID: "p", Status: "completed"}, partPath)
	if result.Valid {
		t.Fatalf("expected invalid result, got %+v", result)
	}
	if !result.Exists {
		t.Fatalf("a present but corrupt file must be reported as existing, got %+v", result)
	}

	missing := verifyPart(ManifestPart{ID: "p", Status: "completed"}, filepath.Join(dir, "nope.csv"))
	if missing.Exists {
		t.Fatalf("expected missing file to be reported as not existing, got %+v", missing)
	}
}
