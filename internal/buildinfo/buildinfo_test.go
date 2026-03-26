package buildinfo

import "testing"

func TestVersionString(t *testing.T) {
	previousVersion := Version
	previousCommit := Commit
	defer func() {
		Version = previousVersion
		Commit = previousCommit
	}()

	Version = ""
	Commit = "none"
	if got := VersionString(); got != "dev" {
		t.Fatalf("expected dev fallback, got %q", got)
	}

	Version = "v1.2.3"
	Commit = "abc123"
	if got := VersionString(); got != "v1.2.3 (abc123)" {
		t.Fatalf("expected version with commit, got %q", got)
	}

	Version = "v1.2.3"
	Commit = "none"
	if got := VersionString(); got != "v1.2.3" {
		t.Fatalf("expected plain version, got %q", got)
	}
}
