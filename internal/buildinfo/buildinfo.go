package buildinfo

import "fmt"

var (
	Version = "dev"
	Commit  = "none"
	Date    = "unknown"
)

func VersionString() string {
	if Version == "" {
		return "dev"
	}
	if Commit != "" && Commit != "none" {
		return fmt.Sprintf("%s (%s)", Version, Commit)
	}
	return Version
}
