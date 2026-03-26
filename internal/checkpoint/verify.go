package checkpoint

import (
	"fmt"
	"path/filepath"

	"github.com/Nosvemos/dukascopy-data-downloader/internal/csvout"
)

type VerificationReport struct {
	ManifestPath string
	Manifest     Manifest
	Parts        []FileVerification
	FinalOutput  *FileVerification
	Valid        bool
}

type FileVerification struct {
	Label   string
	Path    string
	Exists  bool
	Valid   bool
	Rows    int
	Bytes   int64
	SHA256  string
	Problem string
}

func VerifyManifest(manifestPath string) (VerificationReport, error) {
	manifest, err := Load(manifestPath)
	if err != nil {
		return VerificationReport{}, err
	}

	report := VerificationReport{
		ManifestPath: manifestPath,
		Manifest:     manifest,
		Parts:        make([]FileVerification, 0, len(manifest.Parts)),
		Valid:        true,
	}

	for _, part := range manifest.Parts {
		partPath := filepath.Join(manifest.PartsDir, part.File)
		result := verifyPart(part, partPath)
		report.Parts = append(report.Parts, result)
		if !result.Valid {
			report.Valid = false
		}
	}

	if manifest.FinalOutput != nil {
		outputResult := verifyOutput(*manifest.FinalOutput, manifest.OutputPath)
		report.FinalOutput = &outputResult
		if !outputResult.Valid {
			report.Valid = false
		}
	}

	return report, nil
}

func verifyPart(part ManifestPart, path string) FileVerification {
	result := FileVerification{
		Label:  part.ID,
		Path:   path,
		Exists: true,
	}

	audit, err := csvout.AuditCSV(path)
	if err != nil {
		result.Exists = false
		result.Valid = false
		result.Problem = err.Error()
		return result
	}

	result.Rows = audit.Rows
	result.Bytes = audit.Bytes
	result.SHA256 = audit.SHA256
	if part.Status != "completed" {
		result.Valid = false
		result.Problem = fmt.Sprintf("manifest status is %q", part.Status)
		return result
	}
	if part.Rows != audit.Rows {
		result.Valid = false
		result.Problem = fmt.Sprintf("row mismatch: manifest=%d file=%d", part.Rows, audit.Rows)
		return result
	}
	if part.Bytes != 0 && part.Bytes != audit.Bytes {
		result.Valid = false
		result.Problem = fmt.Sprintf("byte mismatch: manifest=%d file=%d", part.Bytes, audit.Bytes)
		return result
	}
	if part.SHA256 != "" && part.SHA256 != audit.SHA256 {
		result.Valid = false
		result.Problem = fmt.Sprintf("sha256 mismatch: manifest=%s file=%s", part.SHA256, audit.SHA256)
		return result
	}

	result.Valid = true
	return result
}

func verifyOutput(output ManifestOutput, path string) FileVerification {
	result := FileVerification{
		Label:  "final-output",
		Path:   path,
		Exists: true,
	}

	audit, err := csvout.AuditCSV(path)
	if err != nil {
		result.Exists = false
		result.Valid = false
		result.Problem = err.Error()
		return result
	}

	result.Rows = audit.Rows
	result.Bytes = audit.Bytes
	result.SHA256 = audit.SHA256
	if output.Rows != audit.Rows {
		result.Valid = false
		result.Problem = fmt.Sprintf("row mismatch: manifest=%d file=%d", output.Rows, audit.Rows)
		return result
	}
	if output.Bytes != 0 && output.Bytes != audit.Bytes {
		result.Valid = false
		result.Problem = fmt.Sprintf("byte mismatch: manifest=%d file=%d", output.Bytes, audit.Bytes)
		return result
	}
	if output.SHA256 != "" && output.SHA256 != audit.SHA256 {
		result.Valid = false
		result.Problem = fmt.Sprintf("sha256 mismatch: manifest=%s file=%s", output.SHA256, audit.SHA256)
		return result
	}

	result.Valid = true
	return result
}
