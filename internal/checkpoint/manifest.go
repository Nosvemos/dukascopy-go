package checkpoint

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const CurrentManifestVersion = 1

type Manifest struct {
	Version     int             `json:"version"`
	OutputPath  string          `json:"output_path"`
	PartsDir    string          `json:"parts_dir"`
	Symbol      string          `json:"symbol"`
	Timeframe   string          `json:"timeframe"`
	Side        string          `json:"side"`
	ResultKind  string          `json:"result_kind"`
	Columns     []string        `json:"columns"`
	Partition   string          `json:"partition"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
	Completed   bool            `json:"completed"`
	FinalOutput *ManifestOutput `json:"final_output,omitempty"`
	Summary     ManifestSummary `json:"summary"`
	Parts       []ManifestPart  `json:"parts"`
}

type ManifestPart struct {
	ID        string    `json:"id"`
	Start     time.Time `json:"start"`
	End       time.Time `json:"end"`
	File      string    `json:"file"`
	Status    string    `json:"status"`
	Rows      int       `json:"rows"`
	Bytes     int64     `json:"bytes,omitempty"`
	SHA256    string    `json:"sha256,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
	Error     string    `json:"error,omitempty"`
}

type ManifestOutput struct {
	Rows      int       `json:"rows"`
	Bytes     int64     `json:"bytes"`
	SHA256    string    `json:"sha256"`
	UpdatedAt time.Time `json:"updated_at"`
}

type ManifestSummary struct {
	TotalParts     int   `json:"total_parts"`
	CompletedParts int   `json:"completed_parts"`
	FailedParts    int   `json:"failed_parts"`
	RunningParts   int   `json:"running_parts"`
	PendingParts   int   `json:"pending_parts"`
	TotalRows      int   `json:"total_rows"`
	OutputRows     int   `json:"output_rows"`
	OutputBytes    int64 `json:"output_bytes"`
}

func DefaultManifestPath(outputPath string) string {
	return outputPath + ".manifest.json"
}

func DefaultPartsDir(outputPath string) string {
	return outputPath + ".parts"
}

func Load(path string) (Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Manifest{}, err
	}

	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return Manifest{}, err
	}
	if manifest.Version != CurrentManifestVersion {
		return Manifest{}, fmt.Errorf("unsupported checkpoint manifest version %d", manifest.Version)
	}
	return manifest, nil
}

func Save(path string, manifest Manifest) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	RefreshSummary(&manifest)
	manifest.UpdatedAt = time.Now().UTC()
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')

	tempFile, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tempPath := tempFile.Name()
	defer os.Remove(tempPath)

	if _, err := tempFile.Write(data); err != nil {
		tempFile.Close()
		return err
	}
	if err := tempFile.Close(); err != nil {
		return err
	}

	if _, err := os.Stat(path); err == nil {
		if err := os.Remove(path); err != nil {
			return err
		}
	}

	return os.Rename(tempPath, path)
}

func ValidateCompatibility(existing Manifest, expected Manifest) error {
	switch {
	case existing.OutputPath != expected.OutputPath:
		return fmt.Errorf("checkpoint manifest output path %q does not match requested output %q", existing.OutputPath, expected.OutputPath)
	case existing.Symbol != expected.Symbol:
		return fmt.Errorf("checkpoint manifest symbol %q does not match requested symbol %q", existing.Symbol, expected.Symbol)
	case existing.Timeframe != expected.Timeframe:
		return fmt.Errorf("checkpoint manifest timeframe %q does not match requested timeframe %q", existing.Timeframe, expected.Timeframe)
	case existing.Side != expected.Side:
		return fmt.Errorf("checkpoint manifest side %q does not match requested side %q", existing.Side, expected.Side)
	case existing.ResultKind != expected.ResultKind:
		return fmt.Errorf("checkpoint manifest result kind %q does not match requested result kind %q", existing.ResultKind, expected.ResultKind)
	case existing.Partition != expected.Partition:
		return fmt.Errorf("checkpoint manifest partition %q does not match requested partition %q", existing.Partition, expected.Partition)
	case existing.PartsDir != expected.PartsDir:
		return fmt.Errorf("checkpoint manifest parts dir %q does not match requested parts dir %q", existing.PartsDir, expected.PartsDir)
	}

	if len(existing.Columns) != len(expected.Columns) {
		return fmt.Errorf("checkpoint manifest columns do not match the selected columns")
	}
	for index := range existing.Columns {
		if existing.Columns[index] != expected.Columns[index] {
			return fmt.Errorf("checkpoint manifest columns do not match the selected columns")
		}
	}

	if len(existing.Parts) != len(expected.Parts) {
		return fmt.Errorf("checkpoint manifest partition count does not match the requested range")
	}
	for index := range existing.Parts {
		left := existing.Parts[index]
		right := expected.Parts[index]
		if left.ID != right.ID || !left.Start.Equal(right.Start) || !left.End.Equal(right.End) || left.File != right.File {
			return fmt.Errorf("checkpoint manifest partitions do not match the requested range")
		}
	}

	return nil
}

func FindPart(manifest *Manifest, id string) *ManifestPart {
	for index := range manifest.Parts {
		if manifest.Parts[index].ID == id {
			return &manifest.Parts[index]
		}
	}
	return nil
}

func RefreshSummary(manifest *Manifest) {
	summary := ManifestSummary{
		TotalParts: len(manifest.Parts),
	}

	for _, part := range manifest.Parts {
		summary.TotalRows += part.Rows
		switch part.Status {
		case "completed":
			summary.CompletedParts++
		case "failed":
			summary.FailedParts++
		case "running":
			summary.RunningParts++
		default:
			summary.PendingParts++
		}
	}

	if manifest.FinalOutput != nil {
		summary.OutputRows = manifest.FinalOutput.Rows
		summary.OutputBytes = manifest.FinalOutput.Bytes
	}

	manifest.Summary = summary
}
