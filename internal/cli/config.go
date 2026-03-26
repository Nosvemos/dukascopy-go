package cli

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

type appConfig struct {
	BaseURL     string                 `json:"base_url"`
	Instruments instrumentsConfig      `json:"instruments"`
	Download    downloadDefaultsConfig `json:"download"`
}

type instrumentsConfig struct {
	Limit *int `json:"limit"`
}

type downloadDefaultsConfig struct {
	Timeframe          string `json:"timeframe"`
	Side               string `json:"side"`
	Simple             *bool  `json:"simple"`
	Full               *bool  `json:"full"`
	CustomColumns      string `json:"custom_columns"`
	Retries            *int   `json:"retries"`
	RetryBackoff       string `json:"retry_backoff"`
	RateLimit          string `json:"rate_limit"`
	Progress           *bool  `json:"progress"`
	Resume             *bool  `json:"resume"`
	Partition          string `json:"partition"`
	Parallelism        *int   `json:"parallelism"`
	CheckpointManifest string `json:"checkpoint_manifest"`
}

var activeConfig *appConfig

func loadActiveConfig(args []string) ([]string, error) {
	configPath, remainingArgs, err := extractConfigPath(args)
	if err != nil {
		return nil, err
	}

	if configPath == "" {
		configPath = strings.TrimSpace(os.Getenv("DUKASCOPY_CONFIG"))
	}
	if configPath == "" {
		activeConfig = nil
		return remainingArgs, nil
	}

	config, err := readConfigFile(configPath)
	if err != nil {
		return nil, err
	}
	activeConfig = config
	return remainingArgs, nil
}

func extractConfigPath(args []string) (string, []string, error) {
	configPath := ""
	remaining := make([]string, 0, len(args))

	for index := 0; index < len(args); index++ {
		arg := args[index]
		switch {
		case arg == "--config":
			if index+1 >= len(args) {
				return "", nil, errors.New("--config requires a file path")
			}
			configPath = strings.TrimSpace(args[index+1])
			index++
		case strings.HasPrefix(arg, "--config="):
			configPath = strings.TrimSpace(strings.TrimPrefix(arg, "--config="))
		default:
			remaining = append(remaining, arg)
		}
	}

	if configPath == "" {
		return "", remaining, nil
	}
	if strings.TrimSpace(configPath) == "" {
		return "", nil, errors.New("--config requires a file path")
	}
	return configPath, remaining, nil
}

func readConfigFile(path string) (*appConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var config appConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("decode config %s: %w", path, err)
	}
	return &config, nil
}

func flagWasSet(fs *flag.FlagSet, name string) bool {
	set := false
	fs.Visit(func(current *flag.Flag) {
		if current.Name == name {
			set = true
		}
	})
	return set
}

func applyInstrumentConfigDefaults(fs *flag.FlagSet, limit *int, baseURL *string) {
	if activeConfig == nil {
		return
	}
	if !flagWasSet(fs, "limit") && activeConfig.Instruments.Limit != nil {
		*limit = *activeConfig.Instruments.Limit
	}
	if !flagWasSet(fs, "base-url") && strings.TrimSpace(activeConfig.BaseURL) != "" {
		*baseURL = strings.TrimSpace(activeConfig.BaseURL)
	}
}

func applyDownloadConfigDefaults(
	fs *flag.FlagSet,
	timeframe *string,
	side *string,
	simpleOutput *bool,
	fullOutput *bool,
	customColumns *string,
	retries *int,
	retryBackoff *time.Duration,
	rateLimit *time.Duration,
	progress *bool,
	resume *bool,
	partition *string,
	parallelism *int,
	checkpointManifest *string,
	baseURL *string,
) error {
	if activeConfig == nil {
		return nil
	}

	config := activeConfig.Download
	if !flagWasSet(fs, "timeframe") && strings.TrimSpace(config.Timeframe) != "" {
		*timeframe = strings.TrimSpace(config.Timeframe)
	}
	if !flagWasSet(fs, "side") && strings.TrimSpace(config.Side) != "" {
		*side = strings.TrimSpace(config.Side)
	}
	if !flagWasSet(fs, "simple") && config.Simple != nil {
		*simpleOutput = *config.Simple
	}
	if !flagWasSet(fs, "full") && config.Full != nil {
		*fullOutput = *config.Full
	}
	if !flagWasSet(fs, "custom-columns") && strings.TrimSpace(config.CustomColumns) != "" {
		*customColumns = strings.TrimSpace(config.CustomColumns)
	}
	if !flagWasSet(fs, "retries") && config.Retries != nil {
		*retries = *config.Retries
	}
	if !flagWasSet(fs, "retry-backoff") && strings.TrimSpace(config.RetryBackoff) != "" {
		value, err := time.ParseDuration(strings.TrimSpace(config.RetryBackoff))
		if err != nil {
			return fmt.Errorf("parse config download.retry_backoff: %w", err)
		}
		*retryBackoff = value
	}
	if !flagWasSet(fs, "rate-limit") && strings.TrimSpace(config.RateLimit) != "" {
		value, err := time.ParseDuration(strings.TrimSpace(config.RateLimit))
		if err != nil {
			return fmt.Errorf("parse config download.rate_limit: %w", err)
		}
		*rateLimit = value
	}
	if !flagWasSet(fs, "progress") && config.Progress != nil {
		*progress = *config.Progress
	}
	if !flagWasSet(fs, "resume") && config.Resume != nil {
		*resume = *config.Resume
	}
	if !flagWasSet(fs, "partition") && strings.TrimSpace(config.Partition) != "" {
		*partition = strings.TrimSpace(config.Partition)
	}
	if !flagWasSet(fs, "parallelism") && config.Parallelism != nil {
		*parallelism = *config.Parallelism
	}
	if !flagWasSet(fs, "checkpoint-manifest") && strings.TrimSpace(config.CheckpointManifest) != "" {
		*checkpointManifest = strings.TrimSpace(config.CheckpointManifest)
	}
	if !flagWasSet(fs, "base-url") && strings.TrimSpace(activeConfig.BaseURL) != "" {
		*baseURL = strings.TrimSpace(activeConfig.BaseURL)
	}
	return nil
}

func configExample() string {
	return strings.TrimSpace(`{
  "base_url": "https://jetta.dukascopy.com",
  "instruments": {
    "limit": 5
  },
  "download": {
    "timeframe": "m1",
    "simple": true,
    "retries": 5,
    "retry_backoff": "750ms",
    "rate_limit": "150ms",
    "partition": "auto",
    "parallelism": 4,
    "progress": true
  }
}`)
}
