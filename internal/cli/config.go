package cli

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	toml "github.com/pelletier/go-toml/v2"
	yaml "gopkg.in/yaml.v3"
)

type appConfig struct {
	BaseURL     string                 `json:"base_url" yaml:"base_url" toml:"base_url"`
	Instruments instrumentsConfig      `json:"instruments" yaml:"instruments" toml:"instruments"`
	Download    downloadDefaultsConfig `json:"download" yaml:"download" toml:"download"`
}

type instrumentsConfig struct {
	Limit *int `json:"limit" yaml:"limit" toml:"limit"`
}

type downloadDefaultsConfig struct {
	Timeframe          string `json:"timeframe" yaml:"timeframe" toml:"timeframe"`
	Side               string `json:"side" yaml:"side" toml:"side"`
	Simple             *bool  `json:"simple" yaml:"simple" toml:"simple"`
	Full               *bool  `json:"full" yaml:"full" toml:"full"`
	CustomColumns      string `json:"custom_columns" yaml:"custom_columns" toml:"custom_columns"`
	Live               *bool  `json:"live" yaml:"live" toml:"live"`
	PollInterval       string `json:"poll_interval" yaml:"poll_interval" toml:"poll_interval"`
	Retries            *int   `json:"retries" yaml:"retries" toml:"retries"`
	RetryBackoff       string `json:"retry_backoff" yaml:"retry_backoff" toml:"retry_backoff"`
	RateLimit          string `json:"rate_limit" yaml:"rate_limit" toml:"rate_limit"`
	Progress           *bool  `json:"progress" yaml:"progress" toml:"progress"`
	Resume             *bool  `json:"resume" yaml:"resume" toml:"resume"`
	Partition          string `json:"partition" yaml:"partition" toml:"partition"`
	Parallelism        *int   `json:"parallelism" yaml:"parallelism" toml:"parallelism"`
	CheckpointManifest string `json:"checkpoint_manifest" yaml:"checkpoint_manifest" toml:"checkpoint_manifest"`
	CacheDir           string `json:"cache_dir" yaml:"cache_dir" toml:"cache_dir"`
	KeepCache          *bool  `json:"keep_cache" yaml:"keep_cache" toml:"keep_cache"`
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
	lower := strings.ToLower(path)
	if strings.HasSuffix(lower, ".yaml") || strings.HasSuffix(lower, ".yml") {
		if err := yaml.Unmarshal(data, &config); err != nil {
			return nil, fmt.Errorf("decode yaml config %s: %w", path, err)
		}
	} else if strings.HasSuffix(lower, ".toml") {
		if err := toml.Unmarshal(data, &config); err != nil {
			return nil, fmt.Errorf("decode toml config %s: %w", path, err)
		}
	} else {
		// Fallback to JSON, or try YAML/TOML if JSON fails
		if err := json.Unmarshal(data, &config); err != nil {
			if errTOML := toml.Unmarshal(data, &config); errTOML == nil {
				return &config, nil
			}
			if errYAML := yaml.Unmarshal(data, &config); errYAML == nil {
				return &config, nil
			}
			return nil, fmt.Errorf("decode config %s: %w", path, err)
		}
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
	live *bool,
	pollInterval *time.Duration,
	retries *int,
	retryBackoff *time.Duration,
	rateLimit *time.Duration,
	progress *bool,
	resume *bool,
	partition *string,
	parallelism *int,
	checkpointManifest *string,
	baseURL *string,
	cacheDir *string,
	keepCache *bool,
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
	if !flagWasSet(fs, "live") && config.Live != nil {
		*live = *config.Live
	}
	if !flagWasSet(fs, "poll-interval") && strings.TrimSpace(config.PollInterval) != "" {
		value, err := time.ParseDuration(strings.TrimSpace(config.PollInterval))
		if err != nil {
			return fmt.Errorf("parse config download.poll_interval: %w", err)
		}
		*pollInterval = value
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
	if !flagWasSet(fs, "cache-dir") && strings.TrimSpace(config.CacheDir) != "" {
		*cacheDir = strings.TrimSpace(config.CacheDir)
	}
	if !flagWasSet(fs, "keep-cache") && config.KeepCache != nil {
		*keepCache = *config.KeepCache
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
    "live": false,
    "poll_interval": "5s",
    "retries": 5,
    "retry_backoff": "750ms",
    "rate_limit": "150ms",
    "partition": "auto",
    "parallelism": 4,
    "progress": true
  }
}`)
}
