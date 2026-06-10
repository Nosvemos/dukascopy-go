package cli

import (
	"flag"
	"os"
	"strings"
	"testing"
	"time"
)

func TestExtractConfigPath(t *testing.T) {
	t.Run("separates config flag from remaining args", func(t *testing.T) {
		path, remaining, err := extractConfigPath([]string{"--config", "test.json", "download", "--symbol", "xauusd"})
		if err != nil {
			t.Fatalf("extractConfigPath returned error: %v", err)
		}
		if path != "test.json" {
			t.Fatalf("expected config path test.json, got %q", path)
		}
		if got := strings.Join(remaining, " "); got != "download --symbol xauusd" {
			t.Fatalf("unexpected remaining args: %q", got)
		}
	})

	t.Run("supports equals syntax", func(t *testing.T) {
		path, remaining, err := extractConfigPath([]string{"--config=test.json", "stats", "--input", "data.csv"})
		if err != nil {
			t.Fatalf("extractConfigPath returned error: %v", err)
		}
		if path != "test.json" {
			t.Fatalf("expected config path test.json, got %q", path)
		}
		if got := strings.Join(remaining, " "); got != "stats --input data.csv" {
			t.Fatalf("unexpected remaining args: %q", got)
		}
	})

	t.Run("rejects missing config path", func(t *testing.T) {
		_, _, err := extractConfigPath([]string{"--config"})
		if err == nil {
			t.Fatal("expected missing config path error")
		}
	})
}

func TestApplyDownloadConfigDefaults(t *testing.T) {
	previousConfig := activeConfig
	defer func() {
		activeConfig = previousConfig
	}()

	activeConfig = &appConfig{
		BaseURL: "https://example.test",
		Download: downloadDefaultsConfig{
			Timeframe:          "h1",
			Side:               "ASK",
			Simple:             boolPtr(true),
			Full:               boolPtr(false),
			CustomColumns:      "timestamp,mid_close",
			Live:               boolPtr(true),
			PollInterval:       "7s",
			Retries:            intPtr(7),
			RetryBackoff:       "750ms",
			RateLimit:          "150ms",
			Progress:           boolPtr(true),
			Resume:             boolPtr(true),
			Partition:          "month",
			Parallelism:        intPtr(3),
			CheckpointManifest: "custom.manifest.json",
		},
	}

	fs := flag.NewFlagSet("download", flag.ContinueOnError)
	timeframe := fs.String("timeframe", "m1", "")
	side := fs.String("side", "BID", "")
	simpleOutput := fs.Bool("simple", false, "")
	fullOutput := fs.Bool("full", true, "")
	customColumns := fs.String("custom-columns", "", "")
	live := fs.Bool("live", false, "")
	pollInterval := fs.Duration("poll-interval", time.Second, "")
	retries := fs.Int("retries", 1, "")
	retryBackoff := fs.Duration("retry-backoff", time.Second, "")
	rateLimit := fs.Duration("rate-limit", time.Second, "")
	progress := fs.Bool("progress", false, "")
	resume := fs.Bool("resume", false, "")
	partition := fs.String("partition", "none", "")
	parallelism := fs.Int("parallelism", 1, "")
	checkpointManifest := fs.String("checkpoint-manifest", "", "")
	baseURL := fs.String("base-url", "https://default.test", "")
	cacheDir := fs.String("cache-dir", "./.dukascopy_cache", "")
	keepCache := fs.Bool("keep-cache", false, "")

	if err := fs.Parse([]string{"--side", "bid", "--retries", "9"}); err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	err := applyDownloadConfigDefaults(
		fs,
		timeframe,
		side,
		simpleOutput,
		fullOutput,
		customColumns,
		live,
		pollInterval,
		retries,
		retryBackoff,
		rateLimit,
		progress,
		resume,
		partition,
		parallelism,
		checkpointManifest,
		baseURL,
		cacheDir,
		keepCache,
	)
	if err != nil {
		t.Fatalf("applyDownloadConfigDefaults returned error: %v", err)
	}

	if *timeframe != "h1" {
		t.Fatalf("expected timeframe h1, got %q", *timeframe)
	}
	if *side != "bid" {
		t.Fatalf("expected explicit side to be preserved, got %q", *side)
	}
	if !*simpleOutput {
		t.Fatal("expected simple output default to be applied")
	}
	if *fullOutput {
		t.Fatal("expected full output default to be applied")
	}
	if *customColumns != "timestamp,mid_close" {
		t.Fatalf("unexpected custom columns: %q", *customColumns)
	}
	if !*live {
		t.Fatal("expected live default to be applied")
	}
	if *pollInterval != 7*time.Second {
		t.Fatalf("unexpected poll interval: %s", *pollInterval)
	}
	if *retries != 9 {
		t.Fatalf("expected explicit retries to be preserved, got %d", *retries)
	}
	if *retryBackoff != 750*time.Millisecond {
		t.Fatalf("unexpected retry backoff: %s", *retryBackoff)
	}
	if *rateLimit != 150*time.Millisecond {
		t.Fatalf("unexpected rate limit: %s", *rateLimit)
	}
	if !*progress {
		t.Fatal("expected progress default to be applied")
	}
	if !*resume {
		t.Fatal("expected resume default to be applied")
	}
	if *partition != "month" {
		t.Fatalf("unexpected partition: %q", *partition)
	}
	if *parallelism != 3 {
		t.Fatalf("unexpected parallelism: %d", *parallelism)
	}
	if *checkpointManifest != "custom.manifest.json" {
		t.Fatalf("unexpected checkpoint manifest: %q", *checkpointManifest)
	}
	if *baseURL != "https://example.test" {
		t.Fatalf("unexpected base URL: %q", *baseURL)
	}
}

func TestApplyDownloadConfigDefaultsRejectsInvalidDuration(t *testing.T) {
	previousConfig := activeConfig
	defer func() {
		activeConfig = previousConfig
	}()

	activeConfig = &appConfig{
		Download: downloadDefaultsConfig{
			RetryBackoff: "not-a-duration",
		},
	}

	fs := flag.NewFlagSet("download", flag.ContinueOnError)
	timeframe := fs.String("timeframe", "m1", "")
	side := fs.String("side", "BID", "")
	simpleOutput := fs.Bool("simple", false, "")
	fullOutput := fs.Bool("full", false, "")
	customColumns := fs.String("custom-columns", "", "")
	live := fs.Bool("live", false, "")
	pollInterval := fs.Duration("poll-interval", time.Second, "")
	retries := fs.Int("retries", 1, "")
	retryBackoff := fs.Duration("retry-backoff", time.Second, "")
	rateLimit := fs.Duration("rate-limit", time.Second, "")
	progress := fs.Bool("progress", false, "")
	resume := fs.Bool("resume", false, "")
	partition := fs.String("partition", "none", "")
	parallelism := fs.Int("parallelism", 1, "")
	checkpointManifest := fs.String("checkpoint-manifest", "", "")
	baseURL := fs.String("base-url", "https://default.test", "")
	cacheDir := fs.String("cache-dir", "./.dukascopy_cache", "")
	keepCache := fs.Bool("keep-cache", false, "")

	err := applyDownloadConfigDefaults(
		fs,
		timeframe,
		side,
		simpleOutput,
		fullOutput,
		customColumns,
		live,
		pollInterval,
		retries,
		retryBackoff,
		rateLimit,
		progress,
		resume,
		partition,
		parallelism,
		checkpointManifest,
		baseURL,
		cacheDir,
		keepCache,
	)
	if err == nil {
		t.Fatal("expected invalid duration error")
	}
}

func boolPtr(value bool) *bool {
	return &value
}

func intPtr(value int) *int {
	return &value
}

func TestReadConfigFile(t *testing.T) {
	t.Run("loads JSON config successfully", func(t *testing.T) {
		tempDir := t.TempDir()
		path := tempDir + "/config.json"
		content := `{"base_url": "https://json.test", "download": {"timeframe": "m5"}}`
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("failed to write temp config: %v", err)
		}
		config, err := readConfigFile(path)
		if err != nil {
			t.Fatalf("readConfigFile returned error: %v", err)
		}
		if config.BaseURL != "https://json.test" {
			t.Errorf("expected base_url https://json.test, got %q", config.BaseURL)
		}
		if config.Download.Timeframe != "m5" {
			t.Errorf("expected timeframe m5, got %q", config.Download.Timeframe)
		}
	})

	t.Run("loads YAML config successfully", func(t *testing.T) {
		tempDir := t.TempDir()
		path := tempDir + "/config.yaml"
		content := `
base_url: https://yaml.test
download:
  timeframe: m15
`
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("failed to write temp config: %v", err)
		}
		config, err := readConfigFile(path)
		if err != nil {
			t.Fatalf("readConfigFile returned error: %v", err)
		}
		if config.BaseURL != "https://yaml.test" {
			t.Errorf("expected base_url https://yaml.test, got %q", config.BaseURL)
		}
		if config.Download.Timeframe != "m15" {
			t.Errorf("expected timeframe m15, got %q", config.Download.Timeframe)
		}
	})

	t.Run("loads TOML config successfully", func(t *testing.T) {
		tempDir := t.TempDir()
		path := tempDir + "/config.toml"
		content := `
base_url = "https://toml.test"
[download]
timeframe = "d1"
`
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("failed to write temp config: %v", err)
		}
		config, err := readConfigFile(path)
		if err != nil {
			t.Fatalf("readConfigFile returned error: %v", err)
		}
		if config.BaseURL != "https://toml.test" {
			t.Errorf("expected base_url https://toml.test, got %q", config.BaseURL)
		}
		if config.Download.Timeframe != "d1" {
			t.Errorf("expected timeframe d1, got %q", config.Download.Timeframe)
		}
	})
}
