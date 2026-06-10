package dukascopy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestClientAdditionalBranches(t *testing.T) {
	t.Run("new client returns error on bad URL", func(t *testing.T) {
		_, err := NewClient("http://[::1", time.Second)
		if err == nil {
			t.Fatal("expected NewClient error for invalid URL")
		}
	})

	t.Run("invalid json and symbol branches", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/v1/instruments":
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"instruments":[`))
			default:
				http.NotFound(w, r)
			}
		}))
		defer server.Close()

		client, err := NewClient(server.URL, time.Second)
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
		if _, err := client.ListInstruments(context.Background()); err == nil {
			t.Fatal("expected invalid JSON error")
		}
	})

	t.Run("unknown symbol and invalid side branches", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/v1/instruments" {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"instruments":[{"id":1,"name":"XAU/USD","code":"XAU-USD","description":"Gold","priceScale":3}]}`))
				return
			}
			http.NotFound(w, r)
		}))
		defer server.Close()

		client, err := NewClient(server.URL, time.Second)
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
		_, err = client.Download(context.Background(), DownloadRequest{
			Symbol:      "eurusd",
			Granularity: GranularityM1,
			Side:        PriceSideBid,
			From:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			To:          time.Date(2024, 1, 2, 0, 1, 0, 0, time.UTC),
		})
		if err == nil {
			t.Fatal("expected unresolved symbol error")
		}

		_, _, err = client.DownloadBarsForSide(context.Background(), DownloadRequest{
			Symbol:      "xauusd",
			Granularity: GranularityM1,
			From:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			To:          time.Date(2024, 1, 2, 0, 1, 0, 0, time.UTC),
		}, PriceSide("bad"))
		if err == nil {
			t.Fatal("expected invalid side error")
		}
	})

	t.Run("waitForRateLimit no wait branch", func(t *testing.T) {
		client, err := NewClient("https://example.test", time.Second)
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
		client = client.WithRateLimit(5 * time.Millisecond)
		if err := client.waitForRateLimit(context.Background()); err != nil {
			t.Fatalf("waitForRateLimit returned error: %v", err)
		}
	})

	t.Run("client options and proxy loader", func(t *testing.T) {
		client, err := NewClient("https://example.test", time.Second)
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
		client.WithForceUpdate(true)
		client.WithEngine(EngineJetta)
		client.WithEngine("")

		tmpProxy := filepath.Join(t.TempDir(), "proxies.txt")
		if err := os.WriteFile(tmpProxy, []byte("http://127.0.0.1:8080\n# comment\n"), 0o644); err != nil {
			t.Fatalf("failed to write tmp proxy file: %v", err)
		}

		if err := client.LoadProxies(tmpProxy); err != nil {
			t.Fatalf("LoadProxies returned error: %v", err)
		}
	})

	t.Run("decoder granularity normalization", func(t *testing.T) {
		if NormalizeGranularity(GranularityTick) != GranularityTick {
			t.Errorf("NormalizeGranularity failed")
		}
	})

	t.Run("instruments cache test", func(t *testing.T) {
		tempFile := filepath.Join(t.TempDir(), "instruments.json")
		setLocalCacheFilePath(tempFile)
		defer func() { setLocalCacheFilePath("") }()

		insts := []Instrument{{ID: 1, Name: "EUR/USD", Code: "EURUSD", PriceScale: 5}}
		saveLocalCache(insts)
		loaded, ok := loadLocalCache()
		if !ok || len(loaded) != 1 || loaded[0].Name != "EUR/USD" {
			t.Fatalf("failed to load saved local cache: %v, ok=%v", loaded, ok)
		}

		// test getLocalCachePath branch
		path := getLocalCachePath()
		if path != tempFile {
			t.Errorf("expected cache path %s, got %s", tempFile, path)
		}
	})
}
