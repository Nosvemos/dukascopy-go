package dukascopy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestClientAdditionalBranches(t *testing.T) {
	t.Run("new client panics on bad URL", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("expected NewClient panic for invalid URL")
			}
		}()
		_ = NewClient("http://[::1", time.Second)
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

		client := NewClient(server.URL, time.Second)
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

		client := NewClient(server.URL, time.Second)
		_, err := client.Download(context.Background(), DownloadRequest{
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
		client := NewClient("https://example.test", time.Second).WithRateLimit(5 * time.Millisecond)
		if err := client.waitForRateLimit(context.Background()); err != nil {
			t.Fatalf("waitForRateLimit returned error: %v", err)
		}
	})
}
