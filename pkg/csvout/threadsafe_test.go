package csvout

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

func TestConcurrentTimezoneCSVWriting(t *testing.T) {
	nyc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("failed to load New York timezone: %v", err)
	}

	tokyo, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		t.Fatalf("failed to load Tokyo timezone: %v", err)
	}

	utcTime := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	bars := []dukascopy.Bar{
		{Time: utcTime, Open: 1.0850, Close: 1.0860, Volume: 100},
	}
	instrument := dukascopy.Instrument{Code: "EURUSD", PriceScale: 5}
	columns := []string{"timestamp", "open"}

	tempDir := t.TempDir()
	nycPath := filepath.Join(tempDir, "nyc.csv")
	tokyoPath := filepath.Join(tempDir, "tokyo.csv")

	nycConfig := &Config{
		Location:        nyc,
		TimestampFormat: "2006-01-02 15:04:05",
		CSVDelimiter:    ',',
		HideHeader:      true,
	}

	tokyoConfig := &Config{
		Location:        tokyo,
		TimestampFormat: "2006/01/02 15:04",
		CSVDelimiter:    ';',
		HideHeader:      true,
	}

	var wg sync.WaitGroup
	wg.Add(2)

	var nycErr, tokyoErr error

	go func() {
		defer wg.Done()
		// Sleep briefly to increase chances of interleaving
		time.Sleep(10 * time.Millisecond)
		nycErr = nycConfig.WriteBars(nycPath, instrument, columns, bars, nil, nil)
	}()

	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond)
		tokyoErr = tokyoConfig.WriteBars(tokyoPath, instrument, columns, bars, nil, nil)
	}()

	wg.Wait()

	if nycErr != nil {
		t.Fatalf("New York write failed: %v", nycErr)
	}
	if tokyoErr != nil {
		t.Fatalf("Tokyo write failed: %v", tokyoErr)
	}

	// Verify New York output
	nycData, err := os.ReadFile(nycPath)
	if err != nil {
		t.Fatalf("failed to read New York CSV: %v", err)
	}
	// UTC 12:00 in America/New_York (DST Active, UTC-4) is 08:00:00
	wantNYC := "2026-05-25 08:00:00,1.08500\n"
	if string(nycData) != wantNYC {
		t.Errorf("New York CSV = %q, want %q", string(nycData), wantNYC)
	}

	// Verify Tokyo output
	tokyoData, err := os.ReadFile(tokyoPath)
	if err != nil {
		t.Fatalf("failed to read Tokyo CSV: %v", err)
	}
	// UTC 12:00 in Asia/Tokyo (UTC+9) is 21:00
	wantTokyo := "2026/05/25 21:00;1.08500\n"
	if string(tokyoData) != wantTokyo {
		t.Errorf("Tokyo CSV = %q, want %q", string(tokyoData), wantTokyo)
	}
}
