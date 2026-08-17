package dukascopy

import (
	"testing"
	"time"
)

func TestCleanTickOutliers(t *testing.T) {
	now := time.Now().UTC()
	ticks := make([]Tick, 20)
	for i := 0; i < 20; i++ {
		ticks[i] = Tick{
			Time: now.Add(time.Duration(i) * time.Second),
			Bid:  1.0800,
			Ask:  1.0802,
		}
	}
	// Inject extreme spike at index 10
	ticks[10].Bid = 50.0000
	ticks[10].Ask = 50.0002

	cleaned := CleanTickOutliers(ticks, 7, 3.0)
	if len(cleaned) != 19 {
		t.Fatalf("expected 19 ticks after removing spike, got %d", len(cleaned))
	}
	for _, tick := range cleaned {
		if tick.Bid > 2.0 {
			t.Errorf("found uncleaned spike tick: %f", tick.Bid)
		}
	}
}

func TestParseOutlierConfig(t *testing.T) {
	cfg, err := ParseOutlierConfig("median:9:4.5")
	if err != nil {
		t.Fatalf("ParseOutlierConfig: %v", err)
	}
	if !cfg.Enabled || cfg.Window != 9 || cfg.Threshold != 4.5 {
		t.Errorf("unexpected parsed config: %+v", cfg)
	}

	cfgAuto, err := ParseOutlierConfig("auto")
	if err != nil || !cfgAuto.Enabled {
		t.Errorf("auto config failed: %v", err)
	}
}
