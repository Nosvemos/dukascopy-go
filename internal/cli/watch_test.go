package cli

import (
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

func TestParseWatchCondition(t *testing.T) {
	eval, err := parseWatchCondition("price > 2500")
	if err != nil {
		t.Fatalf("parseWatchCondition: %v", err)
	}
	if eval.metric != "price" || eval.operator != ">" || eval.target != 2500 {
		t.Errorf("unexpected evaluator: %+v", eval)
	}

	bars := []dukascopy.Bar{
		{Time: time.Now(), Close: 2505.5},
	}
	triggered, val, msg := eval.Evaluate(bars)
	if !triggered {
		t.Errorf("expected triggered true, got false (val: %f, msg: %s)", val, msg)
	}

	// Below target
	bars[0].Close = 2490.0
	triggered, _, _ = eval.Evaluate(bars)
	if triggered {
		t.Errorf("expected triggered false, got true")
	}
}
