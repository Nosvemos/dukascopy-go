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

	// Test <=, >=, ==, < operators
	evalLE, _ := parseWatchCondition("close <= 2500")
	trigLE, _, _ := evalLE.Evaluate(bars)
	if !trigLE {
		t.Errorf("expected true for close <= 2500 with 2490.0")
	}

	evalGE, _ := parseWatchCondition("high >= 2600")
	bars[0].High = 2650.0
	trigGE, _, _ := evalGE.Evaluate(bars)
	if !trigGE {
		t.Errorf("expected true for high >= 2600 with 2650.0")
	}

	evalEQ, _ := parseWatchCondition("open == 2480")
	bars[0].Open = 2480.0
	trigEQ, _, _ := evalEQ.Evaluate(bars)
	if !trigEQ {
		t.Errorf("expected true for open == 2480")
	}

	evalVol, _ := parseWatchCondition("volume > 1000")
	bars[0].Volume = 5000.0
	trigVol, _, _ := evalVol.Evaluate(bars)
	if !trigVol {
		t.Errorf("expected true for volume > 1000")
	}

	// Empty bars evaluation
	trigEmpty, _, _ := evalLE.Evaluate(nil)
	if trigEmpty {
		t.Errorf("expected false on empty bars")
	}

	// Invalid condition formats
	if _, err := parseWatchCondition("invalid condition without op"); err == nil {
		t.Errorf("expected error for condition without operator")
	}
	if _, err := parseWatchCondition("price > not_a_number"); err == nil {
		t.Errorf("expected error for non-number target")
	}
}
