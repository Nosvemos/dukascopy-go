package csvout

import (
	"testing"
	"time"
)

func TestInferTimeframeAliases(t *testing.T) {
	cases := []struct {
		intervals []time.Duration
		want      string
	}{
		{[]time.Duration{time.Millisecond}, "1ms"},
		{[]time.Duration{time.Second}, "1s"},
		{[]time.Duration{3 * time.Minute}, "m3"},
		{[]time.Duration{5 * time.Minute}, "m5"},
		{[]time.Duration{15 * time.Minute}, "m15"},
		{[]time.Duration{30 * time.Minute}, "m30"},
		{[]time.Duration{time.Hour}, "h1"},
		{[]time.Duration{4 * time.Hour}, "h4"},
		{[]time.Duration{24 * time.Hour}, "d1"},
		{[]time.Duration{7 * 24 * time.Hour}, "w1"},
	}

	for _, testCase := range cases {
		if got := inferTimeframe(testCase.intervals); got != testCase.want {
			t.Fatalf("inferTimeframe(%v) = %q, want %q", testCase.intervals, got, testCase.want)
		}
	}
}
