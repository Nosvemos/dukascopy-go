package csvout

import (
	"testing"
	"time"
)

func TestParseFlexibleTimestamp(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantUTC time.Time
		wantErr bool
	}{
		{
			name:    "RFC3339Nano",
			input:   "2024-01-02T00:01:00Z",
			wantUTC: time.Date(2024, 1, 2, 0, 1, 0, 0, time.UTC),
		},
		{
			name:    "RFC3339Nano with nanoseconds",
			input:   "2024-01-02T00:01:00.123456789Z",
			wantUTC: time.Date(2024, 1, 2, 0, 1, 0, 123456789, time.UTC),
		},
		{
			name:    "RFC3339 with timezone offset",
			input:   "2024-01-02T03:01:00+03:00",
			wantUTC: time.Date(2024, 1, 2, 0, 1, 0, 0, time.UTC),
		},
		{
			name:    "MT4 preset format",
			input:   "2024.01.02 00:01",
			wantUTC: time.Date(2024, 1, 2, 0, 1, 0, 0, time.UTC),
		},
		{
			name:    "MT5 preset format",
			input:   "2024.01.02 00:01:30",
			wantUTC: time.Date(2024, 1, 2, 0, 1, 30, 0, time.UTC),
		},
		{
			name:    "Backtrader preset format",
			input:   "2024-01-02 00:01:30",
			wantUTC: time.Date(2024, 1, 2, 0, 1, 30, 0, time.UTC),
		},
		{
			name:    "NinjaTrader preset format",
			input:   "20240102 000130",
			wantUTC: time.Date(2024, 1, 2, 0, 1, 30, 0, time.UTC),
		},
		{
			name:    "date only",
			input:   "2024-01-02",
			wantUTC: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		},
		{
			name:    "common short format",
			input:   "2024-01-02 00:01",
			wantUTC: time.Date(2024, 1, 2, 0, 1, 0, 0, time.UTC),
		},
		{
			name:    "Unix milliseconds",
			input:   "1704153660000",
			wantUTC: time.Date(2024, 1, 2, 0, 1, 0, 0, time.UTC),
		},
		{
			name:    "whitespace trimmed",
			input:   "  2024-01-02T00:01:00Z  ",
			wantUTC: time.Date(2024, 1, 2, 0, 1, 0, 0, time.UTC),
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
		{
			name:    "whitespace only",
			input:   "   ",
			wantErr: true,
		},
		{
			name:    "garbage input",
			input:   "not-a-timestamp",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseFlexibleTimestamp(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Errorf("parseFlexibleTimestamp(%q) = %v, want error", tc.input, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseFlexibleTimestamp(%q) error = %v", tc.input, err)
			}
			if !got.Equal(tc.wantUTC) {
				t.Errorf("parseFlexibleTimestamp(%q) = %v, want %v", tc.input, got, tc.wantUTC)
			}
		})
	}
}
