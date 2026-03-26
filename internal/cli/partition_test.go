package cli

import (
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/checkpoint"
	"github.com/Nosvemos/dukascopy-go/internal/csvout"
	"github.com/Nosvemos/dukascopy-go/internal/dukascopy"
)

func TestNormalizePartition(t *testing.T) {
	testCases := []struct {
		name        string
		value       string
		granularity dukascopy.Granularity
		want        string
	}{
		{name: "tick auto uses hour", value: "auto", granularity: dukascopy.GranularityTick, want: partitionHour},
		{name: "minute auto uses day", value: "auto", granularity: dukascopy.GranularityM1, want: partitionDay},
		{name: "hour auto uses month", value: "auto", granularity: dukascopy.GranularityH1, want: partitionMonth},
		{name: "day auto uses year", value: "auto", granularity: dukascopy.GranularityD1, want: partitionYear},
		{name: "week auto uses week", value: "auto", granularity: dukascopy.GranularityW1, want: partitionWeek},
		{name: "explicit partition preserved", value: "month", granularity: dukascopy.GranularityM1, want: partitionMonth},
		{name: "empty becomes none", value: "", granularity: dukascopy.GranularityM1, want: partitionNone},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := normalizePartition(testCase.value, testCase.granularity)
			if err != nil {
				t.Fatalf("normalizePartition returned error: %v", err)
			}
			if got != testCase.want {
				t.Fatalf("expected %q, got %q", testCase.want, got)
			}
		})
	}
}

func TestBuildPartitionsDay(t *testing.T) {
	from := time.Date(2024, 1, 1, 10, 30, 0, 0, time.UTC)
	to := time.Date(2024, 1, 3, 2, 0, 0, 0, time.UTC)

	partitions, err := buildPartitions(from, to, partitionDay)
	if err != nil {
		t.Fatalf("buildPartitions returned error: %v", err)
	}
	if len(partitions) != 3 {
		t.Fatalf("expected 3 partitions, got %d", len(partitions))
	}

	if !partitions[0].Start.Equal(from) || !partitions[0].End.Equal(time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("unexpected first partition: %+v", partitions[0])
	}
	if !partitions[2].Start.Equal(time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC)) || !partitions[2].End.Equal(to) {
		t.Fatalf("unexpected last partition: %+v", partitions[2])
	}
}

func TestPartAndOutputAuditMatches(t *testing.T) {
	part := checkpoint.ManifestPart{Rows: 2, Bytes: 64, SHA256: "abc"}
	output := checkpoint.ManifestOutput{Rows: 2, Bytes: 64, SHA256: "abc"}
	audit := csvout.FileAudit{Rows: 2, Bytes: 64, SHA256: "abc"}

	if !partAuditMatches(part, audit) {
		t.Fatal("expected part audit to match")
	}
	if !outputAuditMatches(output, audit) {
		t.Fatal("expected output audit to match")
	}

	audit.Rows = 3
	if partAuditMatches(part, audit) {
		t.Fatal("expected part audit mismatch on row count")
	}
	if outputAuditMatches(output, audit) {
		t.Fatal("expected output audit mismatch on row count")
	}
}
