package csvout

import (
	"fmt"
	"strings"
	"time"
)

func parseColumns(value string, allowed map[string]struct{}) ([]string, error) {
	parts := strings.Split(value, ",")
	columns := make([]string, 0, len(parts))
	for _, part := range parts {
		column := strings.TrimSpace(strings.ToLower(part))
		if column == "" {
			continue
		}
		if _, ok := allowed[column]; !ok {
			return nil, fmt.Errorf("unsupported column %q", column)
		}
		columns = append(columns, column)
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("at least one column must be provided")
	}
	return columns, nil
}

func cloneColumns(columns []string) []string {
	cloned := make([]string, len(columns))
	copy(cloned, columns)
	return cloned
}

func recordsEqual(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func indexOfColumn(columns []string, needle string) int {
	for index, column := range columns {
		if strings.EqualFold(strings.TrimSpace(column), needle) {
			return index
		}
	}
	return -1
}

func HeadersMatch(expected []string, actual []string) bool {
	if len(expected) != len(actual) {
		return false
	}
	for index := range expected {
		if expected[index] != actual[index] {
			return false
		}
	}
	return true
}

func timeframeInterval(timeframe string) time.Duration {
	switch strings.ToLower(strings.TrimSpace(timeframe)) {
	case "m1", "minute":
		return time.Minute
	case "m3":
		return 3 * time.Minute
	case "m5":
		return 5 * time.Minute
	case "m15":
		return 15 * time.Minute
	case "m30":
		return 30 * time.Minute
	case "h1", "hour":
		return time.Hour
	case "h4":
		return 4 * time.Hour
	case "d1", "day":
		return 24 * time.Hour
	case "w1":
		return 7 * 24 * time.Hour
	default:
		return 0
	}
}
