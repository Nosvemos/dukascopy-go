package cli

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func getPartitionKey(t time.Time, mode string) string {
	t = t.UTC()
	switch mode {
	case partitionHour:
		return t.Format("20060102T15")
	case partitionDay:
		return t.Format("20060102")
	case partitionWeek:
		year, week := t.ISOWeek()
		return fmt.Sprintf("%04dW%02d", year, week)
	case partitionMonth:
		return t.Format("200601")
	case partitionYear:
		return t.Format("2006")
	default:
		return ""
	}
}

func getPartitionOutputPath(outputPath string, key string) string {
	ext := filepath.Ext(outputPath)
	base := strings.TrimSuffix(outputPath, ext)
	if ext == ".gz" && strings.HasSuffix(strings.ToLower(base), ".csv") {
		ext = ".csv.gz"
		base = strings.TrimSuffix(base, ".csv")
	}
	return fmt.Sprintf("%s_%s%s", base, key, ext)
}

func parquetValueForColumn(column string, value string) (any, error) {
	if strings.EqualFold(strings.TrimSpace(column), "timestamp") {
		return value, nil
	}
	number, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return nil, fmt.Errorf("parse parquet numeric value for column %q: %w", column, err)
	}
	return number, nil
}

func indexOfColumn(columns []string, name string) int {
	for i, col := range columns {
		if strings.EqualFold(strings.TrimSpace(col), name) {
			return i
		}
	}
	return -1
}
