package cli

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/mattn/go-isatty"
)

func isInteractiveTerminal(w io.Writer) bool {
	type fdWriter interface {
		Fd() uintptr
	}
	terminal, ok := w.(fdWriter)
	if !ok {
		return false
	}
	fd := terminal.Fd()
	return isatty.IsTerminal(fd) || isatty.IsCygwinTerminal(fd)
}

func clampFraction(value float64) float64 {
	if value < 0 {
		return 0
	}
	if value > 1 {
		return 1
	}
	return value
}

func partitionRangeLabel(partition downloadPartition) string {
	return partition.Start.Format("2006-01-02") + " -> " + partition.End.Format("2006-01-02")
}

func formatByteCount(value int64) string {
	if value <= 0 {
		return ""
	}
	if value < 1024 {
		return fmt.Sprintf("%d B", value)
	}

	units := []string{"KB", "MB", "GB", "TB"}
	size := float64(value)
	unit := ""
	for _, candidate := range units {
		size /= 1024
		unit = candidate
		if size < 1024 {
			break
		}
	}
	if size >= 100 {
		return fmt.Sprintf("%.0f %s", size, unit)
	}
	if size >= 10 {
		return fmt.Sprintf("%.1f %s", size, unit)
	}
	return fmt.Sprintf("%.2f %s", size, unit)
}

func formatShortDuration(value time.Duration) string {
	if value <= 0 {
		return "-"
	}
	if value < time.Second {
		return "<1s"
	}
	value = value.Round(time.Second)
	if value < time.Minute {
		return fmt.Sprintf("%ds", int(value/time.Second))
	}
	if value < time.Hour {
		return fmt.Sprintf("%dm%02ds", int(value/time.Minute), int((value%time.Minute)/time.Second))
	}
	return fmt.Sprintf("%dh%02dm", int(value/time.Hour), int((value%time.Hour)/time.Minute))
}

func shortenProgressDetail(detail string, maxLen int) string {
	detail = strings.TrimSpace(detail)
	return truncateDisplayWidth(detail, maxLen)
}

func renderASCIIBar(width int, fraction float64) string {
	width = maxInt(width, 3)
	filled := int(clampFraction(fraction) * float64(width))
	if filled > width {
		filled = width
	}
	return "[" + strings.Repeat("=", filled) + strings.Repeat("-", width-filled) + "]"
}

func packSummaryStats(width int, items []string) []string {
	lines := make([]string, 0, len(items))
	current := ""
	limit := maxInt(width, 24)
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if current == "" {
			current = item
			continue
		}
		candidate := current + "  " + item
		if lipgloss.Width(candidate) <= limit {
			current = candidate
			continue
		}
		lines = append(lines, truncateDisplayWidth(current, limit))
		current = item
	}
	if current != "" {
		lines = append(lines, truncateDisplayWidth(current, limit))
	}
	return lines
}

func fitLine(prefix string, value string, width int) string {
	value = strings.TrimSpace(value)
	if value == "" {
		value = "-"
	}
	available := maxInt(1, width-lipgloss.Width(prefix))
	return prefix + truncateDisplayWidth(value, available)
}

func truncateDisplayWidth(value string, maxWidth int) string {
	value = strings.TrimSpace(value)
	if maxWidth <= 0 || value == "" {
		return ""
	}
	if lipgloss.Width(value) <= maxWidth {
		return value
	}
	if maxWidth <= 3 {
		return trimToDisplayWidth(value, maxWidth)
	}
	return trimToDisplayWidth(value, maxWidth-3) + "..."
}

func trimToDisplayWidth(value string, maxWidth int) string {
	if maxWidth <= 0 {
		return ""
	}
	var b strings.Builder
	for _, r := range value {
		candidate := b.String() + string(r)
		if lipgloss.Width(candidate) > maxWidth {
			break
		}
		b.WriteRune(r)
	}
	return b.String()
}

func formatCount(value int) string {
	if value <= 0 {
		return ""
	}
	text := fmt.Sprintf("%d", value)
	if len(text) <= 3 {
		return text
	}
	var groups []string
	for len(text) > 3 {
		groups = append([]string{text[len(text)-3:]}, groups...)
		text = text[:len(text)-3]
	}
	groups = append([]string{text}, groups...)
	return strings.Join(groups, ",")
}

func minInt(left int, right int) int {
	if left < right {
		return left
	}
	return right
}

func intLabel(value int) string {
	if value <= 0 {
		return ""
	}
	return fmt.Sprintf("%d", value)
}

func compactNonEmpty(values []string, emptyFallback string) []string {
	items := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			items = append(items, value)
		}
	}
	if len(items) == 0 {
		return []string{emptyFallback}
	}
	return items
}
