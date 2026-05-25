package cli

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
)

func (m progressTUIModel) View() string {
	width := 72
	if m.width > 0 {
		width = minInt(maxInt(m.width-1, 38), 84)
	}
	height := 0
	if m.height > 0 {
		height = maxInt(m.height-1, 3)
	}

	lines := []string{
		m.renderHeader(width),
		m.subtleStyle().Render(strings.Repeat("-", width)),
		m.renderStatusLine(width),
	}

	lines = append(lines, m.renderSummaryLines(width)...)
	lines = append(lines, m.renderActivityLines(width)...)
	if height > 0 {
		lines = m.trimLinesForHeight(lines, width, height)
	}

	return strings.Join(lines, "\n")
}

func (m progressTUIModel) renderHeader(width int) string {
	left := lipgloss.JoinHorizontal(lipgloss.Left, m.titleStyle().Render("DUKASCOPY-GO"), " ", m.phaseBadge())
	right := m.subtleStyle().Render("elapsed " + formatShortDuration(time.Since(m.bootedAt)))
	if lipgloss.Width(left)+2+lipgloss.Width(right) <= width {
		return lipgloss.JoinHorizontal(lipgloss.Left, left, strings.Repeat(" ", width-lipgloss.Width(left)-lipgloss.Width(right)), right)
	}
	return lipgloss.JoinVertical(lipgloss.Left, left, right)
}

func (m progressTUIModel) renderStatusLine(width int) string {
	percent := fmt.Sprintf("%3.0f%%", m.progressFraction()*100)
	progressLabel := truncateDisplayWidth(m.progressLabel(), maxInt(0, width/4))
	spinnerWidth := lipgloss.Width(m.spinner.View())
	percentWidth := lipgloss.Width(percent)
	labelWidth := 0
	if progressLabel != "" {
		labelWidth = lipgloss.Width(progressLabel) + 2
	}
	barWidth := minInt(maxInt(width-spinnerWidth-percentWidth-labelWidth-14, 8), 28)
	bar := renderASCIIBar(barWidth, m.progressFraction())
	barDisplayWidth := lipgloss.Width(bar)
	statusWidth := maxInt(4, width-spinnerWidth-percentWidth-labelWidth-barDisplayWidth-3)
	status := truncateDisplayWidth(defaultString(m.statusText, "starting"), statusWidth)
	line := fmt.Sprintf("%s %s %s %s", m.spinner.View(), status, bar, percent)
	if progressLabel != "" {
		remaining := maxInt(1, width-lipgloss.Width(line)-2)
		line += "  " + truncateDisplayWidth(progressLabel, remaining)
	}
	if lipgloss.Width(line) > width {
		line = truncateDisplayWidth(line, width)
	}
	return line
}

func (m progressTUIModel) renderSummaryLines(width int) []string {
	lines := []string{
		m.renderKVLine("pair", strings.Join(compactNonEmpty([]string{m.symbol, m.timeframe, m.side}, "-"), "  "), width),
		m.renderKVLine("mode", strings.Join(compactNonEmpty([]string{m.partitionMode, "workers " + defaultString(intLabel(m.parallelism), "-")}, "-"), "  "), width),
		m.renderKVLine("out", defaultString(m.outputPath, "-"), width),
	}

	stats := []string{
		"parts " + defaultString(m.partitionSummary(), "-"),
		"chunk " + defaultString(m.chunkSummary(), "-"),
		"rows " + defaultString(formatCount(m.completedRows), "-"),
		"size " + defaultString(formatByteCount(m.completedBytes), "-"),
		"speed " + defaultString(m.speedText(), "-"),
		"eta " + defaultString(m.etaText(), "-"),
	}

	for _, group := range packSummaryStats(width, stats) {
		lines = append(lines, m.valueStyle().Render(group))
	}

	current := strings.TrimSpace(m.partitionDetail)
	if current != "" {
		lines = append(lines, m.renderKVLine("current", current, width))
	}

	return lines
}

func (m progressTUIModel) renderActivityLines(width int) []string {
	lines := make([]string, 0, 6)

	workerLines := m.renderWorkerSummary(width)
	lines = append(lines, workerLines...)

	if strings.TrimSpace(m.lastRetry) != "" {
		lines = append(lines, m.renderKVLine("retry", m.lastRetry, width))
	}
	if strings.TrimSpace(m.lastError) != "" {
		lines = append(lines, m.renderKVLine("error", m.lastError, width))
	}

	if len(m.logs) == 0 {
		lines = append(lines, m.subtleStyle().Render("recent waiting for events"))
		return lines
	}

	start := 0
	if len(m.logs) > 2 {
		start = len(m.logs) - 2
	}
	for _, line := range m.logs[start:] {
		lines = append(lines, m.renderKVLine("recent", line, width))
	}
	return lines
}

func (m progressTUIModel) renderWorkerSummary(width int) []string {
	if len(m.workers) == 0 {
		return []string{m.subtleStyle().Render("workers idle")}
	}

	ids := make([]int, 0, len(m.workers))
	for id := range m.workers {
		ids = append(ids, id)
	}
	sort.Ints(ids)

	lines := make([]string, 0, minInt(len(ids), 3))
	visible := minInt(len(ids), 2)
	for _, id := range ids[:visible] {
		worker := m.workers[id]
		value := fmt.Sprintf("#%d %s (%s)", id, worker.Detail, formatShortDuration(time.Since(worker.StartedAt)))
		lines = append(lines, m.renderKVLine("worker", value, width))
	}
	if len(ids) > visible {
		lines = append(lines, m.subtleStyle().Render(fmt.Sprintf("workers +%d more active", len(ids)-visible)))
	}
	return lines
}

func (m progressTUIModel) trimLinesForHeight(lines []string, width int, height int) []string {
	if height <= 0 || len(lines) <= height {
		return lines
	}
	if height == 1 {
		return []string{truncateDisplayWidth(lines[0], width)}
	}

	hidden := len(lines) - (height - 1)
	trimmed := append([]string{}, lines[:height-1]...)
	notice := m.subtleStyle().Render(truncateDisplayWidth(fmt.Sprintf("+%d lines hidden due to terminal height", hidden), width))
	return append(trimmed, notice)
}

func (m progressTUIModel) renderKVLine(label string, value string, width int) string {
	prefix := lipgloss.Width(label) + 1
	plain := fitLine(label+" ", defaultString(value, "-"), width)
	if !m.noColor {
		value = truncateDisplayWidth(defaultString(value, "-"), maxInt(1, width-prefix))
		return m.labelStyle().Render(label) + " " + m.valueStyle().Render(value)
	}
	return plain
}

func (m progressTUIModel) progressLabel() string {
	if m.partitionTotal > 0 {
		return fmt.Sprintf("part %d/%d", minInt(m.partitionCompleted, m.partitionTotal), m.partitionTotal)
	}
	if m.chunkTotal > 0 {
		return fmt.Sprintf("%s %d/%d", defaultString(m.chunkScope, "chunk"), m.chunkCurrent, m.chunkTotal)
	}
	return ""
}

func (m progressTUIModel) titleStyle() lipgloss.Style {
	style := lipgloss.NewStyle().Bold(true)
	if !m.noColor {
		style = style.Foreground(lipgloss.Color("159"))
	}
	return style
}

func (m progressTUIModel) subtleStyle() lipgloss.Style {
	style := lipgloss.NewStyle().Faint(true)
	if !m.noColor {
		style = style.Foreground(lipgloss.Color("244"))
	}
	return style
}

func (m progressTUIModel) labelStyle() lipgloss.Style {
	style := lipgloss.NewStyle().Bold(true)
	if !m.noColor {
		style = style.Foreground(lipgloss.Color("86"))
	}
	return style
}

func (m progressTUIModel) valueStyle() lipgloss.Style {
	style := lipgloss.NewStyle()
	if !m.noColor {
		style = style.Foreground(lipgloss.Color("230"))
	}
	return style
}

func (m progressTUIModel) percentStyle() lipgloss.Style {
	style := lipgloss.NewStyle().Bold(true)
	if !m.noColor {
		style = style.Foreground(lipgloss.Color("121"))
	}
	return style
}

func (m progressTUIModel) phaseBadge() string {
	status := strings.ToLower(strings.TrimSpace(m.statusText))
	label := strings.ToUpper(defaultString(m.statusText, "starting"))
	if len(label) > 24 {
		label = strings.ToUpper(shortenProgressDetail(label, 24))
	}

	style := lipgloss.NewStyle().Bold(true).Padding(0, 1)
	if m.noColor {
		return style.Render(label)
	}

	switch {
	case strings.Contains(status, "failed"), strings.Contains(status, "error"):
		style = style.Foreground(lipgloss.Color("231")).Background(lipgloss.Color("160"))
	case strings.Contains(status, "assembling"), strings.Contains(status, "verified"), strings.Contains(status, "completed"):
		style = style.Foreground(lipgloss.Color("16")).Background(lipgloss.Color("114"))
	case strings.Contains(status, "checkpoint"), strings.Contains(status, "scan"):
		style = style.Foreground(lipgloss.Color("16")).Background(lipgloss.Color("221"))
	case strings.Contains(status, "download"):
		style = style.Foreground(lipgloss.Color("16")).Background(lipgloss.Color("81"))
	default:
		style = style.Foreground(lipgloss.Color("16")).Background(lipgloss.Color("252"))
	}
	return style.Render(label)
}
