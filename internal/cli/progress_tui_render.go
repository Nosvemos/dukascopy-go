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
	if m.noColor {
		return style
	}
	switch m.theme {
	case "catppuccin":
		return style.Foreground(lipgloss.Color("#cba6f7")) // Mauve
	case "nord":
		return style.Foreground(lipgloss.Color("#88c0d0")) // Frost Cyan
	case "gruvbox":
		return style.Foreground(lipgloss.Color("#fabd2f")) // Warm Yellow
	case "dracula":
		return style.Foreground(lipgloss.Color("#ff79c6")) // Pink
	default:
		return style.Foreground(lipgloss.Color("159"))
	}
}

func (m progressTUIModel) subtleStyle() lipgloss.Style {
	style := lipgloss.NewStyle().Faint(true)
	if m.noColor {
		return style
	}
	switch m.theme {
	case "catppuccin":
		return style.Foreground(lipgloss.Color("#585b70")) // Surface2
	case "nord":
		return style.Foreground(lipgloss.Color("#4c566a")) // Polar Night Gray
	case "gruvbox":
		return style.Foreground(lipgloss.Color("#928374")) // Gray
	case "dracula":
		return style.Foreground(lipgloss.Color("#6272a4")) // Comment Purple-gray
	default:
		return style.Foreground(lipgloss.Color("244"))
	}
}

func (m progressTUIModel) labelStyle() lipgloss.Style {
	style := lipgloss.NewStyle().Bold(true)
	if m.noColor {
		return style
	}
	switch m.theme {
	case "catppuccin":
		return style.Foreground(lipgloss.Color("#89b4fa")) // Blue
	case "nord":
		return style.Foreground(lipgloss.Color("#81a1c1")) // Frost Blue
	case "gruvbox":
		return style.Foreground(lipgloss.Color("#83a598")) // Blue
	case "dracula":
		return style.Foreground(lipgloss.Color("#8be9fd")) // Cyan
	default:
		return style.Foreground(lipgloss.Color("86"))
	}
}

func (m progressTUIModel) valueStyle() lipgloss.Style {
	style := lipgloss.NewStyle()
	if m.noColor {
		return style
	}
	switch m.theme {
	case "catppuccin":
		return style.Foreground(lipgloss.Color("#cdd6f4")) // Text
	case "nord":
		return style.Foreground(lipgloss.Color("#d8dee9")) // Snow Storm White
	case "gruvbox":
		return style.Foreground(lipgloss.Color("#ebdbb2")) // Warm White
	case "dracula":
		return style.Foreground(lipgloss.Color("#f8f8f2")) // Foreground White
	default:
		return style.Foreground(lipgloss.Color("230"))
	}
}

func (m progressTUIModel) percentStyle() lipgloss.Style {
	style := lipgloss.NewStyle().Bold(true)
	if m.noColor {
		return style
	}
	switch m.theme {
	case "catppuccin":
		return style.Foreground(lipgloss.Color("#a6e3a1")) // Green
	case "nord":
		return style.Foreground(lipgloss.Color("#a3be8c")) // Aurora Green
	case "gruvbox":
		return style.Foreground(lipgloss.Color("#b8bb26")) // Green
	case "dracula":
		return style.Foreground(lipgloss.Color("#50fa7b")) // Green
	default:
		return style.Foreground(lipgloss.Color("121"))
	}
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

	var bg, fg string
	switch m.theme {
	case "catppuccin":
		fg = "#11111b"
		switch {
		case strings.Contains(status, "failed"), strings.Contains(status, "error"):
			bg = "#f38ba8"
		case strings.Contains(status, "assembling"), strings.Contains(status, "verified"), strings.Contains(status, "completed"):
			bg = "#a6e3a1"
		case strings.Contains(status, "checkpoint"), strings.Contains(status, "scan"):
			bg = "#f9e2af"
		case strings.Contains(status, "download"):
			bg = "#89b4fa"
		default:
			bg = "#bac2de"
		}
	case "nord":
		fg = "#2e3440"
		switch {
		case strings.Contains(status, "failed"), strings.Contains(status, "error"):
			bg = "#bf616a"
		case strings.Contains(status, "assembling"), strings.Contains(status, "verified"), strings.Contains(status, "completed"):
			bg = "#a3be8c"
		case strings.Contains(status, "checkpoint"), strings.Contains(status, "scan"):
			bg = "#ebcb8b"
		case strings.Contains(status, "download"):
			bg = "#81a1c1"
		default:
			bg = "#d8dee9"
		}
	case "gruvbox":
		fg = "#282828"
		switch {
		case strings.Contains(status, "failed"), strings.Contains(status, "error"):
			bg = "#fb4934"
		case strings.Contains(status, "assembling"), strings.Contains(status, "verified"), strings.Contains(status, "completed"):
			bg = "#b8bb26"
		case strings.Contains(status, "checkpoint"), strings.Contains(status, "scan"):
			bg = "#fabd2f"
		case strings.Contains(status, "download"):
			bg = "#83a598"
		default:
			bg = "#a89984"
		}
	case "dracula":
		fg = "#282a36"
		switch {
		case strings.Contains(status, "failed"), strings.Contains(status, "error"):
			bg = "#ff5555"
		case strings.Contains(status, "assembling"), strings.Contains(status, "verified"), strings.Contains(status, "completed"):
			bg = "#50fa7b"
		case strings.Contains(status, "checkpoint"), strings.Contains(status, "scan"):
			bg = "#f1fa8c"
		case strings.Contains(status, "download"):
			bg = "#bd93f9"
		default:
			bg = "#f8f8f2"
		}
	default:
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

	style = style.Foreground(lipgloss.Color(fg)).Background(lipgloss.Color(bg))
	return style.Render(label)
}
