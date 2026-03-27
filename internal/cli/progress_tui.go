package cli

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/dukascopy"
	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/mattn/go-isatty"
)

type progressPrinter struct {
	mu      sync.Mutex
	program *tea.Program
	done    chan struct{}
	closed  bool
}

type progressEventMsg struct {
	event dukascopy.ProgressEvent
}

type progressStatusMsg struct {
	status string
}

type progressPartitionMetricsMsg struct {
	total     int
	completed int
	rows      int
	bytes     int64
}

type progressPartitionStartedMsg struct {
	worker    int
	partition downloadPartition
}

type progressPartitionFinishedMsg struct {
	result partitionWorkResult
}

type progressMetaMsg struct {
	symbol      string
	timeframe   string
	side        string
	outputPath  string
	partition   string
	parallelism int
}

type progressLogMsg struct {
	text string
}

type progressFinishMsg struct{}

type workerSnapshot struct {
	ID        int
	Detail    string
	StartedAt time.Time
}

type progressTUIModel struct {
	bootedAt            time.Time
	throughputStartedAt time.Time
	throughputBaseParts int
	throughputBaseRows  int
	spinner             spinner.Model
	statusText          string
	partitionTotal      int
	partitionCompleted  int
	partitionDetail     string
	completedRows       int
	completedBytes      int64
	chunkScope          string
	chunkCurrent        int
	chunkTotal          int
	chunkDetail         string
	retryAttempt        int
	retryMax            int
	retryDetail         string
	lastRetry           string
	lastError           string
	logs                []string
	workers             map[int]workerSnapshot
	symbol              string
	timeframe           string
	side                string
	outputPath          string
	partitionMode       string
	parallelism         int
	width               int
	height              int
	noColor             bool
}

func newProgressPrinter(w io.Writer) *progressPrinter {
	model := newProgressTUIModel(strings.TrimSpace(os.Getenv("NO_COLOR")) != "")
	options := []tea.ProgramOption{
		tea.WithInput(nil),
		tea.WithOutput(w),
		tea.WithoutSignalHandler(),
	}
	if isInteractiveTerminal(w) {
		options = append(options, tea.WithAltScreen())
	}

	program := tea.NewProgram(model, options...)
	p := &progressPrinter{
		program: program,
		done:    make(chan struct{}),
	}
	go func() {
		_, _ = program.Run()
		close(p.done)
	}()
	return p
}

func newProgressTUIModel(noColor bool) progressTUIModel {
	spin := spinner.New(spinner.WithSpinner(spinner.Dot))
	if !noColor {
		spin.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("81"))
	}

	return progressTUIModel{
		bootedAt:   time.Now(),
		spinner:    spin,
		statusText: "starting",
		width:      72,
		height:     16,
		noColor:    noColor,
		workers:    make(map[int]workerSnapshot),
	}
}

func (p *progressPrinter) Print(event dukascopy.ProgressEvent) {
	p.send(progressEventMsg{event: event})
}

func (p *progressPrinter) SetPartitionTotals(total int, completed int) {
	p.send(progressPartitionMetricsMsg{total: total, completed: completed})
}

func (p *progressPrinter) SetPartitionMetrics(total int, completed int, rows int, bytes int64) {
	p.send(progressPartitionMetricsMsg{
		total:     total,
		completed: completed,
		rows:      rows,
		bytes:     bytes,
	})
}

func (p *progressPrinter) SetStatus(status string) {
	p.send(progressStatusMsg{status: status})
}

func (p *progressPrinter) SetDownloadMeta(symbol string, timeframe string, side string, outputPath string, partition string, parallelism int) {
	p.send(progressMetaMsg{
		symbol:      symbol,
		timeframe:   timeframe,
		side:        side,
		outputPath:  outputPath,
		partition:   partition,
		parallelism: parallelism,
	})
}

func (p *progressPrinter) PartitionStarted(worker int, partition downloadPartition) {
	p.send(progressPartitionStartedMsg{worker: worker, partition: partition})
}

func (p *progressPrinter) PartitionFinished(result partitionWorkResult) {
	p.send(progressPartitionFinishedMsg{result: result})
}

func (p *progressPrinter) Finish() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	program := p.program
	done := p.done
	p.mu.Unlock()

	if program != nil {
		program.Send(progressFinishMsg{})
	}
	if done != nil {
		<-done
	}
}

func (p *progressPrinter) Write(data []byte) (int, error) {
	text := strings.TrimSpace(string(data))
	if text != "" {
		p.send(progressLogMsg{text: text})
	}
	return len(data), nil
}

func (p *progressPrinter) send(msg tea.Msg) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed || p.program == nil {
		return
	}
	p.program.Send(msg)
}

func (m progressTUIModel) Init() tea.Cmd {
	return m.spinner.Tick
}

func (m progressTUIModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil
	case progressEventMsg:
		switch msg.event.Kind {
		case "chunk":
			m.chunkScope = msg.event.Scope
			m.chunkCurrent = msg.event.Current
			m.chunkTotal = msg.event.Total
			m.chunkDetail = msg.event.Detail
			m.retryAttempt = 0
			m.retryMax = 0
			m.retryDetail = ""
			if strings.TrimSpace(m.statusText) == "" || m.statusText == "starting" {
				m.statusText = "downloading"
				m.startThroughputWindow()
			}
		case "retry":
			m.retryAttempt = msg.event.Attempt
			m.retryMax = msg.event.MaxAttempt
			m.retryDetail = msg.event.Detail
			m.lastRetry = fmt.Sprintf("%d/%d  %s", msg.event.Attempt, msg.event.MaxAttempt, shortenProgressDetail(msg.event.Detail, 68))
			m.pushLog("retry " + m.lastRetry)
		}
		return m, nil
	case progressStatusMsg:
		status := strings.TrimSpace(msg.status)
		if status == "" || status == m.statusText {
			return m, nil
		}
		wasDownloading := strings.Contains(strings.ToLower(m.statusText), "download")
		nowDownloading := strings.Contains(strings.ToLower(status), "download")
		m.statusText = status
		if nowDownloading && !wasDownloading {
			m.startThroughputWindow()
		}
		if strings.Contains(strings.ToLower(status), "failed") {
			m.lastError = status
		}
		if strings.Contains(strings.ToLower(status), "checkpoint") ||
			strings.Contains(strings.ToLower(status), "assembling") ||
			strings.Contains(strings.ToLower(status), "verified") ||
			strings.Contains(strings.ToLower(status), "completed") ||
			strings.Contains(strings.ToLower(status), "failed") {
			m.pushLog(status)
		}
		return m, nil
	case progressMetaMsg:
		m.symbol = strings.TrimSpace(msg.symbol)
		m.timeframe = strings.TrimSpace(msg.timeframe)
		m.side = strings.TrimSpace(msg.side)
		m.outputPath = strings.TrimSpace(msg.outputPath)
		m.partitionMode = strings.TrimSpace(msg.partition)
		m.parallelism = msg.parallelism
		return m, nil
	case progressPartitionMetricsMsg:
		m.partitionTotal = msg.total
		m.partitionCompleted = msg.completed
		m.completedRows = msg.rows
		m.completedBytes = msg.bytes
		return m, nil
	case progressPartitionStartedMsg:
		m.partitionDetail = partitionRangeLabel(msg.partition)
		m.workers[msg.worker] = workerSnapshot{
			ID:        msg.worker,
			Detail:    partitionRangeLabel(msg.partition),
			StartedAt: time.Now(),
		}
		if strings.TrimSpace(m.statusText) == "" || m.statusText == "starting" {
			m.statusText = "downloading"
			m.startThroughputWindow()
		}
		return m, nil
	case progressPartitionFinishedMsg:
		delete(m.workers, msg.result.Worker)
		m.partitionCompleted++
		if msg.result.Err == nil {
			m.completedRows += msg.result.Audit.Rows
			m.completedBytes += msg.result.Audit.Bytes
		} else {
			m.partitionDetail = shortenProgressDetail(msg.result.Err.Error(), 68)
			m.statusText = "failed"
			m.lastError = shortenProgressDetail(msg.result.Err.Error(), 120)
			m.pushLog("failed: " + shortenProgressDetail(msg.result.Err.Error(), 68))
		}
		return m, nil
	case progressLogMsg:
		m.pushLog(msg.text)
		return m, nil
	case progressFinishMsg:
		return m, tea.Quit
	default:
		return m, nil
	}
}

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

func (m *progressTUIModel) startThroughputWindow() {
	m.throughputStartedAt = time.Now()
	m.throughputBaseParts = m.partitionCompleted
	m.throughputBaseRows = m.completedRows
}

func (m *progressTUIModel) pushLog(line string) {
	line = strings.TrimSpace(line)
	if line == "" {
		return
	}
	if len(m.logs) > 0 && m.logs[len(m.logs)-1] == line {
		return
	}
	m.logs = append(m.logs, line)
	if len(m.logs) > 4 {
		m.logs = m.logs[len(m.logs)-4:]
	}
}

func (m progressTUIModel) progressFraction() float64 {
	switch {
	case m.partitionTotal > 0:
		return clampFraction(float64(m.partitionCompleted) / float64(m.partitionTotal))
	case m.chunkTotal > 0:
		return clampFraction(float64(m.chunkCurrent) / float64(m.chunkTotal))
	default:
		return 0
	}
}

func (m progressTUIModel) partitionSummary() string {
	if m.partitionTotal <= 0 {
		return "-"
	}
	text := fmt.Sprintf("%d/%d", minInt(m.partitionCompleted, m.partitionTotal), m.partitionTotal)
	if active := len(m.workers); active > 0 {
		text += fmt.Sprintf("  active %d", active)
	}
	return text
}

func (m progressTUIModel) chunkSummary() string {
	if m.chunkTotal <= 0 {
		return "-"
	}
	text := fmt.Sprintf("%s %d/%d", defaultString(m.chunkScope, "download"), m.chunkCurrent, m.chunkTotal)
	if strings.TrimSpace(m.chunkDetail) != "" {
		text += "  " + shortenProgressDetail(m.chunkDetail, 24)
	}
	return text
}

func (m progressTUIModel) speedText() string {
	elapsed, rowDelta, partDelta := m.throughputSnapshot()
	if elapsed <= 0 {
		return ""
	}

	switch {
	case rowDelta > 0:
		return fmt.Sprintf("%.0f rows/s", float64(rowDelta)/elapsed.Seconds())
	case partDelta > 0:
		return fmt.Sprintf("%.1f part/min", float64(partDelta)/elapsed.Minutes())
	default:
		return ""
	}
}

func (m progressTUIModel) etaText() string {
	elapsed, _, partDelta := m.throughputSnapshot()
	if elapsed <= 0 {
		return ""
	}

	switch {
	case m.partitionTotal > 0:
		current := m.partitionCompleted
		total := m.partitionTotal
		if partDelta <= 0 || current <= 0 || current >= total {
			return ""
		}
		remainingParts := total - current
		remaining := time.Duration(float64(elapsed) * float64(remainingParts) / float64(partDelta))
		return formatShortDuration(remaining)
	case m.chunkTotal > 0:
		current := m.chunkCurrent
		total := m.chunkTotal
		if current <= 0 || current >= total {
			return ""
		}
		remaining := time.Duration(float64(elapsed) * float64(total-current) / float64(current))
		return formatShortDuration(remaining)
	default:
		return ""
	}
}

func (m progressTUIModel) throughputSnapshot() (time.Duration, int, int) {
	if m.throughputStartedAt.IsZero() {
		return 0, 0, 0
	}
	elapsed := time.Since(m.throughputStartedAt)
	if elapsed <= 0 {
		return 0, 0, 0
	}
	return elapsed, m.completedRows - m.throughputBaseRows, m.partitionCompleted - m.throughputBaseParts
}

func (m progressTUIModel) workerLines() []string {
	if len(m.workers) == 0 {
		return []string{m.subtleStyle().Render("idle")}
	}

	ids := make([]int, 0, len(m.workers))
	for id := range m.workers {
		ids = append(ids, id)
	}
	sort.Ints(ids)

	lines := make([]string, 0, len(ids))
	for _, id := range ids {
		worker := m.workers[id]
		line := fmt.Sprintf("#%d  %s  %s", id, shortenProgressDetail(worker.Detail, 28), formatShortDuration(time.Since(worker.StartedAt)))
		lines = append(lines, m.valueStyle().Render(line))
	}
	return lines
}

func (m progressTUIModel) metric(label string, value string) string {
	return fmt.Sprintf("%s %s", m.labelStyle().Render(label), m.valueStyle().Render(value))
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
