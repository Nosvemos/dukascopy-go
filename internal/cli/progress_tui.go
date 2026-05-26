package cli

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
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
	from        time.Time
	to          time.Time
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
	throughputBaseBytes int64
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
	theme               string
}

func newProgressPrinter(w io.Writer, theme string) *progressPrinter {
	model := newProgressTUIModel(strings.TrimSpace(os.Getenv("NO_COLOR")) != "", theme)
	options := []tea.ProgramOption{
		tea.WithInput(nil),
		tea.WithOutput(w),
		tea.WithoutSignalHandler(),
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

func newProgressTUIModel(noColor bool, theme string) progressTUIModel {
	if theme == "" {
		theme = "default"
	}
	theme = strings.ToLower(theme)

	spin := spinner.New(spinner.WithSpinner(spinner.Dot))
	if !noColor {
		switch theme {
		case "catppuccin":
			spin.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("#cba6f7"))
		case "nord":
			spin.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("#88c0d0"))
		case "gruvbox":
			spin.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("#fabd2f"))
		case "dracula":
			spin.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("#bd93f9"))
		default:
			spin.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("81"))
		}
	}

	return progressTUIModel{
		bootedAt:   time.Now(),
		spinner:    spin,
		statusText: "starting",
		width:      72,
		height:     16,
		noColor:    noColor,
		workers:    make(map[int]workerSnapshot),
		theme:      theme,
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

func (p *progressPrinter) SetDownloadMeta(symbol string, timeframe string, side string, outputPath string, partition string, parallelism int, from time.Time, to time.Time) {
	p.send(progressMetaMsg{
		symbol:      symbol,
		timeframe:   timeframe,
		side:        side,
		outputPath:  outputPath,
		partition:   partition,
		parallelism: parallelism,
		from:        from,
		to:          to,
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
			if m.partitionTotal <= 0 {
				m.completedRows = msg.event.Rows
				m.completedBytes = msg.event.Bytes
			}
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

		if m.partitionMode == "none" {
			var chunkTotal int
			var scope string
			switch m.timeframe {
			case "tick":
				scope = "tick"
				for current := hourStartUTC(msg.from); current.Before(msg.to); current = current.Add(time.Hour) {
					if !dukascopy.IsMarketClosed(m.symbol, current) {
						chunkTotal++
					}
				}
			case "m1", "m3", "m5", "m15", "m30":
				scope = "minute"
				for current := midnightUTC(msg.from); current.Before(msg.to); current = current.AddDate(0, 0, 1) {
					if dukascopy.IsCryptoSymbol(m.symbol) || current.UTC().Weekday() != time.Saturday {
						chunkTotal++
					}
				}
			case "h1", "h4":
				scope = "hour"
				for current := monthStartUTC(msg.from); current.Before(msg.to); current = current.AddDate(0, 1, 0) {
					chunkTotal++
				}
			case "d1", "w1", "mn1":
				scope = "day"
				for year := msg.from.Year(); year <= msg.to.Add(-time.Nanosecond).Year(); year++ {
					chunkTotal++
				}
			}
			m.chunkTotal = chunkTotal
			m.chunkScope = scope
		}
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

func (m *progressTUIModel) startThroughputWindow() {
	m.throughputStartedAt = time.Now()
	m.throughputBaseParts = m.partitionCompleted
	m.throughputBaseRows = m.completedRows
	m.throughputBaseBytes = m.completedBytes
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
	elapsed, rowDelta, partDelta, byteDelta := m.throughputSnapshot()
	if elapsed <= 0 {
		return ""
	}

	switch {
	case byteDelta > 0:
		return fmt.Sprintf("%s/s (%.0f rows/s)", formatByteCount(byteDelta), float64(rowDelta)/elapsed.Seconds())
	case rowDelta > 0:
		return fmt.Sprintf("%.0f rows/s", float64(rowDelta)/elapsed.Seconds())
	case partDelta > 0:
		return fmt.Sprintf("%.1f part/min", float64(partDelta)/elapsed.Minutes())
	default:
		return ""
	}
}

func (m progressTUIModel) etaText() string {
	elapsed, _, partDelta, _ := m.throughputSnapshot()
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

func (m progressTUIModel) throughputSnapshot() (time.Duration, int, int, int64) {
	if m.throughputStartedAt.IsZero() {
		return 0, 0, 0, 0
	}
	elapsed := time.Since(m.throughputStartedAt)
	if elapsed <= 0 {
		return 0, 0, 0, 0
	}
	return elapsed, m.completedRows - m.throughputBaseRows, m.partitionCompleted - m.throughputBaseParts, m.completedBytes - m.throughputBaseBytes
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

func midnightUTC(value time.Time) time.Time {
	value = value.UTC()
	return time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC)
}

func monthStartUTC(value time.Time) time.Time {
	value = value.UTC()
	return time.Date(value.Year(), value.Month(), 1, 0, 0, 0, 0, time.UTC)
}

func hourStartUTC(value time.Time) time.Time {
	value = value.UTC()
	return value.Truncate(time.Hour)
}
