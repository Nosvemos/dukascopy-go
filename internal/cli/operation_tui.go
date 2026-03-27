package cli

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type operationPrinter struct {
	mu      sync.Mutex
	program *tea.Program
	done    chan struct{}
	closed  bool
}

type operationMetaMsg struct {
	command string
	target  string
}

type operationStatusMsg struct {
	status string
}

type operationPhaseMsg struct {
	phase string
}

type operationMetricMsg struct {
	label string
	value string
}

type operationFinishMsg struct{}

type operationMetric struct {
	Label string
	Value string
}

type operationTUIModel struct {
	bootedAt   time.Time
	spinner    spinner.Model
	command    string
	target     string
	statusText string
	phaseText  string
	metrics    []operationMetric
	logs       []string
	width      int
	height     int
	noColor    bool
}

func newOperationPrinter(w io.Writer) *operationPrinter {
	model := newOperationTUIModel(strings.TrimSpace(os.Getenv("NO_COLOR")) != "")
	options := []tea.ProgramOption{
		tea.WithInput(nil),
		tea.WithOutput(w),
		tea.WithoutSignalHandler(),
	}
	if isInteractiveTerminal(w) {
		options = append(options, tea.WithAltScreen())
	}

	program := tea.NewProgram(model, options...)
	p := &operationPrinter{
		program: program,
		done:    make(chan struct{}),
	}
	go func() {
		_, _ = program.Run()
		close(p.done)
	}()
	return p
}

func newOperationTUIModel(noColor bool) operationTUIModel {
	spin := spinner.New(spinner.WithSpinner(spinner.Dot))
	if !noColor {
		spin.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("81"))
	}

	return operationTUIModel{
		bootedAt:   time.Now(),
		spinner:    spin,
		statusText: "starting",
		width:      72,
		height:     14,
		noColor:    noColor,
	}
}

func (p *operationPrinter) SetCommand(command string, target string) {
	p.send(operationMetaMsg{command: command, target: target})
}

func (p *operationPrinter) SetStatus(status string) {
	p.send(operationStatusMsg{status: status})
}

func (p *operationPrinter) SetPhase(phase string) {
	p.send(operationPhaseMsg{phase: phase})
}

func (p *operationPrinter) SetMetric(label string, value string) {
	p.send(operationMetricMsg{label: label, value: value})
}

func (p *operationPrinter) Finish() {
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
		program.Send(operationFinishMsg{})
	}
	if done != nil {
		<-done
	}
}

func (p *operationPrinter) Write(data []byte) (int, error) {
	text := strings.TrimSpace(string(data))
	if text != "" {
		p.send(progressLogMsg{text: text})
	}
	return len(data), nil
}

func (p *operationPrinter) send(msg tea.Msg) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed || p.program == nil {
		return
	}
	p.program.Send(msg)
}

func (m operationTUIModel) Init() tea.Cmd {
	return m.spinner.Tick
}

func (m operationTUIModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil
	case operationMetaMsg:
		m.command = strings.TrimSpace(msg.command)
		m.target = strings.TrimSpace(msg.target)
		return m, nil
	case operationStatusMsg:
		status := strings.TrimSpace(msg.status)
		if status != "" {
			m.statusText = status
			m.pushLog(status)
		}
		return m, nil
	case operationPhaseMsg:
		m.phaseText = strings.TrimSpace(msg.phase)
		return m, nil
	case operationMetricMsg:
		label := strings.TrimSpace(msg.label)
		if label == "" {
			return m, nil
		}
		value := defaultString(strings.TrimSpace(msg.value), "-")
		updated := false
		for index := range m.metrics {
			if m.metrics[index].Label == label {
				m.metrics[index].Value = value
				updated = true
				break
			}
		}
		if !updated {
			m.metrics = append(m.metrics, operationMetric{Label: label, Value: value})
		}
		return m, nil
	case progressLogMsg:
		m.pushLog(msg.text)
		return m, nil
	case operationFinishMsg:
		return m, tea.Quit
	default:
		return m, nil
	}
}

func (m *operationTUIModel) pushLog(line string) {
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

func (m operationTUIModel) View() string {
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
		m.renderKVLine("cmd", defaultString(m.command, "-"), width),
		m.renderKVLine("target", defaultString(m.target, "-"), width),
	}

	if strings.TrimSpace(m.phaseText) != "" {
		lines = append(lines, m.renderKVLine("phase", m.phaseText, width))
	}
	for _, metric := range m.metrics {
		lines = append(lines, m.renderKVLine(metric.Label, metric.Value, width))
	}
	if len(m.logs) == 0 {
		lines = append(lines, m.subtleStyle().Render("recent waiting for events"))
	} else {
		start := 0
		if len(m.logs) > 2 {
			start = len(m.logs) - 2
		}
		for _, line := range m.logs[start:] {
			lines = append(lines, m.renderKVLine("recent", line, width))
		}
	}

	if height > 0 {
		lines = m.trimLinesForHeight(lines, width, height)
	}
	return strings.Join(lines, "\n")
}

func (m operationTUIModel) renderHeader(width int) string {
	left := lipgloss.JoinHorizontal(lipgloss.Left, m.titleStyle().Render("DUKASCOPY-GO"), " ", m.phaseBadge())
	right := m.subtleStyle().Render("elapsed " + formatShortDuration(time.Since(m.bootedAt)))
	if lipgloss.Width(left)+2+lipgloss.Width(right) <= width {
		return lipgloss.JoinHorizontal(lipgloss.Left, left, strings.Repeat(" ", width-lipgloss.Width(left)-lipgloss.Width(right)), right)
	}
	if lipgloss.Width(left) >= width {
		return truncateDisplayWidth(left, width)
	}
	remaining := maxInt(1, width-lipgloss.Width(left)-1)
	return left + " " + truncateDisplayWidth(right, remaining)
}

func (m operationTUIModel) renderStatusLine(width int) string {
	line := fmt.Sprintf("%s %s", m.spinner.View(), truncateDisplayWidth(defaultString(m.statusText, "starting"), maxInt(1, width-lipgloss.Width(m.spinner.View())-1)))
	if lipgloss.Width(line) > width {
		line = truncateDisplayWidth(line, width)
	}
	return line
}

func (m operationTUIModel) renderKVLine(label string, value string, width int) string {
	prefix := lipgloss.Width(label) + 1
	plain := fitLine(label+" ", defaultString(value, "-"), width)
	if !m.noColor {
		value = truncateDisplayWidth(defaultString(value, "-"), maxInt(1, width-prefix))
		return m.labelStyle().Render(label) + " " + m.valueStyle().Render(value)
	}
	return plain
}

func (m operationTUIModel) trimLinesForHeight(lines []string, width int, height int) []string {
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

func (m operationTUIModel) phaseBadge() string {
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
	case strings.Contains(status, "failed"), strings.Contains(status, "error"), strings.Contains(status, "invalid"):
		style = style.Foreground(lipgloss.Color("231")).Background(lipgloss.Color("160"))
	case strings.Contains(status, "verified"), strings.Contains(status, "complete"), strings.Contains(status, "done"):
		style = style.Foreground(lipgloss.Color("16")).Background(lipgloss.Color("114"))
	case strings.Contains(status, "repair"), strings.Contains(status, "verify"), strings.Contains(status, "scan"), strings.Contains(status, "inspect"):
		style = style.Foreground(lipgloss.Color("16")).Background(lipgloss.Color("221"))
	default:
		style = style.Foreground(lipgloss.Color("16")).Background(lipgloss.Color("81"))
	}
	return style.Render(label)
}

func (m operationTUIModel) titleStyle() lipgloss.Style {
	style := lipgloss.NewStyle().Bold(true)
	if !m.noColor {
		style = style.Foreground(lipgloss.Color("159"))
	}
	return style
}

func (m operationTUIModel) subtleStyle() lipgloss.Style {
	style := lipgloss.NewStyle().Faint(true)
	if !m.noColor {
		style = style.Foreground(lipgloss.Color("244"))
	}
	return style
}

func (m operationTUIModel) labelStyle() lipgloss.Style {
	style := lipgloss.NewStyle().Bold(true)
	if !m.noColor {
		style = style.Foreground(lipgloss.Color("86"))
	}
	return style
}

func (m operationTUIModel) valueStyle() lipgloss.Style {
	style := lipgloss.NewStyle()
	if !m.noColor {
		style = style.Foreground(lipgloss.Color("230"))
	}
	return style
}
