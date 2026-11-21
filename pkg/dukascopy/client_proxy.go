package dukascopy

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
)

// ProxyPool rotates requests through a list of proxies. An empty pool means
// direct connections.
type ProxyPool struct {
	mu      sync.Mutex
	proxies []*url.URL
	current int
}

// ErrNoProxies is returned when a proxy list contains no proxies.
var ErrNoProxies = errors.New("dukascopy: proxy list contains no proxies")

// supportedProxySchemes are the schemes net/http's Transport can dial.
var supportedProxySchemes = map[string]bool{
	"http":    true,
	"https":   true,
	"socks5":  true,
	"socks5h": true,
}

// LoadFromFile replaces the pool with the proxies listed in path, one per
// line. Blank lines and lines starting with "#" are ignored; a line without
// a scheme is treated as http://. Each proxy needs a host and one of the
// schemes http, https, socks5 or socks5h.
//
// If any line is invalid, or the file yields no proxies (ErrNoProxies), it
// returns an error and leaves the pool unchanged, so a bad file never
// silently falls back to direct connections. The error names the bad line
// numbers but not their contents, which may hold credentials.
func (p *ProxyPool) LoadFromFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	proxies, err := parseProxyList(file)
	if err != nil {
		return fmt.Errorf("dukascopy: load proxies from %s: %w", path, err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.proxies = proxies
	p.current = 0
	return nil
}

func parseProxyList(r io.Reader) ([]*url.URL, error) {
	var (
		proxies []*url.URL
		bad     []string
	)
	scanner := bufio.NewScanner(r)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		u, reason := parseProxyLine(line)
		if reason != "" {
			bad = append(bad, fmt.Sprintf("line %d: %s", lineNo, reason))
			continue
		}
		proxies = append(proxies, u)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(bad) > 0 {
		return nil, fmt.Errorf("invalid proxy entries (%s)", strings.Join(bad, "; "))
	}
	if len(proxies) == 0 {
		return nil, ErrNoProxies
	}
	return proxies, nil
}

// parseProxyLine returns the proxy URL, or a non-empty reason it is invalid.
func parseProxyLine(line string) (*url.URL, string) {
	if !strings.Contains(line, "://") {
		line = "http://" + line
	}
	u, err := url.Parse(line)
	if err != nil {
		return nil, "cannot parse proxy URL"
	}
	scheme := strings.ToLower(u.Scheme)
	if !supportedProxySchemes[scheme] {
		return nil, fmt.Sprintf("unsupported scheme %q (use http, https, socks5 or socks5h)", u.Scheme)
	}
	if u.Hostname() == "" {
		return nil, "missing host"
	}
	u.Scheme = scheme
	return u, ""
}

// GetNextProxy returns the next proxy in round-robin order, or nil (direct
// connection) when the pool is empty. It matches http.Transport.Proxy.
func (p *ProxyPool) GetNextProxy(req *http.Request) (*url.URL, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.proxies) == 0 {
		return nil, nil
	}
	u := p.proxies[p.current%len(p.proxies)]
	p.current = (p.current + 1) % len(p.proxies)
	return u, nil
}
