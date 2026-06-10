package dukascopy

import (
	"bufio"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
)

type ProxyPool struct {
	mu      sync.Mutex
	proxies []*url.URL
	current int
}

func (p *ProxyPool) LoadFromFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	p.mu.Lock()
	defer p.mu.Unlock()

	p.proxies = nil
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.Contains(line, "://") {
			line = "http://" + line
		}
		u, err := url.Parse(line)
		if err == nil {
			p.proxies = append(p.proxies, u)
		}
	}
	return scanner.Err()
}

func (p *ProxyPool) GetNextProxy(req *http.Request) (*url.URL, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.proxies) == 0 {
		return nil, nil
	}
	u := p.proxies[p.current]
	p.current = (p.current + 1) % len(p.proxies)
	return u, nil
}
