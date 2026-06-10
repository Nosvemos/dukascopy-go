package dukascopy

import (
	"io"
)

type countingReader struct {
	io.Reader
	count *int64
}

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.Reader.Read(p)
	*cr.count += int64(n)
	return n, err
}

func (c *Client) emitProgress(event ProgressEvent) {
	if c.progress == nil {
		return
	}
	c.progress(event)
}
