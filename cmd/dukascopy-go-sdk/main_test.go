//go:build cgo

package main

import (
	"errors"
	"strings"
	"testing"
)

func TestCallGuardedRecoversPanics(t *testing.T) {
	message := callGuarded(func() error {
		var values []int
		_ = values[3] // index out of range panic
		return nil
	})
	if !strings.HasPrefix(message, "internal error:") || !strings.Contains(message, "index out of range") {
		t.Fatalf("panic was not turned into an error message: %q", message)
	}

	if message := callGuarded(func() error { return errors.New("boom") }); message != "boom" {
		t.Fatalf("error message = %q", message)
	}
	if message := callGuarded(func() error { return nil }); message != "" {
		t.Fatalf("success must return an empty message, got %q", message)
	}
}
