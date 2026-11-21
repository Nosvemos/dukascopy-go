# Developer shortcuts. Works with GNU make on Linux, macOS and Git Bash on Windows.
#
# Optional, not part of CI: golangci-lint with the conservative config in
# .golangci.yml (the tree is not fully clean against it yet):
#   golangci-lint run ./...

GO              ?= go
STATICCHECK_VER ?= v0.8.2
COVERPROFILE    ?= coverage.out

ifeq ($(OS),Windows_NT)
  EXE    := .exe
  LIBEXT := dll
else
  EXE    :=
  UNAME_S := $(shell uname -s)
  ifeq ($(UNAME_S),Darwin)
    LIBEXT := dylib
  else
    LIBEXT := so
  endif
endif

SDK_LIB := sdk/python/dukascopy_go/libdukascopy.$(LIBEXT)

.PHONY: all build test e2e race lint vuln cover sdk

all: build

## build: build the CLI binary into the repository root
build:
	$(GO) build -o dukascopy-go$(EXE) ./cmd/dukascopy-go

## test: unit tests (no end-to-end suite)
test:
	$(GO) test ./pkg/... ./internal/... ./cmd/...

## e2e: end-to-end suite (downloads live data; needs network)
e2e:
	$(GO) test -v ./tests/e2e/...

## race: unit tests with the race detector (needs cgo / a C compiler)
race:
	CGO_ENABLED=1 $(GO) test -race -count=1 ./pkg/... ./internal/...

## lint: gofmt check, go vet and staticcheck
lint:
	@unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt needed on:"; echo "$$unformatted"; exit 1; \
	fi
	$(GO) vet ./...
	$(GO) run honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VER) ./...

## vuln: known-vulnerability scan of dependencies and the standard library
vuln:
	$(GO) run golang.org/x/vuln/cmd/govulncheck@latest ./...

## cover: unit-test coverage profile and per-function summary
cover:
	$(GO) test -covermode=atomic -coverprofile=$(COVERPROFILE) ./pkg/... ./internal/...
	$(GO) tool cover -func=$(COVERPROFILE) | tail -n 1

## sdk: build the c-shared library into the Python package directory
sdk:
	CGO_ENABLED=1 $(GO) build -buildmode=c-shared -o $(SDK_LIB) ./cmd/dukascopy-go-sdk
