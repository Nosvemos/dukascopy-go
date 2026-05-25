# Tests

`dukascopy-go` uses two test layers.

## Unit tests

Located next to each package (`internal/.../*_test.go` and `pkg/.../*_test.go`). They test unexported helpers directly and run with:

```bash
go test ./internal/... ./pkg/...
```

Unit tests stay colocated with their packages so they can access private functions without forcing a black-box interface. Moving them under `tests/units/` would require exporting those helpers, which reduces coverage quality.

## End-to-end tests

Located in `tests/e2e/`. They build and invoke the real `dukascopy-go` binary against lightweight mock HTTP servers.

```bash
go test ./tests/e2e -v
```

## Run everything

```bash
go test ./...
```

## Coverage

```bash
go test ./... -coverprofile=coverage.out
go tool cover -html=coverage.out
```

Target: **≥ 95% statement coverage** across all packages.
