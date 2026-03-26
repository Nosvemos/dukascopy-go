# Tests

This repository uses two test layers:

- `tests/e2e`
  End-to-end CLI flows that build and execute the real `dukascopy-go` binary against mock services.

- `internal/.../*_test.go`
  White-box unit tests that stay next to the package they validate.

Why unit tests are not fully moved under `tests/units`:

Go allows package-local tests to access unexported helpers only when those tests live in the same package directory. A full move of all unit tests under `tests/units` would force those tests to become black-box tests, which would reduce internal coverage and remove direct validation of private helpers.

Because of that, the current layout is:

- centralized E2E tests under `tests/e2e`
- unit tests colocated with their packages for maximum coverage and fast feedback
