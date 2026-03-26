# dukascopy-data-downloader

A Go CLI for searching Dukascopy instruments and exporting historical market data from the Dukascopy `jetta` API.

## Features

- Resolves flexible symbols such as `xauusd`, `eur/usd`, and `BTC-USD`
- Supports any instrument returned by Dukascopy's `/v1/instruments` catalog, not just `XAUUSD`
- Searches instruments with the `instruments` command
- Downloads `tick`, `m1`, `m3`, `m5`, `m15`, `m30`, `h1`, `h4`, `d1`, `w1`, and `mn1` data as CSV
- Supports reduced, expanded, and custom CSV column sets with `--simple`, `--full`, and `--custom-columns`
- Exports raw volume values intended to be more suitable for backtesting
- Includes end-to-end tests that run the compiled CLI against a mock HTTP server

## Build

```bash
go build -o dukascopy-data ./cmd/dukascopy
```

## Quick Start

Run directly from the checked-out repository:

```bash
go run ./cmd/dukascopy --help
```

List supported timeframes:

```bash
go run ./cmd/dukascopy --list-timeframes
```

Search an instrument:

```bash
go run ./cmd/dukascopy instruments --query xauusd
```

Download 1-minute bars:

```bash
go run ./cmd/dukascopy download \
  --symbol xauusd \
  --timeframe m1 \
  --from 2024-01-02T00:00:00Z \
  --to 2024-01-02T06:00:00Z \
  --output ./data/xauusd-1m.csv \
  --full
```

## Installation

Install a local binary:

```bash
go install ./cmd/dukascopy
```

After the project is published under its final module path, users will also be able to run it without cloning the repository:

```bash
go run <module-path>/cmd/dukascopy@latest --help
```

Replace `<module-path>` with the final published Go module path.

## Usage Patterns

Search instruments:

```bash
go run ./cmd/dukascopy instruments --query xauusd
```

Download minute bars with the reduced schema:

```bash
go run ./cmd/dukascopy download \
  --symbol xauusd \
  --timeframe m1 \
  --from 2024-01-02T00:00:00Z \
  --to 2024-01-02T06:00:00Z \
  --output ./data/xauusd-minute.csv \
  --simple
```

Download minute bars with the expanded schema:

```bash
go run ./cmd/dukascopy download \
  --symbol xauusd \
  --timeframe m1 \
  --from 2024-01-02T00:00:00Z \
  --to 2024-01-02T06:00:00Z \
  --output ./data/xauusd-minute-full.csv \
  --full
```

Download ticks:

```bash
go run ./cmd/dukascopy download \
  --symbol eurusd \
  --timeframe tick \
  --from 2024-01-02T00:00:00Z \
  --to 2024-01-02T01:00:00Z \
  --output ./data/eurusd-ticks.csv \
  --full
```

Download with custom columns:

```bash
go run ./cmd/dukascopy download \
  --symbol xauusd \
  --timeframe m1 \
  --from 2024-01-02T00:00:00Z \
  --to 2024-01-02T06:00:00Z \
  --output ./data/xauusd-custom.csv \
  --custom-columns timestamp,bid_open,ask_open,volume
```

Download an aggregated timeframe:

```bash
go run ./cmd/dukascopy download \
  --symbol xauusd \
  --timeframe m5 \
  --from 2024-01-02T00:00:00Z \
  --to 2024-01-02T02:00:00Z \
  --output ./data/xauusd-m5.csv \
  --simple
```

Download monthly aggregated bars:

```bash
go run ./cmd/dukascopy download \
  --symbol xauusd \
  --timeframe mn1 \
  --from 2024-01-01T00:00:00Z \
  --to 2024-04-01T00:00:00Z \
  --output ./data/xauusd-mn1.csv \
  --simple
```

Download another Dukascopy instrument:

```bash
go run ./cmd/dukascopy download \
  --symbol eurusd \
  --timeframe h1 \
  --from 2024-01-01T00:00:00Z \
  --to 2024-01-03T00:00:00Z \
  --output ./data/eurusd-h1.csv \
  --full
```

## CSV Schemas

Simple bar schema:

```text
timestamp,open,high,low,close,volume
```

Full bar schema:

```text
timestamp,mid_open,mid_high,mid_low,mid_close,spread,volume,bid_open,bid_high,bid_low,bid_close,ask_open,ask_high,ask_low,ask_close
```

In `--full` bar output, midpoint values are exposed explicitly as `mid_open`, `mid_high`, `mid_low`, and `mid_close`. `spread` is computed as `ask_close - bid_close`.

Midpoint columns are written with one extra decimal place of precision relative to the instrument price scale when needed, so values like `2064.4735` are preserved instead of being rounded to `2064.474`.

When `--custom-columns` is used for bars, you can request any combination of `mid_*`, `bid_*`, `ask_*`, `spread`, and `volume`. Requesting any `mid_*`, `bid_*`, `ask_*`, or `spread` column makes the CLI fetch bid/ask data for the requested range.

Simple tick schema:

```text
timestamp,bid,ask
```

Full tick schema:

```text
timestamp,bid,ask,bid_volume,ask_volume
```

## Timeframes

Preferred timeframe values:

```text
tick
m1
m3
m5
m15
m30
h1
h4
d1
w1
mn1
```

The older `--granularity` flag is still accepted as a compatibility alias.

Timeframe behavior:

```text
tick  raw tick quotes
m1    native 1-minute bars
m3    aggregated from m1
m5    aggregated from m1
m15   aggregated from m1
m30   aggregated from m1
h1    native 1-hour bars
h4    aggregated from h1
d1    native 1-day bars
w1    aggregated from d1
mn1   aggregated from d1 by calendar month
```

## CLI Notes

- `--simple` writes the smallest useful schema.
- `--full` writes midpoint, spread, and explicit bid/ask columns.
- `--custom-columns` lets you request only the columns you need.
- `--list-timeframes` prints the currently supported timeframe values and their meaning.
- `--side` controls the primary side for simple bar exports.
- Commands use ANSI colors for headings, success messages, and table headers by default; set `NO_COLOR=1` to disable coloring.

## Configuration

Default API base URL:

```text
https://jetta.dukascopy.com
```

You can override it with the `DUKASCOPY_API_BASE_URL` environment variable. This is mainly useful for tests or local mocks.

## Tests

Run all tests:

```bash
go test ./...
```

Run end-to-end tests only:

```bash
go test ./e2e -v
```
