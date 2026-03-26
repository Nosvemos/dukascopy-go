# dukascopy-data-downloader

A Go CLI for searching Dukascopy instruments and exporting historical market data from the Dukascopy `jetta` API.

## Features

- Resolves flexible symbols such as `xauusd`, `eur/usd`, and `BTC-USD`
- Supports any instrument returned by Dukascopy's `/v1/instruments` catalog, not just `XAUUSD`
- Searches instruments with the `instruments` command
- Downloads `tick`, `m1`, `h1`, and `d1` data as CSV
- Supports reduced, expanded, and custom CSV column sets with `--simple`, `--full`, and `--custom-columns`
- Exports raw volume values intended to be more suitable for backtesting
- Includes end-to-end tests that run the compiled CLI against a mock HTTP server

## Build

```bash
go build -o dukascopy-data ./cmd/dukascopy
```

## Commands

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
h1
d1
```

The older `--granularity` flag is still accepted as a compatibility alias.

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
