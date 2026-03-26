# dukascopy-data-downloader

A Go CLI for searching Dukascopy instruments and exporting historical market data from the Dukascopy `jetta` API.

## Features

- Resolves flexible symbols such as `xauusd`, `eur/usd`, and `BTC-USD`
- Searches instruments with the `instruments` command
- Downloads `tick`, `minute`, `hour`, and `day` data as CSV
- Supports reduced and expanded CSV column sets with `--simple` and `--full`
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
  --granularity minute \
  --from 2024-01-02T00:00:00Z \
  --to 2024-01-02T06:00:00Z \
  --output ./data/xauusd-minute.csv \
  --simple
```

Download minute bars with the expanded schema:

```bash
go run ./cmd/dukascopy download \
  --symbol xauusd \
  --granularity minute \
  --from 2024-01-02T00:00:00Z \
  --to 2024-01-02T06:00:00Z \
  --output ./data/xauusd-minute-full.csv \
  --full
```

Download ticks:

```bash
go run ./cmd/dukascopy download \
  --symbol eurusd \
  --granularity tick \
  --from 2024-01-02T00:00:00Z \
  --to 2024-01-02T01:00:00Z \
  --output ./data/eurusd-ticks.csv \
  --full
```

## CSV Schemas

Simple bar schema:

```text
timestamp,open,high,low,close,volume
```

Full bar schema:

```text
timestamp,open,high,low,close,volume,bid_open,bid_high,bid_low,bid_close,ask_open,ask_high,ask_low,ask_close
```

In `--full` bar output, the generic `open/high/low/close` columns are midpoint values derived from bid and ask candles. Spread can be computed later from the explicit bid and ask columns.

Simple tick schema:

```text
timestamp,bid,ask
```

Full tick schema:

```text
timestamp,bid,ask,bid_volume,ask_volume
```

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
