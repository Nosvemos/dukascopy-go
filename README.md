# dukascopy-go

<p align="center">
  <b>Download free historical Dukascopy market data — no dependencies.</b><br>
  Forex • Metals • Crypto • Commodities • CFDs • ETFs
</p>

<p align="center">
  <a href="https://github.com/Nosvemos/dukascopy-go/actions/workflows/ci.yml"><img src="https://github.com/Nosvemos/dukascopy-go/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/Nosvemos/dukascopy-go/actions/workflows/release.yml"><img src="https://github.com/Nosvemos/dukascopy-go/actions/workflows/release.yml/badge.svg" alt="Release"></a>
  <a href="https://pkg.go.dev/github.com/Nosvemos/dukascopy-go"><img src="https://pkg.go.dev/badge/github.com/Nosvemos/dukascopy-go.svg" alt="Go Reference"></a>
  <a href="https://github.com/Nosvemos/dukascopy-go/releases"><img src="https://img.shields.io/github/v/release/Nosvemos/dukascopy-go" alt="Latest release"></a>
</p>

<p align="center">
  <a href="#installation">Installation</a> |
  <a href="#quick-start">Quick Start</a> |
  <a href="#commands">Commands</a> |
  <a href="#output-formats">Output Formats</a> |
  <a href="#configuration">Configuration</a> |
  <a href="#sdk">SDK</a>
</p>

---

## Installation

| Method | Command |
| --- | --- |
| Install binary | `go install github.com/Nosvemos/dukascopy-go/cmd/dukascopy-go@latest` |
| Run without cloning | `go run github.com/Nosvemos/dukascopy-go/cmd/dukascopy-go@latest --help` |
| Build from source | `go build -o dukascopy-go ./cmd/dukascopy-go` |

Requires **Go 1.22+**. Pre-built binaries for Linux, macOS, and Windows are available on the [Releases](https://github.com/Nosvemos/dukascopy-go/releases) page.

---

## Quick Start

Search for an instrument:

```bash
dukascopy-go instruments --query xauusd
```

Download 1-minute bars to CSV:

```bash
dukascopy-go download \
  --symbol xauusd \
  --timeframe m1 \
  --from 2024-01-02T00:00:00Z \
  --to 2024-01-02T06:00:00Z \
  --output ./data/xauusd-m1.csv \
  --simple
```

Download to Parquet with parallel workers:

```bash
dukascopy-go download \
  --symbol xauusd \
  --timeframe m1 \
  --from 2024-01-01T00:00:00Z \
  --to 2024-02-01T00:00:00Z \
  --output ./data/xauusd-january.parquet \
  --simple \
  --partition auto \
  --parallelism 4
```

Stream real-time ticks to stdout:

```bash
dukascopy-go live --symbol eurusd --timeframe tick --format jsonl
```

Ingest a CSV file directly into ClickHouse:

```bash
dukascopy-go db-load \
  --input ./data/eurusd-m1.csv \
  --db clickhouse \
  --url http://localhost:8123 \
  --table eurusd_m1
```

---

## Commands

| Command | Purpose |
| --- | --- |
| `instruments` | Search Dukascopy instruments |
| `download` | Download historical data as CSV or Parquet |
| `live` | Stream real-time ticks/bars to stdout and WebSocket |
| `db-load` | Ingest CSV or Parquet directly into ClickHouse or InfluxDB |
| `stats` | Inspect a dataset for gaps, duplicates, and ordering issues |
| `manifest inspect` | Print checkpoint manifest and partition status |
| `manifest verify` | Verify files against manifest checksums |
| `manifest repair` | Rebuild or re-download missing/corrupt partitions |
| `manifest prune` | Remove orphan temp files |
| `list-timeframes` | Print supported timeframe values |
| `version` | Print version, commit, and build date |

### `download`

```bash
dukascopy-go download [flags]
```

| Flag | Default | Description |
| --- | --- | --- |
| `--symbol` | required | instrument such as `eurusd`, `xauusd`, `btcusd` |
| `--timeframe` | required | `tick`, `m1`, `m5`, `m15`, `m30`, `h1`, `h4`, `d1`, `w1`, `mn1` |
| `--from` | required | start time in RFC3339 format |
| `--to` | now | end time in RFC3339 format |
| `--output` | required | output path (`.csv`, `.csv.gz`, `.parquet`, or `-` for stdout) |
| `--simple` | false | minimal schema (timestamp + OHLCV) |
| `--full` | false | full schema with bid/ask/spread columns |
| `--custom-columns` | — | comma-separated list of columns |
| `--engine` | `jetta` | `jetta` or `datafeed` |
| `--partition` | `none` | `none`, `auto`, `hour`, `day`, `week`, `month`, `year` |
| `--parallelism` | `1` | concurrent partition workers |
| `--retries` | `3` | retry attempts on network errors |
| `--rate-limit` | — | minimum delay between requests |
| `--resume` | false | append only rows after the latest saved CSV timestamp |
| `--live` | false | keep polling for new data after reaching the current time |
| `--poll-interval` | `5s` | polling frequency in live mode |
| `--timezone` | UTC | shift timestamps to a local timezone |
| `--preset` | — | `mt4`, `mt5`, `backtrader`, `ninjatrader` |
| `--proxy-file` | — | path to a list of HTTP/SOCKS5 proxies |
| `--progress` | false | print progress to stderr |

### `live`

```bash
dukascopy-go live --symbol eurusd --timeframe tick --port 8080
```

Streams new ticks or bars to stdout. When `--port` is given, also starts a local WebSocket server at `ws://localhost:<port>/stream`.

| Flag | Default | Description |
| --- | --- | --- |
| `--symbol` | required | instrument to stream |
| `--timeframe` | `tick` | `tick`, `m1`, `m5`, … `d1` |
| `--format` | `jsonl` | `jsonl` or `csv` |
| `--port` | `0` | WebSocket server port (disabled when 0) |
| `--poll-interval` | `1s` | request frequency |
| `--output` | `-` | optional file path to append to |

### `db-load`

```bash
dukascopy-go db-load --input data.parquet --db clickhouse --url http://localhost:8123 --table eurusd_m1
```

Streams a local file directly into the target database over HTTP — **no driver, no dependencies**.

| Flag | Required | Description |
| --- | --- | --- |
| `--input` | yes | `.csv`, `.csv.gz`, or `.parquet` |
| `--db` | yes | `clickhouse` or `influxdb` |
| `--url` | yes | database HTTP URL |
| `--table` | yes | table (ClickHouse) or measurement (InfluxDB) |
| `--token` | InfluxDB | InfluxDB API token |
| `--org` | InfluxDB | InfluxDB organization |
| `--bucket` | InfluxDB | InfluxDB bucket |
| `--user` | no | ClickHouse username |
| `--password` | no | ClickHouse password or InfluxDB token |

---

## Output Formats

| Format | Flag |
| --- | --- |
| Minimal OHLCV | `--simple` |
| Full bid/ask/spread | `--full` |
| Custom columns | `--custom-columns timestamp,bid_open,ask_open,volume` |

**Bar schemas:**

```text
# simple
timestamp,open,high,low,close,volume

# full
timestamp,mid_open,mid_high,mid_low,mid_close,spread,volume,bid_open,bid_high,bid_low,bid_close,ask_open,ask_high,ask_low,ask_close
```

**Tick schemas:**

```text
# simple
timestamp,bid,ask

# full
timestamp,bid,ask,bid_volume,ask_volume
```

---

## Timeframes

```text
tick   raw tick quotes
m1     native 1-minute bars
m3     aggregated from m1
m5     aggregated from m1
m15    aggregated from m1
m30    aggregated from m1
h1     native 1-hour bars
h4     aggregated from h1
d1     native 1-day bars
w1     aggregated from d1
mn1    aggregated from d1 by calendar month
```

```bash
dukascopy-go --list-timeframes
```

---

## Checkpointed Downloads

When `--partition` is enabled, each sub-range is written to its own file inside `<output>.parts/`. A `.manifest.json` tracks completion, row counts, and SHA-256 hashes so interrupted runs resume from the last valid partition.

```bash
dukascopy-go download \
  --symbol xauusd \
  --timeframe m1 \
  --from 2023-01-01T00:00:00Z \
  --to 2024-01-01T00:00:00Z \
  --output ./data/xauusd-2023.csv \
  --simple \
  --partition auto \
  --parallelism 4

# Verify integrity after download
dukascopy-go manifest verify --output ./data/xauusd-2023.csv

# Repair damaged or missing partitions
dukascopy-go manifest repair --output ./data/xauusd-2023.csv --redownload-gaps
```

`--partition auto` selects a sensible granularity by timeframe (`tick` → `hour`, `m1` → `day`, `h1` → `month`, `d1` → `year`).

---

## Configuration

Store defaults in a JSON file and pass it with `--config` or `DUKASCOPY_CONFIG`:

```json
{
  "base_url": "https://jetta.dukascopy.com",
  "download": {
    "timeframe": "m1",
    "simple": true,
    "retries": 5,
    "rate_limit": "150ms",
    "partition": "auto",
    "parallelism": 4
  }
}
```

```bash
dukascopy-go --config ./dukascopy.json download --symbol eurusd --from 2024-01-01T00:00:00Z --output ./data/eurusd.csv

# or export once
export DUKASCOPY_CONFIG=./dukascopy.json
```

Override the API base URL with an environment variable:

```bash
export DUKASCOPY_API_BASE_URL=https://jetta.dukascopy.com
```

---

## SDK

`dukascopy-go` can be compiled to a native shared library (`.so` / `.dll` / `.dylib`) for use from Python, C, or C++.

**Build the library:**

```bash
# Linux / macOS
go build -buildmode=c-shared -o sdk/python/libdukascopy.so ./cmd/dukascopy-go-sdk

# Windows
go build -buildmode=c-shared -o sdk/python/libdukascopy.dll ./cmd/dukascopy-go-sdk
```

**Python usage:**

```python
from datetime import datetime
from sdk.python import dukascopy

dukascopy.download(
    symbol="EURUSD",
    timeframe="m1",
    output_path="./data/eurusd_m1.parquet",
    from_date=datetime(2024, 1, 2, 0, 0),
    to_date=datetime(2024, 1, 2, 6, 0),
)
```

---

## Development

```bash
# Run all tests
go test ./...

# Run only e2e tests
go test ./tests/e2e -v

# Build the binary locally
go build -o dukascopy-go ./cmd/dukascopy-go
```

---

## Roadmap

- [x] Dual-Engine Downloader (Jetta JSON + Datafeed .bi5 LZMA)
- [x] Smart Market Calendar Filter (Weekend/Holiday Skip)
- [x] Universal Timezone Shifting & Broker Presets
- [x] Proxy Connection Pool (HTTP/SOCKS5 round-robin)
- [x] Transactional Checkpoint Manifests & Partitioned Downloads
- [x] Duplicate & Out-of-Order Line Pruner
- [x] Interactive TUI Dashboard
- [x] Multi-Language SDK Bindings (Python / C / C++)
- [x] Real-Time Streaming (`live` command + WebSocket broadcast)
- [x] Direct Database Loading (`db-load` → ClickHouse & InfluxDB)

---

## Legal Disclaimer

`dukascopy-go` is not affiliated with, endorsed by, or vetted by Dukascopy Bank SA. It is an independent open-source tool that works with Dukascopy's publicly accessible endpoints and is intended for research, automation, and data engineering workflows.
