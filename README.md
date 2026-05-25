# dukascopy-go

<p align="center">
  <b>Download historical Dukascopy market data with a reliable Go CLI.</b><br>
  Search instruments, export CSV or Parquet, resume interrupted jobs, and verify large datasets with checkpoint manifests.
</p>

<p align="center">
  <a href="https://github.com/Nosvemos/dukascopy-go/actions/workflows/ci.yml"><img src="https://github.com/Nosvemos/dukascopy-go/actions/workflows/ci.yml/badge.svg" alt="CI status"></a>
  <a href="https://github.com/Nosvemos/dukascopy-go/actions/workflows/release.yml"><img src="https://github.com/Nosvemos/dukascopy-go/actions/workflows/release.yml/badge.svg" alt="Release status"></a>
  <a href="https://pkg.go.dev/github.com/Nosvemos/dukascopy-go"><img src="https://pkg.go.dev/badge/github.com/Nosvemos/dukascopy-go.svg" alt="Go Reference"></a>
  <a href="https://github.com/Nosvemos/dukascopy-go/releases"><img src="https://img.shields.io/github/v/release/Nosvemos/dukascopy-go" alt="Latest release"></a>
</p>

<p align="center">
  <a href="#installation">Installation</a> |
  <a href="#quick-start">Quick Start</a> |
  <a href="#commands">Commands</a> |
  <a href="#checkpointed-downloads">Checkpointed Downloads</a> |
  <a href="#configuration">Configuration</a>
</p>

---

## Why `dukascopy-go`?

`dukascopy-go` is built for people who want more than a basic downloader. It helps you search the Dukascopy catalog, export clean datasets, continue interrupted downloads safely, and audit finished files without redownloading everything.

It supports flexible symbol input like `xauusd`, `eur/usd`, and `BTC-USD`, and works with any instrument returned by Dukascopy's `jetta` instrument catalog.

## Highlights

- **Dual-Engine Architecture:** Toggle seamlessly between `--engine jetta` (developer-friendly, structured JSON API) and `--engine datafeed` (Dukascopy .bi5 LZMA-compressed binary files) download engines.
- **High-Performance Decompression:** Multi-threaded, native Go LZMA decompressor and binary parser resolving millions of tick and bar records in milliseconds.
- **Platform Presets:** Ready-made presets (`--preset mt4`, `--preset mt5`, `--preset backtrader`, `--preset ninjatrader`) matching standard broker output columns, separators, and timezone configurations.
- **Smart Market Calendar Filter (Weekend Skip):** Skips Saturdays, Sundays, and market-closed periods at the request layer for Forex/Metals/CFDs, reducing API calls by **30-40%** and preventing rate-limits.
- **Thread-safe Proxy Connection Pool:** Load HTTP/SOCKS5 proxies (`--proxy-file`) to rotate requests in a round-robin cycle and bypass IP bans.
- **Universal Timezone Shifting:** Shift UTC values to any local timezone (Europe/London, America/New_York, EET, EST, etc.) with automatic DST (Daylight Saving Time) handling.
- **Zero-Dependency Single Binary:** Compiled to a single, static multi-platform executable (Windows, Linux, macOS) with no requirements (no Node.js/NPM, no node_modules).
- **Transactional Manifest & Checkpoints:** Resilient partitioning (`--partition`) and parallel execution (`--parallelism 4`). Automatically audits partition row counts and SHA-256 hashes inside a `.manifest.json` file for recovery/verification.
- **Duplicate & Out-of-Order Line Pruner:** In-place, atomic cleaning of duplicate or unsorted timestamps in CSV/Parquet files with `manifest clean-duplicates`.
- **Flexible Exporter:** Export to `.csv`, `.csv.gz`, or compressed columnar `.parquet`. Custom column mapping with `--custom-columns`.
- **Live Syncing & Piping:** Continuous streaming via `--live --poll-interval 5s` or pipe CSV directly to stdout (`--output -`).
- **Interactive TUI Dashboard:** Gorgeous, real-time visual progress rendering MB/s and rows/s throughput, ETA timers, active proxies, and failure rates.

## Installation

| Method | Command |
| --- | --- |
| Install binary with Go | `go install github.com/Nosvemos/dukascopy-go/cmd/dukascopy-go@latest` |
| Run without cloning | `go run github.com/Nosvemos/dukascopy-go/cmd/dukascopy-go@latest --help` |
| Build from local source | `go build -o dukascopy-go ./cmd/dukascopy-go` |

`Go 1.26+` is the current target in this repository.

## Releases

The release workflow supports two paths:

- push a tag such as `v0.2.0`
- run the `Release` workflow from the GitHub Actions UI with a `version_tag` such as `v0.2.0`

When you use the UI button, the workflow creates and pushes the tag for you before running GoReleaser.

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
  --full
```

Download the same range to Parquet:

```bash
dukascopy-go download \
  --symbol xauusd \
  --timeframe m1 \
  --from 2024-01-02T00:00:00Z \
  --to 2024-01-02T06:00:00Z \
  --output ./data/xauusd-m1.parquet \
  --full
```

Download a larger range with durable partitions and parallel workers:

```bash
dukascopy-go download \
  --symbol xauusd \
  --timeframe m1 \
  --from 2024-01-01T00:00:00Z \
  --to 2024-02-01T00:00:00Z \
  --output ./data/xauusd-january.csv \
  --simple \
  --partition auto \
  --parallelism 4 \
  --progress
```

Keep a 1-minute CSV updated in live mode:

```bash
dukascopy-go download \
  --symbol xauusd \
  --timeframe m1 \
  --from 2024-01-02T00:00:00Z \
  --output ./data/xauusd-live.csv \
  --simple \
  --live \
  --poll-interval 5s
```

Inspect the finished dataset:

```bash
dukascopy-go stats --input ./data/xauusd-january.csv
```

Override the gap classifier with an explicit symbol when needed:

```bash
dukascopy-go stats --input ./data/eurusd-m1.csv --symbol eurusd
```

Print the first suspicious gap ranges after the summary:

```bash
dukascopy-go stats --input ./data/xauusd-m1.csv --show-suspicious-gaps --suspicious-gap-limit 20
```

When you run `stats` or `manifest` commands in a real terminal, `dukascopy-go` now opens the same compact interactive dashboard style used by downloads. JSON output stays plain text and non-interactive pipes still print normal line-based output.

If you are working from a clone instead of an installed binary, use:

```bash
go run ./cmd/dukascopy-go --help
```

## Commands

| Command | Purpose |
| --- | --- |
| `instruments` | Search Dukascopy instruments and print matches as text or JSON |
| `download` | Download historical data and save it as CSV or Parquet |
| `live` | Stream real-time ticks or bars to stdout in JSONL/CSV and broadcast over a local WebSocket server |
| `db-load` | Ingest a local CSV or Parquet file directly into ClickHouse or InfluxDB with zero driver dependencies |
| `stats` | Inspect CSV, CSV.GZ, or Parquet datasets for counts, ranges, profile-aware expected vs suspicious gaps, duplicates, and ordering |
| `manifest inspect` | Print checkpoint manifest summaries and partition status |
| `manifest verify` | Verify partition files and final outputs against manifest metadata, and classify expected vs suspicious gaps |
| `manifest repair` | Rebuild missing or invalid files from valid existing data, or re-download partition files that intersect suspicious gaps |
| `manifest prune` | Remove obsolete temp files and orphan partition files safely |
| `list-timeframes` | Print supported timeframe values |
| `version` | Print embedded version, commit, and build date information |

Global config flag:

```bash
dukascopy-go --config ./dukascopy.json instruments --query xauusd
```

## Common Examples

Search as JSON:

```bash
dukascopy-go instruments --query xauusd --json
```

Download compressed CSV directly:

```bash
dukascopy-go download \
  --symbol xauusd \
  --timeframe m1 \
  --from 2024-01-02T00:00:00Z \
  --to 2024-01-02T06:00:00Z \
  --output ./data/xauusd-m1.csv.gz \
  --simple
```

Stream CSV to `stdout`:

```bash
dukascopy-go download \
  --symbol xauusd \
  --timeframe m1 \
  --from 2024-01-02T00:00:00Z \
  --to 2024-01-02T00:03:00Z \
  --output - \
  --simple
```

Use custom columns:

```bash
dukascopy-go download \
  --symbol xauusd \
  --timeframe m1 \
  --from 2024-01-02T00:00:00Z \
  --to 2024-01-02T06:00:00Z \
  --output ./data/xauusd-custom.csv \
  --custom-columns timestamp,bid_open,ask_open,volume
```

Resume an interrupted CSV download:

```bash
dukascopy-go download \
  --symbol xauusd \
  --timeframe m1 \
  --from 2024-01-02T00:00:00Z \
  --to 2024-01-02T06:00:00Z \
  --output ./data/xauusd-m1.csv \
  --simple \
  --resume
```

Keep appending newly completed rows to a plain CSV until you stop the process:

```bash
dukascopy-go download \
  --symbol xauusd \
  --timeframe m1 \
  --from 2024-01-02T00:00:00Z \
  --output ./data/xauusd-live.csv \
  --simple \
  --live \
  --poll-interval 5s
```

Keep a partitioned live download with a custom checkpoint manifest and compressed final output:

```bash
dukascopy-go download \
  --symbol xauusd \
  --timeframe h1 \
  --from 2024-01-01T00:00:00Z \
  --output ./data/xauusd-live.csv.gz \
  --simple \
  --live \
  --partition auto \
  --checkpoint-manifest ./data/xauusd-live.manifest.json \
  --poll-interval 10s
```

Stream live CSV rows directly to stdout:

```bash
dukascopy-go download \
  --symbol xauusd \
  --timeframe m1 \
  --from 2024-01-02T00:00:00Z \
  --output - \
  --simple \
  --live \
  --poll-interval 5s
```

Stream live CSV rows to stdout while keeping checkpointed partition files on disk:

```bash
dukascopy-go download \
  --symbol xauusd \
  --timeframe h1 \
  --from 2024-01-01T00:00:00Z \
  --output - \
  --simple \
  --live \
  --partition auto \
  --checkpoint-manifest ./data/xauusd-live-stream.manifest.json \
  --poll-interval 10s
```

Use live parquet output without specifying a partition mode:

```bash
dukascopy-go download \
  --symbol xauusd \
  --timeframe m1 \
  --from 2024-01-02T00:00:00Z \
  --output ./data/xauusd-live.parquet \
  --simple \
  --live \
  --poll-interval 5s
```

`--live` notes:

- it appends only newly completed intervals in non-partitioned file mode
- with `--partition`, or with parquet output, it keeps extending the checkpoint manifest and rebuilds the final output from partition files
- it supports `.csv`, `.csv.gz`, `.parquet`, and `--output -`
- parquet live output auto-enables `--partition auto` when you do not pass a partition mode yourself
- `--output -` stays as pure CSV stream; with partition/checkpoint enabled it uses a checkpoint-backed cache file internally so restarted processes keep streaming only new rows
- it cannot be combined with `--to`

Verify a manifest without downloading anything:

```bash
dukascopy-go manifest verify --manifest ./data/xauusd-m1.csv.manifest.json
```

Verify the finished output and include data quality checks:

```bash
dukascopy-go manifest verify --output ./data/xauusd-m1.csv --check-data-quality
```

Show suspicious gap ranges during verification:

```bash
dukascopy-go manifest verify --output ./data/xauusd-m1.csv --show-suspicious-gaps --suspicious-gap-limit 20
```

`stats` and `manifest verify --check-data-quality` now split gaps into two buckets:

- `expected gaps` for likely market-closed periods such as weekend, maintenance, and common holiday closures
- `suspicious gaps` for missing intervals that do not match the closure heuristic

Gap classification is profile-aware:

- `fx-24x5` for classic forex pairs such as `EURUSD`
- `otc-24x5` for instruments with daily maintenance windows such as many metals and CFDs
- `crypto-24x7` for symbols such as `BTCUSD`

`stats` auto-detects from `--symbol` or the filename when possible, and you can override it with `--market-profile`.

Repair a dataset from valid existing files:

```bash
dukascopy-go manifest repair --output ./data/xauusd-m1.csv
```

Re-download partition files that intersect suspicious timestamp gaps and rebuild the final output:

```bash
dukascopy-go manifest repair --output ./data/xauusd-m1.csv --redownload-gaps
```

Clean orphan partition and temp files:

```bash
dukascopy-go manifest prune --output ./data/xauusd-m1.csv
```

## Output Formats

Supported output targets:

- `.csv`
- `.csv.gz`
- `.parquet`
- `-` for CSV to `stdout`

Schema options:

- `--simple` writes the smallest practical schema
- `--full` writes midpoint, spread, and explicit bid/ask fields
- `--custom-columns` writes only the columns you ask for
- `--resume` is intentionally CSV-only; for durable Parquet workflows, prefer `--partition`

Simple bar schema:

```text
timestamp,open,high,low,close,volume
```

Full bar schema:

```text
timestamp,mid_open,mid_high,mid_low,mid_close,spread,volume,bid_open,bid_high,bid_low,bid_close,ask_open,ask_high,ask_low,ask_close
```

Simple tick schema:

```text
timestamp,bid,ask
```

Full tick schema:

```text
timestamp,bid,ask,bid_volume,ask_volume
```

When `--custom-columns` is used for bars, you can request any combination of `mid_*`, `bid_*`, `ask_*`, `spread`, and `volume`.

## Timeframes

Supported values:

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

You can also print them from the CLI:

```bash
dukascopy-go --list-timeframes
```

## Checkpointed Downloads

Large downloads are where `dukascopy-go` really separates itself from a simple exporter.

When `--partition` is enabled:

- Each sub-range is written to its own CSV file inside `<output>.parts/`
- A checkpoint manifest tracks partition completion, row counts, and SHA-256 hashes
- Use `--checkpoint-manifest` if you want a custom manifest path instead of `<output>.manifest.json`
- Completed partition files are reused only after passing integrity checks
- Final outputs are assembled from partition files after every partition is complete
- If a run is interrupted, only missing or invalid partitions are downloaded next time
- If the final output is later damaged, the CLI can often rebuild it from valid partition files

Supported partition values:

```text
none
auto
hour
day
week
month
year
```

`auto` uses sensible defaults based on timeframe:

```text
tick  hour
m1    day
m3    day
m5    day
m15   day
m30   day
h1    month
h4    month
d1    year
w1    week
mn1   month
```

## Reliability Features

- `--retries` and `--retry-backoff` handle transient `429` and `5xx` failures
- `--rate-limit` adds a minimum delay between requests
- `--progress` prints chunk and retry progress to `stderr`
- `stats` and `manifest` commands auto-open a compact dashboard on interactive terminals
- `--resume` appends only rows after the latest saved CSV timestamp
- Partitioned downloads keep durable intermediate files for recovery and reuse
- `stats` helps spot expected vs suspicious gaps, duplicates, unexpected intervals, and out-of-order rows

## Configuration

Default API base URL:

```text
https://jetta.dukascopy.com
```

Override it with:

```bash
DUKASCOPY_API_BASE_URL=https://jetta.dukascopy.com
```

You can also store defaults in a JSON config file:

```json
{
  "base_url": "https://jetta.dukascopy.com",
  "instruments": {
    "limit": 5
  },
  "download": {
    "timeframe": "m1",
    "simple": true,
    "retries": 5,
    "retry_backoff": "750ms",
    "rate_limit": "150ms",
    "partition": "auto",
    "parallelism": 4,
    "progress": true
  }
}
```

Use it explicitly:

```bash
dukascopy-go --config ./dukascopy.json download \
  --symbol xauusd \
  --from 2024-01-02T00:00:00Z \
  --to 2024-01-02T06:00:00Z \
  --output ./data/xauusd.csv
```

Or export it once:

```bash
export DUKASCOPY_CONFIG=./dukascopy.json
dukascopy-go instruments --query xauusd
```

## Releases

Tagged releases are built automatically through GitHub Actions and published as GitHub Release artifacts for Linux, macOS, and Windows.

Typical release flow:

```bash
git tag v0.1.0
git push origin v0.1.0
```

Inspect version metadata in any built binary:

```bash
dukascopy-go --version
```

## Multi-Language SDK & Python Wrapper

`dukascopy-go` can be compiled into a C-shared library (`.so`, `.dll`, or `.dylib`), exposing its ultra-fast native downloader to other high-level languages like **Python**, **C++**, and **C** with zero overhead.

We provide a production-ready, memory-safe Python SDK wrapper in `sdk/python/` that loads the compiled library via `ctypes` and provides a clean, pythonic interface.

### 1. Compile the Shared Library Locally

To build the native shared library for your platform, run the following command from the root of the project:

```bash
# On Linux / macOS (.so or .dylib)
go build -buildmode=c-shared -o sdk/python/libdukascopy.so ./cmd/dukascopy-go-sdk

# On Windows (.dll)
go build -buildmode=c-shared -o sdk/python/libdukascopy.dll ./cmd/dukascopy-go-sdk
```

This compiles the Go code into a native shared library and generates the corresponding C header file (`libdukascopy.h`).

### 2. High-Performance Python Usage

Ensure that the compiled library file (`libdukascopy.so`/`.dll`/`.dylib`) is placed in the same directory as `dukascopy.py`.

```python
from datetime import datetime
from sdk.python import dukascopy

try:
    # Download 1-minute bars directly to a highly optimized Parquet file
    dukascopy.download(
        symbol="EURUSD",
        timeframe="m1",
        output_path="./data/eurusd_m1.parquet",
        from_date=datetime(2026, 5, 18, 10, 0, 0),
        to_date=datetime(2026, 5, 18, 11, 0, 0),
        side="BID",
        engine="jetta",        # 'jetta' or 'datafeed'
        price_scale=5          # 5 decimal pip scale
    )
    print("Download completed successfully!")
except dukascopy.DukascopyError as e:
    print(f"Download failed: {e}")
```

### 3. Direct Database Ingestion (ClickHouse / InfluxDB)

Rather than embedding heavy database drivers into the core Go binary, `dukascopy-go` integrates beautifully with modern analytical databases by letting the Python/C SDK output ultra-optimized CSV or Parquet files and piping them directly using standard, highly-optimized client libraries.

#### ClickHouse Direct Ingestion
ClickHouse can ingest Parquet and CSV files at millions of rows per second natively.

```python
import clickhouse_connect
from datetime import datetime
from sdk.python import dukascopy

# 1. Download market data to Parquet
output_file = "./data/eurusd_m1.parquet"
dukascopy.download("EURUSD", "m1", output_file, datetime(2026, 5, 18, 10, 0), datetime(2026, 5, 18, 11, 0))

# 2. Direct high-speed file ingestion using clickhouse-connect
client = clickhouse_connect.get_client(host='localhost', port=8123)

client.command('''
    CREATE TABLE IF NOT EXISTS eurusd_m1 (
        timestamp DateTime,
        open Float64,
        high Float64,
        low Float64,
        close Float64,
        volume Float64
    ) ENGINE = MergeTree()
    ORDER BY timestamp
''')

# Bulk insert files directly into ClickHouse
client.command(f"INSERT INTO eurusd_m1 FORMAT Parquet", file_name=output_file)
print("SUCCESS: Ingested Parquet file into ClickHouse!")
```

#### InfluxDB Batch Ingestion
For time-series storage, ingest data in batches using Pandas and the official InfluxDB Python client:

```python
import pandas as pd
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# 1. Download Parquet
output_file = "./data/eurusd_m1.parquet"
# ... download ...

# 2. Read with pandas and write in batches
df = pd.read_parquet(output_file)

with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org") as client:
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    # Write DataFrame directly to InfluxDB
    write_api.write(
        bucket="market-data",
        record=df,
        data_frame_measurement_name="eurusd",
        data_frame_timestamp_column="timestamp"
    )
```

## Real-Time Streaming (`live`)

`dukascopy-go live` streams real-time tick or bar data directly to your terminal and optionally starts a local WebSocket broadcast server for programmatic clients.

Stream the latest EUR/USD ticks to stdout:

```bash
dukascopy-go live \
  --symbol eurusd \
  --timeframe tick \
  --format jsonl \
  --poll-interval 1s
```

Stream XAU/USD 1-minute bars and broadcast them over WebSocket to all connected clients:

```bash
dukascopy-go live \
  --symbol xauusd \
  --timeframe m1 \
  --format jsonl \
  --port 8080 \
  --poll-interval 30s
```

Connect any WebSocket client to `ws://localhost:8080/stream` and receive every new bar as a JSON message.

Health check the server and query active client count:

```bash
curl http://localhost:8080/health
```

Output format options:

- `jsonl` (default): one JSON object per line (tick or bar)
- `csv`: comma-separated rows with a header on the first line
- `json`: alias for `jsonl`

`live` flags:

| Flag | Default | Purpose |
| --- | --- | --- |
| `--symbol` | required | instrument to stream |
| `--timeframe` | `tick` | `tick`, `m1`, `m3`, …, `d1` |
| `--side` | `bid` | `bid` or `ask` (for bar streaming) |
| `--format` | `jsonl` | `jsonl`, `csv`, or `json` |
| `--port` | `0` (off) | start WebSocket server on this port |
| `--poll-interval` | `1s` | frequency of requests to Dukascopy API |
| `--output` | `-` | optional file path to append output |
| `--base-url` | `https://jetta.dukascopy.com` | override Dukascopy API URL |

---

## Direct Database Loading (`db-load`)

`dukascopy-go db-load` ingests a local CSV or Parquet file directly into ClickHouse or InfluxDB using their native HTTP APIs — **zero driver dependencies, zero external libraries**.

### ClickHouse Ingestion

ClickHouse accepts Parquet and CSV via HTTP POST at millions of rows per second:

```bash
# Ingest a Parquet file
dukascopy-go db-load \
  --input ./data/eurusd_m1.parquet \
  --db clickhouse \
  --url http://localhost:8123 \
  --table eurusd_m1

# Ingest a CSV file with credentials
dukascopy-go db-load \
  --input ./data/eurusd_m1.csv \
  --db clickhouse \
  --url http://localhost:8123 \
  --table eurusd_m1 \
  --user admin \
  --password secret
```

The file is streamed directly via HTTP POST. No temporary copies, no driver overhead.

### InfluxDB Ingestion

`db-load` parses the CSV and converts it to Line Protocol, then sends gzip-compressed batches to the `/api/v2/write` endpoint:

```bash
dukascopy-go db-load \
  --input ./data/eurusd_m1.csv \
  --db influxdb \
  --url http://localhost:8086 \
  --table eurusd \
  --token MY_INFLUXDB_TOKEN \
  --org my-org \
  --bucket market-data \
  --symbol eurusd
```

`db-load` flags:

| Flag | Required | Purpose |
| --- | --- | --- |
| `--input` | yes | path to local `.csv`, `.csv.gz`, or `.parquet` file |
| `--db` | yes | `clickhouse` or `influxdb` |
| `--url` | yes | database HTTP URL |
| `--table` | yes | target table name (ClickHouse) or measurement name (InfluxDB) |
| `--user` | no | ClickHouse username (default: `default`) |
| `--password` | no | ClickHouse password |
| `--token` | InfluxDB | InfluxDB API token |
| `--org` | InfluxDB | InfluxDB organization |
| `--bucket` | InfluxDB | InfluxDB bucket |
| `--symbol` | no | symbol tag to embed in InfluxDB rows |
| `--batch` | no | rows per HTTP batch (default: 10000 CH / 5000 InfluxDB) |
| `--timeout` | no | HTTP request timeout per batch (default: `120s`) |

> **InfluxDB note:** Only CSV input is supported for InfluxDB. For Parquet, first convert with `dukascopy-go download --output file.csv`.

## Roadmap

We are continuously working on transforming `dukascopy-go` into the most complete, blazing-fast, and universal historical and live market data engineering tool on GitHub. Here is our next-generation feature pipeline:

- [x] **Smart Market Calendar Skipping (Weekend Skip):** Request-level filter to skip weekend and holiday empty periods to boost downloads by **30-40%** and avoid rate limits.
- [x] **Universal Timezone Shifting & Presets:** Parameterized `--timezone` shifting with DST support, and ready-made broker presets (`--preset mt4`, `--preset mt5`, `--preset backtrader`, `--preset ninjatrader`).
- [x] **Proxy Connection Pool:** Load, cycle, and rotate SOCKS5/HTTP proxies (`--proxy-file`) in a round-robin cycle to spread connection load.
- [x] **Local Metadata Caching:** 24-hour persistent catalog cache (`~/.dukascopy/instruments_cache.json`) for zero startup overhead.
- [x] **Duplicate & Out-of-Order Line Pruner:** Utilities to cleanly parse and deduplicate CSV/Parquet files in-place: `manifest clean-duplicates`.
- [x] **Dual-Engine Downloader (.bi5 LZMA):** Native Go decompresses and parses custom binary LZMA streams directly from Dukascopy's central data feeds.
- [x] **Multi-Language SDK Bindings (Python/C/C++):** Offer pre-built CGO shared libraries and Python wrappers (`pip install dukascopy-go`) to attract quantitative finance researchers.
- [x] **Real-Time WebSockets Pipeline (`live`):** Connect directly to Dukascopy's public API and stream tick-by-tick quotes to stdout (JSONL/CSV) and broadcast live over a local WebSocket server (`ws://localhost:<port>/stream`).
- [x] **Direct Database Loading (ClickHouse/InfluxDB):** Native zero-driver HTTP command pipes to directly ingest tick and candle datasets into open-source time-series databases (`db-load`).

## Development


Run all tests:

```bash
go test ./...
```

Run end-to-end tests only:

```bash
go test ./e2e -v
```

Build locally:

```bash
go build -o dukascopy-go ./cmd/dukascopy-go
```

## Legal Disclaimer

`dukascopy-go` is not affiliated with, endorsed by, or vetted by Dukascopy Bank SA. It is an independent open-source CLI that works with Dukascopy's publicly accessible endpoints and is intended for research, automation, and data engineering workflows.
