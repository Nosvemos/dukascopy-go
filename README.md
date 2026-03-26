# dukascopy-data-downloader

A Go CLI for searching Dukascopy instruments and exporting historical market data from the Dukascopy `jetta` API.

## Features

- Resolves flexible symbols such as `xauusd`, `eur/usd`, and `BTC-USD`
- Supports any instrument returned by Dukascopy's `/v1/instruments` catalog, not just `XAUUSD`
- Searches instruments with the `instruments` command
- Downloads `tick`, `m1`, `m3`, `m5`, `m15`, `m30`, `h1`, `h4`, `d1`, `w1`, and `mn1` data as CSV
- Downloads `tick`, `m1`, `m3`, `m5`, `m15`, `m30`, `h1`, `h4`, `d1`, `w1`, and `mn1` data as CSV or Parquet
- Supports reduced, expanded, and custom CSV column sets with `--simple`, `--full`, and `--custom-columns`
- Supports plain `.csv` and compressed `.csv.gz` output paths
- Supports `.parquet` output with the same selected columns
- Can stream CSV directly to `stdout` with `--output -`
- Exports raw volume values intended to be more suitable for backtesting
- Retries transient HTTP failures with configurable retry count and backoff
- Can throttle request pace with `--rate-limit`
- Shows optional progress output for chunk downloads and retries
- Can resume into an existing CSV without duplicating the last saved row
- Supports partitioned downloads with checkpoint manifests for large ranges
- Can download partitioned ranges with multiple workers via `--parallelism`
- Reassembles the final CSV from completed partition files after interrupted runs
- Audits completed partition files with row counts and SHA-256 hashes before reusing them
- Audits the assembled final CSV and stores a manifest summary for quick verification
- Includes `manifest inspect` and `manifest verify` commands for offline dataset checks
- Includes `manifest repair` for download-free recovery from valid existing files
- Includes `manifest prune` to clean orphan partition and temp files safely
- Includes a `stats` command for quick dataset inspection, gap detection, and ordering checks
- Supports JSON config files through `--config` or `DUKASCOPY_CONFIG`
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

Run with a shared config file:

```bash
go run ./cmd/dukascopy --config ./dukascopy.json --help
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

Download 1-minute bars as Parquet:

```bash
go run ./cmd/dukascopy download \
  --symbol xauusd \
  --timeframe m1 \
  --from 2024-01-02T00:00:00Z \
  --to 2024-01-02T06:00:00Z \
  --output ./data/xauusd-1m.parquet \
  --full
```

## Installation

Install a local binary:

```bash
go install github.com/Nosvemos/dukascopy-data-downloader/cmd/dukascopy@latest
```

Run directly from the published module without cloning the repository:

```bash
go run github.com/Nosvemos/dukascopy-data-downloader/cmd/dukascopy@latest --help
```

Build from a local checkout:

```bash
go build -o dukascopy-data ./cmd/dukascopy
```

## Releases

Tagged releases are built automatically through GitHub Actions and published as GitHub Release artifacts for Linux, macOS, and Windows.

Typical release flow:

```bash
git tag v0.1.0
git push origin v0.1.0
```

Each release embeds:

- semantic version
- short commit hash
- build date

You can inspect the installed binary with:

```bash
dukascopy-data --version
```

## Usage Patterns

Search instruments:

```bash
go run ./cmd/dukascopy instruments --query xauusd
```

Search instruments as JSON:

```bash
go run ./cmd/dukascopy instruments --query xauusd --json
```

Print dataset stats:

```bash
go run ./cmd/dukascopy stats --input ./data/xauusd.csv
```

Print dataset stats as JSON:

```bash
go run ./cmd/dukascopy stats --input ./data/xauusd.csv --json
```

Inspect a Parquet dataset:

```bash
go run ./cmd/dukascopy stats --input ./data/xauusd.parquet
```

Inspect a checkpoint manifest:

```bash
go run ./cmd/dukascopy manifest inspect --output ./data/xauusd.csv
```

Verify a dataset against its manifest without downloading anything:

```bash
go run ./cmd/dukascopy manifest verify --manifest ./data/xauusd.csv.manifest.json
```

Verify manifest integrity plus duplicate and ordering quality checks:

```bash
go run ./cmd/dukascopy manifest verify --output ./data/xauusd.csv --check-data-quality
```

Repair a damaged final CSV or missing partition from existing valid files:

```bash
go run ./cmd/dukascopy manifest repair --output ./data/xauusd.csv
```

Clean orphan partition files and leftover temp artifacts:

```bash
go run ./cmd/dukascopy manifest prune --output ./data/xauusd.csv
```

Print version metadata:

```bash
go run ./cmd/dukascopy --version
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

Download compressed CSV directly:

```bash
go run ./cmd/dukascopy download \
  --symbol xauusd \
  --timeframe m1 \
  --from 2024-01-02T00:00:00Z \
  --to 2024-01-02T06:00:00Z \
  --output ./data/xauusd-minute.csv.gz \
  --simple
```

Download Parquet directly:

```bash
go run ./cmd/dukascopy download \
  --symbol xauusd \
  --timeframe m1 \
  --from 2024-01-02T00:00:00Z \
  --to 2024-01-02T06:00:00Z \
  --output ./data/xauusd-minute.parquet \
  --simple
```

Stream CSV to stdout:

```bash
go run ./cmd/dukascopy download \
  --symbol xauusd \
  --timeframe m1 \
  --from 2024-01-02T00:00:00Z \
  --to 2024-01-02T00:03:00Z \
  --output - \
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

Resume an interrupted download:

```bash
go run ./cmd/dukascopy download \
  --symbol xauusd \
  --timeframe m1 \
  --from 2024-01-02T00:00:00Z \
  --to 2024-01-02T06:00:00Z \
  --output ./data/xauusd-minute.csv \
  --simple \
  --resume
```

Download with progress and custom retry settings:

```bash
go run ./cmd/dukascopy download \
  --symbol xauusd \
  --timeframe m1 \
  --from 2024-01-02T00:00:00Z \
  --to 2024-01-02T06:00:00Z \
  --output ./data/xauusd-minute.csv \
  --simple \
  --progress \
  --retries 5 \
  --retry-backoff 750ms
```

Download with request pacing:

```bash
go run ./cmd/dukascopy download \
  --symbol xauusd \
  --timeframe m1 \
  --from 2024-01-02T00:00:00Z \
  --to 2024-01-02T06:00:00Z \
  --output ./data/xauusd-minute.csv \
  --simple \
  --rate-limit 150ms
```

Download a large range with partition checkpoints:

```bash
go run ./cmd/dukascopy download \
  --symbol xauusd \
  --timeframe m1 \
  --from 2024-01-01T00:00:00Z \
  --to 2024-02-01T00:00:00Z \
  --output ./data/xauusd-january.csv \
  --simple \
  --partition auto \
  --progress
```

Download a large range with parallel partition workers:

```bash
go run ./cmd/dukascopy download \
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

Use a custom checkpoint manifest path:

```bash
go run ./cmd/dukascopy download \
  --symbol eurusd \
  --timeframe h1 \
  --from 2024-01-01T00:00:00Z \
  --to 2024-03-01T00:00:00Z \
  --output ./data/eurusd-h1.csv \
  --full \
  --partition month \
  --checkpoint-manifest ./data/eurusd-h1.checkpoint.json
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
- `--config` loads JSON defaults for commands. Explicit CLI flags always override config values.
- If `--output` ends with `.gz`, the CLI writes gzip-compressed CSV.
- If `--output` ends with `.parquet`, the CLI writes Parquet instead of CSV.
- If `--output` is `-`, the CLI writes raw CSV rows to `stdout` and skips the success summary line.
- `--resume` reuses an existing CSV header and appends only rows after the last saved record.
- `--resume` requires the selected columns to include `timestamp`.
- `--resume` is intentionally CSV-only. For durable Parquet downloads, use `--partition` and checkpoints instead.
- `--retries` and `--retry-backoff` control retry behavior for transient HTTP failures such as `429` and `5xx`.
- `--rate-limit` adds a minimum delay between HTTP requests. This is useful when you want gentler pacing against the upstream API.
- `--progress` prints chunk and retry updates to `stderr`.
- `--partition` splits a large range into durable sub-ranges. Supported values are `none`, `auto`, `hour`, `day`, `week`, `month`, and `year`.
- `--parallelism` controls how many partition workers run at once. Values greater than `1` require `--partition`.
- `--checkpoint-manifest` overrides the default checkpoint path. If omitted, the CLI uses `<output>.manifest.json`.
- Partitioned downloads keep completed part files in `<output>.parts/` and reuse them automatically on the next run.
- Reused partition files are audited before reuse. If row count or SHA-256 hash does not match the manifest, the CLI downloads that partition again.
- The final output file is assembled from the partition files after all partitions are complete, then audited with row count and SHA-256.
- The manifest includes a summary section with total partition counts and final output row totals.
- If the final output file is modified or corrupted later, the next run detects the mismatch and re-assembles it from valid partition files.
- `--list-timeframes` prints the currently supported timeframe values and their meaning.
- `stats` prints row counts, timestamp range, duplicate counts, inferred timeframe, expected interval, gap counts, and out-of-order row counts for CSV, CSV.GZ, and Parquet files.
- `manifest inspect` prints a readable manifest summary and partition status table.
- `manifest prune` removes orphan partition files and leftover temp files that are no longer referenced by the manifest.
- `manifest repair` tries to rebuild missing or invalid partition files from a valid final CSV, or rebuild the final CSV from valid partition files.
- `manifest verify` audits partition files and the final output CSV against the manifest and exits non-zero on mismatch.
- `manifest verify --check-data-quality` also inspects the final CSV for duplicate rows, duplicate timestamps, and out-of-order records. Gaps are reported as warnings.
- `--version` prints embedded version, commit, and build date metadata.
- `--side` controls the primary side for simple bar exports.
- Commands use ANSI colors for headings, success messages, and table headers by default; set `NO_COLOR=1` to disable coloring.

## Checkpointed Downloads

Partitioned downloads are the safest option for long-running jobs.

What happens when `--partition` is enabled:

- Each sub-range is written to its own CSV file inside `<output>.parts/`.
- Partition files stay in CSV form even when the final output is Parquet. This keeps repair and re-assembly simple and durable.
- A checkpoint manifest tracks which partitions are already complete.
- The manifest stores row counts and SHA-256 hashes for completed partition files.
- The manifest also stores the final output audit and a summary of completed work.
- If the process crashes or the network fails, completed partition files remain intact.
- Running the same command again reuses completed partitions and only downloads missing or invalid ones.
- If `--parallelism` is greater than `1`, multiple partitions can download at the same time while the manifest is still updated safely after each completed part.
- After every partition is complete, the CLI assembles the final output file from the partition files and audits it.

`auto` chooses a partition size based on timeframe:

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

## Configuration

Default API base URL:

```text
https://jetta.dukascopy.com
```

You can override it with the `DUKASCOPY_API_BASE_URL` environment variable. This is mainly useful for tests or local mocks.

The CLI can also load defaults from a JSON config file:

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

Pass it explicitly:

```bash
dukascopy-data --config ./dukascopy.json instruments --query xauusd
```

Or export it once:

```bash
export DUKASCOPY_CONFIG=./dukascopy.json
dukascopy-data download --symbol xauusd --from 2024-01-02T00:00:00Z --to 2024-01-02T06:00:00Z --output ./data/xauusd.csv
```

## Tests

Run all tests:

```bash
go test ./...
```

Run end-to-end tests only:

```bash
go test ./e2e -v
```
