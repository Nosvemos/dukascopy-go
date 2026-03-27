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

- Search Dukascopy instruments from the CLI with `instruments`
- Download `tick`, `m1`, `m3`, `m5`, `m15`, `m30`, `h1`, `h4`, `d1`, `w1`, and `mn1`
- Export to `.csv`, `.csv.gz`, or `.parquet`
- Use compact, full, or custom column layouts with `--simple`, `--full`, and `--custom-columns`
- Stream CSV directly to `stdout` with `--output -`
- Resume interrupted CSV downloads without duplicating the final saved row
- Keep CSV or CSV.GZ outputs continuously updated with `--live`
- Split long ranges into durable partitions with `--partition`
- Run partition workers in parallel with `--parallelism`
- Reassemble final outputs from completed partition files after interruptions
- Verify row counts and SHA-256 hashes before reusing partition files
- Audit finished datasets with `stats`, `manifest inspect`, `manifest verify`, `manifest repair`, and `manifest prune`
- Configure defaults with `--config` or `DUKASCOPY_CONFIG`
- Built-in retry, backoff, rate limiting, and interactive terminal dashboards

## Installation

| Method | Command |
| --- | --- |
| Install binary with Go | `go install github.com/Nosvemos/dukascopy-go/cmd/dukascopy-go@latest` |
| Run without cloning | `go run github.com/Nosvemos/dukascopy-go/cmd/dukascopy-go@latest --help` |
| Build from local source | `go build -o dukascopy-go ./cmd/dukascopy-go` |

`Go 1.26+` is the current target in this repository.

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
