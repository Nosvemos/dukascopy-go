<div align="center">
  <h1>dukascopy-go 🚀</h1>
  <p><b>The fastest, zero-dependency tool to download historical and real-time Dukascopy market data.</b></p>
  
  <p>
    <a href="https://github.com/Nosvemos/dukascopy-go/actions/workflows/ci.yml"><img src="https://github.com/Nosvemos/dukascopy-go/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
    <a href="https://github.com/Nosvemos/dukascopy-go/actions/workflows/release.yml"><img src="https://github.com/Nosvemos/dukascopy-go/actions/workflows/release.yml/badge.svg" alt="Release"></a>
    <a href="https://pkg.go.dev/github.com/Nosvemos/dukascopy-go"><img src="https://pkg.go.dev/badge/github.com/Nosvemos/dukascopy-go.svg" alt="Go Reference"></a>
    <a href="https://github.com/Nosvemos/dukascopy-go/releases"><img src="https://img.shields.io/github/v/release/Nosvemos/dukascopy-go" alt="Latest release"></a>
  </p>
  <p><i>Forex • Metals • Crypto • Commodities • CFDs • ETFs</i></p>
</div>

---

## ⚡ Why `dukascopy-go`?

Compared to Node.js or Python alternatives (like `dukascopy-node`), `dukascopy-go` is built for **speed, scale, and reliability**.

| Feature | `dukascopy-go` | Node.js Alternatives |
|---|---|---|
| **Speed** | 🚀 Native Go (Dual-engine: JSON + LZMA, ~100x Faster) | 🐢 Slower V8/Node.js execution |
| **Dependencies** | ✨ Zero (Standalone single binary) | 📦 Requires Node.js, NPM, and modules |
| **Resumability** | ✅ Automatic manifest checkpoints & auto-resume | ❌ Often restarts from scratch |
| **Parallel Workers** | ✅ Built-in partitioning & concurrent downloading | ❌ Usually single-threaded |
| **Direct DB Load** | 🔥 Streams directly to ClickHouse / InfluxDB | ❌ Requires manual insertion scripts |
| **Real-time Stream**| ✅ Native WebSocket & Stdout (JSONL/CSV) | ⚠️ Node.js API only |
| **SDK Support** | ✅ CLI, Go, CGO, Python wrapper | ⚠️ Node.js only |
| **Deduplication** | ✅ In-place atomic duplicate/gap repair | ❌ Often requires Pandas/external tools |

---

## 🚀 Installation

You don't need Go installed to run the pre-built binaries. Just grab the latest `.exe`, macOS binary, or Linux binary from the **[Releases page](https://github.com/Nosvemos/dukascopy-go/releases)**.

If you have **Go 1.22+**, you can install or run it directly:

```bash
# Install globally
go install github.com/Nosvemos/dukascopy-go/cmd/dukascopy-go@latest

# Or run without installing
go run github.com/Nosvemos/dukascopy-go/cmd/dukascopy-go@latest --help

# Build from source
go build -o dukascopy-go ./cmd/dukascopy-go
```

---

## 📖 Quick Start

### 🪄 Interactive Wizard (Zero Config!)
Don't want to learn CLI flags? Just run the binary without arguments to launch the **Interactive Setup Wizard**:
```bash
dukascopy-go
```
*Use your arrow keys to select instruments, timeframes, and formats interactively.*

### 🔍 Find Instruments
Search for a specific instrument, or list them all:
```bash
dukascopy-go instruments --query xauusd
dukascopy-go instruments  # Lists all available
```

### 📉 Download Historical Data
Download 1-minute gold bars to CSV using a **flexible duration** (`--last 30d`) or exact dates (`YYYY-MM-DD`):
```bash
# Download the last 30 days of 1-minute data
dukascopy-go download \
  --symbol xauusd \
  --timeframe m1 \
  --last 30d \
  --output ./data/xauusd-m1.csv

# Or specify exact dates
dukascopy-go download \
  --symbol xauusd \
  --timeframe m1 \
  --from 2024-01-01 \
  --to 2024-01-02 \
  --output ./data/xauusd-m1.csv
```

**Need a massive dataset?** Use parallel downloading and Parquet (or JSONL) output:
```bash
dukascopy-go download \
  --symbol xauusd \
  --timeframe m1 \
  --from 2020-01-01 \
  --to 2024-01-01 \
  --output ./data/xauusd.parquet \
  --partition auto \
  --parallelism 8
```

### 📈 Stream Real-Time Ticks
Pipe live ticks directly to your terminal or log file:
```bash
dukascopy-go live --symbol eurusd --timeframe tick --format jsonl
```
*Tip: Add `--port 8080` to spin up a zero-dependency WebSocket server!*

### 💾 Load Directly to Database
Stream downloaded data right into PostgreSQL, ClickHouse or InfluxDB without writing custom scripts:
```bash
dukascopy-go db-load \
  --input ./data/eurusd-m1.csv \
  --db postgres \
  --url "postgres://user:pass@localhost:5432/market_data?sslmode=disable" \
  --table eurusd_m1
```

---

## 🛠️ CLI Commands & Options

### Commands Overview
| Command | Purpose |
| --- | --- |
| `instruments` | Search or list all Dukascopy instruments |
| `download` | Download historical data as CSV or Parquet |
| `live` | Stream real-time ticks/bars to stdout and WebSocket |
| `db-load` | Ingest CSV/Parquet directly into ClickHouse or InfluxDB |
| `stats` | Inspect a dataset for gaps, duplicates, and ordering issues |
| `manifest *` | Checkpoint tools: `inspect`, `verify`, `repair`, `prune` |
| `list-timeframes` | Print supported timeframe values (`tick`, `m1`, `h1`, `d1`, etc.) |

### `download` Configuration

| Flag | Description | Example / Default |
| --- | --- | --- |
| `--symbol` | **(Required)** Instrument to download | `eurusd`, `btcusd` |
| `--timeframe` | **(Required)** Resolution of data | `tick`, `m1`, `h1`, `d1` |
| `--last` | **(Optional)** Duration to download back from now | `30d`, `6mo`, `1y` |
| `--from` | **(Required unless --last)** Start time | `2024-01-01` or RFC3339 |
| `--to` | End time. Defaults to *now* | `2024-01-02` |
| `--output` | **(Required)** Output file path | `./data.csv` or `-` for stdout |
| `--simple` / `--full`| Schema type (OHLCV vs Bid/Ask/Spread) | `--simple` |
| `--custom-columns` | Customize output fields | `timestamp,bid_open,ask_open`|
| `--partition` | Chunking by time (`auto`, `day`, `month`) | `none` |
| `--parallelism` | Concurrent workers for partitions | `1` |
| `--resume` | Resume appending to existing CSV | `false` |
| `--timezone` | Shift timestamps to a local TZ | `UTC`, `EST`, `Europe/London`|
| `--preset` | Output presets for specific platforms | `mt4`, `mt5`, `ninjatrader` |

*(Run `dukascopy-go download --help` for the full list of flags including rate-limiting and proxies)*

---

## 🛡️ Robust Architecture

`dukascopy-go` is built with enterprise-grade data engineering in mind:

- **Context-aware Resumability:** Using the `--partition` flag, large downloads are split into small chunks managed by a `.manifest.json`. If your internet drops, you can instantly resume exactly where it failed. You can also run `dukascopy-go manifest repair` to fix corrupted chunks.
- **In-place Deduplication:** Market data can be messy. The pruner automatically detects and eliminates duplicate records or out-of-order ticks, guaranteeing chronological integrity.
- **Proxy Rotation:** Bypassing strict IP rate-limits is easy. Provide a `--proxy-file` containing HTTP/SOCKS5 proxies, and the engine will round-robin through them.

---

## 💻 Python & C SDK 

Not using the CLI? `dukascopy-go` compiles to a C shared library (`.so`, `.dll`, `.dylib`) which can be wrapped in almost any language. We provide a **Python `ctypes` SDK** out of the box!

```python
from sdk.python.dukascopy import DukascopyClient

client = DukascopyClient()
bars = client.download_bars('eurusd', 'm1', '2024-01-01T00:00:00Z', '2024-01-02T00:00:00Z')
print(bars)
```
*(Check the `sdk/python` directory for full usage examples)*

---

## 🤝 Contributing
Contributions, issues and feature requests are welcome! Feel free to check the [issues page](https://github.com/Nosvemos/dukascopy-go/issues).

<div align="center">
  <i>Built for traders, researchers, and quants.</i>
</div>

---

## ⚙️ Configuration

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

## 🛠 Development

```bash
# Run all tests
go test ./...

# Run only e2e tests
go test ./tests/e2e -v

# Build the binary locally
go build -o dukascopy-go ./cmd/dukascopy-go
```

---

## 🗺 Roadmap

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

## ⚖️ Legal Disclaimer

`dukascopy-go` is not affiliated with, endorsed by, or vetted by Dukascopy Bank SA. It is an independent open-source tool that works with Dukascopy's publicly accessible endpoints and is intended for research, automation, and data engineering workflows.
