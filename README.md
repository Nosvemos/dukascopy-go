<div align="center">
  <h1>dukascopy-go 🚀</h1>
  <p><b>The fastest, zero-dependency tool to download historical and real-time Dukascopy market data.</b></p>

  <img width="800" height="210" alt="download" src="https://github.com/user-attachments/assets/f240008c-5e87-4139-bddb-20b55ac15743" />
  
  <p>
    <a href="https://github.com/Nosvemos/dukascopy-go/actions/workflows/ci.yml"><img src="https://github.com/Nosvemos/dukascopy-go/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
    <a href="https://github.com/Nosvemos/dukascopy-go/actions/workflows/release.yml"><img src="https://github.com/Nosvemos/dukascopy-go/actions/workflows/release.yml/badge.svg" alt="Release"></a>
    <a href="https://pkg.go.dev/github.com/Nosvemos/dukascopy-go"><img src="https://pkg.go.dev/badge/github.com/Nosvemos/dukascopy-go.svg" alt="Go Reference"></a>
    <a href="https://github.com/Nosvemos/dukascopy-go/releases"><img src="https://img.shields.io/github/v/release/Nosvemos/dukascopy-go" alt="Latest release"></a>
  </p>
  <p><i>Forex (100+ Pairs) • Metals • Crypto • Commodities • CFDs • Stocks • Indices</i></p>
</div>

---

## ⚡ Why `dukascopy-go`?

| Feature | `dukascopy-go` | Node.js Alternatives |
|---|---|---|
| **Speed** | 🚀 Native Go + 64KB Sequential Buffering (~100× Faster) | 🐢 Slower V8/Node.js execution |
| **Parities** | 🌍 170+ Offline Fallback Database + Online Hot-Reload | ⚠️ Requires full online resolution |
| **Multi-Symbol** | ✅ Comma-separated batch with automatic path formatting | ❌ Single symbol only |
| **Dependencies** | ✨ Zero — standalone single binary | 📦 Requires Node.js, NPM, modules |
| **Delta Sync** | ✅ In-place sync to append missing slices | ❌ Restarts from scratch |
| **Gap Filling** | ✅ Quant-grade forward-filling with weekend/holiday awareness | ❌ Leaves gaps as-is |
| **Throttling** | ✅ AIMD Adaptive Rate Limiting (self-corrects on 429) | ❌ Fixed rate limits |
| **Resumability** | ✅ SHA-256 manifest checkpoints & auto-resume | ❌ Restarts from scratch |
| **Real-time** | ✅ Native WebSocket & Stdout (JSONL/CSV) | ⚠️ Node.js API only |
| **SDK** | ✅ Go library, Python + C wrapper | ⚠️ Node.js only |
| **Deduplication** | ✅ In-place atomic duplicate/gap repair | ❌ Requires external tools |

---

## 🚀 Installation

Pre-built binaries are available on the **[Releases page](https://github.com/Nosvemos/dukascopy-go/releases)** — no Go required.

With **Go 1.22+**:

```bash
go install github.com/Nosvemos/dukascopy-go/cmd/dukascopy-go@latest
```

---

## 📖 Quick Start

Run without arguments to launch the **Interactive Setup Wizard**:

```bash
dukascopy-go
```

Or jump straight to the CLI:

```bash
# Search instruments
dukascopy-go instruments --query gold

# Download 30 days of 1-minute bars
dukascopy-go download --symbol xauusd --timeframe m1 --last 30d --output ./xauusd.csv

# Multi-year tick data with resume (OOM-free)
dukascopy-go download --symbol xauusd --timeframe tick --from 2020-01-01 --to 2024-01-01 \
  --output ./xauusd.parquet --resume --progress

# Batch-download multiple symbols
dukascopy-go download --symbol EUR/USD,GBP/USD,BTC/USD --timeframe d1 --last 1y --output ./data/

# Parallel partitioned download
dukascopy-go download --symbol xauusd --timeframe m1 --from 2020-01-01 --to 2024-01-01 \
  --output ./xauusd.parquet --partition auto --parallelism 8
```

---

## 📋 Download Flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `--symbol` | `string` | *(required)* | Instrument code. Comma-separated for batches: `eurusd,gbpusd` |
| `--timeframe` | `string` | `m1` | `tick`, `m1`, `m3`, `m5`, `m15`, `m30`, `h1`, `h4`, `d1`, `w1`, `mn1` |
| `--side` | `string` | `bid` | `bid` or `ask` |
| `--output` | `string` | *(required)* | `.csv`, `.csv.gz`, `.parquet`, `.jsonl`, `.arrow`, `.ipc`, `.feather` |
| `--last` | `duration` | — | Relative window: `30d`, `6mo`, `1y`. Overrides `--from`/`--to` |
| `--from` | `string` | — | `YYYY-MM-DD`, `YYYY-MM-DD HH:MM`, or RFC3339 |
| `--to` | `string` | — | Same formats as `--from` |
| `--resume` | `bool` | `false` | Chunked cache & resume. Splits range into daily `.part` chunks; auto-skips completed on re-run |
| `--simple` | `bool` | `false` | Reduced column set |
| `--full` | `bool` | `false` | Full Bid + Ask candlestick fields |
| `--fused` | `bool` | `false` | Fused Bid/Ask with dynamic spread (no mid fields) |
| `--custom-columns` | `string` | — | Explicit comma-separated column projection |
| `--progress` | `bool` | auto | Force-enable interactive progress TUI |
| `--tui-theme` | `string` | `default` | `default`, `catppuccin`, `nord`, `gruvbox`, `dracula` |
| `--partition` | `string` | `none` | `auto`, `hour`, `day`, `week`, `month`, `year` |
| `--hive` | `bool` | `false` | Enable Hive-style directory partitioning (e.g. `year=YYYY/month=MM/day=DD/`) |
| `--parallelism` | `int` | `1` | Concurrent partition download workers |
| `--fill-gaps` | `string` | `none` | `forward` — forward-fills during active market sessions |
| `--timezone` | `string` | `UTC` | `Europe/London`, `EST`, `MT4`, `MT5` |
| `--preset` | `string` | — | `mt4`, `mt5`, `backtrader`, `ninjatrader` |
| `--engine` | `string` | `jetta` | `jetta` (JSON) or `datafeed` (`.bi5` binary) |

---

## ⚙️ Configuration File Support

You can configure global defaults for `dukascopy-go` using a JSON, YAML, or TOML configuration file.

Specify the configuration file using either the `--config` flag or the `DUKASCOPY_CONFIG` environment variable:

```bash
# Using the CLI flag
dukascopy-go --config ./config.yaml download --symbol xauusd --output ./xauusd.csv

# Using the environment variable
export DUKASCOPY_CONFIG=./config.toml
dukascopy-go download --symbol xauusd --output ./xauusd.csv
```

### Configuration Schema (YAML Example)

Here is a sample YAML configuration file showing the available options:

```yaml
base_url: "https://jetta.dukascopy.com"
instruments:
  limit: 100
download:
  timeframe: "m1"
  side: "bid"
  simple: true
  retries: 3
  retry_backoff: "100ms"
  rate_limit: "50ms"
  resume: true
  parallelism: 4
```

---

## 💎 Core Features

### 🔄 Smart Delta Sync
Keep datasets current without re-downloading. Inspects the last timestamp in-place and appends only missing data:

```bash
dukascopy-go sync --symbol EUR/USD --output ./data/eur_usd.csv
```

### 💾 Chunked Cache & Resume
Multi-year tick datasets stay O(1) memory. Daily `.part` chunks with SHA-256 manifests survive any interruption:

```bash
dukascopy-go download --symbol xauusd --timeframe tick --from 2020-01-01 --to 2024-01-01 \
  --output ./xauusd.parquet --resume
```

Manage with `manifest` subcommands: `inspect`, `verify`, `repair`, `clean-duplicates`.

### 📊 Gap Filling
Forward-fills liquidity gaps while automatically skipping weekends and holidays:

```bash
dukascopy-go download --symbol EUR/USD --timeframe m1 --last 7d --output ./data.csv --fill-gaps forward
```

### 🛡️ Adaptive Throttling
AIMD rate limiter: backs off instantly on `429` (doubles up to 5s), recovers by 10ms per success.

### 📈 Real-Time Streaming
High-frequency polling engine with built-in RFC-6455 WebSocket server:

```bash
dukascopy-go live --symbol eurusd --timeframe tick --format jsonl --port 8080
```

---

## 🗄️ Direct Database Ingestion

Stream CSV or Parquet files into your time-series database — millions of rows/second, zero intermediary drivers:

| Database | Engine | Write Path |
|---|---|---|
| **ClickHouse** | Native HTTP stream | CSVWithNames / Parquet |
| **InfluxDB v2** | Line Protocol gzip batches | `/api/v2/write` |
| **PostgreSQL / TimescaleDB** | `COPY FROM` CSV | Auto-detects TimescaleDB, creates hypertables |
| **QuestDB** | ILP over TCP & HTTP | TCP port 9009 (recommended) or HTTP 9000 |

```bash
# ClickHouse
dukascopy-go db-load --input ./xauusd.csv --db clickhouse --url http://localhost:8123 --table xauusd

# InfluxDB v2
dukascopy-go db-load --input ./xauusd.csv --db influxdb --url http://localhost:8086 \
  --org myorg --bucket marketdata --token <token> --table xauusd --symbol xauusd

# PostgreSQL / TimescaleDB
dukascopy-go db-load --input ./xauusd.csv --db postgres \
  --url postgres://user:pass@localhost:5432/marketdata --table xauusd

# QuestDB (TCP ILP — lowest latency)
dukascopy-go db-load --input ./xauusd.csv --db questdb --url tcp://localhost:9009 --table xauusd
```

**TimescaleDB** extension is auto-detected; hypertables are created with inferred chunk intervals from the filename (`m1` → 1 day, `h1` → 7 days, `d1` → 30 days). Override with `--chunk-interval`, disable with `--create-hypertable=false`.

**QuestDB ILP** defaults to TCP (`tcp://host:9009`) with `TCP_NODELAY` for minimal latency. HTTP fallback via `http://host:9000`. Override port with `--ilp-port`.

`db-load` flags: `--input` (required), `--db` (required), `--url` (required), `--table` (required), `--user`, `--password`, `--token`, `--org`, `--bucket`, `--symbol`, `--batch`, `--timeout`, `--ilp-port`, `--create-hypertable`, `--chunk-interval`.

---

## 🐹 Go SDK

```go
import "github.com/Nosvemos/dukascopy-go/pkg/dukascopy"

client := dukascopy.NewClient("https://jetta.dukascopy.com", 30*time.Second)
result, _ := client.Download(context.Background(), dukascopy.DownloadRequest{
    Symbol:      "EUR/USD",
    Granularity: dukascopy.GranularityD1,
    Side:        dukascopy.PriceSideBid,
    From:        time.Now().AddDate(0, 0, -5),
    To:          time.Now(),
})
fmt.Printf("Downloaded %d bars: %s\n", len(result.Bars), result.Instrument.Name)
```

Full Go reference: [pkg.go.dev](https://pkg.go.dev/github.com/Nosvemos/dukascopy-go)

---

## 🐍 Python SDK

```python
import dukascopy_go as dukascopy
from datetime import datetime

# Sync download
dukascopy.download(symbol="EURUSD", timeframe="m1",
    output_path="./eurusd_m1.csv",
    from_date=datetime(2026, 5, 18, 10, 0),
    to_date=datetime(2026, 5, 18, 11, 0))

# Async download (does not block the event loop)
await dukascopy.download_async(symbol="EURUSD", timeframe="m1",
    output_path="./eurusd_m1.parquet",
    from_date=datetime(2026, 5, 18, 10, 0),
    to_date=datetime(2026, 5, 18, 11, 0))

# Directly into a pandas DataFrame (no file management needed)
df = dukascopy.to_dataframe(symbol="EURUSD", timeframe="m1",
    from_date=datetime(2026, 5, 18, 10, 0),
    to_date=datetime(2026, 5, 18, 11, 0))

# Async DataFrame
df = await dukascopy.to_dataframe_async(symbol="EURUSD", timeframe="m1",
    from_date=datetime(2026, 5, 18, 10, 0),
    to_date=datetime(2026, 5, 18, 11, 0))

# Stream into database
dukascopy.db_load(db_type="postgres", db_url="postgres://user:pass@localhost:5432/marketdata",
    table_name="eurusd_m1", input_path="./eurusd_m1.csv")
```

**Requirements:** Python 3.9+ (for `asyncio.to_thread()`). Install with:
```bash
pip install dukascopy-go           # core (no pandas)
pip install 'dukascopy-go[pandas]' # with pandas + pyarrow for to_dataframe()
```

See `sdk/python/` for full examples and installation guide.

---

## 🛠️ Development

```bash
git clone https://github.com/Nosvemos/dukascopy-go.git
cd dukascopy-go
go mod download
go test ./...
go build -o dukascopy-go ./cmd/dukascopy-go
```

---

## 🤝 Contributing

1. Fork the project
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

---

## ⚖️ Legal Disclaimer

`dukascopy-go` is not affiliated with, endorsed by, or vetted by Dukascopy Bank SA. It is an independent open-source tool that works with Dukascopy's publicly accessible endpoints and is intended for research, automation, and data engineering workflows.
