<div align="center">
  <h1>dukascopy-go 🚀</h1>
  <p><b>The fastest, zero-dependency engine for historical tick/bar downloads, real-time streaming, and quant feature engineering.</b></p>

  <img width="800" height="210" alt="download" src="https://github.com/user-attachments/assets/f240008c-5e87-4139-bddb-20b55ac15743" />
  
  <p>
    <a href="https://github.com/Nosvemos/dukascopy-go/actions/workflows/ci.yml"><img src="https://github.com/Nosvemos/dukascopy-go/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
    <a href="https://github.com/Nosvemos/dukascopy-go/actions/workflows/release.yml"><img src="https://github.com/Nosvemos/dukascopy-go/actions/workflows/release.yml/badge.svg" alt="Release"></a>
    <a href="https://pkg.go.dev/github.com/Nosvemos/dukascopy-go"><img src="https://pkg.go.dev/badge/github.com/Nosvemos/dukascopy-go.svg" alt="Go Reference"></a>
    <a href="https://github.com/Nosvemos/dukascopy-go/releases"><img src="https://img.shields.io/github/v/release/Nosvemos/dukascopy-go" alt="Latest release"></a>
  </p>
  <p><i>Forex (100+ Pairs) • Precious Metals • Cryptocurrencies • Commodities • CFDs • Stocks • Indices</i></p>
</div>

---

## ⚡ Why `dukascopy-go`?

| Feature | `dukascopy-go` | Legacy Node.js / Python Tools |
|---|---|---|
| **Speed & Concurrency** | 🚀 Native Go + Multi-Worker Parallelism (~100× Faster) | 🐢 Slow single-threaded scripts |
| **Quant Features & ML** | 🧠 On-the-fly Indicators (RSI, EMA, ATR, VWAP, Volatility) | ❌ Raw OHLCV only, requires pandas post-processing |
| **Alternative Bar Sampling** | 📊 Tick, Volume, and Dollar/Nominal Bars | ❌ Time-based bars only |
| **Data Lake & Cloud** | ☁️ Direct S3 / Cloudflare R2 / GCS / MinIO Streaming Upload | ❌ Local disk only |
| **Streaming & Trade Bots** | 🤖 Native WebSocket, Redis Streams/PubSub, NATS | ⚠️ Custom wrappers required |
| **Price & Indicator Watch** | 🔔 `watch` CLI with custom triggers and Webhook POSTs | ❌ Not available |
| **Microstructure & OFI** | 🔬 Order Flow Imbalance, Effective Spread, Amihud Illiquidity | ❌ Not available |
| **Database Ingestion** | 🗄️ Zero-driver fast ingest into ClickHouse, QuestDB, TimescaleDB, InfluxDB | ⚠️ Slow python loops / external ETL |
| **Outlier Scrubbing** | 🛡️ Rolling MAD (Median Absolute Deviation) bad tick scrubber | ❌ Raw noisy ticks |
| **Zero Dependencies** | ✨ Single static binary — no Python/Node runtime needed | 📦 Heavy dependency trees |
| **Resumability** | ✅ SHA-256 chunk manifests & automatic delta sync | ❌ Restarts from scratch |

---

## 🚀 Installation

Pre-built standalone binaries for **Windows, Linux, and macOS (Apple Silicon & Intel)** are available on the **[Releases Page](https://github.com/Nosvemos/dukascopy-go/releases)**.

### Install via Go (1.22+):

```bash
go install github.com/Nosvemos/dukascopy-go/cmd/dukascopy-go@latest
```

### Install Python SDK:

```bash
pip install dukascopy-go           # Core SDK
pip install 'dukascopy-go[pandas]' # With Pandas & PyArrow integration
```

---

## 📖 Quick Start

### 1. Interactive Setup Wizard
Run without arguments to launch the interactive terminal UI:
```bash
dukascopy-go
```

### 2. Search Instruments
Search the offline database and online catalog:
```bash
dukascopy-go instruments --query gold
```

### 3. Fast Parallel Download
Download 1 year of M1 data across 8 parallel workers into Parquet:
```bash
dukascopy-go download --symbol eurusd --timeframe m1 --last 1y \
  --output ./eurusd.parquet --parallelism 8
```

### 4. Quant Feature Engineering on Download
Generate machine learning features directly into your dataset:
```bash
dukascopy-go download --symbol eurusd --timeframe m1 --last 30d --output ./eurusd_ml.parquet \
  --features "returns:log,rsi:14,ema:20,ema:50,atr:14,vwap,volatility:garman-klass"
```

### 5. Alternative Bar Sampling (Tick / Volume / Dollar Bars)
Derive non-time financial bars directly from raw ticks:
```bash
# 500-tick bars
dukascopy-go download --symbol xauusd --bar-type tick --bar-size 500 --last 7d --output ./gold_ticks.csv

# 1,000,000-volume bars
dukascopy-go download --symbol eurusd --bar-type volume --bar-size 1000000 --last 30d --output ./eurusd_vol.parquet
```

### 6. Direct Cloud Storage Upload
Stream directly to your AWS S3, Cloudflare R2, Google Cloud Storage, or MinIO bucket:
```bash
dukascopy-go download --symbol eurusd --timeframe m1 --from 2024-01-01 --to 2024-04-01 \
  --output s3://my-quant-data-lake/forex/eurusd_m1.parquet --parallelism 8
```

---

## 📋 CLI Commands & Subcommands

| Subcommand | Description |
|---|---|
| `download` | Download historical tick/bar data with optional quant features, partitioning, and formats. |
| `sync` | Incremental delta synchronization to append only newly available data in-place. |
| `live` | Real-time tick/bar streaming over Stdout, WebSocket, Redis PubSub/Streams, or NATS. |
| `watch` | Monitor live prices and indicator thresholds with automated HTTP Webhook alerts. |
| `db-load` | High-throughput streaming ingestion into ClickHouse, TimescaleDB, QuestDB, or InfluxDB. |
| `stats` | Inspect dataset integrity, summary statistics, and gap profiles. |
| `manifest` | Manage chunked cache manifests (`inspect`, `verify`, `repair`, `clean-duplicates`). |
| `instruments` | Search offline fallback list and online Dukascopy instrument registry. |

---

## 🛠️ Download Flags Reference

```bash
dukascopy-go download [options]
```

### Core Parameters
| Flag | Type | Default | Description |
|---|---|---|---|
| `--symbol` | `string` | *(required)* | Instrument symbol (e.g. `eurusd`, `xauusd`, `btcusd`). Comma-separated for batches: `eurusd,gbpusd` |
| `--timeframe` | `string` | `m1` | `tick`, `m1`, `m3`, `m5`, `m15`, `m30`, `h1`, `h4`, `d1`, `w1`, `mn1` |
| `--side` | `string` | `bid` | Price side: `bid` or `ask` |
| `--output` | `string` | *(required)* | Output path (`.csv`, `.csv.gz`, `.parquet`, `.arrow`, `.ipc`, `.jsonl`, or `s3://...`) |
| `--from` | `string` | — | Start timestamp: `YYYY-MM-DD`, `YYYY-MM-DD HH:MM`, or RFC3339 |
| `--to` | `string` | — | End timestamp: `YYYY-MM-DD`, `YYYY-MM-DD HH:MM`, or RFC3339 |
| `--last` | `duration` | — | Relative window: `7d`, `30d`, `6mo`, `1y` (overrides `--from`/`--to`) |
| `--parallelism` | `int` | `1` | Number of concurrent download workers |

### Quant & Feature Engineering
| Flag | Type | Default | Description |
|---|---|---|---|
| `--features` | `string` | — | Comma-separated list of indicators to compute and append on the fly. |
| `--bar-type` | `string` | `time` | Bar sampling type: `time`, `tick`, `volume`, `dollar` |
| `--bar-size` | `float` | `0` | Threshold size for custom bar aggregation (e.g. `500` ticks, `1000000` volume) |
| `--clean-outliers` | `string` | `none` | Outlier filtering: `auto` or `median:window:threshold` (e.g. `median:7:5.0`) |

### Format & Presets
| Flag | Type | Default | Description |
|---|---|---|---|
| `--preset` | `string` | — | Backtest format preset: `vectorbt`, `nautilus`, `freqtrade`, `lean`, `mt4`, `mt5`, `backtrader`, `ninjatrader` |
| `--full` | `bool` | `false` | Full Bid + Ask candlestick fields |
| `--fused` | `bool` | `false` | Combined Bid/Ask with floating spread |
| `--simple` | `bool` | `false` | Clean OHLCV column set |
| `--custom-columns` | `string` | — | Explicit comma-separated column projection (e.g. `timestamp,open,close,volume`) |
| `--timezone` | `string` | `UTC` | Timezone conversion (`UTC`, `EST`, `EET`, `Europe/London`, etc.) |
| `--fill-gaps` | `string` | `none` | Forward-fill active market gaps (`forward`) |
| `--partition` | `string` | `none` | File partitioning: `auto`, `hour`, `day`, `week`, `month`, `year` |
| `--hive` | `bool` | `false` | Hive-style directory partitioning (e.g. `year=YYYY/month=MM/`) |

---

## 🧠 Quant Feature Engineering Engine

Calculate high-performance technical indicators on the fly during data download with zero external Python libraries:

```bash
dukascopy-go download --symbol xauusd --timeframe m1 --last 30d --output ./gold_features.parquet \
  --features "returns:log,returns:pct,sma:20,ema:50,rsi:14,atr:14,vwap,obv,volatility:garman-klass:14,macd:12:26:9,bollinger:20:2.0"
```

### Supported Indicators:
- **Returns**: `returns:log` (Logarithmic Return), `returns:pct` (Percentage Return)
- **Moving Averages**: `sma:<period>`, `ema:<period>` (e.g. `sma:20`, `ema:50`, `ema:200`)
- **Momentum**: `rsi:<period>` (e.g. `rsi:14`), `macd:<fast>:<slow>:<signal>` (e.g. `macd:12:26:9`)
- **Volatility**:
  - `atr:<period>` (Average True Range)
  - `bollinger:<period>:<std_dev>` (Upper, Mid, Lower bands)
  - `volatility:garman-klass:<window>` (OHLC Garman-Klass Volatility)
  - `volatility:parkinson:<window>` (High-Low Parkinson Volatility)
- **Volume & Flow**: `vwap` (Volume Weighted Average Price), `obv` (On-Balance Volume)

---

## 🤖 Production Streaming & Trade Bot Integrations

`dukascopy-go` serves as a high-speed gateway for trading bots, algorithmic execution systems, and data pipelines.

### 1. Redis Pub/Sub & Redis Streams
Stream real-time market data directly into Redis for sub-millisecond bot consumption:
```bash
# Redis Pub/Sub
dukascopy-go live --symbol eurusd --redis redis://localhost:6379/market_eurusd

# Redis Stream (XADD)
dukascopy-go live --symbol xauusd --redis redis://:authpass@localhost:6379/stream:gold_ticks
```

### 2. NATS Messaging
```bash
dukascopy-go live --symbol eurusd --nats nats://localhost:4222/market.eurusd
```

### 3. Built-in WebSocket Server
```bash
dukascopy-go live --symbol eurusd --timeframe tick --port 8080
# Clients connect to ws://localhost:8080/stream
```

### 4. Microstructure & Order Flow Metrics (`--microstructure`)
Include real-time high-frequency trading microstructure metrics in the tick stream:
```bash
dukascopy-go live --symbol eurusd --timeframe tick --microstructure --format jsonl
```
**Streamed Metrics:**
- `order_flow_imbalance` (OFI): $(\text{BidVol} - \text{AskVol}) / (\text{BidVol} + \text{AskVol})$
- `effective_spread`: $2 \times |P_{mid} - P_{prev\_mid}|$
- `amihud_illiquidity`: $|\Delta P| / \text{Volume}$
- `price_velocity`: Price points per second

---

## 🔔 Real-Time Watch & Webhook Alerts

The `watch` command monitors market conditions in real time and POSTs JSON alert payloads to your webhook or trade bot:

```bash
# Price threshold trigger
dukascopy-go watch --symbol xauusd --condition "price > 2500" --webhook http://mybot:8080/signal --cooldown 5m

# Technical indicator trigger
dukascopy-go watch --symbol eurusd --condition "rsi < 30" --timeframe m1 --webhook http://mybot:8080/rsi-alert
```

**Delivered Webhook Payload:**
```json
{
  "timestamp": 1704153600000,
  "symbol": "EURUSD",
  "condition": "rsi < 30",
  "current_price": 1.08450,
  "bid": 1.08450,
  "ask": 1.08451,
  "message": "rsi < 30 (current: 27.84)"
}
```

---

## ☁️ Direct Cloud Storage & Data Lake Ingestion

Export directly to AWS S3, Cloudflare R2, MinIO, or Google Cloud Storage with **AWS SigV4 authentication** and zero third-party AWS SDK dependencies:

```bash
# Upload directly to S3
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_REGION="us-east-1"

dukascopy-go download --symbol eurusd --timeframe m1 --last 30d \
  --output s3://my-quant-bucket/forex/eurusd_m1.parquet --parallelism 8

# MinIO / Cloudflare R2 / Custom S3 Endpoint
export S3_ENDPOINT="https://<account-id>.r2.cloudflarestorage.com"
dukascopy-go download --symbol xauusd --timeframe tick --last 7d \
  --output r2://market-data/xauusd_ticks.parquet
```

---

## 🗄️ Direct Database Ingestion (`db-load`)

Stream massive CSV or Parquet files into high-performance time-series databases with millions of rows per second:

| Database | Protocol / Engine | Command Example |
|---|---|---|
| **ClickHouse** | Native HTTP Stream | `dukascopy-go db-load --input ./data.csv --db clickhouse --url http://localhost:8123 --table eurusd` |
| **TimescaleDB / Postgres** | `COPY FROM` Direct Stream | `dukascopy-go db-load --input ./data.csv --db postgres --url postgres://user:pass@localhost:5432/db --table eurusd` |
| **QuestDB** | Native TCP ILP (Influx Line Protocol) | `dukascopy-go db-load --input ./data.csv --db questdb --url tcp://localhost:9009 --table eurusd` |
| **InfluxDB v2** | Gzip Batch Line Protocol | `dukascopy-go db-load --input ./data.csv --db influxdb --url http://localhost:8086 --table eurusd --token <tok> --org myorg --bucket market` |

---

## 🐹 Go SDK

Use `dukascopy-go` directly within your Go applications:

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

func main() {
	client, err := dukascopy.NewClient("https://jetta.dukascopy.com", 30*time.Second)
	if err != nil {
		panic(err)
	}

	result, err := client.Download(context.Background(), dukascopy.DownloadRequest{
		Symbol:      "EURUSD",
		Granularity: dukascopy.GranularityM1,
		Side:        dukascopy.PriceSideBid,
		From:        time.Now().Add(-24 * time.Hour),
		To:          time.Now(),
	})
	if err != nil {
		panic(err)
	}

	fmt.Printf("Downloaded %d bars for %s\n", len(result.Bars), result.Instrument.Name)
}
```

---

## 🐍 Python SDK

```python
import dukascopy_go as dukascopy
from datetime import datetime

# Download directly to pandas DataFrame
df = dukascopy.to_dataframe(
    symbol="EURUSD",
    timeframe="m1",
    from_date=datetime(2024, 1, 1, 0, 0),
    to_date=datetime(2024, 1, 2, 0, 0)
)
print(df.head())

# Async download to Parquet file
await dukascopy.download_async(
    symbol="XAUUSD",
    timeframe="tick",
    output_path="./gold.parquet",
    from_date=datetime(2024, 1, 1),
    to_date=datetime(2024, 1, 2)
)
```

---

## 🛠️ Development & Testing

```bash
# Clone the repository
git clone https://github.com/Nosvemos/dukascopy-go.git
cd dukascopy-go

# Run full test suite with Race Detector
go test -race ./...

# Build binary
go build -o dukascopy-go ./cmd/dukascopy-go
```

---

## ⚖️ Legal Disclaimer

`dukascopy-go` is not affiliated with, endorsed by, or vetted by Dukascopy Bank SA. It is an independent tool that works with Dukascopy's publicly accessible endpoints and is intended for research, data engineering, backtesting, and algorithmic trading workflows.

---

## 📜 License

This project is licensed under the **Dukascopy-Go Source-Available Non-Commercial License**.

- ✅ **Allowed**: Source code viewing, personal use, private research/testing, modification, and free non-commercial redistribution.
- ❌ **Prohibited**: Commercial use, resale, paid distribution, SaaS/hosting services, sublicensing, and integration into commercial products.

See the full [LICENSE](LICENSE) file for details.
