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

Compared to Node.js or Python alternatives (like `dukascopy-node`), `dukascopy-go` is built for **speed, scale, and extreme reliability**.

| Feature | `dukascopy-go` | Node.js Alternatives |
|---|---|---|
| **Speed** | 🚀 Native Go + 64KB Sequential Buffering (~100x Faster) | 🐢 Slower V8/Node.js execution |
| **Parities (Assets)**| 🌍 **170+ Offline Fallback database** + Online Catalog Hot-Reloading | ⚠️ Requires full online resolution |
| **Multi-Symbol** | ✅ Comma-separated list with automatic path formatting | ❌ Single symbol only |
| **Dependencies** | ✨ Zero (Standalone single binary) | 📦 Requires Node.js, NPM, and modules |
| **Delta Sync** | ✅ In-place sync to append missing slices | ❌ Restarts from scratch |
| **Gap Filling** | ✅ Quant-grade forward-filling during market open hours | ❌ Messer gap data left as-is |
| **Throttling** | ✅ AIMD Token-Bucket Adaptive Throttling (handles 429) | ❌ Standard fixed rate limits |
| **Resumability** | ✅ Automatic manifest checkpoints & auto-resume | ❌ Often restarts from scratch |
| **Real-time Stream**| ✅ Native WebSocket & Stdout (JSONL/CSV) | ⚠️ Node.js API only |
| **SDK Support** | ✅ Public importable `pkg/`, Go library, Python wrapper | ⚠️ Node.js only |
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

### 🔍 Find Instruments (170+ Supported!)
Search for a specific instrument, or list them all. Supports live catalog hot-reloading:
```bash
# Search for Turkish Lira crosses
dukascopy-go instruments --query try

# Lists all available instruments (showing up to 20 by default)
dukascopy-go instruments

# Force hot-reload instruments cache online and show ALL 170+ parities
dukascopy-go instruments --limit 0 --update
```

### 📉 Download Historical Data
Download 1-minute gold bars to CSV using a **flexible duration** (`--last 30d`) or exact dates:
```bash
# Download the last 30 days of 1-minute data
dukascopy-go download --symbol xauusd --timeframe m1 --last 30d --output ./data/xauusd-m1.csv

# Multi-symbol batch downloading to directory
dukascopy-go download --symbol EUR/USD,GBP/USD,BTC/USD --timeframe d1 --last 1y --output ./data/
```

**Need a massive dataset?** Use parallel downloading and Parquet output:
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

---

### 📋 CLI Command Reference

Below is a detailed guide to all options supported by `dukascopy-go download`:

| Flag | Type | Default | Description |
|---|---|---|---|
| `--symbol` | `string` | *(required)* | Instrument code (e.g., `eurusd`, `xauusd`). Supports comma-separated batch lists: `eurusd,gbpusd`. |
| `--timeframe` | `string` | `m1` | Granularity layout: `tick`, `m1`, `m3`, `m5`, `m15`, `m30`, `h1`, `h4`, `d1`, `w1`, `mn1`. |
| `--side` | `string` | `bid` | Price side to target: `bid` or `ask`. |
| `--output` | `string` | *(required)* | Output file or directory path. Use `.parquet` for Parquet, `.csv` for CSV, `.csv.gz` for compressed CSV. |
| `--last` | `duration` | `""` | Duration to download relative to now (e.g. `30d`, `6mo`, `1y`). Overrides `--from`/`--to`. |
| `--from` | `string` | `""` | Start timestamp in `YYYY-MM-DD`, `YYYY-MM-DD HH:MM`, or ISO `RFC3339` format. |
| `--to` | `string` | `""` | End timestamp in `YYYY-MM-DD`, `YYYY-MM-DD HH:MM`, or ISO `RFC3339` format. |
| `--simple` | `bool` | `false` | Export basic columns only (reduced CSV footprint). |
| `--full` | `bool` | `false` | Export complete Bid and Ask candlestick fields. |
| `--fused` | `bool` | `false` | Export fused Bid and Ask fields complete with dynamic spread calculations (no mid fields). |
| `--custom-columns` | `string` | `""` | Explicit comma-separated custom column projection list. |
| `--progress` | `bool` | `false` | Force-enable the interactive progress TUI dashboard. |
| `--tui-theme` | `string` | `default` | Dashboard theme: `default`, `catppuccin`, `nord`, `gruvbox`, or `dracula`. |
| `--partition` | `string` | `none` | Partitions range into multiple files: `none`, `auto`, `hour`, `day`, `week`, `month`, `year`. |
| `--parallelism` | `int` | `1` | Number of concurrent downloading partition workers. |
| `--fill-gaps` | `string` | `none` | Gap filling: `none` or `forward` (forward-fills during active market sessions). |
| `--timezone` | `string` | `UTC` | Target output timezone (e.g. `Europe/London`, `EST`, `MT4`, `MT5`). |
| `--preset` | `string` | `""` | Custom format mappings matching backtest engines: `mt4`, `mt5`, `backtrader`, `ninjatrader`. |
| `--engine` | `string` | `jetta` | Underlying data source: `jetta` (JSON) or `datafeed` (binary `.bi5` archives). |

---

## 💎 Elite Quant Enhancements

### 🔄 In-Place Smart Delta Sync (`sync`)
Keep your downloaded historical files completely up to date in-place with zero duplicate or out-of-order records:
```bash
dukascopy-go sync --symbol EUR/USD --output ./data/eur_usd.csv
```
The sync command inspects the target file, extracts the last written timestamp, and automatically queries the missing slices up to the present moment, appending them in-place safely.

### 📊 Quant-Grade Gap Filling (`--fill-gaps`)
Unexpected liquidity gaps can corrupt backtesting results. Enable smart forward-filling to keep timeframe intervals uniform:
```bash
dukascopy-go download --symbol EUR/USD --timeframe m1 --last 7d --output ./data/eur_usd.csv --fill-gaps forward
```
- **Forward-Fill Loop**: Missing bars are forward-filled with the previous Close price and Volume `0`.
- **Weekend Filter**: Weekends and holiday closures are automatically detected and skipped to avoid injecting synthetic bars during expected market closures.

### 🛡️ Token-Bucket Adaptive Throttling
We designed a highly resilient AIMD-based (Additive Increase / Multiplicative Decrease) rate limiting system that self-corrects based on response status codes:
- **Instant Backoff**: If hit with `429 Too Many Requests`, the adaptive rate limit instantly doubles (up to `5s`) to completely clear connection queues.
- **Slow Recovery**: On successful requests, it slowly decreases the delay by `10ms` per response back to your defined base rate limit.

---

## 🐹 Go Library SDK (Public `pkg/`)

Since `dukascopy-go` places its core client and CSV exporter packages under the public `pkg/` folder, you can seamlessly import them as a library in your own Go applications!

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

func main() {
	// Initialize the public SDK client
	client := dukascopy.NewClient("https://jetta.dukascopy.com", 30*time.Second)

	req := dukascopy.DownloadRequest{
		Symbol:      "EUR/USD",
		Granularity: dukascopy.GranularityD1,
		Side:        dukascopy.PriceSideBid,
		From:        time.Now().AddDate(0, 0, -5),
		To:          time.Now(),
	}

	result, err := client.Download(context.Background(), req)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Downloaded %d bars for symbol: %s\n", len(result.Bars), result.Instrument.Name)
	for _, bar := range result.Bars {
		fmt.Printf("Time: %s, Open: %f, Close: %f\n", bar.Time, bar.Open, bar.Close)
	}
}
```

---

## 💻 Python & C SDK 

Not using the CLI? `dukascopy-go` compiles to a C shared library (`.so`, `.dll`, `.dylib`) which can be wrapped in almost any language. We provide a **high-performance Python SDK** out of the box!

### 📥 1. Download Historical Data
```python
import dukascopy_go as dukascopy
from datetime import datetime

# Download EURUSD candles straight to CSV or Parquet
dukascopy.download(
    symbol="EURUSD",
    timeframe="m1",
    output_path="./eurusd_m1.csv",
    from_date=datetime(2026, 5, 18, 10, 0),
    to_date=datetime(2026, 5, 18, 11, 0)
)
```

### ⚡ 2. Stream File Directly into Database
Stream the local file into ClickHouse, PostgreSQL, or InfluxDB at millions of rows/second natively using the Go loader:
```python
dukascopy.db_load(
    db_type="clickhouse",
    db_url="http://localhost:8123",
    table_name="eurusd_m1",
    input_path="./eurusd_m1.csv"
)
```
*(Check the `sdk/python` directory for full usage examples and installation guides)*

---

## 🤝 Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

---

## 🛠️ Development

If you want to build and test the project locally, follow these steps:

1. Clone the repository:
   ```bash
   git clone https://github.com/Nosvemos/dukascopy-go.git
   cd dukascopy-go
   ```
2. Install dependencies:
   ```bash
   go mod download
   ```
3. Run tests:
   ```bash
   go test ./...
   ```
4. Build the binary:
   ```bash
   go build -o dukascopy-go ./cmd/dukascopy-go
   ```

---

## ⚖️ Legal Disclaimer

`dukascopy-go` is not affiliated with, endorsed by, or vetted by Dukascopy Bank SA. It is an independent open-source tool that works with Dukascopy's publicly accessible endpoints and is intended for research, automation, and data engineering workflows.
