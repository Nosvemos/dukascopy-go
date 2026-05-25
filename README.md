<div align="center">
  <h1>dukascopy-go 🚀</h1>
  <p><b>The fastest, zero-dependency tool to download historical and real-time Dukascopy market data.</b></p>
  
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
| **Speed** | 🚀 Native Go (Dual-engine: JSON + LZMA, ~100x Faster) | 🐢 Slower V8/Node.js execution |
| **Parities (Assets)**| 🌍 **170+ Offline Fallback database** + online merging | ⚠️ Requires full online resolution |
| **Dependencies** | ✨ Zero (Standalone single binary) | 📦 Requires Node.js, NPM, and modules |
| **Resumability** | ✅ Automatic manifest checkpoints & auto-resume | ❌ Often restarts from scratch |
| **Date Clamping** | ✅ Smart future dates auto-clamping & history skips | ❌ Throws fatal errors and discards data |
| **Persistent CLI**| ✅ Updates inline; keeps loading stats in scrollback | ❌ TUI alt-screen clears logs on exit |
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

### 🔍 Find Instruments (170+ Supported!)
Search for a specific instrument, or list them all. Works completely offline:
```bash
# Search for Turkish Lira crosses
dukascopy-go instruments --query try

# Search for Tesla stock CFD
dukascopy-go instruments --query tsla

# Lists all 170+ available instruments (Forex, Metals, Cryptos, Indices, Commodities, Stock CFDs)
dukascopy-go instruments
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

---

## 🛡️ Robust Architecture

`dukascopy-go` is built with enterprise-grade data engineering in mind:

- **Smart Date Clamping**: If you request future dates (e.g. `--to 2028-01-01`), the CLI automatically clamps it to the present moment to prevent redundant network queries.
- **Graceful History Skips**: If you request dates before available history, the loop gracefully skips empty chunk responses (`isNoDataError`) and writes all successfully downloaded data, rather than crashing and discarding progress.
- **Context-aware Resumability**: Using the `--partition` flag, large downloads are split into small chunks managed by a `.manifest.json`. If your internet drops, you can instantly resume exactly where it failed. You can also run `dukascopy-go manifest repair` to fix corrupted chunks.
- **Durable TUI Scrollback**: Bubble Tea alternate screen clearing is disabled for downloads. When a download completes, the beautiful progress dashboard and download speed stats remain written inline in your terminal scrollback.
- **In-place Deduplication**: Market data can be messy. The pruner automatically detects and eliminates duplicate records or out-of-order ticks, guaranteeing chronological integrity.
- **Proxy Rotation**: Bypassing strict IP rate-limits is easy. Provide a `--proxy-file` containing HTTP/SOCKS5 proxies, and the engine will round-robin through them.

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

## ⚖️ Legal Disclaimer

`dukascopy-go` is not affiliated with, endorsed by, or vetted by Dukascopy Bank SA. It is an independent open-source tool that works with Dukascopy's publicly accessible endpoints and is intended for research, automation, and data engineering workflows.
