# dukascopy-go Python SDK

Python bindings for [dukascopy-go](https://github.com/Nosvemos/dukascopy-go), a
Dukascopy historical market data downloader. The heavy lifting is done by a
native Go shared library (`libdukascopy.so` / `.dll` / `.dylib`) loaded with
`ctypes`, so wheels are platform-specific.

No wheel is published on PyPI at the moment; build from source.

## Build from source

Requirements: Go (see `go.mod` for the version), a C toolchain for cgo
(gcc/clang; on Windows e.g. MSYS2 UCRT64 gcc), Python 3.9+.

From the repository root:

```bash
# 1. Build the native library into the package directory
go build -buildmode=c-shared -o sdk/python/dukascopy_go/libdukascopy.so ./cmd/dukascopy-go-sdk
#    Windows: .../libdukascopy.dll   macOS: .../libdukascopy.dylib
#    (or simply: make sdk)

# 2. Install the package (optionally with pandas/pyarrow support)
pip install ./sdk/python
pip install './sdk/python[pandas]'
```

## Locating the native library

The library is loaded only from:

1. the path in the `DUKASCOPY_LIB` environment variable, if set, or
2. the `dukascopy_go` package directory.

The current working directory is never searched, so running a script from an
untrusted directory cannot load a planted library.

## Usage

```python
from datetime import datetime
import dukascopy_go as dukascopy

dukascopy.download(
    symbol="EURUSD",
    timeframe="m1",
    output_path="./eurusd.csv",
    from_date=datetime(2024, 1, 10, 10, 0),
    to_date=datetime(2024, 1, 10, 11, 0),
)

df = dukascopy.to_dataframe(  # requires the [pandas] extra
    symbol="EURUSD",
    timeframe="m1",
    from_date=datetime(2024, 1, 10, 10, 0),
    to_date=datetime(2024, 1, 10, 11, 0),
)
```

Async variants (`download_async`, `db_load_async`, `to_dataframe_async`) are
also available.

## Tests

```bash
cd sdk/python
python -m unittest discover -s tests                            # offline tests
DUKASCOPY_NETWORK_TESTS=1 python -m unittest discover -s tests  # + live downloads
```

## License

Proprietary source-available, non-commercial; see the repository's LICENSE.
