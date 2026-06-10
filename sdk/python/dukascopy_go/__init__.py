import asyncio
import ctypes
import os
import sys
import tempfile
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

# Define custom exception
class DukascopyError(Exception):
    pass

# Helper to find and load the shared library
def _load_library():
    # Determine OS and expected library name
    if sys.platform.startswith('win'):
        lib_name = 'libdukascopy.dll'
    elif sys.platform.startswith('darwin'):
        lib_name = 'libdukascopy.dylib'
    else:
        lib_name = 'libdukascopy.so'

    # Check same directory as this file first
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    lib_path = os.path.join(curr_dir, lib_name)

    if not os.path.exists(lib_path):
        # Also check current working directory
        lib_path = os.path.join(os.getcwd(), lib_name)

    if not os.path.exists(lib_path):
        # Try finding in parent directory (useful during development/testing)
        lib_path = os.path.join(os.path.dirname(curr_dir), lib_name)

    if not os.path.exists(lib_path):
        raise FileNotFoundError(
            f"Could not find '{lib_name}'. Please compile it using: \n"
            f"go build -buildmode=c-shared -o sdk/python/{lib_name} ./cmd/dukascopy-go-sdk"
        )

    # Load the library
    try:
        return ctypes.CDLL(lib_path)
    except Exception as e:
        raise OSError(f"Failed to load shared library '{lib_path}': {e}")

import threading

# Initialize the library
_lib = None
_lib_lock = threading.Lock()

def _bind_library_functions(lib):
    # Configure DownloadData
    lib.DownloadData.argtypes = [
        ctypes.c_char_p, # symbol
        ctypes.c_char_p, # timeframe
        ctypes.c_char_p, # side
        ctypes.c_char_p, # fromDate
        ctypes.c_char_p, # toDate
        ctypes.c_char_p, # outputPath
        ctypes.c_char_p, # engine
        ctypes.c_int,    # priceScale
        ctypes.c_char_p, # profile
        ctypes.c_char_p, # timezone
        ctypes.c_char_p, # customColumns
        ctypes.c_int,    # resume
        ctypes.c_int,    # parallelism
        ctypes.c_char_p, # partition
        ctypes.c_char_p, # fillGaps
        ctypes.c_int     # hive
    ]
    lib.DownloadData.restype = ctypes.c_void_p

    # Configure DBLoadData
    lib.DBLoadData.argtypes = [
        ctypes.c_char_p, # dbType
        ctypes.c_char_p, # dbURL
        ctypes.c_char_p, # tableName
        ctypes.c_char_p, # inputPath
        ctypes.c_char_p, # user
        ctypes.c_char_p, # password
        ctypes.c_char_p, # token
        ctypes.c_char_p, # org
        ctypes.c_char_p, # bucket
        ctypes.c_char_p, # symbolTag
        ctypes.c_int,    # batchSize
        ctypes.c_int     # timeoutSec
    ]
    lib.DBLoadData.restype = ctypes.c_void_p

    # Configure FreeString
    lib.FreeString.argtypes = [ctypes.c_void_p]
    lib.FreeString.restype = None

try:
    _lib = _load_library()
    _bind_library_functions(_lib)
except Exception as e:
    # Library not loaded yet, or load deferred until first call
    pass

def download(
    symbol: str,
    timeframe: str,
    output_path: str,
    from_date,
    to_date,
    side: str = 'BID',
    engine: str = 'jetta',
    price_scale: int = 5,
    profile: str = 'simple',
    timezone: str = 'UTC',
    custom_columns: str = '',
    resume: bool = False,
    parallelism: int = 1,
    partition: str = 'none',
    fill_gaps: str = 'none',
    hive: bool = False
):
    """
    Downloads historical market data from Dukascopy using the high-speed Go downloader engine.

    Parameters:
        symbol (str): Instrument symbol, e.g., 'EURUSD', 'GBPUSD'.
        timeframe (str): Granularity/Timeframe, e.g., 'tick', 'm1', 'h1', 'd1'.
        output_path (str): Output file path. Use '.parquet' extension for Parquet, '.csv' for CSV.
        from_date (datetime or str): Start date/time. E.g., datetime(2023, 1, 1) or '2023-01-01T00:00:00Z'.
        to_date (datetime or str): End date/time. E.g., datetime(2023, 1, 2) or '2023-01-02T00:00:00Z'.
        side (str): Price side, either 'BID' or 'ASK'. Default is 'BID'.
        engine (str): Downloader engine, either 'jetta' or 'datafeed'. Default is 'jetta'.
        price_scale (int): Decimal price scale/pip scale of the instrument. Default is 5.
        profile (str): Column profile, 'simple', 'full', or 'fused'. Default is 'simple'.
        timezone (str): Output timezone location, e.g., 'America/New_York', 'UTC'. Default is 'UTC'.
        custom_columns (str): Comma-separated list of columns. Default is ''.
        resume (bool): In-place delta sync / resumed cache download. Default is False.
        parallelism (int): Number of parallel partition downloader workers. Default is 1.
        partition (str): Partition mode: 'none', 'auto', 'day', 'week', etc. Default is 'none'.
        fill_gaps (str): Gap-filling mode: 'none', 'forward'. Default is 'none'.
        hive (bool): Hive-style directory partitioning (for Parquet/CSV). Default is False.

    Raises:
        DukascopyError: If the download fails or parameter validation fails.
    """
    global _lib
    if _lib is None:
        with _lib_lock:
            if _lib is None:
                _lib = _load_library()
                _bind_library_functions(_lib)

    # Convert datetime to ISO-8601 string
    if isinstance(from_date, datetime):
        from_str = from_date.isoformat()
        if not from_date.tzinfo:
            from_str += 'Z'
    else:
        from_str = str(from_date)

    if isinstance(to_date, datetime):
        to_str = to_date.isoformat()
        if not to_date.tzinfo:
            to_str += 'Z'
    else:
        to_str = str(to_date)

    # Encode arguments to C-compatible byte strings
    c_symbol = symbol.encode('utf-8')
    c_timeframe = timeframe.encode('utf-8')
    c_side = side.upper().encode('utf-8')
    c_from_date = from_str.encode('utf-8')
    c_to_date = to_str.encode('utf-8')
    c_output_path = output_path.encode('utf-8')
    c_engine = engine.lower().encode('utf-8')
    c_price_scale = ctypes.c_int(price_scale)
    c_profile = profile.lower().encode('utf-8')
    c_timezone = timezone.encode('utf-8')
    c_custom_columns = custom_columns.encode('utf-8')
    c_resume = ctypes.c_int(1 if resume else 0)
    c_parallelism = ctypes.c_int(parallelism)
    c_partition = partition.lower().encode('utf-8')
    c_fill_gaps = fill_gaps.lower().encode('utf-8')
    c_hive = ctypes.c_int(1 if hive else 0)

    # Call CGO function
    err_ptr = _lib.DownloadData(
        c_symbol,
        c_timeframe,
        c_side,
        c_from_date,
        c_to_date,
        c_output_path,
        c_engine,
        c_price_scale,
        c_profile,
        c_timezone,
        c_custom_columns,
        c_resume,
        c_parallelism,
        c_partition,
        c_fill_gaps,
        c_hive
    )

    # If the returned pointer is not NULL, an error occurred
    if err_ptr:
        err_msg = ctypes.string_at(err_ptr).decode('utf-8')
        # Free the Go C.CString memory to avoid leak
        _lib.FreeString(err_ptr)
        raise DukascopyError(err_msg)

def db_load(
    db_type: str,
    db_url: str,
    table_name: str,
    input_path: str,
    user: str = "",
    password: str = "",
    token: str = "",
    org: str = "",
    bucket: str = "",
    symbol_tag: str = "",
    batch_size: int = 0,
    timeout_sec: int = 120
):
    """
    Ingests market data from a local CSV or Parquet file directly into the target database.
    Supported databases: ClickHouse, PostgreSQL, InfluxDB, QuestDB.

    Parameters:
        db_type (str): Target database, 'clickhouse', 'postgres', 'influxdb', or 'questdb'.
        db_url (str): Connection URL, e.g. 'http://localhost:8123', 'postgres://user:pass@localhost:5432/dbname'.
        table_name (str): Target table or measurement name.
        input_path (str): Path to local CSV or Parquet file to ingest.
        user (str): Database username (optional).
        password (str): Database password (optional).
        token (str): InfluxDB auth token (optional).
        org (str): InfluxDB organization (required for InfluxDB).
        bucket (str): InfluxDB bucket (required for InfluxDB).
        symbol_tag (str): Symbol tag hint for InfluxDB records (optional).
        batch_size (int): Batch size of rows to ingest. Default 0 uses database defaults.
        timeout_sec (int): Request/query timeout in seconds. Default is 120.

    Raises:
        DukascopyError: If the ingestion fails or parameter validation fails.
    """
    global _lib
    if _lib is None:
        with _lib_lock:
            if _lib is None:
                _lib = _load_library()
                _bind_library_functions(_lib)

    # Encode arguments to C-compatible byte strings
    c_db_type = db_type.encode('utf-8')
    c_db_url = db_url.encode('utf-8')
    c_table_name = table_name.encode('utf-8')
    c_input_path = input_path.encode('utf-8')
    c_user = user.encode('utf-8')
    c_password = password.encode('utf-8')
    c_token = token.encode('utf-8')
    c_org = org.encode('utf-8')
    c_bucket = bucket.encode('utf-8')
    c_symbol_tag = symbol_tag.encode('utf-8')
    c_batch_size = ctypes.c_int(batch_size)
    c_timeout_sec = ctypes.c_int(timeout_sec)

    # Call CGO function
    err_ptr = _lib.DBLoadData(
        c_db_type,
        c_db_url,
        c_table_name,
        c_input_path,
        c_user,
        c_password,
        c_token,
        c_org,
        c_bucket,
        c_symbol_tag,
        c_batch_size,
        c_timeout_sec
    )

    # If the returned pointer is not NULL, an error occurred
    if err_ptr:
        err_msg = ctypes.string_at(err_ptr).decode('utf-8')
        # Free the Go C.CString memory to avoid leak
        _lib.FreeString(err_ptr)
        raise DukascopyError(err_msg)


# ═══════════════════════════════════════════════════════════════════════
# Async wrappers (Python 3.9+ asyncio.to_thread)
# ═══════════════════════════════════════════════════════════════════════

def _ensure_pandas():
    """
    Lazily imports pandas and returns the module.
    Raises a helpful ImportError if pandas is not installed.
    """
    try:
        import pandas as _pd
        return _pd
    except ImportError:
        raise ImportError(
            "pandas is required for to_dataframe() and to_dataframe_async(). "
            "Install it with:\n"
            "    pip install pandas pyarrow\n"
            "or:\n"
            "    pip install 'dukascopy-go[pandas]'"
        )


async def download_async(
    symbol: str,
    timeframe: str,
    output_path: str,
    from_date,
    to_date,
    side: str = 'BID',
    engine: str = 'jetta',
    price_scale: int = 5,
    profile: str = 'simple',
    timezone: str = 'UTC',
    custom_columns: str = '',
    resume: bool = False,
    parallelism: int = 1,
    partition: str = 'none',
    fill_gaps: str = 'none',
    hive: bool = False
) -> None:
    """
    Async wrapper around download().
    Runs the blocking CGO DownloadData call in a thread pool so the event loop
    is not blocked.

    Parameters and behavior are identical to download().

    Raises:
        DukascopyError: If the download fails or parameter validation fails.
    """
    return await asyncio.to_thread(
        download,
        symbol=symbol,
        timeframe=timeframe,
        output_path=output_path,
        from_date=from_date,
        to_date=to_date,
        side=side,
        engine=engine,
        price_scale=price_scale,
        profile=profile,
        timezone=timezone,
        custom_columns=custom_columns,
        resume=resume,
        parallelism=parallelism,
        partition=partition,
        fill_gaps=fill_gaps,
        hive=hive
    )


async def db_load_async(
    db_type: str,
    db_url: str,
    table_name: str,
    input_path: str,
    user: str = "",
    password: str = "",
    token: str = "",
    org: str = "",
    bucket: str = "",
    symbol_tag: str = "",
    batch_size: int = 0,
    timeout_sec: int = 120
) -> None:
    """
    Async wrapper around db_load().
    Runs the blocking CGO DBLoadData call in a thread pool so the event loop
    is not blocked.

    Parameters and behavior are identical to db_load().

    Raises:
        DukascopyError: If the ingestion fails or parameter validation fails.
    """
    return await asyncio.to_thread(
        db_load,
        db_type=db_type,
        db_url=db_url,
        table_name=table_name,
        input_path=input_path,
        user=user,
        password=password,
        token=token,
        org=org,
        bucket=bucket,
        symbol_tag=symbol_tag,
        batch_size=batch_size,
        timeout_sec=timeout_sec,
    )


# ═══════════════════════════════════════════════════════════════════════
# Pandas DataFrame integration (requires pandas + pyarrow)
# ═══════════════════════════════════════════════════════════════════════

def to_dataframe(
    symbol: str,
    timeframe: str,
    from_date,
    to_date,
    side: str = 'BID',
    engine: str = 'jetta',
    price_scale: int = 5,
    output_format: str = 'parquet',
    profile: str = 'simple',
    timezone: str = 'UTC',
    custom_columns: str = '',
    resume: bool = False,
    parallelism: int = 1,
    partition: str = 'none',
    fill_gaps: str = 'none'
):
    """
    Downloads market data and returns it as a pandas.DataFrame directly,
    without requiring the user to manage intermediate files.

    Parameters:
        symbol (str): Instrument symbol, e.g., 'EURUSD', 'GBPUSD'.
        timeframe (str): Granularity/Timeframe, e.g., 'tick', 'm1', 'h1', 'd1'.
        from_date (datetime or str): Start date/time.
        to_date (datetime or str): End date/time.
        side (str): Price side, either 'BID' or 'ASK'. Default is 'BID'.
        engine (str): Downloader engine, either 'jetta' or 'datafeed'. Default is 'jetta'.
        price_scale (int): Decimal price scale/pip scale of the instrument. Default is 5.
        output_format (str): 'parquet' (default, recommended for type preservation)
                             or 'csv'.
        profile (str): Column profile, 'simple', 'full', or 'fused'. Default is 'simple'.
        timezone (str): Output timezone location. Default is 'UTC'.
        custom_columns (str): Comma-separated list of columns. Default is ''.
        resume (bool): In-place delta sync / resumed cache download. Default is False.
        parallelism (int): Number of parallel partition downloader workers. Default is 1.
        partition (str): Partition mode. Default is 'none'.
        fill_gaps (str): Gap-filling mode. Default is 'none'.

    Returns:
        pandas.DataFrame with the downloaded market data.

    Raises:
        ImportError: If pandas is not installed.
        DukascopyError: If the download fails or parameter validation fails.
    """
    pd_mod = _ensure_pandas()

    if output_format in ('arrow', 'ipc', 'feather'):
        suffix = '.arrow'
    elif output_format == 'parquet':
        suffix = '.parquet'
    else:
        suffix = '.csv'

    # Create a named temp file and immediately close the Python file handle
    # so the Go shared library can write to it without Windows file-locking issues.
    tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    tmp_path = tmp.name
    tmp.close()

    try:
        download(
            symbol=symbol,
            timeframe=timeframe,
            output_path=tmp_path,
            from_date=from_date,
            to_date=to_date,
            side=side,
            engine=engine,
            price_scale=price_scale,
            profile=profile,
            timezone=timezone,
            custom_columns=custom_columns,
            resume=resume,
            parallelism=parallelism,
            partition=partition,
            fill_gaps=fill_gaps
        )

        # Read the downloaded data into a DataFrame
        if output_format in ('arrow', 'ipc', 'feather'):
            df = pd_mod.read_feather(tmp_path)
        elif output_format == 'parquet':
            df = pd_mod.read_parquet(tmp_path)
        else:
            df = pd_mod.read_csv(tmp_path)

        return df

    finally:
        # Always clean up the temp file, even if download or pandas fails
        try:
            os.unlink(tmp_path)
        except OSError:
            pass  # Best-effort cleanup


async def to_dataframe_async(
    symbol: str,
    timeframe: str,
    from_date,
    to_date,
    side: str = 'BID',
    engine: str = 'jetta',
    price_scale: int = 5,
    output_format: str = 'parquet',
    profile: str = 'simple',
    timezone: str = 'UTC',
    custom_columns: str = '',
    resume: bool = False,
    parallelism: int = 1,
    partition: str = 'none',
    fill_gaps: str = 'none'
):
    """
    Async wrapper around to_dataframe().
    Runs the blocking download and pandas I/O in a thread pool.

    Parameters and behavior are identical to to_dataframe().

    Returns:
        pandas.DataFrame with the downloaded market data.

    Raises:
        ImportError: If pandas is not installed.
        DukascopyError: If the download fails or parameter validation fails.
    """
    _ensure_pandas()  # Fail-fast on import error before entering the thread pool

    return await asyncio.to_thread(
        to_dataframe,
        symbol=symbol,
        timeframe=timeframe,
        from_date=from_date,
        to_date=to_date,
        side=side,
        engine=engine,
        price_scale=price_scale,
        output_format=output_format,
        profile=profile,
        timezone=timezone,
        custom_columns=custom_columns,
        resume=resume,
        parallelism=parallelism,
        partition=partition,
        fill_gaps=fill_gaps
    )
