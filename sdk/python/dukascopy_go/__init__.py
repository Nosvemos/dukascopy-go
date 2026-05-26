import ctypes
import os
import sys
from datetime import datetime

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

# Initialize the library
_lib = None

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
        ctypes.c_int     # priceScale
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
    price_scale: int = 5
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
        
    Raises:
        DukascopyError: If the download fails or parameter validation fails.
    """
    global _lib
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

    # Call CGO function
    err_ptr = _lib.DownloadData(
        c_symbol,
        c_timeframe,
        c_side,
        c_from_date,
        c_to_date,
        c_output_path,
        c_engine,
        c_price_scale
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
    Supported databases: ClickHouse, PostgreSQL, InfluxDB.
    
    Parameters:
        db_type (str): Target database, 'clickhouse', 'postgres', or 'influxdb'.
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
