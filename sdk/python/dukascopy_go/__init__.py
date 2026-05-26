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
try:
    _lib = _load_library()
    
    # Configure DownloadData
    _lib.DownloadData.argtypes = [
        ctypes.c_char_p, # symbol
        ctypes.c_char_p, # timeframe
        ctypes.c_char_p, # side
        ctypes.c_char_p, # fromDate
        ctypes.c_char_p, # toDate
        ctypes.c_char_p, # outputPath
        ctypes.c_char_p, # engine
        ctypes.c_int     # priceScale
    ]
    _lib.DownloadData.restype = ctypes.c_void_p

    # Configure FreeString
    _lib.FreeString.argtypes = [ctypes.c_void_p]
    _lib.FreeString.restype = None
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
        _lib.DownloadData.argtypes = [
            ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p,
            ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int
        ]
        _lib.DownloadData.restype = ctypes.c_void_p
        _lib.FreeString.argtypes = [ctypes.c_void_p]
        _lib.FreeString.restype = None

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
