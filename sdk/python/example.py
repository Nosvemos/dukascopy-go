import os
import sys
from datetime import datetime

# Add the current directory to sys.path so we can import dukascopy directly
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import dukascopy_go as dukascopy

def main():
    print("=" * 60)
    print("Dukascopy Go Python SDK - Professional Demo")
    print("=" * 60)

    # 1. Define download parameters
    symbol = "EURUSD"
    timeframe = "m1"      # 1-minute bars
    side = "BID"
    # Choose a recent Monday to ensure data exists (Dukascopy returns 404 on weekends)
    # Monday, May 18, 2026.
    from_date = datetime(2026, 5, 18, 10, 0, 0)
    to_date = datetime(2026, 5, 18, 11, 0, 0)
    engine = "jetta"      # Can also be "datafeed"
    price_scale = 5

    # Define output files
    csv_output = os.path.join(os.path.dirname(__file__), "eurusd_m1.csv")
    parquet_output = os.path.join(os.path.dirname(__file__), "eurusd_m1.parquet")

    # Clean up previous runs if any
    for path in [csv_output, parquet_output]:
        if os.path.exists(path):
            os.remove(path)

    # 2. Download as CSV
    print(f"\n[1/3] Downloading {symbol} {timeframe} candles to CSV...")
    print(f"Time Range: {from_date} to {to_date}")
    try:
        dukascopy.download(
            symbol=symbol,
            timeframe=timeframe,
            output_path=csv_output,
            from_date=from_date,
            to_date=to_date,
            side=side,
            engine=engine,
            price_scale=price_scale
        )
        print(f"SUCCESS: CSV successfully exported to: {csv_output}")
        if os.path.exists(csv_output):
            size = os.path.getsize(csv_output)
            print(f"  File size: {size / 1024:.2f} KB")
            # Print first few lines
            with open(csv_output, 'r') as f:
                lines = [f.readline().strip() for _ in range(5)]
            print("  Preview:")
            for line in lines:
                print(f"    {line}")
    except Exception as e:
        print(f"ERROR: CSV Download failed: {e}")
        sys.exit(1)

    # 3. Download as Parquet
    print(f"\n[2/3] Downloading {symbol} {timeframe} candles to PARQUET...")
    try:
        dukascopy.download(
            symbol=symbol,
            timeframe=timeframe,
            output_path=parquet_output,
            from_date=from_date,
            to_date=to_date,
            side=side,
            engine=engine,
            price_scale=price_scale
        )
        print(f"SUCCESS: Parquet successfully exported to: {parquet_output}")
        if os.path.exists(parquet_output):
            size = os.path.getsize(parquet_output)
            print(f"  File size: {size / 1024:.2f} KB")
            
            # If pandas & pyarrow are installed, let's read it
            try:
                import pandas as pd
                df = pd.read_parquet(parquet_output)
                print("\n  Ingested into Pandas DataFrame:")
                print(df.head())
            except ImportError:
                print("  (Install pandas and pyarrow/fastparquet to view dataframe preview)")
    except Exception as e:
        print(f"ERROR: Parquet Download failed: {e}")
        sys.exit(1)

    # 4. Integration Guidelines Demo & CGO DB Loader Verification
    print("\n" + "=" * 60)
    print("High-Performance SDK DB Loader Verification")
    print("=" * 60)
    print("Testing Python SDK 'db_load' with ClickHouse (expecting connection refusal error)...")
    
    try:
        dukascopy.db_load(
            db_type="clickhouse",
            db_url="http://127.0.0.1:9999",  # Non-existent port to trigger connection error
            table_name="eurusd_m1",
            input_path=csv_output,
            timeout_sec=5
        )
        print("SUCCESS: ClickHouse ingestion succeeded!")
    except dukascopy.DukascopyError as e:
        print(f"VERIFIED: SDK successfully captured Go database error:\n  -> {e}")
    except Exception as e:
        print(f"FAILED: Unexpected python error occurred: {e}")

    print("\nQuantitative Integration Blueprint")
    print("-" * 35)
    print("""
The Python SDK exposes 'dukascopy.db_load' which streams local files into database tables
natively using Go's high-performance drivers.

Examples:

# Ingest CSV/Parquet into ClickHouse
dukascopy.db_load(
    db_type="clickhouse",
    db_url="http://localhost:8123",
    table_name="eurusd_m1",
    input_path="./eurusd_m1.csv"
)

# Ingest CSV into PostgreSQL using fast COPY
dukascopy.db_load(
    db_type="postgres",
    db_url="postgres://user:pass@localhost:5432/dbname",
    table_name="eurusd_m1",
    input_path="./eurusd_m1.csv"
)

# Ingest CSV into InfluxDB
dukascopy.db_load(
    db_type="influxdb",
    db_url="http://localhost:8086",
    table_name="eurusd_m1",
    input_path="./eurusd_m1.csv",
    org="myorg",
    bucket="mybucket",
    token="mytoken",
    symbol_tag="EURUSD"
)
""")
    print("Demo complete!")

if __name__ == "__main__":
    main()
