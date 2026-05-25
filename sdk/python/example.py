import os
import sys
from datetime import datetime

# Add the current directory to sys.path so we can import dukascopy directly
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import dukascopy

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

    # 4. Integration Guidelines Demo
    print("\n" + "=" * 60)
    print("Quantitative Integration Blueprint")
    print("=" * 60)
    print("""
To stream or insert these files directly into ClickHouse or InfluxDB:

1. CLICKHOUSE DIRECT INGESTION (High Performance):
--------------------------------------------------
import clickhouse_connect

client = clickhouse_connect.get_client(host='localhost', username='default')

# Create schema
client.command('''
    CREATE TABLE IF NOT EXISTS eurusd_m1 (
        timestamp DateTime,
        open Float64,
        high Float64,
        low Float64,
        close Float64,
        volume Float64
    ) ENGINE = MergeTree()
    ORDER BY timestamp
''')

# Direct high-speed file insertion
client.command(
    f"INSERT INTO eurusd_m1 FORMAT CSV",
    file_name=csv_output
)
print("SUCCESS: ClickHouse Ingestion Blueprint complete.")

2. INFLUXDB DIRECT INGESTION:
-----------------------------
from influxdb_client import InfluxDBClient, Point, WriteOptions

# For InfluxDB, parse CSV/Parquet using pandas, convert to line protocol,
# and write with high-performance batching:
# ...
""")
    print("Demo complete!")

if __name__ == "__main__":
    main()
