import unittest
from datetime import datetime, timedelta
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import dukascopy_go as dukascopy

class TestDukascopySDK(unittest.TestCase):
    def test_imports(self):
        self.assertIsNotNone(dukascopy.download)
        self.assertIsNotNone(dukascopy.to_dataframe)

    def test_download_simple_bar(self):
        temp_file = "temp_eurusd.csv"
        if os.path.exists(temp_file):
            os.remove(temp_file)
            
        try:
            # use a slightly older date to ensure it is settled
            to_date = datetime(2026, 5, 10, 12, 0)
            from_date = datetime(2026, 5, 10, 11, 55)
            
            dukascopy.download(
                symbol="EURUSD",
                timeframe="m1",
                output_path=temp_file,
                from_date=from_date,
                to_date=to_date,
                profile="simple",
                fill_gaps="none"
            )
            
            self.assertTrue(os.path.exists(temp_file))
            self.assertGreater(os.path.getsize(temp_file), 0)
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)

    def test_to_dataframe(self):
        try:
            import pandas as pd
        except ImportError:
            self.skipTest("pandas not installed")
            
        to_date = datetime(2026, 5, 10, 12, 0)
        from_date = datetime(2026, 5, 10, 11, 55)
        
        df = dukascopy.to_dataframe(
            symbol="EURUSD",
            timeframe="m1",
            from_date=from_date,
            to_date=to_date,
            profile="simple"
        )
        
        self.assertIsNotNone(df)
        self.assertFalse(df.empty)
        self.assertIn("timestamp", df.columns)
        self.assertIn("open", df.columns)

if __name__ == '__main__':
    unittest.main()
