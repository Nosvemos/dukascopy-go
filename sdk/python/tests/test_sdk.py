import os
import sys
import tempfile
import unittest
from datetime import datetime
from unittest import mock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import dukascopy_go as dukascopy

# Network/integration tests download live data from Dukascopy and need the
# native library. They are skipped unless explicitly enabled:
#   DUKASCOPY_NETWORK_TESTS=1 python -m unittest discover -s tests
NETWORK_TESTS = os.environ.get("DUKASCOPY_NETWORK_TESTS", "").strip().lower() in ("1", "true", "yes")
network_test = unittest.skipUnless(
    NETWORK_TESTS, "network/integration test; set DUKASCOPY_NETWORK_TESTS=1 to run"
)

# Wednesday 10 January 2024 (weekday, FX market open, long settled).
FROM_DATE = datetime(2024, 1, 10, 11, 55)
TO_DATE = datetime(2024, 1, 10, 12, 0)


class TestImports(unittest.TestCase):
    def test_imports(self):
        self.assertIsNotNone(dukascopy.download)
        self.assertIsNotNone(dukascopy.to_dataframe)


class TestLibraryLoading(unittest.TestCase):
    """Offline tests for the shared-library loader (no native code is loaded)."""

    def test_missing_env_path_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            missing = os.path.join(tmp, "nope", "libdukascopy.so")
            with mock.patch.dict(os.environ, {"DUKASCOPY_LIB": missing}):
                with self.assertRaises(FileNotFoundError) as ctx:
                    dukascopy._load_library()
            self.assertIn("DUKASCOPY_LIB", str(ctx.exception))

    def test_env_path_is_used(self):
        with tempfile.TemporaryDirectory() as tmp:
            lib = os.path.join(tmp, "custom-lib.bin")
            open(lib, "wb").close()
            with mock.patch.dict(os.environ, {"DUKASCOPY_LIB": lib}), \
                    mock.patch.object(dukascopy.ctypes, "CDLL") as cdll:
                dukascopy._load_library()
            cdll.assert_called_once_with(os.path.abspath(lib))

    def test_current_directory_is_never_searched(self):
        env = {k: v for k, v in os.environ.items() if k != "DUKASCOPY_LIB"}
        old_cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as tmp:
            # Plant decoy libraries in the cwd and its parent.
            for name in ("libdukascopy.dll", "libdukascopy.so", "libdukascopy.dylib"):
                open(os.path.join(tmp, name), "wb").close()
            sub = os.path.join(tmp, "sub")
            os.mkdir(sub)
            for name in ("libdukascopy.dll", "libdukascopy.so", "libdukascopy.dylib"):
                open(os.path.join(sub, name), "wb").close()
            os.chdir(sub)
            try:
                with mock.patch.dict(os.environ, env, clear=True), \
                        mock.patch.object(dukascopy.ctypes, "CDLL") as cdll:
                    try:
                        dukascopy._load_library()
                    except FileNotFoundError as e:
                        self.assertIn("go build -buildmode=c-shared", str(e))
                for call in cdll.call_args_list:
                    loaded = os.path.abspath(call.args[0])
                    self.assertFalse(
                        loaded.startswith(os.path.abspath(tmp)),
                        f"library loaded from untrusted directory: {loaded}",
                    )
            finally:
                os.chdir(old_cwd)

    def test_c_string_is_freed_even_if_decoding_fails(self):
        freed = []
        fake_lib = mock.Mock()
        fake_lib.FreeString.side_effect = freed.append
        with mock.patch.object(dukascopy, "_lib", fake_lib), \
                mock.patch.object(dukascopy.ctypes, "string_at", side_effect=RuntimeError("boom")):
            with self.assertRaises(RuntimeError):
                dukascopy._take_c_string(1234)
        self.assertEqual(freed, [1234])


@network_test
class TestNetworkDownloads(unittest.TestCase):
    def test_download_simple_bar(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "eurusd.csv")
            dukascopy.download(
                symbol="EURUSD",
                timeframe="m1",
                output_path=out,
                from_date=FROM_DATE,
                to_date=TO_DATE,
                profile="simple",
                fill_gaps="none",
            )
            self.assertTrue(os.path.exists(out))
            self.assertGreater(os.path.getsize(out), 0)

    def test_to_dataframe(self):
        try:
            import pandas  # noqa: F401
        except ImportError:
            self.skipTest("pandas not installed")

        df = dukascopy.to_dataframe(
            symbol="EURUSD",
            timeframe="m1",
            from_date=FROM_DATE,
            to_date=TO_DATE,
            profile="simple",
        )
        self.assertIsNotNone(df)
        self.assertFalse(df.empty)
        self.assertIn("timestamp", df.columns)
        self.assertIn("open", df.columns)


if __name__ == '__main__':
    unittest.main()
