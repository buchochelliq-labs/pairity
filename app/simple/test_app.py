"""Exercise the actual entry point with only Python's standard library."""

import json
import os
import subprocess
import sys
import unittest
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlopen


class AppSmokeTest(unittest.TestCase):
    def test_http_startup(self):
        source = Path(__file__).parent / "src"
        process = subprocess.Popen(
            [sys.executable, "-I", "-u", "app.py"],
            cwd=source,
            env={**os.environ, "DATABRICKS_APP_PORT": "0"},
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            # Bound startup wait even if the app crashes or stops producing output.
            import selectors

            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                self.assertTrue(selector.select(timeout=10), "App did not start within 10 seconds")
            line = process.stdout.readline()
            self.assertTrue(line.startswith("Pairity simple app listening on port "), line)
            base = "http://127.0.0.1:" + line.split()[-1]
            with urlopen(base + "/", timeout=5) as response:
                self.assertEqual(response.status, 200)
                self.assertIn(b"Pairity is running", response.read())
            with urlopen(base + "/health", timeout=5) as response:
                self.assertEqual(json.load(response), {"status": "ok"})
            with self.assertRaises(HTTPError) as error:
                urlopen(base + "/missing", timeout=5)
            self.assertEqual(error.exception.code, 404)
        finally:
            process.terminate()
            try:
                process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate()


if __name__ == "__main__":
    unittest.main()
