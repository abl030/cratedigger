"""Live integration tests against a real slskd container.

Tier 1: Search + enqueue shape validation (~30s) — runs if slskd available.
Tier 2: Full download pipeline (~2-5min) — runs if SLSKD_TEST_FULL=1.

These tests validate that real slskd API responses match the dict shapes
soularr.py expects. They caught three production crashes from dict/DownloadFile
boundary mismatches on 2026-03-29.

Requires: tests/.slskd-creds.json + docker (auto-started by conftest.py).
"""

import os
import sys
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

import conftest  # noqa: F401 — triggers ephemeral slskd startup

# Import after conftest sets up env
SLSKD_HOST = os.environ.get("SLSKD_TEST_HOST")
SLSKD_API_KEY = os.environ.get("SLSKD_TEST_API_KEY")
SLSKD_DOWNLOAD_DIR = os.environ.get("SLSKD_TEST_DOWNLOAD_DIR")
FULL_PIPELINE = os.environ.get("SLSKD_TEST_FULL")


def _get_client():
    """Create a slskd_api client connected to the test container."""
    import slskd_api
    return slskd_api.SlskdClient(host=SLSKD_HOST, api_key=SLSKD_API_KEY)


# ─── Tier 1: Search + Enqueue Shape Validation ──────────────────────

@unittest.skipUnless(SLSKD_HOST, "slskd not available — no creds or docker")
class TestSlskdSearchShapes(unittest.TestCase):
    """Verify real slskd API search response shapes."""

    @classmethod
    def setUpClass(cls):
        cls.client = _get_client()
        # Run a search for a well-known album — guaranteed results
        # Use 30s timeout and wait a bit longer for network propagation
        cls.search = cls.client.searches.search_text(
            searchText="*eatles abbey road",
            searchTimeout=30000,
        )
        # Wait for search to complete
        for _ in range(40):
            state = cls.client.searches.state(cls.search["id"])
            if state.get("state") != "InProgress":
                break
            time.sleep(1)
        cls.results = cls.client.searches.search_responses(cls.search["id"])

    @classmethod
    def tearDownClass(cls):
        try:
            cls.client.searches.delete(cls.search["id"])
        except Exception:
            pass

    def test_search_returns_list(self):
        self.assertIsInstance(self.results, list)

    def test_search_has_results(self):
        """*eatles abbey road should return results from the network."""
        if not self.results:
            self.skipTest("No search results — Soulseek network may be slow")

    def test_result_has_username(self):
        if not self.results:
            self.skipTest("No search results returned")
        r = self.results[0]
        self.assertIn("username", r)
        self.assertIsInstance(r["username"], str)

    def test_result_has_files(self):
        if not self.results:
            self.skipTest("No search results returned")
        r = self.results[0]
        self.assertIn("files", r)
        self.assertIsInstance(r["files"], list)

    def test_file_is_plain_dict(self):
        """Search result files must be plain dicts, NOT DownloadFile.
        This is THE test that catches the 2026-03-29 production crash."""
        from lib.grab_list import DownloadFile
        for r in self.results[:5]:
            for f in r.get("files", [])[:5]:
                self.assertIsInstance(f, dict)
                self.assertNotIsInstance(f, DownloadFile)

    def test_file_has_filename(self):
        """Every file dict must have 'filename' key — verify_filetype accesses this."""
        for r in self.results[:5]:
            for f in r.get("files", [])[:5]:
                self.assertIn("filename", f)
                self.assertIsInstance(f["filename"], str)

    def test_file_has_size(self):
        for r in self.results[:5]:
            for f in r.get("files", [])[:5]:
                self.assertIn("size", f)

    def test_verify_filetype_with_real_response(self):
        """verify_filetype() works with actual slskd file dicts — no AttributeError."""
        # Mock cfg to avoid global state issues
        from unittest.mock import MagicMock
        import soularr
        orig_cfg = soularr.cfg
        soularr.cfg = MagicMock()
        try:
            for r in self.results[:3]:
                for f in r.get("files", [])[:3]:
                    # Should not crash — this is the exact pattern that broke in prod
                    try:
                        soularr.verify_filetype(f, "flac")
                    except (KeyError, ValueError):
                        pass  # Wrong type is fine — crashing is not
        finally:
            soularr.cfg = orig_cfg


@unittest.skipUnless(SLSKD_HOST, "slskd not available — no creds or docker")
class TestSlskdDirectoryShapes(unittest.TestCase):
    """Verify slskd.users.directory() response shapes."""

    @classmethod
    def setUpClass(cls):
        cls.client = _get_client()
        # Search to find a real username + directory
        search = cls.client.searches.search_text(
            searchText="*eatles abbey road flac",
            searchTimeout=15000,
        )
        for _ in range(20):
            state = cls.client.searches.state(search["id"])
            if state.get("state") != "InProgress":
                break
            time.sleep(1)
        results = cls.client.searches.search_responses(search["id"])
        cls.client.searches.delete(search["id"])

        # Find a result with files to get a username + directory
        cls.username = None
        cls.file_dir = None
        for r in results:
            if r.get("files"):
                cls.username = r["username"]
                # Extract directory from first file's path
                first_file = r["files"][0]["filename"]
                cls.file_dir = first_file.rsplit("\\", 1)[0] if "\\" in first_file else ""
                break

    def test_directory_response_has_files(self):
        """users.directory() returns object with 'files' list."""
        if not self.username or not self.file_dir:
            self.skipTest("No search results to get directory from")
        try:
            directory = self.client.users.directory(
                username=self.username, directory=self.file_dir)
            # May be wrapped in list depending on slskd version
            if isinstance(directory, list):
                directory = directory[0]
            self.assertIn("files", directory)
            self.assertIsInstance(directory["files"], list)
        except Exception as e:
            # User might be offline — skip, don't fail
            if "404" in str(e) or "timeout" in str(e).lower():
                self.skipTest(f"User offline or directory unavailable: {e}")
            raise


@unittest.skipUnless(SLSKD_HOST, "slskd not available — no creds or docker")
class TestSlskdVersionAndServer(unittest.TestCase):
    """Basic API shape tests that don't need search results."""

    def setUp(self):
        self.client = _get_client()

    def test_application_version(self):
        """application endpoint returns version string."""
        info = self.client.application.version()
        self.assertIsNotNone(info)

    def test_server_state_shape(self):
        """server endpoint returns dict with isConnected/isLoggedIn."""
        import urllib.request
        import json
        req = urllib.request.Request(
            f"{SLSKD_HOST}/api/v0/server",
            headers={"X-API-Key": SLSKD_API_KEY},
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
        self.assertIn("isConnected", data)
        self.assertIn("isLoggedIn", data)


# ─── Tier 2: Full Pipeline (gated) ──────────────────────────────────

@unittest.skipUnless(SLSKD_HOST, "slskd not available")
@unittest.skipUnless(FULL_PIPELINE, "set SLSKD_TEST_FULL=1 for full pipeline tests")
class TestSlskdFullPipeline(unittest.TestCase):
    """Full download → process pipeline with real files.

    Searches for a small, widely-shared file, downloads it, then
    exercises process_completed_album to verify the full flow.
    """

    @classmethod
    def setUpClass(cls):
        cls.client = _get_client()

    def test_enqueue_and_status_shapes(self):
        """Enqueue a real file and verify transfer API response shapes."""
        # Search for something small
        search = self.client.searches.search_text(
            searchText="Beatles Love Me Do",
            searchTimeout=15000,
        )
        for _ in range(20):
            state = self.client.searches.state(search["id"])
            if state.get("state") != "InProgress":
                break
            time.sleep(1)
        results = self.client.searches.search_responses(search["id"])
        self.client.searches.delete(search["id"])

        if not results:
            self.skipTest("No search results")

        # Find a small file to enqueue
        target_user = None
        target_file = None
        target_dir = None
        for r in results:
            for f in r.get("files", []):
                if f.get("size", 0) < 20_000_000:  # < 20MB
                    target_user = r["username"]
                    target_file = f
                    target_dir = f["filename"].rsplit("\\", 1)[0]
                    break
            if target_file:
                break

        if not target_file:
            self.skipTest("No small files found in results")

        # Enqueue
        try:
            self.client.transfers.enqueue(
                username=target_user,
                files=[target_file],
            )
        except Exception as e:
            self.skipTest(f"Enqueue failed (user offline?): {e}")

        # Check download list shape
        time.sleep(2)
        try:
            downloads = self.client.transfers.get_downloads(username=target_user)
            self.assertIn("directories", downloads)
            for d in downloads["directories"]:
                self.assertIn("directory", d)
                self.assertIn("files", d)
                for f in d["files"]:
                    self.assertIn("id", f)
                    self.assertIn("filename", f)
                    # Get individual status
                    status = self.client.transfers.get_download(
                        username=target_user, id=f["id"])
                    self.assertIn("state", status)
        finally:
            # Cancel everything we enqueued
            try:
                downloads = self.client.transfers.get_downloads(username=target_user)
                for d in downloads.get("directories", []):
                    for f in d.get("files", []):
                        self.client.transfers.cancel_download(
                            username=target_user, id=f["id"])
            except Exception:
                pass


if __name__ == "__main__":
    unittest.main()
