"""Tests for lib/cache.py — cache persistence across runs."""

import json
import os
import tempfile
import time
import unittest

from unittest.mock import MagicMock

from lib.context import SoularrContext


def _make_ctx() -> SoularrContext:
    """Build a minimal SoularrContext for cache tests."""
    cfg = MagicMock()
    slskd = MagicMock()
    pipeline_db_source = MagicMock()
    return SoularrContext(cfg=cfg, slskd=slskd, pipeline_db_source=pipeline_db_source)


class TestCachePersistence(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_save_load_roundtrip(self):
        """Caches saved by one ctx can be loaded into another."""
        from lib.cache import save_caches, load_caches
        ctx1 = _make_ctx()
        ctx1.folder_cache["user1"] = {"dir1": {"files": [{"filename": "a.mp3"}]}}
        ctx1.user_upload_speed["user1"] = 500000
        ctx1.search_dir_audio_count["user1"] = {"dir1": 12}

        save_caches(ctx1, self.tmpdir)

        ctx2 = _make_ctx()
        load_caches(ctx2, self.tmpdir)
        self.assertEqual(ctx2.folder_cache["user1"]["dir1"]["files"][0]["filename"], "a.mp3")
        self.assertEqual(ctx2.user_upload_speed["user1"], 500000)
        self.assertEqual(ctx2.search_dir_audio_count["user1"]["dir1"], 12)

    def test_ttl_eviction_folder_cache(self):
        """folder_cache is not loaded if cache file is too old."""
        from lib.cache import save_caches, load_caches, FOLDER_CACHE_TTL_SECONDS
        ctx1 = _make_ctx()
        ctx1.folder_cache["user1"] = {"dir1": {"files": []}}
        ctx1.user_upload_speed["user1"] = 100000
        ctx1.search_dir_audio_count["user1"] = {"dir1": 5}

        save_caches(ctx1, self.tmpdir)

        # Backdate the saved_at timestamp
        from lib.cache import cache_path
        path = cache_path(self.tmpdir)
        with open(path) as f:
            data = json.load(f)
        data["saved_at"] = "2020-01-01T00:00:00+00:00"  # very old
        with open(path, "w") as f:
            json.dump(data, f)

        ctx2 = _make_ctx()
        load_caches(ctx2, self.tmpdir)
        # folder_cache should NOT be loaded (too old)
        self.assertEqual(ctx2.folder_cache, {})
        # speed + count should still load (no TTL)
        self.assertEqual(ctx2.user_upload_speed["user1"], 100000)
        self.assertEqual(ctx2.search_dir_audio_count["user1"]["dir1"], 5)

    def test_corrupt_file_no_crash(self):
        """Corrupt cache file doesn't crash — starts with empty caches."""
        from lib.cache import load_caches, cache_path
        path = cache_path(self.tmpdir)
        with open(path, "w") as f:
            f.write("NOT VALID JSON {{{")

        ctx = _make_ctx()
        load_caches(ctx, self.tmpdir)  # should not raise
        self.assertEqual(ctx.folder_cache, {})
        self.assertEqual(ctx.user_upload_speed, {})

    def test_missing_file_no_crash(self):
        """No cache file doesn't crash — starts with empty caches."""
        from lib.cache import load_caches
        ctx = _make_ctx()
        load_caches(ctx, self.tmpdir)  # should not raise
        self.assertEqual(ctx.folder_cache, {})

    def test_atomic_write(self):
        """Cache file is written atomically (no partial writes)."""
        from lib.cache import save_caches, cache_path
        ctx = _make_ctx()
        ctx.user_upload_speed["user1"] = 42

        save_caches(ctx, self.tmpdir)

        path = cache_path(self.tmpdir)
        self.assertTrue(os.path.exists(path))
        # No .tmp file left behind
        self.assertFalse(os.path.exists(path + ".tmp"))

    def test_empty_caches_roundtrip(self):
        """Saving and loading empty caches works."""
        from lib.cache import save_caches, load_caches
        ctx1 = _make_ctx()
        save_caches(ctx1, self.tmpdir)

        ctx2 = _make_ctx()
        load_caches(ctx2, self.tmpdir)
        self.assertEqual(ctx2.folder_cache, {})
        self.assertEqual(ctx2.user_upload_speed, {})
        self.assertEqual(ctx2.search_dir_audio_count, {})


if __name__ == "__main__":
    unittest.main()
