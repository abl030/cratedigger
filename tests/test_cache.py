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
        ctx1._folder_cache_ts["user1"] = {"dir1": time.time()}
        ctx1.user_upload_speed["user1"] = 500000
        ctx1._upload_speed_ts["user1"] = time.time()
        ctx1.search_dir_audio_count["user1"] = {"dir1": 12}
        ctx1._dir_audio_count_ts["user1"] = {"dir1": time.time()}

        save_caches(ctx1, self.tmpdir)

        ctx2 = _make_ctx()
        load_caches(ctx2, self.tmpdir)
        self.assertEqual(ctx2.folder_cache["user1"]["dir1"]["files"][0]["filename"], "a.mp3")
        self.assertEqual(ctx2.user_upload_speed["user1"], 500000)
        self.assertEqual(ctx2.search_dir_audio_count["user1"]["dir1"], 12)

    def test_per_entry_ttl_evicts_old_folder_cache(self):
        """Old folder_cache entries are evicted, fresh ones kept."""
        from lib.cache import save_caches, load_caches, FOLDER_CACHE_TTL_SECONDS
        ctx1 = _make_ctx()
        now = time.time()
        ctx1.folder_cache["old_user"] = {"dir1": {"files": []}}
        ctx1._folder_cache_ts["old_user"] = {"dir1": now - FOLDER_CACHE_TTL_SECONDS - 100}
        ctx1.folder_cache["fresh_user"] = {"dir2": {"files": [{"filename": "b.mp3"}]}}
        ctx1._folder_cache_ts["fresh_user"] = {"dir2": now - 60}

        save_caches(ctx1, self.tmpdir)

        ctx2 = _make_ctx()
        load_caches(ctx2, self.tmpdir)
        self.assertNotIn("old_user", ctx2.folder_cache)
        self.assertIn("fresh_user", ctx2.folder_cache)
        self.assertEqual(ctx2.folder_cache["fresh_user"]["dir2"]["files"][0]["filename"], "b.mp3")

    def test_per_entry_ttl_evicts_old_speed(self):
        """Old upload speed entries are evicted."""
        from lib.cache import save_caches, load_caches, FOLDER_CACHE_TTL_SECONDS
        ctx1 = _make_ctx()
        now = time.time()
        ctx1.user_upload_speed["old_user"] = 100000
        ctx1._upload_speed_ts["old_user"] = now - FOLDER_CACHE_TTL_SECONDS - 100
        ctx1.user_upload_speed["fresh_user"] = 500000
        ctx1._upload_speed_ts["fresh_user"] = now - 60

        save_caches(ctx1, self.tmpdir)

        ctx2 = _make_ctx()
        load_caches(ctx2, self.tmpdir)
        self.assertNotIn("old_user", ctx2.user_upload_speed)
        self.assertEqual(ctx2.user_upload_speed["fresh_user"], 500000)

    def test_per_entry_ttl_evicts_old_dir_count(self):
        """Old dir audio count entries are evicted."""
        from lib.cache import save_caches, load_caches, FOLDER_CACHE_TTL_SECONDS
        ctx1 = _make_ctx()
        now = time.time()
        ctx1.search_dir_audio_count["old_user"] = {"dir1": 5}
        ctx1._dir_audio_count_ts["old_user"] = {"dir1": now - FOLDER_CACHE_TTL_SECONDS - 100}
        ctx1.search_dir_audio_count["fresh_user"] = {"dir2": 12}
        ctx1._dir_audio_count_ts["fresh_user"] = {"dir2": now - 60}

        save_caches(ctx1, self.tmpdir)

        ctx2 = _make_ctx()
        load_caches(ctx2, self.tmpdir)
        self.assertNotIn("old_user", ctx2.search_dir_audio_count)
        self.assertEqual(ctx2.search_dir_audio_count["fresh_user"]["dir2"], 12)

    def test_legacy_format_loads_with_fresh_timestamp(self):
        """Cache files without per-entry timestamps load all entries."""
        from lib.cache import load_caches, cache_path
        # Write old-format cache directly
        data = {
            "saved_at": "2026-04-05T00:00:00+00:00",
            "folder_cache": {"user1": {"dir1": {"files": []}}},
            "user_upload_speed": {"user1": 42},
            "search_dir_audio_count": {"user1": {"dir1": 5}},
        }
        with open(cache_path(self.tmpdir), "w") as f:
            json.dump(data, f)

        ctx = _make_ctx()
        load_caches(ctx, self.tmpdir)
        # Legacy entries should load (treated as fresh)
        self.assertIn("user1", ctx.folder_cache)
        self.assertEqual(ctx.user_upload_speed["user1"], 42)
        self.assertEqual(ctx.search_dir_audio_count["user1"]["dir1"], 5)

    def test_corrupt_file_no_crash(self):
        """Corrupt cache file doesn't crash — starts with empty caches."""
        from lib.cache import load_caches, cache_path
        path = cache_path(self.tmpdir)
        with open(path, "w") as f:
            f.write("NOT VALID JSON {{{")

        ctx = _make_ctx()
        load_caches(ctx, self.tmpdir)
        self.assertEqual(ctx.folder_cache, {})
        self.assertEqual(ctx.user_upload_speed, {})

    def test_missing_file_no_crash(self):
        """No cache file doesn't crash — starts with empty caches."""
        from lib.cache import load_caches
        ctx = _make_ctx()
        load_caches(ctx, self.tmpdir)
        self.assertEqual(ctx.folder_cache, {})

    def test_atomic_write(self):
        """Cache file is written atomically (no partial writes)."""
        from lib.cache import save_caches, cache_path
        ctx = _make_ctx()
        ctx.user_upload_speed["user1"] = 42
        ctx._upload_speed_ts["user1"] = time.time()

        save_caches(ctx, self.tmpdir)

        path = cache_path(self.tmpdir)
        self.assertTrue(os.path.exists(path))
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

    def test_timestamps_roundtrip(self):
        """Timestamps survive save/load so entries age correctly across runs."""
        from lib.cache import save_caches, load_caches
        ctx1 = _make_ctx()
        old_ts = time.time() - 3600  # 1 hour ago
        ctx1.folder_cache["user1"] = {"dir1": {"files": []}}
        ctx1._folder_cache_ts["user1"] = {"dir1": old_ts}

        save_caches(ctx1, self.tmpdir)

        ctx2 = _make_ctx()
        load_caches(ctx2, self.tmpdir)
        # Timestamp should be preserved, not reset to now
        loaded_ts = ctx2._folder_cache_ts["user1"]["dir1"]
        self.assertAlmostEqual(loaded_ts, old_ts, delta=1.0)


if __name__ == "__main__":
    unittest.main()
