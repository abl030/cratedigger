"""Tests for SoularrConfig — verify from_ini() matches old global parsing."""

import configparser
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.config import SoularrConfig

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")
TEST_CONFIG = os.path.join(FIXTURES_DIR, "test_config.ini")


def load_test_config():
    """Load the test config.ini the same way soularr.py does."""
    config = configparser.ConfigParser(
        interpolation=configparser.BasicInterpolation()
    )
    config.read(TEST_CONFIG)
    return config


class TestConfigFromIni(unittest.TestCase):
    """Verify from_ini() produces the same values as the old main() parsing."""

    @classmethod
    def setUpClass(cls):
        ini = load_test_config()
        cls.cfg = SoularrConfig.from_ini(ini, config_dir="/etc/soularr", var_dir="/var/lib/soularr")

    # --- Slskd ---
    def test_slskd_api_key(self):
        self.assertEqual(self.cfg.slskd_api_key, "test-slskd-key")

    def test_slskd_host_url(self):
        self.assertEqual(self.cfg.slskd_host_url, "http://localhost:5030")

    def test_slskd_url_base(self):
        self.assertEqual(self.cfg.slskd_url_base, "/")

    def test_slskd_download_dir(self):
        self.assertEqual(self.cfg.slskd_download_dir, "/mnt/virtio/music/slskd")

    def test_stalled_timeout(self):
        self.assertEqual(self.cfg.stalled_timeout, 3600)

    def test_remote_queue_timeout(self):
        self.assertEqual(self.cfg.remote_queue_timeout, 300)

    def test_delete_searches(self):
        self.assertFalse(self.cfg.delete_searches)

    # --- Search ---
    def test_ignored_users_empty(self):
        self.assertEqual(self.cfg.ignored_users, ())

    def test_minimum_match_ratio(self):
        self.assertAlmostEqual(self.cfg.minimum_match_ratio, 0.6)

    def test_page_size(self):
        self.assertEqual(self.cfg.page_size, 5)

    def test_search_blacklist_empty(self):
        self.assertEqual(self.cfg.search_blacklist, ())

    def test_album_prepend_artist(self):
        self.assertTrue(self.cfg.album_prepend_artist)

    def test_track_prepend_artist(self):
        self.assertTrue(self.cfg.track_prepend_artist)

    def test_search_timeout(self):
        self.assertEqual(self.cfg.search_timeout, 60000)

    def test_maximum_peer_queue(self):
        self.assertEqual(self.cfg.maximum_peer_queue, 50)

    def test_minimum_peer_upload_speed(self):
        self.assertEqual(self.cfg.minimum_peer_upload_speed, 0)

    def test_search_for_tracks(self):
        self.assertTrue(self.cfg.search_for_tracks)

    def test_parallel_searches_default(self):
        self.assertEqual(self.cfg.parallel_searches, 8)

    # --- Release ---
    def test_use_most_common_tracknum(self):
        self.assertTrue(self.cfg.use_most_common_tracknum)

    def test_allow_multi_disc(self):
        self.assertTrue(self.cfg.allow_multi_disc)

    def test_accepted_countries(self):
        self.assertEqual(self.cfg.accepted_countries, (
            "Europe", "Japan", "United Kingdom", "United States",
            "[Worldwide]", "Australia", "Canada",
        ))

    def test_skip_region_check(self):
        self.assertFalse(self.cfg.skip_region_check)

    def test_accepted_formats(self):
        self.assertEqual(self.cfg.accepted_formats, ("CD", "Digital Media", "Vinyl"))

    # --- Download ---
    def test_download_filtering(self):
        self.assertTrue(self.cfg.download_filtering)

    def test_use_extension_whitelist(self):
        self.assertFalse(self.cfg.use_extension_whitelist)

    def test_extensions_whitelist(self):
        self.assertEqual(self.cfg.extensions_whitelist, ("lrc", "nfo", "txt"))

    # --- Allowed filetypes ---
    def test_allowed_filetypes(self):
        self.assertEqual(self.cfg.allowed_filetypes, (
            "mp3 v0", "mp3 320",
            "flac 24/192", "flac 24/96", "flac 24/48", "flac 16/44.1", "flac",
            "alac", "aac 256+", "ogg 256+", "opus 192+",
        ))

    # --- Beets ---
    def test_beets_enabled(self):
        self.assertTrue(self.cfg.beets_validation_enabled)

    def test_beets_harness_path(self):
        self.assertIn("harness/run_beets_harness.sh", self.cfg.beets_harness_path)

    def test_beets_distance_threshold(self):
        self.assertAlmostEqual(self.cfg.beets_distance_threshold, 0.15)

    def test_beets_staging_dir(self):
        self.assertEqual(self.cfg.beets_staging_dir, "/mnt/virtio/Music/Incoming")

    def test_beets_tracking_file(self):
        self.assertEqual(self.cfg.beets_tracking_file,
                         "/mnt/virtio/Music/Re-download/beets-validated.jsonl")

    # --- Pipeline DB ---
    def test_pipeline_db_enabled(self):
        self.assertTrue(self.cfg.pipeline_db_enabled)

    def test_pipeline_db_dsn(self):
        self.assertEqual(self.cfg.pipeline_db_dsn,
                         "postgresql://soularr@192.168.100.11:5432/soularr")

    # --- Meelo ---
    def test_meelo_url(self):
        self.assertEqual(self.cfg.meelo_url, "http://192.168.1.29:5001")

    def test_meelo_username(self):
        self.assertEqual(self.cfg.meelo_username, "testuser")

    def test_meelo_password(self):
        self.assertEqual(self.cfg.meelo_password, "testpass")

    # --- Plex ---
    def test_plex_url(self):
        self.assertEqual(self.cfg.plex_url, "http://192.168.1.2:32400")

    def test_plex_token(self):
        self.assertEqual(self.cfg.plex_token, "test-plex-token")

    def test_plex_library_section_id(self):
        self.assertEqual(self.cfg.plex_library_section_id, "3")

    # --- Paths ---
    def test_lock_file_path(self):
        self.assertEqual(self.cfg.lock_file_path, "/var/lib/soularr/.soularr.lock")

    def test_config_file_path(self):
        self.assertEqual(self.cfg.config_file_path, "/etc/soularr/config.ini")



class TestConfigFrozen(unittest.TestCase):
    """Verify config is immutable after creation."""

    def test_cannot_mutate(self):
        ini = load_test_config()
        cfg = SoularrConfig.from_ini(ini)
        with self.assertRaises(AttributeError):
            cfg.page_size = 99  # type: ignore[misc]  # intentional: testing frozen dataclass  # type: ignore[misc]


class TestConfigDefaults(unittest.TestCase):
    """Verify defaults work when sections/keys are missing."""

    def test_empty_config(self):
        config = configparser.ConfigParser()
        # Add empty required sections so getboolean etc. don't fail on missing section
        for section in ["Slskd", "Search Settings", "Release Settings",
                        "Download Settings", "Beets Validation", "Pipeline DB", "Meelo", "Plex"]:
            config.add_section(section)
        cfg = SoularrConfig.from_ini(config)
        self.assertEqual(cfg.page_size, 10)
        self.assertEqual(cfg.stalled_timeout, 3600)
        self.assertAlmostEqual(cfg.beets_distance_threshold, 0.15)
        self.assertFalse(cfg.pipeline_db_enabled)
        self.assertIsNone(cfg.meelo_url)
        self.assertIsNone(cfg.plex_url)
        self.assertIsNone(cfg.plex_token)
        self.assertIsNone(cfg.plex_library_section_id)

    def test_single_filetype(self):
        config = configparser.ConfigParser()
        for section in ["Slskd", "Search Settings", "Release Settings",
                        "Download Settings", "Beets Validation", "Pipeline DB", "Meelo", "Plex"]:
            config.add_section(section)
        config.set("Search Settings", "allowed_filetypes", "flac")
        cfg = SoularrConfig.from_ini(config)
        self.assertEqual(cfg.allowed_filetypes, ("flac",))



if __name__ == "__main__":
    unittest.main()
