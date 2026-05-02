"""Tests for CratediggerConfig — verify from_ini() matches old global parsing."""

import configparser
import os
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.config import (
    CratediggerConfig,
    invalidate_secret_cache,
    read_runtime_config,
    read_runtime_rank_config,
    read_secret_file,
    read_verified_lossless_target,
)

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")
TEST_CONFIG = os.path.join(FIXTURES_DIR, "test_config.ini")


def load_test_config():
    """Load the test config.ini the same way cratedigger.py does (RawConfigParser)."""
    config = configparser.RawConfigParser()
    config.read(TEST_CONFIG)
    return config


class TestConfigFromIni(unittest.TestCase):
    """Verify from_ini() produces the same values as the old main() parsing."""

    @classmethod
    def setUpClass(cls):
        ini = load_test_config()
        cls.cfg = CratediggerConfig.from_ini(ini, config_dir="/etc/cratedigger", var_dir="/var/lib/cratedigger")

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

    def test_browse_parallelism_default(self):
        self.assertEqual(self.cfg.browse_parallelism, 4)

    def test_browse_parallelism_capped_at_8(self):
        """Values > 8 should be clamped to 8."""
        from lib.config import CratediggerConfig
        ini = configparser.ConfigParser()
        ini.read_string("[Search Settings]\nbrowse_parallelism = 20\n")
        cfg = CratediggerConfig.from_ini(ini)
        self.assertEqual(cfg.browse_parallelism, 8)

    def test_search_response_limit_explicit(self):
        """Explicit search_response_limit is parsed verbatim."""
        ini = configparser.ConfigParser()
        ini.read_string("[Search Settings]\nsearch_response_limit = 1500\n")
        cfg = CratediggerConfig.from_ini(ini)
        self.assertEqual(cfg.search_response_limit, 1500)

    def test_search_escalation_threshold_explicit(self):
        """Explicit search_escalation_threshold is parsed verbatim."""
        ini = configparser.ConfigParser()
        ini.read_string("[Search Settings]\nsearch_escalation_threshold = 9\n")
        cfg = CratediggerConfig.from_ini(ini)
        self.assertEqual(cfg.search_escalation_threshold, 9)

    def test_search_response_limit_default(self):
        """Missing key falls back to default of 1000."""
        ini = configparser.ConfigParser()
        ini.add_section("Search Settings")
        cfg = CratediggerConfig.from_ini(ini)
        self.assertEqual(cfg.search_response_limit, 1000)

    def test_search_escalation_threshold_default(self):
        """Missing key falls back to default of 5."""
        ini = configparser.ConfigParser()
        ini.add_section("Search Settings")
        cfg = CratediggerConfig.from_ini(ini)
        self.assertEqual(cfg.search_escalation_threshold, 5)

    def test_search_settings_defaults_when_section_missing(self):
        """Missing [Search Settings] section yields the documented defaults."""
        ini = configparser.ConfigParser()
        cfg = CratediggerConfig.from_ini(ini)
        self.assertEqual(cfg.search_response_limit, 1000)
        self.assertEqual(cfg.search_file_limit, 50000)
        self.assertEqual(cfg.search_escalation_threshold, 5)

    def test_search_file_limit_explicit(self):
        """Explicit search_file_limit is parsed verbatim."""
        ini = configparser.ConfigParser()
        ini.read_string("[Search Settings]\nsearch_file_limit = 100000\n")
        cfg = CratediggerConfig.from_ini(ini)
        self.assertEqual(cfg.search_file_limit, 100000)

    def test_search_file_limit_default(self):
        """Missing key falls back to default of 50000."""
        ini = configparser.ConfigParser()
        ini.add_section("Search Settings")
        cfg = CratediggerConfig.from_ini(ini)
        self.assertEqual(cfg.search_file_limit, 50000)

    def test_search_response_limit_zero_passes_through(self):
        """`from_ini` does not validate; a literal 0 is accepted as-is.

        Defensive validation belongs further down the stack (slskd will reject
        nonsense values); the config layer must remain a transparent loader.
        """
        ini = configparser.ConfigParser()
        ini.read_string("[Search Settings]\nsearch_response_limit = 0\n")
        cfg = CratediggerConfig.from_ini(ini)
        self.assertEqual(cfg.search_response_limit, 0)

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
                         "postgresql://cratedigger@192.168.100.11:5432/cratedigger")

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
        self.assertEqual(self.cfg.lock_file_path, "/var/lib/cratedigger/.cratedigger.lock")

    def test_config_file_path(self):
        self.assertEqual(self.cfg.config_file_path, "/etc/cratedigger/config.ini")



class TestConfigFrozen(unittest.TestCase):
    """Verify config is immutable after creation."""

    def test_cannot_mutate(self):
        ini = load_test_config()
        cfg = CratediggerConfig.from_ini(ini)
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
        cfg = CratediggerConfig.from_ini(config)
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
        cfg = CratediggerConfig.from_ini(config)
        self.assertEqual(cfg.allowed_filetypes, ("flac",))

    def test_verified_lossless_target_default_empty(self):
        config = configparser.ConfigParser()
        for section in ["Slskd", "Search Settings", "Release Settings",
                        "Download Settings", "Beets Validation", "Pipeline DB", "Meelo", "Plex"]:
            config.add_section(section)
        cfg = CratediggerConfig.from_ini(config)
        self.assertEqual(cfg.verified_lossless_target, "")

    def test_verified_lossless_target_set(self):
        config = configparser.ConfigParser()
        for section in ["Slskd", "Search Settings", "Release Settings",
                        "Download Settings", "Beets Validation", "Pipeline DB", "Meelo", "Plex"]:
            config.add_section(section)
        config.set("Beets Validation", "verified_lossless_target", "opus 128")
        cfg = CratediggerConfig.from_ini(config)
        self.assertEqual(cfg.verified_lossless_target, "opus 128")


class TestReadVerifiedLosslessTarget(unittest.TestCase):
    def test_missing_file_returns_empty(self):
        self.assertEqual(
            read_verified_lossless_target("/nonexistent/cratedigger-config.ini"), ""
        )

    def test_reads_runtime_value_from_config_file(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "config.ini")
            with open(path, "w", encoding="utf-8") as f:
                f.write("[Beets Validation]\nverified_lossless_target = aac 128\n")
            self.assertEqual(read_verified_lossless_target(path), "aac 128")


class TestReadRuntimeConfig(unittest.TestCase):
    def test_missing_config_returns_default(self):
        cfg = read_runtime_config("/nonexistent/config.ini")
        self.assertEqual(cfg.beets_harness_path, "")

    def test_reads_full_runtime_config(self):
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".ini") as tmp:
            tmp.write(
                "[Beets Validation]\n"
                "harness_path = /nix/store/test/run_beets_harness.sh\n"
                "verified_lossless_target = opus 128\n"
                "[Meelo]\n"
                "url = http://meelo.test\n"
                "[Plex]\n"
                "url = http://plex.test\n"
                "token = test-token\n"
            )
            config_path = tmp.name
        try:
            cfg = read_runtime_config(config_path)
        finally:
            os.unlink(config_path)

        self.assertEqual(cfg.beets_harness_path, "/nix/store/test/run_beets_harness.sh")
        self.assertEqual(cfg.verified_lossless_target, "opus 128")
        self.assertEqual(cfg.meelo_url, "http://meelo.test")
        self.assertEqual(cfg.plex_url, "http://plex.test")
        self.assertEqual(cfg.plex_token, "test-token")


class TestReadRuntimeRankConfig(unittest.TestCase):
    def test_missing_config_returns_defaults(self):
        cfg = read_runtime_rank_config("/nonexistent/config.ini")
        self.assertEqual(cfg.gate_min_rank.name, "EXCELLENT")

    def test_reads_quality_ranks_from_runtime_config(self):
        from lib.quality import QualityRank

        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".ini") as tmp:
            tmp.write(
                "[Quality Ranks]\n"
                "gate_min_rank = good\n"
                "bitrate_metric = min\n"
            )
            config_path = tmp.name
        try:
            cfg = read_runtime_rank_config(config_path)
        finally:
            os.unlink(config_path)

        self.assertEqual(cfg.gate_min_rank, QualityRank.GOOD)
        self.assertEqual(cfg.bitrate_metric.value, "min")


class TestReadSecretFile(unittest.TestCase):
    """Shared helper — reads a single-line secret from a file with in-process cache.

    This helper is why the whole fix works: it's the one place the pipeline
    reads plaintext secrets, so config.ini never has to embed them.
    """

    def setUp(self):
        invalidate_secret_cache()
        self.addCleanup(invalidate_secret_cache)

    def test_reads_and_strips_trailing_whitespace(self):
        with tempfile.NamedTemporaryFile("w", delete=False) as tmp:
            tmp.write("my-secret\n")
            path = tmp.name
        self.addCleanup(os.unlink, path)
        self.assertEqual(read_secret_file(path), "my-secret")

    def test_in_process_cache_hits_on_second_call(self):
        with tempfile.NamedTemporaryFile("w", delete=False) as tmp:
            tmp.write("v1\n")
            path = tmp.name
        self.addCleanup(os.unlink, path)

        first = read_secret_file(path)
        # Mutate the file — a fresh read would observe v2, but the cache
        # must return v1 because we've already seen this path.
        with open(path, "w", encoding="utf-8") as f:
            f.write("v2\n")
        second = read_secret_file(path)
        self.assertEqual(first, "v1")
        self.assertEqual(second, "v1")

    def test_invalidate_cache_forces_re_read(self):
        with tempfile.NamedTemporaryFile("w", delete=False) as tmp:
            tmp.write("before\n")
            path = tmp.name
        self.addCleanup(os.unlink, path)

        self.assertEqual(read_secret_file(path), "before")
        with open(path, "w", encoding="utf-8") as f:
            f.write("after\n")
        invalidate_secret_cache()
        self.assertEqual(read_secret_file(path), "after")

    def test_missing_file_raises(self):
        with self.assertRaises(FileNotFoundError):
            read_secret_file("/nonexistent/secret")


class TestSecretFileFields(unittest.TestCase):
    """from_ini() reads *_file keys from the INI into CratediggerConfig."""

    def setUp(self):
        invalidate_secret_cache()
        self.addCleanup(invalidate_secret_cache)

    def test_reads_all_secret_file_keys(self):
        config = configparser.RawConfigParser()
        config.read_string(
            "[Slskd]\n"
            "api_key_file = /run/secrets/slskd-key\n"
            "[Meelo]\n"
            "username_file = /run/secrets/meelo-user\n"
            "password_file = /run/secrets/meelo-pass\n"
            "[Plex]\n"
            "token_file = /run/secrets/plex-token\n"
            "[Jellyfin]\n"
            "token_file = /run/secrets/jellyfin-token\n"
        )
        cfg = CratediggerConfig.from_ini(config)
        self.assertEqual(cfg.slskd_api_key_file, "/run/secrets/slskd-key")
        self.assertEqual(cfg.meelo_username_file, "/run/secrets/meelo-user")
        self.assertEqual(cfg.meelo_password_file, "/run/secrets/meelo-pass")
        self.assertEqual(cfg.plex_token_file, "/run/secrets/plex-token")
        self.assertEqual(cfg.jellyfin_token_file, "/run/secrets/jellyfin-token")

    def test_file_paths_default_to_empty(self):
        cfg = CratediggerConfig.from_ini(configparser.RawConfigParser())
        self.assertEqual(cfg.slskd_api_key_file, "")
        self.assertEqual(cfg.meelo_username_file, "")
        self.assertEqual(cfg.meelo_password_file, "")
        self.assertEqual(cfg.plex_token_file, "")
        self.assertEqual(cfg.jellyfin_token_file, "")


class TestResolvedSecrets(unittest.TestCase):
    """Resolver methods prefer *_file path over the legacy plaintext field.

    This is how call sites read secrets now — cfg.resolved_slskd_api_key()
    instead of cfg.slskd_api_key. Guarantees that if a *_file path is set,
    the live file contents are returned (not whatever stale plaintext may
    have survived in the INI).
    """

    def setUp(self):
        invalidate_secret_cache()
        self.addCleanup(invalidate_secret_cache)

    def _write_secret(self, value: str) -> str:
        with tempfile.NamedTemporaryFile("w", delete=False) as tmp:
            tmp.write(value)
            path = tmp.name
        self.addCleanup(os.unlink, path)
        return path

    def test_slskd_api_key_resolves_file_over_direct(self):
        path = self._write_secret("from-file\n")
        cfg = CratediggerConfig(slskd_api_key="from-direct", slskd_api_key_file=path)
        self.assertEqual(cfg.resolved_slskd_api_key(), "from-file")

    def test_slskd_api_key_falls_back_to_direct_when_no_file(self):
        cfg = CratediggerConfig(slskd_api_key="from-direct")
        self.assertEqual(cfg.resolved_slskd_api_key(), "from-direct")

    def test_slskd_api_key_empty_when_neither_set(self):
        cfg = CratediggerConfig()
        self.assertEqual(cfg.resolved_slskd_api_key(), "")

    def test_meelo_credentials_resolve_from_files(self):
        user_path = self._write_secret("meelo-user")
        pass_path = self._write_secret("meelo-pass")
        cfg = CratediggerConfig(
            meelo_username_file=user_path,
            meelo_password_file=pass_path,
        )
        self.assertEqual(cfg.resolved_meelo_username(), "meelo-user")
        self.assertEqual(cfg.resolved_meelo_password(), "meelo-pass")

    def test_plex_token_resolves_from_file(self):
        path = self._write_secret("plex-live-token\n")
        cfg = CratediggerConfig(plex_url="http://plex", plex_token_file=path)
        self.assertEqual(cfg.resolved_plex_token(), "plex-live-token")

    def test_jellyfin_token_resolves_from_file(self):
        path = self._write_secret("jellyfin-live-token\n")
        cfg = CratediggerConfig(jellyfin_url="http://jellyfin", jellyfin_token_file=path)
        self.assertEqual(cfg.resolved_jellyfin_token(), "jellyfin-live-token")

    def test_notifier_resolvers_none_when_unset(self):
        cfg = CratediggerConfig()
        self.assertIsNone(cfg.resolved_meelo_username())
        self.assertIsNone(cfg.resolved_meelo_password())
        self.assertIsNone(cfg.resolved_plex_token())
        self.assertIsNone(cfg.resolved_jellyfin_token())

    def test_legacy_plaintext_field_still_resolves(self):
        """Direct fields on CratediggerConfig keep working so tests and old deployments
        that haven't switched to *_file paths continue to function."""
        cfg = CratediggerConfig(
            meelo_url="http://meelo",
            meelo_username="legacy-user",
            meelo_password="legacy-pass",
            plex_token="legacy-plex",
            jellyfin_token="legacy-jellyfin",
        )
        self.assertEqual(cfg.resolved_meelo_username(), "legacy-user")
        self.assertEqual(cfg.resolved_meelo_password(), "legacy-pass")
        self.assertEqual(cfg.resolved_plex_token(), "legacy-plex")
        self.assertEqual(cfg.resolved_jellyfin_token(), "legacy-jellyfin")


class TestRawConfigParserRegression(unittest.TestCase):
    """RawConfigParser must be used — BasicInterpolation chokes on % in logging format."""

    def test_raw_parser_reads_logging_format_with_percent(self):
        """The logging format has %(levelname)s etc. which BasicInterpolation
        would try to interpolate, raising InterpolationMissingOptionError."""
        raw = configparser.RawConfigParser()
        raw.read(TEST_CONFIG)
        # RawConfigParser returns the literal string with %() intact
        fmt = raw.get("Logging", "format")
        self.assertIn("%(levelname)s", fmt)

    def test_basic_interpolation_fails_on_logging_format(self):
        """Proves BasicInterpolation can't handle our config — the reason we switched."""
        basic = configparser.ConfigParser(
            interpolation=configparser.BasicInterpolation()
        )
        basic.read(TEST_CONFIG)
        with self.assertRaises(configparser.InterpolationMissingOptionError):
            basic.get("Logging", "format")


class TestMainCLIParsing(unittest.TestCase):
    """Test the CLI argument parsing and config loading path in main()."""

    def setUp(self):
        # cratedigger.main() calls reset_umask() (sets umask to 0 for the pipeline's
        # subprocess chain, GH #84). Restore the prior umask so later tests in
        # the same process keep their expected default.
        self._saved_umask = os.umask(0o022)
        os.umask(self._saved_umask)
        self.addCleanup(os.umask, self._saved_umask)

    class _StopMain(Exception):
        """Sentinel to stop main() after config parsing in tests."""

    def _write_minimal_config(self, directory: str, *, api_key: str = "test-key") -> str:
        path = os.path.join(directory, "config.ini")
        with open(path, "w", encoding="utf-8") as f:
            f.write(
                "[Slskd]\n"
                f"api_key = {api_key}\n"
                "host_url = http://localhost:5030\n"
                "download_dir = /tmp/test\n"
                "[Pipeline DB]\n"
                "enabled = True\n"
                "dsn = postgresql://test@localhost/test\n"
            )
        return path

    def _run_main_until_config_parse(self, argv: list[str], callback) -> None:
        import cratedigger

        cratedigger.cfg = None
        cratedigger.pipeline_db_source = None
        cratedigger._module_ctx = None

        def fake_from_ini(config, config_dir=".", var_dir="."):
            callback(config, config_dir, var_dir)
            raise self._StopMain()

        with patch.object(sys, "argv", argv), \
             patch("lib.config.CratediggerConfig.from_ini", side_effect=fake_from_ini):
            with self.assertRaises(self._StopMain):
                cratedigger.main()

    def test_missing_config_exits_with_error(self):
        """main() exits 1 when config.ini doesn't exist at --config-dir."""
        import cratedigger

        with tempfile.TemporaryDirectory() as d:
            with patch.object(sys, "argv", [
                "cratedigger", "--config-dir", d, "--var-dir", d, "--no-lock-file"
            ]):
                with self.assertRaises(SystemExit) as cm:
                    cratedigger.main()
            self.assertEqual(cm.exception.code, 1)

    def test_config_dir_resolves_config_path(self):
        """--config-dir joins with config.ini to find the config file."""
        with tempfile.TemporaryDirectory() as d:
            self._write_minimal_config(d, api_key="from-config-dir")

            def assert_from_ini(config, config_dir, var_dir):
                self.assertIsInstance(config, configparser.RawConfigParser)
                self.assertEqual(config_dir, d)
                self.assertEqual(var_dir, d)
                self.assertEqual(config.get("Slskd", "api_key"), "from-config-dir")

            self._run_main_until_config_parse(
                ["cratedigger", "--config-dir", d, "--var-dir", d, "--no-lock-file"],
                assert_from_ini,
            )

    def test_lock_file_created_in_var_dir(self):
        """Lock file path is var_dir/.cratedigger.lock."""
        with tempfile.TemporaryDirectory() as config_dir, tempfile.TemporaryDirectory() as var_dir:
            self._write_minimal_config(config_dir)
            lock_path = os.path.join(var_dir, ".cratedigger.lock")

            def assert_from_ini(config, actual_config_dir, actual_var_dir):
                self.assertEqual(actual_config_dir, config_dir)
                self.assertEqual(actual_var_dir, var_dir)
                self.assertTrue(os.path.exists(lock_path))

            self._run_main_until_config_parse(
                ["cratedigger", "--config-dir", config_dir, "--var-dir", var_dir],
                assert_from_ini,
            )

    def test_no_lock_file_flag(self):
        """--no-lock-file sets the flag to True."""
        with tempfile.TemporaryDirectory() as d:
            self._write_minimal_config(d)
            lock_path = os.path.join(d, ".cratedigger.lock")

            def assert_from_ini(config, config_dir, var_dir):
                self.assertEqual(config_dir, d)
                self.assertEqual(var_dir, d)
                self.assertFalse(os.path.exists(lock_path))

            self._run_main_until_config_parse(
                ["cratedigger", "--config-dir", d, "--var-dir", d, "--no-lock-file"],
                assert_from_ini,
            )

    def test_default_args(self):
        """Default config-dir and var-dir are cwd."""
        with tempfile.TemporaryDirectory() as cwd:
            self._write_minimal_config(cwd, api_key="from-default-cwd")

            def assert_from_ini(config, config_dir, var_dir):
                self.assertEqual(config_dir, cwd)
                self.assertEqual(var_dir, cwd)
                self.assertEqual(config.get("Slskd", "api_key"), "from-default-cwd")

            with patch("cratedigger.os.getcwd", return_value=cwd):
                self._run_main_until_config_parse(
                    ["cratedigger", "--no-lock-file"],
                    assert_from_ini,
                )


class TestFanoutConfigClamping(unittest.TestCase):
    """Reject zero/negative fan-out config at parse time so a typo doesn't
    crash the first album of every cycle (issue #198 review finding #4)."""

    def _parse(self, **search_settings) -> CratediggerConfig:
        ini = configparser.RawConfigParser()
        ini["Search Settings"] = {k: str(v) for k, v in search_settings.items()}
        return CratediggerConfig.from_ini(ini)

    def test_browse_top_k_zero_clamps_to_one(self):
        cfg = self._parse(browse_top_k=0)
        self.assertEqual(cfg.browse_top_k, 1)

    def test_browse_top_k_negative_clamps_to_one(self):
        cfg = self._parse(browse_top_k=-5)
        self.assertEqual(cfg.browse_top_k, 1)

    def test_browse_global_max_workers_zero_clamps_to_one(self):
        cfg = self._parse(browse_global_max_workers=0)
        self.assertEqual(cfg.browse_global_max_workers, 1)

    def test_search_max_inflight_zero_clamps_to_one(self):
        cfg = self._parse(search_max_inflight=0)
        self.assertEqual(cfg.search_max_inflight, 1)

    def test_browse_wave_deadline_negative_clamps_to_zero(self):
        cfg = self._parse(browse_wave_deadline_s=-1.0)
        self.assertEqual(cfg.browse_wave_deadline_s, 0.0)

    def test_browse_cycle_budget_negative_clamps_to_zero(self):
        cfg = self._parse(browse_cycle_budget_s=-100.0)
        self.assertEqual(cfg.browse_cycle_budget_s, 0.0)

    def test_valid_values_pass_through_unchanged(self):
        cfg = self._parse(
            browse_top_k=15,
            browse_wave_deadline_s=10.0,
            browse_global_max_workers=64,
            browse_cycle_budget_s=300.0,
            search_max_inflight=2,
        )
        self.assertEqual(cfg.browse_top_k, 15)
        self.assertEqual(cfg.browse_wave_deadline_s, 10.0)
        self.assertEqual(cfg.browse_global_max_workers, 64)
        self.assertEqual(cfg.browse_cycle_budget_s, 300.0)
        self.assertEqual(cfg.search_max_inflight, 2)


if __name__ == "__main__":
    unittest.main()
