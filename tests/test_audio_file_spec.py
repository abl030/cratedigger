"""Tests for AudioFileSpec dataclass, parsing, identity, and matching.

TDD: Written RED first, then implemented.
"""

import unittest

from lib.quality import (
    AudioFileSpec,
    parse_filetype_config,
    file_identity,
    filetype_matches,
    AUDIO_EXTENSIONS,
    LOSSLESS_CODECS,
    CATCH_ALL_SPEC,
)


# ---------------------------------------------------------------------------
# AudioFileSpec construction
# ---------------------------------------------------------------------------


class TestParseFiletypeConfig(unittest.TestCase):
    """parse_filetype_config() turns config DSL strings into AudioFileSpec."""

    def test_bare_mp3(self):
        spec = parse_filetype_config("mp3")
        self.assertEqual(spec.codec, "mp3")
        self.assertEqual(spec.extension, "mp3")
        self.assertIsNone(spec.quality)

    def test_bare_flac(self):
        spec = parse_filetype_config("flac")
        self.assertEqual(spec.codec, "flac")
        self.assertEqual(spec.extension, "flac")

    def test_bare_alac_maps_to_m4a_extension(self):
        """THE BUG: config says 'alac' but files are .m4a."""
        spec = parse_filetype_config("alac")
        self.assertEqual(spec.codec, "alac")
        self.assertEqual(spec.extension, "m4a")

    def test_bare_aac(self):
        spec = parse_filetype_config("aac")
        self.assertEqual(spec.codec, "aac")
        self.assertEqual(spec.extension, "aac")

    def test_mp3_v0(self):
        spec = parse_filetype_config("mp3 v0")
        self.assertEqual(spec.codec, "mp3")
        self.assertEqual(spec.quality, "v0")

    def test_mp3_v2(self):
        spec = parse_filetype_config("mp3 v2")
        self.assertEqual(spec.codec, "mp3")
        self.assertEqual(spec.quality, "v2")

    def test_mp3_320(self):
        spec = parse_filetype_config("mp3 320")
        self.assertEqual(spec.codec, "mp3")
        self.assertEqual(spec.quality, "320")

    def test_flac_24_96(self):
        spec = parse_filetype_config("flac 24/96")
        self.assertEqual(spec.codec, "flac")
        self.assertEqual(spec.quality, "24/96")

    def test_aac_256_plus(self):
        spec = parse_filetype_config("aac 256+")
        self.assertEqual(spec.codec, "aac")
        self.assertEqual(spec.quality, "256+")

    def test_ogg_256_plus(self):
        spec = parse_filetype_config("ogg 256+")
        self.assertEqual(spec.codec, "ogg")
        self.assertEqual(spec.quality, "256+")

    def test_opus_192_plus(self):
        spec = parse_filetype_config("opus 192+")
        self.assertEqual(spec.codec, "opus")
        self.assertEqual(spec.quality, "192+")

    def test_strips_whitespace(self):
        spec = parse_filetype_config("  mp3 v0  ")
        self.assertEqual(spec.codec, "mp3")
        self.assertEqual(spec.quality, "v0")

    def test_case_insensitive(self):
        spec = parse_filetype_config("FLAC")
        self.assertEqual(spec.codec, "flac")

    def test_flac_16_44_1(self):
        spec = parse_filetype_config("flac 16/44.1")
        self.assertEqual(spec.codec, "flac")
        self.assertEqual(spec.quality, "16/44.1")

    def test_wav(self):
        spec = parse_filetype_config("wav")
        self.assertEqual(spec.codec, "wav")
        self.assertEqual(spec.extension, "wav")


# ---------------------------------------------------------------------------
# file_identity — slskd file dict → AudioFileSpec
# ---------------------------------------------------------------------------


class TestFileIdentity(unittest.TestCase):
    """file_identity() constructs AudioFileSpec from slskd file dicts."""

    def test_mp3_basic(self):
        f = {"filename": "Music\\Artist\\track.mp3", "bitRate": 320}
        spec = file_identity(f)
        self.assertEqual(spec.codec, "mp3")
        self.assertEqual(spec.extension, "mp3")
        self.assertEqual(spec.bitrate, 320)

    def test_flac_with_metadata(self):
        f = {"filename": "track.flac", "bitRate": 900,
             "sampleRate": 44100, "bitDepth": 16}
        spec = file_identity(f)
        self.assertEqual(spec.codec, "flac")
        self.assertEqual(spec.bit_depth, 16)
        self.assertEqual(spec.sample_rate, 44100)

    def test_m4a_high_bitrate_is_alac(self):
        """High bitrate .m4a = ALAC (lossless)."""
        f = {"filename": "track.m4a", "bitRate": 900}
        spec = file_identity(f)
        self.assertEqual(spec.codec, "alac")
        self.assertEqual(spec.extension, "m4a")

    def test_m4a_with_bitdepth_is_alac(self):
        """Presence of bitDepth = ALAC."""
        f = {"filename": "track.m4a", "bitDepth": 16, "sampleRate": 44100}
        spec = file_identity(f)
        self.assertEqual(spec.codec, "alac")

    def test_m4a_low_bitrate_is_aac(self):
        """Low bitrate .m4a = AAC (lossy)."""
        f = {"filename": "track.m4a", "bitRate": 256}
        spec = file_identity(f)
        self.assertEqual(spec.codec, "aac")
        self.assertEqual(spec.extension, "m4a")

    def test_m4a_very_low_bitrate_is_aac(self):
        f = {"filename": "track.m4a", "bitRate": 128}
        spec = file_identity(f)
        self.assertEqual(spec.codec, "aac")

    def test_m4a_no_metadata_defaults_to_aac(self):
        """Ambiguous .m4a without metadata defaults to AAC."""
        f = {"filename": "track.m4a"}
        spec = file_identity(f)
        self.assertEqual(spec.codec, "aac")

    def test_m4a_borderline_500_is_aac(self):
        """500kbps is ambiguous — could be high-quality AAC. Default AAC."""
        f = {"filename": "track.m4a", "bitRate": 500}
        spec = file_identity(f)
        self.assertEqual(spec.codec, "aac")

    def test_m4a_700_is_alac(self):
        f = {"filename": "track.m4a", "bitRate": 700}
        spec = file_identity(f)
        self.assertEqual(spec.codec, "alac")

    def test_ogg_file(self):
        f = {"filename": "track.ogg", "bitRate": 256}
        spec = file_identity(f)
        self.assertEqual(spec.codec, "ogg")

    def test_opus_file(self):
        f = {"filename": "track.opus", "bitRate": 192}
        spec = file_identity(f)
        self.assertEqual(spec.codec, "opus")

    def test_vbr_flag_preserved(self):
        f = {"filename": "track.mp3", "bitRate": 245, "isVariableBitRate": True}
        spec = file_identity(f)
        self.assertTrue(spec.is_variable_bitrate)

    def test_backslash_path(self):
        """slskd paths use backslashes."""
        f = {"filename": "Music\\Folder\\Sub\\track.flac"}
        spec = file_identity(f)
        self.assertEqual(spec.codec, "flac")
        self.assertEqual(spec.extension, "flac")

    def test_forward_slash_path(self):
        f = {"filename": "Music/Folder/track.mp3", "bitRate": 320}
        spec = file_identity(f)
        self.assertEqual(spec.codec, "mp3")


# ---------------------------------------------------------------------------
# Lossless property
# ---------------------------------------------------------------------------


class TestLosslessProperty(unittest.TestCase):
    """AudioFileSpec.lossless correctly identifies lossless codecs."""

    def test_flac_is_lossless(self):
        self.assertTrue(parse_filetype_config("flac").lossless)

    def test_alac_is_lossless(self):
        self.assertTrue(parse_filetype_config("alac").lossless)

    def test_wav_is_lossless(self):
        self.assertTrue(parse_filetype_config("wav").lossless)

    def test_mp3_is_not_lossless(self):
        self.assertFalse(parse_filetype_config("mp3").lossless)

    def test_aac_is_not_lossless(self):
        self.assertFalse(parse_filetype_config("aac").lossless)

    def test_ogg_is_not_lossless(self):
        self.assertFalse(parse_filetype_config("ogg").lossless)

    def test_opus_is_not_lossless(self):
        self.assertFalse(parse_filetype_config("opus").lossless)

    def test_m4a_alac_identity_is_lossless(self):
        """ALAC file identified from .m4a is lossless."""
        f = {"filename": "track.m4a", "bitRate": 900}
        self.assertTrue(file_identity(f).lossless)

    def test_m4a_aac_identity_is_not_lossless(self):
        """AAC file identified from .m4a is NOT lossless."""
        f = {"filename": "track.m4a", "bitRate": 256}
        self.assertFalse(file_identity(f).lossless)


# ---------------------------------------------------------------------------
# filetype_matches — the new verify_filetype core
# ---------------------------------------------------------------------------


class TestFiletypeMatches(unittest.TestCase):
    """filetype_matches() replaces verify_filetype() internals."""

    # --- Codec matching ---

    def test_codec_must_match(self):
        identity = file_identity({"filename": "track.flac", "bitRate": 800})
        filter_spec = parse_filetype_config("mp3 320")
        self.assertFalse(filetype_matches(identity, filter_spec))

    def test_bare_codec_matches_any_quality(self):
        identity = file_identity({"filename": "track.mp3", "bitRate": 128})
        filter_spec = parse_filetype_config("mp3")
        self.assertTrue(filetype_matches(identity, filter_spec))

    # --- THE ALAC FIX ---

    def test_alac_m4a_matches_alac_config(self):
        """THE BUG FIX: .m4a ALAC file matches 'alac' config."""
        identity = file_identity({"filename": "track.m4a", "bitRate": 900})
        filter_spec = parse_filetype_config("alac")
        self.assertTrue(filetype_matches(identity, filter_spec))

    def test_aac_m4a_matches_aac_config(self):
        """Low bitrate .m4a matches 'aac 256+' config."""
        identity = file_identity({"filename": "track.m4a", "bitRate": 256})
        filter_spec = parse_filetype_config("aac 256+")
        self.assertTrue(filetype_matches(identity, filter_spec))

    def test_aac_m4a_does_not_match_alac_config(self):
        """Low bitrate .m4a is AAC, should NOT match 'alac' config."""
        identity = file_identity({"filename": "track.m4a", "bitRate": 256})
        filter_spec = parse_filetype_config("alac")
        self.assertFalse(filetype_matches(identity, filter_spec))

    def test_alac_m4a_does_not_match_aac_config(self):
        """High bitrate .m4a is ALAC, should NOT match 'aac' config."""
        identity = file_identity({"filename": "track.m4a", "bitRate": 900})
        filter_spec = parse_filetype_config("aac 256+")
        self.assertFalse(filetype_matches(identity, filter_spec))

    # --- Exact bitrate ---

    def test_mp3_320_exact(self):
        identity = file_identity({"filename": "track.mp3", "bitRate": 320})
        filter_spec = parse_filetype_config("mp3 320")
        self.assertTrue(filetype_matches(identity, filter_spec))

    def test_mp3_320_wrong_bitrate(self):
        identity = file_identity({"filename": "track.mp3", "bitRate": 192})
        filter_spec = parse_filetype_config("mp3 320")
        self.assertFalse(filetype_matches(identity, filter_spec))

    def test_mp3_exact_no_bitrate(self):
        identity = file_identity({"filename": "track.mp3"})
        filter_spec = parse_filetype_config("mp3 320")
        self.assertFalse(filetype_matches(identity, filter_spec))

    # --- VBR presets ---

    def test_mp3_v0_matches_245(self):
        identity = file_identity({"filename": "track.mp3", "bitRate": 245})
        self.assertTrue(filetype_matches(identity, parse_filetype_config("mp3 v0")))

    def test_mp3_v0_rejects_cbr_320(self):
        identity = file_identity({"filename": "track.mp3", "bitRate": 320})
        self.assertFalse(filetype_matches(identity, parse_filetype_config("mp3 v0")))

    def test_mp3_v0_rejects_cbr_256(self):
        identity = file_identity({"filename": "track.mp3", "bitRate": 256})
        self.assertFalse(filetype_matches(identity, parse_filetype_config("mp3 v0")))

    def test_mp3_v0_lower_boundary(self):
        identity = file_identity({"filename": "track.mp3", "bitRate": 220})
        self.assertTrue(filetype_matches(identity, parse_filetype_config("mp3 v0")))

    def test_mp3_v0_upper_boundary(self):
        identity = file_identity({"filename": "track.mp3", "bitRate": 280})
        self.assertTrue(filetype_matches(identity, parse_filetype_config("mp3 v0")))

    def test_mp3_v0_below_lower(self):
        identity = file_identity({"filename": "track.mp3", "bitRate": 219})
        self.assertFalse(filetype_matches(identity, parse_filetype_config("mp3 v0")))

    def test_mp3_v0_above_upper(self):
        identity = file_identity({"filename": "track.mp3", "bitRate": 281})
        self.assertFalse(filetype_matches(identity, parse_filetype_config("mp3 v0")))

    def test_mp3_v0_with_vbr_flag_true(self):
        identity = file_identity({"filename": "track.mp3", "bitRate": 245,
                                   "isVariableBitRate": True})
        self.assertTrue(filetype_matches(identity, parse_filetype_config("mp3 v0")))

    def test_mp3_v0_with_vbr_flag_false(self):
        identity = file_identity({"filename": "track.mp3", "bitRate": 245,
                                   "isVariableBitRate": False})
        self.assertFalse(filetype_matches(identity, parse_filetype_config("mp3 v0")))

    def test_mp3_v2_matches_190(self):
        identity = file_identity({"filename": "track.mp3", "bitRate": 190})
        self.assertTrue(filetype_matches(identity, parse_filetype_config("mp3 v2")))

    def test_mp3_v2_rejects_low(self):
        identity = file_identity({"filename": "track.mp3", "bitRate": 120})
        self.assertFalse(filetype_matches(identity, parse_filetype_config("mp3 v2")))

    def test_mp3_v0_no_bitrate(self):
        identity = file_identity({"filename": "track.mp3"})
        self.assertFalse(filetype_matches(identity, parse_filetype_config("mp3 v0")))

    # --- Minimum bitrate ---

    def test_aac_256_plus_passes(self):
        identity = file_identity({"filename": "track.aac", "bitRate": 256})
        self.assertTrue(filetype_matches(identity, parse_filetype_config("aac 256+")))

    def test_aac_256_plus_fails(self):
        identity = file_identity({"filename": "track.aac", "bitRate": 200})
        self.assertFalse(filetype_matches(identity, parse_filetype_config("aac 256+")))

    def test_ogg_256_plus(self):
        identity = file_identity({"filename": "track.ogg", "bitRate": 300})
        self.assertTrue(filetype_matches(identity, parse_filetype_config("ogg 256+")))

    def test_opus_192_plus(self):
        identity = file_identity({"filename": "track.opus", "bitRate": 192})
        self.assertTrue(filetype_matches(identity, parse_filetype_config("opus 192+")))

    def test_min_bitrate_no_bitrate(self):
        identity = file_identity({"filename": "track.aac"})
        self.assertFalse(filetype_matches(identity, parse_filetype_config("aac 256+")))

    # --- Bitdepth/samplerate ---

    def test_flac_24_96_matches(self):
        identity = file_identity({"filename": "track.flac", "bitDepth": 24,
                                   "sampleRate": 96000})
        self.assertTrue(filetype_matches(identity, parse_filetype_config("flac 24/96")))

    def test_flac_24_96_wrong_depth(self):
        identity = file_identity({"filename": "track.flac", "bitDepth": 16,
                                   "sampleRate": 96000})
        self.assertFalse(filetype_matches(identity, parse_filetype_config("flac 24/96")))

    def test_flac_16_44_1_matches(self):
        identity = file_identity({"filename": "track.flac", "bitDepth": 16,
                                   "sampleRate": 44100})
        self.assertTrue(filetype_matches(identity, parse_filetype_config("flac 16/44.1")))

    def test_flac_bitdepth_no_metadata(self):
        identity = file_identity({"filename": "track.flac"})
        self.assertFalse(filetype_matches(identity, parse_filetype_config("flac 24/96")))

    def test_flac_24_48_matches(self):
        identity = file_identity({"filename": "track.flac", "bitDepth": 24,
                                   "sampleRate": 48000})
        self.assertTrue(filetype_matches(identity, parse_filetype_config("flac 24/48")))

    def test_flac_24_192_matches(self):
        identity = file_identity({"filename": "track.flac", "bitDepth": 24,
                                   "sampleRate": 192000})
        self.assertTrue(filetype_matches(identity, parse_filetype_config("flac 24/192")))


# ---------------------------------------------------------------------------
# AUDIO_EXTENSIONS and LOSSLESS_CODECS constants
# ---------------------------------------------------------------------------


class TestConstants(unittest.TestCase):

    def test_audio_extensions_includes_m4a(self):
        self.assertIn("m4a", AUDIO_EXTENSIONS)

    def test_audio_extensions_includes_standard(self):
        for ext in ("mp3", "flac", "ogg", "opus", "aac", "m4a", "wma", "wav"):
            self.assertIn(ext, AUDIO_EXTENSIONS)

    def test_audio_extensions_is_frozenset(self):
        self.assertIsInstance(AUDIO_EXTENSIONS, frozenset)

    def test_lossless_codecs(self):
        self.assertEqual(LOSSLESS_CODECS, frozenset({"flac", "alac", "wav"}))


# ---------------------------------------------------------------------------
# config_string round-trip
# ---------------------------------------------------------------------------


class TestConfigString(unittest.TestCase):

    def test_bare_mp3_roundtrip(self):
        self.assertEqual(parse_filetype_config("mp3").config_string, "mp3")

    def test_mp3_v0_roundtrip(self):
        self.assertEqual(parse_filetype_config("mp3 v0").config_string, "mp3 v0")

    def test_flac_24_96_roundtrip(self):
        self.assertEqual(parse_filetype_config("flac 24/96").config_string, "flac 24/96")

    def test_alac_roundtrip(self):
        self.assertEqual(parse_filetype_config("alac").config_string, "alac")

    def test_aac_256_plus_roundtrip(self):
        self.assertEqual(parse_filetype_config("aac 256+").config_string, "aac 256+")


# ---------------------------------------------------------------------------
# verify_filetype bridge — backward compat
# ---------------------------------------------------------------------------


class TestVerifyFiletypeBridge(unittest.TestCase):
    """verify_filetype() still works after rewrite as bridge."""

    def test_alac_m4a_now_matches(self):
        """THE ALAC BUG FIX via bridge."""
        from lib.quality import verify_filetype
        file = {"filename": "track.m4a", "bitRate": 900}
        self.assertTrue(verify_filetype(file, "alac"))

    def test_aac_m4a_matches_aac_config(self):
        from lib.quality import verify_filetype
        file = {"filename": "track.m4a", "bitRate": 256}
        self.assertTrue(verify_filetype(file, "aac 256+"))

    def test_existing_mp3_v0_still_works(self):
        from lib.quality import verify_filetype
        file = {"filename": "track.mp3", "bitRate": 245}
        self.assertTrue(verify_filetype(file, "mp3 v0"))

    def test_existing_flac_still_works(self):
        from lib.quality import verify_filetype
        file = {"filename": "track.flac", "bitRate": 800}
        self.assertTrue(verify_filetype(file, "flac"))

    def test_catch_all_matches_any(self):
        """verify_filetype with '*' matches anything."""
        from lib.quality import verify_filetype
        self.assertTrue(verify_filetype({"filename": "track.mp3", "bitRate": 128}, "*"))
        self.assertTrue(verify_filetype({"filename": "track.flac"}, "*"))
        self.assertTrue(verify_filetype({"filename": "track.m4a", "bitRate": 900}, "*"))
        self.assertTrue(verify_filetype({"filename": "track.ogg", "bitRate": 256}, "*"))


# ---------------------------------------------------------------------------
# Catch-all mode
# ---------------------------------------------------------------------------


class TestCatchAll(unittest.TestCase):
    """CATCH_ALL_SPEC matches any audio file."""

    def test_catch_all_is_star(self):
        self.assertEqual(CATCH_ALL_SPEC.codec, "*")
        self.assertEqual(CATCH_ALL_SPEC.extension, "*")

    def test_parse_star(self):
        spec = parse_filetype_config("*")
        self.assertEqual(spec.codec, "*")

    def test_parse_any(self):
        spec = parse_filetype_config("any")
        self.assertEqual(spec.codec, "*")

    def test_catch_all_matches_mp3(self):
        identity = file_identity({"filename": "track.mp3", "bitRate": 128})
        self.assertTrue(filetype_matches(identity, CATCH_ALL_SPEC))

    def test_catch_all_matches_flac(self):
        identity = file_identity({"filename": "track.flac"})
        self.assertTrue(filetype_matches(identity, CATCH_ALL_SPEC))

    def test_catch_all_matches_m4a(self):
        identity = file_identity({"filename": "track.m4a", "bitRate": 900})
        self.assertTrue(filetype_matches(identity, CATCH_ALL_SPEC))

    def test_catch_all_matches_ogg(self):
        identity = file_identity({"filename": "track.ogg", "bitRate": 256})
        self.assertTrue(filetype_matches(identity, CATCH_ALL_SPEC))

    def test_catch_all_config_string(self):
        self.assertEqual(CATCH_ALL_SPEC.config_string, "*")

    def test_catch_all_not_lossless(self):
        """Catch-all is not lossless (it's a wildcard, not a codec)."""
        self.assertFalse(CATCH_ALL_SPEC.lossless)


if __name__ == "__main__":
    unittest.main()
