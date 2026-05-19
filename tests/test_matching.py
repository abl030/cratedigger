"""Unit tests for lib/matching.py — album_match + check_for_match.

U2 of search-escalation-and-forensics: album_match returns AlbumMatchScore
(structured per-track scores) and check_for_match accumulates a list of
CandidateScore entries — including cheap entries for dirs that fail the
sub-count gate without ever calling album_match.

Strict-accept behaviour (every track above ratio AND _track_titles_cross_check)
is preserved — these tests are the regression guard.
"""

from __future__ import annotations

import configparser
import unittest
from dataclasses import replace
from typing import Any, cast
from unittest.mock import MagicMock

from cratedigger import SlskdFile, TrackRecord
from lib.config import CratediggerConfig
from lib.context import CratediggerContext
from lib.matching import (
    AlbumMatchScore,
    album_match,
    album_track_num,
    check_for_match,
)
from lib.peer_cache import PeerCache
from lib.quality import CandidateScore
from tests.test_peer_cache import FakeRedis


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------

def _make_cfg(**overrides: Any) -> CratediggerConfig:
    """Build a CratediggerConfig via from_ini, then apply overrides.

    CLAUDE.md / code-quality.md forbids partial-kwarg construction —
    "Always use CratediggerConfig.from_ini() with the runtime config file.
    Partial configs silently diverge when new config fields are added."
    Per-test field tweaks are applied via dataclasses.replace AFTER from_ini,
    so we still benefit from the loader's defaults.
    """
    ini = configparser.ConfigParser()
    ini["Search Settings"] = {
        "minimum_filename_match_ratio": "0.5",
        "ignored_users": "",
        "allowed_filetypes": "flac, mp3",
        "browse_parallelism": "4",
    }
    cfg = CratediggerConfig.from_ini(ini)
    if overrides:
        cfg = replace(cfg, **overrides)
    return cfg


def _make_album(title: str = "Test Album", artist: str = "Test Artist") -> Any:
    """Build a minimal album-info object with the .title/.artist_name attributes
    that matching code reads off ctx.current_album_cache."""
    album = MagicMock()
    album.title = title
    album.artist_name = artist
    return album


def _make_ctx(
    cfg: CratediggerConfig,
    *,
    album_id: int = 1,
    album_title: str = "Test Album",
    album_artist: str = "Test Artist",
) -> CratediggerContext:
    ctx = CratediggerContext(
        cfg=cfg,
        slskd=MagicMock(),
        pipeline_db_source=MagicMock(),
    )
    ctx.current_album_cache[album_id] = _make_album(album_title, album_artist)
    return ctx


def _track(album_id: int, title: str, medium: int = 1) -> TrackRecord:
    return cast(
        TrackRecord,
        {"albumId": album_id, "title": title, "mediumNumber": medium},
    )


def _file(filename: str, **extra: Any) -> SlskdFile:
    return cast(SlskdFile, {"filename": filename, **extra})


# ---------------------------------------------------------------------------
# album_match — pure function returning AlbumMatchScore
# ---------------------------------------------------------------------------

class TestAlbumMatchScore(unittest.TestCase):
    """Pure-function score computation for every input shape."""

    def setUp(self) -> None:
        self.cfg = _make_cfg()
        self.ctx = _make_ctx(self.cfg, album_id=1, album_title="Cool Album")

    def test_returns_album_match_score_dataclass(self) -> None:
        """album_match must return AlbumMatchScore, NOT bool (RED for U2)."""
        tracks = [_track(1, "Song One")]
        files = [_file("Song One.flac")]
        result = album_match(tracks, files, "user1", "flac", self.ctx)
        self.assertIsInstance(result, AlbumMatchScore)

    def test_happy_path_all_tracks_match(self) -> None:
        """All 3 tracks named exactly → matched=total, avg_ratio≈1.0, no missing."""
        tracks = [_track(1, f"Song {n}") for n in ("One", "Two", "Three")]
        files = [_file(f"Song {n}.flac") for n in ("One", "Two", "Three")]

        score = album_match(tracks, files, "user1", "flac", self.ctx)

        self.assertEqual(score.matched_tracks, 3)
        self.assertEqual(score.total_tracks, 3)
        self.assertAlmostEqual(score.avg_ratio, 1.0, places=2)
        self.assertEqual(score.missing_titles, [])
        self.assertEqual(len(score.best_per_track), 3)
        for ratio in score.best_per_track:
            self.assertAlmostEqual(ratio, 1.0, places=2)

    def test_partial_match_lists_missing_titles(self) -> None:
        """2 of 4 tracks unmatched → matched=2, total=4, missing populated."""
        # Use long, distinctive titles so the .flac suffix doesn't carry the
        # SequenceMatcher ratio above 0.5.
        tracks = [
            _track(1, "Walking In The Rain Tonight"),
            _track(1, "Catching Up With The Sunshine"),
            _track(1, "MissingThirdTrackUnique"),
            _track(1, "MissingFourthTrackUnique"),
        ]
        files = [
            _file("Walking In The Rain Tonight.flac"),
            _file("Catching Up With The Sunshine.flac"),
            _file("zzzz.flac"),
            _file("yyyy.flac"),
        ]
        score = album_match(tracks, files, "user1", "flac", self.ctx)

        self.assertEqual(score.matched_tracks, 2)
        self.assertEqual(score.total_tracks, 4)
        self.assertCountEqual(
            score.missing_titles,
            ["MissingThirdTrackUnique", "MissingFourthTrackUnique"],
        )

    def test_track_number_prefix_separator_logic(self) -> None:
        """`01 - Title.flac` style — check_ratio separator path keeps matches."""
        tracks = [_track(1, "Song One"), _track(1, "Song Two")]
        files = [_file("01 - Song One.flac"), _file("02 - Song Two.flac")]
        score = album_match(tracks, files, "user1", "flac", self.ctx)
        self.assertEqual(score.matched_tracks, 2)
        self.assertEqual(score.total_tracks, 2)

    def test_album_name_prefix_retry_path(self) -> None:
        """`Cool Album - Title.flac` matches via album_name + filename retry."""
        tracks = [_track(1, "Song One")]
        files = [_file("Cool Album - Song One.flac")]
        score = album_match(tracks, files, "user1", "flac", self.ctx)
        self.assertEqual(score.matched_tracks, 1)
        self.assertEqual(score.missing_titles, [])

    def test_catch_all_filetype_uses_inferred_extension(self) -> None:
        """`*` filetype derives extension from each slskd file."""
        tracks = [_track(1, "Song One"), _track(1, "Song Two")]
        files = [_file("Song One.mp3"), _file("Song Two.ogg")]
        score = album_match(tracks, files, "user1", "*", self.ctx)
        self.assertEqual(score.matched_tracks, 2)

    def test_alias_filetype_scores_with_actual_extension(self) -> None:
        """AAC tracks commonly arrive as .m4a; score against that extension."""
        cfg = _make_cfg(minimum_match_ratio=0.6)
        ctx = _make_ctx(cfg, album_id=1, album_title="Cool Album")
        tracks = [_track(1, "A")]
        files = [_file("01 - A.m4a", bitRate=256, bitDepth=0)]

        score = album_match(tracks, files, "user1", "aac", ctx)

        self.assertEqual(score.matched_tracks, 1)
        self.assertEqual(score.missing_titles, [])

    def test_empty_slskd_files_lists_every_expected(self) -> None:
        """Empty slskd_tracks → matched=0, missing lists every title."""
        tracks = [_track(1, "Alpha"), _track(1, "Bravo")]
        files: list[SlskdFile] = []
        score = album_match(tracks, files, "user1", "flac", self.ctx)
        self.assertEqual(score.matched_tracks, 0)
        self.assertEqual(score.total_tracks, 2)
        self.assertCountEqual(score.missing_titles, ["Alpha", "Bravo"])
        # best_per_track has one zero entry per expected track
        self.assertEqual(len(score.best_per_track), 2)
        for ratio in score.best_per_track:
            self.assertEqual(ratio, 0.0)

    def test_ignored_user_does_not_affect_score(self) -> None:
        """ignored_users gate is a check_for_match concern, not a score concern.
        Pure scoring returns the same numbers regardless of who the user is."""
        cfg = _make_cfg(ignored_users=("badguy",))
        ctx = _make_ctx(cfg, album_id=1, album_title="Cool Album")
        tracks = [_track(1, "Song One")]
        files = [_file("Song One.flac")]

        good_score = album_match(tracks, files, "goodguy", "flac", ctx)
        bad_score = album_match(tracks, files, "badguy", "flac", ctx)

        self.assertEqual(good_score.matched_tracks, bad_score.matched_tracks)
        self.assertEqual(good_score.total_tracks, bad_score.total_tracks)
        self.assertEqual(good_score.missing_titles, bad_score.missing_titles)


# ---------------------------------------------------------------------------
# album_track_num — current-tier filetype counting
# ---------------------------------------------------------------------------

class TestAlbumTrackNum(unittest.TestCase):
    def test_current_filetype_gate_ignores_other_allowed_codecs(self) -> None:
        cfg = _make_cfg()
        directory = {
            "directory": "Music\\Album",
            "files": [
                _file("Alpha.flac"),
                _file("Bravo.flac"),
            ],
        }

        result = album_track_num(cast(Any, directory), cfg, allowed_filetype="mp3")

        self.assertEqual(result["count"], 0)
        self.assertEqual(result["filetype"], "")

    def test_current_aac_filetype_counts_aac_m4a_files(self) -> None:
        cfg = _make_cfg()
        directory = {
            "directory": "Music\\Album",
            "files": [
                _file("Alpha.m4a", bitRate=256, bitDepth=0),
                _file("Bravo.m4a", bitRate=256, bitDepth=0),
            ],
        }

        result = album_track_num(cast(Any, directory), cfg, allowed_filetype="aac")

        self.assertEqual(result["count"], 2)
        self.assertEqual(result["filetype"], "aac")


# ---------------------------------------------------------------------------
# check_for_match — accumulates CandidateScore for every iterated dir
# ---------------------------------------------------------------------------

class TestCheckForMatchCandidateAccumulation(unittest.TestCase):
    """check_for_match returns the per-dir CandidateScore list as the 4th element
    (or .candidates on a MatchResult dataclass)."""

    def setUp(self) -> None:
        self.cfg = _make_cfg()
        self.ctx = _make_ctx(self.cfg, album_id=1, album_title="Cool Album")
        self.username = "user1"
        self.tracks = [
            _track(1, "Alpha"),
            _track(1, "Bravo"),
            _track(1, "Charlie"),
        ]

    def _set_browse(self, file_dir: str, files: list[SlskdFile]) -> None:
        """Pre-populate folder_cache so the real browse never runs."""
        self.ctx.folder_cache.setdefault(self.username, {})[file_dir] = {
            "directory": file_dir,
            "files": files,
        }

    @staticmethod
    def _candidates(result: Any) -> list[CandidateScore]:
        """Pull candidate list off either tuple or dataclass return shape."""
        if hasattr(result, "candidates"):
            return result.candidates
        # tuple shape (matched, directory, file_dir, candidates)
        return result[3]

    @staticmethod
    def _matched(result: Any) -> bool:
        return result.matched if hasattr(result, "matched") else result[0]

    @staticmethod
    def _file_dir(result: Any) -> str:
        return result.file_dir if hasattr(result, "file_dir") else result[2]

    def test_strict_accept_first_dir_short_circuits(self) -> None:
        """First dir matches strictly → returns True immediately, candidates has
        exactly one entry."""
        self._set_browse("dirA", [
            _file("Alpha.flac"), _file("Bravo.flac"), _file("Charlie.flac"),
        ])
        result = check_for_match(
            self.tracks, "flac", ["dirA"], self.username, self.ctx,
        )
        self.assertTrue(self._matched(result))
        self.assertEqual(self._file_dir(result), "dirA")
        candidates = self._candidates(result)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].dir, "dirA")
        self.assertEqual(candidates[0].matched_tracks, 3)
        self.assertEqual(candidates[0].total_tracks, 3)
        self.assertEqual(candidates[0].file_count, 3)
        self.assertEqual(candidates[0].filetype, "flac")
        self.assertEqual(candidates[0].username, self.username)

    def test_mixed_format_dir_scores_only_current_filetype(self) -> None:
        tracks = [
            _track(1, "Alpha Song Distinct"),
            _track(1, "Bravo Song Distinct"),
        ]
        self._set_browse("dirMixed", [
            _file("Alpha Song Distinct.flac"),
            _file("Bravo Song Distinct.flac"),
            _file("01.mp3"),
            _file("02.mp3"),
        ])

        result = check_for_match(
            tracks, "mp3", ["dirMixed"], self.username, self.ctx,
        )

        self.assertFalse(self._matched(result))
        candidates = self._candidates(result)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].file_count, 2)
        self.assertEqual(candidates[0].matched_tracks, 0)

    def test_mixed_format_search_count_does_not_skip_current_tier(self) -> None:
        tracks = [
            _track(1, "Alpha Song Distinct"),
            _track(1, "Bravo Song Distinct"),
        ]
        self.ctx.search_dir_audio_count = {
            self.username: {"dirMixed": 6},
        }
        self.ctx.search_cache = {
            1: {
                self.username: {
                    "flac": ["dirMixed"],
                    "mp3": ["dirMixed"],
                },
            },
        }
        self._set_browse("dirMixed", [
            _file("Alpha Song Distinct.flac"),
            _file("Bravo Song Distinct.flac"),
            _file("01.mp3"),
            _file("02.mp3"),
            _file("03.mp3"),
            _file("04.mp3"),
        ])

        result = check_for_match(
            tracks, "flac", ["dirMixed"], self.username, self.ctx,
        )

        self.assertTrue(self._matched(result))
        candidates = self._candidates(result)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].file_count, 2)
        self.assertNotIn(
            (self.username, "dirMixed", 2, "flac"),
            self.ctx.negative_matches,
        )

    def test_mixed_format_match_directory_excludes_wrong_codec_audio(self) -> None:
        tracks = [
            _track(1, "Alpha Song Distinct"),
            _track(1, "Bravo Song Distinct"),
        ]
        self._set_browse("dirMixed", [
            _file("Alpha Song Distinct.flac"),
            _file("Bravo Song Distinct.flac"),
            _file("Alpha Song Distinct.mp3"),
            _file("Bravo Song Distinct.mp3"),
            _file("cover.jpg"),
        ])

        result = check_for_match(
            tracks, "flac", ["dirMixed"], self.username, self.ctx,
        )

        self.assertTrue(self._matched(result))
        self.assertEqual(
            [file["filename"] for file in result.directory["files"]],
            [
                "Alpha Song Distinct.flac",
                "Bravo Song Distinct.flac",
                "cover.jpg",
            ],
        )

    def test_alias_filetypes_still_use_search_count_prefilter(self) -> None:
        tracks = [
            _track(1, "Alpha Song Distinct"),
            _track(1, "Bravo Song Distinct"),
        ]
        self.ctx.search_dir_audio_count = {
            self.username: {"dirLong": 30},
        }
        self.ctx.search_cache = {
            1: {
                self.username: {
                    "mp3 v0": ["dirLong"],
                    "mp3": ["dirLong"],
                },
            },
        }

        result = check_for_match(
            tracks, "mp3 v0", ["dirLong"], self.username, self.ctx,
        )

        self.assertFalse(self._matched(result))
        # U2 of search-plan-entropy: pre-filter skips now emit a flagged
        # sample CandidateScore for forensic visibility (one per skip up
        # to ``PRE_FILTER_SKIP_SAMPLE_CAP``).
        candidates = self._candidates(result)
        self.assertEqual(len(candidates), 1)
        self.assertTrue(candidates[0].pre_filter_skip)
        self.assertEqual(candidates[0].file_count, 30)
        self.assertEqual(candidates[0].matched_tracks, 0)
        self.assertEqual(result.pre_filter_skip_count, 1)
        self.assertIn(
            (self.username, "dirLong", 2, "mp3 v0"),
            self.ctx.negative_matches,
        )
        self.assertNotIn(self.username, self.ctx.folder_cache)

    def test_subcount_dir_gets_cheap_candidate_entry(self) -> None:
        """Dir with file_count != track_num emits a cheap CandidateScore
        (matched=0, missing=[], avg_ratio=0.0) — album_match is NOT called."""
        # dirA has only 2 of 3 audio files
        self._set_browse("dirA", [
            _file("Alpha.flac"), _file("Bravo.flac"),
        ])
        # dirB has all 3 — strict accept here
        self._set_browse("dirB", [
            _file("Alpha.flac"), _file("Bravo.flac"), _file("Charlie.flac"),
        ])
        result = check_for_match(
            self.tracks, "flac", ["dirA", "dirB"], self.username, self.ctx,
        )
        self.assertTrue(self._matched(result))
        candidates = self._candidates(result)
        # Both dirs should be in candidates, in iteration order
        self.assertEqual(len(candidates), 2)
        # rank_candidate_dirs may reorder; find by dir name
        by_dir = {c.dir: c for c in candidates}
        cheap = by_dir["dirA"]
        self.assertEqual(cheap.matched_tracks, 0)
        self.assertEqual(cheap.total_tracks, 3)
        self.assertEqual(cheap.avg_ratio, 0.0)
        self.assertEqual(cheap.missing_titles, [])
        self.assertEqual(cheap.file_count, 2)

    def test_dir_with_no_audio_files_emits_zero_filecount_candidate(self) -> None:
        """Dir with `tracks_info["filetype"]==""` (no audio) → cheap candidate
        with file_count=0."""
        self._set_browse("dirEmpty", [_file("cover.jpg"), _file("readme.txt")])
        result = check_for_match(
            self.tracks, "flac", ["dirEmpty"], self.username, self.ctx,
        )
        self.assertFalse(self._matched(result))
        candidates = self._candidates(result)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].file_count, 0)
        self.assertEqual(candidates[0].matched_tracks, 0)

    def test_cross_check_failure_continues_loop_and_records_score(self) -> None:
        """When _track_titles_cross_check fails, dir is added to negative_matches
        and the score is still appended to candidates so the forensic record
        captures the cross-check rejection."""
        # Files match the count gate but titles are wildly different — cross-check
        # tolerates 1/5 mismatch but here every title is wrong, so cross-check fails.
        # However, album_match's filename-ratio path happily accepts these because
        # of separator/album-name retries... we need filenames that pass album_match
        # filename ratio but fail _track_titles_cross_check. The cross-check uses
        # _normalize_title + extracted-title fuzzy matching with 0.5 threshold.
        # Easiest construction: tracks all named distinctly, files use those titles
        # *embedded* but with very different content elsewhere.
        # In practice, the strict-accept path is: all tracks above ratio AND
        # cross-check passes. If the filenames are exact title matches, cross-check
        # also passes — they're correlated. To force cross-check failure we need
        # the album_match per-track ratios to clear minimum_match_ratio while the
        # extracted-title fuzzy check fails.
        # We achieve this with a low minimum_match_ratio so the strict accept path
        # triggers, but slskd filenames are very long so extracted_title differs.
        cfg = _make_cfg(minimum_match_ratio=0.1)
        ctx = _make_ctx(cfg, album_id=1, album_title="Cool Album")
        ctx.folder_cache.setdefault(self.username, {})["dirX"] = {
            "directory": "dirX",
            "files": [
                _file("Alpha.flac"),
                _file("Bravo.flac"),
                _file("Charlie.flac"),
            ],
        }
        # Make tracks barely passing the filename ratio (>=0.1) but with names
        # that differ enough for cross-check.
        tracks = [
            _track(1, "Alpha"),
            _track(1, "Bravo"),
            _track(1, "Charlie"),
        ]
        # With identical filenames and titles, both will accept. To force cross-
        # check failure we need filenames that pass album_match's per-track filename
        # ratio but produce extracted titles that fail _track_titles_cross_check.
        # In practice this means matching is tight — skip this case if cross-check
        # ends up succeeding. The contract we care about: IF cross-check fails,
        # THEN candidate is still appended. We test that contract by mocking.
        from unittest.mock import patch
        with patch("lib.matching._track_titles_cross_check", return_value=False):
            result = check_for_match(
                tracks, "flac", ["dirX"], self.username, ctx,
            )
        self.assertFalse(self._matched(result))
        candidates = self._candidates(result)
        self.assertEqual(len(candidates), 1)
        # The score still reflects a strict filename match
        self.assertEqual(candidates[0].matched_tracks, 3)
        self.assertEqual(candidates[0].total_tracks, 3)
        self.assertEqual(candidates[0].file_count, 3)
        # And the dir was added to negative_matches
        self.assertIn(
            (self.username, "dirX", 3, "flac"),
            ctx.negative_matches,
        )

    def test_no_dirs_returns_empty_candidates(self) -> None:
        """No dirs to try → empty candidate list, no match."""
        result = check_for_match(
            self.tracks, "flac", [], self.username, self.ctx,
        )
        self.assertFalse(self._matched(result))
        self.assertEqual(self._candidates(result), [])

    def test_broken_user_returns_empty_candidates(self) -> None:
        """Broken-user short-circuit returns empty candidates."""
        self.ctx.broken_user.append(self.username)
        result = check_for_match(
            self.tracks, "flac", ["dirA"], self.username, self.ctx,
        )
        self.assertFalse(self._matched(result))
        self.assertEqual(self._candidates(result), [])

    def test_redis_negative_skip_does_not_mark_user_broken(self) -> None:
        redis = FakeRedis()
        peer_cache = PeerCache(redis, ttl_seconds=60, speed_ttl_seconds=10)
        peer_cache.set_negative(self.username, "dirA")
        self.ctx.peer_cache = peer_cache

        result = check_for_match(
            self.tracks, "flac", ["dirA"], self.username, self.ctx,
        )

        self.assertFalse(self._matched(result))
        self.assertEqual(self._candidates(result), [])
        self.assertEqual(self.ctx.broken_user, [])
        self.assertNotIn((self.username, "dirA", 3, "flac"), self.ctx.negative_matches)
        self.assertEqual(self.ctx.peers_browsed_lazy, 0)
        self.assertEqual(self.ctx.cache_neg_hits, 1)

    def test_recorded_redis_negative_skip_avoids_second_lookup(self) -> None:
        redis = FakeRedis()
        peer_cache = PeerCache(redis, ttl_seconds=60, speed_ttl_seconds=10)
        peer_cache.set_negative(self.username, "dirA")
        self.ctx.peer_cache = peer_cache
        self.ctx.peer_cache_negative_skips.add((self.username, "dirA"))

        result = check_for_match(
            self.tracks, "flac", ["dirA"], self.username, self.ctx,
        )

        self.assertFalse(self._matched(result))
        self.assertEqual(redis.get_calls, 0)
        self.assertEqual(self.ctx.cache_neg_hits, 0)
        self.assertEqual(self.ctx.peers_browsed_lazy, 0)

    def test_partial_match_dir_records_partial_score(self) -> None:
        """Dir with full file_count but only 2 of 3 titles matching →
        full CandidateScore with matched_tracks=2, total_tracks=3, and
        missing_titles populated. Strict accept fails (2 != 3)."""
        # Use distinctive long names so the `.flac` suffix doesn't carry the
        # SequenceMatcher ratio above the minimum_match_ratio threshold.
        partial_tracks = [
            _track(1, "First Track Distinct Name"),
            _track(1, "Second Track Distinct Name"),
            _track(1, "QQQQQQQQQQQ"),
        ]
        self._set_browse("dirA", [
            _file("First Track Distinct Name.flac"),
            _file("Second Track Distinct Name.flac"),
            _file("XXXXXXXXXXXXX.flac"),
        ])
        result = check_for_match(
            partial_tracks, "flac", ["dirA"], self.username, self.ctx,
        )
        self.assertFalse(self._matched(result))
        candidates = self._candidates(result)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].matched_tracks, 2)
        self.assertEqual(candidates[0].total_tracks, 3)
        self.assertCountEqual(
            candidates[0].missing_titles, ["QQQQQQQQQQQ"],
        )
        self.assertEqual(candidates[0].file_count, 3)


# ---------------------------------------------------------------------------
# Asymmetric pre-filter (U1 of search-plan-entropy plan)
#
# The pre-filter rule is `search_count > 2 * track_num` — junk-dir guard only,
# not a tight-fit filter. This lets track-fallback queries (search_count=1
# vs track_num=4) reach `album_track_num` so the dir can be browsed and
# scored. The codec guard (`cached_codecs <= 1`) still bypasses the
# pre-filter so multi-codec dirs are never skipped on count alone.
# ---------------------------------------------------------------------------

class TestCheckForMatchAsymmetricPreFilter(unittest.TestCase):
    """The pre-filter only skips dirs whose audio count exceeds 2 * track_num.

    Track-fallback queries (one track in the search response) MUST reach the
    browse + score path; the bidirectional `abs() > 2` rule used to silently
    skip them.
    """

    def setUp(self) -> None:
        self.cfg = _make_cfg()
        self.ctx = _make_ctx(self.cfg, album_id=1, album_title="Cool Album")
        self.username = "user1"
        # 4-track album — the canonical scenario from the plan.
        self.tracks = [
            _track(1, "Alpha"),
            _track(1, "Bravo"),
            _track(1, "Charlie"),
            _track(1, "Delta"),
        ]
        self.full_files = [
            _file("Alpha.flac"),
            _file("Bravo.flac"),
            _file("Charlie.flac"),
            _file("Delta.flac"),
        ]

    def _set_browse(self, file_dir: str, files: list[SlskdFile]) -> None:
        self.ctx.folder_cache.setdefault(self.username, {})[file_dir] = {
            "directory": file_dir,
            "files": files,
        }

    def _set_search_count(self, file_dir: str, count: int) -> None:
        """Populate search_dir_audio_count and search_cache so the pre-filter
        actually consults the cached count for this dir."""
        self.ctx.search_dir_audio_count = {
            self.username: {file_dir: count},
        }
        self.ctx.search_cache = {
            1: {
                self.username: {
                    "flac": [file_dir],
                },
            },
        }

    @staticmethod
    def _matched(result: Any) -> bool:
        return result.matched if hasattr(result, "matched") else result[0]

    @staticmethod
    def _candidates(result: Any) -> list[CandidateScore]:
        if hasattr(result, "candidates"):
            return result.candidates
        return result[3]

    def test_search_count_equals_track_num_passes_prefilter(self) -> None:
        """Happy path: search_count=4, track_num=4 — passes pre-filter,
        browse + score proceeds, dir matches."""
        self._set_search_count("dirA", 4)
        self._set_browse("dirA", self.full_files)

        result = check_for_match(
            self.tracks, "flac", ["dirA"], self.username, self.ctx,
        )

        self.assertTrue(self._matched(result))
        self.assertNotIn(
            (self.username, "dirA", 4, "flac"),
            self.ctx.negative_matches,
        )

    def test_track_fallback_search_count_one_passes_prefilter(self) -> None:
        """The bug fix: track-fallback queries return one file but the album
        has many tracks. Under the old `abs() > 2` rule this was skipped;
        under the new `2n` rule it must reach the browse + score path."""
        self._set_search_count("dirA", 1)
        # Files match the full album — the dir on the peer IS the album,
        # the search response just only had one file in it.
        self._set_browse("dirA", self.full_files)

        result = check_for_match(
            self.tracks, "flac", ["dirA"], self.username, self.ctx,
        )

        self.assertTrue(self._matched(result))
        self.assertNotIn(
            (self.username, "dirA", 4, "flac"),
            self.ctx.negative_matches,
        )

    def test_search_count_at_2n_boundary_passes_prefilter(self) -> None:
        """Boundary: search_count=8, track_num=4 — `8 > 8` is false, so the
        pre-filter does NOT skip. The dir is browsed; the post-browse strict
        -count gate at line 451 then writes its own negative_matches entry
        because 8 != 4. We verify the pre-filter passed by checking that
        a CandidateScore was recorded (pre-filter skips produce zero
        candidates; the strict-count gate produces a cheap one)."""
        self._set_search_count("dirA", 8)
        # Dir actually contains the 4 tracks plus 4 extras — the post-browse
        # strict-count gate will fail (8 != 4), but the pre-filter must pass.
        self._set_browse("dirA", self.full_files + [
            _file("Bonus1.flac"),
            _file("Bonus2.flac"),
            _file("Bonus3.flac"),
            _file("Bonus4.flac"),
        ])

        result = check_for_match(
            self.tracks, "flac", ["dirA"], self.username, self.ctx,
        )

        # Pre-filter did NOT skip — at least one candidate was recorded.
        # (A pre-filter skip produces zero candidates per
        # test_search_count_just_over_2n_is_skipped.)
        candidates = self._candidates(result)
        self.assertEqual(len(candidates), 1)
        # The candidate is the cheap one from the strict-count gate
        # (file_count=8 != track_num=4 → matched_tracks=0).
        self.assertEqual(candidates[0].file_count, 8)
        self.assertEqual(candidates[0].matched_tracks, 0)

    def test_search_count_just_over_2n_is_skipped(self) -> None:
        """Edge case: search_count=9, track_num=4 — `9 > 8`, dir is skipped,
        negative-cache entry written, U2 emits a flagged skip sample."""
        self._set_search_count("dirA", 9)

        result = check_for_match(
            self.tracks, "flac", ["dirA"], self.username, self.ctx,
        )

        self.assertFalse(self._matched(result))
        # U2 of search-plan-entropy: pre-filter skip is recorded as both
        # a scalar counter and a flagged sample CandidateScore row.
        candidates = self._candidates(result)
        self.assertEqual(len(candidates), 1)
        self.assertTrue(candidates[0].pre_filter_skip)
        self.assertEqual(candidates[0].file_count, 9)
        self.assertEqual(candidates[0].matched_tracks, 0)
        self.assertEqual(result.pre_filter_skip_count, 1)
        self.assertIn(
            (self.username, "dirA", 4, "flac"),
            self.ctx.negative_matches,
        )
        # Pre-filter rejected without ever browsing this user.
        self.assertNotIn(self.username, self.ctx.folder_cache)

    def test_junk_dir_well_over_2n_is_skipped(self) -> None:
        """Junk-dir case: search_count=50, track_num=4 — large dump folder.
        Pre-filter must continue to catch this case (and U2 records it)."""
        self._set_search_count("dirJunk", 50)

        result = check_for_match(
            self.tracks, "flac", ["dirJunk"], self.username, self.ctx,
        )

        self.assertFalse(self._matched(result))
        candidates = self._candidates(result)
        self.assertEqual(len(candidates), 1)
        self.assertTrue(candidates[0].pre_filter_skip)
        self.assertEqual(candidates[0].file_count, 50)
        self.assertEqual(result.pre_filter_skip_count, 1)
        self.assertIn(
            (self.username, "dirJunk", 4, "flac"),
            self.ctx.negative_matches,
        )
        self.assertNotIn(self.username, self.ctx.folder_cache)

    def test_codec_guard_bypasses_prefilter_on_large_dir(self) -> None:
        """Codec-guard case: search_count=50, track_num=4 BUT the dir is
        present in the search_cache under multiple concrete codecs — the
        `len(cached_codecs) <= 1` guard bypasses the pre-filter so the
        multi-codec dir is browsed and scored."""
        # Multi-codec presence: both "flac" and "mp3" tiers contain dirMulti.
        self.ctx.search_dir_audio_count = {
            self.username: {"dirMulti": 50},
        }
        self.ctx.search_cache = {
            1: {
                self.username: {
                    "flac": ["dirMulti"],
                    "mp3": ["dirMulti"],
                },
            },
        }
        self._set_browse("dirMulti", self.full_files)

        result = check_for_match(
            self.tracks, "flac", ["dirMulti"], self.username, self.ctx,
        )

        # Pre-filter did NOT add a negative_matches entry — codec guard fired.
        self.assertNotIn(
            (self.username, "dirMulti", 4, "flac"),
            self.ctx.negative_matches,
        )
        # And the dir was actually browsed + scored.
        self.assertTrue(self._matched(result))


# ---------------------------------------------------------------------------
# U2 of search-plan-entropy: pre-filter skip telemetry.
#
# Asserts the contract: ``check_for_match`` returns
# ``MatchResult.pre_filter_skip_count`` as the authoritative scalar count
# of dirs the asymmetric pre-filter rejected, and emits up to
# ``PRE_FILTER_SKIP_SAMPLE_CAP`` flagged ``CandidateScore`` sample rows
# inside the ``candidates`` list for forensic visibility.
# ---------------------------------------------------------------------------

class TestCheckForMatchPreFilterSkipTelemetry(unittest.TestCase):
    """U2 contract: ``pre_filter_skip_count`` aggregates ALL skips,
    sample rows are capped, and happy paths emit zero skip telemetry."""

    def setUp(self) -> None:
        self.cfg = _make_cfg()
        self.ctx = _make_ctx(self.cfg, album_id=1, album_title="Cool Album")
        self.username = "user1"
        self.tracks = [
            _track(1, "Alpha"),
            _track(1, "Bravo"),
            _track(1, "Charlie"),
            _track(1, "Delta"),
        ]
        self.full_files = [
            _file("Alpha.flac"),
            _file("Bravo.flac"),
            _file("Charlie.flac"),
            _file("Delta.flac"),
        ]

    def _set_browse(self, file_dir: str, files: list[SlskdFile]) -> None:
        self.ctx.folder_cache.setdefault(self.username, {})[file_dir] = {
            "directory": file_dir,
            "files": files,
        }

    def test_happy_path_no_skips_zero_count_no_samples(self) -> None:
        """All dirs pass pre-filter → count=0, no flagged samples."""
        # Single dir, search_count == track_num: passes pre-filter.
        self.ctx.search_dir_audio_count = {self.username: {"dirA": 4}}
        self.ctx.search_cache = {
            1: {self.username: {"flac": ["dirA"]}},
        }
        self._set_browse("dirA", self.full_files)

        result = check_for_match(
            self.tracks, "flac", ["dirA"], self.username, self.ctx,
        )

        self.assertEqual(result.pre_filter_skip_count, 0)
        # The one CandidateScore is from the post-browse match path,
        # NOT a pre-filter skip sample.
        self.assertEqual(len(result.candidates), 1)
        self.assertFalse(result.candidates[0].pre_filter_skip)

    def test_skip_count_above_sample_cap_truncates_samples(self) -> None:
        """17 skipped dirs → count=17 but only ``PRE_FILTER_SKIP_SAMPLE_CAP``
        flagged sample rows. The scalar count is authoritative, the
        candidates list is bounded so the JSONB blob stays small even for
        pathological junk-peer searches."""
        from lib.matching import PRE_FILTER_SKIP_SAMPLE_CAP

        # 17 dirs, all over the 2*track_num=8 threshold.
        dir_names = [f"dir{i}" for i in range(17)]
        self.ctx.search_dir_audio_count = {
            self.username: {d: 100 for d in dir_names},
        }
        self.ctx.search_cache = {
            1: {self.username: {"flac": dir_names}},
        }

        result = check_for_match(
            self.tracks, "flac", dir_names, self.username, self.ctx,
        )

        self.assertFalse(result.matched)
        # Authoritative count: every skip is counted exactly once.
        self.assertEqual(result.pre_filter_skip_count, 17)
        # Sample rows: bounded by the cap. All entries are flagged.
        self.assertEqual(len(result.candidates), PRE_FILTER_SKIP_SAMPLE_CAP)
        self.assertTrue(all(c.pre_filter_skip for c in result.candidates))
        # Each sample row carries the cached file_count from the search
        # response so operators can see the noisy peer's profile.
        self.assertTrue(all(c.file_count == 100 for c in result.candidates))

    def test_skip_count_exactly_at_cap_emits_all_samples(self) -> None:
        """Exactly ``PRE_FILTER_SKIP_SAMPLE_CAP`` skips → count and samples
        match. Boundary check that the cap is inclusive."""
        from lib.matching import PRE_FILTER_SKIP_SAMPLE_CAP

        dir_names = [f"dir{i}" for i in range(PRE_FILTER_SKIP_SAMPLE_CAP)]
        self.ctx.search_dir_audio_count = {
            self.username: {d: 100 for d in dir_names},
        }
        self.ctx.search_cache = {
            1: {self.username: {"flac": dir_names}},
        }

        result = check_for_match(
            self.tracks, "flac", dir_names, self.username, self.ctx,
        )

        self.assertEqual(result.pre_filter_skip_count, PRE_FILTER_SKIP_SAMPLE_CAP)
        self.assertEqual(len(result.candidates), PRE_FILTER_SKIP_SAMPLE_CAP)
        self.assertTrue(all(c.pre_filter_skip for c in result.candidates))


class TestCandidateScorePreFilterRoundTrip(unittest.TestCase):
    """Wire-boundary parity: a flagged ``CandidateScore`` survives msgspec
    encode → decode. This is the regression guard for the JSONB blob.

    Per ``.claude/rules/code-quality.md`` § Wire-boundary types: every
    Struct field needs at least one RED-style assertion that it survives
    the round-trip with the value the producer would have written.
    """

    def test_pre_filter_skip_true_survives_json_round_trip(self) -> None:
        import msgspec

        flagged = CandidateScore(
            username="alice",
            dir="Music/Albums/Foo",
            filetype="flac",
            matched_tracks=0,
            total_tracks=12,
            avg_ratio=0.0,
            missing_titles=[],
            file_count=200,
            pre_filter_skip=True,
        )
        encoded = msgspec.json.encode(flagged)
        decoded = msgspec.json.decode(encoded, type=CandidateScore)
        self.assertTrue(decoded.pre_filter_skip)
        self.assertEqual(decoded.file_count, 200)
        self.assertEqual(decoded.username, "alice")

    def test_pre_filter_skip_default_false_when_field_omitted(self) -> None:
        """Historic JSONB blobs written before U2 do not carry the new
        field. Decoding must tolerate the omission and default to False."""
        import msgspec

        # Legacy on-wire shape: no pre_filter_skip key.
        legacy_json = (
            b'{"username":"bob","dir":"x","filetype":"flac",'
            b'"matched_tracks":4,"total_tracks":4,"avg_ratio":0.95,'
            b'"missing_titles":[],"file_count":4}'
        )
        decoded = msgspec.json.decode(legacy_json, type=CandidateScore)
        self.assertFalse(decoded.pre_filter_skip)
        self.assertEqual(decoded.matched_tracks, 4)


if __name__ == "__main__":
    unittest.main()
