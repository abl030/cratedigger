"""Unit tests for lib/matching.py — album_match + check_for_match.

U2 of search-escalation-and-forensics: album_match returns AlbumMatchScore
(structured per-track scores) and check_for_match accumulates a list of
CandidateScore entries — including cheap entries for dirs that fail the
sub-count gate without ever calling album_match.

Strict-accept behaviour (every track above ratio AND _track_titles_cross_check)
is preserved — these tests are the regression guard.
"""

from __future__ import annotations

import unittest
from dataclasses import replace
from typing import Any, cast
from unittest.mock import MagicMock

from cratedigger import SlskdFile, TrackRecord
from lib.config import CratediggerConfig
from lib.context import CratediggerContext
from lib.matching import (
    AlbumMatchScore,
    CandidateScore,
    album_match,
    check_for_match,
)


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------

def _make_cfg(**overrides: Any) -> CratediggerConfig:
    """Build a CratediggerConfig with only the fields matching needs."""
    cfg = CratediggerConfig(
        minimum_match_ratio=0.5,
        ignored_users=(),
        allowed_filetypes=("flac", "mp3"),
        browse_parallelism=4,
    )
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


if __name__ == "__main__":
    unittest.main()
