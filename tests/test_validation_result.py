#!/usr/bin/env python3
"""Tests for ValidationResult + CandidateSummary dataclasses.

RED/GREEN TDD — tests written before implementation.
"""

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.quality import ValidationResult, CandidateSummary


# ============================================================================
# CandidateSummary
# ============================================================================

class TestCandidateSummary(unittest.TestCase):

    def test_defaults(self) -> None:
        c = CandidateSummary()
        self.assertEqual(c.mbid, "")
        self.assertEqual(c.artist, "")
        self.assertEqual(c.album, "")
        self.assertEqual(c.distance, 0.0)
        self.assertEqual(c.track_count, 0)
        self.assertIsNone(c.year)
        self.assertIsNone(c.country)
        self.assertEqual(c.extra_tracks, 0)
        self.assertEqual(c.extra_items, 0)
        self.assertFalse(c.is_target)

    def test_full_construction(self) -> None:
        c = CandidateSummary(
            mbid="abc-123", artist="Ye", album="BULLY",
            distance=0.45, track_count=12, year=2025,
            country="US", extra_tracks=2, extra_items=0,
            is_target=True,
        )
        self.assertEqual(c.mbid, "abc-123")
        self.assertTrue(c.is_target)
        self.assertEqual(c.extra_tracks, 2)

    def test_from_harness_candidate(self) -> None:
        """Simulate constructing from beets harness candidate dict."""
        harness_cand = {
            "album_id": "abc-123", "artist": "Blue Cheer",
            "album": "Vincebus Eruptum", "distance": 0.02,
            "track_count": 6, "year": 1968, "country": "US",
            "label": "Philips", "mediums": 1,
            "albumtype": "album", "albumstatus": "Official",
            "extra_tracks": 0, "extra_items": 0,
            "tracks": [
                {"title": "Summertime Blues", "length": 213.4, "track_id": "t1"},
                {"title": "Rock Me Baby", "length": 244.1, "track_id": "t2"},
            ],
        }
        c = CandidateSummary(
            mbid=harness_cand["album_id"],
            artist=harness_cand["artist"],
            album=harness_cand["album"],
            distance=harness_cand["distance"],
            track_count=harness_cand["track_count"],
            year=harness_cand["year"],
            country=harness_cand["country"],
            label=harness_cand["label"],
            mediums=harness_cand["mediums"],
            albumtype=harness_cand["albumtype"],
            albumstatus=harness_cand["albumstatus"],
            extra_tracks=harness_cand["extra_tracks"],
            extra_items=harness_cand["extra_items"],
            tracks=harness_cand["tracks"],
        )
        self.assertEqual(c.mbid, "abc-123")
        self.assertEqual(c.distance, 0.02)
        self.assertEqual(c.label, "Philips")
        self.assertEqual(len(c.tracks), 2)
        self.assertEqual(c.tracks[0]["title"], "Summertime Blues")

    def test_distance_breakdown(self) -> None:
        """CandidateSummary stores per-component distance weights."""
        c = CandidateSummary(
            mbid="abc-123", distance=0.49,
            distance_breakdown={
                "album": 0.0, "artist": 0.0, "tracks": 0.0,
                "media": 0.25, "source": 0.15, "year": 0.09,
            },
        )
        self.assertEqual(c.distance_breakdown["media"], 0.25)
        self.assertEqual(c.distance_breakdown["source"], 0.15)
        self.assertNotIn("missing_tracks", c.distance_breakdown)

    def test_distance_breakdown_survives_json_round_trip(self) -> None:
        """Distance breakdown serializes through ValidationResult."""
        vr = ValidationResult(
            candidates=[CandidateSummary(
                mbid="abc", distance=0.49, is_target=True,
                distance_breakdown={"album": 0.0, "media": 0.25, "tracks": 0.15},
            )],
        )
        j = vr.to_json()
        vr2 = ValidationResult.from_json(j)
        bd = vr2.candidates[0].distance_breakdown
        self.assertEqual(bd["media"], 0.25)
        self.assertEqual(bd["tracks"], 0.15)

    def test_distance_breakdown_default_empty(self) -> None:
        c = CandidateSummary()
        self.assertEqual(c.distance_breakdown, {})

    def test_tracks_survive_json_round_trip(self) -> None:
        """Track lists serialize and deserialize through ValidationResult."""
        vr = ValidationResult(
            candidates=[CandidateSummary(
                mbid="abc", distance=0.02, is_target=True,
                tracks=[
                    {"title": "Track 1", "length": 180.0, "track_id": "t1"},
                    {"title": "Track 2", "length": 200.0, "track_id": "t2"},
                ],
            )],
        )
        j = vr.to_json()
        vr2 = ValidationResult.from_json(j)
        self.assertEqual(len(vr2.candidates[0].tracks), 2)
        self.assertEqual(vr2.candidates[0].tracks[1]["title"], "Track 2")


# ============================================================================
# ValidationResult construction
# ============================================================================

class TestValidationResultConstruction(unittest.TestCase):

    def test_defaults(self) -> None:
        vr = ValidationResult()
        self.assertFalse(vr.valid)
        self.assertIsNone(vr.distance)
        self.assertIsNone(vr.scenario)
        self.assertIsNone(vr.detail)
        self.assertFalse(vr.mbid_found)
        self.assertIsNone(vr.target_mbid)
        self.assertEqual(vr.candidate_count, 0)
        self.assertEqual(vr.candidates, [])
        self.assertIsNone(vr.local_track_count)
        self.assertIsNone(vr.soulseek_username)
        self.assertIsNone(vr.download_folder)
        self.assertIsNone(vr.failed_path)
        self.assertEqual(vr.denylisted_users, [])
        self.assertEqual(vr.corrupt_files, [])
        self.assertIsNone(vr.error)

    def test_strong_match(self) -> None:
        vr = ValidationResult(
            valid=True, distance=0.02, scenario="strong_match",
            detail="distance=0.02", mbid_found=True,
            target_mbid="abc-123", candidate_count=3,
            candidates=[
                CandidateSummary(mbid="abc-123", distance=0.02, is_target=True),
                CandidateSummary(mbid="def-456", distance=0.15),
                CandidateSummary(mbid="ghi-789", distance=0.45),
            ],
        )
        self.assertTrue(vr.valid)
        self.assertEqual(len(vr.candidates), 3)
        self.assertTrue(vr.candidates[0].is_target)

    def test_high_distance(self) -> None:
        vr = ValidationResult(
            valid=False, distance=0.45, scenario="high_distance",
            detail="distance=0.45, target had 12 tracks, local had 10",
            mbid_found=True, target_mbid="abc-123",
            candidate_count=5, local_track_count=10,
        )
        self.assertFalse(vr.valid)
        self.assertEqual(vr.scenario, "high_distance")
        self.assertEqual(vr.local_track_count, 10)

    def test_audio_corrupt(self) -> None:
        vr = ValidationResult(
            valid=False, scenario="audio_corrupt",
            detail="2 of 12 files failed ffmpeg integrity check",
            corrupt_files=["03 - Bad Track.mp3", "07 - Also Bad.mp3"],
            failed_path="/mnt/virtio/Music/Failed/Artist/Album",
        )
        self.assertEqual(vr.scenario, "audio_corrupt")
        self.assertEqual(len(vr.corrupt_files), 2)
        self.assertIn("03 - Bad Track.mp3", vr.corrupt_files)
        self.assertIsNotNone(vr.failed_path)

    def test_source_info(self) -> None:
        """Source info populated by soularr.py after beets_validate returns."""
        vr = ValidationResult(
            valid=False, scenario="high_distance",
            soulseek_username="baduser123",
            download_folder="/mnt/virtio/music/slskd/Artist - Album",
            failed_path="/mnt/virtio/Music/Failed/Artist/Album",
            denylisted_users=["baduser123"],
        )
        self.assertEqual(vr.soulseek_username, "baduser123")
        self.assertEqual(len(vr.denylisted_users), 1)


# ============================================================================
# JSON serialization
# ============================================================================

class TestValidationResultSerialization(unittest.TestCase):

    def test_round_trip_empty(self) -> None:
        vr = ValidationResult()
        j = vr.to_json()
        vr2 = ValidationResult.from_json(j)
        self.assertEqual(vr, vr2)

    def test_round_trip_full(self) -> None:
        vr = ValidationResult(
            valid=True, distance=0.02, scenario="strong_match",
            detail="distance=0.02", mbid_found=True,
            target_mbid="abc-123", candidate_count=2,
            candidates=[
                CandidateSummary(mbid="abc-123", artist="A", album="B",
                                  distance=0.02, track_count=10, year=2020,
                                  country="US", is_target=True),
                CandidateSummary(mbid="def-456", artist="A", album="B (Deluxe)",
                                  distance=0.35, track_count=15),
            ],
            local_track_count=10,
            soulseek_username="gooduser",
            download_folder="/downloads/A - B",
            denylisted_users=[],
            corrupt_files=[],
        )
        j = vr.to_json()
        vr2 = ValidationResult.from_json(j)
        self.assertEqual(vr2.valid, True)
        self.assertEqual(vr2.distance, 0.02)
        self.assertEqual(len(vr2.candidates), 2)
        self.assertTrue(vr2.candidates[0].is_target)
        self.assertEqual(vr2.candidates[1].track_count, 15)
        self.assertEqual(vr2.soulseek_username, "gooduser")

    def test_round_trip_with_corrupt_files(self) -> None:
        vr = ValidationResult(
            valid=False, scenario="audio_corrupt",
            corrupt_files=["01.mp3", "02.mp3"],
            failed_path="/failed/path",
        )
        j = vr.to_json()
        vr2 = ValidationResult.from_json(j)
        self.assertEqual(vr2.corrupt_files, ["01.mp3", "02.mp3"])
        self.assertEqual(vr2.failed_path, "/failed/path")

    def test_to_json_is_valid_json(self) -> None:
        vr = ValidationResult(scenario="high_distance", distance=0.45)
        parsed = json.loads(vr.to_json())
        self.assertEqual(parsed["scenario"], "high_distance")
        self.assertEqual(parsed["distance"], 0.45)

    def test_from_json_missing_optional_fields(self) -> None:
        """Minimal JSON — missing optional fields get defaults."""
        j = '{"valid": false, "scenario": "high_distance"}'
        vr = ValidationResult.from_json(j)
        self.assertFalse(vr.valid)
        self.assertEqual(vr.scenario, "high_distance")
        self.assertEqual(vr.candidates, [])
        self.assertEqual(vr.corrupt_files, [])

    def test_candidates_survive_round_trip(self) -> None:
        """Candidate list serializes as list of dicts, deserializes back."""
        vr = ValidationResult(
            candidates=[
                CandidateSummary(mbid="a", distance=0.1, is_target=True),
                CandidateSummary(mbid="b", distance=0.5),
            ],
        )
        j = vr.to_json()
        vr2 = ValidationResult.from_json(j)
        self.assertEqual(len(vr2.candidates), 2)
        self.assertIsInstance(vr2.candidates[0], CandidateSummary)
        self.assertTrue(vr2.candidates[0].is_target)
        self.assertEqual(vr2.candidates[1].mbid, "b")


# ============================================================================
# Dict compatibility (for mark_done/mark_failed transition)
# ============================================================================

class TestValidationResultDictCompat(unittest.TestCase):
    """ValidationResult must work with existing code that calls .get() on bv_result."""

    def test_get_method(self) -> None:
        """ValidationResult supports .get() for backward compat during transition."""
        vr = ValidationResult(
            valid=True, distance=0.02, scenario="strong_match",
            detail="distance=0.02",
        )
        self.assertEqual(vr.get("distance"), 0.02)
        self.assertEqual(vr.get("scenario"), "strong_match")
        self.assertIsNone(vr.get("nonexistent"))
        self.assertEqual(vr.get("nonexistent", "default"), "default")

    def test_getitem(self) -> None:
        """ValidationResult supports ["key"] access for backward compat."""
        vr = ValidationResult(valid=True, distance=0.02, scenario="strong_match")
        self.assertEqual(vr["distance"], 0.02)
        self.assertEqual(vr["valid"], True)


if __name__ == "__main__":
    unittest.main()
