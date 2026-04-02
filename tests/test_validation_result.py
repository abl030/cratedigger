#!/usr/bin/env python3
"""Tests for ValidationResult + CandidateSummary dataclasses.

RED/GREEN TDD — tests written before implementation.
"""

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.quality import (ValidationResult, CandidateSummary,
                         HarnessItem, HarnessTrackInfo, TrackMapping)


# ============================================================================
# HarnessItem, HarnessTrackInfo, TrackMapping
# ============================================================================

class TestHarnessItem(unittest.TestCase):

    def test_defaults(self) -> None:
        item = HarnessItem()
        self.assertEqual(item.path, "")
        self.assertEqual(item.title, "")
        self.assertIsNone(item.bitrate)
        self.assertEqual(item.format, "")

    def test_full(self) -> None:
        item = HarnessItem(
            path="01 - Track.flac", title="Track", artist="Artist",
            album="Album", track=1, disc=1, length=213.4,
            bitrate=1411, format="FLAC", mb_trackid="tid-1",
            data_source="MusicBrainz",
        )
        self.assertEqual(item.path, "01 - Track.flac")
        self.assertEqual(item.bitrate, 1411)
        self.assertEqual(item.format, "FLAC")

    def test_attribute_error_on_typo(self) -> None:
        item = HarnessItem()
        with self.assertRaises(AttributeError):
            _ = item.tilte  # type: ignore[attr-defined]


class TestHarnessTrackInfo(unittest.TestCase):

    def test_defaults(self) -> None:
        t = HarnessTrackInfo()
        self.assertEqual(t.title, "")
        self.assertIsNone(t.index)
        self.assertIsNone(t.medium)
        self.assertEqual(t.track_id, "")

    def test_full(self) -> None:
        t = HarnessTrackInfo(
            title="Summertime Blues", artist="Blue Cheer",
            index=1, medium=1, medium_index=1, medium_total=6,
            length=213.4, track_id="tid-1",
            release_track_id="rtid-1", track_alt=None,
            disctitle=None, data_source="MusicBrainz",
        )
        self.assertEqual(t.title, "Summertime Blues")
        self.assertEqual(t.medium_total, 6)
        self.assertEqual(t.release_track_id, "rtid-1")


class TestTrackMapping(unittest.TestCase):

    def test_construction(self) -> None:
        m = TrackMapping(
            item=HarnessItem(path="01.flac", title="Track 1"),
            track=HarnessTrackInfo(title="Track 1", track_id="t1"),
        )
        self.assertEqual(m.item.path, "01.flac")
        self.assertEqual(m.track.track_id, "t1")

    def test_round_trip_json(self) -> None:
        """TrackMapping survives JSON round-trip through ValidationResult."""
        m = TrackMapping(
            item=HarnessItem(path="01.flac", title="Track 1", bitrate=1411),
            track=HarnessTrackInfo(title="Track 1", track_id="t1", length=213.4),
        )
        vr = ValidationResult(
            candidates=[CandidateSummary(
                mbid="abc", mapping=[m],
            )],
        )
        j = vr.to_json()
        vr2 = ValidationResult.from_json(j)
        m2 = vr2.candidates[0].mapping[0]
        self.assertIsInstance(m2, TrackMapping)
        self.assertEqual(m2.item.path, "01.flac")
        self.assertEqual(m2.item.bitrate, 1411)
        self.assertEqual(m2.track.track_id, "t1")
        self.assertEqual(m2.track.length, 213.4)


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
        self.assertEqual(c.extra_tracks, [])
        self.assertEqual(c.extra_items, [])
        self.assertFalse(c.is_target)

    def test_full_construction(self) -> None:
        c = CandidateSummary(
            mbid="abc-123", artist="Ye", album="BULLY",
            distance=0.45, track_count=12, year=2025,
            country="US",
            extra_tracks=[HarnessTrackInfo(title="Bonus 1"),
                          HarnessTrackInfo(title="Bonus 2")],
            extra_items=[],
            is_target=True,
        )
        self.assertEqual(c.mbid, "abc-123")
        self.assertTrue(c.is_target)
        self.assertEqual(len(c.extra_tracks), 2)
        self.assertEqual(c.extra_tracks[0].title, "Bonus 1")

    def test_from_harness_candidate(self) -> None:
        """Simulate constructing from beets harness candidate dict."""
        harness_cand = {
            "album_id": "abc-123", "artist": "Blue Cheer",
            "album": "Vincebus Eruptum", "distance": 0.02,
            "distance_breakdown": {"album": 0.0, "artist": 0.0, "tracks": 0.01},
            "track_count": 6, "year": 1968, "country": "US",
            "label": "Philips", "catalognum": "PHS 600-264",
            "media": "CD", "mediums": 1,
            "albumtype": "album", "albumstatus": "Official",
            "albumdisambig": "", "releasegroup_id": "rg-123",
            "va": False, "data_source": "MusicBrainz",
            "extra_tracks": [],
            "extra_items": [],
            "mapping": [
                {"item": {"path": "01.flac", "title": "Summertime Blues"},
                 "track": {"title": "Summertime Blues", "track_id": "t1"}},
            ],
            "tracks": [
                {"title": "Summertime Blues", "length": 213.4, "track_id": "t1"},
                {"title": "Rock Me Baby", "length": 244.1, "track_id": "t2"},
            ],
        }
        # Use from_dict — same path as real code (JSON → dict → typed)
        c = CandidateSummary.from_dict(harness_cand)
        self.assertEqual(c.mbid, "abc-123")
        self.assertEqual(c.distance, 0.02)
        self.assertEqual(c.label, "Philips")
        self.assertEqual(c.catalognum, "PHS 600-264")
        self.assertEqual(c.media, "CD")
        self.assertEqual(c.data_source, "MusicBrainz")
        self.assertEqual(len(c.tracks), 2)
        self.assertEqual(c.tracks[0].title, "Summertime Blues")
        self.assertEqual(len(c.mapping), 1)
        self.assertEqual(c.mapping[0].item.path, "01.flac")

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
                    HarnessTrackInfo(title="Track 1", length=180.0, track_id="t1"),
                    HarnessTrackInfo(title="Track 2", length=200.0, track_id="t2"),
                ],
            )],
        )
        j = vr.to_json()
        vr2 = ValidationResult.from_json(j)
        self.assertEqual(len(vr2.candidates[0].tracks), 2)
        self.assertIsInstance(vr2.candidates[0].tracks[1], HarnessTrackInfo)
        self.assertEqual(vr2.candidates[0].tracks[1].title, "Track 2")


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
# Attribute access (dict compat removed — verify attributes work correctly)
# ============================================================================

class TestValidationResultAttributeAccess(unittest.TestCase):
    """ValidationResult fields are accessed via attributes, not dict syntax."""

    def test_attribute_read(self) -> None:
        vr = ValidationResult(
            valid=True, distance=0.02, scenario="strong_match",
            detail="distance=0.02",
        )
        self.assertEqual(vr.distance, 0.02)
        self.assertEqual(vr.scenario, "strong_match")

    def test_attribute_write(self) -> None:
        vr = ValidationResult(valid=True)
        vr.valid = False
        self.assertFalse(vr.valid)
        vr.scenario = "spectral_reject"
        self.assertEqual(vr.scenario, "spectral_reject")

    def test_no_dict_access(self) -> None:
        """Dict-style access must raise TypeError — no more dual interface."""
        vr = ValidationResult(valid=True)
        with self.assertRaises(TypeError):
            _ = vr["valid"]  # type: ignore[index]
        with self.assertRaises(TypeError):
            vr["valid"] = False  # type: ignore[index]


if __name__ == "__main__":
    unittest.main()
