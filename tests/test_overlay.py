"""Direct tests for shared release-row overlay helpers."""

import os
import sys
import tempfile
import unittest
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import msgspec

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.banding import (
    CurrentBeetsBandingAmbiguityError,
    band_current_resolutions,
)
from lib.beets_db import BeetsDB, CurrentBeetsItem, CurrentBeetsUnique
from lib.convergence_service import ConvergenceSignal
from lib.quality import QualityRankConfig
from lib.release_identity import ConflictingReleaseIdentityError, ReleaseIdentity
from tests.fakes import FakeBeetsDB
from tests.test_beets_db import _create_test_db, _insert_album
from web.routes._overlay import band_release_ids, overlay_release_rows_in_place

MB_RELEASE_1 = "00000000-0000-0000-0000-000000000001"
MB_RELEASE_2 = "00000000-0000-0000-0000-000000000002"
MB_RELEASE_3 = "00000000-0000-0000-0000-000000000003"


class TestOverlayReleaseRowsInPlace(unittest.TestCase):
    def test_projects_request_convergence_on_the_exact_release_row(self):
        signal = ConvergenceSignal(
            request_id=21,
            latest_qualifying_log_id=101,
            cliff_hz=15_000,
            observation_count=6,
            distinct_peer_count=5,
            distinct_candidate_snapshot_count=4,
            first_observed_at=datetime(2026, 8, 1, tzinfo=UTC),
            latest_observed_at=datetime(2026, 8, 2, tzinfo=UTC),
        )
        rows: list[dict[str, object]] = [{"id": "queued"}]
        with patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={
                    "queued": {
                        "id": 21,
                        "status": "wanted",
                        "has_captured_history": True,
                        "verified_lossless": False,
                        "provisional_lossless": True,
                        "processing_owner": None,
                    },
                }), patch("web.server._beets_db", return_value=None), \
                patch(
                    "web.server.get_convergence_signals",
                    return_value={21: signal},
                    create=True,
                ) as get_signals:
            overlay_release_rows_in_place(rows, ["queued"])

        self.assertEqual(rows[0]["convergence"], msgspec.to_builtins(signal))
        get_signals.assert_called_once_with([21])

    def test_populates_library_and_pipeline_state(self):
        rows: list[dict[str, object]] = [
            {"id": "held", "title": "Held Release"},
            {"id": "queued", "title": "Queued Release"},
            {"id": "both", "title": "Both Release"},
            {"id": "neither", "title": "Neither Release"},
            {"id": "bad-quality", "title": "Bad Quality Release"},
        ]
        mock_beets = MagicMock()
        mock_beets.get_album_ids_by_mbids.return_value = {
            "held": 10,
            "both": 11,
            "bad-quality": 12,
        }
        mock_beets.check_mbids_detail.return_value = {
            "held": {"beets_format": "FLAC", "beets_bitrate": 900,
                     "beets_avg_bitrate": 1100},
            "both": {"beets_format": "MP3", "beets_bitrate": 194,
                     "beets_avg_bitrate": 288},
            "bad-quality": {"beets_format": None, "beets_bitrate": None,
                            "beets_avg_bitrate": None},
        }

        with patch("web.server.check_beets_library",
                   return_value={"held", "both", "bad-quality"}), \
                patch("web.server.check_pipeline",
                      return_value={
                          "queued": {"id": 21, "status": "wanted",
                                     "has_captured_history": False,
                                     "verified_lossless": False,
                                     "provisional_lossless": True,
                                     "processing_owner": None},
                          "both": {"id": 22, "status": "queued",
                                   "has_captured_history": True,
                                   "verified_lossless": True,
                                   "provisional_lossless": False,
                                   "processing_owner": None},
                      }), \
                patch("web.server._beets_db", return_value=mock_beets):
            overlay_release_rows_in_place(rows, [str(r["id"]) for r in rows])

        by_id = {row["id"]: row for row in rows}

        self.assertTrue(by_id["held"]["in_library"])
        self.assertEqual(by_id["held"]["beets_album_id"], 10)
        self.assertEqual(by_id["held"]["library_format"], "FLAC")
        self.assertEqual(by_id["held"]["library_min_bitrate"], 900)
        self.assertEqual(by_id["held"]["library_avg_bitrate"], 1100)
        # Real compute_library_rank — 1100kbps FLAC is lossless.
        self.assertEqual(by_id["held"]["library_rank"], "lossless")
        self.assertIsNone(by_id["held"]["pipeline_status"])

        self.assertFalse(by_id["queued"]["in_library"])
        self.assertIsNone(by_id["queued"]["beets_album_id"])
        self.assertEqual(by_id["queued"]["pipeline_status"], "wanted")
        self.assertEqual(by_id["queued"]["pipeline_id"], 21)
        self.assertFalse(by_id["queued"]["pipeline_verified_lossless"])
        self.assertTrue(by_id["queued"]["pipeline_provisional"])
        self.assertFalse(by_id["queued"]["has_captured_history"])
        self.assertIsNone(by_id["queued"]["processing_owner"])
        self.assertFalse(by_id["held"]["pipeline_verified_lossless"])
        self.assertFalse(by_id["held"]["pipeline_provisional"])
        self.assertFalse(by_id["held"]["has_captured_history"])

        self.assertTrue(by_id["both"]["in_library"])
        self.assertEqual(by_id["both"]["beets_album_id"], 11)
        self.assertEqual(by_id["both"]["library_min_bitrate"], 194)
        self.assertEqual(by_id["both"]["library_avg_bitrate"], 288)
        self.assertEqual(by_id["both"]["library_rank"], "transparent")
        self.assertEqual(by_id["both"]["pipeline_status"], "queued")
        self.assertEqual(by_id["both"]["pipeline_id"], 22)
        self.assertTrue(by_id["both"]["pipeline_verified_lossless"])
        self.assertFalse(by_id["both"]["pipeline_provisional"])
        self.assertTrue(by_id["both"]["has_captured_history"])

        self.assertFalse(by_id["neither"]["in_library"])
        self.assertIsNone(by_id["neither"]["beets_album_id"])
        self.assertIsNone(by_id["neither"]["pipeline_status"])
        self.assertIsNone(by_id["neither"]["pipeline_id"])
        self.assertIsNone(by_id["neither"]["processing_owner"])
        self.assertFalse(by_id["neither"]["has_captured_history"])

        self.assertEqual(by_id["bad-quality"]["library_format"], "")
        self.assertEqual(by_id["bad-quality"]["library_min_bitrate"], 0)
        # Real compute_library_rank — empty format/bitrate is unknown.
        self.assertEqual(by_id["bad-quality"]["library_rank"], "unknown")

    def test_empty_inputs_do_not_touch_backends(self):
        with patch("web.server.check_beets_library") as check_lib, \
                patch("web.server.check_pipeline") as check_pipeline, \
                patch("web.server._beets_db", return_value=None):
            overlay_release_rows_in_place([], [])

        check_lib.assert_not_called()
        check_pipeline.assert_not_called()

    def test_missing_row_id_raises_key_error(self):
        with patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}), \
                patch("web.server._beets_db", return_value=None), self.assertRaises(KeyError):
            overlay_release_rows_in_place([{"title": "No ID"}], [])

    def test_pipeline_snapshot_precedes_beets_failure_and_rows_stay_unprojected(self):
        calls: list[str] = []
        rows: list[dict[str, object]] = [{"id": "captured", "title": "Album"}]

        def check_pipeline(ids: list[str]) -> dict[str, dict[str, object]]:
            calls.append(f"pipeline:{ids}")
            return {
                "captured": {
                    "id": 42,
                    "status": "wanted",
                    "has_captured_history": True,
                    "verified_lossless": True,
                    "provisional_lossless": False,
                    "processing_owner": None,
                },
            }

        def check_beets(ids: list[str]) -> set[str]:
            calls.append(f"beets:{ids}")
            raise OSError("synthetic Beets read failure")

        with patch("web.server.check_pipeline", side_effect=check_pipeline), \
                patch("web.server.check_beets_library", side_effect=check_beets), \
                self.assertRaisesRegex(OSError, "Beets read failure"):
            overlay_release_rows_in_place(rows, ["captured"])

        self.assertEqual(calls, [
            "pipeline:['captured']",
            "beets:['captured']",
        ])
        self.assertEqual(rows, [{"id": "captured", "title": "Album"}])

    def test_conflicting_beets_identity_fails_loud_without_projecting_absence(self):
        rows: list[dict[str, object]] = [
            {
                "id": "unrelated-valid",
                "title": "Unrelated valid pressing",
            },
            {
                "id": "12856590",
                "title": "Conflicting pressing",
            },
        ]
        with patch("web.server.check_pipeline", return_value={}), patch(
            "web.server.check_beets_library",
            side_effect=ConflictingReleaseIdentityError(
                "conflicting numeric Discogs release identities for: 12856590"
            ),
        ), self.assertRaisesRegex(
            ConflictingReleaseIdentityError,
            "12856590",
        ):
            overlay_release_rows_in_place(
                rows,
                ["unrelated-valid", "12856590"],
            )

        self.assertEqual(rows, [
            {
                "id": "unrelated-valid",
                "title": "Unrelated valid pressing",
            },
            {
                "id": "12856590",
                "title": "Conflicting pressing",
            },
        ])


class TestBandReleaseIds(unittest.TestCase):
    def test_unique_mixed_format_band_uses_canonical_precedence_not_item_order(
        self,
    ) -> None:
        identity = ReleaseIdentity(source="musicbrainz", release_id=MB_RELEASE_1)
        items = (
            CurrentBeetsItem(
                id=1,
                path="/music/mixed/01.flac",
                format="FLAC",
                bitrate=1_100_000,
            ),
            CurrentBeetsItem(
                id=2,
                path="/music/mixed/02.mp3",
                format="MP3",
                bitrate=256_000,
            ),
        )

        bands = []
        for ordered_items in (items, tuple(reversed(items))):
            current = CurrentBeetsUnique(
                identity=identity,
                album_id=10,
                album_path="/music/mixed",
                items=ordered_items,
                selectors=(f"mb_albumid:{MB_RELEASE_1}",),
            )
            bands.append(band_current_resolutions(
                {identity: current},
                QualityRankConfig.defaults(),
            )[MB_RELEASE_1])

        self.assertEqual(bands, ["transparent", "transparent"])

    def test_unique_format_only_lossless_album_keeps_codec_only_rank(self) -> None:
        identity = ReleaseIdentity(source="musicbrainz", release_id=MB_RELEASE_1)
        current = CurrentBeetsUnique(
            identity=identity,
            album_id=10,
            album_path="/music/format-only",
            items=(CurrentBeetsItem(
                id=1,
                path="/music/format-only/01.flac",
                format="FLAC",
                bitrate=None,
            ),),
            selectors=(f"mb_albumid:{MB_RELEASE_1}",),
        )

        self.assertEqual(
            band_current_resolutions(
                {identity: current},
                QualityRankConfig.defaults(),
            ),
            {MB_RELEASE_1: "lossless"},
        )

    def test_ambiguous_identity_never_degrades_to_missing(self):
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release(MB_RELEASE_1, [10, 11])

        with patch("web.server._beets_db", return_value=beets), \
                self.assertRaises(CurrentBeetsBandingAmbiguityError) as raised:
            band_release_ids([MB_RELEASE_1])

        self.assertEqual(raised.exception.ambiguities[0].reason,
                         "multiple_matches")
        self.assertEqual(beets.check_mbids_calls, [])
        self.assertEqual(beets.check_mbids_detail_calls, [])

    def test_beets_error_never_degrades_to_missing(self):
        """A failed authority read is not evidence that every release is absent."""
        with patch("web.server._beets_db",
                   side_effect=OSError("db locked")), self.assertRaisesRegex(
                       OSError, "db locked"):
            band_release_ids([MB_RELEASE_1, MB_RELEASE_2])

    def test_bands_three_way_from_exact_resolution_snapshot(self):
        """Missing, unrankable Unique, and rankable Unique stay distinct."""
        beets = FakeBeetsDB()
        beets.set_mbid_detail(
            MB_RELEASE_1,
            {"beets_format": "FLAC", "beets_bitrate": 900,
             "beets_avg_bitrate": 1100},
        )
        beets.set_album_exists(MB_RELEASE_2, True)
        with patch("web.server._beets_db", return_value=beets):
            out = band_release_ids([
                MB_RELEASE_1,
                MB_RELEASE_2,
                MB_RELEASE_3,
            ])
        self.assertEqual(out[MB_RELEASE_1], "lossless")
        self.assertEqual(out[MB_RELEASE_2], "unknown")
        self.assertEqual(out[MB_RELEASE_3], "missing")
        self.assertEqual(beets.check_mbids_calls, [])
        self.assertEqual(beets.check_mbids_detail_calls, [])

    def test_modern_and_legacy_discogs_share_the_exact_batch_resolver(self):
        with tempfile.TemporaryDirectory() as root:
            db_path = os.path.join(root, "beets.db")
            _create_test_db(db_path)
            _insert_album(
                db_path,
                1,
                "",
                [(1_100_000, "/music/modern/01.flac")],
                track_format="FLAC",
                discogs_albumid=12_856_590,
            )
            _insert_album(
                db_path,
                2,
                "5555555",
                [(1_100_000, "/music/legacy/01.flac")],
                track_format="FLAC",
            )
            with BeetsDB(db_path, library_root="/music") as beets, patch(
                "web.server._beets_db", return_value=beets,
            ):
                out = band_release_ids(["12856590", "5555555"])

        self.assertEqual(out, {
            "12856590": "lossless",
            "5555555": "lossless",
        })


if __name__ == "__main__":
    unittest.main()
