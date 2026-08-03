"""Narrow generated guard for #1003's exact-evidence rescue boundary.

The deterministic DICE/payload pin is in ``test_youtube_ingest_service``.
This property varies only the evidence axes that matter at the final
pre-yt-dlp gate; it deliberately does not model arbitrary upstream schemas.
"""

from __future__ import annotations

import math
import unittest

from hypothesis import given
from hypothesis import strategies as st

from lib.youtube_ingest_service import YoutubeIngestService


class TestExactRescueEvidenceGenerated(unittest.TestCase):
    @given(
        outcome=st.sampled_from(["ok", "distance_failed"]),
        distance=st.sampled_from([0.0, 0.15, float("nan"), float("inf"), True]),
        tracks=st.sampled_from([None, 0, 1, 13]),
        duplicate=st.booleans(),
    )
    def test_only_one_finite_ok_exact_distance_with_tracks_is_admitted(
        self, outcome: str, distance: float, tracks: int | None, duplicate: bool,
    ) -> None:
        entry = {"mbid": "exact", "outcome": outcome, "distance": distance,
                 "total_mb_tracks": tracks}
        row = {"distances": [entry] * (2 if duplicate else 1)}
        accepted = YoutubeIngestService._distance_entry(row, "exact")
        expected = (
            not duplicate and outcome == "ok" and not isinstance(distance, bool) and math.isfinite(distance)
            and isinstance(tracks, int) and tracks > 0
        )
        self.assertEqual(accepted is not None, expected)

    def test_known_bad_checker_self_test_rejects_nan(self) -> None:
        row = {"distances": [{"mbid": "exact", "outcome": "ok",
                              "distance": float("nan"), "total_mb_tracks": 1}]}
        self.assertIsNone(YoutubeIngestService._distance_entry(row, "exact"))
