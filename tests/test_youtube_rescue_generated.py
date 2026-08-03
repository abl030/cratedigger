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

from tests.fakes import FakePipelineDB
from tests.test_youtube_ingest_service import (
    BROWSE,
    MB_RG,
    _make_service,
    _seed_resolver_row,
    _seed_wanted_request,
)


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
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb)
        _seed_resolver_row(pdb)
        entry = pdb._youtube_album_mappings[(MB_RG, "mb")][0]["distances"][0]
        entry.update({"outcome": outcome, "distance": distance, "total_mb_tracks": tracks})
        if duplicate:
            pdb._youtube_album_mappings[(MB_RG, "mb")][0]["distances"].append(dict(entry))
        result = _make_service(pdb, mb_count=tracks).submit(42, BROWSE)
        expected = (
            not duplicate and outcome == "ok" and not isinstance(distance, bool) and math.isfinite(distance)
            and isinstance(tracks, int) and tracks > 0
        )
        self.assertEqual(result.outcome == "accepted", expected)
        self.assertEqual(any(row.outcome == "youtube_running" for row in pdb.download_logs), expected)

    def test_known_bad_checker_self_test_rejects_nan(self) -> None:
        pdb = FakePipelineDB(); _seed_wanted_request(pdb); _seed_resolver_row(pdb)
        pdb._youtube_album_mappings[(MB_RG, "mb")][0]["distances"][0]["distance"] = float("nan")
        self.assertEqual(_make_service(pdb).submit(42, BROWSE).outcome, "track_count_precheck_failed")
