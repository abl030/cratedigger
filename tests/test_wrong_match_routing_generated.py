"""Generated property: Wrong Matches Lane A routing (issue #1077).

Drives the REAL ``lib.download_rejection._handle_rejected_result`` — the
shared beets-invalid-match / media-readiness reject handler — over a real
temp filesystem and a real ``album_source.DatabaseSource`` backed by
``FakePipelineDB`` (so ``reject_and_requeue`` actually writes denylist and
download_log rows, not a call recorder), for the production scenario
vocabulary PLUS arbitrary strings. The #1063 lesson applies directly: a
strategy whose world cannot produce an out-of-allowlist scenario proves
nothing, so ``st.text()`` is a first-class arm alongside the known
vocabulary, not an afterthought.

What this lane structurally guarantees, independent of any later evidence-
based decision the cleanup reducer might make (D2/D9, untouched):

* ``audio_corrupt`` is the ONLY scenario this lane ever deletes for. It bans
  the peer, destroys the folder outright, and leaves no ``failed_path`` — so
  it can never appear in the Wrong Matches worklist (D3).
* Every OTHER scenario this lane sees — every member of the production
  vocabulary and every arbitrary string a future producer might invent —
  keeps the folder (moved to quarantine, not deleted), bans the peer, and
  leaves a ``failed_path`` the worklist renders (D1/D4/D6). Whether the
  cleanup reducer LATER deletes a kept, delete-eligible folder is a
  separate, evidence-dependent question this property does not model —
  that is the reducer's own tested domain.
"""

from __future__ import annotations

import os
import tempfile
import unittest
from dataclasses import dataclass

import msgspec
from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from album_source import DatabaseSource
from lib.config import CratediggerConfig
from lib.context import CratediggerContext
from lib.download_rejection import _handle_rejected_result
from lib.quality import ValidationResult
from lib.staged_album import StagedAlbum
from lib.wrong_match_policy import (
    DELETE_ELIGIBLE_REJECTION_SCENARIOS,
    WRONG_MATCH_EXCLUDED_REJECTION_SCENARIOS,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_download_file, make_grab_list_entry, make_request_row

_KNOWN_SCENARIOS = (
    *DELETE_ELIGIBLE_REJECTION_SCENARIOS,
    *WRONG_MATCH_EXCLUDED_REJECTION_SCENARIOS,
    "untracked_audio",
    "request_missing_mbid",
    "request_missing_request_id",
    "validation_error",
    "strong_mismatch",
)
_SCENARIO_STRATEGY = st.one_of(
    st.sampled_from(_KNOWN_SCENARIOS),
    st.text(max_size=40),
)


def assert_lane_a_routing(
    *,
    scenario: str,
    folder_survives: bool,
    denylisted: list[str],
    failed_path: str | None,
    peer: str,
) -> None:
    """Independent oracle: audio_corrupt deletes, everything else keeps.

    Both arms ALWAYS ban the contributing peer (D1) — this lane never skips
    the denylist write regardless of scenario. Only the folder's fate and
    the presence of a worklist-visible ``failed_path`` differ.
    """
    if scenario == "audio_corrupt":
        if folder_survives:
            raise AssertionError(
                "audio_corrupt kept its folder instead of deleting it (D3)"
            )
        if failed_path is not None:
            raise AssertionError(
                "audio_corrupt left a failed_path — it would show as a "
                "Wrong Matches worklist row despite being deleted (D3)"
            )
    else:
        if not folder_survives:
            raise AssertionError(
                f"scenario={scenario!r} deleted its folder outside the "
                "audio_corrupt lane — no other scenario may reach a delete "
                "outcome here (D6)"
            )
        if failed_path is None:
            raise AssertionError(
                f"scenario={scenario!r} left no failed_path — kept but "
                "invisible in the Wrong Matches worklist (D1)"
            )
    if denylisted != [peer]:
        raise AssertionError(
            f"scenario={scenario!r} did not ban the contributing peer: "
            f"denylisted={denylisted!r}"
        )


@dataclass(frozen=True)
class _LaneAWorld:
    folder_survives: bool
    denylisted: list[str]
    failed_path: str | None
    peer: str


def _run_lane_a(*, scenario: str) -> _LaneAWorld:
    peer = "generated-peer"
    with tempfile.TemporaryDirectory() as tmpdir:
        current_path = os.path.join(tmpdir, "Artist - Album")
        os.makedirs(current_path)
        with open(
            os.path.join(current_path, "01 - Track.mp3"), "wb",
        ) as handle:
            handle.write(b"audio bytes")

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="downloading", mb_release_id="generated-mbid",
        ))
        source = DatabaseSource(
            dsn="",
            musicbrainz_ws2_base="https://example.invalid",
            discogs_api_base="https://example.invalid",
            borrowed_db=db,  # pyright: ignore[reportArgumentType]
        )
        cfg = CratediggerConfig(
            beets_tracking_file=os.path.join(tmpdir, "tracking.jsonl"),
        )
        ctx = CratediggerContext(cfg=cfg, slskd=None, pipeline_db_source=source)
        album = make_grab_list_entry(
            files=[make_download_file(username=peer)],
            artist="Artist",
            title="Album",
            mb_release_id="generated-mbid",
            db_request_id=42,
        )
        result = ValidationResult(
            valid=False,
            distance=0.4,
            scenario=scenario,
            detail=f"generated {scenario!r}",
        )

        _handle_rejected_result(
            album,
            result,
            StagedAlbum(current_path=current_path, request_id=42),
            ctx,
        )

        failed_path: str | None = None
        if db.download_logs:
            raw = db.download_logs[-1].validation_result
            decoded = msgspec.json.decode(raw) if isinstance(raw, str) else raw
            if isinstance(decoded, dict):
                candidate = decoded.get("failed_path")
                if isinstance(candidate, str):
                    failed_path = candidate
        # A successful quarantine MOVES the folder off ``current_path``
        # entirely (that is the whole point of "quarantine") — so survival
        # is "the audio bytes exist somewhere on disk", checked at
        # ``failed_path`` when one was recorded, not at the now-vacated
        # original location.
        if failed_path:
            folder_survives = os.path.isdir(failed_path) and bool(
                os.listdir(failed_path)
            )
        else:
            folder_survives = os.path.isdir(current_path)
        return _LaneAWorld(
            folder_survives=folder_survives,
            denylisted=[entry.username for entry in db.denylist],
            failed_path=failed_path,
            peer=peer,
        )


class TestWrongMatchLaneARoutingGenerated(unittest.TestCase):
    @example(scenario="audio_corrupt")
    @example(scenario="high_distance")
    @example(scenario="mbid_not_found")
    @example(scenario="untracked_audio")
    @example(scenario="")
    @example(scenario="a-scenario-nobody-has-invented-yet")
    @given(scenario=_SCENARIO_STRATEGY)
    def test_only_audio_corrupt_deletes_everything_else_keeps_bans_shows(
        self, scenario: str,
    ) -> None:
        world = _run_lane_a(scenario=scenario)
        assert_lane_a_routing(
            scenario=scenario,
            folder_survives=world.folder_survives,
            denylisted=world.denylisted,
            failed_path=world.failed_path,
            peer=world.peer,
        )


class TestInvariantCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests: a checker that cannot fail proves nothing."""

    def test_checker_rejects_a_kept_audio_corrupt_folder(self) -> None:
        with self.assertRaisesRegex(AssertionError, "instead of deleting"):
            assert_lane_a_routing(
                scenario="audio_corrupt",
                folder_survives=True,
                denylisted=["peer"],
                failed_path=None,
                peer="peer",
            )

    def test_checker_rejects_an_audio_corrupt_worklist_row(self) -> None:
        with self.assertRaisesRegex(AssertionError, "worklist row"):
            assert_lane_a_routing(
                scenario="audio_corrupt",
                folder_survives=False,
                denylisted=["peer"],
                failed_path="/some/quarantine/path",
                peer="peer",
            )

    def test_checker_rejects_a_deleted_non_corrupt_scenario(self) -> None:
        """The #1063-relevant case: a NOVEL scenario reaching a delete
        outcome at this lane must trip the checker."""
        with self.assertRaisesRegex(AssertionError, "no other scenario"):
            assert_lane_a_routing(
                scenario="a-brand-new-scenario",
                folder_survives=False,
                denylisted=["peer"],
                failed_path=None,
                peer="peer",
            )

    def test_checker_rejects_a_kept_but_invisible_folder(self) -> None:
        with self.assertRaisesRegex(AssertionError, "invisible"):
            assert_lane_a_routing(
                scenario="high_distance",
                folder_survives=True,
                denylisted=["peer"],
                failed_path=None,
                peer="peer",
            )

    def test_checker_rejects_a_keep_without_a_ban(self) -> None:
        with self.assertRaisesRegex(AssertionError, "did not ban"):
            assert_lane_a_routing(
                scenario="high_distance",
                folder_survives=True,
                denylisted=[],
                failed_path="/some/quarantine/path",
                peer="peer",
            )


if __name__ == "__main__":
    unittest.main()
