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

Issue #1077, F1: "kept" and "visible" are DIFFERENT facts and must be
checked independently. Whether the persisted row's own ``failed_path`` key
is non-``None`` is a proxy for "Lane A wrote a quarantine pointer" — it is
NOT proof the row shows up in the operator's worklist, because
``lib.wrong_matches.wrong_match_row_is_visible`` applies its own,
independent scenario exclusion on top of that pointer (the exact shape of
the F2 defect: kept + banned + a real ``failed_path`` + still invisible).
This property therefore reads visibility back through the REAL composed
path — ``FakePipelineDB.get_wrong_matches()``, the same call the worklist
route makes — never by re-deriving it from ``failed_path`` alone.

What this lane structurally guarantees, independent of any later evidence-
based decision the cleanup reducer might make (D2/D9, untouched):

* ``audio_corrupt`` is the ONLY scenario this lane ever deletes for. It bans
  the peer, destroys the folder outright, leaves no ``failed_path``, and the
  row never appears in ``get_wrong_matches()`` (D3).
* Every OTHER scenario this lane sees — every member of the production
  vocabulary and every arbitrary string a future producer might invent —
  keeps the folder (moved to quarantine, not deleted) and bans the peer
  (D1/D4/D6) unconditionally. Whether the row is additionally VISIBLE in
  the worklist follows the shared taxonomy
  (``WRONG_MATCH_EXCLUDED_REJECTION_SCENARIOS``) exactly: the handful of
  folder/audio-integrity and spectral-only scenarios stay kept+banned but
  invisible (their own recovery path owns them), every other scenario —
  known or novel — must be visible. Whether the cleanup reducer LATER
  deletes a kept, delete-eligible folder is a separate, evidence-dependent
  question this property does not model — that is the reducer's own tested
  domain.
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
    visible: bool,
    peer: str,
) -> None:
    """Independent oracle: audio_corrupt deletes, everything else keeps.

    Both arms ALWAYS ban the contributing peer (D1) — this lane never skips
    the denylist write regardless of scenario. The folder's fate and the
    presence of a ``failed_path`` pointer differ by scenario; worklist
    VISIBILITY is then checked as its own, separate fact against the shared
    taxonomy (issue #1077, F1) — a real ``failed_path`` does not imply a
    visible row.
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
        if visible:
            raise AssertionError(
                "audio_corrupt produced a row visible in get_wrong_matches() "
                "despite being deleted with no failed_path (D3)"
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
        expected_visible = scenario not in WRONG_MATCH_EXCLUDED_REJECTION_SCENARIOS
        if visible is not expected_visible:
            raise AssertionError(
                f"scenario={scenario!r} was kept with a failed_path but "
                f"visible={visible!r} disagrees with the shared taxonomy "
                f"(expected {expected_visible!r}) — kept implies visible "
                "for every scenario outside the excluded set (D1)"
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
    visible: bool
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

        # Visibility is read back through the REAL composed path — the same
        # call the Wrong Matches worklist route makes — never re-derived
        # from ``failed_path`` alone (issue #1077, F1).
        visible = False
        if db.download_logs:
            log_id = db.download_logs[-1].id
            visible = any(
                row.get("download_log_id") == log_id
                for row in db.get_wrong_matches()
            )

        return _LaneAWorld(
            folder_survives=folder_survives,
            denylisted=[entry.username for entry in db.denylist],
            failed_path=failed_path,
            visible=visible,
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
            visible=world.visible,
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
                visible=False,
                peer="peer",
            )

    def test_checker_rejects_an_audio_corrupt_worklist_row(self) -> None:
        with self.assertRaisesRegex(AssertionError, "worklist row"):
            assert_lane_a_routing(
                scenario="audio_corrupt",
                folder_survives=False,
                denylisted=["peer"],
                failed_path="/some/quarantine/path",
                visible=False,
                peer="peer",
            )

    def test_checker_rejects_an_audio_corrupt_row_visible_despite_no_path(
        self,
    ) -> None:
        """A defensive belt-and-braces case: even if some future bug left
        ``failed_path`` unset but the row still surfaced in
        ``get_wrong_matches()`` some other way, the checker must catch it —
        D3 forbids an audio_corrupt row from ever being visible."""
        with self.assertRaisesRegex(AssertionError, "despite being deleted"):
            assert_lane_a_routing(
                scenario="audio_corrupt",
                folder_survives=False,
                denylisted=["peer"],
                failed_path=None,
                visible=True,
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
                visible=False,
                peer="peer",
            )

    def test_checker_rejects_a_kept_but_invisible_folder(self) -> None:
        with self.assertRaisesRegex(AssertionError, "invisible"):
            assert_lane_a_routing(
                scenario="high_distance",
                folder_survives=True,
                denylisted=["peer"],
                failed_path=None,
                visible=False,
                peer="peer",
            )

    def test_checker_rejects_a_kept_row_the_visibility_predicate_hides(
        self,
    ) -> None:
        """The exact F1/F2 pathology: Lane A wrote a real ``failed_path`` for
        a non-excluded scenario (so the folder IS kept and quarantined), but
        the composed ``get_wrong_matches()`` read disagrees with the shared
        taxonomy and reports it invisible anyway — a garbled grab that would
        sit kept, banned, and permanently hidden from the operator."""
        with self.assertRaisesRegex(AssertionError, "disagrees with the shared taxonomy"):
            assert_lane_a_routing(
                scenario="high_distance",
                folder_survives=True,
                denylisted=["peer"],
                failed_path="/some/quarantine/path",
                visible=False,
                peer="peer",
            )

    def test_checker_rejects_an_excluded_scenario_row_reported_visible(
        self,
    ) -> None:
        """The converse of the pathology above: a folder/audio-integrity
        scenario that IS in the excluded set must never be reported visible
        even if it somehow was — visibility must track the taxonomy in both
        directions, not just fail closed toward "hidden"."""
        with self.assertRaisesRegex(AssertionError, "disagrees with the shared taxonomy"):
            assert_lane_a_routing(
                scenario="bad_audio_hash",
                folder_survives=True,
                denylisted=["peer"],
                failed_path="/some/quarantine/path",
                visible=True,
                peer="peer",
            )

    def test_checker_rejects_a_keep_without_a_ban(self) -> None:
        with self.assertRaisesRegex(AssertionError, "did not ban"):
            assert_lane_a_routing(
                scenario="high_distance",
                folder_survives=True,
                denylisted=[],
                failed_path="/some/quarantine/path",
                visible=True,
                peer="peer",
            )


if __name__ == "__main__":
    unittest.main()
