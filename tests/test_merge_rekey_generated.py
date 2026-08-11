"""Generated properties for the import-time MusicBrainz merge rekey (#1059).

The pins in ``tests/test_merge_rekey.py`` prove the exact branches; these
properties patrol the world space around them, driving the REAL
``_process_beets_validation`` over every combination of (validation scenario ×
what the mirror answers × which candidates beets offered × what the library
retag reports × whether this validation owns the request × whether another
request already holds the survivor).

Why this surface earns a property: the seam composes three writers over one
shared namespace — the MusicBrainz mirror, the Beets library, and
``album_requests.mb_release_id`` — and the defect it exists to prevent is
invisible in the END state. Rekey-then-retag and retag-then-rekey both finish
with the row at the survivor and the library at the survivor; only the order
decides whether the next import lands a SECOND album beside the first
(``duplicate_keys: album: [mb_albumid, discogs_albumid]``). So the property
watches the row from INSIDE the retag, where the ordering is observable.

Invariants patrolled — each is a module-level checker so the known-bad
self-tests below can call it directly:

P1  The request row moves ONLY when every authorization held: the validation
    said ``mbid_not_found``, this validation owned the request, MusicBrainz
    declared a different MusicBrainz survivor, beets offered that survivor as
    a candidate, the library retag reached a READY outcome, and no other
    request already held the survivor.
P2  In every world where the row did not move, the request row and the
    in-flight ``GrabListEntry`` are byte-identical to how they started, and so
    is the rejection the operator sees.
P3  The library retag is observed at most once, and never while the request
    row already names the survivor. That is the ordering defect, watched from
    the one place it is visible.
P4  The mirror is asked at most once, and only for an owned, MusicBrainz,
    ``mbid_not_found`` validation. ~8,500 healthy rows a cycle depend on it.
P5  The request is left runnable in every branch: an active acquisition
    status, ``processing`` only while its exact owner is attached, and never
    the frozen ``replaced``. No world parks a request for a human.
"""

from __future__ import annotations

import contextlib
import logging
import os
import tempfile
import unittest
from collections.abc import Iterator, Mapping
from unittest.mock import patch

from hypothesis import example, given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.beets_retag import (
    RETAG_ALREADY_CURRENT,
    RETAG_AMBIGUOUS,
    RETAG_FAILED,
    RETAG_NOT_HELD,
    RETAG_READY_OUTCOMES,
    RETAG_RETAGGED,
    BeetsRetagResult,
    RetagOutcome,
)
from lib.config import CratediggerConfig
from lib.download_validation import _process_beets_validation
from lib.pipeline_db._shared import REQUEST_STATUSES
from lib.quality import ValidationResult
from lib.release_identity import ReleaseIdentity
from lib.staged_album import StagedAlbum
from tests.fakes import FakePipelineDB
from tests.helpers import (
    handoff_automation_owner,
    make_ctx_with_fake_db,
    make_grab_list_entry,
    make_request_row,
)
from tests.test_merge_rekey import (
    MERGED,
    REQUEST_ID,
    SURVIVOR,
    UNRELATED,
    candidate,
)

#: Every scenario ``beets_validate`` can name. Only one may reach the mirror.
SCENARIOS = st.sampled_from([
    "mbid_not_found",
    "strong_match",
    "high_distance",
    "extra_tracks",
    "no_choose_match",
    "validation_error",
])

#: What the mirror answers, including answers its own contract forbids: the
#: stored id back, a non-MusicBrainz id, and a blank string. The seam must fail
#: closed on each rather than trust the resolver to have filtered them.
MIRROR_ANSWERS = st.sampled_from([
    None, SURVIVOR, MERGED, UNRELATED, "1870", "", "not-a-uuid",
])

#: Which candidates beets offered. No plausibility filter: a
#: ``mbid_not_found`` result carrying the stored id is a contradiction the
#: seam must still survive.
CANDIDATE_SETS = st.sampled_from([
    (), (SURVIVOR,), (UNRELATED,), (MERGED,),
    (SURVIVOR, UNRELATED), (MERGED, SURVIVOR),
])

_RETAG_OUTCOME_VALUES: list[RetagOutcome] = [
    RETAG_RETAGGED,
    RETAG_ALREADY_CURRENT,
    RETAG_NOT_HELD,
    RETAG_AMBIGUOUS,
    RETAG_FAILED,
]
RETAG_OUTCOMES = st.sampled_from(_RETAG_OUTCOME_VALUES)


@contextlib.contextmanager
def _silence_logs() -> Iterator[None]:
    previous = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        yield
    finally:
        logging.disable(previous)


# ---------------------------------------------------------------------------
# Invariant checkers — module level so the known-bad self-tests can call them
# ---------------------------------------------------------------------------


def _taken_survivor_id(mirror_answer: str | None) -> str | None:
    """The id a rival request would have to hold to block this world's rekey.

    ``None`` when the mirror named nothing a rekey could ever target, so the
    fixture seeds no rival and ``survivor_taken`` is moot.
    """
    identity = (
        ReleaseIdentity.from_id(mirror_answer)
        if mirror_answer is not None
        else None
    )
    if identity is None or identity.source != "musicbrainz":
        return None
    if identity.release_id == MERGED:
        return None
    return identity.release_id


def rekey_is_authorized(
    *,
    scenario: str,
    owned: bool,
    mirror_answer: str | None,
    candidates: tuple[str, ...],
    retag_outcome: str,
    survivor_taken: bool,
) -> bool:
    """The complete conjunction that permits moving ``mb_release_id``.

    Derived from the world, independently of the production code, so a
    production change that widens ANY term is a property failure rather than a
    silently agreeing reimplementation.
    """
    if scenario != "mbid_not_found" or not owned:
        return False
    identity = (
        ReleaseIdentity.from_id(mirror_answer)
        if mirror_answer is not None
        else None
    )
    if identity is None or identity.source != "musicbrainz":
        return False
    if identity.release_id == MERGED:
        return False
    if identity.release_id not in candidates:
        return False
    if retag_outcome not in RETAG_READY_OUTCOMES:
        return False
    return not survivor_taken


def check_row_moves_only_when_authorized(
    stored_after: str | None,
    *,
    authorized: bool,
    expected_survivor: str | None,
) -> None:
    """P1 — identity moves exactly in the authorized world, and nowhere else."""
    moved = stored_after != MERGED
    if moved and not authorized:
        raise AssertionError(
            f"request identity moved to {stored_after!r} in a world that "
            "authorized nothing"
        )
    if authorized and stored_after != expected_survivor:
        raise AssertionError(
            f"authorized rekey left the identity at {stored_after!r}, not "
            f"{expected_survivor!r}"
        )


def check_unauthorized_world_is_unchanged(
    *,
    authorized: bool,
    row_before: Mapping[str, object],
    row_after: Mapping[str, object],
    entry_release_id: str,
    result_json: str,
    result_json_before: str,
) -> None:
    """P2 — a world that authorized nothing changed nothing observable."""
    if authorized:
        return
    if row_before.get("mb_release_id") != row_after.get("mb_release_id"):
        raise AssertionError(
            "request identity changed without authorization: "
            f"{row_before.get('mb_release_id')!r} -> "
            f"{row_after.get('mb_release_id')!r}"
        )
    if entry_release_id != row_before.get("mb_release_id"):
        raise AssertionError(
            f"in-flight album identity drifted to {entry_release_id!r} while "
            f"the request row still says {row_before.get('mb_release_id')!r}"
        )
    if result_json != result_json_before:
        raise AssertionError(
            "the rejection the operator sees was rewritten without any "
            f"rekey: {result_json_before!r} -> {result_json!r}"
        )


def check_retag_never_saw_a_moved_row(observed_release_ids: list[str | None]) -> None:
    """P3 — the ordering defect, watched from inside the retag."""
    if len(observed_release_ids) > 1:
        raise AssertionError(
            f"the library retag ran {len(observed_release_ids)} times for one "
            "validation"
        )
    for observed in observed_release_ids:
        if observed != MERGED:
            raise AssertionError(
                "the request row already read "
                f"{observed!r} while the library was being retagged — Beets "
                "would flag no duplicate and the import would land a SECOND "
                "album"
            )


def check_mirror_asked_only_where_it_can_be_used(
    calls: list[str],
    *,
    scenario: str,
    owned: bool,
) -> None:
    """P4 — the performance contract, asserted on the mirror itself."""
    if len(calls) > 1:
        raise AssertionError(
            f"the mirror was asked {len(calls)} times for one validation: "
            f"{calls!r}"
        )
    if not calls:
        return
    if scenario != "mbid_not_found":
        raise AssertionError(
            f"the mirror was asked on a {scenario!r} validation; only "
            "mbid_not_found may reach it"
        )
    if not owned:
        raise AssertionError(
            "the mirror was asked by a validation that does not own the "
            "request and could not act on the answer"
        )
    if calls[0] != MERGED:
        raise AssertionError(
            f"the mirror was asked about {calls[0]!r}, not the stored id"
        )


def check_request_remains_runnable(row: Mapping[str, object] | None) -> None:
    """P5 — nothing is parked; the next cycle can always re-derive."""
    if row is None:
        raise AssertionError("the request row disappeared")
    status = row.get("status")
    if status == "replaced":
        raise AssertionError(
            "the merge seam froze the request as a replaced audit ancestor"
        )
    # Every status the pipeline itself can re-derive from. Anything outside
    # the shipped vocabulary is by definition a marker whose only exit is a
    # human, which invariant 11 forbids.
    if status not in REQUEST_STATUSES:
        raise AssertionError(
            f"the request was left in the non-runnable status {status!r}"
        )
    if status == "processing" and row.get("active_automation_import_job_id") is None:
        raise AssertionError(
            "the request is processing with no owner attached — get_wanted() "
            "will never select it again"
        )


class _RecordingCanonical:
    def __init__(self, answer: str | None) -> None:
        self._answer = answer
        self.calls: list[str] = []

    def __call__(self, release_id: str) -> str | None:
        self.calls.append(release_id)
        return self._answer


class _RecordingRetag:
    """Reports one outcome and snapshots the row it ran under (P3)."""

    def __init__(self, db: FakePipelineDB, outcome: RetagOutcome) -> None:
        self._db = db
        self._outcome: RetagOutcome = outcome
        self.observed_release_ids: list[str | None] = []

    def __call__(
        self,
        cfg: CratediggerConfig,
        *,
        old_identity: ReleaseIdentity,
        new_identity: ReleaseIdentity,
    ) -> BeetsRetagResult:
        del cfg, old_identity, new_identity
        row = self._db.request(REQUEST_ID)
        self.observed_release_ids.append(
            None if row is None else row.get("mb_release_id"),
        )
        return BeetsRetagResult(outcome=self._outcome, detail="generated world")


def _validation_result(
    scenario: str,
    candidates: tuple[str, ...],
) -> ValidationResult:
    """The exact result shape ``beets_validate`` returns for each scenario."""
    summaries = [candidate(mbid) for mbid in candidates]
    if scenario == "mbid_not_found":
        return ValidationResult(
            valid=False,
            scenario=scenario,
            detail=f"Target MBID {MERGED} not in candidates",
            target_mbid=MERGED,
            candidate_count=len(summaries),
            candidates=summaries,
        )
    return ValidationResult(
        valid=scenario == "strong_match",
        distance=0.04 if scenario in ("strong_match", "high_distance") else None,
        scenario=scenario,
        detail=scenario,
        mbid_found=scenario in ("strong_match", "high_distance", "extra_tracks"),
        target_mbid=MERGED,
        candidate_count=len(summaries),
        candidates=summaries,
    )


class TestMergeRekeyProperties(unittest.TestCase):
    """P1–P5 over every world, driving the real validation seam."""

    @settings(deadline=None)
    @given(
        scenario=SCENARIOS,
        mirror_answer=MIRROR_ANSWERS,
        candidates=CANDIDATE_SETS,
        retag_outcome=RETAG_OUTCOMES,
        owned=st.booleans(),
        survivor_taken=st.booleans(),
    )
    # The DICE world (request 346), the ordering-critical world, and the two
    # that motivated the READY-membership gate.
    @example(
        scenario="mbid_not_found", mirror_answer=SURVIVOR,
        candidates=(SURVIVOR,), retag_outcome=RETAG_RETAGGED,
        owned=True, survivor_taken=False,
    )
    @example(
        scenario="mbid_not_found", mirror_answer=SURVIVOR,
        candidates=(SURVIVOR,), retag_outcome=RETAG_AMBIGUOUS,
        owned=True, survivor_taken=False,
    )
    @example(
        scenario="mbid_not_found", mirror_answer=SURVIVOR,
        candidates=(SURVIVOR,), retag_outcome=RETAG_RETAGGED,
        owned=True, survivor_taken=True,
    )
    @example(
        scenario="strong_match", mirror_answer=SURVIVOR,
        candidates=(SURVIVOR,), retag_outcome=RETAG_RETAGGED,
        owned=True, survivor_taken=False,
    )
    # Shrunk by the 2026-08-11 fuzz burst: the survivor is an arbitrary MB id
    # rather than the fixture's constant, and a rival request holds THAT id.
    # The world where the fixture and the authorization derivation can drift.
    @example(
        scenario="mbid_not_found", mirror_answer=UNRELATED,
        candidates=(UNRELATED,), retag_outcome=RETAG_RETAGGED,
        owned=True, survivor_taken=True,
    )
    def test_every_world_upholds_the_merge_rekey_invariants(
        self,
        scenario: str,
        mirror_answer: str | None,
        candidates: tuple[str, ...],
        retag_outcome: RetagOutcome,
        owned: bool,
        survivor_taken: bool,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "01 - Track.mp3"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=REQUEST_ID,
                mb_release_id=MERGED,
                artist_name="DICE",
                album_title="Midnight Zoo",
            ))
            # "Taken" means the id this world's mirror actually names, not a
            # fixed one: the fuzz burst found that seeding a constant made the
            # derivation and the fixture disagree whenever the mirror named
            # something else, so the collision is now always the real one.
            taken = _taken_survivor_id(mirror_answer) if survivor_taken else None
            if taken is not None:
                db.seed_request(make_request_row(
                    id=REQUEST_ID + 1,
                    mb_release_id=taken,
                    artist_name="DICE",
                    album_title="Midnight Zoo (other pressing)",
                ))
            if owned:
                import_job_id = handoff_automation_owner(
                    db,
                    REQUEST_ID,
                    state={
                        "filetype": "mp3",
                        "enqueued_at": "2026-08-11T00:00:00+00:00",
                        "current_path": tmpdir,
                        "files": [],
                    },
                    canonical_path=tmpdir,
                ).id
            else:
                import_job_id = 4242
            cfg = CratediggerConfig(
                beets_harness_path="/nix/store/fake/harness.sh",
                beets_distance_threshold=0.15,
                beets_staging_dir=os.path.join(tmpdir, "staging"),
                slskd_download_dir=tmpdir,
                pipeline_db_enabled=True,
            )
            ctx = make_ctx_with_fake_db(db, cfg=cfg)
            album_data = make_grab_list_entry(
                album_id=REQUEST_ID,
                artist="DICE",
                title="Midnight Zoo",
                mb_release_id=MERGED,
                db_source="request",
                db_request_id=REQUEST_ID,
            )
            staged_album = StagedAlbum(
                current_path=tmpdir, request_id=REQUEST_ID,
            )
            bv_result = _validation_result(scenario, candidates)
            rejection_before = _rejection_fingerprint(bv_result)
            row_before = dict(db.request(REQUEST_ID) or {})
            canonical = _RecordingCanonical(mirror_answer)
            retag = _RecordingRetag(db, retag_outcome)

            with (
                _silence_logs(),
                patch("lib.beets.beets_validate", return_value=bv_result),
            ):
                _process_beets_validation(
                    album_data,
                    staged_album,
                    ctx,
                    import_job_id=import_job_id,
                    canonical_release_fn=canonical,
                    retag_fn=retag,
                )

            row_after = dict(db.request(REQUEST_ID) or {})

        authorized = rekey_is_authorized(
            scenario=scenario,
            owned=owned,
            mirror_answer=mirror_answer,
            candidates=candidates,
            retag_outcome=retag_outcome,
            survivor_taken=survivor_taken,
        )
        check_row_moves_only_when_authorized(
            row_after.get("mb_release_id"),
            authorized=authorized,
            expected_survivor=mirror_answer,
        )
        check_unauthorized_world_is_unchanged(
            authorized=authorized,
            row_before=row_before,
            row_after=row_after,
            entry_release_id=album_data.mb_release_id,
            # ``_process_beets_validation`` always stamps source info
            # (username, folder, source dirs) on the result, so the comparison
            # is over the merge-relevant fields — which is exactly what the
            # rejection the operator reads is made of.
            result_json=_rejection_fingerprint(bv_result),
            result_json_before=rejection_before,
        )
        check_retag_never_saw_a_moved_row(retag.observed_release_ids)
        check_mirror_asked_only_where_it_can_be_used(
            canonical.calls, scenario=scenario, owned=owned,
        )
        check_request_remains_runnable(row_after)


def _rejection_fingerprint(result: ValidationResult) -> str:
    """The operator-visible half of a rejection, independent of source info."""
    return (
        f"{result.valid}|{result.scenario}|{result.detail}|"
        f"{result.distance}|{result.mbid_found}|{result.target_mbid}"
    )



def _authorized(
    *,
    scenario: str = "mbid_not_found",
    owned: bool = True,
    mirror_answer: str | None = SURVIVOR,
    candidates: tuple[str, ...] = (SURVIVOR,),
    retag_outcome: RetagOutcome = RETAG_RETAGGED,
    survivor_taken: bool = False,
) -> bool:
    """The fully authorized DICE world, with one term widened at a time."""
    return rekey_is_authorized(
        scenario=scenario,
        owned=owned,
        mirror_answer=mirror_answer,
        candidates=candidates,
        retag_outcome=retag_outcome,
        survivor_taken=survivor_taken,
    )


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    """Every checker owes a planted violation proving it can fail."""

    def test_an_unauthorized_row_move_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_row_moves_only_when_authorized(
                SURVIVOR, authorized=False, expected_survivor=SURVIVOR,
            )

    def test_an_authorized_rekey_that_did_not_land_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_row_moves_only_when_authorized(
                MERGED, authorized=True, expected_survivor=SURVIVOR,
            )

    def test_a_silent_identity_change_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_unauthorized_world_is_unchanged(
                authorized=False,
                row_before={"mb_release_id": MERGED},
                row_after={"mb_release_id": SURVIVOR},
                entry_release_id=MERGED,
                result_json="x",
                result_json_before="x",
            )

    def test_a_drifted_in_flight_identity_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_unauthorized_world_is_unchanged(
                authorized=False,
                row_before={"mb_release_id": MERGED},
                row_after={"mb_release_id": MERGED},
                entry_release_id=SURVIVOR,
                result_json="x",
                result_json_before="x",
            )

    def test_a_rewritten_rejection_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_unauthorized_world_is_unchanged(
                authorized=False,
                row_before={"mb_release_id": MERGED},
                row_after={"mb_release_id": MERGED},
                entry_release_id=MERGED,
                result_json="True|strong_match",
                result_json_before="False|mbid_not_found",
            )

    def test_a_retag_that_saw_a_moved_row_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_retag_never_saw_a_moved_row([SURVIVOR])

    def test_a_retag_invoked_twice_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_retag_never_saw_a_moved_row([MERGED, MERGED])

    def test_a_mirror_call_on_a_healthy_validation_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_mirror_asked_only_where_it_can_be_used(
                [MERGED], scenario="strong_match", owned=True,
            )

    def test_a_mirror_call_without_ownership_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_mirror_asked_only_where_it_can_be_used(
                [MERGED], scenario="mbid_not_found", owned=False,
            )

    def test_two_mirror_calls_are_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_mirror_asked_only_where_it_can_be_used(
                [MERGED, MERGED], scenario="mbid_not_found", owned=True,
            )

    def test_a_replaced_request_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_request_remains_runnable({"status": "replaced"})

    def test_a_parked_status_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_request_remains_runnable({"status": "recovery_required"})

    def test_processing_without_an_owner_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_request_remains_runnable({
                "status": "processing",
                "active_automation_import_job_id": None,
            })

    def test_the_authorization_predicate_rejects_a_widened_term(self) -> None:
        """Known-bad for the derivation itself: each term is load-bearing."""
        self.assertTrue(_authorized())
        widened: tuple[tuple[str, bool], ...] = (
            ("healthy scenario", _authorized(scenario="strong_match")),
            ("no owner", _authorized(owned=False)),
            ("no redirect", _authorized(mirror_answer=None)),
            ("survivor is the stored id", _authorized(mirror_answer=MERGED)),
            ("non-musicbrainz survivor", _authorized(mirror_answer="1870")),
            ("survivor not offered", _authorized(candidates=(UNRELATED,))),
            ("ambiguous retag", _authorized(retag_outcome=RETAG_AMBIGUOUS)),
            ("failed retag", _authorized(retag_outcome=RETAG_FAILED)),
            ("survivor already held", _authorized(survivor_taken=True)),
        )
        for label, authorized in widened:
            with self.subTest(widened=label):
                self.assertFalse(authorized)

    def test_checkers_accept_the_legitimate_dice_rekey(self) -> None:
        """Must-still-work: the real fix passes every checker."""
        check_row_moves_only_when_authorized(
            SURVIVOR, authorized=True, expected_survivor=SURVIVOR,
        )
        check_unauthorized_world_is_unchanged(
            authorized=True,
            row_before={"mb_release_id": MERGED},
            row_after={"mb_release_id": SURVIVOR},
            entry_release_id=SURVIVOR,
            result_json="True|strong_match",
            result_json_before="False|mbid_not_found",
        )
        check_retag_never_saw_a_moved_row([MERGED])
        check_mirror_asked_only_where_it_can_be_used(
            [MERGED], scenario="mbid_not_found", owned=True,
        )
        check_request_remains_runnable({
            "status": "processing",
            "active_automation_import_job_id": 12,
        })


if __name__ == "__main__":
    unittest.main()
