"""Generated property for the operator merge-rekey action (#1089).

The deterministic pins in ``tests/test_merge_rekey_service.py`` and the
real-PostgreSQL transcript in
``tests/test_pipeline_db.py::TestMergeRekeyUnderOperatorClaim`` prove the
exact named worlds; this property patrols the world space around them,
driving the REAL production entry point — ``MergeRekeyService.rekey_request``
— over generated combinations of request eligibility, in-flight import jobs,
rival/collision state, Beets state at BOTH the survivor and the stored id,
and the tagged mirror answer (answered-vs-silent, #1089 BLOCKING-1).

Invariant patrolled (module-level checkers so the known-bad self-tests below
can call them directly):

O1  The operator rekey arm rekeys ONLY an ``imported``, MB-sourced, unowned
    request with a legitimate different-MusicBrainz survivor MusicBrainz
    ANSWERED (never a silent/unavailable mirror), Beets holding exactly one
    album at that survivor AND nothing at the stored id, no
    ``queued``/``running`` import job, no rival request already at the
    survivor, and no colliding evidence fingerprint at the survivor. It
    NEVER fires on ``processing`` / ``replaced`` / other non-``imported``
    statuses, on an automation-owned row, or on a Discogs-sourced /
    identity-less row — the service's own precondition refuses those before
    any mirror or Beets call.
O2  Every world that did not rekey left the row's ``mb_release_id`` and the
    request's evidence lineage exactly where they started.
O3  A world whose request write refuses (an in-flight import job) OR whose
    pre-check refuses (a rival request already at the survivor, or a
    colliding evidence fingerprint) moves NEITHER the row NOR its evidence —
    evidence only ever follows the row.
O4  A rival request or a colliding evidence fingerprint at the survivor is
    reported as ``survivor_collision``, never folded into ``rekey_refused``
    (#1089 MAJOR-2) — the two outcomes are mutually exclusive causes.
"""

from __future__ import annotations

import unittest
from collections.abc import Mapping

from hypothesis import example, given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.import_queue import (
    IMPORT_JOB_FORCE,
    IMPORT_JOB_YOUTUBE,
    force_import_dedupe_key,
    force_import_payload,
    youtube_import_dedupe_key,
    youtube_import_payload,
)
from lib.mb_canonical import (
    CanonicalReleaseAnswer,
    CanonicalReleaseCurrent,
    CanonicalReleaseRedirected,
    CanonicalReleaseUnavailable,
)
from lib.merge_rekey_service import (
    RESULT_LIBRARY_NOT_AT_SURVIVOR,
    RESULT_LIBRARY_STILL_AT_STORED,
    RESULT_MIRROR_UNAVAILABLE,
    RESULT_NOT_MERGED,
    RESULT_REKEY_REFUSED,
    RESULT_REKEYED,
    RESULT_SURVIVOR_COLLISION,
    RESULT_WRONG_STATE,
    MergeRekeyService,
)
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import (
    handoff_automation_owner,
    make_album_quality_evidence,
    make_request_row,
)

MERGED = "6b209cc5-62b0-4ef7-9336-c2dbd876301a"
SURVIVOR = "9b59f78b-3ca6-41e1-8025-6ed4bcfad4e4"
UNRELATED_MB = "11111111-2222-3333-4444-555555555555"
REQUEST_ID = 8792
RIVAL_REQUEST_ID = REQUEST_ID + 1

WORLD_KINDS = st.sampled_from([
    "eligible",
    "wanted",
    "downloading",
    "unsearchable",
    "replaced",
    "processing_owned",
    "discogs_sourced",
    "no_identity",
])
JOB_TYPES = st.sampled_from([IMPORT_JOB_FORCE, IMPORT_JOB_YOUTUBE])
JOB_STATUSES = st.sampled_from(["queued", "running", "completed", "failed"])
#: ``different_mb`` — a legitimate redirect. ``current_no_redirect`` —
#: MusicBrainz ANSWERED and names no different survivor (the #8792
#: refusal). ``unavailable`` — no answer at all (#1089 BLOCKING-1's own
#: dimension: answered-vs-silent). ``same_as_stored`` / ``non_mb`` — a
#: redirect this seam's own defensive re-check must not trust.
SURVIVOR_KINDS = st.sampled_from([
    "different_mb", "current_no_redirect", "unavailable",
    "same_as_stored", "non_mb",
])
BEETS_SURVIVOR_KINDS = st.sampled_from(["unique", "missing", "ambiguous"])
#: #1089 MAJOR-3: Beets state at the STORED (merged-away) id. ``missing`` is
#: the legitimate "library already moved" world; ``present`` is the
#: transplant hazard this dimension exists to patrol.
STORED_ID_BEETS_KINDS = st.sampled_from(["missing", "present"])


def expected_outcome(
    *,
    world_kind: str,
    active_jobs: tuple[tuple[str, str], ...],
    rival_at_survivor: bool,
    fingerprint_collision: bool,
    survivor_kind: str,
    beets_survivor_kind: str,
    stored_id_beets_kind: str,
) -> str:
    """The complete decision, derived independently of production (O1)."""
    if world_kind != "eligible":
        return RESULT_WRONG_STATE
    if survivor_kind == "unavailable":
        return RESULT_MIRROR_UNAVAILABLE
    if survivor_kind != "different_mb":
        return RESULT_NOT_MERGED
    if beets_survivor_kind != "unique":
        return RESULT_LIBRARY_NOT_AT_SURVIVOR
    if stored_id_beets_kind != "missing":
        return RESULT_LIBRARY_STILL_AT_STORED
    if rival_at_survivor or fingerprint_collision:
        return RESULT_SURVIVOR_COLLISION
    has_active_job = any(
        status in ("queued", "running") for _job_type, status in active_jobs
    )
    if has_active_job:
        return RESULT_REKEY_REFUSED
    return RESULT_REKEYED


class RecordingCanonical:
    """Recording TAGGED merge-survivor lookup — see
    :class:`lib.mb_canonical.CanonicalReleaseAnswer`."""

    def __init__(self, answer: CanonicalReleaseAnswer) -> None:
        self._answer = answer
        self.calls: list[str] = []

    def __call__(self, release_id: str) -> CanonicalReleaseAnswer:
        self.calls.append(release_id)
        return self._answer


def _seed_active_jobs(
    db: FakePipelineDB, active_jobs: tuple[tuple[str, str], ...],
) -> None:
    for index, (job_type, status) in enumerate(active_jobs):
        if job_type == IMPORT_JOB_FORCE:
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=REQUEST_ID,
                dedupe_key=force_import_dedupe_key(1000 + index),
                payload=force_import_payload(
                    download_log_id=1000 + index,
                    failed_path=f"/quarantine/slipknot-{index}",
                ),
            )
        else:
            job = db.enqueue_import_job(
                IMPORT_JOB_YOUTUBE,
                request_id=REQUEST_ID,
                dedupe_key=youtube_import_dedupe_key(2000 + index),
                payload=youtube_import_payload(
                    staged_path=f"/Incoming/auto-import/slipknot-{index}",
                    request_id=REQUEST_ID,
                    browse_id=f"MPREb_{index}",
                    download_log_id=2000 + index,
                ),
            )
        if status == "queued":
            continue
        if status == "running":
            db.mark_import_job_preview_importable(
                job.id, preview_result={}, message="ready",
            )
            if job_type == IMPORT_JOB_FORCE:
                claimed = db.claim_force_import_job_under_lock(
                    job.id, request_id=REQUEST_ID, worker_id="prop-test",
                )
            else:
                claimed = db.claim_import_job_candidate(
                    job.id, worker_id="prop-test",
                )
            assert claimed is not None and claimed.status == "running"
        elif status == "completed":
            db.mark_import_job_completed(job.id, result={}, message="done")
        elif status == "failed":
            db.mark_import_job_failed(job.id, error="synthetic failure")


def _build_world(
    *,
    world_kind: str,
    active_jobs: tuple[tuple[str, str], ...],
    rival_at_survivor: bool,
    fingerprint_collision: bool,
    survivor_kind: str,
    beets_survivor_kind: str,
    stored_id_beets_kind: str,
) -> tuple[FakePipelineDB, FakeBeetsDB, RecordingCanonical, int | None]:
    """Returns ``(db, beets, canonical, seeded_evidence_id)``.

    ``seeded_evidence_id`` is ``None`` for ``no_identity`` — a request with
    no release identity at all has nothing content-addressed evidence could
    be filed under, so O2/O3 (row and evidence move together) is vacuous for
    that world and the test skips the evidence half of the check.
    """
    db = FakePipelineDB()
    identity_for_evidence: str | None = MERGED
    if world_kind == "processing_owned":
        db.seed_request(make_request_row(
            id=REQUEST_ID, mb_release_id=MERGED, status="wanted",
            artist_name="Slipknot", album_title="Vol. 3",
        ))
        handoff_automation_owner(
            db,
            REQUEST_ID,
            state={
                "filetype": "flac",
                "enqueued_at": "2026-08-13T00:00:00+00:00",
                "current_path": "/processing/albums/slipknot",
                "files": [],
            },
            canonical_path="/processing/albums/slipknot",
        )
    elif world_kind == "discogs_sourced":
        identity_for_evidence = "1870"
        db.seed_request(make_request_row(
            id=REQUEST_ID, mb_release_id="1870", discogs_release_id="1870",
            status="imported", active_automation_import_job_id=None,
            artist_name="Slipknot", album_title="Vol. 3",
        ))
    elif world_kind == "no_identity":
        identity_for_evidence = None
        db.seed_request(make_request_row(
            id=REQUEST_ID, mb_release_id=None, discogs_release_id=None,
            status="imported", active_automation_import_job_id=None,
            artist_name="Slipknot", album_title="Vol. 3",
        ))
    else:
        status = "imported" if world_kind == "eligible" else world_kind
        db.seed_request(make_request_row(
            id=REQUEST_ID, mb_release_id=MERGED, status=status,
            active_automation_import_job_id=None,
            artist_name="Slipknot", album_title="Vol. 3",
        ))

    evidence_id: int | None = None
    if identity_for_evidence is not None:
        evidence = make_album_quality_evidence(
            mb_release_id=identity_for_evidence,
            source_path="/library/slipknot",
        )
        db.upsert_album_quality_evidence(evidence)
        stored = db.find_album_quality_evidence(
            mb_release_id=identity_for_evidence,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        evidence_id = stored.id

    beets = FakeBeetsDB()
    survivor_answer: CanonicalReleaseAnswer = {
        "different_mb": CanonicalReleaseRedirected(SURVIVOR),
        "current_no_redirect": CanonicalReleaseCurrent(),
        "unavailable": CanonicalReleaseUnavailable(),
        "same_as_stored": CanonicalReleaseRedirected(MERGED),
        "non_mb": CanonicalReleaseRedirected("1870"),
    }[survivor_kind]
    canonical = RecordingCanonical(survivor_answer)

    if world_kind == "eligible" and survivor_kind == "different_mb":
        _seed_active_jobs(db, active_jobs)
        if rival_at_survivor:
            db.seed_request(make_request_row(
                id=RIVAL_REQUEST_ID, mb_release_id=SURVIVOR,
                artist_name="Slipknot", album_title="Vol. 3 (rival pressing)",
            ))
        if fingerprint_collision:
            db.upsert_album_quality_evidence(make_album_quality_evidence(
                mb_release_id=SURVIVOR, source_path="/library/slipknot",
            ))
        beets_ids = {
            "unique": [19345],
            "missing": [],
            "ambiguous": [19345, 19999],
        }[beets_survivor_kind]
        beets.set_album_ids_for_release(SURVIVOR, beets_ids)
        if stored_id_beets_kind == "present":
            beets.set_album_ids_for_release(MERGED, [111])
        # "missing" needs no seed — an unseeded release id already
        # resolves to CurrentBeetsMissing in FakeBeetsDB by default.

    return db, beets, canonical, evidence_id


def check_row_and_evidence_move_only_when_rekeyed(
    *,
    outcome: str,
    row_before: Mapping[str, object],
    row_after: Mapping[str, object] | None,
    evidence_release_after: str | None,
    evidence_seeded: bool = True,
) -> None:
    """O2/O3 — the row and its evidence move together, or not at all.

    ``evidence_seeded=False`` skips the evidence half entirely — the
    ``no_identity`` world has no release identity to file evidence under in
    the first place, so O2/O3 is vacuous for it.
    """
    if row_after is None:
        raise AssertionError("the request row disappeared")
    moved = row_after.get("mb_release_id") != row_before.get("mb_release_id")
    if outcome == RESULT_REKEYED:
        if not moved:
            raise AssertionError(
                "a rekeyed outcome left mb_release_id unchanged"
            )
        if evidence_seeded and evidence_release_after != row_after.get(
            "mb_release_id",
        ):
            raise AssertionError(
                f"the row moved to {row_after.get('mb_release_id')!r} but "
                f"its evidence is still at {evidence_release_after!r}"
            )
    else:
        if moved:
            raise AssertionError(
                f"outcome {outcome!r} moved mb_release_id from "
                f"{row_before.get('mb_release_id')!r} to "
                f"{row_after.get('mb_release_id')!r}"
            )
        if evidence_seeded and evidence_release_after != row_before.get(
            "mb_release_id",
        ):
            raise AssertionError(
                f"outcome {outcome!r} moved the evidence to "
                f"{evidence_release_after!r} without moving the row"
            )


def check_collision_and_refused_are_mutually_exclusive_causes(
    *,
    outcome: str,
    rival_at_survivor: bool,
    fingerprint_collision: bool,
    has_active_job: bool,
) -> None:
    """O4 — #1089 MAJOR-2: a permanent collision must never be reported as
    the transient ``rekey_refused``, and vice versa."""
    if (rival_at_survivor or fingerprint_collision) and outcome == RESULT_REKEY_REFUSED:
        raise AssertionError(
            "a rival/fingerprint collision was reported as the transient "
            "rekey_refused outcome instead of survivor_collision"
        )
    if (
        has_active_job
        and not rival_at_survivor
        and not fingerprint_collision
        and outcome == RESULT_SURVIVOR_COLLISION
    ):
        raise AssertionError(
            "an in-flight import job with no collision was reported as "
            "survivor_collision instead of the transient rekey_refused"
        )


class TestMergeRekeyServiceProperty(unittest.TestCase):
    @settings(deadline=None)
    @given(
        world_kind=WORLD_KINDS,
        active_jobs=st.lists(
            st.tuples(JOB_TYPES, JOB_STATUSES), max_size=2,
        ).map(tuple).filter(
            # Production (mirrored by the fake) allows at most one active
            # youtube_import per request; a fixture-only concern, not part
            # of the invariant this property patrols.
            lambda jobs: sum(
                1 for job_type, _status in jobs if job_type == IMPORT_JOB_YOUTUBE
            ) <= 1,
        ),
        rival_at_survivor=st.booleans(),
        fingerprint_collision=st.booleans(),
        survivor_kind=SURVIVOR_KINDS,
        beets_survivor_kind=BEETS_SURVIVOR_KINDS,
        stored_id_beets_kind=STORED_ID_BEETS_KINDS,
    )
    # The clean happy path.
    @example(
        world_kind="eligible", active_jobs=(), rival_at_survivor=False,
        fingerprint_collision=False, survivor_kind="different_mb",
        beets_survivor_kind="unique", stored_id_beets_kind="missing",
    )
    # The #8792 refusal — MusicBrainz ANSWERED, no redirect.
    @example(
        world_kind="eligible", active_jobs=(), rival_at_survivor=False,
        fingerprint_collision=False, survivor_kind="current_no_redirect",
        beets_survivor_kind="unique", stored_id_beets_kind="missing",
    )
    # #1089 BLOCKING-1: configured but down/silent — distinct from the
    # #8792 refusal above.
    @example(
        world_kind="eligible", active_jobs=(), rival_at_survivor=False,
        fingerprint_collision=False, survivor_kind="unavailable",
        beets_survivor_kind="unique", stored_id_beets_kind="missing",
    )
    # #1089 MAJOR-3: Beets has not moved yet — the transplant hazard.
    @example(
        world_kind="eligible", active_jobs=(), rival_at_survivor=False,
        fingerprint_collision=False, survivor_kind="different_mb",
        beets_survivor_kind="unique", stored_id_beets_kind="present",
    )
    # #1089 MAJOR-2: a rival request already at the survivor.
    @example(
        world_kind="eligible", active_jobs=(), rival_at_survivor=True,
        fingerprint_collision=False, survivor_kind="different_mb",
        beets_survivor_kind="unique", stored_id_beets_kind="missing",
    )
    # #1089 MAJOR-2: a colliding evidence fingerprint.
    @example(
        world_kind="eligible", active_jobs=(), rival_at_survivor=False,
        fingerprint_collision=True, survivor_kind="different_mb",
        beets_survivor_kind="unique", stored_id_beets_kind="missing",
    )
    # A queued force import blocks the write.
    @example(
        world_kind="eligible", active_jobs=((IMPORT_JOB_FORCE, "queued"),),
        rival_at_survivor=False, fingerprint_collision=False,
        survivor_kind="different_mb", beets_survivor_kind="unique",
        stored_id_beets_kind="missing",
    )
    # An in-flight rescue blocks it too — no job_type filter.
    @example(
        world_kind="eligible",
        active_jobs=((IMPORT_JOB_YOUTUBE, "running"),),
        rival_at_survivor=False, fingerprint_collision=False,
        survivor_kind="different_mb", beets_survivor_kind="unique",
        stored_id_beets_kind="missing",
    )
    # A terminal job never blocks (must-still-work).
    @example(
        world_kind="eligible",
        active_jobs=((IMPORT_JOB_FORCE, "completed"),),
        rival_at_survivor=False, fingerprint_collision=False,
        survivor_kind="different_mb", beets_survivor_kind="unique",
        stored_id_beets_kind="missing",
    )
    def test_every_world_upholds_the_operator_rekey_invariants(
        self,
        world_kind: str,
        active_jobs: tuple[tuple[str, str], ...],
        rival_at_survivor: bool,
        fingerprint_collision: bool,
        survivor_kind: str,
        beets_survivor_kind: str,
        stored_id_beets_kind: str,
    ) -> None:
        db, beets, canonical, evidence_id = _build_world(
            world_kind=world_kind,
            active_jobs=active_jobs,
            rival_at_survivor=rival_at_survivor,
            fingerprint_collision=fingerprint_collision,
            survivor_kind=survivor_kind,
            beets_survivor_kind=beets_survivor_kind,
            stored_id_beets_kind=stored_id_beets_kind,
        )
        row_before = dict(db.request(REQUEST_ID) or {})
        service = MergeRekeyService(
            db, beets, canonical_release_fn=canonical,
        )

        result = service.rekey_request(REQUEST_ID)

        expected = expected_outcome(
            world_kind=world_kind,
            active_jobs=active_jobs,
            rival_at_survivor=rival_at_survivor,
            fingerprint_collision=fingerprint_collision,
            survivor_kind=survivor_kind,
            beets_survivor_kind=beets_survivor_kind,
            stored_id_beets_kind=stored_id_beets_kind,
        )
        self.assertEqual(result.outcome, expected)

        row_after = db.request(REQUEST_ID)
        evidence = (
            db.load_album_quality_evidence_by_id(evidence_id)
            if evidence_id is not None else None
        )
        check_row_and_evidence_move_only_when_rekeyed(
            outcome=result.outcome,
            row_before=row_before,
            row_after=row_after,
            evidence_release_after=(
                evidence.mb_release_id if evidence is not None else None
            ),
            evidence_seeded=evidence_id is not None,
        )
        check_collision_and_refused_are_mutually_exclusive_causes(
            outcome=result.outcome,
            rival_at_survivor=rival_at_survivor,
            fingerprint_collision=fingerprint_collision,
            has_active_job=any(
                status in ("queued", "running")
                for _job_type, status in active_jobs
            ),
        )


class TestInvariantCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests for the checkers above."""

    def test_a_rekeyed_outcome_with_an_unmoved_row_is_rejected(self) -> None:
        with self.assertRaisesRegex(AssertionError, "left mb_release_id unchanged"):
            check_row_and_evidence_move_only_when_rekeyed(
                outcome=RESULT_REKEYED,
                row_before={"mb_release_id": MERGED},
                row_after={"mb_release_id": MERGED},
                evidence_release_after=MERGED,
            )

    def test_a_rekeyed_outcome_with_stranded_evidence_is_rejected(self) -> None:
        with self.assertRaisesRegex(AssertionError, "still at"):
            check_row_and_evidence_move_only_when_rekeyed(
                outcome=RESULT_REKEYED,
                row_before={"mb_release_id": MERGED},
                row_after={"mb_release_id": SURVIVOR},
                evidence_release_after=MERGED,
            )

    def test_a_refused_outcome_that_moved_the_row_is_rejected(self) -> None:
        with self.assertRaisesRegex(AssertionError, "moved mb_release_id"):
            check_row_and_evidence_move_only_when_rekeyed(
                outcome=RESULT_REKEY_REFUSED,
                row_before={"mb_release_id": MERGED},
                row_after={"mb_release_id": SURVIVOR},
                evidence_release_after=MERGED,
            )

    def test_a_refused_outcome_that_moved_only_the_evidence_is_rejected(
        self,
    ) -> None:
        with self.assertRaisesRegex(AssertionError, "moved the evidence"):
            check_row_and_evidence_move_only_when_rekeyed(
                outcome=RESULT_WRONG_STATE,
                row_before={"mb_release_id": MERGED},
                row_after={"mb_release_id": MERGED},
                evidence_release_after=SURVIVOR,
            )

    def test_a_missing_row_is_rejected(self) -> None:
        with self.assertRaisesRegex(AssertionError, "disappeared"):
            check_row_and_evidence_move_only_when_rekeyed(
                outcome=RESULT_REKEYED,
                row_before={"mb_release_id": MERGED},
                row_after=None,
                evidence_release_after=SURVIVOR,
            )

    def test_the_legitimate_rekey_and_the_untouched_world_both_pass(self) -> None:
        check_row_and_evidence_move_only_when_rekeyed(
            outcome=RESULT_REKEYED,
            row_before={"mb_release_id": MERGED},
            row_after={"mb_release_id": SURVIVOR},
            evidence_release_after=SURVIVOR,
        )
        check_row_and_evidence_move_only_when_rekeyed(
            outcome=RESULT_WRONG_STATE,
            row_before={"mb_release_id": MERGED},
            row_after={"mb_release_id": MERGED},
            evidence_release_after=MERGED,
        )

    def test_a_collision_reported_as_refused_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            "rival/fingerprint collision was reported as the transient",
        ):
            check_collision_and_refused_are_mutually_exclusive_causes(
                outcome=RESULT_REKEY_REFUSED,
                rival_at_survivor=True,
                fingerprint_collision=False,
                has_active_job=False,
            )

    def test_an_active_job_reported_as_collision_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            "in-flight import job with no collision was reported as",
        ):
            check_collision_and_refused_are_mutually_exclusive_causes(
                outcome=RESULT_SURVIVOR_COLLISION,
                rival_at_survivor=False,
                fingerprint_collision=False,
                has_active_job=True,
            )

    def test_the_legitimate_collision_and_refusal_both_pass(self) -> None:
        check_collision_and_refused_are_mutually_exclusive_causes(
            outcome=RESULT_SURVIVOR_COLLISION,
            rival_at_survivor=True,
            fingerprint_collision=False,
            has_active_job=False,
        )
        check_collision_and_refused_are_mutually_exclusive_causes(
            outcome=RESULT_REKEY_REFUSED,
            rival_at_survivor=False,
            fingerprint_collision=False,
            has_active_job=True,
        )

    def test_the_expected_outcome_derivation_rejects_a_widened_term(self) -> None:
        """Known-bad for the derivation itself: each term is load-bearing."""

        def outcome(
            *,
            world_kind: str = "eligible",
            active_jobs: tuple[tuple[str, str], ...] = (),
            rival_at_survivor: bool = False,
            fingerprint_collision: bool = False,
            survivor_kind: str = "different_mb",
            beets_survivor_kind: str = "unique",
            stored_id_beets_kind: str = "missing",
        ) -> str:
            """The fully-authorized baseline world, one term widened at a
            time — explicit keyword defaults, never ``**dict`` unpacking, so
            each override stays statically typed against
            ``expected_outcome``'s own signature."""
            return expected_outcome(
                world_kind=world_kind,
                active_jobs=active_jobs,
                rival_at_survivor=rival_at_survivor,
                fingerprint_collision=fingerprint_collision,
                survivor_kind=survivor_kind,
                beets_survivor_kind=beets_survivor_kind,
                stored_id_beets_kind=stored_id_beets_kind,
            )

        self.assertEqual(outcome(), RESULT_REKEYED)
        widened: tuple[tuple[str, str], ...] = (
            ("non-eligible world", outcome(world_kind="wanted")),
            ("processing owned", outcome(world_kind="processing_owned")),
            (
                "mirror unavailable",
                outcome(survivor_kind="unavailable"),
            ),
            ("no redirect", outcome(survivor_kind="current_no_redirect")),
            ("same as stored", outcome(survivor_kind="same_as_stored")),
            ("non-MB survivor", outcome(survivor_kind="non_mb")),
            (
                "library missing the survivor",
                outcome(beets_survivor_kind="missing"),
            ),
            (
                "library ambiguous at the survivor",
                outcome(beets_survivor_kind="ambiguous"),
            ),
            (
                "library still at the stored id",
                outcome(stored_id_beets_kind="present"),
            ),
            (
                "a queued job is active",
                outcome(active_jobs=((IMPORT_JOB_FORCE, "queued"),)),
            ),
            (
                "a running job is active",
                outcome(active_jobs=((IMPORT_JOB_YOUTUBE, "running"),)),
            ),
            ("rival at survivor", outcome(rival_at_survivor=True)),
            (
                "fingerprint collision",
                outcome(fingerprint_collision=True),
            ),
        )
        for label, world_outcome in widened:
            with self.subTest(widened=label):
                self.assertNotEqual(world_outcome, RESULT_REKEYED)
        # And the two new #1089 outcomes are exactly which term fires.
        self.assertEqual(
            outcome(survivor_kind="unavailable"), RESULT_MIRROR_UNAVAILABLE,
        )
        self.assertEqual(
            outcome(stored_id_beets_kind="present"),
            RESULT_LIBRARY_STILL_AT_STORED,
        )
        self.assertEqual(
            outcome(rival_at_survivor=True), RESULT_SURVIVOR_COLLISION,
        )
        self.assertEqual(
            outcome(fingerprint_collision=True), RESULT_SURVIVOR_COLLISION,
        )
        # A terminal job must NOT be mistaken for an active one.
        self.assertEqual(
            outcome(active_jobs=((IMPORT_JOB_FORCE, "completed"),)),
            RESULT_REKEYED,
        )


if __name__ == "__main__":
    unittest.main()
