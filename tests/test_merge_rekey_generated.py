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
P6  The request's linked current evidence ALWAYS names the request's own
    release id. Evidence is content-addressed by
    ``(mb_release_id, snapshot_fingerprint)``, so a rekey that leaves it
    behind strands the verified-lossless proof: the HAVE rebuild drops it,
    ``_refresh_current_evidence_after_import`` returns ``identity_mismatch``,
    and the quality gate reopens full-tier search on the import this seam
    exists to enable.
P7  The library retag is only ever observed with BOTH release identities'
    ``RELEASE`` advisory locks held. The retag mutates two release identities
    at once and the destructive operator lanes fence per release from other
    processes; contention keeps today's rejection instead of waiting.
P11 The library and the request never disagree about which release this is.
    Every other invariant here watches the ROW; this one watches the pair.
    A retag that succeeds is durable, so a seam that retags and is THEN
    refused the rekey leaves the installed album at the survivor and the
    request at the merged-away id — and nothing re-derives it, because the
    collision that refused the write is still there on the next attempt,
    which is now refused at the pre-check before the library is read at all.
    The seam therefore reads both ``UniqueViolation`` refusal causes BEFORE
    it retags, and the property watches the resulting pair in every world.
P12 A merge refusal no retry can clear is recorded durably, in whichever
    lane met it. An occupied survivor stays occupied until an operator acts,
    so every later attempt is refused identically — and force, which imports
    despite the verdict, then reports a bare ``mbid_missing`` from
    ``import_one.py`` with nothing naming the merge. The blocked world
    therefore owes exactly one ``download_log`` row carrying the producer's
    sentence, and no other world may carry it.
"""

from __future__ import annotations

import contextlib
import logging
import os
import tempfile
import unittest
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from unittest.mock import patch

from hypothesis import example, given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.beets import FORCE_IMPORT_DISTANCE_THRESHOLD
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
from lib.download_validation import (
    _process_beets_validation,
    merge_rekey_blocked_audit_message,
)
from lib.pipeline_db import (
    ADVISORY_LOCK_NAMESPACE_RELEASE,
    MergeRekeyCollision,
    release_id_to_lock_key,
)
from lib.pipeline_db._shared import REQUEST_STATUSES
from lib.quality import (
    AudioQualityMeasurement,
    ValidationResult,
    VerifiedLosslessProof,
)
from lib.release_identity import ReleaseIdentity
from lib.staged_album import StagedAlbum
from tests.dispatch_helpers import handoff_automation_owner
from tests.evidence_helpers import make_album_quality_evidence
from tests.fakes import FakePipelineDB
from tests.helpers import (
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

#: Which RELEASE advisory lock (if any) another process already holds. The
#: destructive operator lanes take these per release from OTHER processes, so
#: either side can be contended independently.
RELEASE_LOCK_STATES = st.sampled_from(["free", "old_held", "new_held"])


@contextlib.contextmanager
def _silence_logs() -> Iterator[None]:
    previous = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        yield
    finally:
        logging.disable(previous)


def _link_current_evidence(db: FakePipelineDB, release_id: str) -> None:
    """Give the request a linked HAVE row carrying a verified-lossless proof.

    ``source_path`` is deliberately the library path, never the staged
    download folder, so this row is the request's CURRENT evidence and can
    never be mistaken for the action's candidate evidence.
    """
    evidence = make_album_quality_evidence(
        mb_release_id=release_id,
        source_path=f"/library/{release_id}",
        measurement=AudioQualityMeasurement(
            min_bitrate_kbps=900,
            avg_bitrate_kbps=950,
            median_bitrate_kbps=940,
            format="FLAC",
        ),
        codec="flac",
        container="flac",
        storage_format="FLAC",
        verified_lossless_proof=VerifiedLosslessProof(
            provenance="measured",
            source="flac",
            classifier="spectral_verified_lossless",
            detail="genuine",
        ),
    )
    db.upsert_album_quality_evidence(evidence)
    stored = db.find_album_quality_evidence(
        mb_release_id=release_id,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
    )
    assert stored is not None and stored.id is not None
    assert db.set_request_current_evidence(REQUEST_ID, stored.id)


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


def retag_is_authorized(
    *,
    scenario: str,
    owned: bool,
    mirror_answer: str | None,
    candidates: tuple[str, ...],
    release_lock_state: str,
    survivor_taken: bool = False,
) -> bool:
    """Everything that must hold before Beets may be mutated at all.

    ``survivor_taken`` is a term of THIS predicate, not only of the rekey's:
    a rival already holding the survivor is a documented refusal of the write,
    it is knowable by a plain read, and mutating the shared library for a
    rekey that is already refused is what leaves the library and the request
    disagreeing (P11).
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
    if survivor_taken:
        return False
    return release_lock_state == "free"


def library_before_retag(
    retag_outcome: str,
    survivor: str | None,
) -> str | None:
    """Where the installed album is filed when the seam arrives.

    Derived from the outcome the library authority reports, because that
    outcome IS a statement about the library's placement:
    ``already_current`` means it was at the survivor before we asked,
    ``not_held`` means the library holds neither identity, and every other
    outcome describes an album still filed under the merged-away id — which
    is the upgrade world this whole seam exists for.
    """
    if retag_outcome == RETAG_ALREADY_CURRENT:
        return survivor
    if retag_outcome == RETAG_NOT_HELD:
        return None
    return MERGED


def library_after_retag(
    retag_outcome: str,
    survivor: str | None,
    *,
    retag_ran: bool,
) -> str | None:
    """Where the installed album is filed once the seam is done with it."""
    before = library_before_retag(retag_outcome, survivor)
    if retag_ran and retag_outcome == RETAG_RETAGGED:
        return survivor
    return before


def rekey_is_authorized(
    *,
    scenario: str,
    owned: bool,
    mirror_answer: str | None,
    candidates: tuple[str, ...],
    retag_outcome: str,
    survivor_taken: bool,
    release_lock_state: str = "free",
) -> bool:
    """The complete conjunction that permits moving ``mb_release_id``.

    Derived from the world, independently of the production code, so a
    production change that widens ANY term is a property failure rather than a
    silently agreeing reimplementation.
    """
    if not retag_is_authorized(
        scenario=scenario,
        owned=owned,
        mirror_answer=mirror_answer,
        candidates=candidates,
        release_lock_state=release_lock_state,
        survivor_taken=survivor_taken,
    ):
        return False
    return retag_outcome in RETAG_READY_OUTCOMES


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


def check_current_evidence_follows_the_request(
    *,
    request_release_id: object,
    evidence_release_id: str | None,
) -> None:
    """P6 — the linked HAVE row always names the request's own pressing."""
    if evidence_release_id is None:
        raise AssertionError(
            "the request lost its linked current evidence entirely"
        )
    if evidence_release_id != request_release_id:
        raise AssertionError(
            f"the request now names {request_release_id!r} while its linked "
            f"current evidence is still filed at {evidence_release_id!r} — "
            "the verified-lossless proof is stranded, the HAVE rebuild will "
            "drop it, and the quality gate will reopen full-tier search"
        )


def check_library_and_request_agree(
    *,
    library_before: str | None,
    library_after: str | None,
    stored_before: str | None,
    stored_after: str | None,
) -> None:
    """P11 — the seam never creates a library/request identity disagreement.

    Watches the PAIR, which is the one thing no other checker here can see:
    P1/P2 watch the row, P3 watches the retag's view of the row, and both are
    satisfied by a world where the library moved and the row correctly did
    not. That world is the durable split — nothing re-derives it, because the
    collision that refused the write is still there on the next attempt,
    which the occupancy pre-check now refuses before the library is read at
    all.

    A disagreement that was already there when the seam arrived is not this
    seam's doing (it is the residue of an earlier one, and it is what the
    audit row records), so the checker fires only when this execution moved
    the library or the row.
    """
    if library_after == stored_after:
        return
    if library_after is None:
        # The library holds neither identity; there is nothing to disagree.
        return
    if library_after == library_before and stored_after == stored_before:
        # Arrived divergent, left it exactly so.
        return
    raise AssertionError(
        "the merge seam left the installed album filed at "
        f"{library_after!r} while the request names {stored_after!r} "
        f"(library was {library_before!r}, request was {stored_before!r}): "
        "nothing repairs that split and nothing re-derives it — the next "
        "attempt is refused at the occupancy pre-check, before it ever "
        "reads the library"
    )


def check_retag_ran_under_both_release_locks(
    observed_locks: list[tuple[tuple[int, int], ...]],
    *,
    survivor: str | None,
) -> None:
    """P7 — Beets is only mutated with both release identities fenced."""
    if not observed_locks:
        return
    if survivor is None:
        raise AssertionError("the retag ran with no survivor identity")
    required = {
        (ADVISORY_LOCK_NAMESPACE_RELEASE, release_id_to_lock_key(MERGED)),
        (ADVISORY_LOCK_NAMESPACE_RELEASE, release_id_to_lock_key(survivor)),
    }
    for held in observed_locks:
        missing = required - set(held)
        if missing:
            raise AssertionError(
                "the library retag mutated Beets without holding "
                f"{sorted(missing)!r}: an operator Bad Rip or library-delete "
                "on either identity could bind to the album being retagged"
            )


class _RecordingCanonical:
    def __init__(self, answer: str | None) -> None:
        self._answer = answer
        self.calls: list[str] = []

    def __call__(self, release_id: str) -> str | None:
        self.calls.append(release_id)
        return self._answer


class _RecordingRetag:
    """Reports one outcome and snapshots the world it ran under (P3, P7)."""

    def __init__(self, db: FakePipelineDB, outcome: RetagOutcome) -> None:
        self._db = db
        self._outcome: RetagOutcome = outcome
        self.observed_release_ids: list[str | None] = []
        self.observed_locks: list[tuple[tuple[int, int], ...]] = []

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
        self.observed_locks.append(tuple(self._db.advisory_lock_calls))
        return BeetsRetagResult(outcome=self._outcome, detail="generated world")


def _validation_result(
    scenario: str,
    candidates: tuple[str, ...],
    *,
    survivor_distance: float = 0.04,
) -> ValidationResult:
    """The exact result shape ``beets_validate`` returns for each scenario.

    ``survivor_distance`` is carried by EVERY candidate, so whichever one a
    world's mirror names is the one the threshold has to judge (#1080's P9).
    """
    summaries = [
        candidate(mbid, distance=survivor_distance) for mbid in candidates
    ]
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
        release_lock_state=RELEASE_LOCK_STATES,
    )
    # The DICE world (request 346), the ordering-critical world, and the two
    # that motivated the READY-membership gate.
    @example(
        scenario="mbid_not_found", mirror_answer=SURVIVOR,
        candidates=(SURVIVOR,), retag_outcome=RETAG_RETAGGED,
        owned=True, survivor_taken=False, release_lock_state="free",
    )
    @example(
        scenario="mbid_not_found", mirror_answer=SURVIVOR,
        candidates=(SURVIVOR,), retag_outcome=RETAG_AMBIGUOUS,
        owned=True, survivor_taken=False, release_lock_state="free",
    )
    @example(
        scenario="mbid_not_found", mirror_answer=SURVIVOR,
        candidates=(SURVIVOR,), retag_outcome=RETAG_RETAGGED,
        owned=True, survivor_taken=True, release_lock_state="free",
    )
    @example(
        scenario="strong_match", mirror_answer=SURVIVOR,
        candidates=(SURVIVOR,), retag_outcome=RETAG_RETAGGED,
        owned=True, survivor_taken=False, release_lock_state="free",
    )
    # Shrunk by the 2026-08-11 fuzz burst: the survivor is an arbitrary MB id
    # rather than the fixture's constant, and a rival request holds THAT id.
    # The world where the fixture and the authorization derivation can drift.
    @example(
        scenario="mbid_not_found", mirror_answer=UNRELATED,
        candidates=(UNRELATED,), retag_outcome=RETAG_RETAGGED,
        owned=True, survivor_taken=True, release_lock_state="free",
    )
    # An operator destructive action already fences each identity in turn:
    # the survivor's lock is the one a Bad Rip on "the album at the survivor"
    # would hold while the retag was mid-flight.
    @example(
        scenario="mbid_not_found", mirror_answer=SURVIVOR,
        candidates=(SURVIVOR,), retag_outcome=RETAG_RETAGGED,
        owned=True, survivor_taken=False, release_lock_state="new_held",
    )
    @example(
        scenario="mbid_not_found", mirror_answer=SURVIVOR,
        candidates=(SURVIVOR,), retag_outcome=RETAG_RETAGGED,
        owned=True, survivor_taken=False, release_lock_state="old_held",
    )
    def test_every_world_upholds_the_merge_rekey_invariants(
        self,
        scenario: str,
        mirror_answer: str | None,
        candidates: tuple[str, ...],
        retag_outcome: RetagOutcome,
        owned: bool,
        survivor_taken: bool,
        release_lock_state: str,
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
            # P6: the request always carries a linked HAVE row with a proof.
            # Whatever the seam does to the identity, the evidence must end up
            # naming the same pressing the request does.
            _link_current_evidence(db, MERGED)
            # P7: another process may already hold either release lock.
            survivor_id = _taken_survivor_id(mirror_answer)
            held_key = {
                "free": None,
                "old_held": release_id_to_lock_key(MERGED),
                "new_held": (
                    None if survivor_id is None
                    else release_id_to_lock_key(survivor_id)
                ),
            }[release_lock_state]
            if held_key is not None:
                db.set_advisory_lock_result(
                    lambda namespace, key, held_key=held_key: not (
                        namespace == ADVISORY_LOCK_NAMESPACE_RELEASE
                        and key == held_key
                    )
                )
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
            current_evidence_id = db.get_request_current_evidence_id(REQUEST_ID)
            linked_evidence = (
                db.load_album_quality_evidence_by_id(current_evidence_id)
                if current_evidence_id is not None
                else None
            )
            evidence_release_after = (
                None if linked_evidence is None
                else linked_evidence.mb_release_id
            )

        authorized = rekey_is_authorized(
            scenario=scenario,
            owned=owned,
            mirror_answer=mirror_answer,
            candidates=candidates,
            retag_outcome=retag_outcome,
            survivor_taken=survivor_taken,
            release_lock_state=release_lock_state,
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
        check_current_evidence_follows_the_request(
            request_release_id=row_after.get("mb_release_id"),
            evidence_release_id=evidence_release_after,
        )
        check_retag_ran_under_both_release_locks(
            retag.observed_locks,
            survivor=_taken_survivor_id(mirror_answer),
        )
        survivor_id = _taken_survivor_id(mirror_answer)
        retag_ran = bool(retag.observed_release_ids)
        check_library_and_request_agree(
            library_before=library_before_retag(retag_outcome, survivor_id),
            library_after=library_after_retag(
                retag_outcome, survivor_id, retag_ran=retag_ran,
            ),
            stored_before=row_before.get("mb_release_id"),
            stored_after=row_after.get("mb_release_id"),
        )
        if not retag_is_authorized(
            scenario=scenario,
            owned=owned,
            mirror_answer=mirror_answer,
            candidates=candidates,
            release_lock_state=release_lock_state,
            survivor_taken=survivor_taken,
        ) and retag.observed_locks:
            raise AssertionError(
                "the library was retagged in a world that authorized no "
                "Beets mutation at all"
            )


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
    release_lock_state: str = "free",
) -> bool:
    """The fully authorized DICE world, with one term widened at a time."""
    return rekey_is_authorized(
        scenario=scenario,
        owned=owned,
        mirror_answer=mirror_answer,
        candidates=candidates,
        retag_outcome=retag_outcome,
        survivor_taken=survivor_taken,
        release_lock_state=release_lock_state,
    )


def _both_release_locks() -> tuple[tuple[int, int], ...]:
    return (
        (ADVISORY_LOCK_NAMESPACE_RELEASE, release_id_to_lock_key(MERGED)),
        (ADVISORY_LOCK_NAMESPACE_RELEASE, release_id_to_lock_key(SURVIVOR)),
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
            (
                "old release lock contended",
                _authorized(release_lock_state="old_held"),
            ),
            (
                "survivor release lock contended",
                _authorized(release_lock_state="new_held"),
            ),
        )
        for label, authorized in widened:
            with self.subTest(widened=label):
                self.assertFalse(authorized)
        # And the RETAG predicate carries the rival term too, not only the
        # rekey's: mutating the shared library for a write that is already
        # refused is what creates the split (P11).
        self.assertTrue(retag_is_authorized(
            scenario="mbid_not_found",
            owned=True,
            mirror_answer=SURVIVOR,
            candidates=(SURVIVOR,),
            release_lock_state="free",
        ))
        self.assertFalse(retag_is_authorized(
            scenario="mbid_not_found",
            owned=True,
            mirror_answer=SURVIVOR,
            candidates=(SURVIVOR,),
            release_lock_state="free",
            survivor_taken=True,
        ))

    def test_stranded_current_evidence_is_rejected(self) -> None:
        """P6 known-bad: the row moved and its evidence did not."""
        with self.assertRaises(AssertionError) as caught:
            check_current_evidence_follows_the_request(
                request_release_id=SURVIVOR, evidence_release_id=MERGED,
            )
        self.assertIn("stranded", str(caught.exception))

    def test_a_request_with_no_linked_evidence_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_current_evidence_follows_the_request(
                request_release_id=SURVIVOR, evidence_release_id=None,
            )

    def test_a_moved_library_with_an_unmoved_request_is_rejected(self) -> None:
        """P11 known-bad: exactly the #1080 split state.

        The library was retagged onto the survivor and the rekey was then
        refused — the world every other checker here calls legal.
        """
        with self.assertRaises(AssertionError) as caught:
            check_library_and_request_agree(
                library_before=MERGED,
                library_after=SURVIVOR,
                stored_before=MERGED,
                stored_after=MERGED,
            )
        self.assertIn("nothing repairs that split", str(caught.exception))

    def test_a_moved_request_with_an_unmoved_library_is_rejected(self) -> None:
        """P11 known-bad: the other half — the second-album ordering defect."""
        with self.assertRaises(AssertionError):
            check_library_and_request_agree(
                library_before=MERGED,
                library_after=MERGED,
                stored_before=MERGED,
                stored_after=SURVIVOR,
            )

    def test_a_pre_existing_divergence_is_not_blamed_on_this_seam(self) -> None:
        """P11 must-still-work: arrived divergent, moved nothing, left it so."""
        check_library_and_request_agree(
            library_before=SURVIVOR,
            library_after=SURVIVOR,
            stored_before=MERGED,
            stored_after=MERGED,
        )

    def test_the_agreeing_pairs_pass_p11(self) -> None:
        """P11 must-still-work: the legitimate rekey and the untouched world."""
        check_library_and_request_agree(
            library_before=MERGED,
            library_after=SURVIVOR,
            stored_before=MERGED,
            stored_after=SURVIVOR,
        )
        check_library_and_request_agree(
            library_before=MERGED,
            library_after=MERGED,
            stored_before=MERGED,
            stored_after=MERGED,
        )
        # A library holding neither identity can disagree with nothing.
        check_library_and_request_agree(
            library_before=None,
            library_after=None,
            stored_before=MERGED,
            stored_after=SURVIVOR,
        )

    def test_a_retag_without_the_survivor_lock_is_rejected(self) -> None:
        """P7 known-bad: exactly the state before the RELEASE fence."""
        with self.assertRaises(AssertionError) as caught:
            check_retag_ran_under_both_release_locks(
                [(_both_release_locks()[0],)], survivor=SURVIVOR,
            )
        self.assertIn("without holding", str(caught.exception))

    def test_a_retag_without_the_merged_away_lock_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_retag_ran_under_both_release_locks(
                [(_both_release_locks()[1],)], survivor=SURVIVOR,
            )

    def test_a_retag_holding_no_release_lock_at_all_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_retag_ran_under_both_release_locks([()], survivor=SURVIVOR)

    def test_checkers_accept_the_legitimate_dice_rekey(self) -> None:
        """Must-still-work: the real fix passes every checker."""
        check_current_evidence_follows_the_request(
            request_release_id=SURVIVOR, evidence_release_id=SURVIVOR,
        )
        check_retag_ran_under_both_release_locks(
            [_both_release_locks()], survivor=SURVIVOR,
        )
        check_retag_ran_under_both_release_locks([], survivor=None)
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


# ---------------------------------------------------------------------------
# #1080 — force import is the same path with the distance overridden
# ---------------------------------------------------------------------------
#
# P8  The merge decision does not depend on WHICH lane is asking. Given the
#     same world, the automation lane and the force lane retag the same
#     library, move the same row to the same survivor, and spend the same one
#     mirror lookup. Before #1080 the force lane made no merge decision at
#     all: it never reached the comparison seam, so a merged-away request was
#     rejected ``mbid_missing`` by ``import_one.py`` forever.
# P9  The distance threshold is the ONLY thing the two lanes do differently.
#     Automation validates at ``beets_distance_threshold``; force validates at
#     ``FORCE_IMPORT_DISTANCE_THRESHOLD``. That single argument is what makes
#     a rekeyed force result acceptable at any distance, and it is the whole
#     of "exactly the same as anything else just with beets distance
#     over-ridden".
# P10 A force import launches Beets at the release the request row names.
#     The decided consequence of the seam, asserted at the argv boundary
#     rather than on an intermediate field: an id that disagrees with the row
#     is the second-album defect the ordering rule exists to prevent.

#: Which import lane is asking. Both reach the comparison seam through their
#: own outermost real adapter — ``_process_beets_validation`` and
#: ``dispatch_import_from_db`` respectively — never through a shared helper
#: the property calls on their behalf.
LANES = st.sampled_from(["automation", "force"])

#: The rival request the fixtures seed at the survivor. Named once so the
#: world model, the fixture and P12's expected copy cannot drift apart.
RIVAL_REQUEST_ID = REQUEST_ID + 1

#: The survivor candidate's Beets distance. One inside
#: ``beets_distance_threshold`` and one far outside it, so the threshold
#: override is exercised as a discriminator rather than described.
SURVIVOR_DISTANCES = st.sampled_from([0.02, 0.62])

CONFIG_DISTANCE_THRESHOLD = 0.15


def lane_distance_threshold(lane: str, *, config_threshold: float) -> float:
    """The threshold each lane owes, derived independently of production."""
    if lane == "force":
        return FORCE_IMPORT_DISTANCE_THRESHOLD
    return config_threshold


def check_validation_ran_at_the_lane_threshold(
    lane: str,
    observed: float | None,
    *,
    config_threshold: float,
) -> None:
    """P9 — the one argument the two lanes are allowed to disagree on."""
    if observed is None:
        raise AssertionError(
            f"the {lane} lane never reached the exact-release comparison; "
            "both lanes must run the same seam"
        )
    expected = lane_distance_threshold(lane, config_threshold=config_threshold)
    if observed != expected:
        raise AssertionError(
            f"the {lane} lane validated at threshold {observed}, not the "
            f"{expected} its lane owes"
        )


def check_rekeyed_verdict_uses_the_lane_threshold(
    *,
    lane: str,
    authorized: bool,
    valid: bool,
    survivor_distance: float,
    config_threshold: float,
) -> None:
    """P9's consequence: the rekeyed result is named by the lane's threshold."""
    if not authorized:
        return
    threshold = lane_distance_threshold(lane, config_threshold=config_threshold)
    expected = survivor_distance <= threshold
    if valid is not expected:
        raise AssertionError(
            f"a rekeyed {lane} result at distance {survivor_distance} was "
            f"valid={valid}; threshold {threshold} demands {expected}"
        )


def check_force_launches_the_release_the_row_names(
    launched: list[str],
    stored_after: str | None,
) -> None:
    """P10 — Beets is launched at the identity the request actually holds."""
    for release_id in launched:
        if release_id != stored_after:
            raise AssertionError(
                f"force launched Beets at {release_id} while the request row "
                f"names {stored_after}; a mismatch lands a second album"
            )


def merge_is_blocked_before_the_retag(
    *,
    scenario: str,
    owned: bool,
    mirror_answer: str | None,
    candidates: tuple[str, ...],
    release_lock_state: str,
    survivor_taken: bool,
) -> bool:
    """The world reaches the occupancy pre-check AND the pre-check refuses.

    Derived from the world independently of production: everything a retag
    needs held, except that the survivor is already occupied. ``survivor_taken``
    is passed as ``False`` to the retag predicate on purpose — the question is
    "would this world have retagged if the survivor were free", which is
    exactly the world in which the pre-check has something to refuse.
    """
    if not survivor_taken:
        return False
    return retag_is_authorized(
        scenario=scenario,
        owned=owned,
        mirror_answer=mirror_answer,
        candidates=candidates,
        release_lock_state=release_lock_state,
        survivor_taken=False,
    )


def expected_blocked_audit_message(survivor: str | None) -> str | None:
    """The sentence the blocked world owes, from its two producers.

    ``merge_rekey_blocked_audit_message`` composes it and
    ``MergeRekeyCollision.detail`` composes the collision fragment inside it,
    so this property can never assert copy production cannot emit
    (test-fidelity Rule C). ``None`` when the world has no survivor a rival
    could occupy.
    """
    if survivor is None:
        return None
    return merge_rekey_blocked_audit_message(
        old_release_id=MERGED,
        new_release_id=survivor,
        collision_detail=MergeRekeyCollision(
            rival_request_id=RIVAL_REQUEST_ID,
        ).detail(),
    )


def check_a_blocked_merge_is_audited(
    audit_rows: list[tuple[str | None, str | None]],
    *,
    blocked: bool,
    expected_message: str | None,
) -> None:
    """P12 — the one refusal no retry can clear leaves durable evidence.

    Counted by the producer's exact sentence rather than by outcome, so
    unrelated ``download_log`` rows (the rejection each lane writes for
    itself) neither satisfy nor break it.
    """
    matched = [
        outcome for outcome, message in audit_rows
        if expected_message is not None and message == expected_message
    ]
    if not blocked:
        if matched:
            raise AssertionError(
                "a world that was never blocked recorded the blocked audit "
                f"{len(matched)} time(s): {expected_message!r}"
            )
        return
    if expected_message is None:
        raise AssertionError(
            "a blocked world has no survivor identity to name — the world "
            "model and the seam disagree about what blocked means"
        )
    if len(matched) != 1:
        raise AssertionError(
            f"a blocked merge recorded {len(matched)} audit rows, not 1; the "
            "operator is the only one who can clear this world, so every "
            "execution that meets it owes exactly one durable row"
        )
    if matched[0] != "failed":
        raise AssertionError(
            f"the blocked merge audit was recorded under {matched[0]!r}, not "
            "the environment-failure outcome the operator's Recents surfaces"
        )


@dataclass
class LaneRun:
    """What one lane did to the shared world, in lane-independent terms."""

    stored_after: str | None
    mirror_calls: list[str]
    retag_observed_release_ids: list[str | None]
    validation_threshold: float | None
    launched_release_ids: list[str]
    valid: bool
    #: Every ``download_log`` row the lane left behind, as
    #: ``(outcome, error_message)``. P12 counts the merge audit inside it by
    #: the producer's sentence, so each lane's own rejection rows are simply
    #: other rows rather than noise the fixture has to filter.
    audit_rows: list[tuple[str | None, str | None]]


def _audit_rows(db: FakePipelineDB) -> list[tuple[str | None, str | None]]:
    """Every ``download_log`` row the lane wrote, as ``(outcome, message)``."""
    return [(row.outcome, row.error_message) for row in db.download_logs]


def _merge_world_cfg(tmpdir: str) -> CratediggerConfig:
    return CratediggerConfig(
        beets_harness_path="/nix/store/fake/harness.sh",
        beets_distance_threshold=CONFIG_DISTANCE_THRESHOLD,
        beets_staging_dir=os.path.join(tmpdir, "staging"),
        slskd_download_dir=tmpdir,
        pipeline_db_enabled=True,
    )


def _seed_rival_and_locks(
    db: FakePipelineDB,
    *,
    mirror_answer: str | None,
    survivor_taken: bool,
    release_lock_state: str,
) -> None:
    """Apply the two world dimensions that live outside the fixture builders."""
    taken = _taken_survivor_id(mirror_answer) if survivor_taken else None
    if taken is not None:
        db.seed_request(make_request_row(
            id=RIVAL_REQUEST_ID,
            mb_release_id=taken,
            artist_name="DICE",
            album_title="Midnight Zoo (other pressing)",
        ))
    survivor_id = _taken_survivor_id(mirror_answer)
    held_key = {
        "free": None,
        "old_held": release_id_to_lock_key(MERGED),
        "new_held": (
            None if survivor_id is None else release_id_to_lock_key(survivor_id)
        ),
    }[release_lock_state]
    if held_key is not None:
        db.set_advisory_lock_result(
            lambda namespace, key, held_key=held_key: not (
                namespace == ADVISORY_LOCK_NAMESPACE_RELEASE
                and key == held_key
            )
        )


class TestForceAndAutomationAgreeOnTheMerge(unittest.TestCase):
    """P8–P10 over both real lane adapters (#1080)."""

    def _run_automation(
        self,
        tmpdir: str,
        *,
        claimed: bool,
        bv_result: ValidationResult,
        canonical: _RecordingCanonical,
        retag_outcome: RetagOutcome,
        mirror_answer: str | None,
        survivor_taken: bool,
        release_lock_state: str,
    ) -> LaneRun:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=REQUEST_ID,
            mb_release_id=MERGED,
            artist_name="DICE",
            album_title="Midnight Zoo",
        ))
        if claimed:
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
        _seed_rival_and_locks(
            db,
            mirror_answer=mirror_answer,
            survivor_taken=survivor_taken,
            release_lock_state=release_lock_state,
        )
        cfg = _merge_world_cfg(tmpdir)
        ctx = make_ctx_with_fake_db(db, cfg=cfg)
        album_data = make_grab_list_entry(
            album_id=REQUEST_ID,
            artist="DICE",
            title="Midnight Zoo",
            mb_release_id=MERGED,
            db_source="request",
            db_request_id=REQUEST_ID,
        )
        retag = _RecordingRetag(db, retag_outcome)
        with (
            _silence_logs(),
            patch("lib.beets.beets_validate", return_value=bv_result) as validate,
        ):
            _process_beets_validation(
                album_data,
                StagedAlbum(current_path=tmpdir, request_id=REQUEST_ID),
                ctx,
                import_job_id=import_job_id,
                canonical_release_fn=canonical,
                retag_fn=retag,
            )
        row = db.request(REQUEST_ID)
        return LaneRun(
            stored_after=None if row is None else row.get("mb_release_id"),
            mirror_calls=list(canonical.calls),
            retag_observed_release_ids=list(retag.observed_release_ids),
            validation_threshold=(
                validate.call_args.args[3] if validate.call_args else None
            ),
            # P10 is the force lane's consequence: the automation lane's
            # launch is fenced by evidence this fixture deliberately does not
            # seed, so it reports no launch rather than a fabricated one.
            launched_release_ids=[],
            valid=bv_result.valid,
            audit_rows=_audit_rows(db),
        )

    def _run_force(
        self,
        tmpdir: str,
        *,
        claimed: bool,
        bv_result: ValidationResult,
        canonical: _RecordingCanonical,
        retag_outcome: RetagOutcome,
        mirror_answer: str | None,
        survivor_taken: bool,
        release_lock_state: str,
    ) -> LaneRun:
        from tests.test_force_import_merge_redirect import (
            _ForceWorld,
            _RecordingRunImport,
            force_dispatch,
        )

        with contextlib.ExitStack() as stack:
            world = _ForceWorld(stack, claim=claimed, path=tmpdir)
            retag = _RecordingRetag(world.db, retag_outcome)
            _seed_rival_and_locks(
                world.db,
                mirror_answer=mirror_answer,
                survivor_taken=survivor_taken,
                release_lock_state=release_lock_state,
            )
            runner = _RecordingRunImport()
            _, _, validate = force_dispatch(
                world,
                bv_result,
                canonical=canonical,
                retag=retag,
                run_import=runner,
            )
            row = world.db.request(REQUEST_ID)
            return LaneRun(
                stored_after=None if row is None else row.get("mb_release_id"),
                mirror_calls=list(canonical.calls),
                retag_observed_release_ids=list(retag.observed_release_ids),
                validation_threshold=(
                    validate.call_args.args[3] if validate.call_args else None
                ),
                launched_release_ids=list(runner.release_ids),
                valid=bv_result.valid,
                audit_rows=_audit_rows(world.db),
            )

    @settings(deadline=None)
    @given(
        lane=LANES,
        scenario=SCENARIOS,
        mirror_answer=MIRROR_ANSWERS,
        candidates=CANDIDATE_SETS,
        retag_outcome=RETAG_OUTCOMES,
        claimed=st.booleans(),
        survivor_taken=st.booleans(),
        release_lock_state=RELEASE_LOCK_STATES,
        survivor_distance=SURVIVOR_DISTANCES,
    )
    # The live request-346 world, in each lane. The force one is the world
    # #1080 was reported from; before the fix it rekeyed nothing.
    @example(
        lane="force", scenario="mbid_not_found", mirror_answer=SURVIVOR,
        candidates=(SURVIVOR,), retag_outcome=RETAG_RETAGGED, claimed=True,
        survivor_taken=False, release_lock_state="free", survivor_distance=0.02,
    )
    @example(
        lane="automation", scenario="mbid_not_found", mirror_answer=SURVIVOR,
        candidates=(SURVIVOR,), retag_outcome=RETAG_RETAGGED, claimed=True,
        survivor_taken=False, release_lock_state="free", survivor_distance=0.02,
    )
    # The threshold discriminator: the same survivor, far outside
    # ``beets_distance_threshold``. Automation must name it ``high_distance``;
    # force must name it ``strong_match`` and import it.
    @example(
        lane="force", scenario="mbid_not_found", mirror_answer=SURVIVOR,
        candidates=(SURVIVOR,), retag_outcome=RETAG_RETAGGED, claimed=True,
        survivor_taken=False, release_lock_state="free", survivor_distance=0.62,
    )
    @example(
        lane="automation", scenario="mbid_not_found", mirror_answer=SURVIVOR,
        candidates=(SURVIVOR,), retag_outcome=RETAG_RETAGGED, claimed=True,
        survivor_taken=False, release_lock_state="free", survivor_distance=0.62,
    )
    # An unclaimed lane has no authority in either shape.
    @example(
        lane="force", scenario="mbid_not_found", mirror_answer=SURVIVOR,
        candidates=(SURVIVOR,), retag_outcome=RETAG_RETAGGED, claimed=False,
        survivor_taken=False, release_lock_state="free", survivor_distance=0.02,
    )
    # The blocked world in each lane: the survivor is already held, so the
    # library is never touched, the row never moves, and the durable audit is
    # the only evidence the operator ever gets (P12).
    @example(
        lane="force", scenario="mbid_not_found", mirror_answer=SURVIVOR,
        candidates=(SURVIVOR,), retag_outcome=RETAG_RETAGGED, claimed=True,
        survivor_taken=True, release_lock_state="free", survivor_distance=0.02,
    )
    @example(
        lane="automation", scenario="mbid_not_found", mirror_answer=SURVIVOR,
        candidates=(SURVIVOR,), retag_outcome=RETAG_RETAGGED, claimed=True,
        survivor_taken=True, release_lock_state="free", survivor_distance=0.02,
    )
    def test_both_lanes_decide_the_same_merge(
        self,
        lane: str,
        scenario: str,
        mirror_answer: str | None,
        candidates: tuple[str, ...],
        retag_outcome: RetagOutcome,
        claimed: bool,
        survivor_taken: bool,
        release_lock_state: str,
        survivor_distance: float,
    ) -> None:
        bv_result = _validation_result(
            scenario, candidates, survivor_distance=survivor_distance,
        )
        canonical = _RecordingCanonical(mirror_answer)
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "01 - Track.mp3"), "wb") as handle:
                handle.write(b"audio")
            lane_runner = (
                self._run_force if lane == "force" else self._run_automation
            )
            run = lane_runner(
                tmpdir,
                claimed=claimed,
                bv_result=bv_result,
                canonical=canonical,
                retag_outcome=retag_outcome,
                mirror_answer=mirror_answer,
                survivor_taken=survivor_taken,
                release_lock_state=release_lock_state,
            )

        # P8: the authorization is derived from the world alone. Nothing in it
        # names a lane, so asserting it against both lanes IS the parity claim.
        authorized = rekey_is_authorized(
            scenario=scenario,
            owned=claimed,
            mirror_answer=mirror_answer,
            candidates=candidates,
            retag_outcome=retag_outcome,
            survivor_taken=survivor_taken,
            release_lock_state=release_lock_state,
        )
        check_row_moves_only_when_authorized(
            run.stored_after,
            authorized=authorized,
            expected_survivor=mirror_answer,
        )
        check_retag_never_saw_a_moved_row(run.retag_observed_release_ids)
        # P11 in BOTH lanes: whichever one is asking, it never walks away from
        # a library and a request naming different releases.
        survivor_id = _taken_survivor_id(mirror_answer)
        check_library_and_request_agree(
            library_before=library_before_retag(retag_outcome, survivor_id),
            library_after=library_after_retag(
                retag_outcome,
                survivor_id,
                retag_ran=bool(run.retag_observed_release_ids),
            ),
            stored_before=MERGED,
            stored_after=run.stored_after,
        )
        check_mirror_asked_only_where_it_can_be_used(
            run.mirror_calls, scenario=scenario, owned=claimed,
        )
        # P9: both lanes reached the seam, at their own threshold and no other
        # difference.
        check_validation_ran_at_the_lane_threshold(
            lane,
            run.validation_threshold,
            config_threshold=CONFIG_DISTANCE_THRESHOLD,
        )
        check_rekeyed_verdict_uses_the_lane_threshold(
            lane=lane,
            authorized=authorized,
            valid=run.valid,
            survivor_distance=survivor_distance,
            config_threshold=CONFIG_DISTANCE_THRESHOLD,
        )
        # P10: whatever the seam decided, the launch agrees with the row.
        check_force_launches_the_release_the_row_names(
            run.launched_release_ids, run.stored_after,
        )
        # P12: the refusal the operator alone can clear is durable in BOTH
        # lanes — and no other world claims it.
        check_a_blocked_merge_is_audited(
            run.audit_rows,
            blocked=merge_is_blocked_before_the_retag(
                scenario=scenario,
                owned=claimed,
                mirror_answer=mirror_answer,
                candidates=candidates,
                release_lock_state=release_lock_state,
                survivor_taken=survivor_taken,
            ),
            expected_message=expected_blocked_audit_message(survivor_id),
        )


class TestForceParityCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests for the #1080 checkers."""

    def test_a_lane_that_skipped_the_seam_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_validation_ran_at_the_lane_threshold(
                "force", None, config_threshold=CONFIG_DISTANCE_THRESHOLD,
            )

    def test_a_lane_validating_at_the_other_threshold_is_rejected(self) -> None:
        for lane, wrong in (
            ("force", CONFIG_DISTANCE_THRESHOLD),
            ("automation", FORCE_IMPORT_DISTANCE_THRESHOLD),
        ):
            with self.subTest(lane=lane), self.assertRaises(AssertionError):
                check_validation_ran_at_the_lane_threshold(
                    lane, wrong, config_threshold=CONFIG_DISTANCE_THRESHOLD,
                )

    def test_a_force_verdict_narrowed_to_the_config_threshold_is_rejected(
        self,
    ) -> None:
        with self.assertRaises(AssertionError):
            check_rekeyed_verdict_uses_the_lane_threshold(
                lane="force", authorized=True, valid=False,
                survivor_distance=0.62,
                config_threshold=CONFIG_DISTANCE_THRESHOLD,
            )

    def test_an_automation_verdict_widened_to_the_override_is_rejected(
        self,
    ) -> None:
        with self.assertRaises(AssertionError):
            check_rekeyed_verdict_uses_the_lane_threshold(
                lane="automation", authorized=True, valid=True,
                survivor_distance=0.62,
                config_threshold=CONFIG_DISTANCE_THRESHOLD,
            )

    def test_a_launch_disagreeing_with_the_row_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_force_launches_the_release_the_row_names([MERGED], SURVIVOR)

    def test_a_blocked_merge_that_recorded_nothing_is_rejected(self) -> None:
        """P12 known-bad: exactly the #1080 silence, in either lane."""
        with self.assertRaises(AssertionError):
            check_a_blocked_merge_is_audited(
                [("rejected", "Target MBID not in candidates")],
                blocked=True,
                expected_message=expected_blocked_audit_message(SURVIVOR),
            )

    def test_a_blocked_audit_under_the_wrong_outcome_is_rejected(self) -> None:
        message = expected_blocked_audit_message(SURVIVOR)
        with self.assertRaises(AssertionError):
            check_a_blocked_merge_is_audited(
                [("rejected", message)],
                blocked=True,
                expected_message=message,
            )

    def test_a_blocked_merge_audited_twice_in_one_execution_is_rejected(
        self,
    ) -> None:
        message = expected_blocked_audit_message(SURVIVOR)
        with self.assertRaises(AssertionError):
            check_a_blocked_merge_is_audited(
                [("failed", message), ("failed", message)],
                blocked=True,
                expected_message=message,
            )

    def test_an_unblocked_world_claiming_the_blocked_audit_is_rejected(
        self,
    ) -> None:
        message = expected_blocked_audit_message(SURVIVOR)
        with self.assertRaises(AssertionError):
            check_a_blocked_merge_is_audited(
                [("failed", message)],
                blocked=False,
                expected_message=message,
            )

    def test_a_blocked_world_with_no_survivor_identity_is_rejected(
        self,
    ) -> None:
        """Fail closed: "blocked" with nothing to name is a model disagreement."""
        with self.assertRaises(AssertionError):
            check_a_blocked_merge_is_audited(
                [], blocked=True, expected_message=None,
            )

    def test_the_blocked_predicate_names_the_pre_check_world(self) -> None:
        """The world model's own contract, derived without production."""
        self.assertTrue(merge_is_blocked_before_the_retag(
            scenario="mbid_not_found", owned=True, mirror_answer=SURVIVOR,
            candidates=(SURVIVOR,), release_lock_state="free",
            survivor_taken=True,
        ))
        # A free survivor is not blocked, and neither is a world that never
        # reached the pre-check at all.
        self.assertFalse(merge_is_blocked_before_the_retag(
            scenario="mbid_not_found", owned=True, mirror_answer=SURVIVOR,
            candidates=(SURVIVOR,), release_lock_state="free",
            survivor_taken=False,
        ))
        self.assertFalse(merge_is_blocked_before_the_retag(
            scenario="strong_match", owned=True, mirror_answer=SURVIVOR,
            candidates=(SURVIVOR,), release_lock_state="free",
            survivor_taken=True,
        ))
        self.assertFalse(merge_is_blocked_before_the_retag(
            scenario="mbid_not_found", owned=True, mirror_answer=SURVIVOR,
            candidates=(SURVIVOR,), release_lock_state="old_held",
            survivor_taken=True,
        ))
        self.assertFalse(merge_is_blocked_before_the_retag(
            scenario="mbid_not_found", owned=False, mirror_answer=SURVIVOR,
            candidates=(SURVIVOR,), release_lock_state="free",
            survivor_taken=True,
        ))

    def test_the_agreeing_worlds_still_pass(self) -> None:
        check_validation_ran_at_the_lane_threshold(
            "force", FORCE_IMPORT_DISTANCE_THRESHOLD,
            config_threshold=CONFIG_DISTANCE_THRESHOLD,
        )
        check_validation_ran_at_the_lane_threshold(
            "automation", CONFIG_DISTANCE_THRESHOLD,
            config_threshold=CONFIG_DISTANCE_THRESHOLD,
        )
        check_rekeyed_verdict_uses_the_lane_threshold(
            lane="force", authorized=True, valid=True, survivor_distance=0.62,
            config_threshold=CONFIG_DISTANCE_THRESHOLD,
        )
        check_rekeyed_verdict_uses_the_lane_threshold(
            lane="automation", authorized=True, valid=False,
            survivor_distance=0.62,
            config_threshold=CONFIG_DISTANCE_THRESHOLD,
        )
        check_force_launches_the_release_the_row_names([SURVIVOR], SURVIVOR)
        check_force_launches_the_release_the_row_names([], None)
        blocked_message = expected_blocked_audit_message(SURVIVOR)
        # The lane's own rejection row sits alongside the audit and neither
        # satisfies nor breaks P12.
        check_a_blocked_merge_is_audited(
            [("rejected", "beets rejected the download"),
             ("failed", blocked_message)],
            blocked=True,
            expected_message=blocked_message,
        )
        check_a_blocked_merge_is_audited(
            [("rejected", "beets rejected the download")],
            blocked=False,
            expected_message=blocked_message,
        )
