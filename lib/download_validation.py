"""Completed-download manifest validation and validated-result dispatch.

This module owns the boundary from a materialized album through beets exact-
release validation and candidate-evidence gating to the staged dispatch handoff.
Completion result tagging remains in :mod:`lib.download_processing`, filesystem
materialization in :mod:`lib.download_materialization`, and reject persistence
in :mod:`lib.download_rejection`.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Literal, Protocol

from lib.beets_retag import (
    RETAG_FAILED,
    RETAG_READY_OUTCOMES,
    RETAG_RETAGGED,
    BeetsRetagResult,
    MergeRetagFn,
    retag_merged_album,
)
from lib.beets_tag_sync import TagSyncLockDB, TagSyncResult
from lib.dispatch import (
    DispatchCoreFn,
    DispatchOutcome,
    DispatchRequest,
    QualityGateFn,
    _build_download_info,
    _check_quality_gate_core,
    _requeue_import_job_to_preview,
    dispatch_import_core,
)
from lib.download_rejection import (
    _handle_rejected_result,
    _reject_request_auto_import,
)
from lib.grab_list import GrabListEntry
from lib.import_evidence import (
    CandidateEvidenceActionResult,
    ensure_candidate_evidence_for_action,
)
from lib.import_execution import (
    CancellationToken,
    ExecutionOwnerProof,
    checkpoint,
)
from lib.import_manifest import (
    audio_relative_paths,
    check_audio_manifest,
    manifest_trace_summary,
    tracked_audio_paths_for_downloads,
)
from lib.import_queue import IMPORT_JOB_AUTOMATION, IMPORT_JOB_FORCE, ImportJob
from lib.mb_canonical import CanonicalReleaseFn, production_canonical_release_fn
from lib.processing_paths import source_dirs_for_album, stage_to_ai_path
from lib.quality import (
    SpectralEvidenceFacts,
    ValidationResult,
    compute_effective_override_bitrate,
    interpret_spectral_evidence,
)
from lib.release_identity import ReleaseIdentity, normalize_release_id
from lib.staged_album import StagedAlbum
from lib.util import log_validation_result

if TYPE_CHECKING:
    from lib.config import CratediggerConfig
    from lib.context import CratediggerContext
    from lib.pipeline_db._shared import MergeRekeyCollision
    from lib.pipeline_db.download_log import DownloadLogOutcome
    from lib.pipeline_db.rows import AlbumRequestRow

logger = logging.getLogger("cratedigger")

#: The production merge-survivor resolver, bound once at import. It reads the
#: process's configured WS/2 base LATE (per call), so startup order cannot
#: matter, and holding it in a module singleton keeps it a definition-time
#: default that tests inject rather than patch.
_PRODUCTION_CANONICAL_RELEASE_FN: Final[CanonicalReleaseFn] = (
    production_canonical_release_fn()
)


class HandleValidFn(Protocol):
    """Exact injection contract for the validated-result handoff."""

    def __call__(
        self,
        album_data: GrabListEntry,
        bv_result: ValidationResult,
        staged_album: StagedAlbum,
        ctx: CratediggerContext,
        *,
        import_job_id: int | None = None,
        prevalidated_candidate_result: CandidateEvidenceActionResult | None = None,
        quality_gate_fn: QualityGateFn | None = None,
        dispatch_fn: DispatchCoreFn | None = None,
        cancellation_token: CancellationToken | None = None,
        owner_proof: ExecutionOwnerProof | None = None,
    ) -> DispatchOutcome | None: ...


class ValidateFn(Protocol):
    """Exact injection contract for materialized-album validation."""

    def __call__(
        self,
        album_data: GrabListEntry,
        staged_album: StagedAlbum,
        ctx: CratediggerContext,
        *,
        import_job_id: int,
        handle_valid_fn: HandleValidFn | None = None,
        dispatch_fn: DispatchCoreFn | None = None,
        cancellation_token: CancellationToken | None = None,
        owner_proof: ExecutionOwnerProof | None = None,
    ) -> DispatchOutcome | None: ...


#: The request was never a candidate for a merge rekey: no request row, a
#: release identity MusicBrainz has no redirect concept for (Discogs), or a
#: validation that never reached ``mbid_not_found`` at all.
MERGE_NOT_APPLICABLE: Final = "not_applicable"
#: This validation does not hold the request's exact import claim, so it has
#: no authority to retag the library or move the row (a YouTube rescue, or a
#: stale claim). Checked BEFORE the mirror so the lookup is never spent on a
#: world that could not act on the answer.
MERGE_NOT_OWNED: Final = "not_owned"
#: MusicBrainz still considers the stored id current, or would not answer.
#: The overwhelmingly common result, and the reason the mirror is only asked
#: on ``mbid_not_found``.
MERGE_NO_REDIRECT: Final = "no_redirect"
#: A merge is real, but this download is not the survivor either.
MERGE_SURVIVOR_NOT_OFFERED: Final = "survivor_not_offered"
#: Another process holds the per-release lock on one of the two identities the
#: retag mutates. Nothing was attempted; the request stays runnable.
MERGE_RELEASE_LOCKED: Final = "release_locked"
#: The survivor is already occupied — another request holds it, or an
#: evidence row already exists at ``(survivor, fingerprint)`` — so the rekey
#: is refused BEFORE the library is touched. Read under the release locks,
#: immediately before the retag: the alternative is discovering the refusal
#: afterwards, with the library moved and the request left behind. Durably
#: audited, because it is the one non-ready outcome no retry can clear.
MERGE_REKEY_BLOCKED: Final = "rekey_blocked"
#: The library could not be moved onto the survivor. Nothing else happens:
#: rekeying now would make the next import land a SECOND album.
MERGE_RETAG_NOT_READY: Final = "retag_not_ready"
#: The library moved but the request row did not: the survivor was taken in
#: the window between the pre-check and the write, or the claim was lost.
#: Fails closed, and — when this execution is what moved the library — records
#: a durable audit row, because a split nobody is told about is one nothing
#: will re-derive.
MERGE_REKEY_REFUSED: Final = "rekey_refused"
#: The library is at the survivor and the request row now names it.
MERGE_REKEYED: Final = "rekeyed"

type MergeRekeyStatus = Literal[
    "not_applicable",
    "not_owned",
    "no_redirect",
    "survivor_not_offered",
    "release_locked",
    "rekey_blocked",
    "retag_not_ready",
    "rekey_refused",
    "rekeyed",
]

#: The audit outcome BOTH durable merge audits are recorded under — the
#: occupied survivor and the residual split. Neither is a quality verdict and
#: neither is a download result: the world is stuck and surfaced (invariant
#: 11), so they take the existing environment-failure outcome rather than a
#: new one that would need a migration to say the same thing.
_MERGE_AUDIT_OUTCOME: Final = "failed"


@dataclass(frozen=True)
class MergeRekeyOutcome:
    """What the merge-redirect branch did, and why."""

    status: MergeRekeyStatus
    detail: str = ""
    survivor: str | None = None
    #: THIS execution observably moved the installed album onto the survivor
    #: (``RETAG_RETAGGED``). Set independently of ``status`` because the two
    #: together name the one state that needs both an audit row and a refusal
    #: to launch Beets: moved library, unmoved request.
    library_moved: bool = False

    @property
    def rekeyed(self) -> bool:
        return self.status == MERGE_REKEYED

    @property
    def split_identity(self) -> bool:
        """The library moved onto the survivor and the request did not.

        The durable divergence: Beets holds the album at one release id and
        the request names another. Nothing is guaranteed to re-derive it —
        whatever claimed the survivor inside the race window still holds it,
        so the next attempt is refused at the occupancy pre-check BEFORE the
        library is read at all — so it is audited, and the force lane refuses
        to launch Beets at the id the row still names.

        Scoped to the split THIS execution created (``library_moved`` is
        ``RETAG_RETAGGED``), never a detector for one that already exists: a
        pre-existing split arrives as :data:`MERGE_REKEY_BLOCKED`, which is
        why that outcome is durably audited too.
        """
        return self.library_moved and not self.rekeyed


@dataclass(frozen=True)
class ReleaseValidation:
    """One exact-release validation plus whatever the merge seam did about it."""

    result: ValidationResult
    merge: MergeRekeyOutcome


class MergeRekeyDB(Protocol):
    """The exact pipeline-DB surface the merge seam needs.

    Declared locally on purpose: the seam depends on a handful of methods, not
    on the whole ``PipelineDB``, and both lanes that reach it (the automation
    validation and the force-import dispatch entry point) satisfy it
    structurally.
    """

    def get_request(self, request_id: int) -> AlbumRequestRow | None: ...

    def get_import_job(self, job_id: int) -> ImportJob | None: ...

    def advisory_lock(
        self,
        namespace: int,
        key: int,
    ) -> AbstractContextManager[bool]: ...

    def merge_rekey_collision(
        self,
        request_id: int,
        *,
        old_release_id: str,
        new_release_id: str,
    ) -> MergeRekeyCollision: ...

    def update_request_release_for_merge(
        self,
        request_id: int,
        *,
        old_release_id: str,
        new_release_id: str,
        expected_import_job_id: int,
    ) -> bool: ...

    def log_download(
        self,
        request_id: int,
        *,
        beets_detail: str | None = ...,
        outcome: DownloadLogOutcome | None = ...,
        error_message: str | None = ...,
    ) -> int: ...


#: The two request states an import claim can legally rekey identity from.
#: Verbatim the complement of the request fence
#: ``claim_force_import_job_under_lock`` claims through — since issue #1313
#: that fence lives in the lane-taking
#: ``_ImportJobsMixin._claim_request_scoped_job_in_lane``, which force and
#: local share. ``processing`` belongs to an automation owner (and is that
#: claim's own arm below), and ``replaced`` rows are frozen audit ancestors.
_FORCE_CLAIM_EXCLUDED_STATUSES: Final = frozenset({"processing", "replaced"})


def merge_rekey_claim_holds(
    row: Mapping[str, object],
    job: ImportJob,
) -> bool:
    """Does ``job`` still hold the import claim that authorizes a rekey?

    Moving ``mb_release_id`` is an identity write, so it is fenced on the
    caller still holding the exact claim it took. There are two claims in
    production and this mirrors both, term for term:

    * ``claim_automation_import_job_under_lock`` — the request is
      ``processing`` and its ``active_automation_import_job_id`` names this
      job. That pointer IS ownership (invariant 10).
    * ``claim_force_import_job_under_lock`` — the request has no automation
      owner and is neither ``processing`` nor the frozen ``replaced``, and
      this force job is ``running`` against it. The force lane cannot take
      the ``processing`` pointer at all: migration 066's owner-equivalence
      CHECK and its partial unique index reserve it for one active
      ``automation_import`` job.

    A YouTube rescue job holds neither claim and never rekeys, exactly as
    before (#1059).

    This is the PRE-check, not the authority: it exists so an unclaimed world
    never spends a mirror lookup or retags the shared Beets library for a
    rekey that could not land. ``PipelineDB.update_request_release_for_merge``
    re-decides the same conjunction atomically and is what actually writes.
    """
    if job.job_type == IMPORT_JOB_AUTOMATION:
        return (
            row.get("status") == "processing"
            and row.get("active_automation_import_job_id") == job.id
        )
    if job.job_type == IMPORT_JOB_FORCE:
        return (
            job.status == "running"
            and row.get("active_automation_import_job_id") is None
            and row.get("status") not in _FORCE_CLAIM_EXCLUDED_STATUSES
        )
    return False


def _retag_merged_album_with_beets(
    cfg: CratediggerConfig,
    *,
    old_identity: ReleaseIdentity,
    new_identity: ReleaseIdentity,
) -> BeetsRetagResult:
    """Open the deployment-owned Beets library and run the one-album retag.

    The Beets handle is the only thing this adds over
    :func:`lib.beets_retag.retag_merged_album`; a handle that cannot be opened
    is a typed failure, never an exception into the import path.
    """
    from lib.beets_db import open_beets_db

    try:
        with open_beets_db(cfg) as beets:
            return retag_merged_album(
                beets,
                old_identity=old_identity,
                new_identity=new_identity,
            )
    except Exception as exc:  # noqa: BLE001 - external edge, typed outcome
        return BeetsRetagResult(
            outcome=RETAG_FAILED,
            detail=(
                "Beets library could not be opened for the merge retag: "
                f"{type(exc).__name__}: {exc}"
            ),
        )


#: The seam's injected best-effort tag sync — ``(db, cfg, survivor)``.
type MergeTagSyncFn = Callable[
    [TagSyncLockDB, "CratediggerConfig", str], TagSyncResult,
]


def _sync_release_tags_with_beets(
    db: TagSyncLockDB,
    cfg: CratediggerConfig,
    release_id: str,
) -> TagSyncResult:
    """Open the deployment-owned Beets library and sync the one album at
    ``release_id`` — the production ``sync_fn`` behind
    :func:`_sync_file_tags_after_merge_rekey`."""
    from lib.beets_db import open_beets_db
    from lib.beets_tag_sync import sync_release_file_tags_from_factory

    return sync_release_file_tags_from_factory(
        lambda: open_beets_db(cfg), db, release_id=release_id,
    )


def _sync_file_tags_after_merge_rekey(
    db: TagSyncLockDB,
    cfg: CratediggerConfig,
    release_id: str,
    *,
    sync_fn: MergeTagSyncFn = _sync_release_tags_with_beets,
) -> None:
    """Best-effort file-tag sync after a completed merge rekey (#1260).

    A ``retagged`` rekey moved the Beets DB onto the survivor without
    touching any installed file's tag; unless an accepted import later
    replaces the files, the stale tag sits armed until the census flags it
    (a later ``beet update`` would copy it back over the DB row). This
    call converges the files immediately — STRICTLY outcome-inert: it runs
    only after the rekey has durably landed, nothing consumes its result,
    a failure is one log line, and the census/dashboard button remain the
    reconciliation loop for whatever it could not fix. It must never raise
    into the importer; note the blanket except also swallows a
    cancellation raised inside the sync's own DB-lock I/O, which the
    caller's immediately-following ``checkpoint`` re-raises (#1260 review
    F8). The other two ready rekey worlds need no assertion here: the
    service re-derives them from Beets — ``not_held`` resolves to
    ``not_found``; ``already_current`` typically to
    ``already_synced``/``synced``, though any of the service's typed
    outcomes can follow from what the re-read actually shows (#1260
    re-review C2).

    ``sync_fn`` is a definition-time default: tests INJECT a replacement,
    they never patch the module binding (`.claude/rules/code-quality.md`
    § mocks, strategy 2).
    """
    try:
        result = sync_fn(db, cfg, release_id)
        detail = f" — {result.error_message}" if result.error_message else ""
        logger.info(
            "MERGE TAG SYNC: %s at %s (album %s)%s",
            result.outcome, release_id, result.album_id, detail,
        )
    except Exception:
        logger.warning(
            "MERGE TAG SYNC: best-effort file-tag sync at %s failed",
            release_id, exc_info=True,
        )


def split_identity_audit_message(
    *,
    old_release_id: str,
    new_release_id: str,
    retag_detail: str,
) -> str:
    """The operator-facing sentence for a moved library and an unmoved request.

    One producer, so the copy the operator reads in Recents and the copy the
    audit row stores are the same string (``.claude/rules/test-fidelity.md``
    Rule C: a pin for this copy takes its input from here, never a literal).

    Deliberately carries no lane-specific prefix: BOTH lanes can write this
    row, so "force import attempt failed" would be false half the time. It is
    genuinely import-phase text, which is exactly what
    ``lib.failure_presentation`` labels ``Import error:``.
    """
    return (
        f"Library and request disagree after a MusicBrainz merge: "
        f"{old_release_id} was merged into {new_release_id} and the installed "
        f"album was retagged onto the survivor ({retag_detail}), but the "
        "request could not be rekeyed — the survivor was claimed, or this "
        "import's claim was lost, between the check and the write. Two "
        "requests over one release is an operator decision."
    )


def merge_rekey_blocked_audit_message(
    *,
    old_release_id: str,
    new_release_id: str,
    collision_detail: str,
) -> str:
    """The operator-facing sentence for a survivor that is already occupied.

    One producer, so the outcome detail, the durable audit row and the copy
    the operator reads in Recents are the same string
    (``.claude/rules/test-fidelity.md`` Rule C: a pin for this copy takes its
    input from here, never a literal).

    Lane-neutral like its split sibling — both lanes reach this branch — and
    it deliberately names the collision rather than the lane's symptom,
    because the collision is the only thing an operator can act on.
    """
    return (
        f"{old_release_id} was merged into {new_release_id}, but the survivor "
        f"is already occupied: {collision_detail}. The library was not "
        "touched, and no retry clears this: resolving the collision — two "
        "requests over one release, or two measurements of the same bytes — "
        "is an operator decision."
    )


def _record_merge_audit(
    db: MergeRekeyDB,
    *,
    request_id: int,
    log_label: str,
    beets_detail: str,
    message: str,
) -> None:
    """Record a merge outcome the pipeline cannot clear by trying again.

    Most non-ready outcomes leave the world exactly as the next cycle will
    find it — the mirror said nothing, a lock was held, the retag failed — so
    the existing rejection IS the audit. Exactly two are different, and both
    come here:

    * :data:`MERGE_REKEY_BLOCKED` — the collision persists until an operator
      resolves it, so every later attempt is refused identically. The force
      lane in particular carries no rejection of its own to explain it: force
      imports DESPITE the verdict, so it goes on to meet the merged-away id
      inside ``import_one.py`` and reports ``mbid_missing`` with no reason
      attached, attempt after attempt.
    * :data:`MERGE_REKEY_REFUSED` after this execution moved the library — no
      later attempt can tell that state apart from a request that was always
      wrong.

    Invariant 11's "record Recents audit evidence" is the whole point: a log
    line is gone at the next journal rotation. One row per execution that
    reaches the branch, deliberately NOT deduplicated — an execution is a
    discrete operator force action or completed-download validation, each of
    which already writes its own ``download_log`` row, so the audit trail
    stays proportional to the work attempted rather than to elapsed time.

    Deliberately unguarded: a DB blip here raises into the importer, which
    self-heals the request and re-derives on the next cycle. Swallowing it
    would trade the audit for silence, which is the defect this exists to fix.
    """
    logger.error("%s: request=%s %s", log_label, request_id, message)
    db.log_download(
        request_id,
        outcome=_MERGE_AUDIT_OUTCOME,
        beets_detail=beets_detail,
        error_message=message,
    )


def _follow_merged_release(
    bv_result: ValidationResult,
    *,
    db: MergeRekeyDB,
    cfg: CratediggerConfig,
    request_id: int | None,
    stored_release_id: str | None,
    import_job_id: int | None,
    distance_threshold: float,
    canonical_release_fn: CanonicalReleaseFn,
    retag_fn: MergeRetagFn,
) -> MergeRekeyOutcome:
    """Follow a MusicBrainz merge when the exact release stops matching.

    ``mbid_not_found`` is where a merged-away request announces itself: Beets
    offers the survivor, our matcher demands the stored id, and the download is
    rejected forever. This is the only place that asks MusicBrainz what the
    release is called now, and it asks ONLY here — the ~8,500 healthy rows a
    cycle never touch the mirror.

    **Ordering is the whole design.** Beets keys album duplicate detection on
    ``mb_albumid`` (``duplicate_keys: album: [mb_albumid, discogs_albumid]``),
    so a request rekeyed to the survivor while the installed album is still
    filed under the merged-away id flags NO duplicate: the import lands a
    SECOND album beside the first, and the existing-album lookup misses so the
    quality decision routes through ``import_no_exist`` and silently skips the
    downgrade guard. Retag first, verify the library observably moved, and only
    then move the row.

    **Both release locks are held across the retag and the rekey.** The retag
    mutates TWO release identities at once — it takes the installed album away
    from the merged-away id and files it under the survivor — and
    ``lib/destructive_release_service.py`` fences Beets mutation per release
    with ``RELEASE(release_id)`` from OTHER processes (the web routes and
    ``pipeline-cli destructive``). Without both locks, an operator Bad Rip or
    library-delete resolving "the one album at the survivor" can bind to the
    album the retag just moved onto that id and delete files the operator
    never selected. The IMPORT lock both callers already run under stays
    outer, preserving the documented ``IMPORT → RELEASE`` order
    (``docs/advisory-locks.md``); the acquires are non-blocking, so contention
    is a typed non-ready outcome, never a wait.

    **The rekey's two UNIQUE-violating refusals are read BEFORE the retag.**
    Both causes — a rival request already at the survivor, and an evidence row
    already at ``(survivor, fingerprint)`` — are plain reads
    (:meth:`PipelineDB.merge_rekey_collision`), taken under the release locks
    already held, immediately before Beets is mutated. (The write's other
    refusals are compare-and-set misses whose world the next attempt
    re-derives; these two persist until an operator acts, which is why exactly
    these two are worth a pre-check.) Retagging first and discovering the
    refusal afterwards leaves the installed album filed under the survivor
    while the request still names the merged-away id, and nothing is
    guaranteed to re-derive that: the collision that refused the write is
    still there on the next attempt, which is now refused at this same
    pre-check — before the library is read at all. A blocked world is
    therefore durably audited too (:func:`_record_merge_audit`): it is the one
    non-ready outcome no retry can clear.

    Every failure keeps today's rejection exactly as it was and leaves the
    request runnable for the next cycle — nothing is flagged for a human
    (invariant 11). ``bv_result`` is mutated only on the final success, after
    every fallible step has already succeeded, and the caller learns the new
    identity from :attr:`MergeRekeyOutcome.survivor` rather than from a
    mutated argument.

    The pre-check narrows the race; it cannot close it, because no lock covers
    "some other request acquires this release id", and none covers "another
    lane's preview writes an evidence row at ``(survivor, fingerprint)``"
    either. If the survivor is taken in that window, the retag has already
    moved the library and the write refuses:
    :attr:`MergeRekeyOutcome.library_moved` says so, a durable
    ``download_log`` row records it for the operator, and the force lane
    refuses to launch Beets at the id the row still names. The REQUEST is
    still runnable in that world — but the LIBRARY has moved, which is why
    "nothing is flagged for a human" describes the request and never the
    library.
    """
    from contextlib import ExitStack

    from lib.pipeline_db import (
        ADVISORY_LOCK_NAMESPACE_RELEASE,
        release_id_to_lock_key,
    )

    if request_id is None:
        return MergeRekeyOutcome(MERGE_NOT_APPLICABLE, "no request row")
    stored = normalize_release_id(stored_release_id)
    old_identity = ReleaseIdentity.from_id(stored)
    if old_identity is None or old_identity.source != "musicbrainz":
        # Discogs release ids have no redirect concept; this is not an
        # adapter between the two sources.
        return MergeRekeyOutcome(
            MERGE_NOT_APPLICABLE,
            f"{stored_release_id!r} is not a MusicBrainz release id",
        )

    # The write below is fenced on all of these anyway; checking them here
    # means an unclaimed world (a YouTube rescue, a stale claim, a row
    # somebody else already moved) never spends a mirror lookup or retags the
    # shared Beets library for a rekey that could not land.
    row = db.get_request(request_id)
    if row is None:
        return MergeRekeyOutcome(
            MERGE_NOT_OWNED, f"request {request_id} no longer exists",
        )
    job = db.get_import_job(import_job_id) if import_job_id is not None else None
    if job is None or job.request_id != request_id:
        return MergeRekeyOutcome(
            MERGE_NOT_OWNED,
            (
                f"import job {import_job_id!r} does not name request "
                f"{request_id}"
            ),
        )
    if not merge_rekey_claim_holds(row, job):
        return MergeRekeyOutcome(
            MERGE_NOT_OWNED,
            (
                f"request {request_id} is {row.get('status')!r} owned by "
                f"{row.get('active_automation_import_job_id')!r}; "
                f"{job.job_type} job {job.id} ({job.status}) does not hold "
                "its import claim"
            ),
        )
    if normalize_release_id(row.get("mb_release_id")) != old_identity.release_id:
        return MergeRekeyOutcome(
            MERGE_NOT_OWNED,
            (
                f"request {request_id} now names "
                f"{row.get('mb_release_id')!r}, not the validated "
                f"{old_identity.release_id}"
            ),
        )

    survivor = canonical_release_fn(old_identity.release_id)
    new_identity = (
        ReleaseIdentity.from_id(survivor) if survivor is not None else None
    )
    if (
        new_identity is None
        or new_identity.source != "musicbrainz"
        or new_identity == old_identity
    ):
        # Fails closed on every non-answer alike, INCLUDING a resolver that
        # hands back the stored id or a non-MusicBrainz one. Its contract
        # already forbids both; a seam that authorizes a retag of installed
        # files and a rekey of the request does not delegate that check
        # (found by the generated property, which supplies both).
        return MergeRekeyOutcome(
            MERGE_NO_REDIRECT,
            f"MusicBrainz declares no successor for {old_identity.release_id}",
        )

    match = next(
        (
            candidate
            for candidate in bv_result.candidates
            if normalize_release_id(candidate.mbid) == new_identity.release_id
        ),
        None,
    )
    if match is None:
        return MergeRekeyOutcome(
            MERGE_SURVIVOR_NOT_OFFERED,
            (
                f"{old_identity.release_id} was merged into "
                f"{new_identity.release_id}, but this download does not match "
                "the survivor either"
            ),
            survivor=new_identity.release_id,
        )

    # Deterministic key order so two racing followers of the same merge queue
    # behind each other identically. The acquires are ``pg_try_advisory_lock``,
    # so a deadlock is impossible in either order; the fixed order just makes
    # contention reproducible.
    release_lock_keys = sorted({
        release_id_to_lock_key(old_identity.release_id),
        release_id_to_lock_key(new_identity.release_id),
    })
    with ExitStack() as release_locks:
        for lock_key in release_lock_keys:
            if not release_locks.enter_context(db.advisory_lock(
                ADVISORY_LOCK_NAMESPACE_RELEASE, lock_key,
            )):
                return MergeRekeyOutcome(
                    MERGE_RELEASE_LOCKED,
                    (
                        "another process holds the release lock covering "
                        f"{old_identity.release_id} -> "
                        f"{new_identity.release_id}; the library was not "
                        "touched"
                    ),
                    survivor=new_identity.release_id,
                )

        # Ask what already occupies the survivor while nothing has moved yet.
        # Both refusal causes are reads, so "retag, then discover we cannot
        # rekey" is avoidable in every world except a live race.
        collision = db.merge_rekey_collision(
            request_id,
            old_release_id=old_identity.release_id,
            new_release_id=new_identity.release_id,
        )
        if collision.blocked:
            blocked_detail = merge_rekey_blocked_audit_message(
                old_release_id=old_identity.release_id,
                new_release_id=new_identity.release_id,
                collision_detail=collision.detail(),
            )
            # The library is untouched, so nothing here is broken — but
            # nothing here re-derives either, and the operator is the only
            # one who can clear it. Durable evidence, once per execution that
            # reaches this branch.
            _record_merge_audit(
                db,
                request_id=request_id,
                log_label="MERGE REKEY BLOCKED",
                beets_detail=(
                    "merge rekey blocked before the retag onto "
                    f"{new_identity.release_id}"
                ),
                message=blocked_detail,
            )
            return MergeRekeyOutcome(
                MERGE_REKEY_BLOCKED,
                blocked_detail,
                survivor=new_identity.release_id,
            )

        retag = retag_fn(
            cfg,
            old_identity=old_identity,
            new_identity=new_identity,
        )
        if retag.outcome not in RETAG_READY_OUTCOMES:
            # Gate on membership, never on ``!= failed``: ``ambiguous`` is not
            # a failure and still must not authorize a rekey.
            return MergeRekeyOutcome(
                MERGE_RETAG_NOT_READY,
                f"library retag returned {retag.outcome}: {retag.detail}",
                survivor=new_identity.release_id,
            )

        # Only ``retagged`` means THIS execution moved the installed album.
        # ``already_current`` found it there and ``not_held`` found no album
        # at all; neither leaves a divergence this execution created. Widening
        # this to "the retag was ready" would make a refused rekey assert a
        # move that never happened — a false audit row, and a force refusal in
        # a world this execution did not create. Pinned by
        # ``test_a_ready_but_unmoved_library_never_claims_a_retag``.
        library_moved = retag.outcome == RETAG_RETAGGED

        if not db.update_request_release_for_merge(
            request_id,
            old_release_id=old_identity.release_id,
            new_release_id=new_identity.release_id,
            # The claim verified above IS the write's fence, so the job that
            # proved it is the job the compare-and-set names.
            expected_import_job_id=job.id,
        ):
            detail = (
                f"request {request_id} could not be rekeyed onto "
                f"{new_identity.release_id}; another request may already "
                "hold it (merging or deleting a request is an operator "
                "decision)"
            )
            if library_moved:
                # ONE producer for the split sentence: the durable audit row,
                # the outcome detail, and therefore the force lane's refusal
                # message are all this string.
                detail = split_identity_audit_message(
                    old_release_id=old_identity.release_id,
                    new_release_id=new_identity.release_id,
                    retag_detail=retag.detail,
                )
                _record_merge_audit(
                    db,
                    request_id=request_id,
                    log_label="MERGE REKEY SPLIT IDENTITY",
                    beets_detail=(
                        "merge rekey refused after retag onto "
                        f"{new_identity.release_id}"
                    ),
                    message=detail,
                )
            return MergeRekeyOutcome(
                MERGE_REKEY_REFUSED,
                detail,
                survivor=new_identity.release_id,
                library_moved=library_moved,
            )

    # ONE place turns a candidate into a scenario — the same function
    # ``beets_validate`` uses for the requested release (issue #1059) — and it
    # is handed THIS validation's threshold, not the config default, so a
    # rekeyed force import is named by the override it actually ran under
    # (#1080).
    from lib.beets import apply_candidate_scenario

    apply_candidate_scenario(bv_result, match, distance_threshold)
    return MergeRekeyOutcome(
        MERGE_REKEYED,
        (
            f"{old_identity.release_id} was merged into "
            f"{new_identity.release_id}; {retag.detail}"
        ),
        survivor=new_identity.release_id,
        library_moved=library_moved,
    )


def validate_release_with_merge_redirect(
    *,
    db: MergeRekeyDB,
    cfg: CratediggerConfig,
    album_path: str,
    request_id: int | None,
    release_id: str,
    import_job_id: int | None,
    distance_threshold: float,
    cancellation_token: CancellationToken | None = None,
    canonical_release_fn: CanonicalReleaseFn | None = None,
    retag_fn: MergeRetagFn | None = None,
) -> ReleaseValidation:
    """Validate one album against one exact release, following MB merges.

    THE exact-release comparison seam. Both import lanes run it, differing in
    exactly one argument — the distance threshold, which force import
    overrides to :data:`lib.beets.FORCE_IMPORT_DISTANCE_THRESHOLD` and
    automation takes from ``beets_distance_threshold`` (#1080). Everything
    else, including the merge-redirect follow, is the same code over the same
    inputs, so a request whose release MusicBrainz merged away is rescued by
    whichever lane reaches it first.

    Before #1080 only the automation lane called this: force import went
    straight to ``dispatch_import_core`` and so met the merged-away release at
    the OTHER comparison site, ``harness/import_one.py::_find_target_candidate``,
    which has no redirect concept and rejects ``mbid_missing`` forever.

    The mirror is asked ONLY on ``mbid_not_found``, so a healthy validation
    never makes a network call — the ~8,500-rows-a-cycle performance contract.

    The returned :class:`ReleaseValidation` reports the redirect outcome
    separately from the validation result. The caller decides what to do with
    each: automation routes on ``result.valid``; force import uses only
    ``merge.survivor``, because "import despite the verdict" is what force is.

    ``canonical_release_fn`` / ``retag_fn`` resolve to this module's
    production singletons when omitted. Tests INJECT replacements explicitly;
    they never patch the module binding, because patching does not replace a
    captured default (``.claude/rules/code-quality.md`` § mocks, strategy 2).
    """
    from lib.beets import beets_validate as _bv

    result = _bv(
        cfg.beets_harness_path,
        album_path,
        release_id,
        distance_threshold,
    )
    if result.scenario != "mbid_not_found":
        return ReleaseValidation(
            result,
            MergeRekeyOutcome(
                MERGE_NOT_APPLICABLE,
                f"validation named {result.scenario!r}, not mbid_not_found",
            ),
        )
    # An execution that lost its owner while the harness was running must not
    # go on to retag the shared library and move an identity. The harness is
    # the long pole here, so the checkpoint belongs after it and before the
    # first mutation, not only at the caller's next stage.
    checkpoint(cancellation_token)
    # The one place a MusicBrainz merge is followed. Gated on the exact
    # scenario so the mirror is never touched by a healthy validation.
    merge = _follow_merged_release(
        result,
        db=db,
        cfg=cfg,
        request_id=request_id,
        stored_release_id=release_id,
        import_job_id=import_job_id,
        distance_threshold=distance_threshold,
        canonical_release_fn=(
            canonical_release_fn
            if canonical_release_fn is not None
            else _PRODUCTION_CANONICAL_RELEASE_FN
        ),
        retag_fn=(
            retag_fn if retag_fn is not None else _retag_merged_album_with_beets
        ),
    )
    if merge.rekeyed:
        logger.info(
            "MERGE REKEY: request=%s %s (now scenario=%s valid=%s)",
            request_id,
            merge.detail,
            result.scenario,
            result.valid,
        )
    elif merge.status not in (MERGE_NOT_APPLICABLE, MERGE_NO_REDIRECT):
        # Surfaced, never parked: the existing rejection stands and the
        # request goes back to the search pool for the next cycle.
        logger.warning(
            "MERGE REKEY DECLINED (%s): request=%s %s",
            merge.status,
            request_id,
            merge.detail,
        )
    return ReleaseValidation(result, merge)


def _check_staged_audio_manifest(
    album_data: GrabListEntry,
    staged_album: StagedAlbum,
) -> tuple[bool, str]:
    check = check_audio_manifest(
        staged_album.current_path,
        tracked_audio_paths_for_downloads(album_data.files),
    )
    if check.ok:
        return True, ""
    detail = (
        "Staged import folder does not match the selected audio manifest: "
        f"{check.detail()}"
    )
    logger.error(
        "IMPORT MANIFEST REJECTED: request_id=%s path=%s %s",
        album_data.db_request_id,
        staged_album.current_path,
        detail,
    )
    return False, detail


def _process_beets_validation(
    album_data: GrabListEntry,
    staged_album: StagedAlbum,
    ctx: CratediggerContext,
    *,
    import_job_id: int,
    handle_valid_fn: HandleValidFn | None = None,
    dispatch_fn: DispatchCoreFn | None = None,
    cancellation_token: CancellationToken | None = None,
    owner_proof: ExecutionOwnerProof | None = None,
    canonical_release_fn: CanonicalReleaseFn = _PRODUCTION_CANONICAL_RELEASE_FN,
    retag_fn: MergeRetagFn = _retag_merged_album_with_beets,
    tag_sync_fn: MergeTagSyncFn = _sync_release_tags_with_beets,
) -> DispatchOutcome | None:
    """Validate one exact release and route its canonical result.

    Candidate evidence must already have been produced by preview. Missing
    evidence requeues the job to preview; the importer never measures inline.

    ``canonical_release_fn``, ``retag_fn``, and ``tag_sync_fn`` are
    definition-time defaults for the MusicBrainz merge seam below: tests
    INJECT replacements, they never patch the module binding, because
    patching does not replace a captured default
    (``.claude/rules/code-quality.md`` § mocks, strategy 2).
    """
    current_path = staged_album.current_path
    manifest_ok, manifest_detail = _check_staged_audio_manifest(
        album_data,
        staged_album,
    )
    logger.info(
        "MANIFEST-TRACE check request=%s ok=%s %s actual_audio=%s path=%s",
        album_data.db_request_id,
        manifest_ok,
        manifest_trace_summary(album_data.files),
        len(audio_relative_paths(current_path)),
        current_path,
    )
    if not manifest_ok:
        return _reject_request_auto_import(
            album_data,
            ValidationResult(
                valid=False,
                scenario="untracked_audio",
                detail=manifest_detail,
                error=manifest_detail,
                path=current_path,
            ),
            staged_album,
            ctx,
            detail=manifest_detail,
            scenario="untracked_audio",
            error=manifest_detail,
            import_job_id=import_job_id,
            cancellation_token=cancellation_token,
        )
    checkpoint(cancellation_token)
    validation = validate_release_with_merge_redirect(
        db=ctx.pipeline_db_source._get_db(),
        cfg=ctx.cfg,
        album_path=current_path,
        request_id=album_data.db_request_id,
        release_id=album_data.mb_release_id,
        import_job_id=import_job_id,
        distance_threshold=ctx.cfg.beets_distance_threshold,
        cancellation_token=cancellation_token,
        canonical_release_fn=canonical_release_fn,
        retag_fn=retag_fn,
    )
    bv_result = validation.result
    if validation.merge.rekeyed and validation.merge.survivor is not None:
        # The row and the library are both at the survivor now; the in-flight
        # entry follows so dispatch imports the identity that was rekeyed.
        album_data.mb_release_id = validation.merge.survivor
        checkpoint(cancellation_token)
        # Best-effort file-tag convergence at the survivor (#1260), for
        # EVERY completed rekey. Deliberately NOT gated on
        # ``bv_result.valid``: validity is a Beets MATCH verdict, and a
        # valid revalidation can still be quality-REJECTED downstream
        # (``full_pipeline_decision_from_evidence`` — the -W cohort is by
        # construction competing with an installed copy, where rejection
        # is likely), in which case no import replaces the files. Only an
        # ACCEPTED import rewrites tags itself, and this seam cannot know
        # acceptance — so the write is wasted-but-harmless when acceptance
        # follows, and the heal otherwise (#1260 review F1). The sync
        # asserts nothing about the file-tag world: an ``already_current``
        # or ``not_held`` retag re-derives inside the service from Beets
        # itself, to whichever typed outcome the re-read shows (review
        # F2, re-review C2).
        # Outcome-inert by contract: the helper never raises and nothing
        # reads its result. It swallows even a cancellation raised inside
        # its DB-lock I/O — benign ONLY because ``checkpoint`` on the
        # next line re-raises; keep that pairing if this call ever moves
        # (review F8). The checkpoint ABOVE keeps an already-cancelled job
        # from spending the write budget first.
        _sync_file_tags_after_merge_rekey(
            ctx.pipeline_db_source._get_db(),
            ctx.cfg,
            validation.merge.survivor,
            sync_fn=tag_sync_fn,
        )
    checkpoint(cancellation_token)
    usernames_pre = {f.username for f in album_data.files if f.username}
    bv_result.soulseek_username = (
        ", ".join(sorted(usernames_pre)) if usernames_pre else None
    )
    bv_result.download_folder = current_path
    bv_result.source_dirs = source_dirs_for_album(album_data)
    if bv_result.valid:
        checkpoint(cancellation_token)
        db = ctx.pipeline_db_source._get_db()
        candidate_result = ensure_candidate_evidence_for_action(
            db,
            source_path=current_path,
            import_job_id=import_job_id,
        )
        if not candidate_result.available:
            reason = (
                candidate_result.provenance.fallback_reason
                or candidate_result.provenance.candidate_status
                or "missing"
            )
            return _requeue_import_job_to_preview(
                db,
                import_job_id=import_job_id,
                reason=reason,
                expected_execution_lease=(
                    owner_proof.execution_lease if owner_proof is not None else None
                ),
            )
        resolved_handle_valid = (
            handle_valid_fn if handle_valid_fn is not None else _handle_valid_result
        )
        return resolved_handle_valid(
            album_data,
            bv_result,
            staged_album,
            ctx,
            import_job_id=import_job_id,
            prevalidated_candidate_result=candidate_result,
            dispatch_fn=dispatch_fn,
            cancellation_token=cancellation_token,
            owner_proof=owner_proof,
        )
    return _handle_rejected_result(
        album_data,
        bv_result,
        staged_album,
        ctx,
        import_job_id=import_job_id,
        cancellation_token=cancellation_token,
    )


def _handle_valid_result(
    album_data: GrabListEntry,
    bv_result: ValidationResult,
    staged_album: StagedAlbum,
    ctx: CratediggerContext,
    *,
    import_job_id: int | None = None,
    prevalidated_candidate_result: CandidateEvidenceActionResult | None = None,
    quality_gate_fn: QualityGateFn | None = None,
    dispatch_fn: DispatchCoreFn | None = None,
    cancellation_token: CancellationToken | None = None,
    owner_proof: ExecutionOwnerProof | None = None,
) -> DispatchOutcome | None:
    """Dispatch a valid exact-release result from its authoritative path.

    Request imports remain at their durable processing-owner path. Redownloads
    move to manual-review staging and mark the request done.
    """
    from contextlib import nullcontext

    from lib.pipeline_db import (
        ADVISORY_LOCK_NAMESPACE_RELEASE,
        release_id_to_lock_key,
    )

    source_type = album_data.db_source or "redownload"
    request_id = album_data.db_request_id
    dist = bv_result.distance if bv_result.distance is not None else 1.0
    wants_auto_import = (
        source_type == "request"
        and dist <= ctx.cfg.beets_distance_threshold
    )

    if wants_auto_import and request_id is None:
        return _reject_request_auto_import(
            album_data,
            bv_result,
            staged_album,
            ctx,
            detail=(
                "Request auto-import is missing db_request_id; automatic "
                "resume/import is disabled."
            ),
            scenario="request_missing_request_id",
            error="missing_request_id",
            import_job_id=import_job_id,
            cancellation_token=cancellation_token,
        )

    if wants_auto_import and not album_data.mb_release_id:
        return _reject_request_auto_import(
            album_data,
            bv_result,
            staged_album,
            ctx,
            detail="Request auto-import requires a MusicBrainz release ID",
            scenario="request_missing_mbid",
            error="missing_mbid",
            import_job_id=import_job_id,
            cancellation_token=cancellation_token,
        )

    will_auto_import = wants_auto_import
    pdb = None

    if will_auto_import and album_data.mb_release_id:
        pdb = ctx.pipeline_db_source._get_db()
        lock_ctx = pdb.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_RELEASE,
            release_id_to_lock_key(album_data.mb_release_id),
        )
    else:
        lock_ctx = nullcontext(True)

    with lock_ctx as got_release_lock:
        if not got_release_lock:
            logger.warning(
                f"AUTO-IMPORT DEFERRED: {album_data.artist} - "
                f"{album_data.title} — release lock held by another "
                f"process (mbid={album_data.mb_release_id}); skipping "
                "dispatch. Files stay at "
                f"{staged_album.current_path} so the next cycle can "
                "idempotently resume from process_completed_album."
            )
            if owner_proof is not None:
                return _requeue_import_job_to_preview(
                    ctx.pipeline_db_source._get_db(),
                    import_job_id=import_job_id,
                    reason="release lock contention",
                    expected_execution_lease=owner_proof.execution_lease,
                )
            return DispatchOutcome(
                success=False,
                message=(
                    "Another import is already in progress for "
                    f"this release ({album_data.mb_release_id})"
                ),
                deferred=True,
            )

        if will_auto_import:
            # The processing handoff persisted this exact path as immutable
            # owner provenance. Beets launch, journaled cleanup, and terminal
            # acknowledgement all fence on the same value, so relocating the
            # folder here would split live filesystem state from its durable
            # authority.
            dest = staged_album.current_path
        else:
            checkpoint(cancellation_token)
            dest = staged_album.move_to(
                stage_to_ai_path(
                    artist=album_data.artist,
                    title=album_data.title,
                    staging_dir=ctx.cfg.beets_staging_dir,
                    request_id=request_id,
                    auto_import=False,
                ),
                cancellation_token=cancellation_token,
            )
        checkpoint(cancellation_token)
        album_data.import_folder = dest
        log_validation_result(album_data, bv_result, ctx.cfg, dest_path=dest)
        logger.info(
            f"{'PROCESSING SOURCE' if will_auto_import else 'STAGED'}: "
            f"{album_data.artist} - {album_data.title} "
            f"(scenario={bv_result.scenario}, "
            f"distance={bv_result.distance:.4f}) → {dest}"
        )

        dl_info = _build_download_info(album_data)
        dl_info.validation_result = bv_result.to_json()
        if album_data.download_spectral is not None:
            dl_info.download_spectral = album_data.download_spectral
            dl_info.current_spectral = album_data.current_spectral
            dl_info.existing_min_bitrate = album_data.current_min_bitrate
            dl_info.slskd_filetype = dl_info.filetype
            dl_info.actual_filetype = dl_info.filetype
        if will_auto_import:
            assert request_id is not None, "pipeline request must have db_request_id"
            assert pdb is not None, "auto-import path must hold a pipeline DB handle"
            # This branch is reached only for ``bv_result.valid``, and
            # ``beets_validate`` sets ``valid`` in exactly one place — the
            # ``strong_match`` arm, which names the scenario in the same
            # statement. The ``or "auto_import"`` placeholder that used to
            # sit on both dispatch calls below was therefore unreachable
            # (zero live ``download_log`` rows ever carried it) and is gone
            # (issue #888).
            dispatch_scenario = bv_result.scenario
            assert dispatch_scenario is not None, (
                "beets_validate names a scenario on every valid result"
            )
            current_spectral = album_data.current_spectral
            # Codec-aware (issue #829 Phase 5 PR2b). This fallback seam holds
            # only a fresh spectral audit of the installed files, so the
            # measured ``codec_family``/``cliff_hz`` captured alongside the
            # grade are the whole codec context; a legacy audit that captured
            # neither withholds, which leaves the container bitrate untouched.
            # ``lib/dispatch/core.py`` overrides this value from linked current
            # evidence (a strictly richer resolution) whenever one exists.
            override_min_bitrate = compute_effective_override_bitrate(
                album_data.current_min_bitrate,
                interpret_spectral_evidence(SpectralEvidenceFacts(
                    spectral_grade=(
                        current_spectral.grade
                        if current_spectral is not None
                        else None
                    ),
                    codec_family=(
                        current_spectral.codec_family
                        if current_spectral is not None
                        else None
                    ),
                    cliff_hz=(
                        current_spectral.cliff_hz
                        if current_spectral is not None
                        else None
                    ),
                    spectral_bitrate_kbps=(
                        current_spectral.bitrate_kbps
                        if current_spectral is not None
                        else None
                    ),
                )),
            )

            resolved_quality_gate_fn = (
                quality_gate_fn
                if quality_gate_fn is not None
                else _check_quality_gate_core
            )
            checkpoint(cancellation_token)
            # One construction, two possible callees. Before issue #1277 this
            # same 25-kwarg call was spelled twice, verbatim, differing only
            # in which callable it named — the widest drift surface in the
            # file. The checkpoint above still precedes every argument
            # evaluation, exactly as it did when the arguments were the call's
            # own kwargs.
            dispatch_request = DispatchRequest(
                path=dest,
                mb_release_id=album_data.mb_release_id or "",
                request_id=request_id,
                label=f"{album_data.artist} - {album_data.title}",
                force=False,
                override_min_bitrate=override_min_bitrate,
                target_format=album_data.db_target_format,
                verified_lossless_target=ctx.cfg.verified_lossless_target,
                beets_harness_path=ctx.cfg.beets_harness_path,
                dl_info=dl_info,
                distance=bv_result.distance,
                scenario=dispatch_scenario,
                files=album_data.files,
                outcome_label="success",
                requeue_on_failure=True,
                cooled_down_users=ctx.cooled_down_users,
                source_dirs=source_dirs_for_album(album_data),
                candidate_import_job_id=import_job_id,
                candidate_download_log_id=None,
                prevalidated_candidate_result=prevalidated_candidate_result,
                execution_lease=(
                    owner_proof.execution_lease if owner_proof is not None else None
                ),
                owner_session_identity=(
                    owner_proof.owner_session_identity
                    if owner_proof is not None else None
                ),
            )
            dispatch = (
                dispatch_fn if dispatch_fn is not None
                else dispatch_import_core
            )
            return dispatch(
                dispatch_request,
                pdb,
                cfg=ctx.cfg,
                quality_gate_fn=resolved_quality_gate_fn,
                cancellation_token=cancellation_token,
            )
        checkpoint(cancellation_token)
        pending = ctx.pipeline_db_source.mark_done(
            album_data,
            bv_result,
            dest_path=dest,
            download_info=dl_info,
            import_job_id=import_job_id,
        )
        if import_job_id is not None:
            from lib.terminal_outcomes import PendingImportTerminalOutcome
            if isinstance(pending, PendingImportTerminalOutcome):
                return DispatchOutcome(
                    success=True,
                    message="Staged for manual review",
                    terminal_outcome=pending,
                )
        return None


# Executable, pyright-visible proof that production functions implement the
# exact injection contracts used by the completion orchestrator and tests.
_validate_conformance: ValidateFn = _process_beets_validation
_handle_valid_conformance: HandleValidFn = _handle_valid_result
