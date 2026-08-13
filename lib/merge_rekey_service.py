"""Operator merge-rekey action — heal a request→Beets join after MusicBrainz
merges the release a request points at (#1089).

MusicBrainz editors merge two release entries; the loser's MBID becomes a
permanent redirect to the survivor. #1059/#1080 taught the automation and
force import lanes to follow that redirect at import time — but the
population this service acts on has ALREADY imported, under the merged-away
id, and Beets has since been retagged onto the survivor out of band (an
operator's own ``beet modify``, or a later re-import that landed there). The
pipeline ledger is the only thing still naming the old id, so the request
→Beets join silently breaks: the dashboard's disk-coverage drift panel
(``lib/disk_coverage_service.py``) reports the row as off-disk forever,
because ``BeetsDB.check_mbids`` can never again match the stored id.

**Request-ledger-only.** This service NEVER mutates Beets — that is the
settled operator correction on this slice. For every row it can legitimately
act on, Beets already holds the survivor; that is exactly what makes the row
drift. The one write is ``PipelineDB.update_request_release_for_merge``'s
operator claim arm, which moves ``album_requests.mb_release_id`` (and the
row's ``album_quality_evidence`` lineage) onto the identity Beets already
has. The three-mutation-lane boundary (serial importer harness, exact-album
delete child, import-time merge retag) is untouched.

Authority: "really we need to re-key mbid and beets don't we so they go
away. we could surface these here and have a button which re-keys with the
current machinery we've built couldn't we?" — followed by, on the
request-ledger-only refinement: "this is what I want for sure." (operator,
2026-08-13 session) —
https://github.com/abl030/cratedigger/issues/1089#issuecomment-5274933957

**Known residual: ``mb_release_group_id`` can go stale on a cross-release-
group merge (#1089 NOTE-4 / MAJOR-B, review rounds 2-3).** This is a
PRE-EXISTING residual shared by all three rekey arms — not unique to the
operator arm. #1059/#1080's own retag/rekey (the automation lane while
``processing``, and the #1080 force lane, which CAN run against a row
already ``imported`` and returns it to ``imported``) can leave exactly the
same stale ``mb_release_group_id`` naming the LOSING release's group after
the identity itself has moved on to the survivor, if that survivor belongs
to a different release group. Nothing self-heals it on ANY of the three
arms: ``field_resolver_service.py::apply_resolve_all_result`` writes
``mb_release_group_id`` only when the existing value is ``None``
(``existing_mb_release_group_id is None``), and
``POST /api/pipeline/<id>/resolve-rg``
(``web/routes/release_identity_routes.py::post_pipeline_resolve_rg``) is
explicitly idempotent and returns an already-non-null value UNTOUCHED — so
neither the ordinary lazy backfill nor the operator's own manual resolve-rg
tool can ever correct a stale-but-present group id. The tagged resolver's
fetch (``{base}/release/<id>?fmt=json``, no ``inc=`` clause) never requests
release-group data at all, so the survivor's release-group id is not a fact
this lookup already has lying around either — growing the fetch contract to
request it would be new surface for one residual field shared by all three
arms, not a use of data already in hand. Left as a documented gap; only a
direct operator write (``pipeline-cli query --write --confirm WRITE -``)
can correct it, if a stale group id is ever observed to matter in practice.

``pipeline-cli merge-rekey`` and ``POST /api/pipeline/<id>/merge-rekey`` are
thin adapters that wrap ``MergeRekeyService.rekey_request`` — the CLI relays
the canonical web route's response (CD-QUAL-01 shape), it does not construct
this service directly.

Outcome → exit code / HTTP status convention (matches
``lib/mbid_replace_service.py`` / ``lib/search_plan_service.py``):

    rekeyed                   200 / 0
    not_found                 404 / 2
    wrong_state                409 / 4
    not_merged                 422 / 3
    library_not_at_survivor    409 / 4
    library_still_at_stored    409 / 4
    evidence_fingerprint_mismatch 409 / 4
    survivor_collision         409 / 4
    rekey_refused               409 / 4
    mirror_unavailable          503 / 5
    beets_unavailable           503 / 5
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Protocol, runtime_checkable

import msgspec

from lib.beets_db import (
    BEETS_AUTHORITY_UNAVAILABLE_MESSAGE,
    CurrentBeetsAmbiguous,
    CurrentBeetsMissing,
    CurrentBeetsResolution,
    CurrentBeetsUnique,
    beets_authority_availability_category,
)
from lib.mb_canonical import (
    CanonicalReleaseRedirected,
    CanonicalReleaseUnavailable,
    TaggedCanonicalReleaseFn,
    production_tagged_canonical_release_fn,
)
from lib.quality_evidence import SnapshotAudioFilesError, fingerprint_album_path
from lib.release_identity import ReleaseIdentity

if TYPE_CHECKING:
    from lib.import_queue import ImportJob
    from lib.pipeline_db._shared import MergeRekeyCollision
    from lib.pipeline_db.rows import AlbumRequestRow
    from lib.quality_evidence import AlbumQualityEvidence

logger = logging.getLogger(__name__)

RESULT_REKEYED = "rekeyed"
RESULT_NOT_FOUND = "not_found"
RESULT_WRONG_STATE = "wrong_state"
RESULT_MIRROR_UNAVAILABLE = "mirror_unavailable"
RESULT_BEETS_UNAVAILABLE = "beets_unavailable"
RESULT_NOT_MERGED = "not_merged"
RESULT_LIBRARY_NOT_AT_SURVIVOR = "library_not_at_survivor"
RESULT_LIBRARY_STILL_AT_STORED = "library_still_at_stored"
#: Named after ``lib.world_invariants``'s own ``evidence_fingerprint_mismatch``
#: code, but NOT because the two check "the same fact the same way" — the
#: audit uses four separate codes (``evidence_link_without_album``,
#: ``current_evidence_missing``, ``current_evidence_dangling``,
#: ``evidence_fingerprint_mismatch``) for the distinct facts this ONE
#: operator-facing outcome collapses (no linked evidence, a dangling link,
#: an unwitnessable survivor, a genuine mismatch). The honest rationale is
#: narrower: one outcome per OPERATOR DECISION, not one outcome per
#: underlying fact — every one of these worlds asks the operator to look
#: and decide, so they share a single actionable name here (#1089 MAJOR-3,
#: review round 2; corrected #1089 m3, review round 4).
RESULT_EVIDENCE_FINGERPRINT_MISMATCH = "evidence_fingerprint_mismatch"
RESULT_SURVIVOR_COLLISION = "survivor_collision"
RESULT_REKEY_REFUSED = "rekey_refused"

#: The route's status mapping. The CLI needs no paired exit-code dict of its
#: own: every status here already matches ``_exit_code``'s default
#: status->exit mapping (200/0, 404/2, 422/3, 409/4, 503/5) in
#: ``scripts/pipeline_cli/api_mutations.py``, so ``cmd_merge_rekey`` relays
#: with no ``exit_overrides``.
MERGE_REKEY_HTTP_STATUS: dict[str, int] = {
    RESULT_REKEYED: 200,
    RESULT_NOT_FOUND: 404,
    RESULT_WRONG_STATE: 409,
    RESULT_NOT_MERGED: 422,
    RESULT_LIBRARY_NOT_AT_SURVIVOR: 409,
    RESULT_LIBRARY_STILL_AT_STORED: 409,
    RESULT_EVIDENCE_FINGERPRINT_MISMATCH: 409,
    RESULT_SURVIVOR_COLLISION: 409,
    RESULT_REKEY_REFUSED: 409,
    RESULT_MIRROR_UNAVAILABLE: 503,
    RESULT_BEETS_UNAVAILABLE: 503,
}


class MergeRekeyResult(msgspec.Struct, frozen=True):
    """Outcome of one ``rekey_request`` call.

    ``outcome`` is one of the ``RESULT_*`` constants above.
    ``new_release_id`` echoes ``old_release_id`` (#1089 NOTE-5, review
    round 2) on ``not_merged`` specifically — MusicBrainz answered and
    named no DIFFERENT survivor, so there is no distinct "new" identity to
    report; the stored id remains current as far as this lookup can tell,
    and the field names that fact rather than being left ``None``.
    ``beets_album_ids`` carries what Beets currently resolves at the id named
    by ``beets_checked_release_id`` — populated on ``not_merged`` (what the
    stored id itself holds, the #8792 refusal), on
    ``library_not_at_survivor`` (what the survivor holds, when it is
    anything other than exactly one album), and on
    ``library_still_at_stored`` (what the stored id still holds) — so the UI
    can explain the refusal instead of a bare error string.
    ``rival_request_id`` / ``colliding_fingerprints`` are populated only on
    ``survivor_collision``, mirroring
    ``lib.pipeline_db._shared.MergeRekeyCollision``.
    """

    outcome: str
    request_id: int
    old_release_id: str | None = None
    new_release_id: str | None = None
    beets_album_id: int | None = None
    beets_checked_release_id: str | None = None
    beets_album_ids: tuple[int, ...] = ()
    rival_request_id: int | None = None
    colliding_fingerprints: tuple[str, ...] = ()
    error_message: str | None = None


@runtime_checkable
class MergeRekeyDB(Protocol):
    """The PipelineDB surface the operator merge-rekey action uses (#1089)."""

    def get_request(self, request_id: int) -> AlbumRequestRow | None: ...

    def list_active_import_jobs(
        self, *, request_id: int | None = None, limit: int = 50,
    ) -> list[ImportJob]: ...

    def load_album_quality_evidence_by_id(
        self, evidence_id: int | None,
    ) -> AlbumQualityEvidence | None: ...

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
        expected_import_job_id: int | None,
    ) -> bool: ...


@runtime_checkable
class MergeRekeyBeetsDB(Protocol):
    """The BeetsDB surface this action uses — read-only, on purpose.

    Request-ledger-only: this service never calls a Beets mutation method.
    """

    def resolve_current_release(
        self, identity: ReleaseIdentity,
    ) -> CurrentBeetsResolution: ...


def _beets_album_ids(resolution: CurrentBeetsResolution) -> tuple[int, ...]:
    if isinstance(resolution, CurrentBeetsUnique):
        return (resolution.album_id,)
    if isinstance(resolution, CurrentBeetsAmbiguous):
        return resolution.album_ids
    return ()


class MergeRekeyService:
    """Service for the operator merge-rekey action.

    Construct one per call (or per logical caller); it is stateless beyond
    its dependencies. ``canonical_release_fn`` defaults to the process-wide
    configured tagged resolver
    (``lib.mb_canonical.production_tagged_canonical_release_fn``) — inert
    until ``configure_canonical_release_lookup`` has run at process startup
    (``web/server.py::main`` / ``scripts/importer.py::main``).
    """

    def __init__(
        self,
        db: MergeRekeyDB,
        beets_db: MergeRekeyBeetsDB,
        *,
        canonical_release_fn: TaggedCanonicalReleaseFn | None = None,
    ) -> None:
        self.db = db
        self.beets_db = beets_db
        self.canonical_release_fn = (
            canonical_release_fn
            if canonical_release_fn is not None
            else production_tagged_canonical_release_fn()
        )

    def _resolve_current_release(
        self, identity: ReleaseIdentity, *, request_id: int,
    ) -> tuple[CurrentBeetsResolution | None, MergeRekeyResult | None]:
        """Read ALREADY-OPEN Beets state, classifying an authority failure
        into ``beets_unavailable`` using the same
        ``beets_authority_availability_category`` classify-or-reraise idiom
        as ``web/routes/pipeline.py``'s own boundary: a locked/IO/cant-open
        SQLite read is retryable (503); anything else is a real bug and
        re-raises. This covers only reads through an already-open handle —
        the caller (``web/routes/release_identity_routes.py::
        post_pipeline_merge_rekey``) classifies the earlier OPEN of that
        handle separately, with the identical idiom, before this service is
        even constructed. Returns ``(resolution, None)`` on success or
        ``(None, failure_result)`` on a classified failure.
        """
        try:
            return self.beets_db.resolve_current_release(identity), None
        except Exception as exc:
            category = beets_authority_availability_category(exc)
            if category is None and not isinstance(exc, OSError):
                raise
            logger.exception(
                "current Beets authority unavailable for request %s (%s)",
                request_id, category or type(exc).__name__,
            )
            return None, MergeRekeyResult(
                outcome=RESULT_BEETS_UNAVAILABLE,
                request_id=request_id,
                error_message=BEETS_AUTHORITY_UNAVAILABLE_MESSAGE,
            )

    def _verify_survivor_evidence_lineage(
        self,
        *,
        request_id: int,
        old_release_id: str,
        survivor: str,
        current_evidence_id: int | None,
        survivor_resolution: CurrentBeetsUnique,
    ) -> MergeRekeyResult | None:
        """#1089 MAJOR-3 / MAJOR-C (review rounds 2-3) — a request must
        witness that the survivor album's ACTUAL bytes are the ones its
        linked current evidence describes, not merely that Beets shows
        exactly one album there and nothing at the stored id.

        "Stored empty + survivor unique" is ALSO satisfied when this
        request's own album was deleted out of band and an unrelated,
        pipeline-untracked album happens to occupy the survivor MBID —
        ``merge_rekey_collision`` only sees rival REQUESTS, never untracked
        Beets albums (the untracked cohort is first-class in
        ``lib.disk_coverage_service.BeetsUntrackedAlbum``). Adopting that
        album would transplant this request's evidence lineage — including
        any verified-lossless proof — onto bytes nobody ever measured for
        this request: a proof lock protecting the wrong album.

        **The witness is MANDATORY, not conditional on a linked row (#1089
        MAJOR-C, review round 3).** An earlier version skipped this witness
        entirely with no linked current evidence, reasoning "nothing to
        transplant, nothing to protect" — false in both directions: the
        write moves EVERY evidence row at the old id regardless of which
        one (if any) ``current_evidence_id`` names, so there can be
        evidence to transplant even with no linked row, and the
        untracked-album adoption hazard above is completely unwitnessed
        for that population. No linked current evidence therefore refuses
        too — the request has no proof to check the adoption against, and
        the operator decides. Live exposure at the time of the fix was
        zero (0 of 7,224 imported rows lacked ``current_evidence_id``), so
        this correction changes no measured live outcome; it forecloses a
        real, if currently unobserved, gap.

        Deliberately NOT path equality: capture-time paths are history, and
        a legitimate retag+move breaks them (verbatim from
        ``lib.world_invariants.EvidenceDiskSnapshot``'s own docstring: "the
        content fingerprint resolved from fresh Beets authority, never path
        equality with that historical snapshot"). The witness here calls
        ``lib.quality_evidence.fingerprint_album_path`` — the ONE canonical
        composition of ``snapshot_audio_files`` + ``snapshot_fingerprint``
        (#1089 NOTE-H, review round 3), which ``lib.world_audit_service``
        calls for its own ``evidence_fingerprint_mismatch`` invariant too —
        genuinely reused, never a second, textually-duplicated fingerprint
        formula. It also returns ``None`` for a vanished or genuinely-empty
        survivor directory (#1089 NOTE-I): an installed album with zero
        audio files is not a witnessable survivor, so that world refuses
        here rather than silently comparing against the empty-fileset
        digest.

        **Known limitation:** an out-of-band retag that WRITES tags (not
        the sanctioned import-time retag, which is ``-W`` no-write and so
        never trips this) changes file sizes and will fail this witness
        closed. That refusal is operator-visible (``evidence_fingerprint_
        mismatch``, an honest "verify me" message), not silent corruption;
        the escape is manual operator reconciliation, same as any other
        witness mismatch.

        Returns ``None`` when the survivor's freshly computed fingerprint
        equals the linked evidence row's ``snapshot_fingerprint`` — the
        write may proceed.
        """
        if current_evidence_id is None:
            return MergeRekeyResult(
                outcome=RESULT_EVIDENCE_FINGERPRINT_MISMATCH,
                request_id=request_id,
                old_release_id=old_release_id,
                new_release_id=survivor,
                beets_album_id=survivor_resolution.album_id,
                error_message=(
                    f"request {request_id} has no current evidence lineage "
                    "to witness the survivor adoption with; the operator "
                    "decides"
                ),
            )
        evidence = self.db.load_album_quality_evidence_by_id(
            current_evidence_id,
        )
        if evidence is None:
            return MergeRekeyResult(
                outcome=RESULT_EVIDENCE_FINGERPRINT_MISMATCH,
                request_id=request_id,
                old_release_id=old_release_id,
                new_release_id=survivor,
                beets_album_id=survivor_resolution.album_id,
                error_message=(
                    f"request {request_id} links evidence "
                    f"{current_evidence_id}, which no longer exists; the "
                    "album at the survivor cannot be verified against it"
                ),
            )
        try:
            actual_fingerprint = fingerprint_album_path(
                survivor_resolution.album_path,
            )
        except SnapshotAudioFilesError as exc:
            logger.exception(
                "could not compute a fresh fingerprint for the survivor "
                "album at %s (request %s)",
                survivor_resolution.album_path, request_id,
            )
            return MergeRekeyResult(
                outcome=RESULT_EVIDENCE_FINGERPRINT_MISMATCH,
                request_id=request_id,
                old_release_id=old_release_id,
                new_release_id=survivor,
                beets_album_id=survivor_resolution.album_id,
                error_message=(
                    "could not read the survivor album's files to verify "
                    f"them against request {request_id}'s evidence: {exc}"
                ),
            )
        if actual_fingerprint is None:
            # #1089 NOTE-I (review round 3): a vanished or genuinely-empty
            # survivor album directory is not a witnessable survivor —
            # fingerprint_album_path returns None for exactly this, never
            # the empty-fileset digest. Silently accepting that digest
            # would let a linked evidence row whose OWN recorded
            # fingerprint happens to be that same degenerate value read as
            # "matches" against an album that is actually gone.
            logger.warning(
                "the survivor album at %s (request %s) walked cleanly but "
                "has zero audio files — not witnessable",
                survivor_resolution.album_path, request_id,
            )
            return MergeRekeyResult(
                outcome=RESULT_EVIDENCE_FINGERPRINT_MISMATCH,
                request_id=request_id,
                old_release_id=old_release_id,
                new_release_id=survivor,
                beets_album_id=survivor_resolution.album_id,
                error_message=(
                    f"the survivor album has no audio files to verify "
                    f"against request {request_id}'s evidence; the "
                    "operator must decide"
                ),
            )
        if actual_fingerprint != evidence.snapshot_fingerprint:
            return MergeRekeyResult(
                outcome=RESULT_EVIDENCE_FINGERPRINT_MISMATCH,
                request_id=request_id,
                old_release_id=old_release_id,
                new_release_id=survivor,
                beets_album_id=survivor_resolution.album_id,
                error_message=(
                    f"the album at the survivor is not the one request "
                    f"{request_id}'s evidence describes (expected "
                    f"fingerprint {evidence.snapshot_fingerprint!r}, found "
                    f"{actual_fingerprint!r}); the operator must decide"
                ),
            )
        return None

    def _rekey_refused_message(self, request_id: int) -> str:
        """#1089 MINOR-4 (review round 2) — the write's compare-and-set
        collapses several distinct causes into one ``rowcount = 0``; the
        original message named only the RACE causes ("changed underneath
        the rekey", "a rival took it"), but the write's own predicate names
        a fourth, far more ORDINARY one: a queued/running import job that
        was already there the whole time (step 2's own precondition never
        checks ``import_jobs`` at all — only the write's own ``NOT EXISTS``
        term does). For that world nothing raced and nothing changed;
        retrying immediately cannot succeed, so the message must say
        wait-for-drain instead of retry. Diagnosed post-hoc (only once the
        write has already refused) rather than pre-checked, since the
        write's own atomic predicate remains the sole authority on WHY it
        refused — this only chooses which honest sentence to show.
        """
        blocking_jobs = [
            job for job in self.db.list_active_import_jobs(
                request_id=request_id,
            )
            if job.status in ("queued", "running")
        ]
        if blocking_jobs:
            return (
                f"request {request_id} has a queued or running import job "
                "— the merge-rekey write refuses while any import job "
                "could still move this request's identity; nothing changed "
                "underneath this call, so an immediate retry will not "
                "help — wait for the job to finish first"
            )
        return (
            f"request {request_id} changed underneath the rekey, or a "
            "rival request took the survivor in the same instant — retry"
        )

    def rekey_request(self, request_id: int) -> MergeRekeyResult:
        """Rekey ``request_id`` onto MusicBrainz's current survivor, or refuse.

        1. Load the request; missing → ``not_found``.
        2. Require MB-sourced (not Discogs-only), ``status == 'imported'``,
           no automation owner attached → else ``wrong_state``. This is the
           service's own precondition, checked BEFORE any network or Beets
           call — it never spends either on a row the write would refuse
           for a reason this call can already see. The raw stored
           ``mb_release_id`` is still reported even when identity resolution
           itself failed (e.g. a conflicting Discogs field), so the payload
           never reads as "no identity at all".
        3. Resolve the stored id's current MusicBrainz survivor at click
           time via the TAGGED resolver
           (``lib.mb_canonical.canonical_release_status``), which keeps
           "no answer obtained" distinct from "MusicBrainz answered, no
           redirect" — the collapsed ``CanonicalReleaseFn`` the import seam
           uses cannot make that distinction, and conflating them here would
           read a down mirror as "MusicBrainz confirms this was never
           merged" (#1089 BLOCKING-1).

           * ``CanonicalReleaseUnavailable`` → ``mirror_unavailable``: no
             answer was obtained at all (unconfigured or unreachable).
           * ``CanonicalReleaseCurrent``, OR a ``CanonicalReleaseRedirected``
             this call does not trust (survivor equal to the stored id, or
             not itself a MusicBrainz identity — a defensive re-check on the
             injected resolver, mirroring the #1059 seam's own
             don't-trust-the-resolver-blindly precedent) → ``not_merged``:
             MusicBrainz answered and this request was never merged away
             (the #8792 Slipknot Vol. 3 refusal). Reports what Beets
             currently holds at the stored id.
        4. Require Beets to resolve EXACTLY ONE album at the survivor →
           else ``library_not_at_survivor``.
        5. Require Beets to resolve NOTHING at the stored (old) id →
           else ``library_still_at_stored`` (#1089 MAJOR-3): "exactly one
           album at the survivor" alone does not witness that the library
           MOVED — an unrelated album could already occupy the survivor
           while this request's own album still sits at the stored id, and
           rekeying would transplant the evidence lineage onto that
           unrelated album. What steps 4+5 together actually witness is
           narrower than "a clean move": Beets shows one album at the
           survivor and none at the stored id — NOT that the album at the
           survivor is this request's own album (step 5.5 below closes
           that gap for the population it can affect).
        5.5. MANDATORY (#1089 MAJOR-C, review round 3 — not conditional on
           a linked row): require the survivor album's FRESHLY computed
           content fingerprint to equal the request's linked current
           evidence's ``snapshot_fingerprint`` → else
           ``evidence_fingerprint_mismatch`` (#1089 MAJOR-3). Steps 4+5
           alone are also satisfied when this request's own album was
           deleted out of band and an unrelated, pipeline-untracked album
           happens to occupy the survivor MBID — adopting it would
           proof-lock the wrong album's bytes. No linked current evidence
           (``current_evidence_id IS NULL``) ALSO refuses here: the write
           moves EVERY evidence row at the old id regardless of which one
           (if any) is linked, so "no linked row" is not "nothing to
           transplant" — there is simply nothing to verify the adoption
           against, and the operator decides. See
           :meth:`_verify_survivor_evidence_lineage` for the full
           rationale, the live-exposure measurement, and why this is
           never a path comparison.
        6. Pre-check ``PipelineDB.merge_rekey_collision`` (#1089 MAJOR-2),
           the same read the import-validation seam takes before its own
           retag (``lib/download_validation.py``): a rival request already
           at the survivor, or a colliding evidence fingerprint, both
           persist until an operator acts → ``survivor_collision``,
           carrying the rival request id / colliding fingerprints so the UI
           can explain it. This keeps ``rekey_refused`` (below) reserved
           for genuinely transient causes only.
        7. Write the operator claim arm of
           ``update_request_release_for_merge`` (``expected_import_job_id
           =None``). ``False`` → ``rekey_refused``, diagnosed post-hoc by
           :meth:`_rekey_refused_message` (#1089 MINOR-4, review round 2):
           its MOST ORDINARY cause is a queued/running import job that was
           ALREADY there — step 2's own precondition never checks
           ``import_jobs`` at all, only the write's own ``NOT EXISTS`` term
           does — for which nothing raced and immediate retry cannot
           succeed, so the message says wait-for-drain. The remaining,
           genuinely transient causes (this request's own status/owner/
           identity changed concurrently, or — the narrow residual step 6
           cannot close, since no lock is held between the pre-check and
           the write — a rival took the survivor in that same window) keep
           the retry wording; a renewed rival collision reports
           ``survivor_collision`` again on the very next click. ``True`` →
           ``rekeyed``.
        """
        row = self.db.get_request(request_id)
        if row is None:
            return MergeRekeyResult(
                outcome=RESULT_NOT_FOUND,
                request_id=request_id,
                error_message=f"request {request_id} not found",
            )

        identity = ReleaseIdentity.from_strict_fields(
            row.get("mb_release_id"), row.get("discogs_release_id"),
        )
        status = row.get("status")
        owner = row.get("active_automation_import_job_id")
        if (
            identity is None
            or identity.source != "musicbrainz"
            or status != "imported"
            or owner is not None
        ):
            raw_stored = row.get("mb_release_id")
            return MergeRekeyResult(
                outcome=RESULT_WRONG_STATE,
                request_id=request_id,
                old_release_id=(
                    identity.release_id if identity is not None
                    else (str(raw_stored) if raw_stored is not None else None)
                ),
                error_message=(
                    f"request {request_id} is not an owner-free imported "
                    "MusicBrainz-sourced request (status="
                    f"{status!r}, active_automation_import_job_id={owner!r}, "
                    f"discogs_release_id={row.get('discogs_release_id')!r})"
                ),
            )
        old_release_id = identity.release_id
        old_identity = ReleaseIdentity(
            source="musicbrainz", release_id=old_release_id,
        )

        answer = self.canonical_release_fn(old_release_id)
        if isinstance(answer, CanonicalReleaseUnavailable):
            return MergeRekeyResult(
                outcome=RESULT_MIRROR_UNAVAILABLE,
                request_id=request_id,
                old_release_id=old_release_id,
                error_message=(
                    "MusicBrainz merge-survivor resolution did not answer "
                    f"for {old_release_id} (unconfigured, or the mirror is "
                    "unreachable)"
                ),
            )

        # ``canonical_release_status``'s own contract already guarantees a
        # ``CanonicalReleaseRedirected`` survivor is a different MusicBrainz
        # id — but this seam re-checks rather than trusting an INJECTED
        # resolver blindly (mirroring the #1059 seam's own precedent), since
        # a misbehaving test double, not production, is the only way this
        # branch is ever reached.
        survivor: str | None = None
        if isinstance(answer, CanonicalReleaseRedirected):
            survivor_identity = ReleaseIdentity.from_id(answer.survivor)
            if (
                survivor_identity is not None
                and survivor_identity.source == "musicbrainz"
                and survivor_identity.release_id != old_release_id
            ):
                survivor = survivor_identity.release_id

        if survivor is None:
            stored_resolution, failure = self._resolve_current_release(
                old_identity, request_id=request_id,
            )
            if failure is not None:
                return failure
            assert stored_resolution is not None
            return MergeRekeyResult(
                outcome=RESULT_NOT_MERGED,
                request_id=request_id,
                old_release_id=old_release_id,
                new_release_id=old_release_id,
                beets_checked_release_id=old_release_id,
                beets_album_ids=_beets_album_ids(stored_resolution),
                error_message=(
                    f"MusicBrainz names no merge survivor for "
                    f"{old_release_id}; this request has not been merged"
                ),
            )
        survivor_identity = ReleaseIdentity(
            source="musicbrainz", release_id=survivor,
        )

        survivor_resolution, failure = self._resolve_current_release(
            survivor_identity, request_id=request_id,
        )
        if failure is not None:
            return failure
        assert survivor_resolution is not None
        if not isinstance(survivor_resolution, CurrentBeetsUnique):
            return MergeRekeyResult(
                outcome=RESULT_LIBRARY_NOT_AT_SURVIVOR,
                request_id=request_id,
                old_release_id=old_release_id,
                new_release_id=survivor,
                beets_checked_release_id=survivor,
                beets_album_ids=_beets_album_ids(survivor_resolution),
                error_message=(
                    f"Beets does not resolve exactly one album at survivor "
                    f"{survivor}; the library must already hold the "
                    "survivor before the ledger can follow it"
                ),
            )

        stored_resolution, failure = self._resolve_current_release(
            old_identity, request_id=request_id,
        )
        if failure is not None:
            return failure
        assert stored_resolution is not None
        if not isinstance(stored_resolution, CurrentBeetsMissing):
            return MergeRekeyResult(
                outcome=RESULT_LIBRARY_STILL_AT_STORED,
                request_id=request_id,
                old_release_id=old_release_id,
                new_release_id=survivor,
                beets_checked_release_id=old_release_id,
                beets_album_ids=_beets_album_ids(stored_resolution),
                error_message=(
                    f"Beets still resolves an album at the merged-away id "
                    f"{old_release_id}; retag the library onto the "
                    "survivor before the ledger can follow it"
                ),
            )

        # #1089 MAJOR-C (review round 3): the witness is MANDATORY,
        # unconditional on whether a linked row exists — see
        # _verify_survivor_evidence_lineage's own docstring for why "no
        # linked evidence" is itself a refusal, not a skip.
        lineage_failure = self._verify_survivor_evidence_lineage(
            request_id=request_id,
            old_release_id=old_release_id,
            survivor=survivor,
            current_evidence_id=row.get("current_evidence_id"),
            survivor_resolution=survivor_resolution,
        )
        if lineage_failure is not None:
            return lineage_failure

        collision = self.db.merge_rekey_collision(
            request_id, old_release_id=old_release_id, new_release_id=survivor,
        )
        if collision.blocked:
            return MergeRekeyResult(
                outcome=RESULT_SURVIVOR_COLLISION,
                request_id=request_id,
                old_release_id=old_release_id,
                new_release_id=survivor,
                beets_album_id=survivor_resolution.album_id,
                rival_request_id=collision.rival_request_id,
                colliding_fingerprints=collision.colliding_fingerprints,
                error_message=(
                    f"cannot rekey request {request_id} onto {survivor}: "
                    f"{collision.detail()}"
                ),
            )

        applied = self.db.update_request_release_for_merge(
            request_id,
            old_release_id=old_release_id,
            new_release_id=survivor,
            expected_import_job_id=None,
        )
        if not applied:
            return MergeRekeyResult(
                outcome=RESULT_REKEY_REFUSED,
                request_id=request_id,
                old_release_id=old_release_id,
                new_release_id=survivor,
                beets_album_id=survivor_resolution.album_id,
                error_message=self._rekey_refused_message(request_id),
            )
        return MergeRekeyResult(
            outcome=RESULT_REKEYED,
            request_id=request_id,
            old_release_id=old_release_id,
            new_release_id=survivor,
            beets_album_id=survivor_resolution.album_id,
        )


__all__ = [
    "MERGE_REKEY_HTTP_STATUS",
    "RESULT_BEETS_UNAVAILABLE",
    "RESULT_EVIDENCE_FINGERPRINT_MISMATCH",
    "RESULT_LIBRARY_NOT_AT_SURVIVOR",
    "RESULT_LIBRARY_STILL_AT_STORED",
    "RESULT_MIRROR_UNAVAILABLE",
    "RESULT_NOT_FOUND",
    "RESULT_NOT_MERGED",
    "RESULT_REKEYED",
    "RESULT_REKEY_REFUSED",
    "RESULT_SURVIVOR_COLLISION",
    "RESULT_WRONG_STATE",
    "MergeRekeyBeetsDB",
    "MergeRekeyDB",
    "MergeRekeyResult",
    "MergeRekeyService",
]
