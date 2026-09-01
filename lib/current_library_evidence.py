"""The request's current-library (HAVE) evidence: load, plan, enrich, persist.

Beets owns current library facts; this module is the ONE place that answers
"what does the library already have for this request?" and makes a freshly
measured answer durable on the content-addressed ``AlbumQualityEvidence``
row the importer reads.

Everything here is preview-owned in the two-worker contract (preview
measures and persists evidence; the importer decides). Import and cleanup
actions only read the persisted row.

Three shapes make up the surface:

- **Predicates and plans** — ``preserve_existing_source_spectral``,
  ``current_spectral_evidence_reusable``, ``plan_current_evidence_enrichment``:
  pure questions about one already-loaded row.
- **Resolution** — ``resolve_current_library_evidence`` composes the exact
  sequence every attempt lane needs (load the linked row, re-authorize it
  against a fresh Beets resolution, project its persisted spectral fact,
  and decide reuse/preserve) into one tagged result. Both preview lanes and
  the preview worker's front-gate reuse path go through it, so the sequence
  exists once.
- **Persistence and enrichment** — ``persist_exact_current_spectral_from_attempt``
  and ``enrich_current_v0_research_for_preview`` write under their own
  exact-snapshot guards; the failure-lane orchestrators
  (``prepare_current_evidence_for_failure`` /
  ``enrich_incomplete_current_evidence_for_request``) return the typed
  outcomes below rather than bare strings, and each outcome owns whether it
  spends a caller's per-cycle enrichment budget.
"""

from __future__ import annotations

import enum
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from lib.measurement import (
    SpectralDetailAnalyzer,
    analyze_spectral_audit_path,
    diagnostic_from_stderr,
    spectral_detail_from_persisted_source,
)
from lib.quality import (
    AlbumQualityEvidence,
    AlbumQualityV0Metric,
    CodecFamily,
    QualityRankConfig,
    SpectralAnalysisDetail,
    V0ProbeEvidence,
)
from lib.quality_evidence import (
    EvidenceBuildResult,
    QualityEvidenceDB,
    audio_snapshot_matches,
    current_evidence_for_policy,
    current_evidence_preserves_source_spectral,
    current_evidence_rebuild_reasons,
    current_spectral_evidence_policy_usable,
    fingerprint_album_path,
    load_or_backfill_current_evidence,
    neutral_v0_metric_from_probe,
    spectral_measurement_generation_is_current,
)
from lib.v0_probe import probe_installed_album_as_v0

logger = logging.getLogger("cratedigger")


@runtime_checkable
class CurrentLibraryEvidenceDB(QualityEvidenceDB, Protocol):
    """The PipelineDB surface this module writes current (HAVE) facts through.

    Extends ``QualityEvidenceDB``, which already supplies the two reads every
    verb here starts from (``get_request_current_evidence_id`` and
    ``load_album_quality_evidence_by_id``). The members below are the
    current-evidence writers: one exact-snapshot spectral persist, and the
    claim/persist/release trio that makes on-disk V0 research once-only.
    """

    def persist_current_spectral_measurement(
        self,
        *,
        request_id: int,
        expected_evidence_id: int,
        expected_snapshot_fingerprint: str,
        grade: str,
        bitrate_kbps: int | None,
        cliff_hz: int | None = None,
        codec_family: CodecFamily | None = None,
        ultrasonic_deficit_db: float | None = None,
        spectral_measurement_version: int | None = None,
    ) -> bool: ...

    def claim_current_v0_research_attempt(
        self,
        *,
        request_id: int,
        expected_evidence_id: int,
        expected_snapshot_fingerprint: str,
    ) -> bool: ...

    def persist_current_v0_research_metric(
        self,
        *,
        request_id: int,
        expected_evidence_id: int,
        expected_snapshot_fingerprint: str,
        metric: AlbumQualityV0Metric,
    ) -> bool: ...

    def release_current_v0_research_attempt(
        self,
        *,
        expected_evidence_id: int,
        expected_snapshot_fingerprint: str,
    ) -> bool: ...


class HavePreparation(enum.Enum):
    """What ``prepare_current_evidence_for_failure`` resolved.

    ``charges_budget`` is the whole policy a caller needs: a failure spends
    one unit of the caller's per-cycle enrichment budget so a burst of broken
    worlds cannot balloon a cycle, while a resolved row and an authoritative
    absence both cost nothing. Callers branch on the member, never on a
    string — the tuple-membership checks this type replaced could not be
    type-checked and silently admitted tokens no producer emits.
    """

    READY = "ready"
    NO_CURRENT_EVIDENCE = "no_current_evidence"
    FAILED = "failed"

    @property
    def charges_budget(self) -> bool:
        return self == HavePreparation.FAILED


class HaveEnrichment(enum.Enum):
    """What ``enrich_incomplete_current_evidence_for_request`` resolved.

    ``ENRICHED`` and ``PARTIAL`` both spend a budget unit: between them they
    cover every lane that did work, or could not prove it did none. A
    rebuild counts as work even when nothing was left to measure
    (``ENRICHED`` with an empty plan), and an adapter or backfill failure
    before any measurement counts as ``PARTIAL`` — the same rule
    ``docs/quality-verification.md`` states. ``COMPLETE`` (nothing was
    missing), ``STALE`` (the files moved under the capture) and
    ``NO_CURRENT_EVIDENCE`` (Beets authoritatively has nothing) each return
    having neither measured nor rebuilt, and are free.
    """

    COMPLETE = "complete"
    ENRICHED = "enriched"
    PARTIAL = "partial"
    STALE = "stale"
    NO_CURRENT_EVIDENCE = "no_current_evidence"

    @property
    def charges_budget(self) -> bool:
        return self in (HaveEnrichment.ENRICHED, HaveEnrichment.PARTIAL)


@dataclass(frozen=True)
class CurrentLibraryEvidence:
    """Resolved HAVE authority for one attempt.

    ``evidence`` is ``None`` exactly when Beets authoritatively holds nothing
    for the request's release — an authoritative absence, never a failure.
    """

    evidence: AlbumQualityEvidence | None
    existing_spectral_evidence: SpectralAnalysisDetail
    reuse_have_evidence: bool
    preserve_have_source: bool


@dataclass(frozen=True)
class CurrentLibraryAuthorityUnavailable:
    """The HAVE authority could not be resolved, so the attempt must fail.

    ``detail`` is the operator-facing diagnostic each lane wraps in its own
    ``measurement_failed`` shape.
    """

    detail: str


def persist_exact_current_spectral_from_attempt(
    db: CurrentLibraryEvidenceDB,
    *,
    request_id: int,
    current_evidence: AlbumQualityEvidence | None,
    measured_existing: SpectralAnalysisDetail | None,
    measured_existing_path: str | None,
) -> EvidenceBuildResult:
    """Persist the exact attempt-time HAVE scan onto current evidence.

    ``measure_preimport_state`` independently scans the exact installed
    release before an import decision. This helper makes that successful scan
    durable on the already-linked, content-addressed current evidence row.

    A successful fresh audit of the matched-fingerprint bytes is authoritative
    (issue #815 fresh-audit-wins): it re-persists grade + bitrate over a
    disagreeing persisted installed-subject value with
    ``spectral_provenance='measured'``, so a stale legacy grade cannot survive a
    fresh scan of the same bytes. It still refuses a Beets path that is not the
    path snapshotted by the evidence row; a FAILED fresh audit never clears a
    persisted grade (fail-soft ``incomplete``); and a lossless-sourced row keeps
    its source spectral (R19) — an installed-derivative scan is never persisted
    as its grade, whatever the caller's preserve flag said.
    """
    if current_evidence is None or current_evidence.id is None:
        return EvidenceBuildResult(None, "missing", "current evidence is missing")
    if (
        measured_existing is None
        or not measured_existing.attempted
        or measured_existing.error is not None
        or measured_existing.grade in (None, "error")
    ):
        if measured_existing is None:
            reason = "attempt returned no HAVE spectral result"
        elif not measured_existing.attempted:
            reason = "attempt did not run HAVE spectral analysis"
        elif measured_existing.error is not None:
            reason = measured_existing.error
        else:
            reason = "attempt did not produce a usable HAVE spectral grade"
        return EvidenceBuildResult(
            current_evidence,
            "incomplete",
            reason,
        )
    if not spectral_measurement_generation_is_current(measured_existing):
        return EvidenceBuildResult(
            current_evidence,
            "incomplete",
            "attempt did not produce current-generation HAVE spectral evidence",
        )
    if not measured_existing_path:
        return EvidenceBuildResult(
            current_evidence,
            "stale",
            "attempt did not resolve a current Beets path",
        )
    try:
        measured_fingerprint = fingerprint_album_path(measured_existing_path)
    except OSError as exc:
        return EvidenceBuildResult(
            current_evidence,
            "failed",
            f"{type(exc).__name__}: {exc}",
        )
    if (
        measured_fingerprint != current_evidence.snapshot_fingerprint
    ):
        return EvidenceBuildResult(
            current_evidence,
            "stale",
            "attempt HAVE path does not match current evidence fingerprint",
        )
    try:
        current_id = db.get_request_current_evidence_id(request_id)
        refreshed = db.load_album_quality_evidence_by_id(current_evidence.id)
    except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
        return EvidenceBuildResult(None, "failed", f"{type(exc).__name__}: {exc}")
    if (
        current_id != current_evidence.id
        or refreshed is None
        or refreshed.id != current_evidence.id
        or refreshed.mb_release_id != current_evidence.mb_release_id
        or refreshed.snapshot_fingerprint != current_evidence.snapshot_fingerprint
        or not audio_snapshot_matches(measured_existing_path, refreshed.files)
    ):
        return EvidenceBuildResult(
            current_evidence,
            "stale",
            "current evidence changed before HAVE spectral persistence",
        )
    # R19 belt-and-braces: only the exact known-lossy derivative keeps its
    # source spectral (or stays empty until it is carried in) — an
    # attempt-time scan of that installed derivative must never be persisted
    # as its grade, whatever the caller's preserve flag said.
    if preserve_existing_source_spectral(refreshed):
        return EvidenceBuildResult(
            refreshed,
            "skipped",
            "lossless-sourced copy keeps its source spectral (R19)",
        )
    # Fresh-audit-wins (issue #815): a successful fresh audit of the
    # matched-fingerprint bytes re-persists over ANY disagreeing persisted
    # installed-subject grade. The fill-only-if-NULL early return that used to
    # sit here silently discarded a fresh genuine/160 audit and let a stale
    # likely_transcode/128 landmine drive a real library downgrade.
    try:
        persisted = db.persist_current_spectral_measurement(
            request_id=request_id,
            expected_evidence_id=current_evidence.id,
            expected_snapshot_fingerprint=current_evidence.snapshot_fingerprint,
            grade=measured_existing.grade,
            bitrate_kbps=measured_existing.bitrate_kbps,
            cliff_hz=measured_existing.cliff_hz,
            codec_family=measured_existing.codec_family,
            ultrasonic_deficit_db=measured_existing.ultrasonic_deficit_db,
            spectral_measurement_version=measured_existing.spectral_measurement_version,
        )
        loaded = db.load_album_quality_evidence_by_id(current_evidence.id)
        linked_id = db.get_request_current_evidence_id(request_id)
    except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
        return EvidenceBuildResult(None, "failed", f"{type(exc).__name__}: {exc}")
    if (
        linked_id != current_evidence.id
        or loaded is None
        or loaded.id != current_evidence.id
        or loaded.snapshot_fingerprint != current_evidence.snapshot_fingerprint
    ):
        return EvidenceBuildResult(
            current_evidence,
            "stale",
            "current evidence changed during HAVE spectral persistence",
        )
    loaded_measurement = loaded.measurement
    if not persisted and (
        loaded_measurement.spectral_grade is None
        and loaded_measurement.spectral_bitrate_kbps is None
    ):
        return EvidenceBuildResult(
            loaded,
            "stale",
            "exact current evidence rejected HAVE spectral persistence",
        )
    return EvidenceBuildResult(loaded, "ready")


def load_linked_current_evidence(
    db: CurrentLibraryEvidenceDB,
    request_id: int,
) -> AlbumQualityEvidence | None:
    """Load the evidence row this request's current-library link names.

    ``None`` covers four worlds on purpose, and the sole caller wants the same
    thing from all four. There may be no link; the link may name a row that is
    gone; either read may raise. In every case the resolver hands ``None`` to
    its loader as ``preloaded_evidence``, which then resolves Beets freshly and
    backfills — so an unreadable link re-derives from the installed files
    instead of proceeding on a row nothing could read.

    Until issue #1313 this returned a 3-tuple, and two thirds of it were dead.
    The second element projected the row's spectral detail; the resolver
    reassigned that name in every one of its three branches before reading it.
    The third said whether a link had existed at all, and the resolver
    discarded it into an underscore. Only two tests ever read either, which is
    how both survived.
    """
    try:
        evidence_id = db.get_request_current_evidence_id(request_id)
    except Exception:
        logger.warning(
            "Unable to resolve current spectral evidence for request %s",
            request_id,
            exc_info=True,
        )
        return None
    if evidence_id is None:
        return None
    try:
        current_evidence = db.load_album_quality_evidence_by_id(evidence_id)
    except Exception:
        logger.warning(
            "Unable to load current spectral evidence %s for request %s",
            evidence_id,
            request_id,
            exc_info=True,
        )
        return None
    if current_evidence is None:
        logger.warning(
            "Current spectral evidence %s is missing for request %s",
            evidence_id,
            request_id,
        )
    return current_evidence


def enrich_current_v0_research_for_preview(
    db: CurrentLibraryEvidenceDB,
    *,
    request_id: int,
    expected_evidence_id: int,
    expected_snapshot_fingerprint: str,
    current_album_path: str,
    probe_fn: Callable[[str], V0ProbeEvidence | None] = (
        probe_installed_album_as_v0
    ),
) -> EvidenceBuildResult:
    """Research missing HAVE V0 once for one exact current snapshot.

    This is deliberately preview-owned. Import and cleanup actions only read
    the persisted row. The exact current FK, evidence id, fingerprint, and
    on-disk audio snapshot must all still agree before the probe can run, so
    deploy orchestration can safely invoke the helper for a known historical
    row without introducing a one-shot script or a proximity-based lookup.

    A probe exception or ``None`` result is still persisted as an attempted
    research fact. Since the marker lives on the content-addressed evidence
    row, an unchanged snapshot is never re-encoded while a changed snapshot
    naturally receives a fresh row and another opportunity.
    """

    try:
        current_id = db.get_request_current_evidence_id(request_id)
        if current_id != expected_evidence_id:
            return EvidenceBuildResult(
                None,
                "stale",
                "request current evidence no longer matches expected id",
            )
        evidence = db.load_album_quality_evidence_by_id(expected_evidence_id)
    except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
        return EvidenceBuildResult(
            None,
            "failed",
            f"{type(exc).__name__}: {exc}",
        )
    if evidence is None:
        return EvidenceBuildResult(None, "missing", "current evidence is missing")
    if evidence.snapshot_fingerprint != expected_snapshot_fingerprint:
        return EvidenceBuildResult(
            None,
            "stale",
            "current evidence fingerprint no longer matches expected snapshot",
        )
    if not audio_snapshot_matches(current_album_path, evidence.files):
        return EvidenceBuildResult(
            None,
            "stale",
            "current album files changed since evidence capture",
        )
    if evidence.id != expected_evidence_id:
        return EvidenceBuildResult(
            None,
            "stale",
            "loaded evidence identity no longer matches expected id",
        )
    if evidence.v0_metric is not None or evidence.on_disk_v0_research_attempted:
        return EvidenceBuildResult(evidence, "ready")

    try:
        claimed = db.claim_current_v0_research_attempt(
            request_id=request_id,
            expected_evidence_id=expected_evidence_id,
            expected_snapshot_fingerprint=expected_snapshot_fingerprint,
        )
    except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
        return EvidenceBuildResult(
            None,
            "failed",
            f"{type(exc).__name__}: {exc}",
        )
    if not claimed:
        # Another preview may have won the claim after our initial read. Its
        # committed marker is enough to make this caller once-only; reload the
        # exact row so callers see the claimed state without probing again.
        try:
            current_id = db.get_request_current_evidence_id(request_id)
            claimed_evidence = db.load_album_quality_evidence_by_id(
                expected_evidence_id
            )
        except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
            return EvidenceBuildResult(
                None,
                "failed",
                f"{type(exc).__name__}: {exc}",
            )
        if current_id != expected_evidence_id:
            return EvidenceBuildResult(
                None,
                "stale",
                "request current evidence changed before V0 research claim",
            )
        if (
            claimed_evidence is None
            or claimed_evidence.id != expected_evidence_id
            or claimed_evidence.mb_release_id != evidence.mb_release_id
            or claimed_evidence.snapshot_fingerprint
                != expected_snapshot_fingerprint
        ):
            return EvidenceBuildResult(
                None,
                "stale",
                "evidence identity changed before V0 research claim",
            )
        if (
            claimed_evidence.v0_metric is not None
            or claimed_evidence.on_disk_v0_research_attempted
        ):
            return EvidenceBuildResult(claimed_evidence, "ready")
        return EvidenceBuildResult(
            None,
            "failed",
            "current evidence V0 research claim was not acquired",
        )

    metric = None
    try:
        metric = neutral_v0_metric_from_probe(probe_fn(current_album_path))
    except Exception:
        logger.warning(
            "Current on-disk V0 research probe failed for %s",
            current_album_path,
            exc_info=True,
        )

    # ffmpeg may run for long enough that the request link or album bytes can
    # change underneath it. Recheck every authority component after the probe
    # and before writing a metric. A live stale caller releases its marker;
    # only a process crash intentionally leaves the once-only claim behind.
    try:
        current_id = db.get_request_current_evidence_id(request_id)
        refreshed = db.load_album_quality_evidence_by_id(expected_evidence_id)
        fresh = (
            current_id == expected_evidence_id
            and refreshed is not None
            and refreshed.id == expected_evidence_id
            and refreshed.mb_release_id == evidence.mb_release_id
            and refreshed.snapshot_fingerprint == expected_snapshot_fingerprint
            and audio_snapshot_matches(current_album_path, refreshed.files)
        )
    except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
        try:
            db.release_current_v0_research_attempt(
                expected_evidence_id=expected_evidence_id,
                expected_snapshot_fingerprint=expected_snapshot_fingerprint,
            )
        except Exception:
            logger.warning(
                "Unable to release unverifiable V0 research claim %s",
                expected_evidence_id,
                exc_info=True,
            )
        return EvidenceBuildResult(
            None,
            "failed",
            f"{type(exc).__name__}: {exc}",
        )
    if not fresh:
        try:
            db.release_current_v0_research_attempt(
                expected_evidence_id=expected_evidence_id,
                expected_snapshot_fingerprint=expected_snapshot_fingerprint,
            )
        except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
            return EvidenceBuildResult(
                None,
                "failed",
                f"{type(exc).__name__}: {exc}",
            )
        return EvidenceBuildResult(
            None,
            "stale",
            "current evidence changed while V0 research probe was running",
        )

    if metric is not None:
        try:
            persisted_metric = db.persist_current_v0_research_metric(
                request_id=request_id,
                expected_evidence_id=expected_evidence_id,
                expected_snapshot_fingerprint=expected_snapshot_fingerprint,
                metric=metric,
            )
        except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
            return EvidenceBuildResult(
                None,
                "failed",
                f"{type(exc).__name__}: {exc}",
            )
        if not persisted_metric:
            try:
                db.release_current_v0_research_attempt(
                    expected_evidence_id=expected_evidence_id,
                    expected_snapshot_fingerprint=expected_snapshot_fingerprint,
                )
            except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
                return EvidenceBuildResult(
                    None,
                    "failed",
                    f"{type(exc).__name__}: {exc}",
                )
            return EvidenceBuildResult(
                None,
                "stale",
                "current evidence changed before V0 research persistence",
            )

    try:
        persisted = db.load_album_quality_evidence_by_id(expected_evidence_id)
    except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
        return EvidenceBuildResult(
            None,
            "failed",
            f"{type(exc).__name__}: {exc}",
        )
    if (
        persisted is None
        or persisted.id != expected_evidence_id
        or persisted.snapshot_fingerprint != expected_snapshot_fingerprint
        or not persisted.on_disk_v0_research_attempted
    ):
        return EvidenceBuildResult(
            None,
            "failed",
            "claimed current evidence did not preserve the expected identity",
        )
    return EvidenceBuildResult(persisted, "ready")


@dataclass(frozen=True)
class EnrichmentPlan:
    """Which measurements a current-evidence row is missing."""

    spectral: bool
    v0: bool

    @property
    def any(self) -> bool:
        return self.spectral or self.v0


def plan_current_evidence_enrichment(
    evidence: AlbumQualityEvidence,
) -> EnrichmentPlan:
    """Pure decision: measure exactly the missing HAVE pieces.

    Generation freshness is distinct from policy usability: ordinary current
    bytes need the running generation, while a preserved source subject is
    intentionally never regenerated from its lossy installed derivative.
    The policy projection separately withholds error, blank, and unknown
    grades. A V0 metric or the attempted marker means the research probe
    already ran. Complete rows therefore cost nothing to re-plan.
    """
    measurement = evidence.measurement
    preserve_source = current_evidence_preserves_source_spectral(evidence)
    return EnrichmentPlan(
        spectral=(
            not preserve_source
            and (
                (
                    measurement.spectral_grade is None
                    and measurement.spectral_bitrate_kbps is None
                )
                or not spectral_measurement_generation_is_current(measurement)
            )
        ),
        v0=(
            evidence.v0_metric is None
            and not evidence.on_disk_v0_research_attempted
        ),
    )


def current_spectral_evidence_reusable(
    evidence: AlbumQualityEvidence,
) -> bool:
    """Whether an authorized HAVE row has a decision-usable spectral fact.

    The enrichment planner records whether analysis already ran, so an
    attempted-but-failed ``"error"`` grade is intentionally complete for that
    once-only bookkeeping.  Reuse delegates to the one policy-usability rule:
    ordinary installed evidence needs this analyzer generation, while a
    recognized, irreplaceable carried source grade is reused without scanning
    the installed lossy derivative.
    """
    return current_spectral_evidence_policy_usable(evidence)


def prepare_current_evidence_for_failure(
    db: CurrentLibraryEvidenceDB,
    *,
    request_id: int,
    mb_release_id: str,
    quality_ranks: QualityRankConfig,
    beets_library_root: str,
    load_fn: Callable[..., EvidenceBuildResult] = load_or_backfill_current_evidence,
) -> HavePreparation:
    """Freshly resolve and link usable HAVE before failure logging.

    Returns ``READY`` only when the request FK resolves to the surviving
    evidence row, ``NO_CURRENT_EVIDENCE`` only when Beets authoritatively says
    the exact release is absent, and ``FAILED`` for adapter, snapshot, or
    persistence failures. Even a linked row is re-authorized against a fresh
    exact Beets resolution and current fingerprint before it can be reused.
    """
    try:
        current_id = db.get_request_current_evidence_id(request_id)
        current = (
            db.load_album_quality_evidence_by_id(current_id)
            if current_id is not None
            else None
        )
    except Exception:
        logger.warning(
            "Could not resolve current evidence for request %s",
            request_id,
            exc_info=True,
        )
        return HavePreparation.FAILED
    try:
        result = load_fn(
            db,
            request_id=request_id,
            mb_release_id=mb_release_id,
            quality_ranks=quality_ranks,
            preloaded_evidence=current,
            preloaded=current is not None,
            beets_library_root=beets_library_root,
        )
    except Exception:
        logger.warning(
            "Could not load/backfill current evidence for request %s",
            request_id,
            exc_info=True,
        )
        return HavePreparation.FAILED
    if result.status == "empty_current":
        return HavePreparation.NO_CURRENT_EVIDENCE
    if result.status != "ready" or result.evidence is None:
        logger.warning(
            "Could not prepare current evidence for request %s: %s%s",
            request_id,
            result.status,
            f" ({result.reason})" if result.reason else "",
        )
        return HavePreparation.FAILED
    try:
        current_id = db.get_request_current_evidence_id(request_id)
        evidence = (
            db.load_album_quality_evidence_by_id(current_id)
            if current_id is not None
            else None
        )
    except Exception:
        logger.warning(
            "Could not resolve prepared current evidence for request %s",
            request_id,
            exc_info=True,
        )
        return HavePreparation.FAILED
    if evidence is None or evidence.id is None:
        logger.warning(
            "Prepared current evidence was not linked for request %s",
            request_id,
        )
        return HavePreparation.FAILED
    if (
        (
            result.evidence.id is not None
            and evidence.id != result.evidence.id
        )
        or evidence.mb_release_id != result.evidence.mb_release_id
        or evidence.snapshot_fingerprint
            != result.evidence.snapshot_fingerprint
    ):
        logger.warning(
            "Prepared current evidence link changed for request %s",
            request_id,
        )
        return HavePreparation.FAILED
    return HavePreparation.READY


def enrich_incomplete_current_evidence_for_request(
    db: CurrentLibraryEvidenceDB,
    *,
    request_id: int,
    mb_release_id: str,
    quality_ranks: QualityRankConfig,
    beets_library_root: str,
    beets_library_db_path: str | None = None,
    spectral_analyzer: SpectralDetailAnalyzer = analyze_spectral_audit_path,
    probe_fn: Callable[[str], V0ProbeEvidence | None] = (
        probe_installed_album_as_v0
    ),
    load_fn: Callable[..., EvidenceBuildResult] = load_or_backfill_current_evidence,
) -> HaveEnrichment:
    """Opportunistically complete a request's HAVE evidence in place.

    Driven from the download-failure path after its canonical HAVE snapshot
    has been prepared and failure bookkeeping has completed. It repeats the
    exact current-Beets resolution before measuring any remaining enrichment.
    All writes go through the preview-owned helpers, so the once-only,
    exact-snapshot, and never-overwrite guards hold unchanged.

    Returns ``NO_CURRENT_EVIDENCE`` (nothing linked), ``STALE`` (files changed
    since capture), ``COMPLETE`` (nothing missing — zero cost), ``ENRICHED``
    (a rebuild or every missing piece resolved), or ``PARTIAL`` (work ran but
    something is still unresolved).
    """
    try:
        current_id = db.get_request_current_evidence_id(request_id)
        initial_evidence = (
            db.load_album_quality_evidence_by_id(current_id)
            if current_id is not None
            else None
        )
    except Exception:
        logger.warning(
            "Could not load current evidence for request %s",
            request_id,
            exc_info=True,
        )
        return HaveEnrichment.PARTIAL
    try:
        result = load_fn(
            db,
            request_id=request_id,
            mb_release_id=mb_release_id,
            quality_ranks=quality_ranks,
            preloaded_evidence=initial_evidence,
            preloaded=initial_evidence is not None,
            beets_library_db_path=beets_library_db_path,
            beets_library_root=beets_library_root,
        )
    except Exception:
        logger.warning(
            "Could not resolve current evidence for request %s",
            request_id,
            exc_info=True,
        )
        return HaveEnrichment.PARTIAL
    if result.status == "empty_current":
        return HaveEnrichment.NO_CURRENT_EVIDENCE
    if (
        result.status != "ready"
        or result.evidence is None
        or result.evidence.id is None
        or result.current_album_path is None
    ):
        logger.warning(
            "Could not authorize current evidence for request %s: %s%s",
            request_id,
            result.status,
            f" ({result.reason})" if result.reason else "",
        )
        return HaveEnrichment.PARTIAL
    evidence = result.evidence
    assert evidence.id is not None
    current_album_path = result.current_album_path
    rebuilt = (
        initial_evidence is None
        or bool(current_evidence_rebuild_reasons(initial_evidence))
        or initial_evidence.id != evidence.id
        or initial_evidence.snapshot_fingerprint != evidence.snapshot_fingerprint
    )
    plan = plan_current_evidence_enrichment(evidence)
    if not plan.any:
        return HaveEnrichment.ENRICHED if rebuilt else HaveEnrichment.COMPLETE
    # Cheap freshness pre-check before any expensive measurement; the
    # persist/claim helpers each re-verify under their own authority.
    if not audio_snapshot_matches(current_album_path, evidence.files):
        return HaveEnrichment.STALE
    all_ok = True
    if plan.spectral:
        detail = spectral_analyzer(current_album_path)
        spectral_result = persist_exact_current_spectral_from_attempt(
            db,
            request_id=request_id,
            current_evidence=evidence,
            measured_existing=detail,
            measured_existing_path=current_album_path,
        )
        all_ok = all_ok and spectral_result.status == "ready"
    if plan.v0:
        v0_result = enrich_current_v0_research_for_preview(
            db,
            request_id=request_id,
            expected_evidence_id=evidence.id,
            expected_snapshot_fingerprint=evidence.snapshot_fingerprint,
            current_album_path=current_album_path,
            probe_fn=probe_fn,
        )
        all_ok = all_ok and v0_result.status == "ready"
    return HaveEnrichment.ENRICHED if all_ok else HaveEnrichment.PARTIAL


def authorize_current_evidence_for_preview(
    db: CurrentLibraryEvidenceDB,
    *,
    request_id: int,
    mb_release_id: str,
    quality_ranks: QualityRankConfig,
    beets_library_root: str,
    preloaded_evidence: AlbumQualityEvidence | None,
) -> EvidenceBuildResult:
    """Resolve and re-link the fresh exact Beets snapshot for preview use."""

    try:
        load_result = load_or_backfill_current_evidence(
            db,
            request_id=request_id,
            mb_release_id=mb_release_id,
            quality_ranks=quality_ranks,
            preloaded_evidence=preloaded_evidence,
            preloaded=preloaded_evidence is not None,
            beets_library_root=beets_library_root,
        )
    except Exception as exc:
        logger.warning(
            "Unable to load/backfill preview HAVE evidence for request %s",
            request_id,
            exc_info=True,
        )
        return EvidenceBuildResult(
            None,
            "failed",
            "current evidence preparation failed: "
            f"{type(exc).__name__}: {diagnostic_from_stderr(str(exc))}",
        )
    if load_result.status != "ready" or load_result.evidence is None:
        return load_result
    current = load_result.evidence
    current_album_path = load_result.current_album_path
    if current_album_path is None:
        logger.warning(
            "Current Beets path was not returned for request %s",
            request_id,
        )
        return EvidenceBuildResult(
            None,
            "failed",
            "current Beets path was not returned",
        )

    # Backfill returns its pre-upsert value; reload through the exact request
    # FK so the public enrichment helper always receives the surviving id.
    try:
        evidence_id = db.get_request_current_evidence_id(request_id)
        linked = (
            db.load_album_quality_evidence_by_id(evidence_id)
            if evidence_id is not None
            else None
        )
        if (
            linked is None
            or (
                current.id is not None
                and linked.id != current.id
            )
            or linked.mb_release_id != current.mb_release_id
            or linked.snapshot_fingerprint != current.snapshot_fingerprint
        ):
            logger.warning(
                "Preview current evidence link changed for request %s",
                request_id,
            )
            return EvidenceBuildResult(
                None,
                "stale",
                "preview current evidence link changed",
            )
        current = linked
    except Exception as exc:
        logger.warning(
            "Unable to resolve preview HAVE evidence for request %s",
            request_id,
            exc_info=True,
        )
        return EvidenceBuildResult(
            None,
            "failed",
            f"{type(exc).__name__}: {exc}",
        )

    return EvidenceBuildResult(
        current_evidence_for_policy(current),
        "ready",
        current_album_path=current_album_path,
    )


def load_current_evidence_for_preview(
    db: CurrentLibraryEvidenceDB,
    *,
    request_id: int,
    mb_release_id: str,
    quality_ranks: QualityRankConfig,
    beets_library_root: str,
    preloaded_evidence: AlbumQualityEvidence | None,
    enrich_current_fn: Callable[..., EvidenceBuildResult] | None = None,
) -> EvidenceBuildResult:
    """Load/backfill HAVE and perform preview-owned neutral enrichment."""

    authorized = authorize_current_evidence_for_preview(
        db,
        request_id=request_id,
        mb_release_id=mb_release_id,
        quality_ranks=quality_ranks,
        beets_library_root=beets_library_root,
        preloaded_evidence=preloaded_evidence,
    )
    if authorized.status != "ready" or authorized.evidence is None:
        logger.warning(
            "Unable to authorize preview HAVE evidence for request %s: %s%s",
            request_id,
            authorized.status,
            f" ({authorized.reason})" if authorized.reason else "",
        )
        return authorized
    current = authorized.evidence
    current_album_path = authorized.current_album_path
    assert current_album_path is not None

    if current.id is None:
        return authorized
    enrich = enrich_current_fn or enrich_current_v0_research_for_preview
    enriched = enrich(
        db,
        request_id=request_id,
        expected_evidence_id=current.id,
        expected_snapshot_fingerprint=current.snapshot_fingerprint,
        current_album_path=current_album_path,
    )
    if enriched.status != "ready" or enriched.evidence is None:
        logger.warning(
            "Preview HAVE enrichment lost authority for request %s: %s%s",
            request_id,
            enriched.status,
            f" ({enriched.reason})" if enriched.reason else "",
        )
        return enriched
    return EvidenceBuildResult(
        current_evidence_for_policy(enriched.evidence),
        enriched.status,
        enriched.reason,
        current_album_path=current_album_path,
    )


def preserve_existing_source_spectral(
    current_evidence: AlbumQualityEvidence | None,
) -> bool:
    """Whether HAVE must retain lossless-source pre-conversion evidence.

    R19: a recorded lossless conversion into a known lossy installed codec
    wears its SOURCE's spectral; scanning that derivative can rewrite a
    transcode-like source as apparently genuine (fullband codecs like Opus
    always scan clean). Source V0/proof records are provenance only. The
    predicate fails closed for native lossless, mixed, or unresolved files —
    in particular, an .m4a container is not evidence of AAC over ALAC.
    """
    return (
        current_evidence is not None
        and current_evidence_preserves_source_spectral(current_evidence)
    )


def resolve_current_library_evidence(
    db: CurrentLibraryEvidenceDB,
    *,
    request_id: int,
    mb_release_id: str,
    quality_ranks: QualityRankConfig,
    beets_library_root: str,
    loader: Callable[..., EvidenceBuildResult] = load_current_evidence_for_preview,
) -> CurrentLibraryEvidence | CurrentLibraryAuthorityUnavailable:
    """Resolve one attempt's exact-current (HAVE) authority, once.

    This is the sequence — load the linked row, re-authorize it through
    ``loader`` against a fresh exact Beets resolution, project the surviving
    row's persisted spectral fact, and decide reuse/preserve — that every
    attempt lane needs before it can measure or reuse candidate evidence.
    It existed twice before issue #1313, reached from three call sites:
    ``lib/import_preview.py::_resolve_lane_current_evidence``, shared by both
    preview lanes, and — copy-pasted down to this function's own
    unavailable-detail string — the preview worker's front-gate reuse path.

    ``loader`` is the lanes' only difference. The measure-and-persist lane
    and the worker's reuse path pass ``load_current_evidence_for_preview``
    (authorize plus preview-owned V0 research enrichment); the classify lane
    (CLI inspector, wrong-match triage UI) passes
    ``authorize_current_evidence_for_preview`` — a synchronous operator
    surface takes no enrichment work.

    The three outcomes are deliberately distinct. A resolved row returns
    ``CurrentLibraryEvidence`` with ``evidence`` set. An ``empty_current``
    loader status returns ``CurrentLibraryEvidence`` with ``evidence=None``:
    Beets authoritatively holds nothing, so stale linked HAVE facts describe
    no current bytes and must not influence candidate measurement or the
    decision inputs — an absence, never a failure. Anything else is a real
    authority failure and returns ``CurrentLibraryAuthorityUnavailable``, so
    a lane can never proceed on a stale or unreadable HAVE row.
    """
    current_evidence = load_linked_current_evidence(db, request_id)
    existing_spectral_evidence = SpectralAnalysisDetail(attempted=False)
    reuse_have_evidence = False
    current_result = loader(
        db,
        request_id=request_id,
        mb_release_id=mb_release_id,
        quality_ranks=quality_ranks,
        beets_library_root=beets_library_root,
        preloaded_evidence=current_evidence,
    )
    if current_result.status == "empty_current":
        # Authoritative absence: stale linked HAVE facts describe no current
        # bytes and cannot influence candidate measurement or decision inputs.
        current_evidence = None
    elif current_result.status != "ready" or current_result.evidence is None:
        return CurrentLibraryAuthorityUnavailable(
            f"{current_result.status}: "
            f"{current_result.reason or 'current authority unavailable'}"
        )
    else:
        current_evidence = current_result.evidence
        current_m = current_evidence.measurement
        existing_spectral_evidence = spectral_detail_from_persisted_source(
            current_m.spectral_grade,
            current_m.spectral_bitrate_kbps,
            cliff_hz=current_m.cliff_hz,
            codec_family=current_m.codec_family,
            ultrasonic_deficit_db=current_m.ultrasonic_deficit_db,
            spectral_measurement_version=(
                current_m.spectral_measurement_version
            ),
        )
        reuse_have_evidence = current_spectral_evidence_reusable(
            current_evidence,
        )
    return CurrentLibraryEvidence(
        evidence=current_evidence,
        existing_spectral_evidence=existing_spectral_evidence,
        reuse_have_evidence=reuse_have_evidence,
        preserve_have_source=preserve_existing_source_spectral(current_evidence),
    )


def persist_measured_have_spectral(
    db: CurrentLibraryEvidenceDB,
    *,
    request_id: int,
    resolved: CurrentLibraryEvidence,
    measured_existing: SpectralAnalysisDetail | None,
    measured_existing_path: str | None,
) -> EvidenceBuildResult | None:
    """Make a freshly scanned HAVE spectral fact durable, or decline to.

    **A newly measured HAVE fact must become durable BEFORE the importer can
    decide.** An audit-only scan left the decision spectrally blind
    (download_log 37206): the preview reported the fact in its audit payload,
    marked the job importable, and the importer then read a current-evidence
    row that still carried no spectral grade. The preview worker's front-gate
    reuse path measures HAVE and then marks the job importable, so it owes
    this call in between.

    This is the WORKER lane's guard, not a universal one.
    ``lib/import_preview.py``'s measure-and-persist lane reaches
    ``persist_exact_current_spectral_from_attempt`` directly under its own
    condition (``not reuse_have_evidence``) — a predicate this one cannot
    express, since a reusing lane never produces a fresh scan to persist at
    all. What both lanes share is that persister's own guards, which are the
    real floor: R19, exact path, exact snapshot.

    Returns ``None`` when there is nothing to persist — no linked HAVE row,
    a preserved lossless-source row (R19: an installed-derivative scan is
    never that row's grade), or no resolved installed path. Otherwise the
    persist helper's own exact-path and exact-snapshot guards decide, and its
    result is returned for callers that want the surviving row. Collaborator
    exceptions propagate: the caller owns whether a failed HAVE write is
    fail-soft for its lane.
    """
    if (
        resolved.evidence is None
        or resolved.preserve_have_source
        or not measured_existing_path
    ):
        return None
    return persist_exact_current_spectral_from_attempt(
        db,
        request_id=request_id,
        current_evidence=resolved.evidence,
        measured_existing=measured_existing,
        measured_existing_path=measured_existing_path,
    )
