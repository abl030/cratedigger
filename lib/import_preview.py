"""Unified import preview service.

Preview answers the operator's "would this import?" question without beets,
pipeline DB, queue, denylist, or source-folder mutation. Real-folder preview
uses the same preimport gates and import_one.py harness protocol as force-import,
but runs both against isolated temporary copies.
"""

from __future__ import annotations

import logging
import os
import shutil
import stat
import tempfile
import fcntl
from collections.abc import Mapping, Callable
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Protocol, TYPE_CHECKING, runtime_checkable

import msgspec

if TYPE_CHECKING:
    from lib.pipeline_db.rows import DownloadLogWithEvidenceRow

from lib.dispatch import run_import_one
from lib.dispatch.types import ImportOneRun
from lib.measurement import (
    ExistingSpectralResolver,
    PreimportMeasurement,
    SpectralDetailAnalyzer,
    analyze_spectral_audit_path,
    inspect_local_files,
    measure_preimport_state,
    spectral_detail_from_persisted_source,
)
from lib.quality_evidence import (
    EvidenceBuildResult,
    QualityEvidenceDB,
    audio_snapshot_matches,
    current_evidence_rebuild_reasons,
    load_or_backfill_current_evidence,
    audit_v0_probe_from_metric,
    evidence_from_measurement,
    neutral_v0_metric_from_probe,
    persist_candidate_evidence_from_import_result,
    persist_candidate_evidence_from_measurement,
    snapshot_audio_files,
    snapshot_fingerprint,
)
from lib.quality import (
    LOSSLESS_CODECS,
    EVIDENCE_SUBJECT_SOURCE,
    AlbumQualityEvidence,
    AlbumQualityEvidenceFile,
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    ImportResult,
    MeasurementFailure,
    MeasurementFailureReason,
    QUALITY_DECISION_IMPORT_STAGE_DECISIONS,
    QualityRankConfig,
    QualityEvidenceActionPayload,
    QualityEvidenceActionProvenance,
    SpectralAnalysisDetail,
    SpectralDetail,
    TargetQualityContract,
    V0ProbeEvidence,
    classify_full_pipeline_decision,
    classify_quality_import_stages,
    compute_effective_override_bitrate,
    full_pipeline_decision,
    quality_gate_decision,
)
from lib.fs_authority import (
    FilesystemAuthorityError,
    copy_opened_file,
    open_directory_path,
    open_private_child_directory,
    open_private_processing_root,
    open_regular_relative,
)
from lib.processing_paths import path_is_within_root, processing_albums_dir, processing_preview_dir
from lib.util import repair_mp3_headers
from lib.validation_envelope import decode_validation_envelope
from lib.v0_probe import probe_installed_album_as_v0

logger = logging.getLogger("cratedigger")

_PREVIEW_MAX_DEPTH = 32
_PREVIEW_MAX_FILES = 5000
_PREVIEW_MAX_BYTES = 100 * 1024**3


def _authorized_failed_root(raw_path: str, cfg: Any) -> str:
    """Resolve a tainted DB failed_path through configured quarantine roots."""
    roots = (
        os.path.join(cfg.slskd_download_dir, "failed_imports"),
        os.path.join(cfg.slskd_download_dir, "wrong_matches"),
        os.path.join(cfg.beets_staging_dir, "failed_imports"),
        os.path.join(processing_albums_dir(cfg.processing_dir), "failed_imports"),
        os.path.join(processing_albums_dir(cfg.processing_dir), "wrong_matches"),
    )
    candidates = [raw_path] if os.path.isabs(raw_path) else [
        os.path.join(root, raw_path) for root in roots
    ]
    for candidate in candidates:
        if not any(path_is_within_root(candidate, root) for root in roots):
            continue
        try:
            with open_directory_path(candidate):
                pass
        except FilesystemAuthorityError:
            continue
        return os.path.abspath(candidate)
    raise FilesystemAuthorityError("failed_path is outside configured quarantine roots")


@contextmanager
def _preview_copy_lock(cfg: Any) -> Any:
    """Serialize bounded source snapshots before they consume private disk.

    The lock intentionally covers only the untrusted-tree copy and its
    free-space admission check.  Measurement and the harness run after it is
    released, so one slow preview cannot serialize all operator work.
    """
    with open_private_processing_root(
        cfg.processing_dir, cfg.slskd_download_dir,
    ) as processing_fd:
        with open_private_child_directory(processing_fd, "preview"):
            lock_path = os.path.join(processing_preview_dir(cfg.processing_dir), ".snapshot.lock")
            lock_fd = os.open(
                lock_path,
                os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW,
                0o600,
            )
            try:
                lock_info = os.fstat(lock_fd)
                if not stat.S_ISREG(lock_info.st_mode):
                    raise FilesystemAuthorityError("preview snapshot lock is not regular")
                fcntl.flock(lock_fd, fcntl.LOCK_EX)
                yield
            finally:
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_UN)
                finally:
                    os.close(lock_fd)


def _snapshot_authorized_directory(path: str, cfg: Any) -> str:
    """No-follow bounded copy of an authorized source into private preview."""
    snapshot: str | None = None
    files = 0
    copied_bytes = 0
    try:
        with _preview_copy_lock(cfg):
            preview_parent = processing_preview_dir(cfg.processing_dir)
            snapshot = tempfile.mkdtemp(prefix="preview-", dir=preview_parent)
            with open_directory_path(path) as root_fd:
                # The maximum is an admission cap, not a promise that the
                # filesystem can supply it.  The private-copy lock makes this
                # free-space check meaningful across concurrent snapshots.
                available = os.statvfs(preview_parent).f_bavail * os.statvfs(preview_parent).f_frsize
                stack: list[tuple[int, str, int]] = [(os.dup(root_fd), snapshot, 0)]
                try:
                    while stack:
                        source_dir_fd, destination_dir, depth = stack.pop()
                        try:
                            entries = sorted(list(os.scandir(source_dir_fd)), key=lambda entry: entry.name)
                            for entry in entries:
                                if entry.is_symlink():
                                    raise FilesystemAuthorityError("snapshot contains symlink")
                                info = entry.stat(follow_symlinks=False)
                                destination = os.path.join(destination_dir, entry.name)
                                if stat.S_ISDIR(info.st_mode):
                                    if depth >= _PREVIEW_MAX_DEPTH:
                                        raise FilesystemAuthorityError("preview depth limit exceeded")
                                    os.mkdir(destination, 0o700)
                                    child_fd = os.open(
                                        entry.name,
                                        os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
                                        dir_fd=source_dir_fd,
                                    )
                                    stack.append((child_fd, destination, depth + 1))
                                    continue
                                if not stat.S_ISREG(info.st_mode):
                                    raise FilesystemAuthorityError("snapshot contains non-regular file")
                                files += 1
                                copied_bytes += info.st_size
                                if files > _PREVIEW_MAX_FILES or copied_bytes > _PREVIEW_MAX_BYTES:
                                    raise FilesystemAuthorityError("preview snapshot limit exceeded")
                                if copied_bytes > available:
                                    raise FilesystemAuthorityError("insufficient private preview space")
                                opened = open_regular_relative(source_dir_fd, entry.name)
                                try:
                                    destination_fd = os.open(
                                        destination,
                                        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
                                        0o600,
                                    )
                                    try:
                                        copy_opened_file(
                                            opened.fd,
                                            destination_fd,
                                            max_bytes=opened.stat_result.st_size,
                                        )
                                    finally:
                                        os.close(destination_fd)
                                finally:
                                    opened.close()
                        finally:
                            os.close(source_dir_fd)
                finally:
                    for pending_fd, _destination, _depth in stack:
                        os.close(pending_fd)
        assert snapshot is not None
        return snapshot
    except Exception:
        if snapshot is not None:
            _remove_preview_tree(snapshot)
        raise


def _remove_preview_tree(path: str) -> None:
    """Cleanup failure is a real capacity/security failure, never ignored."""
    shutil.rmtree(path)


def _prefer_successful_spectral_detail(
    measured: SpectralAnalysisDetail | None,
    harness: SpectralAnalysisDetail | None,
) -> SpectralAnalysisDetail | None:
    """Prefer successful audit evidence; retain an error only as fallback."""
    if measured is not None and measured.attempted and measured.error is None:
        return measured
    if harness is not None and harness.attempted and harness.error is None:
        return harness
    if measured is not None and measured.attempted:
        return measured
    return harness


def compose_attempt_spectral_audit(
    measured: SpectralDetail,
    harness: SpectralDetail,
) -> SpectralDetail:
    """Compose IN from the best scan and HAVE from the preview measurement."""
    candidate = _prefer_successful_spectral_detail(
        measured.candidate, harness.candidate)
    existing = measured.existing
    return SpectralDetail(
        cliff_freq_hz=harness.cliff_freq_hz,
        suspect_pct=(candidate.suspect_pct or 0.0) if candidate else 0.0,
        per_track=list(candidate.per_track) if candidate else [],
        existing_suspect_pct=(
            existing.suspect_pct or 0.0 if existing else 0.0
        ),
        candidate=candidate,
        existing=existing,
    )


def _lossless_candidate_spectral_failure(
    measurement: PreimportMeasurement,
    *,
    lossless_candidate: bool,
) -> str | None:
    """Return diagnostics when lossless verification lacks a usable grade."""
    if not lossless_candidate:
        return None
    candidate = measurement.spectral_audit.candidate
    if candidate is None or not candidate.attempted:
        return "lossless candidate spectral analysis did not run"
    if candidate.error:
        return candidate.error
    if candidate.grade in (None, "error"):
        return "lossless candidate spectral analysis returned no usable grade"
    return None


def _write_preview_spectral_evidence_file(
    *,
    directory: str,
    mb_release_id: str,
    source_path: str,
    measurement: PreimportMeasurement,
    files: list[AlbumQualityEvidenceFile] | None,
    lossless_candidate: bool,
) -> str | None:
    """Carry preview-measured lossless spectral facts into the dry-run harness."""
    if not lossless_candidate:
        return None
    built = evidence_from_measurement(
        mb_release_id=mb_release_id,
        source_path=source_path,
        measurement=measurement,
        files=files,
    )
    if built.evidence is None:
        raise ValueError(
            built.reason or "could not encode preview spectral evidence"
        )
    payload = QualityEvidenceActionPayload(
        candidate=built.evidence,
        provenance=QualityEvidenceActionProvenance(
            candidate_status="preview_measured",
            snapshot_status="matched",
        ),
    )
    path = os.path.join(directory, "preview-spectral-evidence.json")
    with open(path, "wb") as handle:
        handle.write(msgspec.json.encode(payload))
    return path


@runtime_checkable
class ImportPreviewDB(QualityEvidenceDB, Protocol):
    """The PipelineDB surface the preview entry points use (#409).

    Extends ``QualityEvidenceDB`` because the handle is forwarded into the
    evidence persisters. Parity tests live in
    ``tests/test_import_preview.py``.
    """

    def get_download_log_entry(
        self, log_id: int,
    ) -> "DownloadLogWithEvidenceRow | None": ...

    def persist_current_spectral_measurement(
        self,
        *,
        request_id: int,
        expected_evidence_id: int,
        expected_snapshot_fingerprint: str,
        grade: str,
        bitrate_kbps: int | None,
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


def persist_exact_current_spectral_from_attempt(
    db: ImportPreviewDB,
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
    if not measured_existing_path:
        return EvidenceBuildResult(
            current_evidence,
            "stale",
            "attempt did not resolve a current Beets path",
        )
    try:
        measured_files = snapshot_audio_files(measured_existing_path)
    except OSError as exc:
        return EvidenceBuildResult(
            current_evidence,
            "failed",
            f"{type(exc).__name__}: {exc}",
        )
    if (
        not measured_files
        or snapshot_fingerprint(measured_files)
            != current_evidence.snapshot_fingerprint
    ):
        return EvidenceBuildResult(
            current_evidence,
            "stale",
            "attempt HAVE path does not match current evidence fingerprint",
        )
    try:
        current_id = db.get_request_current_evidence_id(request_id)
        refreshed = db.load_album_quality_evidence_by_id(current_evidence.id)
    except Exception as exc:
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
    # R19 belt-and-braces: a lossless-sourced row keeps its source spectral
    # (or stays empty until it is carried in) — an attempt-time scan of the
    # installed derivative must never be persisted as its grade, whatever
    # the caller's preserve flag said.
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
        )
        loaded = db.load_album_quality_evidence_by_id(current_evidence.id)
        linked_id = db.get_request_current_evidence_id(request_id)
    except Exception as exc:
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


def load_persisted_existing_spectral(
    db: ImportPreviewDB,
    request_id: int,
) -> tuple[AlbumQualityEvidence | None, SpectralAnalysisDetail, bool]:
    """Load linked HAVE provenance for the conditional audit boundary."""
    try:
        evidence_id = db.get_request_current_evidence_id(request_id)
    except Exception:
        logger.warning(
            "Unable to resolve current spectral evidence for request %s",
            request_id,
            exc_info=True,
        )
        return None, SpectralAnalysisDetail(attempted=False), True
    if evidence_id is None:
        return None, SpectralAnalysisDetail(attempted=False), False
    try:
        current_evidence = db.load_album_quality_evidence_by_id(evidence_id)
    except Exception:
        logger.warning(
            "Unable to load current spectral evidence %s for request %s",
            evidence_id,
            request_id,
            exc_info=True,
        )
        return None, SpectralAnalysisDetail(attempted=False), True
    if current_evidence is None:
        logger.warning(
            "Current spectral evidence %s is missing for request %s",
            evidence_id,
            request_id,
        )
        return None, SpectralAnalysisDetail(attempted=False), True
    measurement = current_evidence.measurement
    return (
        current_evidence,
        spectral_detail_from_persisted_source(
            measurement.spectral_grade,
            measurement.spectral_bitrate_kbps,
        ),
        True,
    )


def enrich_current_v0_research_for_preview(
    db: ImportPreviewDB,
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
    except Exception as exc:
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
    except Exception as exc:
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
        except Exception as exc:
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
    except Exception:  # noqa: BLE001 - neutral research must remain fail-soft
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
    except Exception as exc:
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
        except Exception as exc:
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
        except Exception as exc:
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
            except Exception as exc:
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
    except Exception as exc:
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

    Mirrors the once-only guards of the persist/claim helpers: any spectral
    field present means the scan already happened; a V0 metric or the
    attempted marker means the research probe already ran. Complete rows
    therefore cost nothing to re-plan.
    """
    measurement = evidence.measurement
    return EnrichmentPlan(
        spectral=(
            measurement.spectral_grade is None
            and measurement.spectral_bitrate_kbps is None
        ),
        v0=(
            evidence.v0_metric is None
            and not evidence.on_disk_v0_research_attempted
        ),
    )


def prepare_current_evidence_for_failure(
    db: ImportPreviewDB,
    *,
    request_id: int,
    mb_release_id: str,
    quality_ranks: QualityRankConfig,
    beets_library_root: str,
    load_fn: Callable[..., EvidenceBuildResult] = load_or_backfill_current_evidence,
) -> str:
    """Freshly resolve and link usable HAVE before failure logging.

    Returns ``ready`` only when the request FK resolves to the surviving
    evidence row, ``no_current_evidence`` only when Beets authoritatively says
    the exact release is absent, and ``failed`` for adapter, snapshot, or
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
        return "failed"
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
        return "failed"
    if result.status == "empty_current":
        return "no_current_evidence"
    if result.status != "ready" or result.evidence is None:
        logger.warning(
            "Could not prepare current evidence for request %s: %s%s",
            request_id,
            result.status,
            f" ({result.reason})" if result.reason else "",
        )
        return "failed"
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
        return "failed"
    if evidence is None or evidence.id is None:
        logger.warning(
            "Prepared current evidence was not linked for request %s",
            request_id,
        )
        return "failed"
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
        return "failed"
    return "ready"


def enrich_incomplete_current_evidence_for_request(
    db: ImportPreviewDB,
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
) -> str:
    """Opportunistically complete a request's HAVE evidence in place.

    Driven from the download-failure path after its canonical HAVE snapshot
    has been prepared and failure bookkeeping has completed. It repeats the
    exact current-Beets resolution before measuring any remaining enrichment.
    All writes go through the preview-owned helpers, so the once-only,
    exact-snapshot, and never-overwrite guards hold unchanged.

    Returns "no_current_evidence" (nothing linked), "stale" (files changed
    since capture), "complete" (nothing missing — zero cost), "enriched"
    (a rebuild or every missing piece resolved), or "partial" (work ran but
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
        return "partial"
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
        return "partial"
    if result.status == "empty_current":
        return "no_current_evidence"
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
        return "partial"
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
        return "enriched" if rebuilt else "complete"
    # Cheap freshness pre-check before any expensive measurement; the
    # persist/claim helpers each re-verify under their own authority.
    if not audio_snapshot_matches(current_album_path, evidence.files):
        return "stale"
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
    return "enriched" if all_ok else "partial"


def _authorize_current_evidence_for_preview(
    db: ImportPreviewDB,
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
    except Exception:
        logger.warning(
            "Unable to load/backfill preview HAVE evidence for request %s",
            request_id,
            exc_info=True,
        )
        return EvidenceBuildResult(
            None,
            "failed",
            "current Beets authority resolution raised",
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
        current,
        "ready",
        current_album_path=current_album_path,
    )


def load_current_evidence_for_preview(
    db: ImportPreviewDB,
    *,
    request_id: int,
    mb_release_id: str,
    quality_ranks: QualityRankConfig,
    beets_library_root: str,
    preloaded_evidence: AlbumQualityEvidence | None,
    enrich_current_fn: Callable[..., EvidenceBuildResult] | None = None,
) -> EvidenceBuildResult:
    """Load/backfill HAVE and perform preview-owned neutral enrichment."""

    authorized = _authorize_current_evidence_for_preview(
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


def preserve_existing_source_spectral(
    current_evidence: AlbumQualityEvidence | None,
) -> bool:
    """Whether HAVE must retain lossless-source pre-conversion evidence.

    R19: a lossless-sourced installed copy wears its SOURCE's spectral;
    scanning the installed derivative can rewrite a transcode-like source
    as apparently genuine (fullband codecs like Opus always scan clean).
    Any row-local acquisition fact proves lossless lineage: a lossless
    ``was_converted_from``, verified-lossless proof, or a source-subject
    V0 anchor (the provisional lane's marker — enrichment-born rows carry
    no ``was_converted_from``, which is how the #711 deploy-night rows
    were minted ``genuine/installed``).
    """
    if current_evidence is None:
        return False
    converted_from = (
        current_evidence.measurement.was_converted_from or ""
    ).lower()
    if converted_from in LOSSLESS_CODECS:
        return True
    if current_evidence.verified_lossless_proof is not None:
        return True
    v0_metric = current_evidence.v0_metric
    return (
        v0_metric is not None
        and v0_metric.subject == EVIDENCE_SUBJECT_SOURCE
    )


# Verdict values for `ImportPreviewResult.verdict`. After U5 the
# measure-and-persist entry point (`measure_and_persist_candidate_evidence`)
# emits only the two new verdicts (`evidence_ready` / `measurement_failed`);
# the classify entry points (`preview_import_from_path` and friends — CLI
# inspector, wrong_match triage, values-mode synthetic preview) still return
# `would_import` / `confident_reject` / `uncertain` from the classifier.
PREVIEW_VERDICT_WOULD_IMPORT = "would_import"
PREVIEW_VERDICT_CONFIDENT_REJECT = "confident_reject"
PREVIEW_VERDICT_UNCERTAIN = "uncertain"
PREVIEW_VERDICT_EVIDENCE_READY = "evidence_ready"
PREVIEW_VERDICT_MEASUREMENT_FAILED = "measurement_failed"


class ImportPreviewValues(msgspec.Struct, frozen=True):
    """Typed values for synthetic import-preview simulation."""

    is_flac: bool = False
    min_bitrate: int | None = None
    is_cbr: bool = False
    is_vbr: bool | None = None
    avg_bitrate: int | None = None
    spectral_grade: str | None = None
    spectral_bitrate: int | None = None
    existing_min_bitrate: int | None = None
    existing_avg_bitrate: int | None = None
    existing_spectral_bitrate: int | None = None
    existing_spectral_grade: str | None = None
    override_min_bitrate: int | None = None
    existing_format: str | None = None
    existing_is_cbr: bool = False
    post_conversion_min_bitrate: int | None = None
    post_conversion_is_cbr: bool | None = None
    converted_count: int = 0
    candidate_verified_lossless_proof: bool = False
    verified_lossless_target: str | None = None
    target_format: str | None = None
    new_format: str | None = None
    audio_check_mode: str = "normal"
    audio_corrupt: bool = False
    has_nested_audio: bool = False
    candidate_v0_probe_avg: int | None = None
    candidate_v0_probe_min: int | None = None
    existing_v0_probe_avg: int | None = None
    candidate_v0_probe_kind: str | None = None
    existing_v0_probe_kind: str | None = None
    supported_lossless_source: bool | None = None


class ImportPreviewResult(msgspec.Struct):
    """Common preview result returned by CLI/API/triage code.

    U5 added two new verdicts: ``evidence_ready`` and ``measurement_failed``.
    The preview worker (``scripts/import_preview_worker.py``) emits only these
    two in production after U5; legacy callers (CLI inspector, wrong-match
    triage, values-mode synthetic preview) continue to receive
    ``would_import`` / ``confident_reject`` / ``uncertain`` from the classifier.

    When ``verdict='measurement_failed'``, ``failure`` carries the typed
    ``MeasurementFailure`` payload that the preview worker passes to
    ``_record_preview_measurement_failed`` for self-healing finalize.
    """

    mode: str
    verdict: str
    would_import: bool = False
    confident_reject: bool = False
    uncertain: bool = False
    cleanup_eligible: bool = False
    decision: str | None = None
    reason: str | None = None
    detail: str | None = None
    stage_chain: list[str] = []
    request_id: int | None = None
    download_log_id: int | None = None
    source_path: str | None = None
    import_result: ImportResult | None = None
    simulation: dict[str, Any] | None = None
    failure: MeasurementFailure | None = None

    def to_dict(self) -> dict[str, Any]:
        return msgspec.to_builtins(self)

    def to_json(self) -> str:
        return msgspec.json.encode(self).decode()


def _preview_result(
    *,
    mode: str,
    verdict: str,
    decision: str | None = None,
    reason: str | None = None,
    detail: str | None = None,
    stage_chain: list[str] | None = None,
    request_id: int | None = None,
    download_log_id: int | None = None,
    source_path: str | None = None,
    import_result: ImportResult | None = None,
    simulation: dict[str, Any] | None = None,
    cleanup_eligible: bool = False,
    failure: MeasurementFailure | None = None,
) -> ImportPreviewResult:
    would_import = verdict == PREVIEW_VERDICT_WOULD_IMPORT
    confident_reject = verdict == PREVIEW_VERDICT_CONFIDENT_REJECT
    uncertain = verdict == PREVIEW_VERDICT_UNCERTAIN
    return ImportPreviewResult(
        mode=mode,
        verdict=verdict,
        would_import=would_import,
        confident_reject=confident_reject,
        uncertain=uncertain,
        cleanup_eligible=cleanup_eligible if confident_reject else False,
        decision=decision,
        reason=reason or decision,
        detail=detail,
        stage_chain=stage_chain or [],
        request_id=request_id,
        download_log_id=download_log_id,
        source_path=source_path,
        import_result=import_result,
        simulation=simulation,
        failure=failure,
    )


# Bound on how much of a subprocess's stderr we fold into a persisted
# MeasurementFailure.detail / the journal — this is diagnostic breadcrumb,
# not a log dump. Individual import_one.py [FAIL] lines are themselves
# already bounded to a ~230-char ffmpeg-stderr tail (see
# harness/import_one.py::convert_lossless), so a handful of them fits
# comfortably; this is a hard ceiling against pathological inputs.
_STDERR_DIAGNOSTIC_MAX_CHARS = 2000


def _diagnostic_from_stderr(stderr: str, max_chars: int = _STDERR_DIAGNOSTIC_MAX_CHARS) -> str:
    """Extract the useful per-file failure signal from an import_one.py stderr blob.

    ``convert_lossless`` (``harness/import_one.py``) prints one
    ``[FAIL] <file>: <ffmpeg tail>`` line per failed conversion to stderr,
    but the aggregate ``ImportResult.error`` on a ``conversion_failed`` /
    ``target_conversion_failed`` result only ever says e.g. "11 FLAC files
    failed to convert" — the per-file *why* was otherwise dropped on the
    floor (never persisted, never streamed to the journal on the preview
    path, unlike the importer's ``dispatch_import_core``). This pulls every
    ``[FAIL]`` line back out so the true tool-level reason survives.

    Falls back to a bounded tail of the raw stderr when no ``[FAIL]`` marker
    is present (e.g. a crash before the per-file loop even started), so a
    non-empty stderr never yields an empty diagnostic.

    Keeps whole lines (never slices a line in half) so a captured ``[FAIL]``
    marker is never partially truncated; only hard-truncates as a last
    resort for a single line that alone exceeds ``max_chars``. Never raises
    — this runs on arbitrary subprocess output.
    """
    if not stderr or not stderr.strip():
        return ""

    lines = [ln.strip() for ln in stderr.splitlines() if ln.strip()]
    fail_lines = [ln for ln in lines if "[FAIL]" in ln]
    source_lines = fail_lines if fail_lines else lines

    # Keep the most recent lines within the char budget (newest failures are
    # the most actionable), without truncating a kept line mid-string.
    kept: list[str] = []
    total = 0
    for line in reversed(source_lines):
        joiner_cost = 3 if kept else 0  # " / "
        added = len(line) + joiner_cost
        if total + added > max_chars and kept:
            break
        kept.append(line)
        total += added
    kept.reverse()

    result = " / ".join(kept)
    if len(result) > max_chars:
        # Pathological single oversized line — hard ceiling wins.
        result = result[:max_chars]
    return result


def _measurement_failed_result(
    *,
    mode: str,
    reason: MeasurementFailureReason,
    decision: str,
    detail: str,
    source_path: str | None = None,
    request_id: int | None = None,
    download_log_id: int | None = None,
    import_result: ImportResult | None = None,
    stage_chain: list[str] | None = None,
    subprocess_stderr: str | None = None,
) -> ImportPreviewResult:
    """Build a ``verdict='measurement_failed'`` preview result with typed payload.

    ``subprocess_stderr``, when supplied, is the raw ``ImportOneRun.stderr``
    from the ``run_import_one`` call that triggered this failure. The
    per-file tool diagnostic it carries (e.g. the real ffmpeg decode error
    behind a "conversion_failed") is folded into ``detail`` (so it reaches
    both the persisted ``MeasurementFailure`` payload and the operator-
    facing preview result) and logged at WARNING so it is visible in the
    journal without needing to reproduce the failure. This closes the gap
    where a conversion failure's only DB/journal trace was an aggregate
    count ("11 FLAC files failed to convert") with the actual per-file
    reason discarded.
    """
    full_detail = detail
    if subprocess_stderr:
        diagnostic = _diagnostic_from_stderr(subprocess_stderr)
        if diagnostic:
            full_detail = f"{detail} | {diagnostic}"
            logger.warning(
                "measurement_failed decision=%s request_id=%s: %s",
                decision, request_id, diagnostic,
            )
    payload = MeasurementFailure(
        reason=reason,
        detail=full_detail,
        source_path=source_path or "",
    )
    return _preview_result(
        mode=mode,
        verdict=PREVIEW_VERDICT_MEASUREMENT_FAILED,
        decision=decision,
        reason=reason,
        detail=full_detail,
        stage_chain=stage_chain,
        request_id=request_id,
        download_log_id=download_log_id,
        source_path=source_path,
        import_result=import_result,
        failure=payload,
    )


def _evidence_ready_result(
    *,
    mode: str,
    decision: str,
    reason: str | None = None,
    detail: str | None = None,
    stage_chain: list[str] | None = None,
    request_id: int | None = None,
    download_log_id: int | None = None,
    source_path: str | None = None,
    import_result: ImportResult | None = None,
) -> ImportPreviewResult:
    """Build a ``verdict='evidence_ready'`` preview result.

    Used by the worker-mode entry point when preview successfully measured the
    candidate and persisted evidence. The importer reads the persisted
    evidence and decides accept/reject via
    ``full_pipeline_decision_from_evidence`` (U11).
    """
    return _preview_result(
        mode=mode,
        verdict=PREVIEW_VERDICT_EVIDENCE_READY,
        decision=decision,
        reason=reason or decision,
        detail=detail,
        stage_chain=stage_chain,
        request_id=request_id,
        download_log_id=download_log_id,
        source_path=source_path,
        import_result=import_result,
    )


def _stage_chain_from_simulation(simulation: dict[str, Any]) -> list[str]:
    chain: list[str] = []
    for key in (
        "preimport_nested",
        "preimport_audio",
        "stage0_spectral_gate",
        "stage1_spectral",
        "stage2_import",
        "stage3_quality_gate",
    ):
        value = simulation.get(key)
        if value is not None:
            chain.append(f"{key}:{value}")
    return chain


def preview_import_from_values(
    values: ImportPreviewValues,
    *,
    cfg: QualityRankConfig | None = None,
) -> ImportPreviewResult:
    """Preview a synthetic typed scenario through the shared simulator seam."""
    simulation = full_pipeline_decision(
        is_flac=values.is_flac,
        min_bitrate=values.min_bitrate or 0,
        is_cbr=values.is_cbr,
        is_vbr=values.is_vbr,
        avg_bitrate=values.avg_bitrate,
        spectral_grade=values.spectral_grade,
        spectral_bitrate=values.spectral_bitrate,
        existing_min_bitrate=values.existing_min_bitrate,
        existing_avg_bitrate=values.existing_avg_bitrate,
        existing_spectral_grade=values.existing_spectral_grade,
        existing_spectral_bitrate=values.existing_spectral_bitrate,
        override_min_bitrate=values.override_min_bitrate,
        existing_format=values.existing_format,
        existing_is_cbr=values.existing_is_cbr,
        post_conversion_min_bitrate=values.post_conversion_min_bitrate,
        post_conversion_is_cbr=values.post_conversion_is_cbr,
        converted_count=values.converted_count,
        candidate_verified_lossless_proof=(
            values.candidate_verified_lossless_proof
        ),
        verified_lossless_target=values.verified_lossless_target,
        target_format=values.target_format,
        new_format=values.new_format,
        audio_check_mode=values.audio_check_mode,
        audio_corrupt=values.audio_corrupt,
        has_nested_audio=values.has_nested_audio,
        candidate_v0_probe_avg=values.candidate_v0_probe_avg,
        candidate_v0_probe_min=values.candidate_v0_probe_min,
        existing_v0_probe_avg=values.existing_v0_probe_avg,
        candidate_v0_probe_kind=values.candidate_v0_probe_kind,
        existing_v0_probe_kind=values.existing_v0_probe_kind,
        supported_lossless_source=values.supported_lossless_source,
        cfg=cfg,
    )
    verdict, cleanup_eligible, reason = classify_full_pipeline_decision(simulation)
    return _preview_result(
        mode="values",
        verdict=verdict,
        decision=reason,
        reason=reason,
        stage_chain=_stage_chain_from_simulation(simulation),
        simulation=simulation,
        cleanup_eligible=cleanup_eligible,
    )


def _quality_gate_stage(
    measurement: AudioQualityMeasurement | None,
    cfg: QualityRankConfig,
    target_contract: TargetQualityContract | None = None,
    verified_lossless_proof: bool = False,
) -> str | None:
    if measurement is None:
        return None
    return quality_gate_decision(
        measurement,
        cfg=cfg,
        target_contract=target_contract,
        verified_lossless_proof=verified_lossless_proof,
    )


def _classify_import_result(
    ir: ImportResult | None,
    *,
    cfg: QualityRankConfig,
) -> tuple[str, bool, str | None, list[str]]:
    if ir is None:
        return "uncertain", False, "no_json_result", ["harness:no_json_result"]
    decision = ir.decision or "unknown"
    chain = [f"stage2_import:{decision}"]
    gate: str | None = None
    if decision in ("import", "preflight_existing"):
        gate = _quality_gate_stage(
            ir.source_measurement,
            cfg,
            ir.target_quality_contract,
            ir.verified_lossless_proof is not None,
        )
        if gate is not None:
            chain.append(f"stage3_quality_gate:{gate}")
    if decision in ("conversion_failed", "target_conversion_failed", "crash"):
        return "uncertain", False, decision, chain
    verdict, cleanup_eligible, reason = classify_quality_import_stages(
        decision,
        gate if decision in ("import", "preflight_existing") else None,
        imported=decision in QUALITY_DECISION_IMPORT_STAGE_DECISIONS,
    )
    return verdict, cleanup_eligible, reason, chain


def _request_label(req: Mapping[str, Any]) -> str:
    return f"{req.get('artist_name', '')} - {req.get('album_title', '')}".strip(" -")


def measure_and_persist_candidate_evidence(
    db: ImportPreviewDB,
    *,
    request_id: int,
    path: str,
    force: bool = True,
    download_log_id: int | None = None,
    import_job_id: int | None = None,
    persist_measurement_fn: Callable[..., EvidenceBuildResult] | None = None,
    run_import_fn: Callable[..., ImportOneRun] | None = None,
    spectral_detail_analyzer: SpectralDetailAnalyzer | None = None,
    existing_spectral_resolver: ExistingSpectralResolver | None = None,
    current_evidence_loader: Callable[..., EvidenceBuildResult] | None = None,
) -> ImportPreviewResult:
    """Measure a source folder and persist candidate evidence; never decide.

    The worker/refresh contract (preview worker, #271 stale-evidence
    refresh): purely a fact-gathering surface. It calls
    ``measure_preimport_state`` and persists the resulting facts on
    ``AlbumQualityEvidence``, returning only ``evidence_ready`` /
    ``measurement_failed``. The importer's
    ``full_pipeline_decision_from_evidence`` (U11) reads the persisted
    evidence row and makes every import decision — folder/audio-integrity
    facts are early-exit reject branches at the top of that function.
    For the classify contract (CLI inspector, wrong-match triage UI,
    values preview) use ``preview_import_from_path``.

    ``download_log_id`` / ``import_job_id`` are how the persisted evidence
    gets linked onto the addressing entities (``download_log.
    candidate_evidence_id`` / ``import_jobs.candidate_evidence_id``).
    Omitting both still persists the content-addressed evidence row, but
    triage's FK walk won't find it — pass whichever IDs the call site has.

    Flow:
      1. Validate request / mbid / path inputs (return measurement_failed on
         any sanity-check failure).
      2. Snapshot source files via ``snapshot_audio_files`` for the candidate
         evidence ``files`` column AND the post-measurement stale-source guard.
      3. Materialize into a temp copy so the harness has an isolated working
         dir (matches existing preview behavior).
      4. Inspect the temp copy for filetype / bitrate / vbr hints.
      5. Call ``measure_preimport_state`` (the pure measurement helper — no
         denylist writes, no decision branches). This runs the audio integrity
         gate, bad-hash gate, spectral gate, and persists on-disk spectral
         state to ``album_requests`` per issue #90 propagation.
      6. If the measurement carries an importer-rejecting fact (audio_corrupt,
         bad_audio_hash, nested layout, empty fileset), persist evidence
         straight from the measurement (no harness call) and return
         ``evidence_ready``. The importer's
         ``full_pipeline_decision_from_evidence`` (U11) reads those facts off
         the persisted evidence row and rejects via the four-fact early-exit
         branches upstream of the quality gate.
      7. Otherwise, run ``run_import_one`` in dry-run mode to produce an
         ``ImportResult`` with ``source_measurement``. Persist evidence built
         from both the measurement (U1 facts) and the import result (audio
         measurement, spectral, V0 probe).
      8. Return ``evidence_ready`` when persistence succeeded; otherwise
         ``measurement_failed`` with the appropriate ``MeasurementFailureReason``.
    """
    from lib.config import read_runtime_config

    # --- Sanity checks ---
    req = db.get_request(request_id)
    if not req:
        return _measurement_failed_result(
            mode="path",
            reason="request_not_found",
            decision="request_not_found",
            detail=f"Request {request_id} not found",
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=path,
        )

    mbid = str(req.get("mb_release_id") or "")
    if not mbid:
        return _measurement_failed_result(
            mode="path",
            reason="missing_release_id",
            decision="missing_release_id",
            detail="No MusicBrainz release ID",
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=path,
        )

    if not os.path.isdir(path):
        return _measurement_failed_result(
            mode="path",
            reason="source_vanished",
            decision="path_missing",
            detail=f"Path not found: {path}",
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=path,
        )

    # --- Snapshot for freshness guard + evidence files column ---
    try:
        source_snapshot = snapshot_audio_files(path)
    except OSError as exc:
        return _measurement_failed_result(
            mode="path",
            reason="snapshot_stale",
            decision="evidence_snapshot_failed",
            detail=str(exc),
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=path,
        )

    cfg = read_runtime_config()
    (
        current_evidence,
        existing_spectral_evidence,
        _current_evidence_authoritative,
    ) = load_persisted_existing_spectral(db, request_id)
    load_current = current_evidence_loader or load_current_evidence_for_preview
    current_result = load_current(
        db,
        request_id=request_id,
        mb_release_id=mbid,
        quality_ranks=cfg.quality_ranks,
        beets_library_root=getattr(cfg, "beets_directory", ""),
        preloaded_evidence=current_evidence,
    )
    if current_result.status == "empty_current":
        current_evidence = None
        existing_spectral_evidence = SpectralAnalysisDetail(attempted=False)
    elif current_result.status != "ready" or current_result.evidence is None:
        return _measurement_failed_result(
            mode="path",
            reason="measurement_crashed",
            decision="current_evidence_failed",
            detail=(
                f"{current_result.status}: "
                f"{current_result.reason or 'current authority unavailable'}"
            ),
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=path,
        )
    else:
        current_evidence = current_result.evidence
        current_m = current_evidence.measurement
        existing_spectral_evidence = spectral_detail_from_persisted_source(
            current_m.spectral_grade,
            current_m.spectral_bitrate_kbps,
        )
    preserve_have_source = preserve_existing_source_spectral(current_evidence)

    try:
        temp_root = _snapshot_authorized_directory(path, cfg)
    except (FilesystemAuthorityError, OSError) as exc:
        return _measurement_failed_result(
            mode="path",
            reason="materialization_error",
            decision="materialization_failed",
            detail=f"private descriptor snapshot failed: {exc}",
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=path,
        )
    try:
        preview_path = temp_root
        try:
            repair_mp3_headers(preview_path)
        except Exception:
            pass
        inspection = inspect_local_files(preview_path)

        # --- Run the pure measurement helper (no decision) ---
        try:
            measurement = measure_preimport_state(
                path=preview_path,
                mb_release_id=mbid,
                label=_request_label(req),
                download_filetype=inspection.filetype,
                download_min_bitrate_bps=inspection.min_bitrate_bps,
                download_is_vbr=inspection.is_vbr,
                cfg=cfg,
                # db=None / request_id=None: HAVE spectral state is persisted
                # later via the content-addressed AlbumQualityEvidence row that
                # the importer reads. Preview is a pure measurement surface.
                db=None,
                request_id=None,
                existing_spectral_evidence=existing_spectral_evidence,
                preserve_existing_source_spectral=preserve_have_source,
                precomputed_inspection=inspection,
                spectral_detail_analyzer=spectral_detail_analyzer,
                existing_spectral_resolver=existing_spectral_resolver,
            )
            spectral_result = persist_exact_current_spectral_from_attempt(
                db,
                request_id=request_id,
                current_evidence=current_evidence,
                measured_existing=measurement.spectral_audit.existing,
                measured_existing_path=measurement.existing_spectral_path,
            )
            if spectral_result.evidence is not None:
                current_evidence = spectral_result.evidence
        except Exception as exc:
            return _measurement_failed_result(
                mode="path",
                reason="measurement_crashed",
                decision="measurement_crashed",
                detail=f"{type(exc).__name__}: {exc}",
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
            )

        # Integrity facts are authoritative even when the same corrupt
        # lossless source also prevents spectral analysis from producing a
        # grade. Persist/reject the concrete decode failure instead of
        # demoting it to the secondary measurement failure.
        measurement_rejecting = (
            measurement.audio_corrupt
            or measurement.matched_bad_hash_id is not None
            or measurement.folder_layout == "nested"
            or (measurement.audio_file_count == 0 and not source_snapshot)
        )
        spectral_failure = _lossless_candidate_spectral_failure(
            measurement,
            lossless_candidate=measurement.lossless_candidate,
        )
        if spectral_failure is not None and not measurement_rejecting:
            return _measurement_failed_result(
                mode="path",
                reason="measurement_crashed",
                decision="spectral_analysis_failed",
                detail=spectral_failure,
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
                import_result=ImportResult(spectral=measurement.spectral_audit),
            )

        # --- Measurement-only evidence path ---
        # When the measurement carries any importer-rejecting fact, skip the
        # harness (it would either fail or produce misleading state) and
        # persist evidence straight from the measurement. The importer's
        # ``full_pipeline_decision_from_evidence`` (U11) reads those facts
        # off the persisted evidence row and rejects via the four-fact
        # early-exit branches.
        audit_result = ImportResult(spectral=measurement.spectral_audit)
        if measurement_rejecting:
            if not audio_snapshot_matches(path, source_snapshot):
                return _measurement_failed_result(
                    mode="path",
                    reason="snapshot_stale",
                    decision="source_changed_during_preview",
                    detail="source files changed while preview was running",
                    request_id=request_id,
                    download_log_id=download_log_id,
                    source_path=path,
                    import_result=audit_result,
                )
            try:
                evidence_result = (
                    persist_measurement_fn
                    or persist_candidate_evidence_from_measurement
                )(
                    db,
                    mb_release_id=mbid,
                    source_path=path,
                    measurement=measurement,
                    download_log_id=download_log_id,
                    import_job_id=import_job_id,
                    files=source_snapshot,
                )
            except Exception as exc:
                return _measurement_failed_result(
                    mode="path",
                    reason="evidence_persist_failed",
                    decision="evidence_persist_failed",
                    detail=f"{type(exc).__name__}: {exc}",
                    request_id=request_id,
                    download_log_id=download_log_id,
                    source_path=path,
                    import_result=audit_result,
                )
            if evidence_result.status != "ready":
                return _measurement_failed_result(
                    mode="path",
                    reason="evidence_persist_failed",
                    decision=f"evidence_{evidence_result.status}",
                    detail=evidence_result.reason or f"evidence_{evidence_result.status}",
                    request_id=request_id,
                    download_log_id=download_log_id,
                    source_path=path,
                    import_result=audit_result,
                )
            decision_hint = _measurement_decision_hint(measurement)
            return _evidence_ready_result(
                mode="path",
                decision=decision_hint,
                reason=decision_hint,
                detail=f"measurement persisted: {decision_hint}",
                stage_chain=[f"measure_preimport:{decision_hint}"],
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
                import_result=ImportResult(spectral=measurement.spectral_audit),
            )

        # --- Harness path: measurement allows continuing ---
        existing_v0_probe: V0ProbeEvidence | None = None
        existing_spectral = measurement.existing_spectral
        existing_grade = (
            existing_spectral.grade
            if existing_spectral is not None
            else existing_spectral_evidence.grade
        )
        existing_bitrate = (
            existing_spectral.bitrate_kbps
            if existing_spectral is not None
            else existing_spectral_evidence.bitrate_kbps
        )
        if current_evidence is not None:
            current_m = current_evidence.measurement
            existing_grade = current_m.spectral_grade
            existing_bitrate = current_m.spectral_bitrate_kbps
            if current_evidence.v0_metric is not None:
                existing_v0_probe = audit_v0_probe_from_metric(
                    current_evidence.v0_metric
                )
        override_min_bitrate = compute_effective_override_bitrate(
            (
                current_evidence.measurement.min_bitrate_kbps
                if current_evidence is not None
                else measurement.existing_min_bitrate
            ),
            existing_bitrate if isinstance(existing_bitrate, int) else None,
            existing_grade if isinstance(existing_grade, str) else None,
        )

        try:
            preview_spectral_file = _write_preview_spectral_evidence_file(
                directory=temp_root,
                mb_release_id=mbid,
                source_path=path,
                measurement=measurement,
                files=source_snapshot,
                lossless_candidate=measurement.lossless_candidate,
            )
            run = (run_import_fn or run_import_one)(
                path=preview_path,
                mb_release_id=mbid,
                request_id=None,
                force=force,
                preserve_source=True,
                dry_run=True,
                override_min_bitrate=override_min_bitrate,
                target_format=req.get("target_format"),
                verified_lossless_target=cfg.verified_lossless_target,
                beets_harness_path=cfg.beets_harness_path,
                quality_rank_config_json=cfg.quality_ranks.to_json(),
                existing_v0_probe=existing_v0_probe,
                quality_evidence_action_file=preview_spectral_file,
            )
        except Exception as exc:
            return _measurement_failed_result(
                mode="path",
                reason="measurement_crashed",
                decision="harness_crashed",
                detail=f"{type(exc).__name__}: {exc}",
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
                import_result=audit_result,
            )

        if run.import_result is None:
            return _measurement_failed_result(
                mode="path",
                reason="measurement_crashed",
                decision="no_json_result",
                detail="import_one.py emitted no JSON",
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
                import_result=audit_result,
                subprocess_stderr=run.stderr,
            )
        # The preview worker's independent two-sided audit is the attempt
        # record. Keep it separate from decision measurements and replace the
        # harness-local spectral detail with the best successful evidence from
        # either pass, retaining an error only when neither pass succeeded.
        run.import_result.spectral = compose_attempt_spectral_audit(
            measurement.spectral_audit,
            run.import_result.spectral,
        )
        if run.import_result.decision in (
            "conversion_failed",
            "target_conversion_failed",
            "crash",
        ):
            # "crash" is the harness's top-level exception envelope: whatever
            # partial measurements it carries were interrupted mid-build
            # (2026-07-18: the proof mint crashed AFTER source_measurement was
            # set, so the partial result looked persistable but had lost its
            # verified-lossless proof). Analysis failure aborts loudly — it
            # never becomes evidence_ready.
            return _measurement_failed_result(
                mode="path",
                reason="measurement_crashed",
                decision=run.import_result.decision,
                detail=run.import_result.error or run.import_result.decision,
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
                import_result=run.import_result,
                subprocess_stderr=run.stderr,
            )
        if run.import_result.source_measurement is None:
            return _measurement_failed_result(
                mode="path",
                reason="measurement_crashed",
                decision=run.import_result.decision or "missing_source_measurement",
                detail="ImportResult missing source_measurement",
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
                import_result=run.import_result,
                subprocess_stderr=run.stderr,
            )

        # --- Snapshot freshness guard (post-measurement) ---
        if not audio_snapshot_matches(path, source_snapshot):
            return _measurement_failed_result(
                mode="path",
                reason="snapshot_stale",
                decision="source_changed_during_preview",
                detail="source files changed while preview was running",
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
                import_result=run.import_result,
            )

        # --- Persist candidate evidence ---
        try:
            evidence_result = persist_candidate_evidence_from_import_result(
                db,
                mb_release_id=mbid,
                source_path=path,
                import_result=run.import_result,
                download_log_id=download_log_id,
                import_job_id=import_job_id,
                files=source_snapshot,
                measurement=measurement,
            )
        except Exception as exc:
            return _measurement_failed_result(
                mode="path",
                reason="evidence_persist_failed",
                decision="evidence_persist_failed",
                detail=f"{type(exc).__name__}: {exc}",
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
                import_result=run.import_result,
            )
        if evidence_result.status != "ready":
            return _measurement_failed_result(
                mode="path",
                reason="evidence_persist_failed",
                decision=f"evidence_{evidence_result.status}",
                detail=evidence_result.reason or f"evidence_{evidence_result.status}",
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
                import_result=run.import_result,
            )

        return _evidence_ready_result(
            mode="path",
            decision=run.import_result.decision or "evidence_ready",
            reason=run.import_result.decision,
            detail=run.import_result.error,
            stage_chain=[
                f"measure_preimport:ok",
                f"stage2_import:{run.import_result.decision}",
            ],
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=path,
            import_result=run.import_result,
        )
    finally:
        _remove_preview_tree(temp_root)


def _measurement_decision_hint(measurement: Any) -> str:
    """Derive a short label for measurement-only evidence_ready returns.

    Used purely for log/decision-string display — the importer's
    ``full_pipeline_decision_from_evidence`` (U11) makes the actual reject
    call from the persisted evidence via its four-fact early-exit branches.
    Order mirrors that decider's evaluation order.
    """
    if measurement.audio_corrupt:
        return "audio_corrupt"
    if measurement.matched_bad_hash_id is not None:
        return "bad_audio_hash"
    if measurement.folder_layout == "nested":
        return "nested_layout"
    if measurement.audio_file_count == 0:
        return "empty_fileset"
    return "evidence_ready"


def preview_import_from_path(
    db: ImportPreviewDB,
    *,
    request_id: int,
    path: str,
    force: bool = True,
    download_log_id: int | None = None,
    import_job_id: int | None = None,
    persist_candidate_evidence: bool = False,
    _already_isolated: bool = False,
) -> ImportPreviewResult:
    """Classify a real source folder without mutating source files or beets.

    The classify contract (CLI inspector, wrong-match triage UI, values
    preview): returns the classifier's ``would_import`` /
    ``confident_reject`` / ``uncertain`` verdicts for audit/UI display.
    DB evidence persistence is opt-in via ``persist_candidate_evidence``.
    For the measure-and-persist worker/refresh contract (verdicts
    ``evidence_ready`` / ``measurement_failed``) use
    ``measure_and_persist_candidate_evidence``.

    Contract: preview only measures. Facts come from
    ``measure_preimport_state``; the five folder/audio-integrity facts are
    inlined as a confident_reject verdict for CLI/triage UI. Spectral /
    codec rank / V0 / quality-gate decisions belong to the importer's
    ``full_pipeline_decision_from_evidence``.
    """
    req = db.get_request(request_id)
    if not req:
        return _preview_result(
            mode="path",
            verdict=PREVIEW_VERDICT_UNCERTAIN,
            decision="request_not_found",
            reason=f"Request {request_id} not found",
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=path,
        )

    mbid = str(req.get("mb_release_id") or "")
    if not mbid:
        return _preview_result(
            mode="path",
            verdict=PREVIEW_VERDICT_UNCERTAIN,
            decision="missing_release_id",
            reason="No MusicBrainz release ID",
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=path,
        )
    if not os.path.isdir(path):
        return _preview_result(
            mode="path",
            verdict=PREVIEW_VERDICT_UNCERTAIN,
            decision="path_missing",
            reason=f"Path not found: {path}",
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=path,
        )

    from lib.config import read_runtime_config

    cfg = read_runtime_config()
    (
        preloaded_current_evidence,
        _,
        _,
    ) = load_persisted_existing_spectral(db, request_id)
    current_authority = _authorize_current_evidence_for_preview(
        db,
        request_id=request_id,
        mb_release_id=mbid,
        quality_ranks=cfg.quality_ranks,
        beets_library_root=getattr(cfg, "beets_directory", ""),
        preloaded_evidence=preloaded_current_evidence,
    )
    if current_authority.status == "empty_current":
        # Authoritative absence: stale linked HAVE facts describe no current
        # bytes and cannot influence candidate measurement or decision inputs.
        current_evidence = None
        existing_spectral_evidence = SpectralAnalysisDetail(attempted=False)
    elif (
        current_authority.status != "ready"
        or current_authority.evidence is None
    ):
        return _measurement_failed_result(
            mode="path",
            reason="measurement_crashed",
            decision="current_evidence_failed",
            detail=(
                f"{current_authority.status}: "
                f"{current_authority.reason or 'current authority unavailable'}"
            ),
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=path,
        )
    else:
        current_evidence = current_authority.evidence
        current_measurement = current_evidence.measurement
        existing_spectral_evidence = spectral_detail_from_persisted_source(
            current_measurement.spectral_grade,
            current_measurement.spectral_bitrate_kbps,
        )
    preserve_have_source = preserve_existing_source_spectral(current_evidence)

    # Every preview runs against one bounded, descriptor-copied private
    # snapshot.  The download-log entry point has already made that snapshot;
    # direct CLI path mode takes the same no-follow route here rather than an
    # unbounded ``copytree``.  No external tool receives a DB- or CLI-supplied
    # pathname before this boundary is complete.
    try:
        temp_root = None if _already_isolated else _snapshot_authorized_directory(
            path, cfg,
        )
    except (FilesystemAuthorityError, OSError) as exc:
        return _preview_result(
            mode="path",
            verdict=PREVIEW_VERDICT_UNCERTAIN,
            decision="path_unauthorized",
            reason="path_unauthorized",
            detail=str(exc),
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=path,
        )
    try:
        preview_path = path if temp_root is None else temp_root
        # Header repair is deliberately against the private copy. This keeps
        # the source immutable while preserving the importer-facing order:
        # repair before inspection, measurement, and the dry-run harness.
        try:
            repair_mp3_headers(preview_path)
        except Exception:
            pass

        source_snapshot = None
        if persist_candidate_evidence:
            try:
                source_snapshot = snapshot_audio_files(path)
            except OSError as exc:
                return _preview_result(
                    mode="path",
                    verdict=PREVIEW_VERDICT_UNCERTAIN,
                    decision="evidence_snapshot_failed",
                    reason="evidence_snapshot_failed",
                    detail=str(exc),
                    request_id=request_id,
                    download_log_id=download_log_id,
                    source_path=path,
                )
            if not source_snapshot:
                # Empty source snapshot: evidence persistence requires at
                # least one file, so surface the empty fileset as uncertain.
                return _preview_result(
                    mode="path",
                    verdict=PREVIEW_VERDICT_UNCERTAIN,
                    decision="evidence_empty_fileset",
                    reason="evidence_empty_fileset",
                    detail="no audio files found",
                    request_id=request_id,
                    download_log_id=download_log_id,
                    source_path=path,
                )
        inspection = inspect_local_files(preview_path)
        if inspection.has_nested_audio:
            detail = (
                "Audio files are in subdirectories — flatten the folder "
                "before import."
            )
            return _preview_result(
                mode="path",
                verdict=PREVIEW_VERDICT_CONFIDENT_REJECT,
                decision="nested_layout",
                reason="nested_layout",
                detail=detail,
                stage_chain=["preimport_nested:reject_nested"],
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
                cleanup_eligible=True,
            )

        # Preview measures; never decides. Mirror the measure-and-persist
        # pattern: collect facts via ``measure_preimport_state`` (no denylist
        # writes, no decision branches), then surface the five folder/audio-
        # integrity facts as a confident reject for the CLI/triage UI.
        # ``db=None`` / ``request_id=None``: HAVE spectral state belongs to the
        # persisted ``AlbumQualityEvidence`` row that the importer reads —
        # preview is a pure measurement surface.
        try:
            measurement = measure_preimport_state(
                path=preview_path,
                mb_release_id=mbid,
                label=_request_label(req),
                download_filetype=inspection.filetype,
                download_min_bitrate_bps=inspection.min_bitrate_bps,
                download_is_vbr=inspection.is_vbr,
                cfg=cfg,
                db=None,
                request_id=None,
                existing_spectral_evidence=(
                    existing_spectral_evidence
                ),
                preserve_existing_source_spectral=preserve_have_source,
                precomputed_inspection=inspection,
            )
            spectral_result = persist_exact_current_spectral_from_attempt(
                db,
                request_id=request_id,
                current_evidence=current_evidence,
                measured_existing=measurement.spectral_audit.existing,
                measured_existing_path=measurement.existing_spectral_path,
            )
            if spectral_result.evidence is not None:
                current_evidence = spectral_result.evidence
        except Exception as exc:
            return _measurement_failed_result(
                mode="path",
                reason="measurement_crashed",
                decision="measurement_crashed",
                detail=f"{type(exc).__name__}: {exc}",
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
            )

        # Four-fact reject (mirror worker-mode lines 517-522). ``nested_layout``
        # is already handled by the ``inspection.has_nested_audio`` branch
        # above; ``empty_fileset`` is handled by the ``not source_snapshot``
        # branch on the persist path. At this site only ``audio_corrupt`` and
        # ``bad_audio_hash`` can fire — but we check ``folder_layout``/
        # ``audio_file_count`` defensively so the measurement-derived facts
        # stay the single source of truth.
        audio_corrupt = measurement.audio_corrupt
        bad_audio_hash = measurement.matched_bad_hash_id is not None
        nested_layout = measurement.folder_layout == "nested"
        empty_fileset = measurement.audio_file_count == 0
        if audio_corrupt or bad_audio_hash or nested_layout or empty_fileset:
            scenario = (
                "audio_corrupt" if audio_corrupt
                else "bad_audio_hash" if bad_audio_hash
                else "nested_layout" if nested_layout
                else "empty_fileset"
            )
            detail: str | None = None
            if audio_corrupt:
                detail = measurement.audio_error
                if detail is None and measurement.corrupt_files:
                    detail = (
                        f"{len(measurement.corrupt_files)} files failed ffmpeg decode"
                    )
            elif bad_audio_hash and measurement.matched_bad_track_path:
                detail = (
                    f"matched bad_audio_hash id={measurement.matched_bad_hash_id} "
                    f"on track {measurement.matched_bad_track_path}"
                )
            return _preview_result(
                mode="path",
                verdict=PREVIEW_VERDICT_CONFIDENT_REJECT,
                decision=scenario,
                reason=scenario,
                detail=detail,
                stage_chain=[f"preimport:{scenario}"],
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
                cleanup_eligible=True,
            )

        spectral_failure = _lossless_candidate_spectral_failure(
            measurement,
            lossless_candidate=measurement.lossless_candidate,
        )
        if spectral_failure is not None:
            return _measurement_failed_result(
                mode="path",
                reason="measurement_crashed",
                decision="spectral_analysis_failed",
                detail=spectral_failure,
                request_id=request_id,
                download_log_id=download_log_id,
                source_path=path,
                import_result=ImportResult(spectral=measurement.spectral_audit),
            )

        existing_spectral = measurement.existing_spectral
        existing_grade = (
            existing_spectral.grade
            if existing_spectral is not None
            else existing_spectral_evidence.grade
        )
        existing_bitrate = (
            existing_spectral.bitrate_kbps
            if existing_spectral is not None
            else existing_spectral_evidence.bitrate_kbps
        )
        if current_evidence is not None:
            current_m = current_evidence.measurement
            existing_grade = current_m.spectral_grade
            existing_bitrate = current_m.spectral_bitrate_kbps
        override_min_bitrate = compute_effective_override_bitrate(
            (
                current_evidence.measurement.min_bitrate_kbps
                if current_evidence is not None
                else measurement.existing_min_bitrate
            ),
            existing_bitrate if isinstance(existing_bitrate, int) else None,
            existing_grade if isinstance(existing_grade, str) else None,
        )

        existing_v0_probe: V0ProbeEvidence | None = None
        if current_evidence is not None and current_evidence.v0_metric is not None:
            existing_v0_probe = audit_v0_probe_from_metric(
                current_evidence.v0_metric
            )

        preview_spectral_file = _write_preview_spectral_evidence_file(
            directory=temp_root or preview_path,
            mb_release_id=mbid,
            source_path=path,
            measurement=measurement,
            files=source_snapshot,
            lossless_candidate=measurement.lossless_candidate,
        )
        run = run_import_one(
            path=preview_path,
            mb_release_id=mbid,
            request_id=None,
            force=force,
            preserve_source=True,
            dry_run=True,
            override_min_bitrate=override_min_bitrate,
            target_format=req.get("target_format"),
            verified_lossless_target=cfg.verified_lossless_target,
            beets_harness_path=cfg.beets_harness_path,
            quality_rank_config_json=cfg.quality_ranks.to_json(),
            existing_v0_probe=existing_v0_probe,
            quality_evidence_action_file=preview_spectral_file,
        )
        if run.import_result is not None:
            run.import_result.spectral = compose_attempt_spectral_audit(
                measurement.spectral_audit,
                run.import_result.spectral,
            )
        verdict, cleanup_eligible, reason, chain = _classify_import_result(
            run.import_result,
            cfg=cfg.quality_ranks,
        )
        evidence_status: str | None = None
        evidence_reason: str | None = None
        if persist_candidate_evidence:
            if source_snapshot is None or not audio_snapshot_matches(path, source_snapshot):
                detail = "source files changed while preview was running"
                return _preview_result(
                    mode="path",
                    verdict=PREVIEW_VERDICT_UNCERTAIN,
                    decision="source_changed_during_preview",
                    reason="source_changed_during_preview",
                    detail=detail,
                    request_id=request_id,
                    download_log_id=download_log_id,
                    source_path=path,
                    import_result=run.import_result,
                )
            try:
                evidence = persist_candidate_evidence_from_import_result(
                    db,
                    mb_release_id=mbid,
                    source_path=path,
                    import_result=run.import_result,
                    download_log_id=download_log_id,
                    import_job_id=import_job_id,
                    files=source_snapshot,
                )
                evidence_status = evidence.status
                evidence_reason = evidence.reason
            except Exception as exc:
                evidence_status = "failed"
                evidence_reason = f"{type(exc).__name__}: {exc}"
            if evidence_status != "ready":
                return _preview_result(
                    mode="path",
                    verdict=PREVIEW_VERDICT_UNCERTAIN,
                    decision=f"evidence_{evidence_status}",
                    reason=f"evidence_{evidence_status}",
                    detail=evidence_reason,
                    request_id=request_id,
                    download_log_id=download_log_id,
                    source_path=path,
                    import_result=run.import_result,
                )
        final_decision = (
            run.import_result.decision if run.import_result else reason
        )
        final_detail = (
            run.import_result.error
            if run.import_result and run.import_result.error
            else evidence_reason
            if evidence_status in {"failed", "incomplete", "empty_fileset"}
            else "import_one.py emitted no JSON"
            if run.import_result is None
            else None
        )
        return _preview_result(
            mode="path",
            verdict=verdict,
            decision=final_decision,
            reason=reason,
            detail=final_detail,
            stage_chain=chain,
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=path,
            import_result=run.import_result,
            cleanup_eligible=cleanup_eligible,
        )
    finally:
        if temp_root is not None:
            _remove_preview_tree(temp_root)


def preview_import_from_download_log(
    db: ImportPreviewDB,
    download_log_id: int,
) -> ImportPreviewResult:
    """Preview the failed source referenced by one download_log row.

    Classify contract only (wrong-match triage, ad-hoc CLI inspection) —
    delegates to ``preview_import_from_path`` after resolving the row's
    ``failed_path``.
    """
    entry = db.get_download_log_entry(download_log_id)
    if not entry:
        return _preview_result(
            mode="download_log",
            verdict=PREVIEW_VERDICT_UNCERTAIN,
            decision="download_log_not_found",
            reason=f"Download log entry {download_log_id} not found",
            download_log_id=download_log_id,
        )
    # ``request_id`` is a required, non-nullable ``download_log`` column
    # (DownloadLogWithEvidenceRow), so the row type already proves this is
    # an ``int`` — no runtime narrowing needed once the row comes through
    # the typed projection.
    request_id_raw = entry["request_id"]
    vr = decode_validation_envelope(entry.get("validation_result"))
    raw_path = vr.failed_path
    if not raw_path:
        return _preview_result(
            mode="download_log",
            verdict=PREVIEW_VERDICT_UNCERTAIN,
            decision="missing_failed_path",
            reason="Download log row has no failed_path",
            request_id=request_id_raw,
            download_log_id=download_log_id,
        )
    from lib.config import read_runtime_config

    cfg = read_runtime_config()
    try:
        resolved = _authorized_failed_root(raw_path, cfg)
        snapshot = _snapshot_authorized_directory(resolved, cfg)
    except (FilesystemAuthorityError, OSError) as exc:
        return _preview_result(
            mode="download_log",
            verdict=PREVIEW_VERDICT_UNCERTAIN,
            decision="path_unauthorized",
            reason=f"Failed path is missing or unauthorized: {raw_path}",
            detail=str(exc),
            request_id=request_id_raw,
            download_log_id=download_log_id,
            source_path=raw_path,
        )
    try:
        return preview_import_from_path(
            db,
            request_id=request_id_raw,
            path=snapshot,
            force=True,
            download_log_id=download_log_id,
            _already_isolated=True,
        )
    finally:
        _remove_preview_tree(snapshot)
