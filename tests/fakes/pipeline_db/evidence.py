"""FakePipelineDB evidence cluster — mirrors ``lib/pipeline_db/evidence.py``.

Content-addressed ``album_quality_evidence`` rows.
"""
from __future__ import annotations

import copy
from collections.abc import (
    Sequence,
)

import msgspec

from lib.import_execution import (
    ExecutionLeaseSnapshot,
)
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
)
from lib.quality import (
    EVIDENCE_PROVENANCE_MEASURED,
    EVIDENCE_SUBJECT_INSTALLED,
    AlbumQualityEvidence,
    AlbumQualityV0Metric,
    CodecFamily,
)
from lib.quality_evidence import (
    SpectralWriteIntent,
    current_evidence_preserves_source_spectral,
    snapshot_fingerprint,
)
from tests.fakes._shared import _utcnow
from tests.fakes.pipeline_db._base import _FakePipelineDBBase


class _FakeEvidenceMixin(_FakePipelineDBBase):
    """Content-addressed ``album_quality_evidence`` rows."""

    @staticmethod
    def _assert_album_quality_evidence_constraints(
        evidence: AlbumQualityEvidence,
    ) -> None:
        # Migration 073 retires the old database-only subject/lineage check:
        # conversion lineage persists across fresh installed measurements.
        # R19 reuse is instead the exact manifest-aware application predicate.
        del evidence

    def _store_album_quality_evidence(
        self,
        evidence: AlbumQualityEvidence,
    ) -> None:
        """Mirror PostgreSQL constraints at every fake evidence write."""
        self._assert_album_quality_evidence_constraints(evidence)
        evidence_id = evidence.id
        if evidence_id is None:
            raise ValueError("stored album quality evidence requires an id")
        stored = copy.deepcopy(evidence)
        key = (stored.mb_release_id, stored.snapshot_fingerprint)
        self.album_quality_evidence[key] = stored
        self._evidence_by_id[evidence_id] = stored

    def upsert_album_quality_evidence(
        self,
        evidence: AlbumQualityEvidence,
        *,
        spectral_write_intent: SpectralWriteIntent = "merge",
    ) -> None:
        evidence = evidence.sorted_for_storage()
        if spectral_write_intent not in {"merge", "replace"}:
            raise ValueError(
                f"invalid spectral write intent: {spectral_write_intent!r}"
            )
        derived_fingerprint = snapshot_fingerprint(evidence.files)
        if evidence.snapshot_fingerprint != derived_fingerprint:
            raise ValueError(
                "snapshot_fingerprint does not match the persisted file inventory"
            )
        errors = evidence.storage_validation_errors()
        if errors:
            raise ValueError("; ".join(errors))
        key = (evidence.mb_release_id, evidence.snapshot_fingerprint)
        existing = self.album_quality_evidence.get(key)
        incoming_preserves_source_spectral = (
            current_evidence_preserves_source_spectral(evidence)
        )
        current_owned = (
            existing is not None
            and any(
                request.get("current_evidence_id") == existing.id
                for request in self._requests.values()
            )
        )
        protect_current_source_spectral = (
            spectral_write_intent == "replace"
            and current_owned
            and existing is not None
            and current_evidence_preserves_source_spectral(existing)
        )
        if (
            existing is not None
            and evidence.measurement.was_converted_from is None
            and existing.measurement.was_converted_from is not None
            and any(
                request.get("current_evidence_id") == existing.id
                for request in self._requests.values()
            )
        ):
            evidence = msgspec.structs.replace(
                evidence,
                measurement=msgspec.structs.replace(
                    evidence.measurement,
                    was_converted_from=(
                        existing.measurement.was_converted_from
                    ),
                ),
            )
        # Spectral is an atomic pair. A stale writer without a grade cannot
        # erase a successful attempt-time scan on the same audio snapshot.
        # R19 is the exception: only an exact, known-lossy derivative clears
        # a stored installed-subject tuple. Provenance alone is not enough.
        #
        # This condition mirrors the real SQL's CASE guard exactly (issue
        # #829 Phase 5 PR1 review round 2, should-fix 7) — it does NOT
        # additionally require ``existing.measurement.spectral_grade is not
        # None``. The SQL's ELSE (preserve-stored) branch fires whenever
        # ``lineage_version >= 4 AND EXCLUDED.spectral_grade IS NULL AND NOT
        # exception``, regardless of what the STORED grade already was; an
        # earlier draft of this fake added that extra precondition, which a
        # previous version of this comment claimed (wrongly) was already an
        # exact mirror.
        if (
            existing is not None
            and existing.lineage_version >= 4
            and (
                protect_current_source_spectral
                or (
                    evidence.measurement.spectral_grade is None
                    and not (
                        spectral_write_intent == "replace"
                        and not current_owned
                    )
                    and not (
                        incoming_preserves_source_spectral
                        and existing.measurement.spectral_subject
                            == EVIDENCE_SUBJECT_INSTALLED
                    )
                )
            )
        ):
            # cliff_hz/codec_family/ultrasonic_deficit_db/
            # spectral_measurement_version are measured in the same pass as
            # spectral_grade, so they preserve under the exact same guard.
            evidence = msgspec.structs.replace(
                evidence,
                measurement=msgspec.structs.replace(
                    evidence.measurement,
                    spectral_grade=existing.measurement.spectral_grade,
                    spectral_bitrate_kbps=(
                        existing.measurement.spectral_bitrate_kbps
                    ),
                    spectral_subject=existing.measurement.spectral_subject,
                    spectral_provenance=(
                        existing.measurement.spectral_provenance
                    ),
                    cliff_hz=existing.measurement.cliff_hz,
                    codec_family=existing.measurement.codec_family,
                    ultrasonic_deficit_db=(
                        existing.measurement.ultrasonic_deficit_db
                    ),
                    spectral_measurement_version=(
                        existing.measurement.spectral_measurement_version
                    ),
                ),
            )
        # V0 is an atomic tuple. A stale writer with no metric preserves the
        # whole stored fact; a valid incoming metric replaces it wholesale.
        if (
            existing is not None
            and existing.lineage_version >= 4
            and existing.v0_metric is not None
            and evidence.v0_metric is None
        ):
            evidence = msgspec.structs.replace(
                evidence,
                v0_metric=existing.v0_metric,
            )
        # The AAC lattice capture (issue #829 PR-A) follows the V0 tuple's
        # guard, not the spectral one: an incoming row without a lattice
        # preserves the stored one wholesale. Mirrors the real SQL's
        # ``EXCLUDED.aac_lattice_tracks IS NOT NULL`` CASE exactly.
        if (
            existing is not None
            and existing.aac_lattice is not None
            and evidence.aac_lattice is None
        ):
            evidence = msgspec.structs.replace(
                evidence,
                aac_lattice=existing.aac_lattice,
            )
        if (
            existing is not None
            and existing.on_disk_v0_research_attempted
            and not evidence.on_disk_v0_research_attempted
        ):
            evidence = msgspec.structs.replace(
                evidence,
                on_disk_v0_research_attempted=True,
            )
        if (
            existing is not None
            and existing.current_enrichment_required
            and not evidence.current_enrichment_required
        ):
            evidence = msgspec.structs.replace(
                evidence,
                current_enrichment_required=True,
            )
        if (
            existing is not None
            and existing.source_path.strip()
            and evidence.source_path != existing.source_path
        ):
            evidence = msgspec.structs.replace(
                evidence,
                source_path=existing.source_path,
            )
        if (
            existing is not None
            and evidence.audio_validation.outcome
                in {"legacy_unrecorded", "skipped"}
            and existing.audio_validation.outcome
                not in {"legacy_unrecorded", "skipped"}
        ):
            existing_decode_by_path = {
                file.relative_path: file.decode_ok
                for file in existing.files
            }
            evidence = msgspec.structs.replace(
                evidence,
                audio_validation=existing.audio_validation,
                audio_corrupt=existing.audio_corrupt,
                audio_error=existing.audio_error,
                files=[
                    msgspec.structs.replace(
                        file,
                        decode_ok=existing_decode_by_path.get(
                            file.relative_path,
                            file.decode_ok,
                        ),
                    )
                    for file in evidence.files
                ],
            )
        if existing is not None and existing.id is not None:
            evidence_id = existing.id
        else:
            self._next_evidence_id += 1
            evidence_id = self._next_evidence_id
        self._store_album_quality_evidence(
            msgspec.structs.replace(evidence, id=evidence_id)
        )

    def load_album_quality_evidence_by_id(
        self,
        evidence_id: int | None,
    ) -> AlbumQualityEvidence | None:
        if evidence_id is None:
            return None
        evidence = self._evidence_by_id.get(int(evidence_id))
        return copy.deepcopy(evidence) if evidence is not None else None

    def find_album_quality_evidence(
        self,
        *,
        mb_release_id: str,
        snapshot_fingerprint: str,
    ) -> AlbumQualityEvidence | None:
        evidence = self.album_quality_evidence.get(
            (mb_release_id, snapshot_fingerprint)
        )
        return copy.deepcopy(evidence) if evidence is not None else None

    def claim_current_v0_research_attempt(
        self,
        *,
        request_id: int,
        expected_evidence_id: int,
        expected_snapshot_fingerprint: str,
    ) -> bool:
        request = self._requests.get(int(request_id))
        evidence = self._evidence_by_id.get(int(expected_evidence_id))
        if (
            request is None
            or request.get("current_evidence_id") != int(expected_evidence_id)
            or evidence is None
            or evidence.snapshot_fingerprint != expected_snapshot_fingerprint
            or evidence.v0_metric is not None
            or evidence.on_disk_v0_research_attempted
        ):
            return False
        claimed = msgspec.structs.replace(
            evidence,
            on_disk_v0_research_attempted=True,
        )
        self._store_album_quality_evidence(claimed)
        return True

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
    ) -> bool:
        request = self._requests.get(int(request_id))
        evidence = self._evidence_by_id.get(int(expected_evidence_id))
        if (
            request is None
            or request.get("current_evidence_id") != int(expected_evidence_id)
            or evidence is None
            or evidence.snapshot_fingerprint != expected_snapshot_fingerprint
        ):
            return False
        # Fresh-audit-wins (issue #815): overwrite ANY disagreeing persisted
        # spectral with the fresh measured installed-subject audit. The old
        # fill-only-if-NULL guard is gone; mirrors the production SQL. Output
        # conversion lineage deliberately remains durable across this write.
        # The four capture facts (issue #829 phase 5) travel with the grade
        # as one atomic fact, mirroring the production SQL column list.
        measurement = msgspec.structs.replace(
            evidence.measurement,
            spectral_grade=grade,
            spectral_bitrate_kbps=bitrate_kbps,
            spectral_subject=EVIDENCE_SUBJECT_INSTALLED,
            spectral_provenance=EVIDENCE_PROVENANCE_MEASURED,
            cliff_hz=cliff_hz,
            codec_family=codec_family,
            ultrasonic_deficit_db=ultrasonic_deficit_db,
            spectral_measurement_version=spectral_measurement_version,
        )
        completed = msgspec.structs.replace(
            evidence,
            measurement=measurement,
        )
        self._store_album_quality_evidence(completed)
        return True

    def persist_current_v0_research_metric(
        self,
        *,
        request_id: int,
        expected_evidence_id: int,
        expected_snapshot_fingerprint: str,
        metric: AlbumQualityV0Metric,
    ) -> bool:
        request = self._requests.get(int(request_id))
        evidence = self._evidence_by_id.get(int(expected_evidence_id))
        if (
            request is None
            or request.get("current_evidence_id") != int(expected_evidence_id)
            or evidence is None
            or evidence.snapshot_fingerprint != expected_snapshot_fingerprint
            or not evidence.on_disk_v0_research_attempted
            or evidence.v0_metric is not None
        ):
            return False
        completed = msgspec.structs.replace(
            evidence,
            v0_metric=metric,
        )
        self._store_album_quality_evidence(completed)
        return True

    def release_current_v0_research_attempt(
        self,
        *,
        expected_evidence_id: int,
        expected_snapshot_fingerprint: str,
    ) -> bool:
        evidence = self._evidence_by_id.get(int(expected_evidence_id))
        if (
            evidence is None
            or evidence.snapshot_fingerprint != expected_snapshot_fingerprint
            or not evidence.on_disk_v0_research_attempted
            or evidence.v0_metric is not None
        ):
            return False
        released = msgspec.structs.replace(
            evidence,
            on_disk_v0_research_attempted=False,
        )
        self._store_album_quality_evidence(released)
        return True

    def set_import_job_candidate_evidence(
        self,
        import_job_id: int,
        evidence_id: int | None,
        *,
        expected_execution_lease: ExecutionLeaseSnapshot | None = None,
    ) -> bool:
        if self._live_beets_child_refuses(expected_execution_lease):
            return False
        for row in self._import_jobs:
            if row.get("id") == import_job_id:
                if row.get("job_type") == IMPORT_JOB_AUTOMATION and (
                    row.get("status") != "queued"
                    or row.get("preview_status") != "running"
                    or not self._automation_job_has_authority(row)
                    or not self._execution_lease_matches(
                        row,
                        expected_execution_lease,
                        include_child=True,
                    )
                    or expected_execution_lease is None
                ):
                    return False
                row["candidate_evidence_id"] = evidence_id
                row["updated_at"] = _utcnow()
                return True
        return False

    def set_download_log_candidate_evidence(
        self,
        download_log_id: int,
        evidence_id: int | None,
        *,
        direct_attribution: bool = False,
        contributor_usernames: Sequence[str] | None = None,
    ) -> None:
        from lib.convergence_service import normalize_contributor_usernames

        for row in self.download_logs:
            if row.id == download_log_id:
                row.candidate_evidence_id = evidence_id
                normalized = list(normalize_contributor_usernames(
                    contributor_usernames or (),
                )) or None
                if normalized is not None:
                    row.candidate_contributor_usernames = normalized
                row.candidate_evidence_direct = bool(
                    direct_attribution
                    and evidence_id is not None
                    and row.candidate_contributor_usernames
                )
                return

    def set_request_current_evidence(
        self,
        request_id: int,
        evidence_id: int | None,
        *,
        expected_status: str | None = None,
    ) -> bool:
        row = self._requests.get(request_id)
        if (
            row is not None
            and row.get("status") != "replaced"
            and (
                expected_status is None
                or row.get("status") == expected_status
            )
        ):
            row["current_evidence_id"] = evidence_id
            row["updated_at"] = _utcnow()
            return True
        return False

    def get_import_job_candidate_evidence_id(
        self,
        import_job_id: int,
    ) -> int | None:
        for row in self._import_jobs:
            if row.get("id") == import_job_id:
                val = row.get("candidate_evidence_id")
                return int(val) if val is not None else None
        return None

    def get_download_log_candidate_evidence_id(
        self,
        download_log_id: int,
    ) -> int | None:
        for row in self.download_logs:
            if row.id == download_log_id:
                return (
                    int(row.candidate_evidence_id)
                    if row.candidate_evidence_id is not None
                    else None
                )
        return None

    def get_latest_download_log_candidate_evidence_id(
        self,
        request_id: int,
    ) -> int | None:
        candidate_ids = [
            (row.id, row.candidate_evidence_id)
            for row in self.download_logs
            if row.request_id == request_id
            and row.candidate_evidence_id is not None
        ]
        if not candidate_ids:
            return None
        candidate_ids.sort(key=lambda pair: pair[0], reverse=True)
        return int(candidate_ids[0][1])

    def get_request_current_evidence_id(
        self,
        request_id: int,
    ) -> int | None:
        row = self._requests.get(request_id)
        if row is None:
            return None
        val = row.get("current_evidence_id")
        return int(val) if val is not None else None

