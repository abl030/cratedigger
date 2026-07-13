"""album_quality_evidence content-addressed keying + FK setters."""
import json
from typing import Any

from lib.quality import (
    AlbumQualityEvidence,
    AlbumQualityEvidenceFile,
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    VerifiedLosslessProof,
)

from lib.pipeline_db._core import _PipelineDBBase


class _EvidenceMixin(_PipelineDBBase):
    """album_quality_evidence content-addressed keying + FK setters."""


    # --- active album-quality evidence --------------------------------------

    def upsert_album_quality_evidence(
        self,
        evidence: AlbumQualityEvidence,
    ) -> None:
        """Atomically upsert evidence by ``(mb_release_id, snapshot_fingerprint)``.

        The row's surviving id can be fetched via
        :func:`find_album_quality_evidence`. Addressing FKs on
        ``import_jobs`` / ``download_log`` / ``album_requests`` are written
        separately via the dedicated setters.
        """
        evidence = evidence.sorted_for_storage()
        errors = evidence.storage_validation_errors()
        if errors:
            raise ValueError("; ".join(errors))

        v0 = evidence.v0_metric
        proof = evidence.verified_lossless_proof
        m = evidence.measurement
        file_rows = [
            {
                "ordinal": ordinal,
                "relative_path": file.relative_path,
                "size_bytes": file.size_bytes,
                "mtime_ns": file.mtime_ns,
                "extension": file.extension,
                "container": file.container,
                "codec": file.codec,
                "decode_ok": file.decode_ok,
            }
            for ordinal, file in enumerate(evidence.files)
        ]
        self._execute(
            """
            WITH upserted AS (
                INSERT INTO album_quality_evidence (
                    mb_release_id, snapshot_fingerprint, source_path,
                    measured_at, codec, container,
                    storage_format, target_format, target_is_cbr,
                    lineage_version,
                    min_bitrate_kbps,
                    avg_bitrate_kbps, median_bitrate_kbps, format, is_cbr,
                    spectral_grade, spectral_bitrate_kbps,
                    verified_lossless, was_converted_from,
                    v0_min_bitrate_kbps, v0_avg_bitrate_kbps,
                    v0_median_bitrate_kbps, v0_source_lineage,
                    v0_source_provenance, v0_proof_provenance,
                    verified_lossless_proof_origin,
                    verified_lossless_source, verified_lossless_classifier,
                    verified_lossless_detail,
                    audio_corrupt, folder_layout, audio_file_count,
                    filetype_band, matched_bad_audio_hash_id,
                    matched_bad_audio_hash_path,
                    updated_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    NOW()
                )
                ON CONFLICT (mb_release_id, snapshot_fingerprint)
                DO UPDATE SET
                    source_path = EXCLUDED.source_path,
                    measured_at = EXCLUDED.measured_at,
                    codec = EXCLUDED.codec,
                    container = EXCLUDED.container,
                    storage_format = EXCLUDED.storage_format,
                    target_format = EXCLUDED.target_format,
                    target_is_cbr = EXCLUDED.target_is_cbr,
                    lineage_version = EXCLUDED.lineage_version,
                    min_bitrate_kbps = EXCLUDED.min_bitrate_kbps,
                    avg_bitrate_kbps = EXCLUDED.avg_bitrate_kbps,
                    median_bitrate_kbps = EXCLUDED.median_bitrate_kbps,
                    format = EXCLUDED.format,
                    is_cbr = EXCLUDED.is_cbr,
                    spectral_grade = EXCLUDED.spectral_grade,
                    spectral_bitrate_kbps = EXCLUDED.spectral_bitrate_kbps,
                    verified_lossless = EXCLUDED.verified_lossless,
                    was_converted_from = EXCLUDED.was_converted_from,
                    v0_min_bitrate_kbps = EXCLUDED.v0_min_bitrate_kbps,
                    v0_avg_bitrate_kbps = EXCLUDED.v0_avg_bitrate_kbps,
                    v0_median_bitrate_kbps = EXCLUDED.v0_median_bitrate_kbps,
                    v0_source_lineage = EXCLUDED.v0_source_lineage,
                    v0_source_provenance = EXCLUDED.v0_source_provenance,
                    v0_proof_provenance = EXCLUDED.v0_proof_provenance,
                    verified_lossless_proof_origin =
                        EXCLUDED.verified_lossless_proof_origin,
                    verified_lossless_source =
                        EXCLUDED.verified_lossless_source,
                    verified_lossless_classifier =
                        EXCLUDED.verified_lossless_classifier,
                    verified_lossless_detail =
                        EXCLUDED.verified_lossless_detail,
                    audio_corrupt = EXCLUDED.audio_corrupt,
                    folder_layout = EXCLUDED.folder_layout,
                    audio_file_count = EXCLUDED.audio_file_count,
                    filetype_band = EXCLUDED.filetype_band,
                    matched_bad_audio_hash_id = EXCLUDED.matched_bad_audio_hash_id,
                    matched_bad_audio_hash_path =
                        EXCLUDED.matched_bad_audio_hash_path,
                    updated_at = NOW()
                RETURNING id
            ),
            deleted AS (
                DELETE FROM album_quality_evidence_files
                WHERE evidence_id = (SELECT id FROM upserted)
                RETURNING 1
            ),
            delete_complete AS (
                SELECT COUNT(*) AS ignored FROM deleted
            ),
            file_rows AS (
                SELECT *
                FROM jsonb_to_recordset(%s::jsonb) AS row(
                    ordinal INTEGER,
                    relative_path TEXT,
                    size_bytes BIGINT,
                    mtime_ns BIGINT,
                    extension TEXT,
                    container TEXT,
                    codec TEXT,
                    decode_ok BOOLEAN
                )
            )
            INSERT INTO album_quality_evidence_files (
                evidence_id, ordinal, relative_path, size_bytes, mtime_ns,
                extension, container, codec, decode_ok
            )
            SELECT upserted.id, file_rows.ordinal, file_rows.relative_path,
                   file_rows.size_bytes, file_rows.mtime_ns,
                   file_rows.extension, file_rows.container, file_rows.codec,
                   COALESCE(file_rows.decode_ok, TRUE)
            FROM upserted
            CROSS JOIN delete_complete
            CROSS JOIN file_rows
            """,
            (
                evidence.mb_release_id,
                evidence.snapshot_fingerprint,
                evidence.source_path,
                evidence.measured_at,
                evidence.codec,
                evidence.container,
                evidence.storage_format,
                evidence.target_format,
                evidence.target_is_cbr,
                evidence.lineage_version,
                m.min_bitrate_kbps,
                m.avg_bitrate_kbps,
                m.median_bitrate_kbps,
                m.format,
                m.is_cbr,
                m.spectral_grade,
                m.spectral_bitrate_kbps,
                m.verified_lossless,
                m.was_converted_from,
                v0.min_bitrate_kbps if v0 else None,
                v0.avg_bitrate_kbps if v0 else None,
                v0.median_bitrate_kbps if v0 else None,
                v0.source_lineage if v0 else None,
                v0.source_provenance if v0 else None,
                v0.proof_provenance if v0 else None,
                proof.proof_origin if proof else None,
                proof.source if proof else None,
                proof.classifier if proof else None,
                proof.detail if proof else None,
                evidence.audio_corrupt,
                evidence.folder_layout,
                evidence.audio_file_count,
                evidence.filetype_band,
                evidence.matched_bad_audio_hash_id,
                evidence.matched_bad_audio_hash_path,
                json.dumps(file_rows),
            ),
        )


    def load_album_quality_evidence_by_id(
        self,
        evidence_id: int | None,
    ) -> AlbumQualityEvidence | None:
        """Load evidence by surrogate id (the addressing-FK target)."""
        if evidence_id is None:
            return None
        cur = self._execute(
            "SELECT * FROM album_quality_evidence WHERE id = %s",
            (int(evidence_id),),
        )
        row = cur.fetchone()
        if row is None:
            return None
        files_cur = self._execute(
            """
            SELECT relative_path, size_bytes, mtime_ns, extension, container,
                   codec, decode_ok
            FROM album_quality_evidence_files
            WHERE evidence_id = %s
            ORDER BY relative_path
            """,
            (int(row["id"]),),
        )
        file_rows = [dict(r) for r in files_cur.fetchall()]
        return self._album_quality_evidence_from_row(dict(row), file_rows)


    def find_album_quality_evidence(
        self,
        *,
        mb_release_id: str,
        snapshot_fingerprint: str,
    ) -> AlbumQualityEvidence | None:
        """Find evidence by its content-addressed key."""
        cur = self._execute(
            """
            SELECT * FROM album_quality_evidence
            WHERE mb_release_id = %s AND snapshot_fingerprint = %s
            """,
            (mb_release_id, snapshot_fingerprint),
        )
        row = cur.fetchone()
        if row is None:
            return None
        files_cur = self._execute(
            """
            SELECT relative_path, size_bytes, mtime_ns, extension, container,
                   codec, decode_ok
            FROM album_quality_evidence_files
            WHERE evidence_id = %s
            ORDER BY relative_path
            """,
            (int(row["id"]),),
        )
        file_rows = [dict(r) for r in files_cur.fetchall()]
        return self._album_quality_evidence_from_row(dict(row), file_rows)


    def set_import_job_candidate_evidence(
        self,
        import_job_id: int,
        evidence_id: int | None,
    ) -> None:
        self._execute(
            "UPDATE import_jobs SET candidate_evidence_id = %s WHERE id = %s",
            (evidence_id, int(import_job_id)),
        )
        self.conn.commit()


    def set_download_log_candidate_evidence(
        self,
        download_log_id: int,
        evidence_id: int | None,
    ) -> None:
        self._execute(
            "UPDATE download_log SET candidate_evidence_id = %s WHERE id = %s",
            (evidence_id, int(download_log_id)),
        )
        self.conn.commit()


    def set_request_current_evidence(
        self,
        request_id: int,
        evidence_id: int | None,
    ) -> None:
        self._execute(
            "UPDATE album_requests SET current_evidence_id = %s WHERE id = %s",
            (evidence_id, int(request_id)),
        )
        self.conn.commit()


    def get_import_job_candidate_evidence_id(
        self,
        import_job_id: int,
    ) -> int | None:
        cur = self._execute(
            "SELECT candidate_evidence_id FROM import_jobs WHERE id = %s",
            (int(import_job_id),),
        )
        row = cur.fetchone()
        if row is None or row["candidate_evidence_id"] is None:
            return None
        return int(row["candidate_evidence_id"])


    def get_download_log_candidate_evidence_id(
        self,
        download_log_id: int,
    ) -> int | None:
        cur = self._execute(
            "SELECT candidate_evidence_id FROM download_log WHERE id = %s",
            (int(download_log_id),),
        )
        row = cur.fetchone()
        if row is None or row["candidate_evidence_id"] is None:
            return None
        return int(row["candidate_evidence_id"])


    def get_request_current_evidence_id(
        self,
        request_id: int,
    ) -> int | None:
        cur = self._execute(
            "SELECT current_evidence_id FROM album_requests WHERE id = %s",
            (int(request_id),),
        )
        row = cur.fetchone()
        if row is None or row["current_evidence_id"] is None:
            return None
        return int(row["current_evidence_id"])


    def _album_quality_evidence_from_row(
        self,
        row: dict[str, Any],
        file_rows: list[dict[str, Any]],
    ) -> AlbumQualityEvidence:
        v0_metric = None
        if (
            row.get("v0_min_bitrate_kbps") is not None
            or row.get("v0_avg_bitrate_kbps") is not None
            or row.get("v0_median_bitrate_kbps") is not None
            or row.get("v0_source_lineage") is not None
        ):
            v0_metric = AlbumQualityV0Metric(
                min_bitrate_kbps=row.get("v0_min_bitrate_kbps"),
                avg_bitrate_kbps=row.get("v0_avg_bitrate_kbps"),
                median_bitrate_kbps=row.get("v0_median_bitrate_kbps"),
                source_lineage=row.get("v0_source_lineage"),
                source_provenance=row.get("v0_source_provenance"),
                proof_provenance=row.get("v0_proof_provenance"),
            )
        proof = None
        if row.get("verified_lossless"):
            proof = VerifiedLosslessProof(
                proof_origin=row["verified_lossless_proof_origin"],
                source=row["verified_lossless_source"],
                classifier=row["verified_lossless_classifier"],
                detail=row.get("verified_lossless_detail"),
            )
        return AlbumQualityEvidence(
            mb_release_id=row["mb_release_id"],
            snapshot_fingerprint=row["snapshot_fingerprint"],
            source_path=row.get("source_path") or "",
            id=int(row["id"]) if row.get("id") is not None else None,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=row.get("min_bitrate_kbps"),
                avg_bitrate_kbps=row.get("avg_bitrate_kbps"),
                median_bitrate_kbps=row.get("median_bitrate_kbps"),
                format=row.get("format"),
                is_cbr=bool(row.get("is_cbr")),
                spectral_grade=row.get("spectral_grade"),
                spectral_bitrate_kbps=row.get("spectral_bitrate_kbps"),
                verified_lossless=bool(row.get("verified_lossless")),
                was_converted_from=row.get("was_converted_from"),
            ),
            measured_at=row["measured_at"],
            files=[
                AlbumQualityEvidenceFile(
                    relative_path=file["relative_path"],
                    size_bytes=int(file["size_bytes"]),
                    mtime_ns=int(file["mtime_ns"]),
                    extension=file["extension"],
                    container=file["container"],
                    codec=file.get("codec"),
                    decode_ok=bool(file["decode_ok"]) if "decode_ok" in file else True,
                )
                for file in file_rows
            ],
            codec=row.get("codec"),
            container=row.get("container"),
            storage_format=row.get("storage_format"),
            target_format=row.get("target_format"),
            target_is_cbr=row.get("target_is_cbr"),
            lineage_version=int(row.get("lineage_version") or 1),
            v0_metric=v0_metric,
            verified_lossless_proof=proof,
            audio_corrupt=bool(row.get("audio_corrupt", False)),
            folder_layout=row.get("folder_layout") or "flat",
            audio_file_count=int(row.get("audio_file_count") or 0),
            filetype_band=row.get("filetype_band") or "",
            matched_bad_audio_hash_id=(
                int(row["matched_bad_audio_hash_id"])
                if row.get("matched_bad_audio_hash_id") is not None
                else None
            ),
            matched_bad_audio_hash_path=row.get("matched_bad_audio_hash_path"),
        )
