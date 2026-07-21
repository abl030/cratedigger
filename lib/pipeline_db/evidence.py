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
                    spectral_subject, spectral_provenance,
                    verified_lossless, was_converted_from,
                    v0_min_bitrate_kbps, v0_avg_bitrate_kbps,
                    v0_median_bitrate_kbps, v0_subject,
                    v0_provenance,
                    on_disk_v0_research_attempted,
                    current_enrichment_required,
                    verified_lossless_provenance,
                    verified_lossless_source, verified_lossless_classifier,
                    verified_lossless_detail,
                    audio_corrupt, audio_error, folder_layout, audio_file_count,
                    filetype_band, matched_bad_audio_hash_id,
                    matched_bad_audio_hash_path,
                    updated_at
                )
                VALUES (
                    %s, %s, %s, -- identity + path
                    %s, %s, %s, -- measurement time + codec/container
                    %s, %s, %s, %s, -- storage/target + lineage
                    %s, %s, %s, %s, %s, -- bitrate/format/mode
                    %s, %s, %s, %s, %s, %s, -- spectral/lossless/conversion
                    %s, %s, %s, %s, %s, -- V0 metric
                    %s, -- on-disk V0 research attempted
                    %s, -- changed-current enrichment required
                    %s, %s, %s, %s, -- verified-lossless proof
                    %s, %s, %s, %s, %s, %s, %s, -- preview facts
                    NOW()
                )
                ON CONFLICT (mb_release_id, snapshot_fingerprint)
                DO UPDATE SET
                    -- A content-addressed row's source_path is the immutable
                    -- historical capture location. Current Beets location is
                    -- resolved separately at the point of use.
                    source_path = CASE
                        WHEN NULLIF(BTRIM(album_quality_evidence.source_path), '')
                            IS NULL
                        THEN EXCLUDED.source_path
                        ELSE album_quality_evidence.source_path
                    END,
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
                    -- Spectral is one atomic fact. A grade makes an incoming
                    -- pair valid (genuine legitimately has no bitrate); an
                    -- empty or bitrate-only v4 stale writer preserves the
                    -- whole stored pair so it cannot erase an attempt-time
                    -- scan. When the incoming row establishes lossless
                    -- lineage, however, an installed-subject stored tuple is
                    -- stale by definition and is cleared atomically. A
                    -- legacy row is replaced wholesale during its v4 rebuild,
                    -- including when the new fact is absent.
                    spectral_grade = CASE WHEN
                        album_quality_evidence.lineage_version < 4 OR
                        EXCLUDED.spectral_grade IS NOT NULL OR
                        (
                            album_quality_evidence.spectral_subject =
                                'installed' AND
                            (
                                EXCLUDED.v0_subject = 'source' OR
                                EXCLUDED.verified_lossless IS TRUE OR
                                LOWER(COALESCE(
                                    EXCLUDED.was_converted_from, ''))
                                    IN ('flac', 'alac', 'wav')
                            )
                        )
                        THEN EXCLUDED.spectral_grade
                        ELSE album_quality_evidence.spectral_grade END,
                    spectral_bitrate_kbps = CASE WHEN
                        album_quality_evidence.lineage_version < 4 OR
                        EXCLUDED.spectral_grade IS NOT NULL OR
                        (
                            album_quality_evidence.spectral_subject =
                                'installed' AND
                            (
                                EXCLUDED.v0_subject = 'source' OR
                                EXCLUDED.verified_lossless IS TRUE OR
                                LOWER(COALESCE(
                                    EXCLUDED.was_converted_from, ''))
                                    IN ('flac', 'alac', 'wav')
                            )
                        )
                        THEN EXCLUDED.spectral_bitrate_kbps
                        ELSE album_quality_evidence.spectral_bitrate_kbps END,
                    spectral_subject = CASE WHEN
                        album_quality_evidence.lineage_version < 4 OR
                        EXCLUDED.spectral_grade IS NOT NULL OR
                        (
                            album_quality_evidence.spectral_subject =
                                'installed' AND
                            (
                                EXCLUDED.v0_subject = 'source' OR
                                EXCLUDED.verified_lossless IS TRUE OR
                                LOWER(COALESCE(
                                    EXCLUDED.was_converted_from, ''))
                                    IN ('flac', 'alac', 'wav')
                            )
                        )
                        THEN EXCLUDED.spectral_subject
                        ELSE album_quality_evidence.spectral_subject END,
                    spectral_provenance = CASE WHEN
                        album_quality_evidence.lineage_version < 4 OR
                        EXCLUDED.spectral_grade IS NOT NULL OR
                        (
                            album_quality_evidence.spectral_subject =
                                'installed' AND
                            (
                                EXCLUDED.v0_subject = 'source' OR
                                EXCLUDED.verified_lossless IS TRUE OR
                                LOWER(COALESCE(
                                    EXCLUDED.was_converted_from, ''))
                                    IN ('flac', 'alac', 'wav')
                            )
                        )
                        THEN EXCLUDED.spectral_provenance
                        ELSE album_quality_evidence.spectral_provenance END,
                    verified_lossless = EXCLUDED.verified_lossless,
                    was_converted_from = EXCLUDED.was_converted_from,
                    -- V0 is one atomic fact, not six independently mergeable
                    -- columns. A valid incoming metric has a lineage and at
                    -- least one bitrate; replace the whole tuple in that case.
                    -- An absent or partial incoming tuple preserves the whole
                    -- stored tuple so v4 stale writers cannot mix or erase
                    -- it. Legacy rows are replaced wholesale during rebuild.
                    v0_min_bitrate_kbps = CASE WHEN
                        album_quality_evidence.lineage_version < 4 OR
                        (EXCLUDED.v0_subject IS NOT NULL AND
                         (EXCLUDED.v0_min_bitrate_kbps IS NOT NULL OR
                          EXCLUDED.v0_avg_bitrate_kbps IS NOT NULL OR
                          EXCLUDED.v0_median_bitrate_kbps IS NOT NULL))
                        THEN EXCLUDED.v0_min_bitrate_kbps
                        ELSE album_quality_evidence.v0_min_bitrate_kbps END,
                    v0_avg_bitrate_kbps = CASE WHEN
                        album_quality_evidence.lineage_version < 4 OR
                        (EXCLUDED.v0_subject IS NOT NULL AND
                         (EXCLUDED.v0_min_bitrate_kbps IS NOT NULL OR
                          EXCLUDED.v0_avg_bitrate_kbps IS NOT NULL OR
                          EXCLUDED.v0_median_bitrate_kbps IS NOT NULL))
                        THEN EXCLUDED.v0_avg_bitrate_kbps
                        ELSE album_quality_evidence.v0_avg_bitrate_kbps END,
                    v0_median_bitrate_kbps = CASE WHEN
                        album_quality_evidence.lineage_version < 4 OR
                        (EXCLUDED.v0_subject IS NOT NULL AND
                         (EXCLUDED.v0_min_bitrate_kbps IS NOT NULL OR
                          EXCLUDED.v0_avg_bitrate_kbps IS NOT NULL OR
                          EXCLUDED.v0_median_bitrate_kbps IS NOT NULL))
                        THEN EXCLUDED.v0_median_bitrate_kbps
                        ELSE album_quality_evidence.v0_median_bitrate_kbps END,
                    v0_subject = CASE WHEN
                        album_quality_evidence.lineage_version < 4 OR
                        (EXCLUDED.v0_subject IS NOT NULL AND
                         (EXCLUDED.v0_min_bitrate_kbps IS NOT NULL OR
                          EXCLUDED.v0_avg_bitrate_kbps IS NOT NULL OR
                          EXCLUDED.v0_median_bitrate_kbps IS NOT NULL))
                        THEN EXCLUDED.v0_subject
                        ELSE album_quality_evidence.v0_subject END,
                    v0_provenance = CASE WHEN
                        album_quality_evidence.lineage_version < 4 OR
                        (EXCLUDED.v0_subject IS NOT NULL AND
                         (EXCLUDED.v0_min_bitrate_kbps IS NOT NULL OR
                          EXCLUDED.v0_avg_bitrate_kbps IS NOT NULL OR
                          EXCLUDED.v0_median_bitrate_kbps IS NOT NULL))
                        THEN EXCLUDED.v0_provenance
                        ELSE album_quality_evidence.v0_provenance END,
                    on_disk_v0_research_attempted =
                        album_quality_evidence.on_disk_v0_research_attempted
                        OR EXCLUDED.on_disk_v0_research_attempted,
                    current_enrichment_required =
                        album_quality_evidence.current_enrichment_required
                        OR EXCLUDED.current_enrichment_required,
                    verified_lossless_provenance =
                        EXCLUDED.verified_lossless_provenance,
                    verified_lossless_source =
                        EXCLUDED.verified_lossless_source,
                    verified_lossless_classifier =
                        EXCLUDED.verified_lossless_classifier,
                    verified_lossless_detail =
                        EXCLUDED.verified_lossless_detail,
                    audio_corrupt = EXCLUDED.audio_corrupt,
                    audio_error = EXCLUDED.audio_error,
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
                m.spectral_subject,
                m.spectral_provenance,
                proof is not None,
                m.was_converted_from,
                v0.min_bitrate_kbps if v0 else None,
                v0.avg_bitrate_kbps if v0 else None,
                v0.median_bitrate_kbps if v0 else None,
                v0.subject if v0 else None,
                v0.provenance if v0 else None,
                evidence.on_disk_v0_research_attempted,
                evidence.current_enrichment_required,
                proof.provenance if proof else None,
                proof.source if proof else None,
                proof.classifier if proof else None,
                proof.detail if proof else None,
                evidence.audio_corrupt,
                evidence.audio_error,
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


    def claim_current_v0_research_attempt(
        self,
        *,
        request_id: int,
        expected_evidence_id: int,
        expected_snapshot_fingerprint: str,
    ) -> bool:
        """Atomically claim the once-only on-disk V0 encode.

        The attempted marker is the claim: it is committed before ffmpeg runs,
        so concurrent previews and a worker crash cannot encode the same
        content-addressed snapshot again. The request FK and evidence identity
        are checked in the same UPDATE that flips the marker.
        """
        cur = self._execute(
            """
            UPDATE album_quality_evidence AS evidence
            SET on_disk_v0_research_attempted = TRUE,
                updated_at = NOW()
            FROM album_requests AS request
            WHERE request.id = %s
              AND request.current_evidence_id = evidence.id
              AND evidence.id = %s
              AND evidence.snapshot_fingerprint = %s
              AND evidence.on_disk_v0_research_attempted = FALSE
              AND evidence.v0_min_bitrate_kbps IS NULL
              AND evidence.v0_avg_bitrate_kbps IS NULL
              AND evidence.v0_median_bitrate_kbps IS NULL
              AND evidence.v0_subject IS NULL
              AND evidence.v0_provenance IS NULL
            RETURNING evidence.id
            """,
            (
                int(request_id),
                int(expected_evidence_id),
                expected_snapshot_fingerprint,
            ),
        )
        claimed = cur.fetchone() is not None
        self.conn.commit()
        return claimed


    def persist_current_spectral_measurement(
        self,
        *,
        request_id: int,
        expected_evidence_id: int,
        expected_snapshot_fingerprint: str,
        grade: str,
        bitrate_kbps: int | None,
    ) -> bool:
        """Fill spectral fields on one exact, still-current empty snapshot."""
        cur = self._execute(
            """
            UPDATE album_quality_evidence AS evidence
            SET spectral_grade = %s,
                spectral_bitrate_kbps = %s,
                spectral_subject = 'installed',
                spectral_provenance = 'measured',
                updated_at = NOW()
            FROM album_requests AS request
            WHERE request.id = %s
              AND request.current_evidence_id = evidence.id
              AND evidence.id = %s
              AND evidence.snapshot_fingerprint = %s
              AND evidence.spectral_grade IS NULL
              AND evidence.spectral_bitrate_kbps IS NULL
            RETURNING evidence.id
            """,
            (
                grade,
                bitrate_kbps,
                int(request_id),
                int(expected_evidence_id),
                expected_snapshot_fingerprint,
            ),
        )
        persisted = cur.fetchone() is not None
        self.conn.commit()
        return persisted


    def persist_current_v0_research_metric(
        self,
        *,
        request_id: int,
        expected_evidence_id: int,
        expected_snapshot_fingerprint: str,
        metric: AlbumQualityV0Metric,
    ) -> bool:
        """Complete a claimed probe without widening its authority.

        Completion rechecks the exact current request FK and evidence content
        address atomically. It only fills a still-empty metric on an already
        claimed row, and never overwrites another producer's evidence.
        """
        cur = self._execute(
            """
            UPDATE album_quality_evidence AS evidence
            SET v0_min_bitrate_kbps = %s,
                v0_avg_bitrate_kbps = %s,
                v0_median_bitrate_kbps = %s,
                v0_subject = %s,
                v0_provenance = %s,
                updated_at = NOW()
            FROM album_requests AS request
            WHERE request.id = %s
              AND request.current_evidence_id = evidence.id
              AND evidence.id = %s
              AND evidence.snapshot_fingerprint = %s
              AND evidence.on_disk_v0_research_attempted = TRUE
              AND evidence.v0_min_bitrate_kbps IS NULL
              AND evidence.v0_avg_bitrate_kbps IS NULL
              AND evidence.v0_median_bitrate_kbps IS NULL
              AND evidence.v0_subject IS NULL
              AND evidence.v0_provenance IS NULL
            RETURNING evidence.id
            """,
            (
                metric.min_bitrate_kbps,
                metric.avg_bitrate_kbps,
                metric.median_bitrate_kbps,
                metric.subject,
                metric.provenance,
                int(request_id),
                int(expected_evidence_id),
                expected_snapshot_fingerprint,
            ),
        )
        persisted = cur.fetchone() is not None
        self.conn.commit()
        return persisted


    def release_current_v0_research_attempt(
        self,
        *,
        expected_evidence_id: int,
        expected_snapshot_fingerprint: str,
    ) -> bool:
        """Release a live claim when the post-probe snapshot became stale.

        A crash intentionally leaves the marker claimed (fail-soft and
        once-only). This release is only for a caller that survived the probe
        and proved its pre-probe evidence identity is no longer current.
        """
        cur = self._execute(
            """
            UPDATE album_quality_evidence
            SET on_disk_v0_research_attempted = FALSE,
                updated_at = NOW()
            WHERE id = %s
              AND snapshot_fingerprint = %s
              AND on_disk_v0_research_attempted = TRUE
              AND v0_min_bitrate_kbps IS NULL
              AND v0_avg_bitrate_kbps IS NULL
              AND v0_median_bitrate_kbps IS NULL
              AND v0_subject IS NULL
              AND v0_provenance IS NULL
            RETURNING id
            """,
            (int(expected_evidence_id), expected_snapshot_fingerprint),
        )
        released = cur.fetchone() is not None
        self.conn.commit()
        return released


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
        *,
        expected_status: str | None = None,
    ) -> bool:
        cur = self._execute(
            "UPDATE album_requests SET current_evidence_id = %s "
            "WHERE id = %s AND status != 'replaced' "
            "AND (%s IS NULL OR status = %s)",
            (
                evidence_id,
                int(request_id),
                expected_status,
                expected_status,
            ),
        )
        self.conn.commit()
        return cur.rowcount > 0


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
            or row.get("v0_subject") is not None
        ):
            v0_metric = AlbumQualityV0Metric(
                subject=row["v0_subject"],
                provenance=row["v0_provenance"],
                min_bitrate_kbps=row.get("v0_min_bitrate_kbps"),
                avg_bitrate_kbps=row.get("v0_avg_bitrate_kbps"),
                median_bitrate_kbps=row.get("v0_median_bitrate_kbps"),
            )
        proof = None
        if row.get("verified_lossless"):
            proof = VerifiedLosslessProof(
                provenance=row["verified_lossless_provenance"],
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
                spectral_subject=row.get("spectral_subject"),
                spectral_provenance=row.get("spectral_provenance"),
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
            on_disk_v0_research_attempted=bool(
                row.get("on_disk_v0_research_attempted", False)
            ),
            current_enrichment_required=bool(
                row["current_enrichment_required"]
            ),
            verified_lossless_proof=proof,
            audio_corrupt=bool(row.get("audio_corrupt", False)),
            audio_error=row.get("audio_error"),
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
