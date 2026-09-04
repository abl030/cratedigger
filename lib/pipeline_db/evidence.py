"""album_quality_evidence content-addressed keying + FK setters."""

import json
from collections.abc import Mapping, Sequence
from datetime import datetime

import msgspec

from lib.convergence_service import normalize_contributor_usernames
from lib.evidence_media_identity import (
    EVIDENCE_LOSSLESS_CODECS,
    LOSSY_CODECS_BY_CONTAINER,
)
from lib.import_execution import ExecutionLeaseSnapshot
from lib.pipeline_db._core import _PipelineDBBase
from lib.quality import (
    EVIDENCE_PROVENANCE_MEASURED,
    EVIDENCE_SUBJECT_INSTALLED,
    EVIDENCE_SUBJECT_SOURCE,
    AacLatticeCapture,
    AacLatticeTrackScore,
    AlbumQualityEvidence,
    AlbumQualityEvidenceFile,
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    AudioValidationReport,
    CdRipBitVerification,
    CodecFamily,
    EvidenceProvenance,
    EvidenceSubject,
    VerifiedLosslessProof,
    cd_rip_proof_pair_validation_errors,
)
from lib.quality_evidence import (
    SpectralWriteIntent,
    current_evidence_preserves_source_spectral,
    snapshot_fingerprint,
)

_PRESERVED_SOURCE_LOSSY_PAIRS_JSON = json.dumps([
    {"container": container, "codec": codec}
    for container, codecs in sorted(LOSSY_CODECS_BY_CONTAINER.items())
    for codec in sorted(codecs)
])

# The single source of truth for "does this write replace the stored
# spectral tuple with the incoming one?", spliced into all eight spectral
# column CASE expressions below (issue #1355 WE1). This MUST stay a
# correlated expression referencing the live conflicting row directly
# (``album_quality_evidence.<col>``, ``EXCLUDED.<col>``) rather than a
# value precomputed in an earlier CTE: ``INSERT ... ON CONFLICT DO UPDATE``
# evaluates its SET expressions against the row it just found and locked,
# which can be NEWER than the snapshot an ordinary read-only CTE in the
# same WITH list sees if a concurrent transaction committed the same
# ``(mb_release_id, snapshot_fingerprint)`` between this statement's
# snapshot and its conflict detection. An early CTE reading
# ``album_quality_evidence`` by that same key would silently see nothing
# in exactly that race and every column would fall through to "preserve",
# which is not what production has ever done. Verified empirically: a
# throwaway two-session probe against a real server showed a CTE-sourced
# reference stays stuck on the pre-commit (empty) snapshot after the
# blocking transaction commits, while a direct ``target_table.col``
# reference inside the same ``DO UPDATE SET`` correctly resolves to the
# just-committed row. Splicing one Python string into eight positions
# keeps a policy change to one edit site without paying for that safety
# with a second (Python-computed) SQL twin of the same policy.
_SPECTRAL_TUPLE_USE_INCOMING_SQL = """(
                        NOT (
                            (SELECT replace_spectral FROM write_policy)
                            AND EXISTS (
                                SELECT 1
                                FROM preserved_current_source_spectral AS preserved
                                WHERE preserved.id = album_quality_evidence.id
                            )
                        )
                        AND (
                            album_quality_evidence.lineage_version < 4
                            OR EXCLUDED.spectral_grade IS NOT NULL
                            OR (
                                (SELECT replace_spectral FROM write_policy)
                                AND NOT EXISTS (
                                    SELECT 1 FROM album_requests AS current_owner
                                    WHERE current_owner.current_evidence_id =
                                        album_quality_evidence.id
                                )
                            )
                            OR (
                                album_quality_evidence.spectral_subject =
                                    (SELECT subject_installed FROM write_policy)
                                AND (
                                    SELECT incoming_preserves_source_spectral
                                    FROM write_policy
                                )
                            )
                        )
                    )"""


class PersistedEvidenceFileRow(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    """The exact file projection consumed by the production evidence decoder."""

    relative_path: str
    size_bytes: int
    mtime_ns: int
    extension: str
    container: str
    codec: str | None
    decode_ok: bool


class PersistedAlbumQualityEvidenceRow(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True
):
    """The strict PostgreSQL projection consumed by evidence decoding.

    This is the one shared contract for production reads and corpus export.
    Cursor adapters reject missing and extra columns before conversion.
    """

    id: int
    mb_release_id: str
    snapshot_fingerprint: str
    source_path: str
    measured_at: datetime
    min_bitrate_kbps: int | None
    avg_bitrate_kbps: int | None
    median_bitrate_kbps: int | None
    format: str | None
    is_cbr: bool
    spectral_grade: str | None
    spectral_bitrate_kbps: int | None
    spectral_subject: EvidenceSubject | None
    spectral_provenance: EvidenceProvenance | None
    was_converted_from: str | None
    cliff_hz: int | None
    codec_family: CodecFamily | None
    ultrasonic_deficit_db: float | None
    spectral_measurement_version: int | None
    codec: str | None
    container: str | None
    storage_format: str | None
    target_format: str | None
    target_is_cbr: bool | None
    lineage_version: int
    v0_min_bitrate_kbps: int | None
    v0_avg_bitrate_kbps: int | None
    v0_median_bitrate_kbps: int | None
    v0_subject: EvidenceSubject | None
    v0_provenance: EvidenceProvenance | None
    on_disk_v0_research_attempted: bool
    current_enrichment_required: bool
    verified_lossless: bool
    verified_lossless_provenance: EvidenceProvenance | None
    verified_lossless_source: str | None
    verified_lossless_classifier: str | None
    verified_lossless_detail: str | None
    cd_rip_verification: CdRipBitVerification | None
    audio_validation: AudioValidationReport
    audio_corrupt: bool
    audio_error: str | None
    folder_layout: str
    audio_file_count: int
    filetype_band: str
    matched_bad_audio_hash_id: int | None
    matched_bad_audio_hash_path: str | None
    aac_lattice_tracks: list[AacLatticeTrackScore] | None
    aac_lattice_modal_offset: int | None
    aac_lattice_modal_count: int | None
    aac_lattice_scored_tracks: int | None
    aac_lattice_max_z: float | None


EVIDENCE_PROJECTION_COLUMNS: tuple[str, ...] = (
    PersistedAlbumQualityEvidenceRow.__struct_fields__
)
EVIDENCE_FILE_PROJECTION_COLUMNS: tuple[str, ...] = (
    PersistedEvidenceFileRow.__struct_fields__
)

# Keep the read SQL visible to structural mutation audits. The strict
# PersistedAlbumQualityEvidenceRow conversion below fails closed if this
# literal projection ever drifts from the typed contract above.
_EVIDENCE_PROJECTION_SQL = (
    "id, mb_release_id, snapshot_fingerprint, source_path, measured_at, "
    "min_bitrate_kbps, avg_bitrate_kbps, median_bitrate_kbps, format, is_cbr, "
    "spectral_grade, spectral_bitrate_kbps, spectral_subject, "
    "spectral_provenance, was_converted_from, cliff_hz, codec_family, "
    "ultrasonic_deficit_db, spectral_measurement_version, codec, container, "
    "storage_format, target_format, target_is_cbr, lineage_version, "
    "v0_min_bitrate_kbps, v0_avg_bitrate_kbps, v0_median_bitrate_kbps, "
    "v0_subject, v0_provenance, on_disk_v0_research_attempted, "
    "current_enrichment_required, verified_lossless, "
    "verified_lossless_provenance, verified_lossless_source, "
    "verified_lossless_classifier, verified_lossless_detail, "
    "cd_rip_verification, audio_validation, audio_corrupt, audio_error, "
    "folder_layout, audio_file_count, filetype_band, "
    "matched_bad_audio_hash_id, matched_bad_audio_hash_path, "
    "aac_lattice_tracks, aac_lattice_modal_offset, aac_lattice_modal_count, "
    "aac_lattice_scored_tracks, aac_lattice_max_z"
)


def _strict_pg_row[
    PersistedRow: (PersistedAlbumQualityEvidenceRow, PersistedEvidenceFileRow)
](
    value: Mapping[str, object],
    typ: type[PersistedRow],
) -> PersistedRow:
    """Reject projection drift and return its exact typed row."""
    fields = set(typ.__struct_fields__)
    if set(value) != fields:
        raise msgspec.ValidationError(
            f"projection columns drifted: missing={sorted(fields - set(value))}, "
            f"extra={sorted(set(value) - fields)}"
        )
    return msgspec.convert(value, type=typ)


def _typed_evidence_rows_from_pg(
    row: Mapping[str, object],
    file_rows: Sequence[Mapping[str, object]],
) -> tuple[PersistedAlbumQualityEvidenceRow, list[PersistedEvidenceFileRow]]:
    """Decode the exact PostgreSQL projection before evidence semantics.

    This is shared by the two live load paths and the corpus replayer's
    static boundary. Every caller receives canonical Structs; semantic code
    below never receives a cursor dict or a JSONB builtin tree.
    """
    return (
        _strict_pg_row(row, PersistedAlbumQualityEvidenceRow),
        [
            _strict_pg_row(file_row, PersistedEvidenceFileRow)
            for file_row in file_rows
        ],
    )


class _EvidenceMixin(_PipelineDBBase):
    """album_quality_evidence content-addressed keying + FK setters."""

    # --- active album-quality evidence --------------------------------------

    def upsert_album_quality_evidence(
        self,
        evidence: AlbumQualityEvidence,
        *,
        spectral_write_intent: SpectralWriteIntent = "merge",
    ) -> None:
        """Atomically upsert evidence by ``(mb_release_id, snapshot_fingerprint)``.

        The row's surviving id can be fetched via
        :func:`find_album_quality_evidence`. Addressing FKs on
        ``import_jobs`` / ``download_log`` / ``album_requests`` are written
        separately via the dedicated setters.
        """
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

        v0 = evidence.v0_metric
        proof = evidence.verified_lossless_proof
        lattice = evidence.aac_lattice
        m = evidence.measurement
        audio_validation_json = msgspec.json.encode(
            evidence.audio_validation,
        ).decode()
        aac_lattice_tracks_json = (
            msgspec.json.encode(lattice.tracks).decode()
            if lattice is not None
            else None
        )
        cd_rip_verification_json = (
            msgspec.json.encode(evidence.cd_rip_verification).decode()
            if evidence.cd_rip_verification is not None else None
        )
        preserve_existing_audio_validation = evidence.audio_validation.outcome in {
            "legacy_unrecorded",
            "skipped",
        }
        # Keep the PostgreSQL same-address merge aligned with the policy's
        # single, manifest-aware R19 predicate. Provenance alone (V0/proof)
        # must never erase a fresh installed-subject measurement.
        incoming_preserves_source_spectral = (
            current_evidence_preserves_source_spectral(evidence)
        )
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
            WITH write_policy AS MATERIALIZED (
                SELECT
                    %s::boolean AS replace_spectral,
                    %s::text[] AS lossless_source_codecs,
                    %s::jsonb AS lossy_media_pairs,
                    %s::text AS subject_source,
                    %s::text AS subject_installed,
                    %s::boolean AS incoming_preserves_source_spectral
            ),
            existing_row AS MATERIALIZED (
                -- Feeds preserved_current_source_spectral's own R19 shape
                -- check below ONLY. This is a plain snapshot-scoped read,
                -- same as preserved_current_source_spectral's identity
                -- lookup always was before this refactor -- unlike the
                -- eight spectral columns' write decision (see
                -- _SPECTRAL_TUPLE_USE_INCOMING_SQL above this method),
                -- gate A's shape predicate has never needed to react to a
                -- same-key writer that commits after this statement's own
                -- snapshot, so hoisting its lookup into an ordinary CTE
                -- changes nothing about its race profile.
                SELECT
                    stored.id,
                    stored.spectral_subject,
                    stored.was_converted_from,
                    EXISTS (
                        SELECT 1
                        FROM album_requests AS current_owner
                        WHERE current_owner.current_evidence_id = stored.id
                    ) AS is_current_owned
                FROM album_quality_evidence AS stored
                WHERE stored.mb_release_id = %s
                  AND stored.snapshot_fingerprint = %s
            ),
            preserved_current_source_spectral AS MATERIALIZED (
                -- This is the stored-row SQL twin of
                -- current_evidence_preserves_source_spectral. Current
                -- ownership and a source subject are not enough: only an
                -- irreplaceable derivative from a recorded lossless source
                -- may retain its tuple across a candidate collision.
                SELECT existing_row.id
                FROM existing_row
                JOIN album_quality_evidence AS stored
                    ON stored.id = existing_row.id
                CROSS JOIN write_policy
                WHERE existing_row.is_current_owned
                  AND LOWER(BTRIM(existing_row.was_converted_from)) = ANY(
                      write_policy.lossless_source_codecs
                  )
                  AND existing_row.spectral_subject =
                      write_policy.subject_source
                  AND EXISTS (
                      SELECT 1
                      FROM album_quality_evidence_files AS stored_file
                      WHERE stored_file.evidence_id = stored.id
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM album_quality_evidence_files AS stored_file
                      WHERE stored_file.evidence_id = stored.id
                        AND (
                            stored_file.container IS NULL OR
                            stored_file.container = '' OR
                            stored_file.codec IS NULL OR
                            stored_file.codec = ''
                        )
                  )
                  AND 1 = (
                      SELECT COUNT(DISTINCT LOWER(stored_file.container))
                      FROM album_quality_evidence_files AS stored_file
                      WHERE stored_file.evidence_id = stored.id
                  )
                  AND COALESCE(
                      NULLIF(LOWER(BTRIM(stored.storage_format)), ''),
                      NULLIF(LOWER(BTRIM(stored.format)), '')
                  ) IS NOT NULL
                  AND (
                      NULLIF(LOWER(BTRIM(stored.storage_format)), '') IS NULL OR
                      NULLIF(LOWER(BTRIM(stored.format)), '') IS NULL OR
                      LOWER(BTRIM(stored.storage_format)) =
                          LOWER(BTRIM(stored.format))
                  )
                  AND EXISTS (
                      SELECT 1
                      FROM jsonb_to_recordset(
                          write_policy.lossy_media_pairs
                      ) AS pair(container TEXT, codec TEXT)
                      WHERE pair.container = (
                          SELECT MIN(LOWER(stored_file.container))
                          FROM album_quality_evidence_files AS stored_file
                          WHERE stored_file.evidence_id = stored.id
                      )
                        AND pair.codec = COALESCE(
                            NULLIF(LOWER(BTRIM(stored.storage_format)), ''),
                            NULLIF(LOWER(BTRIM(stored.format)), '')
                        )
                  )
            ),
            upserted AS (
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
                    cliff_hz, codec_family, ultrasonic_deficit_db,
                    spectral_measurement_version,
                    v0_min_bitrate_kbps, v0_avg_bitrate_kbps,
                    v0_median_bitrate_kbps, v0_subject,
                    v0_provenance,
                    on_disk_v0_research_attempted,
                    current_enrichment_required,
                    verified_lossless_provenance,
                    verified_lossless_source, verified_lossless_classifier,
                    verified_lossless_detail,
                    cd_rip_verification,
                    audio_corrupt, audio_error, audio_validation,
                    folder_layout, audio_file_count,
                    filetype_band, matched_bad_audio_hash_id,
                    matched_bad_audio_hash_path,
                    aac_lattice_tracks, aac_lattice_modal_offset,
                    aac_lattice_modal_count, aac_lattice_scored_tracks,
                    aac_lattice_max_z,
                    updated_at
                )
                VALUES (
                    %s, %s, %s, -- identity + path
                    %s, %s, %s, -- measurement time + codec/container
                    %s, %s, %s, %s, -- storage/target + lineage
                    %s, %s, %s, %s, %s, -- bitrate/format/mode
                    %s, %s, %s, %s, %s, %s, -- spectral/lossless/conversion
                    %s, %s, %s, %s, -- spectral capture facts (issue #829 PR1)
                    %s, %s, %s, %s, %s, -- V0 metric
                    %s, -- on-disk V0 research attempted
                    %s, -- changed-current enrichment required
                    %s, %s, %s, %s, -- verified-lossless proof
                    %s, -- positive CD rip bit verification
                    %s, %s, %s, %s, %s, %s, %s, %s, -- preview facts
                    %s, %s, %s, %s, %s, -- AAC lattice capture (issue #829 PR-A)
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
                    -- Spectral is one atomic fact: every column below
                    -- shares the exact same decision, spliced in from the
                    -- single _SPECTRAL_TUPLE_USE_INCOMING_SQL constant
                    -- above this method instead of being hand-copied once
                    -- per column. A grade makes an incoming pair valid
                    -- (genuine legitimately has no bitrate); an empty or
                    -- bitrate-only v4 stale writer preserves the whole
                    -- stored pair so it cannot erase an attempt-time scan;
                    -- the R19 exception clears a stale installed-subject
                    -- tuple when the incoming row is the exact
                    -- irreplaceable lossy derivative it describes; and a
                    -- legacy row is replaced wholesale during its v4
                    -- rebuild, including when the new fact is absent.
                    -- issue #829 Phase 5 PR1: cliff_hz/codec_family/
                    -- ultrasonic_deficit_db/spectral_measurement_version are
                    -- measured in the SAME pass as spectral_grade, so they
                    -- share the exact same decision — one atomic fact,
                    -- eight columns wide.
                    spectral_grade = CASE
                        WHEN """ + _SPECTRAL_TUPLE_USE_INCOMING_SQL + """
                        THEN EXCLUDED.spectral_grade
                        ELSE album_quality_evidence.spectral_grade
                    END,
                    spectral_bitrate_kbps = CASE
                        WHEN """ + _SPECTRAL_TUPLE_USE_INCOMING_SQL + """
                        THEN EXCLUDED.spectral_bitrate_kbps
                        ELSE album_quality_evidence.spectral_bitrate_kbps
                    END,
                    spectral_subject = CASE
                        WHEN """ + _SPECTRAL_TUPLE_USE_INCOMING_SQL + """
                        THEN EXCLUDED.spectral_subject
                        ELSE album_quality_evidence.spectral_subject
                    END,
                    spectral_provenance = CASE
                        WHEN """ + _SPECTRAL_TUPLE_USE_INCOMING_SQL + """
                        THEN EXCLUDED.spectral_provenance
                        ELSE album_quality_evidence.spectral_provenance
                    END,
                    cliff_hz = CASE
                        WHEN """ + _SPECTRAL_TUPLE_USE_INCOMING_SQL + """
                        THEN EXCLUDED.cliff_hz
                        ELSE album_quality_evidence.cliff_hz
                    END,
                    codec_family = CASE
                        WHEN """ + _SPECTRAL_TUPLE_USE_INCOMING_SQL + """
                        THEN EXCLUDED.codec_family
                        ELSE album_quality_evidence.codec_family
                    END,
                    ultrasonic_deficit_db = CASE
                        WHEN """ + _SPECTRAL_TUPLE_USE_INCOMING_SQL + """
                        THEN EXCLUDED.ultrasonic_deficit_db
                        ELSE album_quality_evidence.ultrasonic_deficit_db
                    END,
                    spectral_measurement_version = CASE
                        WHEN """ + _SPECTRAL_TUPLE_USE_INCOMING_SQL + """
                        THEN EXCLUDED.spectral_measurement_version
                        ELSE
                            album_quality_evidence.spectral_measurement_version
                    END,
                    verified_lossless = CASE
                        WHEN EXCLUDED.cd_rip_verification IS NULL
                             AND album_quality_evidence.cd_rip_verification
                                 IS NOT NULL
                        THEN album_quality_evidence.verified_lossless
                        ELSE EXCLUDED.verified_lossless
                    END,
                    -- Candidate and current FKs may share this canonical row.
                    -- A candidate NULL clears unowned legacy contamination,
                    -- but cannot erase lineage while the row is installed
                    -- current evidence. Candidate readers project the shared
                    -- row back to source semantics before policy or launch.
                    was_converted_from = CASE
                        WHEN EXCLUDED.was_converted_from IS NULL
                             AND EXISTS (
                                 SELECT 1
                                 FROM album_requests AS current_owner
                                 WHERE current_owner.current_evidence_id =
                                     album_quality_evidence.id
                             )
                        THEN album_quality_evidence.was_converted_from
                        ELSE EXCLUDED.was_converted_from
                    END,
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
                    verified_lossless_provenance = CASE
                        WHEN EXCLUDED.cd_rip_verification IS NULL
                             AND album_quality_evidence.cd_rip_verification
                                 IS NOT NULL
                        THEN album_quality_evidence.verified_lossless_provenance
                        ELSE EXCLUDED.verified_lossless_provenance END,
                    verified_lossless_source = CASE
                        WHEN EXCLUDED.cd_rip_verification IS NULL
                             AND album_quality_evidence.cd_rip_verification
                                 IS NOT NULL
                        THEN album_quality_evidence.verified_lossless_source
                        ELSE EXCLUDED.verified_lossless_source END,
                    verified_lossless_classifier = CASE
                        WHEN EXCLUDED.cd_rip_verification IS NULL
                             AND album_quality_evidence.cd_rip_verification
                                 IS NOT NULL
                        THEN album_quality_evidence.verified_lossless_classifier
                        ELSE EXCLUDED.verified_lossless_classifier END,
                    verified_lossless_detail = CASE
                        WHEN EXCLUDED.cd_rip_verification IS NULL
                             AND album_quality_evidence.cd_rip_verification
                                 IS NOT NULL
                        THEN album_quality_evidence.verified_lossless_detail
                        ELSE EXCLUDED.verified_lossless_detail END,
                    cd_rip_verification = COALESCE(
                        EXCLUDED.cd_rip_verification,
                        album_quality_evidence.cd_rip_verification
                    ),
                    audio_validation = CASE
                        WHEN EXCLUDED.audio_validation->>'outcome' IN (
                            'legacy_unrecorded', 'skipped'
                        )
                        AND album_quality_evidence.audio_validation->>'outcome'
                            NOT IN ('legacy_unrecorded', 'skipped')
                        THEN album_quality_evidence.audio_validation
                        ELSE EXCLUDED.audio_validation
                    END,
                    audio_corrupt = CASE
                        WHEN EXCLUDED.audio_validation->>'outcome' IN (
                            'legacy_unrecorded', 'skipped'
                        )
                        AND album_quality_evidence.audio_validation->>'outcome'
                            NOT IN ('legacy_unrecorded', 'skipped')
                        THEN album_quality_evidence.audio_corrupt
                        ELSE EXCLUDED.audio_corrupt
                    END,
                    audio_error = CASE
                        WHEN EXCLUDED.audio_validation->>'outcome' IN (
                            'legacy_unrecorded', 'skipped'
                        )
                        AND album_quality_evidence.audio_validation->>'outcome'
                            NOT IN ('legacy_unrecorded', 'skipped')
                        THEN album_quality_evidence.audio_error
                        ELSE EXCLUDED.audio_error
                    END,
                    folder_layout = EXCLUDED.folder_layout,
                    audio_file_count = EXCLUDED.audio_file_count,
                    filetype_band = EXCLUDED.filetype_band,
                    matched_bad_audio_hash_id = EXCLUDED.matched_bad_audio_hash_id,
                    matched_bad_audio_hash_path =
                        EXCLUDED.matched_bad_audio_hash_path,
                    -- issue #829 AAC-lattice leg PR-A: the lattice is one
                    -- atomic fact across five columns, and an expensive
                    -- once-per-content measurement (tens of seconds of CPU
                    -- per track), so it follows the V0 tuple's guard rather
                    -- than the spectral one: a writer that carries no lattice
                    -- preserves the stored one wholesale, and a writer that
                    -- carries one replaces it wholesale. The measurement is
                    -- gated on the spectral grade, so a same-snapshot
                    -- re-persist from a path that did not run the gate must
                    -- not erase what an earlier pass measured on the exact
                    -- same bytes.
                    aac_lattice_tracks = CASE
                        WHEN EXCLUDED.aac_lattice_tracks IS NOT NULL
                        THEN EXCLUDED.aac_lattice_tracks
                        ELSE album_quality_evidence.aac_lattice_tracks END,
                    aac_lattice_modal_offset = CASE
                        WHEN EXCLUDED.aac_lattice_tracks IS NOT NULL
                        THEN EXCLUDED.aac_lattice_modal_offset
                        ELSE album_quality_evidence.aac_lattice_modal_offset END,
                    aac_lattice_modal_count = CASE
                        WHEN EXCLUDED.aac_lattice_tracks IS NOT NULL
                        THEN EXCLUDED.aac_lattice_modal_count
                        ELSE album_quality_evidence.aac_lattice_modal_count END,
                    aac_lattice_scored_tracks = CASE
                        WHEN EXCLUDED.aac_lattice_tracks IS NOT NULL
                        THEN EXCLUDED.aac_lattice_scored_tracks
                        ELSE album_quality_evidence.aac_lattice_scored_tracks END,
                    aac_lattice_max_z = CASE
                        WHEN EXCLUDED.aac_lattice_tracks IS NOT NULL
                        THEN EXCLUDED.aac_lattice_max_z
                        ELSE album_quality_evidence.aac_lattice_max_z END,
                    updated_at = NOW()
                RETURNING id, %s::boolean AS preserve_existing_audio_validation
            ),
            preserved_file_rows AS MATERIALIZED (
                SELECT files.relative_path, files.decode_ok
                FROM album_quality_evidence_files files
                JOIN upserted ON upserted.id = files.evidence_id
            ),
            deleted AS (
                DELETE FROM album_quality_evidence_files
                WHERE evidence_id = (SELECT id FROM upserted)
                  AND (
                    SELECT COUNT(*) FROM preserved_file_rows
                  ) >= 0
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
                   CASE
                       WHEN upserted.preserve_existing_audio_validation
                       THEN COALESCE(
                           preserved_file_rows.decode_ok,
                           file_rows.decode_ok,
                           TRUE
                       )
                       ELSE COALESCE(file_rows.decode_ok, TRUE)
                   END
            FROM upserted
            CROSS JOIN delete_complete
            CROSS JOIN file_rows
            LEFT JOIN preserved_file_rows
              ON preserved_file_rows.relative_path = file_rows.relative_path
            -- Issue #1355 Batch E (E1): a concurrent writer of the exact
            -- same content address can lose the row-level race on
            -- ``upserted`` (it blocks on the winner's uncommitted INSERT,
            -- then resumes) while this DELETE ran against its own
            -- pre-block snapshot and therefore never saw the winner's
            -- already-committed file rows. Its own plain INSERT above
            -- then collided with them. The two writers share a content
            -- address, so relative_path/size_bytes/extension/container/
            -- codec are identical by construction (they are exactly what
            -- the fingerprint hashes). ordinal is not hashed but is still
            -- identical: both writers sort by relative_path first
            -- (``evidence.sorted_for_storage()``), so the same path set
            -- enumerates to the same ordinal on both sides -- which is
            -- what keeps the sibling ``UNIQUE (evidence_id, ordinal)``
            -- constraint from ever firing independently of this one.
            -- mtime_ns and decode_ok are excluded from that hash by
            -- design and can legitimately differ, and whichever writer
            -- won is already committed correctly. DO NOTHING keeps that
            -- committed row exactly as it is rather than risking an
            -- update computed from this writer's own stale
            -- preserved_file_rows read -- see
            -- test_concurrent_same_address_upsert_keeps_the_winners_decode_ok
            -- for why a same-looking DO UPDATE would be wrong.
            ON CONFLICT (evidence_id, relative_path) DO NOTHING
            """,
            (
                spectral_write_intent == "replace",
                sorted(EVIDENCE_LOSSLESS_CODECS),
                _PRESERVED_SOURCE_LOSSY_PAIRS_JSON,
                EVIDENCE_SUBJECT_SOURCE,
                EVIDENCE_SUBJECT_INSTALLED,
                incoming_preserves_source_spectral,
                evidence.mb_release_id,
                evidence.snapshot_fingerprint,
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
                m.cliff_hz,
                m.codec_family,
                m.ultrasonic_deficit_db,
                m.spectral_measurement_version,
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
                cd_rip_verification_json,
                evidence.audio_corrupt,
                evidence.audio_error,
                audio_validation_json,
                evidence.folder_layout,
                evidence.audio_file_count,
                evidence.filetype_band,
                evidence.matched_bad_audio_hash_id,
                evidence.matched_bad_audio_hash_path,
                aac_lattice_tracks_json,
                lattice.modal_offset if lattice is not None else None,
                lattice.modal_count if lattice is not None else None,
                lattice.scored_tracks if lattice is not None else None,
                lattice.max_z if lattice is not None else None,
                preserve_existing_audio_validation,
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
            "SELECT "
            + _EVIDENCE_PROJECTION_SQL
            + " FROM album_quality_evidence WHERE id = %s",
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
        persisted, persisted_files = _typed_evidence_rows_from_pg(
            row,
            files_cur.fetchall(),
        )
        return self._album_quality_evidence_from_persisted_rows(
            persisted,
            persisted_files,
        )

    def find_album_quality_evidence(
        self,
        *,
        mb_release_id: str,
        snapshot_fingerprint: str,
    ) -> AlbumQualityEvidence | None:
        """Find evidence by its content-addressed key."""
        cur = self._execute(
            "SELECT "
            + _EVIDENCE_PROJECTION_SQL
            + " FROM album_quality_evidence "
            "WHERE mb_release_id = %s AND snapshot_fingerprint = %s",
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
        persisted, persisted_files = _typed_evidence_rows_from_pg(
            row,
            files_cur.fetchall(),
        )
        return self._album_quality_evidence_from_persisted_rows(
            persisted,
            persisted_files,
        )

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
        cliff_hz: int | None = None,
        codec_family: CodecFamily | None = None,
        ultrasonic_deficit_db: float | None = None,
        spectral_measurement_version: int | None = None,
    ) -> bool:
        """Persist a fresh measured installed-subject spectral on one exact snapshot.

        Fresh-audit-wins (issue #815): the caller only reaches here with a
        successful fresh audit of the exact matched-fingerprint bytes, so this
        re-persists ``grade``/``bitrate`` as ``installed``/``measured`` over ANY
        disagreeing persisted value on the still-current row — the old
        fill-only-if-NULL guard let a stale legacy grade survive a fresh scan.
        ``was_converted_from`` is durable output lineage, not the spectral
        subject. A fresh installed measurement therefore leaves it intact;
        R19's reuse predicate independently permits carried source spectral
        facts only for an exact known-lossy derivative.

        The four measured capture facts (issue #829 phase 5) are one atomic
        fact with ``grade`` — every writer of ``spectral_grade`` carries them
        together, so a fresh re-audit here never strands a stale capture
        fact behind a fresh grade.
        """
        cur = self._execute(
            """
            UPDATE album_quality_evidence AS evidence
            SET spectral_grade = %s,
                spectral_bitrate_kbps = %s,
                spectral_subject = %s,
                spectral_provenance = %s,
                cliff_hz = %s,
                codec_family = %s,
                ultrasonic_deficit_db = %s,
                spectral_measurement_version = %s,
                updated_at = NOW()
            FROM album_requests AS request
            WHERE request.id = %s
              AND request.current_evidence_id = evidence.id
              AND evidence.id = %s
              AND evidence.snapshot_fingerprint = %s
            RETURNING evidence.id
            """,
            (
                grade,
                bitrate_kbps,
                EVIDENCE_SUBJECT_INSTALLED,
                EVIDENCE_PROVENANCE_MEASURED,
                cliff_hz,
                codec_family,
                ultrasonic_deficit_db,
                spectral_measurement_version,
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
        *,
        expected_execution_lease: ExecutionLeaseSnapshot | None = None,
    ) -> bool:
        """Bind preview evidence only while the exact producer owns the job."""
        lease = expected_execution_lease
        if lease is not None and lease.beets is not None:
            return False
        cur = self._execute(
            """
            UPDATE import_jobs AS job
            SET candidate_evidence_id = %s,
                updated_at = NOW()
            WHERE job.id = %s
              AND (
                  job.job_type <> 'automation_import'
                  OR (
                      %s IS NOT NULL
                      AND job.status = 'queued'
                      AND job.preview_status = 'running'
                      AND EXISTS (
                          SELECT 1
                          FROM album_requests AS request
                          WHERE request.id = job.request_id
                            AND request.status = 'processing'
                            AND request.active_automation_import_job_id = job.id
                      )
                      AND job.execution_invocation_id = %s
                      AND job.execution_host_boot_id = %s
                      AND job.execution_systemd_unit = %s
                      AND job.execution_worker_pid = %s
                      AND job.execution_worker_start_ticks = %s
                      AND job.execution_beets_pid IS NULL
                      AND job.execution_beets_start_ticks IS NULL
                  )
              )
            RETURNING job.id
            """,
            (
                evidence_id,
                int(import_job_id),
                lease.invocation_id if lease is not None else None,
                lease.invocation_id if lease is not None else None,
                lease.host_boot_id if lease is not None else None,
                lease.systemd_unit if lease is not None else None,
                lease.worker.pid if lease is not None else None,
                lease.worker.start_ticks if lease is not None else None,
            ),
        )
        persisted = cur.fetchone() is not None
        self.conn.commit()
        return persisted

    def set_download_log_candidate_evidence(
        self,
        download_log_id: int,
        evidence_id: int | None,
        *,
        direct_attribution: bool = False,
        contributor_usernames: Sequence[str] | None = None,
    ) -> None:
        """Address evidence and positively mark only an exact producer link.

        Historical/render-only cross-walk callers use the default false.
        Callers may pass true only while holding the exact attempt/evidence
        relationship. The row must also have a structured contributor set;
        a missing evidence id or contributor identity clears the positive bit.
        """
        normalized_contributors = list(normalize_contributor_usernames(
            contributor_usernames or (),
        )) or None
        self._execute(
            "UPDATE download_log "
            "SET candidate_evidence_id = %s, "
            "candidate_contributor_usernames = COALESCE("
            "%s::TEXT[], candidate_contributor_usernames), "
            "candidate_evidence_direct = ("
            "%s AND %s IS NOT NULL AND COALESCE(CARDINALITY(COALESCE("
            "%s::TEXT[], candidate_contributor_usernames)), 0) > 0) "
            "WHERE id = %s",
            (
                evidence_id,
                normalized_contributors,
                bool(direct_attribution),
                evidence_id,
                normalized_contributors,
                int(download_log_id),
            ),
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

    def get_latest_download_log_candidate_evidence_id(
        self,
        request_id: int,
    ) -> int | None:
        """Return the newest download_log candidate evidence id for this
        request, or None if no download attempt left candidate evidence.

        Candidate evidence is addressed per-attempt (``download_log_id``),
        not per-request — this is the one place that walks attempt history
        to find "the request's last candidate", for diagnostics
        (``pipeline-cli quality <id>``'s live-candidate replay tier, issue
        #813). Production dispatch never needs this: it always already
        knows the exact ``download_log_id``/``import_job_id`` in flight.
        """
        cur = self._execute(
            """
            SELECT candidate_evidence_id
            FROM download_log
            WHERE request_id = %s
              AND candidate_evidence_id IS NOT NULL
            ORDER BY id DESC
            LIMIT 1
        """,
            (int(request_id),),
        )
        row = cur.fetchone()
        return int(row["candidate_evidence_id"]) if row else None

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

    @staticmethod
    def _album_quality_evidence_from_row(
        row: Mapping[str, object],
        file_rows: Sequence[Mapping[str, object]],
    ) -> AlbumQualityEvidence:
        """Decode raw cursor-shaped evidence for corpus replay compatibility.

        Production loads call :meth:`_album_quality_evidence_from_persisted_rows`
        after this same strict raw-PG adapter. This static spelling remains the
        corpus replayer's entrypoint, so replay and production share the exact
        decoder without a second semantic mapper.
        """
        persisted, persisted_files = _typed_evidence_rows_from_pg(
            row,
            file_rows,
        )
        return _EvidenceMixin._album_quality_evidence_from_persisted_rows(
            persisted,
            persisted_files,
        )

    @staticmethod
    def _album_quality_evidence_from_persisted_rows(
        persisted: PersistedAlbumQualityEvidenceRow,
        persisted_files: Sequence[PersistedEvidenceFileRow],
    ) -> AlbumQualityEvidence:
        """Build semantic evidence from the already-strict persisted rows."""
        v0_metric = None
        if (
            persisted.v0_min_bitrate_kbps is not None
            or persisted.v0_avg_bitrate_kbps is not None
            or persisted.v0_median_bitrate_kbps is not None
            or persisted.v0_subject is not None
        ):
            assert persisted.v0_subject is not None
            assert persisted.v0_provenance is not None
            v0_metric = AlbumQualityV0Metric(
                subject=persisted.v0_subject,
                provenance=persisted.v0_provenance,
                min_bitrate_kbps=persisted.v0_min_bitrate_kbps,
                avg_bitrate_kbps=persisted.v0_avg_bitrate_kbps,
                median_bitrate_kbps=persisted.v0_median_bitrate_kbps,
            )
        aac_lattice = None
        if persisted.aac_lattice_tracks is not None:
            if persisted.aac_lattice_scored_tracks is None:
                raise msgspec.ValidationError(
                    "aac lattice tracks require scored_tracks",
                )
            aac_lattice = AacLatticeCapture(
                tracks=persisted.aac_lattice_tracks,
                modal_offset=persisted.aac_lattice_modal_offset,
                modal_count=persisted.aac_lattice_modal_count,
                scored_tracks=persisted.aac_lattice_scored_tracks,
                max_z=persisted.aac_lattice_max_z,
            )
        proof = None
        if persisted.verified_lossless:
            assert persisted.verified_lossless_provenance is not None
            assert persisted.verified_lossless_source is not None
            assert persisted.verified_lossless_classifier is not None
            proof = VerifiedLosslessProof(
                provenance=persisted.verified_lossless_provenance,
                source=persisted.verified_lossless_source,
                classifier=persisted.verified_lossless_classifier,
                detail=persisted.verified_lossless_detail,
            )
        cd_rip_verification = persisted.cd_rip_verification
        cd_errors = cd_rip_proof_pair_validation_errors(
            cd_rip_verification,
            proof,
        )
        if cd_errors:
            raise ValueError("; ".join(cd_errors))
        return AlbumQualityEvidence(
            mb_release_id=persisted.mb_release_id,
            snapshot_fingerprint=persisted.snapshot_fingerprint,
            source_path=persisted.source_path,
            id=persisted.id,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=persisted.min_bitrate_kbps,
                avg_bitrate_kbps=persisted.avg_bitrate_kbps,
                median_bitrate_kbps=persisted.median_bitrate_kbps,
                format=persisted.format,
                is_cbr=persisted.is_cbr,
                spectral_grade=persisted.spectral_grade,
                spectral_bitrate_kbps=persisted.spectral_bitrate_kbps,
                spectral_subject=persisted.spectral_subject,
                spectral_provenance=persisted.spectral_provenance,
                was_converted_from=persisted.was_converted_from,
                cliff_hz=persisted.cliff_hz,
                codec_family=persisted.codec_family,
                ultrasonic_deficit_db=persisted.ultrasonic_deficit_db,
                spectral_measurement_version=persisted.spectral_measurement_version,
            ),
            measured_at=persisted.measured_at,
            files=[
                AlbumQualityEvidenceFile(
                    relative_path=file.relative_path,
                    size_bytes=file.size_bytes,
                    mtime_ns=file.mtime_ns,
                    extension=file.extension,
                    container=file.container,
                    codec=file.codec,
                    decode_ok=file.decode_ok,
                )
                for file in persisted_files
            ],
            codec=persisted.codec,
            container=persisted.container,
            storage_format=persisted.storage_format,
            target_format=persisted.target_format,
            target_is_cbr=persisted.target_is_cbr,
            lineage_version=persisted.lineage_version,
            v0_metric=v0_metric,
            on_disk_v0_research_attempted=persisted.on_disk_v0_research_attempted,
            current_enrichment_required=persisted.current_enrichment_required,
            verified_lossless_proof=proof,
            cd_rip_verification=cd_rip_verification,
            audio_validation=persisted.audio_validation,
            audio_corrupt=persisted.audio_corrupt,
            audio_error=persisted.audio_error,
            folder_layout=persisted.folder_layout,
            audio_file_count=persisted.audio_file_count,
            filetype_band=persisted.filetype_band,
            matched_bad_audio_hash_id=persisted.matched_bad_audio_hash_id,
            matched_bad_audio_hash_path=persisted.matched_bad_audio_hash_path,
            aac_lattice=aac_lattice,
        )
