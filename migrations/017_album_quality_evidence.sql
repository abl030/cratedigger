-- 017_album_quality_evidence.sql - active relational album-quality evidence
--
-- Active candidate/current evidence lives in typed relational rows. Historical
-- JSONB/scalar quality columns remain audit surfaces only.

CREATE TABLE album_quality_evidence (
    id BIGSERIAL PRIMARY KEY,
    owner_type TEXT NOT NULL CHECK (
        owner_type IN (
            'download_log_candidate',
            'import_job_candidate',
            'request_current'
        )
    ),
    owner_id INTEGER NOT NULL CHECK (owner_id > 0),
    measured_at TIMESTAMPTZ NOT NULL,
    codec TEXT,
    container TEXT,
    storage_format TEXT,
    target_format TEXT,
    min_bitrate_kbps INTEGER CHECK (
        min_bitrate_kbps IS NULL OR min_bitrate_kbps >= 0
    ),
    avg_bitrate_kbps INTEGER CHECK (
        avg_bitrate_kbps IS NULL OR avg_bitrate_kbps >= 0
    ),
    median_bitrate_kbps INTEGER CHECK (
        median_bitrate_kbps IS NULL OR median_bitrate_kbps >= 0
    ),
    format TEXT,
    is_cbr BOOLEAN NOT NULL DEFAULT FALSE,
    spectral_grade TEXT,
    spectral_bitrate_kbps INTEGER CHECK (
        spectral_bitrate_kbps IS NULL OR spectral_bitrate_kbps >= 0
    ),
    verified_lossless BOOLEAN NOT NULL DEFAULT FALSE,
    was_converted_from TEXT,
    v0_min_bitrate_kbps INTEGER CHECK (
        v0_min_bitrate_kbps IS NULL OR v0_min_bitrate_kbps >= 0
    ),
    v0_avg_bitrate_kbps INTEGER CHECK (
        v0_avg_bitrate_kbps IS NULL OR v0_avg_bitrate_kbps >= 0
    ),
    v0_median_bitrate_kbps INTEGER CHECK (
        v0_median_bitrate_kbps IS NULL OR v0_median_bitrate_kbps >= 0
    ),
    v0_source_lineage TEXT CHECK (
        v0_source_lineage IS NULL OR v0_source_lineage NOT IN (
            'lossless_source_v0',
            'native_lossy_research_v0',
            'on_disk_research_v0'
        )
    ),
    v0_source_provenance TEXT,
    v0_proof_provenance TEXT,
    verified_lossless_proof_origin TEXT,
    verified_lossless_source TEXT,
    verified_lossless_classifier TEXT,
    verified_lossless_detail TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT album_quality_evidence_one_per_owner
        UNIQUE (owner_type, owner_id),
    CONSTRAINT album_quality_evidence_v0_metric_shape CHECK (
        (
            v0_min_bitrate_kbps IS NULL
            AND v0_avg_bitrate_kbps IS NULL
            AND v0_median_bitrate_kbps IS NULL
            AND v0_source_lineage IS NULL
            AND v0_source_provenance IS NULL
            AND v0_proof_provenance IS NULL
        )
        OR (
            v0_source_lineage IS NOT NULL
            AND (
                v0_min_bitrate_kbps IS NOT NULL
                OR v0_avg_bitrate_kbps IS NOT NULL
                OR v0_median_bitrate_kbps IS NOT NULL
            )
        )
    ),
    CONSTRAINT album_quality_evidence_verified_proof_shape CHECK (
        (
            verified_lossless = TRUE
            AND verified_lossless_proof_origin IS NOT NULL
            AND verified_lossless_source IS NOT NULL
            AND verified_lossless_classifier IS NOT NULL
        )
        OR (
            verified_lossless = FALSE
            AND verified_lossless_proof_origin IS NULL
            AND verified_lossless_source IS NULL
            AND verified_lossless_classifier IS NULL
            AND verified_lossless_detail IS NULL
        )
    )
);

CREATE TABLE album_quality_evidence_files (
    id BIGSERIAL PRIMARY KEY,
    evidence_id BIGINT NOT NULL
        REFERENCES album_quality_evidence(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    relative_path TEXT NOT NULL CHECK (length(relative_path) > 0),
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    mtime_ns BIGINT NOT NULL CHECK (mtime_ns >= 0),
    extension TEXT NOT NULL CHECK (length(extension) > 0),
    container TEXT NOT NULL CHECK (length(container) > 0),
    codec TEXT,
    UNIQUE (evidence_id, relative_path),
    UNIQUE (evidence_id, ordinal)
);

CREATE INDEX idx_album_quality_evidence_owner
    ON album_quality_evidence(owner_type, owner_id);

CREATE INDEX idx_album_quality_evidence_files_parent_path
    ON album_quality_evidence_files(evidence_id, relative_path);
