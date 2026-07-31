-- 069_aac_lattice_capture.sql - issue #829 AAC-lattice leg, PR-A capture
--
-- Persists the AAC MDCT-frame-lattice measurement the preview worker takes on
-- the promotion-plausible cohort (lossless containers whose album spectral
-- grade is genuine/marginal). Capture-only: nothing in the production decider
-- reads these columns -- PR-B owns the proof leg that will.
--
-- aac_lattice_tracks is the per-track array (filename + offset/z/proba, or an
-- error string); the four scalars are the album statistics the
-- offset-concentration rule reads, broken out so SQL can aggregate them
-- without unnesting the JSONB. The shape CHECK below mirrors
-- lib/quality/evidence_types.py::AacLatticeCapture.validation_errors exactly.
--
-- NULL across all five columns means NEVER MEASURED; a row with
-- aac_lattice_scored_tracks = 0 means measured and nothing scored (every
-- track errored -- 96 kHz has no scalefactor-band table at all). Those two are
-- deliberately distinguishable.
--
-- aac_lattice_max_z is DOUBLE PRECISION, not REAL: the same z values are also
-- stored at full double precision inside aac_lattice_tracks, and a single
-- precision scalar would disagree with its own array on read-back.
--
-- Forward-only, no backfill (.claude/rules/scope.md): existing rows stay NULL
-- and are simply never-measured.

ALTER TABLE album_quality_evidence
    ADD COLUMN aac_lattice_tracks JSONB,
    ADD COLUMN aac_lattice_modal_offset INTEGER CHECK (
        aac_lattice_modal_offset IS NULL
        OR (aac_lattice_modal_offset >= 0 AND aac_lattice_modal_offset < 1024)
    ),
    ADD COLUMN aac_lattice_modal_count INTEGER CHECK (
        aac_lattice_modal_count IS NULL OR aac_lattice_modal_count >= 1
    ),
    ADD COLUMN aac_lattice_scored_tracks INTEGER CHECK (
        aac_lattice_scored_tracks IS NULL OR aac_lattice_scored_tracks >= 0
    ),
    ADD COLUMN aac_lattice_max_z DOUBLE PRECISION,
    ADD CONSTRAINT album_quality_evidence_aac_lattice_shape CHECK (
        -- never measured
        (aac_lattice_tracks IS NULL
         AND aac_lattice_modal_offset IS NULL
         AND aac_lattice_modal_count IS NULL
         AND aac_lattice_scored_tracks IS NULL
         AND aac_lattice_max_z IS NULL)
        -- measured, nothing scored
        OR (aac_lattice_tracks IS NOT NULL
            AND aac_lattice_scored_tracks = 0
            AND aac_lattice_modal_offset IS NULL
            AND aac_lattice_modal_count IS NULL
            AND aac_lattice_max_z IS NULL)
        -- measured, at least one track scored
        OR (aac_lattice_tracks IS NOT NULL
            AND aac_lattice_scored_tracks > 0
            AND aac_lattice_modal_offset IS NOT NULL
            AND aac_lattice_modal_count IS NOT NULL
            AND aac_lattice_max_z IS NOT NULL
            AND aac_lattice_modal_count <= aac_lattice_scored_tracks)
    );

COMMENT ON COLUMN album_quality_evidence.aac_lattice_tracks IS
    'Per-track AAC MDCT-frame-lattice rows: filename plus offset/z/proba, or an error string. NULL means the album was never measured (issue #829 AAC-lattice leg PR-A).';

COMMENT ON COLUMN album_quality_evidence.aac_lattice_modal_offset IS
    'Most-repeated MDCT frame offset (0-1023) across the scored tracks. Apple/CoreAudio concentrates on 960, ffmpeg-native AAC on 0, genuine albums are uniform.';

COMMENT ON COLUMN album_quality_evidence.aac_lattice_modal_count IS
    'How many scored tracks recovered aac_lattice_modal_offset -- the k of the parameter-free k>=4 offset-concentration rule.';

COMMENT ON COLUMN album_quality_evidence.aac_lattice_scored_tracks IS
    'How many tracks produced a lattice statistic. 0 means measured but nothing scored (e.g. every track is 96 kHz, which has no scalefactor-band table).';

COMMENT ON COLUMN album_quality_evidence.aac_lattice_max_z IS
    'Highest per-track sweep contrast z across the scored tracks. Triage-grade only: z>12 is the conservative operating point, z>6.914 prices at ~490 false positives per 5000 albums.';
