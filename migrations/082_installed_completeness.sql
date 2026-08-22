-- 082_installed_completeness.sql - issue #1241 installed-copy completeness
--
-- Whether the INSTALLED copy holds every audio component its exact source
-- release declares. Measured at preview time by
-- lib/library_completeness.py::classify_installed_release -- the same
-- classifier the daily census (#1149) runs, with the boolean
-- composite-silence-gap instrument deliberately not consulted.
--
-- Shape: lib/quality/evidence_types.py::InstalledCompleteness. NULL means
-- NEVER MEASURED, which reads as "unknown" and changes nothing. A stored
-- verdict of 'unknown' means measurement was ATTEMPTED and could not decide
-- (unreadable mirror, ambiguous Beets identity, malformed manifest); it is
-- deliberately distinguishable from NULL so a dead mirror is not re-fetched
-- every preview cycle, and it behaves identically to NULL for policy.
--
-- Only ever written for a current (installed) evidence row. Candidate rows
-- keep it NULL: beets' own extra_tracks reject (lib/beets.py) is the
-- candidate side's completeness authority and runs before evidence exists.
--
-- The CHECK below mirrors InstalledCompleteness.validation_errors exactly:
-- a verdict that accuses or withholds must name why, and a decided verdict
-- must name the source manifest it was decided against.
--
-- No lineage bump: this is a brand-new fact whose absence is not a
-- re-interpretation of any stored value (migration 069's convention).
-- Forward-only, no backfill (.claude/rules/scope.md): existing rows stay
-- NULL and are filled by the preview enrichment lane on their next cycle.

ALTER TABLE album_quality_evidence
    ADD COLUMN installed_completeness JSONB,
    ADD CONSTRAINT album_quality_evidence_installed_completeness_shape CHECK (
        CASE
        WHEN installed_completeness IS NULL THEN TRUE
        WHEN jsonb_typeof(installed_completeness) <> 'object' THEN FALSE
        ELSE ((
            installed_completeness->>'verdict'
                IN ('complete', 'incomplete', 'unknown')
            AND (
                installed_completeness->>'verdict' = 'complete'
                OR NULLIF(BTRIM(
                    COALESCE(installed_completeness->>'detail', '')
                ), '') IS NOT NULL
            )
            AND (
                installed_completeness->>'verdict' = 'unknown'
                OR installed_completeness->>'source'
                    IN ('musicbrainz', 'discogs')
            )
            AND (installed_completeness->>'declared_audio_components')
                ~ '^(0|[1-9][0-9]*)$'
            AND (installed_completeness->>'physical_audio_files')
                ~ '^(0|[1-9][0-9]*)$'
        ) IS TRUE)
        END
    );

COMMENT ON COLUMN album_quality_evidence.installed_completeness IS
    'Whether the INSTALLED copy holds every audio component its exact source release declares (issue #1241). Shape: lib/quality/evidence_types.py::InstalledCompleteness. NULL means never measured, which reads as "unknown" and changes nothing. Only ever written for a current (installed) evidence row.';
