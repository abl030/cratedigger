-- 078_quality_evidence_lineage_v5.sql
--
-- Issue #1145. v4 rows derived an MP3's rank format from an inferred encoding
-- mode (per-track bitrate uniformity) and chose between two band tables 75
-- kbps apart. v5 derives it from one table plus a LAME `-V` contract proven
-- from `items.encoder_settings` / the file's own LAME tag.
--
-- No backfill: the bump alone makes every v4 row report itself stale to
-- `lib.quality_evidence.current_evidence_rebuild_reasons`, which rebuilds it
-- from live Beets facts on next touch. That is the same lazy conversion
-- migrations 050 and 055 used.
--
-- The two-axis fact vocabulary is IDENTICAL between v4 and v5, so every
-- version-gated shape/domain CHECK on this table is written
-- `lineage_version < 4 OR ...` and already applies unchanged to a v5 row.
-- All ten live ones came from migration 055 (which also added the
-- `lineage_version` domain check this migration replaces); 057 added an
-- eleventh, 072 rewrote it, and 073 dropped it outright, so nothing from
-- 057/072/073 survives to re-gate here.
--
-- The same reasoning covers the `lineage_version < 4` merge predicates in
-- `lib/pipeline_db/evidence.py`: they mean "predates the two-axis
-- vocabulary", not "predates the current version", and widening them would
-- replace rather than merge the preserved spectral/V0 tuples on every v4
-- row's rebuild.

ALTER TABLE album_quality_evidence
    DROP CONSTRAINT album_quality_evidence_lineage_version_check;

ALTER TABLE album_quality_evidence
    ALTER COLUMN lineage_version SET DEFAULT 5;

ALTER TABLE album_quality_evidence
    ADD CONSTRAINT album_quality_evidence_lineage_version_check
        CHECK (lineage_version IN (1, 3, 4, 5));

COMMENT ON COLUMN album_quality_evidence.lineage_version IS
    '1=historical ambiguous storage/target projection, 3=separate source and '
    'target facts, 4=two-axis subject/provenance vocabulary, 5=one MP3 band '
    'table plus proven LAME -V contract (4 and 5 share the v4 fact vocabulary)';
