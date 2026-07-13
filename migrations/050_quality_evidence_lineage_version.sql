-- 050_quality_evidence_lineage_version.sql
--
-- Existing album_quality_evidence rows predate the separation between the
-- downloaded source codec and projected target policy. Some historical rows
-- stored an explicit target label in storage_format, so consumers cannot
-- distinguish that shape by inspecting values. Mark history explicitly as
-- lineage v1; every new typed producer writes lineage v3.

ALTER TABLE album_quality_evidence
    ADD COLUMN target_is_cbr BOOLEAN,
    ADD COLUMN lineage_version SMALLINT NOT NULL DEFAULT 1;

-- The ADD COLUMN default above marks every already-present row as historical.
-- New SQL writers that omit the explicit v3 field must fail safe into the
-- current typed lineage instead of silently manufacturing another v1 row.
ALTER TABLE album_quality_evidence
    ALTER COLUMN lineage_version SET DEFAULT 3;

ALTER TABLE album_quality_evidence
    ADD CONSTRAINT album_quality_evidence_lineage_version_check
    CHECK (lineage_version IN (1, 3));

COMMENT ON COLUMN album_quality_evidence.lineage_version IS
    '1=historical ambiguous storage/target projection, 3=separate source and target facts';

COMMENT ON COLUMN album_quality_evidence.target_is_cbr IS
    'album-wide bitrate mode of the projected target/probe; independent of source and output measurements';
