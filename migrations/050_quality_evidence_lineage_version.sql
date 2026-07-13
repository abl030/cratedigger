-- 050_quality_evidence_lineage_version.sql
--
-- Existing album_quality_evidence rows predate the separation between the
-- downloaded source codec and projected target policy. Some historical rows
-- stored an explicit target label in storage_format, so consumers cannot
-- distinguish that shape by inspecting values. Mark history explicitly as
-- lineage v1; every new typed producer writes lineage v3.

ALTER TABLE album_quality_evidence
    ADD COLUMN lineage_version SMALLINT NOT NULL DEFAULT 1;

ALTER TABLE album_quality_evidence
    ADD CONSTRAINT album_quality_evidence_lineage_version_check
    CHECK (lineage_version IN (1, 3));

COMMENT ON COLUMN album_quality_evidence.lineage_version IS
    '1=historical ambiguous storage/target projection, 3=separate source and target facts';
