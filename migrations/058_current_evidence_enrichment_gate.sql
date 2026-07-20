-- A changed installed snapshot is linked before spectral/V0 enrichment so
-- the enrichment writers can address the exact content row. Persist the gate
-- that keeps retries fail-closed until those facts are available.
ALTER TABLE album_quality_evidence
    ADD COLUMN current_enrichment_required BOOLEAN NOT NULL DEFAULT FALSE;
