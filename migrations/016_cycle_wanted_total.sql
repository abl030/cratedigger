-- 016_cycle_wanted_total.sql - Snapshot wanted backlog size per cycle.
--
-- The Pipeline dashboard already persists one row per completed cratedigger
-- cycle. Recording the wanted count there gives operators a cheap trend line:
-- "is the backlog actually draining, and at what pace?" Historical rows remain
-- NULL because the request table only stores current status, not status history.

ALTER TABLE cycle_metrics
    ADD COLUMN wanted_total INTEGER;

ALTER TABLE cycle_metrics
    ADD CONSTRAINT cycle_metrics_wanted_total_nonneg_check
        CHECK (wanted_total IS NULL OR wanted_total >= 0);

CREATE INDEX idx_cycle_metrics_wanted_total_created_at
    ON cycle_metrics(created_at DESC)
    WHERE wanted_total IS NOT NULL;
