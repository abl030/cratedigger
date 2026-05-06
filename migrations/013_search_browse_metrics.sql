-- 013_search_browse_metrics.sql - Attribute browse fan-out cost per search.
--
-- cycle_metrics keeps aggregate per-cycle peer/dir totals. These columns let
-- dashboard diagnostics identify the exact search_log row, release, and query
-- tokens that generated the largest fan-out work inside those cycles.

ALTER TABLE search_log
    ADD COLUMN browse_time_s DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN match_time_s DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN peers_browsed INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN peers_browsed_lazy INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN fanout_waves INTEGER NOT NULL DEFAULT 0;

CREATE INDEX idx_search_log_browse_cost_created_at
    ON search_log (
        ((peers_browsed + peers_browsed_lazy)) DESC,
        created_at DESC
    );
