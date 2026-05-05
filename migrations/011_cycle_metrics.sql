-- 011_cycle_metrics.sql - Persist per-cycle runtime telemetry for web dashboards.
--
-- Cycle timing and cache counters used to exist only in journal log lines. The
-- web UI needs median/outlier views without scraping systemd logs, so persist
-- one compact row at the end of each cratedigger cycle.

CREATE TABLE cycle_metrics (
    id SERIAL PRIMARY KEY,
    started_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    cycle_total_s DOUBLE PRECISION NOT NULL,
    browse_time_s DOUBLE PRECISION NOT NULL DEFAULT 0,
    match_time_s DOUBLE PRECISION NOT NULL DEFAULT 0,
    search_time_s DOUBLE PRECISION NOT NULL DEFAULT 0,
    cache_pos_hits INTEGER NOT NULL DEFAULT 0,
    cache_neg_hits INTEGER NOT NULL DEFAULT 0,
    cache_misses INTEGER NOT NULL DEFAULT 0,
    cache_errors INTEGER NOT NULL DEFAULT 0,
    cache_fuse_tripped INTEGER NOT NULL DEFAULT 0,
    cache_write_errors INTEGER NOT NULL DEFAULT 0,
    peers_browsed INTEGER NOT NULL DEFAULT 0,
    peers_browsed_lazy INTEGER NOT NULL DEFAULT 0,
    fanout_waves INTEGER NOT NULL DEFAULT 0,
    cycle_searches_watchdog_killed INTEGER NOT NULL DEFAULT 0,
    find_download_queued INTEGER NOT NULL DEFAULT 0,
    find_download_completed INTEGER NOT NULL DEFAULT 0,
    find_download_drain_time_s DOUBLE PRECISION NOT NULL DEFAULT 0
);

CREATE INDEX idx_cycle_metrics_created_at ON cycle_metrics(created_at DESC);
CREATE INDEX idx_search_log_created_at ON search_log(created_at DESC);
CREATE INDEX idx_search_log_request_created_at ON search_log(request_id, created_at DESC);
