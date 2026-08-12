-- 077_unfindable_run_metrics.sql - Persist per-run unfindable-detection
-- telemetry for the web dashboard.
--
-- Mirrors 011_cycle_metrics.sql: one compact row per
-- cratedigger-unfindable.service run so the dashboard can show run
-- health (cohort/backlog size, batch capacity, per-outcome counts,
-- circuit-breaker trips, wall time) without scraping journal logs.
-- Issue #1112 item 1.

-- candidates_processed vs. probes_attempted (review round 1, F7):
-- candidates_processed is every candidate the batch attempted (the six
-- RESULT_* counts below partition exactly this number); probes_attempted
-- is the subset that actually fired a Soulseek search -- it excludes
-- not_due_count and request_not_found_count, the two outcomes decided
-- before any probe.
CREATE TABLE unfindable_run_metrics (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    cohort_total INTEGER NOT NULL,
    due_backlog_at_start INTEGER NOT NULL,
    batch_limit INTEGER NOT NULL,
    candidates_processed INTEGER NOT NULL,
    probes_attempted INTEGER NOT NULL,
    categorised_count INTEGER NOT NULL DEFAULT 0,
    downgraded_count INTEGER NOT NULL DEFAULT 0,
    no_change_count INTEGER NOT NULL DEFAULT 0,
    probe_failed_count INTEGER NOT NULL DEFAULT 0,
    not_due_count INTEGER NOT NULL DEFAULT 0,
    request_not_found_count INTEGER NOT NULL DEFAULT 0,
    breaker_tripped BOOLEAN NOT NULL DEFAULT FALSE,
    duration_seconds DOUBLE PRECISION NOT NULL
);

CREATE INDEX idx_unfindable_run_metrics_created_at
    ON unfindable_run_metrics(created_at DESC);
