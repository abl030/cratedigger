-- 025_search_log_pre_filter_skip_count.sql
--
-- Aggregable scalar count of pre-filter-skipped dirs per search_log
-- row. Companion to the ≤5 sample CandidateScore rows in the candidates
-- JSONB blob. Default 0 — no backfill (historical rows did not capture
-- this; column is NOT NULL so forward queries can skip NULL guards).
-- See ``docs/brainstorms/`` for the full search-plan-entropy plan.

ALTER TABLE search_log
    ADD COLUMN pre_filter_skip_count INTEGER NOT NULL DEFAULT 0;
