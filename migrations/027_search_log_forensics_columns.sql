-- 027_search_log_forensics_columns.sql
--
-- PR1 (Phase 1) of search-plan iteration 2: add forensics columns to
-- search_log so per-search rejection reasons, query entropy, matcher
-- score, and uncapped result counts are queryable as scalars instead
-- of requiring JSONB introspection.
--
-- See R22-R27 in
-- docs/brainstorms/2026-05-25-search-plan-iteration-2-requirements.md.
--
-- All columns nullable. Historical rows pre-deploy carry NULL; new code
-- populates them at log-write time (wired in Phase 3 U11). The
-- request_search_summary view (031) reads rejection_reason; the unit
-- ordering between 027 and 031 is intentional.

ALTER TABLE search_log
    ADD COLUMN rejection_reason TEXT,
    ADD COLUMN result_count_uncapped INTEGER,
    ADD COLUMN query_token_count INTEGER,
    ADD COLUMN query_distinct_token_count INTEGER,
    ADD COLUMN expected_track_count INTEGER,
    ADD COLUMN matcher_score_top1 REAL,
    ADD COLUMN query_template TEXT;
