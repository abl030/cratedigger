-- 031_request_search_summary_view.sql
--
-- PR1 of search-plan iteration 2: per-request rollup view over the
-- last 14 days of search_log. Powers `pipeline-cli triage` (Phase 4
-- U16) and the future operator dashboard. Replaces the 60+ lines of
-- ad-hoc SQL that the post-deploy investigation kept rewriting.
--
-- See R29 in
-- docs/brainstorms/2026-05-25-search-plan-iteration-2-requirements.md.
--
-- Depends on columns added by:
--   * 025 — pre_filter_skip_count
--   * 027 — rejection_reason (read by dominant_rejection_reason)
-- Ordering 027 < 031 is the contract for the view to compile cleanly.
--
-- Restricted to a 14-day window to bound scan cost; operator triage
-- windows that need older data should query search_log directly. The
-- supporting composite index below makes the per-request lookup path
-- efficient.
--
-- Plain VIEW, not materialised: operator triage frequency is human-
-- paced (a few queries per minute at most), per-query cost is bounded
-- by the existing `idx_search_log_request_created_at` composite index
-- (added in migration 011_cycle_metrics.sql for the same access
-- pattern). No new index needed.

CREATE VIEW request_search_summary AS
SELECT
    sl.request_id,
    COUNT(*) AS total_searches,
    COUNT(*) FILTER (
        WHERE jsonb_array_length(coalesce(sl.candidates, '[]'::jsonb)) > 0
    ) AS with_cands_count,
    COUNT(*) FILTER (WHERE sl.outcome = 'found') AS found_count,
    COUNT(*) FILTER (WHERE sl.result_count >= 950) AS near_cap_count,
    COUNT(*) FILTER (WHERE sl.result_count = 0) AS zero_results_count,
    COALESCE(SUM(sl.pre_filter_skip_count), 0)::BIGINT AS pre_filter_skips_total,
    (
        SELECT plan_strategy
        FROM search_log s2
        WHERE s2.request_id = sl.request_id
          AND s2.created_at >= NOW() - INTERVAL '14 days'
          AND jsonb_array_length(coalesce(s2.candidates, '[]'::jsonb)) > 0
        ORDER BY s2.created_at ASC
        LIMIT 1
    ) AS first_strategy_with_cands,
    MODE() WITHIN GROUP (ORDER BY sl.rejection_reason)
        FILTER (WHERE sl.rejection_reason IS NOT NULL) AS dominant_rejection_reason,
    MAX(sl.created_at) AS last_search_at
FROM search_log sl
WHERE sl.created_at >= NOW() - INTERVAL '14 days'
GROUP BY sl.request_id;
