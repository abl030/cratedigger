-- 079_search_log_cross_request_conflict.sql
--
-- Issue #1196 item 2: a distinct forensics marker for a cross-request
-- enqueue-guard skip (#1178), so triage/search forensics can tell "this
-- attempt was deliberately declined because another request already held
-- the queue keys" from "no peer had it" -- both currently surface as the
-- SAME search_log.outcome='no_match' (or 'error' when combined with an
-- enqueue_failed candidate). Deliberately NOT a new outcome value and NOT
-- a rejection_reason override: both feed
-- ``UnfindableSearchLogSignal``/``classify_unfindable_from_state``
-- (lib/unfindable_detection_service.py), and this column is never
-- referenced by that query, so it cannot change unfindable-classification
-- inputs.
--
-- Nullable, no CHECK constraint (mirrors rejection_reason, migration 027):
-- NULL means the guard never fired for this search; a non-NULL array
-- carries the exact conflicting request id(s) it recorded.

ALTER TABLE search_log
    ADD COLUMN cross_request_conflict_request_ids INTEGER[];
