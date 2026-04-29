-- 010_search_forensics_and_manual_reason.sql - Search forensics + manual reason
--
-- U1 of search-escalation-and-forensics. Adds the columns the upcoming search
-- escalation logic and the web "Manual review" UI need to record what actually
-- happened at search time and why a request ended up in manual:
--
--   * search_log.candidates  - top-20 candidate scores per search (JSONB)
--   * search_log.variant     - variant tag like 'default', 'v1_year', 'v4_tracks_0'
--   * search_log.final_state - slskd terminal state string
--   * album_requests.manual_reason - human/decision-readable manual cause
--
-- Also extends search_log.outcome to allow 'exhausted' (every variant tried,
-- nothing matched). Postgres auto-named the original inline CHECK
-- search_log_outcome_check (see migrations/001_initial.sql:119-121); we drop
-- and recreate it here so the constraint stays named and can be evolved by
-- future migrations.

ALTER TABLE search_log ADD COLUMN candidates JSONB;
ALTER TABLE search_log ADD COLUMN variant TEXT;
ALTER TABLE search_log ADD COLUMN final_state TEXT;

ALTER TABLE album_requests ADD COLUMN manual_reason TEXT;

ALTER TABLE search_log DROP CONSTRAINT search_log_outcome_check;
ALTER TABLE search_log ADD CONSTRAINT search_log_outcome_check
  CHECK (outcome IN ('found','no_match','no_results','timeout','error','empty_query','exhausted'));
