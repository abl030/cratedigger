-- 030_album_request_field_resolutions.sql
--
-- PR1 of search-plan iteration 2: side table tracking resolution
-- attempts for every external-metadata field cratedigger populates
-- from MB or Discogs. Replaces silent-failure semantics with structured
-- "we tried, here's what happened, here's when we'll retry" data, so
-- the operator can see which requests are running degraded search
-- plans because their data is incomplete.
--
-- See R14, R15, R17 in
-- docs/brainstorms/2026-05-25-search-plan-iteration-2-requirements.md.
--
-- Status enum lives in lib/field_resolver_service.py — not enforced via
-- DB CHECK because new statuses will appear as the system grows and
-- shipped migrations are frozen history. The service layer is the
-- canonical source.
--
-- Working values (Phase 1 U2):
--   resolved                          — value populated
--   unresolved_404                    — upstream returned not-found (sticky 30d)
--   unresolved_malformed              — input ID is malformed (sticky permanent)
--   unresolved_mirror_unavailable     — network/HTTP error (retry 1d)
--   unresolved_timeout                — request exceeded timeout (retry 1d)
--   unresolved_field_missing_upstream — upstream has record but no field (sticky 30d)

CREATE TABLE album_request_field_resolutions (
    id SERIAL PRIMARY KEY,
    request_id INTEGER NOT NULL REFERENCES album_requests(id) ON DELETE CASCADE,
    field_name TEXT NOT NULL,
    resolved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status TEXT NOT NULL,
    reason_code TEXT,
    attempts INTEGER NOT NULL DEFAULT 1,
    UNIQUE (request_id, field_name)
);

-- One index per dominant access pattern:
--   (request_id)            — triage "show me everything about request N"
--   (field_name, status)    — cohort "show all rows where track_artist is unresolved_404"
--   (field_name, resolved_at) — backfill "find oldest probes per field"
CREATE INDEX idx_arfr_request
    ON album_request_field_resolutions(request_id);
CREATE INDEX idx_arfr_field_status
    ON album_request_field_resolutions(field_name, status);
CREATE INDEX idx_arfr_field_resolved_at
    ON album_request_field_resolutions(field_name, resolved_at);
