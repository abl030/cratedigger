-- 023_add_replace_supersede_schema.sql — Replace operator action schema
--
-- Adds the schema affordances for the supersede pattern used by the
-- Replace operator action (see docs/plans/2026-05-18-001-feat-replace-
-- operator-action-plan.md):
--
--   * `album_requests.replaces_request_id` — nullable self-referencing
--     FK pointing at the row that was abandoned. ON DELETE RESTRICT
--     enforces the brainstorm invariant that replaced rows are never
--     silently deleted; pruning a lineage chain must walk descendants
--     first.
--   * Partial index on the lineage FK so reverse lookups (descendant of
--     N?) are cheap.
--   * Extend the `album_requests_status_check` CHECK constraint to
--     include `'replaced'` — the new terminal status the supersede
--     transaction writes onto the OLD row.

ALTER TABLE album_requests
    ADD COLUMN replaces_request_id INTEGER
        REFERENCES album_requests(id) ON DELETE RESTRICT;

CREATE INDEX idx_album_requests_replaces_request_id
    ON album_requests(replaces_request_id)
    WHERE replaces_request_id IS NOT NULL;

DO $$ BEGIN
    ALTER TABLE album_requests DROP CONSTRAINT album_requests_status_check;
    ALTER TABLE album_requests ADD CONSTRAINT album_requests_status_check
        CHECK(status IN ('wanted', 'downloading', 'imported', 'manual', 'replaced'));
END $$;
