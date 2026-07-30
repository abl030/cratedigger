-- The operator close/retry surface was removed once automation world failures
-- began recording audit evidence and returning requests to the search pool.
-- These columns served only that unreachable declared-close protocol.
ALTER TABLE processing_cleanup_journal
    DROP CONSTRAINT processing_cleanup_journal_declaration_complete,
    DROP COLUMN declared_result_status,
    DROP COLUMN declared_reason,
    DROP COLUMN evidence_revision;
