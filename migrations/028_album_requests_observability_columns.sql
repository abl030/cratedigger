-- 028_album_requests_observability_columns.sql
--
-- PR1 of search-plan iteration 2: add 8 columns to album_requests for
-- observability and detection state.
--
--   * failure_class                  — 5-bucket classification at plan-wrap (R28)
--   * is_va_compilation              — VA detection flag persisted at enqueue (R12)
--   * unfindable_category            — 4-category taxonomy (R18-R20)
--   * unfindable_categorised_at      — when the categorisation last ran
--   * last_artist_probe_at           — most recent artist-only catalog probe
--   * last_artist_probe_match_count  — result of that probe
--   * rescued_at                     — long-tail-rescue audit timestamp (R21)
--   * prior_unfindable_category      — category cleared by the rescue (R21)
--
-- CHECK constraints on the enum-shaped TEXT columns follow the existing
-- search_log.outcome pattern — typos from scripts surface as constraint
-- violations rather than silent corruption.
--
-- See:
--   docs/brainstorms/2026-05-25-search-plan-iteration-2-requirements.md (R12, R18-R21, R28)
--   docs/plans/2026-05-25-001-feat-search-plan-iteration-2-plan.md (U1)

ALTER TABLE album_requests
    ADD COLUMN failure_class TEXT,
    ADD COLUMN is_va_compilation BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN unfindable_category TEXT,
    ADD COLUMN unfindable_categorised_at TIMESTAMPTZ,
    ADD COLUMN last_artist_probe_at TIMESTAMPTZ,
    ADD COLUMN last_artist_probe_match_count INTEGER,
    ADD COLUMN rescued_at TIMESTAMPTZ,
    ADD COLUMN prior_unfindable_category TEXT;

ALTER TABLE album_requests
    ADD CONSTRAINT album_requests_failure_class_check
    CHECK (failure_class IS NULL OR failure_class IN (
        'A_zero_results_dominant',
        'B_cands_never_match',
        'D_found_but_no_import',
        'E_mixed',
        'resolved'
    ));

ALTER TABLE album_requests
    ADD CONSTRAINT album_requests_unfindable_category_check
    CHECK (unfindable_category IS NULL OR unfindable_category IN (
        'artist_absent',
        'album_absent_artist_present',
        'one_track_structural',
        'wrong_pressing_available'
    ));

ALTER TABLE album_requests
    ADD CONSTRAINT album_requests_prior_unfindable_category_check
    CHECK (prior_unfindable_category IS NULL OR prior_unfindable_category IN (
        'artist_absent',
        'album_absent_artist_present',
        'one_track_structural',
        'wrong_pressing_available'
    ));

-- Partial index for cohort scans — the operator surface filters by
-- "any unfindable category" frequently; categorised rows are a small
-- minority of the full table.
CREATE INDEX idx_album_requests_unfindable_category
    ON album_requests(unfindable_category)
    WHERE unfindable_category IS NOT NULL;
