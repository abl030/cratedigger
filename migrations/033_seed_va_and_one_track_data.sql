-- 033_seed_va_and_one_track_data.sql
--
-- Pure-SQL data seed for the columns that 028 added. Replaces what
-- would have been a committed backfill script (deleted as part of the
-- archivist-frame cleanup — see commit history). The single-operator
-- invariant says backfills are operator-driven one-shots, NOT product
-- code; this migration covers the work that doesn't require an
-- external network call. The agent runs the network-dependent half
-- (release_group_year / release_group_id / track_artist /
-- catalog_number for rows still missing them) as a transient heredoc
-- during the deploy window.
--
-- This migration is itself one-shot by virtue of the migrator's
-- schema_migrations contract — applied exactly once per database.
-- Forward-only; no re-run, no idempotency machinery needed.
--
-- See:
--   docs/search-plan-iter2-deploy.md (deploy runbook with the
--     transient one-shot's exact shape for reproducibility)
--   CLAUDE.md § "Why this exists — the archivist frame"
--   CLAUDE.md § "Single-operator invariant" (TBD — landing alongside
--     this cleanup)
--
-- Coverage:
--   * is_va_compilation: TRUE for rows whose mb_artist_id matches the
--     canonical VA MBID (89ad4ac3-...). Rows whose Discogs artist_id
--     is 194 OR whose mb_artist_id was never populated need the
--     agent's one-shot — they fall through and stay FALSE here.
--   * unfindable_category: 'one_track_structural' for every request
--     with exactly one album_tracks row and a NULL unfindable_category.
--     Pure structural categorisation; no network needed.

-- MB-side VA seed. Catches rows whose enqueue path correctly populated
-- mb_artist_id with the canonical VA identity.
UPDATE album_requests
SET is_va_compilation = TRUE
WHERE mb_artist_id = '89ad4ac3-39f7-470e-963a-56509c546377'
  AND is_va_compilation = FALSE;

-- One-track-structural categorisation. Every request that has exactly
-- one row in album_tracks and no operator-set unfindable_category gets
-- categorised here. R19 (unfindable taxonomy) treats single-track
-- requests as structurally unfindable as full-album acquisitions.
UPDATE album_requests
SET unfindable_category = 'one_track_structural',
    unfindable_categorised_at = NOW()
WHERE unfindable_category IS NULL
  AND id IN (
      SELECT request_id
      FROM album_tracks
      GROUP BY request_id
      HAVING COUNT(*) = 1
  );
