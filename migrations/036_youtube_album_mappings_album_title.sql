-- 036_youtube_album_mappings_album_title.sql
--
-- Round 2 review finding P0-1: ``album_title`` was added to the row-dict
-- payload of ``PipelineDB.upsert_youtube_album_mapping`` in round 1 (the
-- #15 ``albumartist`` / cache-fallback fidelity fix) but the INSERT
-- column list was never widened to match. ``psycopg2.extras.execute_values``
-- silently ignores extra dict keys, so production writes dropped the
-- field, ``get_youtube_album_mapping`` always returned ``None`` for it,
-- and ``_rows_to_youtube_releases`` rehydrated ``SyntheticItem.album=""``
-- for every cached row.
--
-- ``album_artist`` has the same lossy-round-trip shape (the cache-fallback
-- rehydrates ``SyntheticItem.albumartist`` from the per-track artist when
-- the album-level artist was lost on write — round 2 maintainability-5).
-- Persist it row-level so the cache round-trip is fidelity-equal to the
-- fresh-resolve path.
--
-- Both columns are NULLable. There ARE no legacy rows on this branch yet
-- (the feature has never shipped), but per the forward-only single-operator
-- convention every new column is NULLable by default — keeps the
-- migration test trivial and matches the rest of the table.

ALTER TABLE youtube_album_mappings
    ADD COLUMN album_title  TEXT,
    ADD COLUMN album_artist TEXT;
