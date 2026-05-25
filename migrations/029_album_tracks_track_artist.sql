-- 029_album_tracks_track_artist.sql
--
-- PR1 of search-plan iteration 2: add per-track artist credit column
-- to album_tracks. Required for the VA-specific strategy mix (R13) —
-- peers file Various Artists compilations under the actual track
-- artist ("Nat King Cole - Christmas Song"), not the album-level
-- "Various Artists" credit. The track_artist column gives the
-- generator the entropy it needs.
--
-- Nullable: tracks without per-track credit data (or sources that
-- lack it on a given record) stay NULL; the generator skips the
-- track-artist slot for them.
--
-- See R10 in
-- docs/brainstorms/2026-05-25-search-plan-iteration-2-requirements.md.

ALTER TABLE album_tracks
    ADD COLUMN track_artist TEXT;
