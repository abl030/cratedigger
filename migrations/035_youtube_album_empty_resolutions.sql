-- 035_youtube_album_empty_resolutions.sql
--
-- Side table marking ``(release_group_identifier, source)`` pairs whose
-- YouTube Music search returned zero album results. Companion to
-- migration 034.
--
-- Why a side table: ``youtube_album_mappings`` is row-shaped (one row per
-- YT browse_id) and a YT search that returns nothing has zero rows to
-- insert. Without a marker, the cache-hit gate in the resolver can't tell
-- "we have never resolved this pair" from "we resolved it and found
-- nothing". The former should re-poll YT (cache MISS); the latter should
-- short-circuit (cache HIT). Per R14 (operator-triggered refresh only),
-- empty-search release groups must NOT re-poll YT on every resolve.
--
-- The alternative (a NULL ``yt_browse_id`` sentinel row inside
-- ``youtube_album_mappings``) would either require dropping the NOT NULL
-- constraint on ``yt_browse_id`` or a magic placeholder value — both
-- worse than a dedicated marker.
--
-- The marker is written transactionally by
-- ``PipelineDB.upsert_youtube_album_mapping`` whenever ``rows`` is empty;
-- it is deleted by the same method when ``rows`` is non-empty (so a later
-- resolve that finds albums supersedes the empty-resolution flag).

CREATE TABLE youtube_album_empty_resolutions (
    release_group_identifier TEXT NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('mb', 'discogs')),
    resolved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (release_group_identifier, source)
);
