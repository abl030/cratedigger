-- 034_youtube_album_mappings.sql
--
-- YouTube Music album resolver cache (U3 of the YT resolver feature).
-- See:
--   docs/brainstorms/2026-05-27-youtube-music-album-resolver-requirements.md (R12, R13, R14)
--   docs/plans/2026-05-27-001-feat-youtube-music-album-resolver-plan.md      (U3)
--
-- Stores the scored matrix the resolver service produces: for each
-- MusicBrainz release-group (or Discogs master), one row per YouTube
-- Music sibling album that the search-then-expand step surfaced, with
-- per-MBID beets distance scores attached as JSONB.
--
-- Key shape: ``(release_group_identifier, source, yt_browse_id)`` is the
-- natural-key UNIQUE. ``release_group_identifier`` carries either an MB
-- release-group MBID or a Discogs master ID as TEXT (different ID
-- spaces, same column, ``source`` discriminates). ``yt_browse_id`` is
-- the stable ``MPREb_…`` identifier YouTube Music assigns to the album
-- entity; the ``OLAK5uy_…`` audio-playlist ID is stored separately
-- because (a) it is the public URL handle and (b) it can be NULL for
-- some album entities (rare; documented in the resolver service).
--
-- ``yt_tracks`` persists the per-track snapshot the resolver scored
-- against (title, artists, length_seconds, track_number, disc_number,
-- video_id) so future re-scoring against a newly-added MB sibling can
-- skip the YouTube Music fetch entirely. ``distances`` carries the
-- per-MBID beets-distance results — one entry per MB sibling in the
-- group — preserving per-pair outcomes (R17 partial-failure semantics).
--
-- No TTL. YouTube Music ``MPREb_`` / ``OLAK5uy_`` IDs are stable for
-- the life of an album entity; cache freshness is operator-triggered
-- via the resolver's ``--refresh`` flag, not time-based.

CREATE TABLE youtube_album_mappings (
    id BIGSERIAL PRIMARY KEY,
    release_group_identifier TEXT NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('mb', 'discogs')),
    yt_browse_id TEXT NOT NULL,
    yt_audio_playlist_id TEXT,
    yt_url TEXT NOT NULL,
    yt_year INTEGER,
    yt_track_count INTEGER NOT NULL,
    yt_tracks JSONB NOT NULL,
    distances JSONB NOT NULL,
    resolved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (release_group_identifier, source, yt_browse_id)
);

-- The dominant read path is "give me the full matrix for one
-- release-group / source pair" -- the resolver's cache-first lookup
-- and the operator's ``pipeline-cli youtube-album <id>`` both query
-- on the (release_group_identifier, source) prefix. The UNIQUE above
-- already covers this prefix, but an explicit non-unique index keeps
-- planner choices stable as the table grows.
CREATE INDEX idx_yam_release_group
    ON youtube_album_mappings(release_group_identifier, source);
