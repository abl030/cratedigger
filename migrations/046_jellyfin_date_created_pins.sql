-- 046_jellyfin_date_created_pins.sql
--
-- Jellyfin "Recently Added" pin store (issue #574) — the Jellyfin sibling of
-- migration 040's plex_added_at_pins. When an album is re-acquired at higher
-- quality (an upgrade re-import), beets replaces the on-disk files and the
-- Jellyfin rescan deletes + recreates the album's Audio items, stamping each
-- new item's ``DateCreated`` from file ctime (= import time). Jellyfin's
-- "Recently Added"/Latest row orders MusicAlbums by their children's
-- ``DateCreated``, so the upgraded album wrongly jumps to the top. Sometimes
-- the MusicAlbum item itself is recreated too (observed live 2026-07-10:
-- 1 of 3 upgrades), bumping the album's own date as well.
--
-- The fix is a capture-then-reconcile loop. At import time, BEFORE firing the
-- Jellyfin refresh, we locate the album by folder path and record its current
-- ``DateCreated`` here, along with a snapshot of the album item id and its
-- Audio children ids. Genuinely-new albums are not in Jellyfin yet, so no pin
-- is written — the table self-selects upgrades.
--
-- Unlike Plex there is no ``locked`` field, and our refresh trigger is a
-- full-library scan whose completion time is unbounded (and inotify on the
-- fuse mount Jellyfin reads may never fire), so a fixed settle window is not
-- enough. Instead the reconciler holds a pin pending until the rescan is
-- OBSERVABLE — the album item id or the children id-set differs from the
-- snapshot — then writes the original ``DateCreated`` back onto the album and
-- every drifted child. Jellyfin only stamps ``DateCreated`` at item creation,
-- so the restored value sticks without a lock. A pin whose rescan never
-- becomes observable (e.g. a same-filename upgrade that keeps item ids, which
-- Jellyfin doesn't re-stamp at all) expires after a TTL.
--
-- ``original_date_created`` is Jellyfin's ISO-8601 string, stored verbatim so
-- the write-back round-trips exactly. ``status`` is one of
-- 'pending' | 'done' | 'skipped' | 'expired'.

CREATE TABLE IF NOT EXISTS jellyfin_date_created_pins (
    id                    BIGSERIAL PRIMARY KEY,
    request_id            BIGINT,
    imported_path         TEXT        NOT NULL,
    original_date_created TEXT        NOT NULL,
    album_item_id         TEXT        NOT NULL,
    children_item_ids     JSONB       NOT NULL DEFAULT '[]'::jsonb,
    status                TEXT        NOT NULL DEFAULT 'pending',
    captured_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    reconciled_at         TIMESTAMPTZ
);

-- The reconciler reads "pending pins captured before <cutoff>, oldest first".
-- A partial index keyed on captured_at keeps that scan cheap as terminal
-- rows accumulate.
CREATE INDEX IF NOT EXISTS idx_jellyfin_date_created_pins_pending
    ON jellyfin_date_created_pins (captured_at)
    WHERE status = 'pending';
