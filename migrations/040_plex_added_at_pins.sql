-- 040_plex_added_at_pins.sql
--
-- Plex "Recently Added" pin store. When an album is re-acquired at higher
-- quality (an upgrade re-import), beets replaces the on-disk files — often
-- with a new extension (FLAC/MP3 -> Opus) — and the post-import Plex partial
-- scan makes Plex re-stamp the album's ``addedAt`` to now, so the album
-- wrongly jumps to the top of "Recently Added".
--
-- The fix is a capture-then-reconcile loop. At import time, BEFORE firing the
-- Plex refresh, we read the album currently at the folder and record its
-- original ``addedAt`` here (one row, status='pending'). The old item still
-- carries the pre-upgrade date because the refresh has not run yet. A deferred
-- reconciler (the 5-min cratedigger cycle) later re-finds the album by folder
-- path and, if Plex bumped the date, writes the original value back with the
-- field locked so future metadata refreshes do not clobber it.
--
-- Genuinely-new albums are not in Plex at capture time, so the finder returns
-- nothing and no pin row is written — the table self-selects upgrades.
--
-- ``original_added_at`` is a Unix epoch (seconds), the wire format Plex's
-- ``addedAt`` uses. ``status`` is one of 'pending' | 'done' | 'skipped'.

CREATE TABLE IF NOT EXISTS plex_added_at_pins (
    id                BIGSERIAL PRIMARY KEY,
    request_id        BIGINT,
    imported_path     TEXT        NOT NULL,
    original_added_at BIGINT      NOT NULL,
    rating_key        TEXT,
    status            TEXT        NOT NULL DEFAULT 'pending',
    captured_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    reconciled_at     TIMESTAMPTZ
);

-- The reconciler reads "pending pins captured before <cutoff>, oldest first".
-- A partial index keyed on captured_at keeps that scan cheap as done/skipped
-- rows accumulate.
CREATE INDEX IF NOT EXISTS idx_plex_added_at_pins_pending
    ON plex_added_at_pins (captured_at)
    WHERE status = 'pending';
