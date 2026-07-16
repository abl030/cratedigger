-- 053_jellyfin_pin_floor.sql
--
-- Path-changing upgrades (issue: Arcade Fire "B-Sides & Rarities",
-- 2026-07-16) rename the beets album folder (e.g. a year-token drift when
-- MusicBrainz metadata changes), so at capture time the pre-upgrade Jellyfin
-- album exists only at the OLD path. Jellyfin item identity is an MD5 of the
-- item path, so the rescan deletes the old items and mints fresh ones stamped
-- "now" — the upgrade wrongly tops 'Recently Added'.
--
-- Capture now falls back to the replaced beets albums' old paths (threaded
-- from the harness dup-guard), and when no pre-upgrade item is findable at
-- all it still writes a FLOOR pin: original_date_created comes from the
-- pipeline's own knowledge (Plex's preserved addedAt, else the oldest
-- created_at across the request's replace chain) and there is no item-id
-- snapshot — the pin lands once ANY album with children appears at the new
-- path. A floor pin therefore has no album_item_id.
ALTER TABLE jellyfin_date_created_pins
    ALTER COLUMN album_item_id DROP NOT NULL;
