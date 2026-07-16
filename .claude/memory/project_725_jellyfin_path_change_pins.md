---
name: project-725-jellyfin-path-change-pins
description: PR
metadata: 
  node_type: memory
  type: project
  originSessionId: abe899f8-274b-4934-bf94-d7b238ae47e0
---

2026-07-16, PR #725 merged + deployed (nixosconfig 64470185, migration 053), live-verified.

**Incident**: request 8504 (Arcade Fire "B-Sides & Rarities") upgrade renamed the beets folder `2007 - …` → `0000 - …` (MB dropped the release date). Pin capture looked up Jellyfin by the NEW path only → `no_album` → upgrade topped Recently Added. 34 imports since the 046 pin shipped, 33 pins — this was the only miss.

**Jellyfin facts (verified from source, master @ f8771b52)**: item ID = MD5(type+path), no provider-id reconciliation, path change = delete+create in one scan pass; new items get DateCreated from folder ctime; music Latest sorts on the **album item's own DateCreated** (children only qualify inclusion); any refresh where file mtime drifts >1s re-stamps DateCreated from ctime. Plex is immune to path changes (metadata-keyed identity — addedAt survived untouched).

**Fix shape**: dup-guard allowed removals cross the wire as `postflight.replaced_albums`; capture falls back new-path → replaced old paths → FLOOR pin (`album_item_id` NULL) dated min(Plex addedAt, oldest created_at over the replace chain, via `get_oldest_request_chain_created_at`); reconcile treats absent-album as WAIT (skipped only at TTL) and NULL snapshot as landed-on-any-album. `_date_newer` compares chronologically (Jellyfin 7-digit fractions vs our seconds-only strings mis-sort lexically).

**Repair**: agent one-shot inserted floor pin 35 (original `2026-04-01T12:51:14Z` from Plex); the deployed production reconciler restored album + 19 children — doubling as live verification of the new path.

Deferred: [[project-725-jellyfin-path-change-pins]] follow-up issue #727 (generated harness `_DATES` single-format; checkers use string compares).
