---
name: project-574-jellyfin-recently-added
description: "Issue #574 COMPLETE 2026-07-10 (PR #592, v2026.07.10-3): Jellyfin DateCreated pin deployed + 92.8k-item backfill; Jellyfin API traps (full-dto POST, no path filter, replace-all-metadata re-stamps from ctime)"
metadata: 
  node_type: memory
  type: project
  originSessionId: 2b48efce-109c-42e5-b1b3-6606caed0e65
---

Issue #574 (Jellyfin "Recently Added" shows upgrades): CLOSED 2026-07-10. PR #592 merged (7a7c764), deployed via fleet-update, tagged v2026.07.10-3 after live verification (capture + reconcile `waiting=1` hold observed in production; test pin removed). Follow-ups filed as issue #593 (unify-or-document pin orchestration, pin-table pruning, expose notifiers.jellyfin.libraryId — module never renders library_id so refreshes are full-library). nixosconfig wrapper got `notifiers.jellyfin.pathMap = "/mnt/virtio/Music/Beets:/mnt/fuse/Media/Music/Beets"` (commit 9c74c865). Design doc: docs/jellyfin-primer.md.

Backfill one-shot (agent-driven, deleted after run, log kept at doc2:/tmp/jf_backfill.log): 92,817/92,822 drifted Audio items re-dated to their album's DateCreated in ~50 min, 5 stale-id 404s. Two corrections after operator review: (1) Peter Broderick "(Colours of the Night) Satellite" was a FRESH import, not an upgrade — the agent's validation hand-edit had wrongly dated it 2026-04-26; restored to its true 2026-07-09 import date. Lesson: **absence of a plex_added_at_pins row for an imported_path proves fresh-vs-upgrade** (capture writes nothing when Plex had no album there). (2) 46 upgraded albums whose Jellyfin album item had been recreated by past mass events (2026-05-18 asciify, 06-10) read their recreation date; fixed album+children (563 items, 0 fails) to max(plex pin original, 2026-04-26 bulk-add epoch). Root cause of the LIBRARY-WIDE drift (6,995/8,518 albums, not just upgrades): the 2026-07-09 #570 permissions sweep bumped every file's ctime, and the operator's follow-up Jellyfin metadata refresh re-stamped DateCreated from ctime. **If "Replace all metadata" is ever run on the music library again, dates re-scramble the same way and the one-shot must be re-run** (recipe: page Audio+MusicAlbum items under library 7e64e319657a9516ec78490da03edccb with fields=DateCreated,AlbumId,Path; for each Beets-tree child whose date != album date, GET full dto /Items/{id}?userId=<first user> → set DateCreated → POST /Items/{id}; ~4 threads, ~30 req/s total). The pin feature only protects the per-upgrade path.

Non-obvious Jellyfin 10.11 facts (all verified live):
- Latest/"Recently Added" orders by the **Audio children's** DateCreated, not the album's; upgrades re-stamp children from file ctime (=import time), ~1/3 recreate the album item too.
- `POST /Items/{id}` REPLACES metadata — must round-trip the full dto from `GET /Items/{id}?userId=<any user>` (userId mandatory; POST returns 204). DateCreated is only stamped at item creation, so a one-time restore sticks; no lock exists.
- `/Items` has NO path filter — an unknown `path` param degenerates to an unfiltered recursive sweep that 504s through nginx. Find by searchTerm + exact `Path` equality.
- Music library item id `7e64e319657a9516ec78490da03edccb`; token at doc2:/run/cratedigger-secrets/JELLYFIN_TOKEN. In zsh one-liners on doc2, `UID` is readonly — use another variable name.
- Reconciler deliberately departs from the Plex settle-window design: waits until the rescan is *observable* (album id / children id-set drift vs capture snapshot) and *settled* (non-empty children), 48h TTL → `expired`. Cycle log line: `JELLYFIN PIN reconcile: pinned=… waiting=…`.
