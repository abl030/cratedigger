---
title: "Plex partial scan returns HTTP 200 for any path — including invalid ones"
date: 2026-05-09
category: runtime-errors
problem_type: silent-failure
component: plex-notifier
tags:
  - plex
  - notifier
  - path-translation
  - beets
  - silent-failure
related_prs:
  - https://github.com/abl030/cratedigger/pull/236
related_commits:
  - 0d8d18d  # initial path_map (April 2 — built on wrong assumption)
  - 189d98e  # this fix (May 9)
---

# Plex partial scan returns HTTP 200 for any path — including invalid ones

## Context

Cratedigger triggers Plex partial scans after each import via
`lib/util.py::trigger_plex_scan(cfg, imported_path)`. The function appends
`?path=<imported_path>` to `/library/sections/<id>/refresh` and applies
`plex_path_map` to translate the local filesystem path into the Docker
container's mount path (e.g., `/mnt/virtio/Music/Beets` → `/prom_music`).

The user reported that albums showed as "this album is unavailable" or
didn't appear at all in Plex, with the trigger seeming to work
"sometimes". Cratedigger logs were green: `PLEX: triggered partial scan
for ... (HTTP 200)` for every import.

## Root cause — two interacting facts

1. **Plex's `/library/sections/<id>/refresh?path=...` endpoint returns
   HTTP 200 regardless of the input.** It returns 200 for valid paths,
   for paths outside any library section, for nonsense like
   `?path=/nonexistent/foo`, and for malformed strings. There is no error
   surface — the only way to verify a partial scan is real is to hit the
   API with a path matching the section's `Location`, then observe the
   library state change.

2. **Beets stores file paths in its SQLite library DB as relative to its
   `directory:` root.** With `directory: /mnt/virtio/Music/Beets`, the
   `items.path` column contains values like
   `Roky Erickson with Okkervil River/2010 - True Love Cast Out All Evil/01 …opus`.
   Anything that decodes that path and treats it as absolute will misbehave.

Combine the two: `BeetsDB.get_album_info()` returns the relative
directory; the dispatch path passes it as `PostflightInfo.imported_path`;
`trigger_plex_scan` had `if scan_path.startswith(local_prefix)` which
never matches a relative path, so the path-map substitution silently
no-opped; Plex received `?path=Roky%20Erickson%20with%20Okkervil…`,
couldn't match it to any library section's `Location`, returned 200,
and scanned nothing. New albums appeared only when Plex's daily
scheduled full scan ran.

## Guidance

### Plex API verification rule

> **HTTP 200 from Plex's refresh endpoint is not evidence the scan
> happened.** It only means the request was authenticated and well-formed.

When you change anything that affects how cratedigger talks to Plex,
verify by observing **library state**, not return codes. The cheapest
check:

```bash
# 1. Trigger the scan with the path you expect to send
ssh doc2 'TOKEN=$(sudo cat /run/cratedigger-secrets/PLEX_TOKEN); \
  curl -s -o /dev/null -w "HTTP %{http_code}\n" \
  "https://plex.ablz.au/library/sections/3/refresh?path=<URL-encoded>&X-Plex-Token=$TOKEN"'

# 2. Wait a few seconds, then search Plex for the title
ssh doc2 'TOKEN=$(sudo cat /run/cratedigger-secrets/PLEX_TOKEN); \
  curl -s "https://plex.ablz.au/library/sections/3/search?type=9&title=<title>&X-Plex-Token=$TOKEN" \
  | grep -oE "title=\"[^\"]*\"|key=\"[^\"]*\""'
```

If the search doesn't return the album, the scan didn't actually run —
regardless of the HTTP status.

### Beets path semantics

> **`BeetsDB.get_album_info().album_path` is RELATIVE to the beets
> library root.** Any consumer that does `os.path.dirname()`,
> `os.path.isdir()`, `os.listdir()`, or string-based prefix matching on
> it must handle relative paths.

Current consumers in this repo and their state:

- `lib/util.py::trigger_plex_scan` — fixed (PR #236).
- `lib/util.py::cleanup_disambiguation_orphans` — same latent assumption
  (`os.path.dirname` then `os.path.isdir` on a relative path). Only runs
  on the rare `ir.postflight.disambiguated == True` branch so hasn't been
  noticed; worth a follow-up to either absolutize at one source or harden
  each consumer.
- The web UI displays `album_requests.imported_path` as-is — semantics
  there are "relative to the beets library", which is fine for display.

### When designing path-translation logic

If you write a `local_prefix:container_prefix` style translator, accept
both shapes the upstream actually emits, and **fail loud** when neither
matches. Silent no-ops on a translator that exists *because* paths need
translating are a 5-week regression waiting to happen.

```python
# Pattern that would have prevented this bug
if scan_path.startswith(local_prefix):
    scan_path = container_prefix + scan_path[len(local_prefix):]
elif not os.path.isabs(scan_path):
    scan_path = container_prefix.rstrip("/") + "/" + scan_path
else:
    logger.warning(f"PLEX: unmappable path {scan_path!r}; Plex may no-op")
```

## Why the test suite didn't catch it

`tests/test_util.py::TestTriggerPlexScan` had several tests but **none
exercised `plex_path_map`** — the test `_make_cfg` defaulted
`path_map=None`. So the substitution branch was unreached. The fix in
PR #236 adds three tests covering: relative path anchoring (the bug),
absolute path substitution (the original April-2 behavior), and the
defensive warning on unmappable absolute paths.

## When to apply

- Adding any new path-translation between cratedigger's filesystem and a
  containerized media server (Plex, Jellyfin, Navidrome, etc.).
- Touching `lib/util.py::trigger_*_scan` functions.
- Wiring any new consumer of `PostflightInfo.imported_path` or
  `album_requests.imported_path`.
- Debugging "the trigger fires but the album doesn't show up" reports
  for any media server with a `?path=` API.
