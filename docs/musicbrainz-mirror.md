# MusicBrainz Mirror

The dedicated MusicBrainz guest serves the local mirror at
`http://192.168.1.43:5200`. It is used by the web UI browse tab and Beets.

## Common queries

```bash
# Search releases
curl -s "http://192.168.1.43:5200/ws/2/release?query=artist:ARTIST+AND+release:ALBUM&fmt=json"

# Get release with tracks
curl -s "http://192.168.1.43:5200/ws/2/release/MBID?inc=recordings+media&fmt=json"

# Get release group
curl -s "http://192.168.1.43:5200/ws/2/release-group/RGID?inc=releases&fmt=json"
```

## Notes

- The mirror URL is configuration, not a constant:
  `services.cratedigger.musicbrainz.apiBase` supplies Cratedigger web, CLI, and
  pipeline lookups. The external Beets owner configures its own corresponding
  `musicbrainz.*` endpoint; disagreement is a startup warning, not an automatic
  rewrite. Public MB is the supported-but-slow Cratedigger default. Setup and
  fallback math: `docs/mirrors.md`.
- Timeout is ~15s — broad queries (e.g. `artist:Radiohead`) can hit it. Prefer specific artist+album pairs.
- The deployment-owned Beets config uses this mirror; the upstream server is
  only an intentional fallback selected by the librarian.
- Pipeline entries store MB release UUIDs in `album_requests.mb_release_id`. Numeric IDs in the same column indicate a Discogs-sourced release (see `docs/discogs-mirror.md`).

## Merged releases and redirects

MusicBrainz editors merge duplicate release entries. The loser's MBID becomes a
permanent `301` to the survivor, and `mbsync` retags the local files onto it —
both sides doing their job. A stored acquisition ID can therefore name a
release that no longer exists under that ID upstream. Measured 2026-08-06:
**8 of 8,097** stored release IDs are merged, a rate of roughly 6/year.

Cratedigger resolves this **at the point of use and stores nothing**
(`lib/mb_canonical.py`, issue #1049). The stored acquisition ID is frozen
history and is never mutated: it says what was acquired, and that is still
true. There is no alias table, no column, and no backfill — the `301` is
authoritative and always current, so it is asked when needed and thrown away.

Two seams resolve on a miss, never on a scan:

- **The Beets join** (`BeetsDB.resolve_current_releases`) — a release we hold
  only under the survivor's ID resolves instead of reporting
  `current_beets_missing`.
- **Validation and import** (`lib/beets.py::beets_validate`,
  `harness/import_one.py`) — beets is invoked with `--search-id <stored id>`,
  MusicBrainz answers the redirect, so every candidate and every retagged
  duplicate arrives wearing the survivor's ID.

Following a redirect is not inferring a sibling: MusicBrainz declares exactly
one successor or none, and nothing chooses between pressings.

### Trust rules for this mirror

- **`301` → trustworthy.** Verified against public MB on every cross-check.
- **`4xx` → NEVER read as "this release was deleted."** The WS/2 app layer has
  served poisoned 404s that the mirror's own PostgreSQL contradicted (a
  TTL-less Valkey gid key replication cannot invalidate; flushed, with a
  durable post-replication flush deployed in nixosconfig `e823e104`). A bogus
  UUID answers **`400`**, not `404`, so neither code means absence. Escalate to
  public MB before concluding deletion.
- **`200` → not proof of currency** for ~25 h (≤24 h replication lag plus ≤1 h
  stale entity cache).

Resolution is fail-open by contract: any failure leaves the stored ID in force,
so a mirror that is down, rate-limiting, or serving a 4xx is never worse than
not asking. It is also inert until a process wires a base — an entry point that
skips `configure_api_bases_from_runtime_config()` keeps the literal ID rather
than silently reaching public MusicBrainz.

```bash
# Full-catalogue redirect sweep (read-only, ~40s), run on doc2.
# Include status='replaced' rows — frozen rows can still own library albums.
xargs -P 6 -I@ curl -s -m 25 -o /dev/null \
  -w "@\t%{http_code}\t%{redirect_url}\n" \
  "http://192.168.1.43:5200/ws/2/release/@?fmt=json" < ids.txt
```
