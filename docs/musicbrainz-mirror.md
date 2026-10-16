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

## Materialized tables are required

A mirror must have upstream's materialized tables built
(`./admin/BuildMaterializedTables --database=MAINTENANCE all`, run once
after the import; replication keeps them current afterwards).
`examples/musicbrainz-mirror.nix` builds them with a unit that is a no-op
once they exist.

**Symptom without them:** an artist page reads "Artist page temporarily
unavailable", and `cratedigger-web` logs
`MusicBrainzArtistCatalogueIncomplete: browse did not conserve N unique
identities` for `/api/artist/<mbid>` and `/api/artist/compare`. The guard
in `lib/mb_api.py::_fetch_browse_pages` is correct: the mirror really did
return an incomplete catalogue.

**Why:** MusicBrainz serves `/ws/2/release?artist=` from the
`artist_release` table, ordered by a key ending in the release id. When
that table is empty it falls back to a query ordered only by date,
country, barcode and title. Releases tied on all of those can then fall
on LIMIT/OFFSET pages inconsistently, so one appears twice and a sibling
pressing never appears. Measured on the operator's mirror on 2026-10-16:
Stevie Wonder's browse returned 820 rows but 819 releases. *In Square
Circle* (ES, 1985, catalogue `SPL1-60223`) came back twice and
`SPK1-60223` never; the two differ only in catalogue number. Public
MusicBrainz returned all 820.

**Check:** on the mirror database,

```sql
SELECT (SELECT count(*) FROM artist_release)       AS artist_release,
       (SELECT count(*) FROM artist_release_group) AS artist_release_group;
```

Both should be non-zero.

**Unrelated but similar:** a mirror's PostgreSQL that logs
`database "musicbrainz_db" has a collation version mismatch` after an OS or
image upgrade has text indexes in the old sort order. Rebuild them, then
record the new version: `REINDEX DATABASE musicbrainz_db;` followed by
`ALTER DATABASE musicbrainz_db REFRESH COLLATION VERSION;`. This did not
cause the browse defect above; that persisted after the reindex.
