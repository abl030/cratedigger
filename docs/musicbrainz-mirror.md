# MusicBrainz Mirror

Local mirror on doc2 at `http://192.168.1.35:5200`. Used by the web UI browse tab and by beets.

## Common queries

```bash
# Search releases
curl -s "http://192.168.1.35:5200/ws/2/release?query=artist:ARTIST+AND+release:ALBUM&fmt=json"

# Get release with tracks
curl -s "http://192.168.1.35:5200/ws/2/release/MBID?inc=recordings+media&fmt=json"

# Get release group
curl -s "http://192.168.1.35:5200/ws/2/release-group/RGID?inc=releases&fmt=json"
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
