# Discogs Mirror Primer

## What It Is

A self-hosted mirror of the Discogs music database, serving a JSON API at `https://discogs.ablz.au`. Built in Rust, imports monthly CC0 XML dumps (~19M releases) into PostgreSQL, provides full-text search and entity lookups. Intended as the Discogs counterpart to the MusicBrainz mirror for release disambiguation in cratedigger-web.

- **Source repo**: https://github.com/abl030/discogs-api
- **Live API**: https://discogs.ablz.au
- **Data source**: https://data.discogs.com/ (CC0, monthly XML dumps)
- **Language**: Rust (edition 2024)
- **NixOS module**: `nixosconfig/modules/nixos/services/discogs.nix`

## Where It Runs

| What | Value |
|------|-------|
| Guest | dedicated unprivileged Proxmox CT 102 (`192.168.1.44`) |
| API port | 8086 |
| External URL | https://discogs.ablz.au |
| PostgreSQL | native `postgresql.service` inside CT 102 |
| Import coordination | doc2 `discogs-import.service` through the metadata gate |

The exact guest storage, deployment, and rollback layout is owned by
`nixosconfig/docs/wiki/services/discogs.md` and its linked metadata-mirror
migration runbook. Do not use the retired doc2 nspawn database as the active
endpoint.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Status, release count, last import time, dump date |
| GET | `/api/search?artist=X&title=Y&artist_id=N&page=1&per_page=25` | Full-text search, enriched with artists/labels/formats. `artist_id` is an exact structural filter (release credited to that artist id) — `web/discogs.py::search_releases` pins `artist_id=194` (Various Artists, whose name row is absent from the dump and can't match the text index) for VA compilation queries. See #199. |
| GET | `/api/releases/{id}` | Full release: tracks, genres, styles, identifiers |
| GET | `/api/masters/{id}` | Master release with all child releases |
| GET | `/api/artists/{id}` | Artist profile, aliases, name variations |
| GET | `/api/artists/{id}/masters?page=1&per_page=100` | Paginated primary-credit masters and masterless releases; each row includes required sorted/deduplicated `primary_types` (`Album`, `EP`, `Single`), `format_qualifiers`, and `provenance` (`ordinary`, `promo`, `unofficial`), all aggregated across every child pressing |
| GET | `/api/artists/{id}/masters/all` | All primary-credit masters and masterless releases in the same strict response shape, projected set-wise for cold large-artist reads |
| GET | `/api/artists/{id}/appearances` | Track-credit appearances in the same strict row shape as `/masters`, including all three required evidence arrays |
| GET | `/api/labels?name=X&page=1&per_page=25` | Label search with release counts and parent-label context |
| GET | `/api/labels/{id}` | Label profile, direct release count, parent, and direct sub-labels |
| GET | `/api/labels/{id}/releases?page=1&per_page=100&include_sublabels=true` | Paginated releases for a label, optionally including recursive sub-label releases |

Label release pagination is capped at `per_page=100` in both cratedigger and
discogs-api. Release rows currently include both `label_id` and the legacy
`via_label_id` key during the cross-repo rollout; new code should read
`label_id`.

The label releases endpoint can return `503 Service Unavailable` with
`{"error":"timeout","label_id":<id>}` when `include_sublabels=true` exceeds
the mirror's 15 second recursive CTE statement timeout. Cratedigger's
`web.discogs.get_label_releases()` retries once with `include_sublabels=false`
on HTTP 503 or timeout-class upstream failures and returns
`sub_labels_dropped=true` so the UI can show direct releases instead of failing
the label page.

Cratedigger artist pages use the explicit `/masters/all` route plus
`/appearances`. They never turn a bulk read into a query option on the legacy
paginated route and never fall back to page walking: an older mirror therefore
fails loudly instead of silently returning the first page. Both bulk envelopes
are accepted only when `page == 1` and the result count equals `total`;
`per_page` remains informational. The normalized catalogue and the outer
cross-source compare skeleton have independent 24-hour Redis keys. Concurrent
cold misses for either key are process-local single-flight fills, with
deep-copied results per request so live overlays cannot mutate another caller's
metadata. A client disconnect during the fill does not cancel it, so the next
request can consume the completed cache entry without refetching.

The three evidence arrays are required, sorted, and duplicate-free; missing,
wrongly typed, unsorted, or duplicated fields fail the artist request instead
of silently falling back to the legacy representative-pressing scalar
`type`. Cratedigger normalizes master rows as work identities and masterless
rows as release identities. Both may enter the conservative cross-source
association policy, but a masterless row always remains a release identity.
Unmatched release identities stay conserved in the diagnostic response and
navigate to the exact release endpoint from the artist page's collapsed Other
releases area; they do not enter the work-level Missing buckets. Association
never substitutes a pressing or confers counterpart ownership. A mixed master (for
example, ordinary plus unofficial child pressings) keeps every provenance
value so the UI can display that evidence without misclassifying the whole
work as an ordinary or unofficial-only album. For an associated pair, positive
MusicBrainz release-group primary or secondary type evidence authors the work's
display section; Discogs structural and format-qualifier evidence is the
fallback only when that MB work classification is genuinely unknown. This
prevents edition-level Discogs Compilation/Live/etc. qualifiers from overriding
a known MB work type while preserving the selected exact Discogs identity.

Artist-row identity syntax is strict at the Cratedigger boundary: a master is
a positive integer and a masterless row is exactly
`release-<positive integer>`. Values such as `foo`, `release-`,
`release-abc`, `release-0`, zero, negative IDs, and numeric strings in the
master namespace fail the request instead of being normalized into ambiguous
catalogue identities.

### API Examples

```bash
# Health check
curl https://discogs.ablz.au/health
# {"status":"ok","releases":19035253,"last_import":"2026-04-12T06:03:26...","dump_date":"20260401"}

# Search by artist + title (both optional, at least one required)
curl 'https://discogs.ablz.au/api/search?artist=Radiohead&title=OK+Computer'

# Title-only search with pagination
curl 'https://discogs.ablz.au/api/search?title=Blue+Train&page=2&per_page=10'

# Full release detail (tracklist, genres, identifiers, etc.)
curl https://discogs.ablz.au/api/releases/83182

# All pressings of a master release
curl https://discogs.ablz.au/api/masters/21491

# Artist profile with aliases
curl https://discogs.ablz.au/api/artists/3840
```

Search uses PostgreSQL GIN full-text indexes. Artist search does an EXISTS subquery against `release_artist` joined to `artist`. Results are enriched with artists, labels, and formats per release in batch (no N+1).

## Architecture

```
data.discogs.com (monthly XML dumps, ~12 GB compressed)
        |
        v  doc2 coordinator + metadata hold
+-----------------------------+
| CT 102 discogs-import       |
| Rust: quick-xml streaming   |
| -> binary COPY into PG     |
| 10K batch, channel pipeline |
+-------------+---------------+
              v
+-----------------------------+
| native PostgreSQL           |
| dedicated unprivileged LXC  |
| 192.168.1.44                |
+-------------+---------------+
              v
+-----------------------------+
| CT 102 discogs-api          |
| Rust: axum HTTP server      |
| port 8086                   |
| discogs.ablz.au             |
+-----------------------------+
```

Two binaries from one crate:
- **`discogs-import`**: discovers latest dump, downloads to `.partial` (atomic rename), streams XML through `flate2::GzDecoder`, parses with `quick-xml`, sends 10K batches through an `mpsc` channel to async binary COPY. Full import ~18 minutes for 19M releases.
- **`discogs-api`**: axum server with `tokio-postgres` through a
  `deadpool-postgres` connection pool. Each HTTP query helper acquires one
  connection for the request; the importer remains a separate single-client
  COPY pipeline. The pool has bounded wait/create/recycle timeouts so saturated
  label-release requests shed as 503 instead of waiting indefinitely.

## Source Repo Structure

The source lives at `/home/abl030/discogs-api` (and https://github.com/abl030/discogs-api):

```
src/
  types.rs     -- Import entity structs + API response types (serde)
  schema.rs    -- DDL constants: CREATE TABLE, indexes, VACUUM
  xml.rs       -- Streaming XML parsers for artists/labels/masters/releases
  db.rs        -- Postgres: connect, binary COPY helpers, query helpers
  import.rs    -- Binary: CLI, download, parse+COPY pipeline
  server.rs    -- Binary: axum routes + handlers
  lib.rs       -- Module root (re-exports db, xml, types, schema)
docs/
  plan.md      -- Original architecture plan
```

## NixOS Configuration

Module: `nixosconfig/modules/nixos/services/discogs.nix`

The dedicated guest module owns native PostgreSQL, `discogs-api.service`, and
`discogs-import.service`. The guest-local import timer is disabled: doc2 owns
the monthly schedule, enters the durable `discogs-import` metadata hold, and
invokes the guest through a restricted forced-command SSH boundary. Cratedigger
resumes only after both Discogs and MusicBrainz representative probes pass.

Flake input: `discogs-src` (non-flake, `github:abl030/discogs-api`). The Rust crate is built with `pkgs.rustPlatform.buildRustPackage`.

## Database Schema

16 tables. ~80-120 GB with indexes after full import.

**Core entities**: `artist`, `label`, `master`, `release`

**Relations**: `release_artist`, `release_label`, `release_format`, `release_track`, `release_track_artist`, `release_genre`, `release_style`, `release_identifier`, `artist_alias`, `artist_namevariation`, `master_artist`

**Metadata**: `import_meta` (key-value: `last_import`, `dump_date`)

Full DDL is in `src/schema.rs`. Indexes: B-tree on all FK columns, GIN full-text on `release.title` and `artist.name`.

## Editing, Fixing, and Redeploying

### Making code changes

```bash
cd ~/discogs-api

# Edit the code
# ... make changes to src/*.rs ...

# Check it compiles
nix-shell -p cargo rustc pkg-config openssl --run "cargo check"

# Run tests (XML parser tests)
nix-shell -p cargo rustc pkg-config openssl --run "cargo test"

# Commit and push
git add -A && git commit -m "description" && git push
```

### Deploying changes

```bash
# Update nixosconfig flake lock to pick up the new commit
cd ~/nixosconfig
nix flake update discogs-src
git add flake.lock
git commit -S -m "discogs: description"
# Push the signed commit to the Forgejo deployment root.
```

Deploy the dedicated metadata guest from doc1 using the current procedure in
`nixosconfig/docs/wiki/services/discogs.md` and
`nixosconfig/docs/wiki/infrastructure/metadata-mirror-lxc-migration.md`. Do not rebuild
doc2 from GitHub or reactivate its frozen nspawn rollback source. The API
service restarts on guest deployment; the importer remains coordinator-driven.

### Debugging

```bash
# Probe the active guest from the Cratedigger host.
ssh doc2 'curl -fsS http://192.168.1.44:8086/health | jq'

# Inspect the doc2-side coordinator and durable hold.
ssh doc2 'systemctl status discogs-import.service discogs-import.timer'
ssh doc2 'sudo cratedigger-metadata-gate status'
ssh doc2 'journalctl -u discogs-import.service -f'

# Start a coordinated manual import without blocking SSH. This enters the
# hold before invoking CT 102 and releases only after representative probes.
ssh doc2 'sudo systemctl start discogs-import.service --no-block'
```

For guest-local `discogs-api.service`, `discogs-import.service`, PostgreSQL,
and database diagnostics, enter CT 102 through the nixosconfig metadata-guest
runbook. Do not query or restart the frozen doc2 rollback database.

### Common issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| Health returns `{"status":"awaiting_import"}` | CT 102 has no admitted release data | Start the coordinated doc2 remote-import service and keep the metadata hold until it succeeds. |
| Import fails with "unexpected end of file" | Truncated download from a previous interrupted run | Follow the CT 102 storage runbook, delete only the named corrupt guest dump, and re-run through the doc2 coordinator. Downloads use atomic `.partial` rename. |
| Import fails with "no dumps found" | data.discogs.com HTML format changed | Check `curl https://data.discogs.com/` and fix `discover_latest_dump()` in `src/import.rs` |
| API returns 500 on search | Guest PostgreSQL is unavailable or tables are missing | Check native PostgreSQL and `discogs-api.service` inside CT 102. |
| VACUUM warnings about `pg_authid` | Non-superuser can't vacuum system catalogs | Harmless. VACUUM is scoped to owned tables in latest code. |

### Key files to edit

| Task | File |
|------|------|
| Change API response shape | `src/types.rs` (API structs) + `src/db.rs` (query functions) |
| Change database schema | `src/schema.rs` + `src/db.rs` (COPY + query functions) |
| Fix XML parsing bugs | `src/xml.rs` (state machine parsers, one per entity type) |
| Fix download/import issues | `src/import.rs` (discovery, download, pipeline orchestration) |
| Change API routes or add endpoints | `src/server.rs` (axum handlers) |
| Change NixOS service config | `nixosconfig/modules/nixos/services/discogs.nix` |

## Comparison to MusicBrainz Mirror

| Aspect | MusicBrainz | Discogs |
|--------|-------------|---------|
| Deployment | dedicated CT 100: native PG + explicit Podman app units | dedicated CT 102: native PG + Rust API |
| DB size | ~100 GB+ including search indexes | ~80-120 GB |
| Replication | Daily | Monthly full re-import |
| API | Included (Perl webapp) | Custom Rust (this project) |
| Search | Solr | Postgres FTS (GIN) |
| Data freshness | ~24h lag | ~30 day lag |
| License | CC BY-NC-SA | CC0 |
| Import time | ~6h initial | ~18 min |
