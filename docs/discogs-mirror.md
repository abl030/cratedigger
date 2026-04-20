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
| Host | doc2 (192.168.1.35, Proxmox VM) |
| API port | 8086 |
| External URL | https://discogs.ablz.au |
| PostgreSQL | nspawn container `discogs-db`, hostNum=6, IP 192.168.100.13:5432 |
| DSN | `postgresql://discogs@192.168.100.13:5432/discogs` |
| Data dir | `/mnt/mirrors/discogs` (re-downloadable, NOT backed up) |
| Postgres data | `/mnt/mirrors/discogs/postgres` |
| Dump files | `/mnt/mirrors/discogs/dumps` (cleaned up after each import) |

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Status, release count, last import time, dump date |
| GET | `/api/search?artist=X&title=Y&page=1&per_page=25` | Full-text search, enriched with artists/labels/formats |
| GET | `/api/releases/{id}` | Full release: tracks, genres, styles, identifiers |
| GET | `/api/masters/{id}` | Master release with all child releases |
| GET | `/api/artists/{id}` | Artist profile, aliases, name variations |
| GET | `/api/artists/{id}/releases?page=1&per_page=100` | Paginated releases for an artist (includes masterless) |

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
        v  systemd timer (2nd of month, 04:00)
+-----------------------------+
| discogs-import (oneshot)    |
| Rust: quick-xml streaming   |
| -> binary COPY into PG     |
| 10K batch, channel pipeline |
+-------------+---------------+
              v
+-----------------------------+
| container@discogs-db        |
| nspawn, PostgreSQL 16       |
| hostNum=6                   |
| 192.168.100.12 / .13       |
+-------------+---------------+
              v
+-----------------------------+
| discogs-api (systemd)       |
| Rust: axum HTTP server      |
| port 8086                   |
| discogs.ablz.au             |
+-----------------------------+
```

Two binaries from one crate:
- **`discogs-import`**: discovers latest dump, downloads to `.partial` (atomic rename), streams XML through `flate2::GzDecoder`, parses with `quick-xml`, sends 10K batches through an `mpsc` channel to async binary COPY. Full import ~18 minutes for 19M releases.
- **`discogs-api`**: axum server with `tokio-postgres`. Single connection, no pool (low-traffic internal API).

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

```nix
homelab.services.discogs = {
  enable = true;
  mirrorDir = "/mnt/mirrors/discogs";  # dumps + postgres data
  apiPort = 8086;                       # default
};
```

The module creates:
- `containers.discogs-db` -- nspawn PG container via `mk-pg-container.nix` (hostNum=6)
- `discogs-import.service` -- oneshot importer
- `discogs-import.timer` -- monthly trigger (`*-*-02 04:00:00`)
- `discogs-api.service` -- long-running API server
- `localProxy` entry for `discogs.ablz.au` (auto ACME + Cloudflare DNS)

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
git add flake.lock && git commit -m "discogs: description" && git push

# Deploy to doc2
ssh doc2 'sudo nixos-rebuild switch --flake github:abl030/nixosconfig#doc2 --refresh'
```

The API service restarts automatically on deploy. The import service does NOT restart (it's a timer-triggered oneshot).

### Debugging

```bash
# Check service status
ssh doc2 'systemctl status discogs-api.service'
ssh doc2 'systemctl status container@discogs-db.service'

# API logs
ssh doc2 'journalctl -u discogs-api.service -f'

# Import logs (if running)
ssh doc2 'journalctl -u discogs-import.service -f'

# Test API directly on doc2
ssh doc2 'curl -s http://127.0.0.1:8086/health'

# Query Postgres directly
ssh doc2 'psql -h 192.168.100.13 -U discogs -d discogs -c "SELECT count(*) FROM release"'

# Restart the API
ssh doc2 'sudo systemctl restart discogs-api.service'

# Run a manual import (drops all data and re-imports)
ssh doc2 'sudo systemctl start discogs-import'
ssh doc2 'journalctl -u discogs-import -f'  # watch progress
```

### Common issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| Health returns `{"status":"awaiting_import"}` | No data imported yet | Run `sudo systemctl start discogs-import` on doc2 |
| Import fails with "unexpected end of file" | Truncated download from a previous interrupted run | Delete the corrupt file in `/mnt/mirrors/discogs/dumps/` and re-run. Downloads are now atomic (`.partial` rename) so this shouldn't recur. |
| Import fails with "no dumps found" | data.discogs.com HTML format changed | Check `curl https://data.discogs.com/` and fix `discover_latest_dump()` in `src/import.rs` |
| API returns 500 on search | Postgres connection lost or tables missing | Check `container@discogs-db.service` is running, restart `discogs-api` |
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
| Deployment | Podman-compose (PG + Solr + RabbitMQ) | nspawn PG + Rust API |
| DB size | ~30 GB | ~80-120 GB |
| Replication | Daily | Monthly full re-import |
| API | Included (Perl webapp) | Custom Rust (this project) |
| Search | Solr | Postgres FTS (GIN) |
| Data freshness | ~24h lag | ~30 day lag |
| License | CC BY-NC-SA | CC0 |
| Import time | ~6h initial | ~18 min |
