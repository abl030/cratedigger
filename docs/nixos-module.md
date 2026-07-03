# NixOS Module

The upstream module lives in this repo at `nix/module.nix`, exposed via `nixosModules.default` in `flake.nix`. It is generic and homelab-agnostic: every secret is a `*File` path, the DB is a `dsn` string, no sops/nspawn/reverse-proxy assumptions.

The flake export is a wrapper that pins the module's package set to **cratedigger's own flake.lock**: the runtime python env (and beets) is built from the nixpkgs rev cratedigger's test suite ran against, not the consumer's nixpkgs. This costs a second nixpkgs evaluation on the consumer host. The escape hatch is `services.cratedigger.packageSet = pkgs;` (or any package set) — setting it forfeits the tested-closure guarantee: your beets/python may then drift from what the suite and the real-beets contract test verified, which is exactly the dev/prod skew that shipped the 2026-06-29 beets 2.12 import breakage.

`~/nixosconfig/modules/nixos/services/cratedigger.nix` is a thin homelab wrapper (~150 lines) that imports the upstream module and adds:
- sops-nix per-key secret materialization (`cratedigger-secrets-split` oneshot — see below)
- the nspawn PostgreSQL container for the pipeline DB
- the `homelab.localProxy.hosts` entry for `music.ablz.au`
- systemd `after`/`wants`/`restartTriggers` splicing in `container@cratedigger-db.service`

## Key options (full set in `nix/module.nix`)

| Option | Default | Purpose |
|---|---|---|
| `enable` | `false` | Master switch |
| `user` / `group` | `"root"` | Service identity. Default root because slskd downloads + beets need broad fs access. |
| `src` | `../.` | Path to cratedigger source tree. Defaults to this flake's repo root. |
| `packageSet` | cratedigger's own locked nixpkgs (via the flake export) | Package set for the runtime closure. Override = escape hatch, forfeits the tested-closure guarantee. |
| `beets.discogsMirrorUrl` | `null` | When set, build-time-patches the beets discogs plugin to hit this mirror instead of api.discogs.com. |
| `beets.lrclibUrl` | `null` | When set, build-time-patches the beets lyrics plugin's LRCLIB base to this URL. |
| `beets.discogsTokenFile` | `null` | `*File` secret (issue #117): materialized into `${stateDir}/beets/secrets.yaml` (0400) + `include:` in the rendered config. Null = placeholder token (plugins load cleanly; public-Discogs lookups are token-required). |
| `beetsConfig.{directory,library}` | production values | Rendered into the module-owned `config.yaml`. `beetsDirectory` (config.ini) follows `beetsConfig.directory` by default. |
| `beetsConfig.fetchart.{maxwidth,minwidth}` | `500` / `300` | Load-bearing: library size (embedded art × every track) and the Meelo black-box floor. |
| `beetsConfig.musicbrainz.{host,https,ratelimit}` | derived from `musicbrainz.apiBase` | mkDefault-derived (mirror ⇒ host:port/http/ratelimit 100; public ⇒ musicbrainz.org/https/1); override to pin explicitly. |
| `musicbrainz.apiBase` | `https://musicbrainz.org` | ONE MB origin for web/mb.py (`--mb-api`), pipeline-cli lookups, and the rendered beets musicbrainz block (KTD6). Public default is functional but ~1 req/s. |
| `discogs.apiBase` | `null` | Discogs mirror origin. Mirror-REQUIRED: unset ⇒ Discogs browse off with a 503 mirror-required message (public api.discogs.com does not serve this API shape). |
| `stateDir` | `/var/lib/cratedigger` | Runtime state (config.ini, lock file). |
| `slskd.apiKeyFile` | (required) | Path to a file containing the raw slskd API key (one line). |
| `slskd.downloadDir` | (required) | Where slskd downloads land. |
| `slskd.hostUrl` | `http://localhost:5030` | slskd HTTP base URL. |
| `pipelineDb.dsn` | `null` | PostgreSQL DSN. Required unless `createLocally`. |
| `pipelineDb.createLocally` | `false` | Provision local PostgreSQL: role + database named after `cfg.user`, unix-socket peer auth (no password material anywhere), socket DSN default, migrate unit ordered after postgresql.service. doc2 keeps `false` + its nspawn DSN. |
| `redis.{enable,host,port,maxmemory}` | enabled, `127.0.0.1:6379`, `2gb` | App-owned local Redis server for the pipeline peer cache and web metadata cache. Uses `allkeys-lru`. |
| `peerCache.{ttlSeconds,speedTtlSeconds,redisConnectTimeoutMs,redisOperationTimeoutMs}` | 7d, 24h, 200ms, 100ms | Redis TTL and timeout settings rendered into `[Peer Cache]`. |
| `beetsValidation.{enable,distanceThreshold,stagingDir,trackingFile,verifiedLosslessTarget}` | sensible defaults | Beets validation config. |
| `web.{enable,port,beetsDb,redis.host,redis.port}` | port=8085 | Web UI config. `web.redis.*` follows the shared app Redis defaults unless explicitly overridden. |
| `notifiers.meelo.{enable,url,usernameFile,passwordFile}` | disabled | Meelo notifier. |
| `notifiers.plex.{enable,url,tokenFile,librarySectionId,pathMap}` | disabled | Plex notifier. |
| `notifiers.jellyfin.{enable,url,tokenFile}` | disabled | Jellyfin notifier. |
| `healthCheck.{enable,onFailureCommand}` | enabled, no recovery | Pre-cycle slskd healthcheck. `onFailureCommand` runs to recover (e.g. `systemctl restart slskd.service`). |
| `releaseSettings.*` / `searchSettings.*` / `downloadSettings.*` | match config.ini defaults | Pipeline tunables. See "Search loop tunables" below for the trio that caps the slskd search window. |
| `qualityRanks.*` | mirror of `QualityRankConfig.defaults()` | See README § "Tuning the quality rank model". |
| `timer.{enable,onBootSec,onUnitInactiveSec}` | 1s after exit | Cycle frequency. |
| `importer.enable` | `true` | Long-lived serial importer that drains queued import work. |
| `importer.preview.enable` | `false` | Enable the async preview gate. When disabled, new import jobs are marked importable immediately for backward-compatible draining. |
| `importer.previewWorkers` | `2` | Async preview worker concurrency when `importer.preview.enable = true`. Must be at least 1. |
| `logging.{level,format,datefmt}` | INFO | Python logging config. |

## Search loop tunables

Three options under `services.cratedigger.searchSettings.*` control the slskd search window and the persisted-search-plans escalation ladder. Listed together here because they're easy to forget when triaging stuck releases.

| Option | Default | Maps to | Effect |
|--------|---------|---------|--------|
| `searchResponseLimit` | `1000` | slskd `responseLimit` | Caps peer responses per search. The slskd-api default is 100; popular albums returning more than 100 peers had their results truncated. 1000 covers ~99% of observed searches without triggering the cap. |
| `searchFileLimit` | `50000` | slskd `fileLimit` | Caps total files across all peer responses. The slskd-api default is 10000; popular multi-disc/OST/compilation searches (peers each holding 50+ tracks) fill 10000 in ~3 seconds and terminate the search early — sometimes before the right peer responds. 50000 lets the buffer run to the search timeout for these. |
| `searchEscalationThreshold` | `5` | cratedigger only | Number of repeated default slots the persisted-search-plans generator (`lib/search.py`, `SEARCH_PLAN_GENERATOR_ID`) emits at the head of each plan before stepping into `unwild`, optional `unwild_year`, and up to three track slots. The legacy `select_variant`/`search_attempts` ladder is gone — see [`docs/pipeline-db-schema.md`](pipeline-db-schema.md#persisted-search-plans-migration-014) for the new schema and [`docs/parallel-search.md`](parallel-search.md#plan-driven-execution-post-2026-05-cutover) for execution flow. |

**The 30s cycle floor is upstream.** `cfg.search_timeout` exists but slskd caps it at 30000ms; values above that are silently ignored. With response/file limits high enough that they rarely cap, every search runs the full 30s. The path to shorter cycles is changing the client (issue #196), not tuning these options.

## What the module does

1. Builds a Python environment with dependencies (`nix/package.nix`: psycopg2, music-tag, beets, msgspec, redis, zstandard) from the pinned `packageSet`. The beets in that env is the cratedigger-owned derivation (`nix/beets.nix`) — one store path serving the python library (`lib/beets_distance.py`), the `cratedigger-beet` wrapper (which pins `BEETSDIR` at `${stateDir}/beets`), and — from U5 — the harness.
2. Wraps `cratedigger.py` / `pipeline_cli.py` / `migrate_db.py` / `scripts/importer.py` / `scripts/import_preview_worker.py` / `web/server.py` in shell scripts with ffmpeg, sox, mp3val, flac in PATH.
3. Renders `/var/lib/cratedigger/config.ini` at boot from option values, sed-substituting credentials read from each `*File` path. App units render through an atomic temp-file-and-rename step because importer, preview, web, and timer-driven services can start concurrently after migrations.
3b. Renders the beets `config.yaml` into `${stateDir}/beets/` (BEETSDIR) the same way. `import.duplicate_keys.album: [mb_albumid, discogs_albumid]` (the Palo Santo data-loss invariant), the plugin list, and the path templates are fixed literals — NOT options. Only `beetsConfig.*` (directory, library, fetchart widths, musicbrainz host/https/ratelimit) is operator-tunable. With `beets.discogsTokenFile` set, `secrets.yaml` (0400) is materialized next to it and included.
4. Enables `redis-cratedigger.service` by default with bounded memory and `allkeys-lru`.
5. Pre-start: health-check slskd → render config.ini → start `cratedigger.py`.

## Systemd units

- `cratedigger-db-migrate.service` — oneshot, `restartIfChanged = true`, `RemainAfterExit = true`. Runs the schema migrator on every `nixos-rebuild switch`. The app units `requires` it, so they cannot start against an un-migrated DB.
- `redis-cratedigger.service` — app-owned Redis server for peer cache and web metadata cache. `cratedigger.service` and `cratedigger-web.service` want/after it, but do not require it; runtime Redis failures degrade to cold-cache behavior.
- `cratedigger.service` — oneshot pipeline run. `restartIfChanged = false` (5-min timer picks up new code).
- `cratedigger.timer` — starts the next cycle after the previous oneshot exits
  (configurable via `timer.onUnitInactiveSec`).
- `cratedigger-importer.service` — long-running serial beets import worker. It
  claims queued import jobs after async preview marks durable candidate
  evidence as `evidence_ready` (legacy `would_import` rows remain claimable).
- `cratedigger-import-preview-worker.service` — optional long-running async preview worker. It starts after DB migrations when `importer.preview.enable = true`, defaults to two worker loops, and runs validation/spectral/measurement preview outside the beets mutation lane.
- `cratedigger-unfindable.service` — oneshot, `Type=oneshot`, `restartIfChanged = false`, `TimeoutStartSec=2h`, runs as `cfg.user`. Wraps `scripts/run_unfindable_detection.py` via the `cratedigger-unfindable` wrapper bin. `requires = ["cratedigger-db-migrate.service"]` and shares the same `ExecStartPre` chain as `cratedigger.service` (`slskdHealthCheck` when `healthCheck.enable = true`, then `preStartScript`) — a slskd outage should fail the unit fast rather than write garbage `last_artist_probe_match_count=0` rows for every cohort member. Lives in its own systemd unit, NOT inline in the 5-min `cratedigger.service` loop, because R20 ("the system never stops searching") forbids the regular search cadence from being throttled by detection state. Implements PR3 U13 (`docs/plans/2026-05-25-001-feat-search-plan-iteration-2-plan.md`). The upstream module sets `Environment="PIPELINE_DB_DSN=..."` only; the downstream wrapper must augment `serviceConfig.EnvironmentFile` with the sops `cratedigger-pgpass` path (same pattern the wrapper uses for `cratedigger.service`) — see `docs/search-plan-iter2-deploy.md` § "PR3 — Detection + telemetry" for the exact incantation and the 2026-05-26 first-deploy gotcha.
- `cratedigger-unfindable.timer` — `OnCalendar=daily`, `Persistent=true`, `RandomizedDelaySec=30min`. The 30-min jitter is purely local cron-collision avoidance (logrotate, postgres autovacuum on doc2); the single-operator install has no fleet to spread across. The daily fire processes K=100 rows per run with a ~7-day per-request cadence target; full cohort coverage finishes in ~9 days for a ~830-row wanted cohort.
- `cratedigger-web.service` — long-running web UI for music.ablz.au.

## Sops + per-key secrets

sops-nix's `key = "..."` does NOT actually extract a single value from a multi-key dotenv file (it writes the whole `KEY=VALUE` envfile regardless — verified empirically; same gotcha is documented in `~/nixosconfig/modules/nixos/services/alerting.nix` for the gotify token). The upstream module wants raw values per file, so the homelab wrapper materializes them via a `cratedigger-secrets-split` oneshot at boot:

```nix
systemd.services.cratedigger-secrets-split = {
  before = ["cratedigger.service" "cratedigger-web.service" "cratedigger-db-migrate.service"];
  serviceConfig.ExecStart = pkgs.writeShellScript "cratedigger-secrets-split" ''
    set -euo pipefail
    install -d -m 0700 /run/cratedigger-secrets
    for key in SOULARR_SLSKD_API_KEY MEELO_USERNAME MEELO_PASSWORD PLEX_TOKEN JELLYFIN_TOKEN; do
      grep -m1 "^$key=" "${config.sops.secrets."soularr/env".path}" \
        | cut -d= -f2- | tr -d '\n' > "/run/cratedigger-secrets/$key"
      chmod 0400 "/run/cratedigger-secrets/$key"
    done
  '';
};
services.cratedigger.slskd.apiKeyFile = "/run/cratedigger-secrets/SOULARR_SLSKD_API_KEY";
services.cratedigger.notifiers.meelo.usernameFile = "/run/cratedigger-secrets/MEELO_USERNAME";
# ... etc
```

If you don't use sops or have one key per encrypted file, skip the splitter and point `apiKeyFile` directly at the secret path.

## Flake outputs

```
github:abl030/cratedigger
├── packages.<system>.default          ← operator CLI bundle (pipeline-cli, pipeline-migrate) — `nix run .#pipeline-cli`
├── apps.<system>.pipeline-cli         ← `nix run github:abl030/cratedigger#pipeline-cli -- --help`
├── nixosModules.default              ← upstream NixOS module (pins packageSet to this flake's lock)
├── devShells.<system>.default         ← test/dev environment (same pinned nixpkgs)
├── checks.<system>.moduleVm           ← NixOS VM test (boots module against ephemeral postgres)
├── checks.<system>.packageSetPin      ← eval guard: default packageSet = own lock; override honoured
├── checks.<system>.moduleAssertions   ← eval guard: friendly required-option messages; doc2 + stranger shapes clean
├── checks.<system>.apiBaseDerivation  ← eval guard: beets musicbrainz block derives from musicbrainz.apiBase
├── checks.<system>.beetsMirrorPatches ← beets mirror knobs patch/don't-patch as configured
└── checks.<system>.packageDefault     ← the CLI bundle builds (`nix run` stays green)
```

## Validating before deploy

The flake exposes a NixOS VM check that boots the upstream module against an ephemeral postgres + stub slskd:

```bash
nix build .#checks.x86_64-linux.moduleVm    # ~30s after first build
```

This catches: option surface breakage, prestart sed-substitution bugs, systemd dep graph cycles, wrapper script PYTHONPATH errors, missing python deps. It does NOT exercise slskd interaction or real downloads (those need fixture data — see the python suite). Run before any `nix/module.nix` change.

For Redis peer-cache changes, verify with a paused timer and one manual cycle:

```bash
sudo systemctl stop cratedigger.timer
sudo nixos-rebuild switch --flake .#HOST
systemctl is-active redis-cratedigger.service
redis-cli -p 6379 CONFIG GET maxmemory-policy
systemctl show -p After -p Wants cratedigger.service cratedigger-web.service
grep -A8 '^\[Peer Cache\]' /var/lib/cratedigger/config.ini
sudo systemctl start cratedigger.service
journalctl -u cratedigger.service -n 80 --no-pager | grep 'Cratedigger cycle complete'
redis-cli -p 6379 --scan --pattern 'peer_*' | wc -l
sudo systemctl start cratedigger.timer
```

Expected output: Redis is `active`, `maxmemory-policy` is `allkeys-lru`,
both app units list `redis-cratedigger.service` in `After` and `Wants`, and
`config.ini` contains `[Peer Cache]` with the rendered Redis host, port, TTL,
and timeout values. The first cycle is allowed to be cold; later cycle
summaries should show `cache_pos_hits`, `cache_neg_hits`, and `cache_misses`
moving while `cache_errors=0 cache_fuse_tripped=0 cache_write_errors=0`.

Stop and roll back if cache error counters are nonzero after Redis is active,
if Redis key growth is far above the number of browsed peer directories, or if
matching/download behavior regresses. The old
`/var/lib/cratedigger/cratedigger_cache.json` is no longer read or updated by
new code; keep it on disk only as a rollback aid after code rollback.

Rollback:

```bash
sudo systemctl stop cratedigger.timer cratedigger.service
sudo nixos-rebuild switch --flake .#HOST --rollback
# Optional when bad Redis writes are suspected:
redis-cli -p 6379 --scan --pattern 'peer_dir:*' | xargs -r redis-cli -p 6379 DEL
redis-cli -p 6379 --scan --pattern 'peer_dir_neg:*' | xargs -r redis-cli -p 6379 DEL
redis-cli -p 6379 --scan --pattern 'peer_speed:*' | xargs -r redis-cli -p 6379 DEL
redis-cli -p 6379 --scan --pattern 'peer_dir_count:*' | xargs -r redis-cli -p 6379 DEL
sudo systemctl start cratedigger.service
stat /var/lib/cratedigger/cratedigger_cache.json
sudo systemctl start cratedigger.timer
```

After deploy, verify the queue workers before assuming imports will drain:

```bash
systemctl status cratedigger-db-migrate cratedigger-import-preview-worker cratedigger-importer
journalctl -u cratedigger-import-preview-worker -u cratedigger-importer -n 100 --no-pager
```

Queued jobs should move from `preview_status='waiting'` to `evidence_ready` or
a terminal preview failure. The importer claims `evidence_ready` jobs, with
legacy `would_import` rows drained as preview-disabled compatibility.
If `importer.preview.enable = false`, `cratedigger-import-preview-worker.service`
should not exist and newly queued jobs should already have
`preview_status='would_import'` with `preview_message='Preview gate disabled'`.

Rollback note: before starting pre-018 importer or preview-worker code, stop the
queue services and reset active `evidence_ready` rows to `waiting` so old code
re-previews them instead of treating a neutral readiness token as import
authority. Include `running` rows because a stopped new importer can leave a
claimed job with `preview_status='evidence_ready'`, which old startup recovery
would otherwise requeue without changing the preview token:

```sql
UPDATE import_jobs
SET status = 'queued',
    worker_id = NULL,
    started_at = NULL,
    heartbeat_at = NULL,
    preview_status = 'waiting',
    importable_at = NULL,
    updated_at = NOW()
WHERE status IN ('queued', 'running')
  AND preview_status = 'evidence_ready';
```
