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
| `user` / `group` | `"root"` | Service identity. Default root because slskd downloads + beets need broad fs access. See "Running non-root + filesystem permissions" below to switch. |
| `src` | `../.` | Path to cratedigger source tree. Defaults to this flake's repo root. |
| `packageSet` | cratedigger's own locked nixpkgs (via the flake export) | Package set for the runtime closure. Override = escape hatch, forfeits the tested-closure guarantee. |
| `beets.package.discogsMirrorUrl` | `null` | When set, build-time-patches the beets discogs plugin to hit this mirror instead of api.discogs.com. |
| `beets.package.lrclibUrl` | `null` | When set, build-time-patches the beets lyrics plugin's LRCLIB base to this URL. |
| `beets.package.discogsTokenFile` | `null` | `*File` secret (issue #117): materialized into `${stateDir}/beets/secrets.yaml` + `include:` in the rendered config. Null = placeholder token (plugins load cleanly; public-Discogs lookups are token-required). |
| `beets.package.discogsOperatorGroup` | `null` | Optional group allowed to read rendered `secrets.yaml` for operator-side Beets actions. Set with `discogsTokenFile` to render `cratedigger:<group>` mode 0440; the module creates the group and joins its non-root service user. Null keeps service-only mode 0400 and removes any stale rendered secret from a prior group-readable profile. |
| `beets.config.{directory,library}` | production values | The single shipped Beets root/SQLite pair. Both values are rendered into module-owned `config.yaml` and `[Beets]` in `config.ini`; there is no second root override. |
| `beets.config.fetchart.{maxwidth,minwidth}` | `500` / `300` | Load-bearing: library size (embedded art × every track) and the collection's artwork-quality floor. |
| `beets.config.musicbrainz.{host,https,ratelimit}` | derived from `musicbrainz.apiBase` | mkDefault-derived (mirror ⇒ host:port/http/ratelimit 100; public ⇒ musicbrainz.org/https/1); override to pin explicitly. |
| `musicbrainz.apiBase` | `https://musicbrainz.org` | ONE MB origin for web/mb.py (via config.ini, read at `cratedigger-web` startup), pipeline-cli lookups, and the rendered beets musicbrainz block (KTD6). Public default is functional but ~1 req/s. |
| `discogs.apiBase` | `null` | Discogs mirror origin. Mirror-REQUIRED: unset ⇒ Discogs browse off with a 503 mirror-required message (public api.discogs.com does not serve this API shape). |
| `stateDir` | `/var/lib/cratedigger` | Runtime state (config.ini, lock file). |
| `processingDir` | `${stateDir}/processing` | Private `0700` Cratedigger-owned root: canonical albums live in `albums/`, bounded preview scratch in `preview/`. Must be absolute and disjoint from slskd's download tree. |
| `slskd.apiKeyFile` | (required) | Path to a file containing the raw slskd API key (one line). |
| `slskd.downloadDir` | (required) | Where slskd downloads land. |
| `slskd.hostUrl` | `http://localhost:5030` | slskd HTTP base URL. |
| `pipelineDb.dsn` | `null` | PostgreSQL DSN. Required unless `createLocally`. |
| `pipelineDb.createLocally` | `false` | Provision local PostgreSQL: role + database named after `cfg.user`, unix-socket peer auth (no password material anywhere), socket DSN default, migrate unit ordered after postgresql.service. doc2 keeps `false` + its nspawn DSN. |
| `redis.{enable,host,port,maxmemory}` | enabled, `127.0.0.1:6379`, `2gb` | App-owned local Redis server for the pipeline peer cache and web metadata cache. Uses `allkeys-lru`. |
| `peerCache.{ttlSeconds,speedTtlSeconds,redisConnectTimeoutMs,redisOperationTimeoutMs}` | 7d, 24h, 200ms, 100ms | Redis TTL and timeout settings rendered into `[Peer Cache]`. |
| `beets.validation.{enable,distanceThreshold,stagingDir,trackingFile,verifiedLosslessTarget}` | sensible defaults | Beets validation config. |
| `web.{enable,port,redis.host,redis.port}` | port=8085 | Web UI config. The web process reads the Beets DB/root pair from `beets.config.{library,directory}` through `[Beets]`; `web.redis.*` follows the shared app Redis defaults unless explicitly overridden. |
| `notifiers.plex.{enable,url,tokenFile,librarySectionId,pathMap}` | disabled | Plex notifier. |
| `notifiers.jellyfin.{enable,url,tokenFile,libraryId,pathMap}` | disabled | Jellyfin notifier. Every import reports only its mapped final album path through `POST /Library/Media/Updated`; `pathMap` supplies Jellyfin's view of that path and enables the upgrade DateCreated pin. `libraryId` is only a deletion-observation fallback (issues #574/#697, `docs/jellyfin-primer.md`). |
| `healthCheck.{enable,onFailureCommand}` | enabled, no recovery | Pre-cycle slskd healthcheck. `onFailureCommand` runs to recover (e.g. `systemctl restart slskd.service`). |
| `releaseSettings.*` / `searchSettings.*` / `downloadSettings.*` | match config.ini defaults | Pipeline tunables. See "Search loop tunables" below for the trio that caps the slskd search window. |
| `qualityRanks.*` | mirror of `QualityRankConfig.defaults()` | See docs/quality-ranks.md § "Tuning reference (Nix options)". |
| `timer.{enable,onBootSec,onUnitInactiveSec}` | 1s after exit | Cycle frequency. |
| `importer.enable` | `true` | Enable both long-lived queue workers: async preview and the serial importer. Disabled queues remain non-runnable. |
| `importer.previewWorkers` | `2` | Async preview worker concurrency when `importer.enable = true`. Must be at least 1. |
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
3. Renders `/var/lib/cratedigger/config.ini` from option values through the dedicated `cratedigger-config-render.service` on boot and whenever the declarative template changes. App units retain the same atomic temp-file-and-rename render as an idempotent fallback. The independent unit ensures a downstream `ExecCondition` cannot leave stale mutable config by skipping every app's `ExecStartPre`; it deliberately does not touch the pipeline singleton lock. Only the main `cratedigger.service` pre-start clears a stale pipeline lock; worker and unfindable starts are render-only.
3b. Renders the beets `config.yaml` into `${stateDir}/beets/` (BEETSDIR) the same way. `import.duplicate_keys.album: [mb_albumid, discogs_albumid]` (the Palo Santo data-loss invariant), the plugin list, and the path templates are fixed literals — NOT options. Only `beets.config.*` (directory, library, fetchart widths, musicbrainz host/https/ratelimit) is operator-tunable. With `beets.package.discogsTokenFile` set, `secrets.yaml` is materialized next to it and included; `discogsOperatorGroup` changes it from service-only 0400 to explicit group-read 0440 for authorized CLI operators.
4. Enables `redis-cratedigger.service` by default with bounded memory and `allkeys-lru`.
5. Pre-start: health-check slskd → render config.ini → start `cratedigger.py`.

## Running non-root + filesystem permissions

The `user`/`group` table row above defaults to root — zero-config, since slskd downloads and the beets library commonly live outside any unprivileged user's reach. Running non-root is fully supported (issue #570) and is the right shape when other services (Jellyfin, Plex) need to read AND write inside the same library tree.

### Private processing boundary

`processingDir` is deliberately separate from `slskd.downloadDir`: slskd is a
source authority, not a safe destination. The module creates the root and its
`albums/` and `preview/` children as `0700` for the Cratedigger identity;
tmpfiles may age-clean only `preview/` children. Put a large processing root
directly beneath a root-owned, non-group-writable parent. Do not run slskd as
the Cratedigger user and do not put processing beneath a parent writable by
slskd. The module rejects relative and lexically overlapping paths, while the
runtime also refuses symlinked/unsafe roots.

### The `permissions` plugin + `fix_library_modes`

The rendered beets config enables the built-in `permissions` plugin with `file = "0664"` and `dir = "02775"` (setgid + group-writable) — these are fixed literals in `nix/module.nix`, not module options. Its `art_set` listener (`fix_art`) fixes fetched-art mode on BOTH initial import and a manual `beet fetchart` re-fetch. This exists because beets' own `fetchart` writes art via `mkstemp` (which forces `0600` regardless of umask) and nothing else chmods it afterward — without the plugin, art lands `0600` and a media server reading it as a different user throws `UnauthorizedAccessException` (issue #570 defect 1).

`lib/permissions.py::fix_library_modes` is the post-import belt-and-suspenders pass: `LIBRARY_DIR_MODE = 0o2775`, applied recursively to the imported album/artist dirs and everything the plugin's per-item listener doesn't reach (empty/intermediate dirs beets creates along the way). `reset_umask()` sets the process umask to `0o002` (group-writable) at every pipeline entry point, since a unit's `UMask=0000` alone doesn't reliably survive the subprocess chain down to beets.

**`dir = 02775` (setgid) is load-bearing, not cosmetic.** Plain `0775` strips the setgid bit, so every child album dir beets creates underneath would stop inheriting the library's group — the group-inheritance layout below silently breaks the moment this gets "simplified" to `0775`.

### Switching to a non-root service user

Set `services.cratedigger.user` / `.group` to something other than `"root"`. No module edits are needed: the module already auto-declares the system user (`users.users.${cfg.user}` behind an internal `mkIf (cfg.user != "root")`), and every state-dir tmpfiles rule is keyed on `cfg.user`/`cfg.group`.

A non-root cratedigger needs supplementary group membership for two things root gets for free:

1. **The slskd download directory's group** — the reaper (`reap_disk_orphans`) deletes/moves in-flight downloads via directory-write permission, not file ownership, so it needs write access to that directory's group (typically slskd's own service group).
2. **The group that owns its runtime secrets** (`/run/cratedigger-secrets/*` — the raw slskd API key, notifier creds) — whichever secrets backend materializes these needs to make them group-readable by cratedigger's group, or add cratedigger's user to the group that owns them.

The pgpass `EnvironmentFile` for `pipelineDb.createLocally` needs no special handling: systemd (PID1, root) reads `EnvironmentFile=` before dropping privileges to `cfg.user`, so a non-root service user never has to read that file itself.

### The group-`users` setgid library layout

Give the library tree a shared consumer group — `users` (gid 100) is the conventional choice, since that's commonly what Jellyfin/Plex containers run as — with setgid directories (`2775`). New album/artist dirs then inherit the group automatically (the setgid bit above), and any gid-100 media server can both READ fetched art and WRITE NFO/artwork alongside the media. This is the #570 "group twin" fix: `root:music-import 0775` dirs (no setgid, root-owned) previously blocked media-server writes outright.

Provision the library roots with a setgid tmpfiles rule:

```nix
systemd.tmpfiles.rules = [
  "d /srv/music/library 2775 cratedigger users -"
];
```

For a tree that already exists, fix it once — this is an operator action, not committed config (`.claude/rules/scope.md`):

```bash
chgrp -R users /srv/music/library
find /srv/music/library -type d -exec chmod 2775 {} +
find /srv/music/library -type f -exec chmod 0664 {} +
```

### Caveat: a root-owned secret under a non-root state dir

If a secret file (e.g. `beets.package.discogsTokenFile`) lives UNDER the state dir and the state dir's owner just changed from root to `cfg.user`, systemd-tmpfiles can no longer manage that file's permissions from a rule — it refuses with "unsafe path transition" (a safety check against operating through a non-root-owned parent directory). Since the non-root service's preStart reads the token with a plain `cat`, a stale root-owned `0400` token then fails EVERY unit at startup (`cat: discogs-token: Permission denied`), not just the Discogs pathway.

Fix with a durable one-time `chown root:<secrets-group>` + `chmod 0440` on the token file (the out-of-band-secret pattern), or manage the token via sops-nix with `owner = cfg.user` so sops-nix — not tmpfiles — sets the correct ownership from the start.

### Health check still runs as root

`healthCheck`'s `ExecStartPre` (`slskdHealthCheck`) is `+`-prefixed, so it always runs as root regardless of `services.cratedigger.user` — this is what lets `onFailureCommand` (e.g. `systemctl restart slskd.service`) keep working under a non-root service user. `preStartScript` stays unprefixed, so config rendering happens as the service user.

### Minimal non-root snippet

```nix
users.users.cratedigger = {
  isSystemUser = true;
  group = "users";
  extraGroups = [ "slskd" "cratedigger-ops" ];  # download-dir group + secrets group
};

services.cratedigger = {
  user = "cratedigger";
  group = "users";
  # ... slskd / beets / web options unchanged
};

systemd.tmpfiles.rules = [
  "d /srv/music/library 2775 cratedigger users -"
];
```

See [`examples/cratedigger.nix`](../examples/cratedigger.nix) for the full worked example.

## Systemd units

- `cratedigger-config-render.service` — oneshot, `restartIfChanged = true`, `RemainAfterExit = true`. Materializes `config.ini` and the module-owned beets configuration independently of application health gates; app units keep an idempotent pre-start render fallback. Runs as `cfg.user`/`cfg.group` and never removes `.cratedigger.lock`, so a config-only deploy cannot disturb an active timer-owned cycle.
- `cratedigger-db-migrate.service` — oneshot, `restartIfChanged = true`, `RemainAfterExit = true`. Runs the schema migrator on every `nixos-rebuild switch`. The long-running workers (`cratedigger-web`, `cratedigger-importer`, `cratedigger-import-preview-worker`, `cratedigger-youtube-ingest`) `requires` it, so they cannot start against an un-migrated DB. `cratedigger.service` and `cratedigger-unfindable.service` deliberately do NOT — both are timer-driven with `restartIfChanged = false`, and this unit's `ExecStart` store path changes on every deploy, so a `requires` edge would propagate its every-switch restart as a SIGTERM to a mid-flight cycle; they use `wants`+`after` instead and gate on schema currency themselves at startup (`lib/migrator.py::assert_schema_current`) so a behind/missing schema still aborts them before any work runs.
- `redis-cratedigger.service` — app-owned Redis server for peer cache and web metadata cache. `cratedigger.service` and `cratedigger-web.service` want/after it, but do not require it; runtime Redis failures degrade to cold-cache behavior.
- `cratedigger.service` — oneshot pipeline run. `restartIfChanged = false` (the timer picks up new code on the next cycle).
- `cratedigger.timer` — starts the next cycle after the previous oneshot exits
  (configurable via `timer.onUnitInactiveSec`).
- `cratedigger-importer.service` — long-running serial beets import worker. It
  claims queued import jobs after async preview marks durable candidate
  evidence as `evidence_ready`; historical/raw `would_import` rows are
  non-runnable display/audit data and are not claimable.
- `cratedigger-import-preview-worker.service` — long-running async preview
  worker enabled with `importer.enable`. It starts after DB migrations,
  defaults to two worker loops, and runs validation/spectral/measurement
  preview outside the beets mutation lane.
- `cratedigger-unfindable.service` — oneshot, `Type=oneshot`, `restartIfChanged = false`, `TimeoutStartSec=2h`, runs as `cfg.user`. Wraps `scripts/run_unfindable_detection.py` via the `cratedigger-unfindable` wrapper bin. `wants = ["cratedigger-db-migrate.service"]` (not `requires` — see the migrate unit's entry above) and shares the same `ExecStartPre` chain as `cratedigger.service` (`slskdHealthCheck` when `healthCheck.enable = true`, then `preStartScript`) — a slskd outage should fail the unit fast rather than write garbage `last_artist_probe_match_count=0` rows for every cohort member. Lives in its own systemd unit, NOT inline in the main `cratedigger.service` loop, because R20 ("the system never stops searching") forbids the regular search cadence from being throttled by detection state. Implements PR3 U13 (`docs/plans/2026-05-25-001-feat-search-plan-iteration-2-plan.md`). The upstream module sets `Environment="PIPELINE_DB_DSN=..."` only; the downstream wrapper must augment `serviceConfig.EnvironmentFile` with the sops `cratedigger-pgpass` path (same pattern the wrapper uses for `cratedigger.service`) — see `docs/search-plan-iter2-deploy.md` § "PR3 — Detection + telemetry" for the exact incantation and the 2026-05-26 first-deploy gotcha.
- `cratedigger-unfindable.timer` — `OnCalendar=daily`, `Persistent=true`, `RandomizedDelaySec=30min`. The 30-min jitter is purely local cron-collision avoidance (logrotate, postgres autovacuum on doc2); the single-operator install has no fleet to spread across. The daily fire processes K=100 rows per run with a ~7-day per-request cadence target; full cohort coverage finishes in ~9 days for a ~830-row wanted cohort.
- `cratedigger-web.service` — long-running web UI for music.ablz.au.

### Untrusted-input service sandbox

Exactly four long-running units receive the shared systemd sandbox:
`cratedigger-web.service`, `cratedigger-importer.service`,
`cratedigger-import-preview-worker.service`, and
`cratedigger-youtube-ingest.service`. The timer-driven
`cratedigger.service`/`cratedigger-unfindable.service` and the migration
oneshot deliberately remain outside this boundary.

Every sandboxed unit has `NoNewPrivileges=yes`, `PrivateTmp=yes`,
`ProtectSystem=strict`, `ProtectHome=yes`, and
`RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6`. `SystemCallFilter` uses
systemd's portable `@system-service` allowlist. It intentionally includes the
ordinary `fchownat` operation used by the renderer's explicit group-read
secrets handoff; do not add a broad negative syscall class without re-running
the module VM, because it can prevent config rendering before the worker starts.

`ReadWritePaths` is derived from the configured authority roots and is exact:

| Unit | Writable paths |
|---|---|
| web | `stateDir`, `processingDir`, `slskd.downloadDir`, Beets root, dedicated parent of the Beets library DB, validation staging root |
| importer | web paths plus parent of `beets.validation.trackingFile` |
| import-preview-worker | `stateDir`, `processingDir`, `slskd.downloadDir` |
| youtube-ingest | `stateDir`, `youtubeIngest.tempDir`, validation staging root |

An optional path (`slskd.downloadDir`, validation staging/tracking) is omitted
when its option is `null`. `ReadWritePaths` only makes the named portion of the
otherwise read-only system namespace writable; it neither grants Unix
permissions nor makes a path visible in a downstream private mount namespace.
A downstream writable `BindPaths` entry is an independent mount grant and can
reopen its target even when the upstream `ReadWritePaths` list is narrower.
For a consumer using `TemporaryFileSystem=/mnt`, the recommended composition is
broad shared-tree visibility through `BindReadOnlyPaths`, followed by
`BindPaths` only for that unit's exact writable roots from the table above.
Those writable binds must be narrow and per-unit; the default Beets DB parent is
`${stateDir}-beets-db` (not the music root or stateDir), and explicit library overrides keep
their parent operator-owned. Do not bind an entire shared music or data parent
writable for every worker. Verify effective denial rather
than inferring confinement from the rendered property strings alone. The
upstream module VM proves the generic module boundary without downstream
writable binds.

All service phases inherit the sandbox, including downstream `ExecCondition`
and `ExecStartPre` commands. On doc2, the metadata gate used by web, importer,
and import-preview-worker writes under `/run/cratedigger-metadata-gate`, so
that exact directory must appear in those units' `ReadWritePaths`. The
upstream module deliberately does not grant generic write access to `/run`.

## Sops + per-key secrets

sops-nix's `key = "..."` does NOT actually extract a single value from a multi-key dotenv file (it writes the whole `KEY=VALUE` envfile regardless — verified empirically; same gotcha is documented in `~/nixosconfig/modules/nixos/services/alerting.nix` for the gotify token). The upstream module wants raw values per file, so the homelab wrapper materializes them via a `cratedigger-secrets-split` oneshot at boot:

```nix
systemd.services.cratedigger-secrets-split = {
  before = ["cratedigger.service" "cratedigger-web.service" "cratedigger-db-migrate.service"];
  serviceConfig.ExecStart = pkgs.writeShellScript "cratedigger-secrets-split" ''
    set -euo pipefail
    install -d -m 0700 /run/cratedigger-secrets
    for key in SOULARR_SLSKD_API_KEY PLEX_TOKEN JELLYFIN_TOKEN; do
      grep -m1 "^$key=" "${config.sops.secrets."soularr/env".path}" \
        | cut -d= -f2- | tr -d '\n' > "/run/cratedigger-secrets/$key"
      chmod 0400 "/run/cratedigger-secrets/$key"
    done
  '';
};
services.cratedigger.slskd.apiKeyFile = "/run/cratedigger-secrets/SOULARR_SLSKD_API_KEY";
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
├── checks.<system>.jellyfinMetadataVm ← Jellyfin 10.11.11 tagged-metadata + DateCreated pin lifecycle VM
├── checks.<system>.packageSetPin      ← eval guard: default packageSet = own lock; override honoured
├── checks.<system>.moduleAssertions   ← eval guard: friendly required-option messages; doc2 + stranger shapes clean
├── checks.<system>.apiBaseDerivation  ← eval guard: beets musicbrainz block derives from musicbrainz.apiBase
├── checks.<system>.beetsMirrorPatches ← beets mirror knobs patch/don't-patch as configured
└── checks.<system>.packageDefault     ← the CLI bundle builds (`nix run` stays green)
```

## Validating before deploy

The flake exposes separate NixOS VM checks for module wiring and the real
Jellyfin integration contract:

```bash
nix build .#checks.x86_64-linux.moduleVm    # ~30s after first build
nix build .#checks.x86_64-linux.jellyfinMetadataVm
```

This catches: option surface breakage, prestart sed-substitution bugs, systemd dep graph cycles, wrapper script PYTHONPATH errors, missing python deps. It does NOT exercise slskd interaction or real downloads (those need fixture data — see the python suite). Run before any `nix/module.nix` change.

`jellyfinMetadataVm` boots the flake-pinned Jellyfin, invokes the production
targeted notifier against tagged FLAC fixtures, and proves metadata population,
scoped targeting, curated-field preservation, and the real PostgreSQL-backed
DateCreated capture/reconcile lifecycle. Run it for Jellyfin notifier, pin, or
flake-pinned Jellyfin changes.

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
historical/raw `would_import` rows retained as non-runnable display/audit data.
If `importer.enable = false`, neither queue worker should exist; the operator
must restore the preview/evidence path before queueing beets-mutating work.

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
