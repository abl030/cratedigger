# NixOS Module

The upstream module lives in this repo at `nix/module.nix`, exposed via `nixosModules.default` in `flake.nix`. It is generic and homelab-agnostic: every secret is a `*File` path, the DB is a `dsn` string, no sops/nspawn/reverse-proxy assumptions.

`~/nixosconfig/modules/nixos/services/cratedigger.nix` is a thin homelab wrapper (~150 lines) that imports the upstream module and adds:
- sops-nix per-key secret materialization (`cratedigger-secrets-split` oneshot — see below)
- the nspawn PostgreSQL container for the pipeline DB
- the redis instance for the web UI cache
- the `homelab.localProxy.hosts` entry for `music.ablz.au`
- systemd `after`/`wants`/`restartTriggers` splicing in `container@cratedigger-db.service`

## Key options (full set in `nix/module.nix`)

| Option | Default | Purpose |
|---|---|---|
| `enable` | `false` | Master switch |
| `user` / `group` | `"root"` | Service identity. Default root because slskd downloads + beets need broad fs access. |
| `src` | `../.` | Path to cratedigger source tree. Defaults to this flake's repo root. |
| `stateDir` | `/var/lib/cratedigger` | Runtime state (config.ini, lock file). |
| `slskd.apiKeyFile` | (required) | Path to a file containing the raw slskd API key (one line). |
| `slskd.downloadDir` | (required) | Where slskd downloads land. |
| `slskd.hostUrl` | `http://localhost:5030` | slskd HTTP base URL. |
| `pipelineDb.dsn` | (required) | PostgreSQL DSN. |
| `beetsValidation.{enable,distanceThreshold,stagingDir,trackingFile,verifiedLosslessTarget}` | sensible defaults | Beets validation config. |
| `web.{enable,port,beetsDb,redis.host,redis.port}` | port=8085 | Web UI config. The module does NOT enable redis — provide one. |
| `notifiers.meelo.{enable,url,usernameFile,passwordFile}` | disabled | Meelo notifier. |
| `notifiers.plex.{enable,url,tokenFile,librarySectionId,pathMap}` | disabled | Plex notifier. |
| `notifiers.jellyfin.{enable,url,tokenFile}` | disabled | Jellyfin notifier. |
| `healthCheck.{enable,onFailureCommand}` | enabled, no recovery | Pre-cycle slskd healthcheck. `onFailureCommand` runs to recover (e.g. `systemctl restart slskd.service`). |
| `releaseSettings.*` / `searchSettings.*` / `downloadSettings.*` | match config.ini defaults | Pipeline tunables. See "Search loop tunables" below for the trio that caps the slskd search window. |
| `qualityRanks.*` | mirror of `QualityRankConfig.defaults()` | See README § "Tuning the quality rank model". |
| `timer.{enable,onBootSec,onUnitActiveSec}` | every 5 min | Cycle frequency. |
| `importer.enable` | `true` | Long-lived serial importer that drains queued import work. |
| `importer.preview.enable` | `false` | Enable the async preview gate. When disabled, new import jobs are marked importable immediately for backward-compatible draining. |
| `importer.previewWorkers` | `2` | Async preview worker concurrency when `importer.preview.enable = true`. Must be at least 1. |
| `logging.{level,format,datefmt}` | INFO | Python logging config. |

## Search loop tunables

Three options under `services.cratedigger.searchSettings.*` control the slskd search window and the variant escalation ladder shipped in PR #193. Listed together here because they're easy to forget when triaging stuck releases.

| Option | Default | Maps to | Effect |
|--------|---------|---------|--------|
| `searchResponseLimit` | `1000` | slskd `responseLimit` | Caps peer responses per search. The slskd-api default is 100; popular albums returning more than 100 peers had their results truncated. 1000 covers ~99% of observed searches without triggering the cap. |
| `searchFileLimit` | `50000` | slskd `fileLimit` | Caps total files across all peer responses. The slskd-api default is 10000; popular multi-disc/OST/compilation searches (peers each holding 50+ tracks) fill 10000 in ~3 seconds and terminate the search early — sometimes before the right peer responds. 50000 lets the buffer run to the search timeout for these. |
| `searchEscalationThreshold` | `5` | cratedigger only | After this many failed cycles, `lib/search.py:select_variant` switches from the default `<artist> <album>` query to V1 (year-augmented), V4 (rotating 3-token track-name slices), then `exhausted` (which resets `search_attempts=0` so the ladder wraps; see [`docs/pipeline-db-schema.md`](pipeline-db-schema.md#search_log)). |

**The 30s cycle floor is upstream.** `cfg.search_timeout` exists but slskd caps it at 30000ms; values above that are silently ignored. With response/file limits high enough that they rarely cap, every search runs the full 30s. The path to shorter cycles is changing the client (issue #196), not tuning these options.

## What the module does

1. Builds a Python environment with dependencies (`nix/package.nix`: psycopg2, music-tag, beets, msgspec, redis, slskd-api).
2. Wraps `cratedigger.py` / `pipeline_cli.py` / `migrate_db.py` / `scripts/importer.py` / `scripts/import_preview_worker.py` / `web/server.py` in shell scripts with ffmpeg, sox, mp3val, flac in PATH.
3. Renders `/var/lib/cratedigger/config.ini` at boot from option values, sed-substituting credentials read from each `*File` path. App units render through an atomic temp-file-and-rename step because importer, preview, web, and timer-driven services can start concurrently after migrations.
4. Pre-start: health-check slskd → render config.ini → start `cratedigger.py`.

## Systemd units

- `cratedigger-db-migrate.service` — oneshot, `restartIfChanged = true`, `RemainAfterExit = true`. Runs the schema migrator on every `nixos-rebuild switch`. The app units `requires` it, so they cannot start against an un-migrated DB.
- `cratedigger.service` — oneshot pipeline run. `restartIfChanged = false` (5-min timer picks up new code).
- `cratedigger.timer` — fires every 5 minutes (configurable via `timer.onUnitActiveSec`).
- `cratedigger-importer.service` — long-running serial beets import worker. It only claims queued import jobs after async preview marks them `would_import`.
- `cratedigger-import-preview-worker.service` — optional long-running async preview worker. It starts after DB migrations when `importer.preview.enable = true`, defaults to two worker loops, and runs validation/spectral/measurement preview outside the beets mutation lane.
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
├── nixosModules.default              ← upstream NixOS module
├── packages.<system>.slskd-api        ← slskd-api PyPI build (not in nixpkgs)
├── devShells.<system>.default         ← test/dev environment
└── checks.<system>.moduleVm           ← NixOS VM test (boots module against ephemeral postgres)
```

## Validating before deploy

The flake exposes a NixOS VM check that boots the upstream module against an ephemeral postgres + stub slskd:

```bash
nix build .#checks.x86_64-linux.moduleVm    # ~30s after first build
```

This catches: option surface breakage, prestart sed-substitution bugs, systemd dep graph cycles, wrapper script PYTHONPATH errors, missing python deps. It does NOT exercise slskd interaction or real downloads (those need fixture data — see the python suite). Run before any `nix/module.nix` change.

After deploy, verify the queue workers before assuming imports will drain:

```bash
systemctl status cratedigger-db-migrate cratedigger-import-preview-worker cratedigger-importer
journalctl -u cratedigger-import-preview-worker -u cratedigger-importer -n 100 --no-pager
```

Queued jobs should move from `preview_status='waiting'` to `would_import` or a
terminal preview failure. The importer should only claim `would_import` jobs.
If `importer.preview.enable = false`, `cratedigger-import-preview-worker.service`
should not exist and newly queued jobs should already have
`preview_status='would_import'` with `preview_message='Preview gate disabled'`.
