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
| `releaseSettings.*` / `searchSettings.*` / `downloadSettings.*` | match config.ini defaults | Pipeline tunables. |
| `qualityRanks.*` | mirror of `QualityRankConfig.defaults()` | See README § "Tuning the quality rank model". |
| `timer.{enable,onBootSec,onUnitActiveSec}` | every 5 min | Cycle frequency. |
| `importer.{enable,previewWorkers}` | enabled, `2` preview workers | Long-lived serial importer plus async preview worker concurrency. `previewWorkers` must be at least 1. |
| `logging.{level,format,datefmt}` | INFO | Python logging config. |

## What the module does

1. Builds a Python environment with dependencies (`nix/package.nix`: psycopg2, music-tag, beets, msgspec, redis, slskd-api).
2. Wraps `cratedigger.py` / `pipeline_cli.py` / `migrate_db.py` / `scripts/importer.py` / `scripts/import_preview_worker.py` / `web/server.py` in shell scripts with ffmpeg, sox, mp3val, flac in PATH.
3. Renders `/var/lib/cratedigger/config.ini` at boot from option values, sed-substituting credentials read from each `*File` path.
4. Pre-start: health-check slskd → render config.ini → start `cratedigger.py`.

## Systemd units

- `cratedigger-db-migrate.service` — oneshot, `restartIfChanged = true`, `RemainAfterExit = true`. Runs the schema migrator on every `nixos-rebuild switch`. Both `cratedigger.service` and `cratedigger-web.service` `requires` it, so the app cannot start against an un-migrated DB.
- `cratedigger.service` — oneshot pipeline run. `restartIfChanged = false` (5-min timer picks up new code).
- `cratedigger.timer` — fires every 5 minutes (configurable via `timer.onUnitActiveSec`).
- `cratedigger-importer.service` — long-running serial beets import worker. It only claims queued import jobs after async preview marks them `would_import`.
- `cratedigger-import-preview-worker.service` — long-running async preview worker. It starts after DB migrations, defaults to two worker loops, and runs validation/spectral/measurement preview outside the beets mutation lane.
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
