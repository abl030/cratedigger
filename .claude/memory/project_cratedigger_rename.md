---
name: project rename to cratedigger
description: Project was renamed soularr → cratedigger on 2026-04-20. Python code, NixOS module/wrapper, DB, state dirs, systemd units, GH repo, working dir, memory dir — all renamed.
type: project
originSessionId: a018bdd2-f59a-49b3-90f9-f7edc2682a61
---
Rename landed 2026-04-20. GH issue abl030/cratedigger#134 tracks the full scope.

**Why:** The code had long since diverged from mrusse/soularr — no longer a Lidarr bridge, the pipeline DB is the source of truth, "cratedigger" fits what it actually does. Also: the repo's old About blurb still said "A Python script that connects Lidarr with Soulseek!" with a non-ours homepage `soularr.net`.

**How to apply:**
- GH repo is now `abl030/cratedigger` (old URL auto-redirects).
- Python entrypoint is `cratedigger.py`, classes are `CratediggerConfig`/`CratediggerContext`, logger is `"cratedigger"`.
- NixOS module uses `services.cratedigger.*` (upstream, in this repo) and `homelab.services.cratedigger.*` (downstream wrapper in nixosconfig).
- Live on doc2: systemd units `cratedigger.service`, `cratedigger-web.service`, `cratedigger-db-migrate.service`, `cratedigger.timer`, `container@cratedigger-db.service`, `redis-cratedigger.service`.
- PostgreSQL DB: `cratedigger` owned by role `cratedigger` (renamed from soularr).
- State dir: `/var/lib/cratedigger/`. Data dir: `/mnt/virtio/cratedigger/postgres/`.

**Deliberately kept on "soularr"** (encrypted state + upstream attribution):
- `mrusse/soularr` attribution in README (origin story).
- sops file `secrets/soularr.env` and sops key `soularr/env` inside nixosconfig.
- `SOULARR_SLSKD_API_KEY` env var name inside the sops bundle.
(no live paths remain on "soularr" after 2026-04-20)

**Rollback path** (if anything breaks in future): `/tmp/cratedigger-backup-20260420-1219.sql` on doc2 holds the pre-rename DB dump (49MB). Reverse mv'ing `/var/lib/cratedigger` and `/mnt/virtio/cratedigger` + ALTER DATABASE + ALTER USER + `nixos-rebuild switch --rollback` would restore.
