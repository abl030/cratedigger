---
name: project_discogs_api_deploy_and_db_access
description: "How to deploy the discogs-api mirror (push-deploy to the discogs LXC) and reach its nspawn DB — both differ from the repo's own (stale) docs"
metadata: 
  node_type: memory
  type: project
  originSessionId: ac3cee54-cdb8-4006-aa26-f3b571dc04d9
---

The discogs-api mirror (`~/discogs-api`, GitHub `abl030/discogs-api`, flake input `discogs-src` in nixosconfig) runs on the **discogs LXC at 192.168.1.44** (NOT doc2), serving port 8086. Its own CLAUDE.md deploy section is STALE (still describes doc2 fleet-update).

**Deploy flow (verified working 2026-08-25).** The LXC has no `nixos-upgrade.service` and password-only sudo, so `fleet-deploy discogs` / `ssh discogs sudo fleet-update` do NOT work. It uses **push-deploy** (`homelab.update.pushDeploy`, forgejo#10, `modules/nixos/autoupdate/push-deploy.nix`): doc1 builds the closure and a root forced-command key activates it. On doc1:

1. Merge to discogs-api `main` on GitHub (merge commit via `gh pr merge --merge`).
2. In `~/nixosconfig`: `nix flake update discogs-src --override-input discogs-src github:abl030/discogs-api/<full-sha>`, then `SSH_AUTH_SOCK='' git commit` (signed) and token-header push to Forgejo master (`GIT_CONFIG_COUNT/KEY_0='http.https://git.ablz.au.extraHeader'/VALUE_0` env, token from `/run/secrets/forgejo/nixbot-token`, never argv). The pre-push hook builds the new discogs-api package as part of its fleet audit.
3. `nix build ~/nixosconfig#nixosConfigurations.discogs.config.system.build.toplevel --out-link <tmp>` (nixcache.ablz.au is nix-serve over doc1's live store, so a local build is immediately substitutable by the guest).
4. Trigger: `env -u SSH_AUTH_SOCK ssh -i /run/secrets/deploy-trigger/key -o IdentitiesOnly=yes -o BatchMode=yes root@192.168.1.44 "<store-path>"` — the forced command stages the path and fires `push-activate.service`.
5. Poll `ssh discogs` for `systemctl is-active push-activate.service` + `readlink -f /nix/var/nix/profiles/system` == the store path, Result=success. `discogs-api.service` restarts on the switch. Whole cycle ~1 min after the build.

**Reaching the nspawn DB to verify SQL before deploy.** Docs say `psql -h 192.168.100.13` but that times out. The container is at **10.20.0.13**; password in `/run/secrets/discogs-pgpass` (`POSTGRES_PASSWORD=...` env format). From doc2:
```
ssh doc2 'export PGPASSWORD=$(sudo cat /run/secrets/discogs-pgpass | grep -oP "POSTGRES_PASSWORD=\K.*"); psql -h 10.20.0.13 -U discogs -d discogs -c "..."'
```
Repo convention is "DB layer verified against the live instance"; `src/db.rs` now also has in-file `#[cfg(test)]` unit tests (run `nix-shell -p cargo rustc pkg-config openssl postgresql --run "cargo test"` — postgresql needed since the SQL wire-contract tests boot an ephemeral initdb).

**VA search:** `/api/search` takes `artist_id=N` (added #199) — an EXISTS on `release_artist`. VA artist is id 194, name row absent from the dump. `web/discogs.py::search_releases` pins `artist_id=194`. See [[project_full_library_backfill]] context for the curation frame.

**Serve-time heading inference (2026-08-25, discogs-api#14):** the beets-compat `/releases/{id}` surface types a row `"heading"` iff position AND duration are both empty and the release positions at least one other row (dumps carry no type field); raw `/api/*` unchanged. Mirrors cratedigger's downstream rule (issue #1261 series; harness filter from PR #1273 kept as defense-in-depth).
