---
name: project_discogs_api_deploy_and_db_access
description: "How to deploy the discogs-api mirror and reach its nspawn DB — both differ from the repo's own (stale) docs"
metadata: 
  node_type: memory
  type: project
  originSessionId: ac3cee54-cdb8-4006-aa26-f3b571dc04d9
---

The discogs-api mirror (`~/discogs-api`, GitHub `abl030/discogs-api`, flake input `discogs-src` in nixosconfig) powers `discogs.ablz.au` on doc2.

**Deploy flow — its own CLAUDE.md is STALE.** The repo's CLAUDE.md still says `nixos-rebuild switch --flake github:abl030/nixosconfig#doc2 --refresh`. Do NOT use that. Since the 2026-06-10 Forgejo cutover, the nixosconfig leg goes through Forgejo + fleet-update exactly like cratedigger (see [[project_forgejo_cutover_deploy_flow]]): push discogs-api to GitHub → on doc1 `nix flake update discogs-src` + signed commit + token-header push to Forgejo → `ssh doc2 'sudo fleet-update'`. `discogs-api.service` restarts on switch. Only the discogs-api repo push still targets GitHub.

**Reaching the nspawn DB to verify SQL before deploy.** Docs say `psql -h 192.168.100.13` but that TCP address times out from doc2's host namespace. The container is actually at **10.20.0.13** (`machinectl list` shows it). The service authenticates with a password from `/run/secrets/discogs-pgpass` in `POSTGRES_PASSWORD=...` env format. To run a verification query from doc2:
```
ssh doc2 'export PGPASSWORD=$(sudo cat /run/secrets/discogs-pgpass | grep -oP "POSTGRES_PASSWORD=\K.*"); psql -h 10.20.0.13 -U discogs -d discogs -c "..."'
```
Repo convention is "DB layer verified against the live instance" (no DB integration tests — only XML parser tests in `src/xml.rs`), so this live-query path is how you red/green a `db.rs` query change.

**VA search:** `/api/search` takes `artist_id=N` (added #199) — an EXISTS on `release_artist`. VA artist is id 194, name row absent from the dump. `web/discogs.py::search_releases` pins `artist_id=194`. See [[project_full_library_backfill]] context for the curation frame.
