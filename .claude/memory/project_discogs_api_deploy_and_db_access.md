---
name: project_discogs_api_deploy_and_db_access
description: "Current discogs-api deploy, nspawn DB access, and real-PostgreSQL test boundaries"
metadata: 
  node_type: memory
  type: project
  originSessionId: ac3cee54-cdb8-4006-aa26-f3b571dc04d9
---

The discogs-api mirror (`~/discogs-api`, GitHub `abl030/discogs-api`, flake input `discogs-src` in nixosconfig) powers `discogs.ablz.au` on doc2.

**Deploy flow.** Push discogs-api itself to GitHub. On doc1, update `discogs-src` in an up-to-date isolated nixosconfig worktree, make an SSH-signed commit, and push it to Forgejo with the token-header flow (see [[project_forgejo_cutover_deploy_flow]]). Deploy the sibling with `fleet-deploy doc2`; do not use a direct GitHub `nixos-rebuild` or SSH to doc2 to invoke `fleet-update`. `discogs-api.service` restarts on switch. Only the discogs-api repository push still targets GitHub.

**Reaching the nspawn DB for live verification.** The container is at **10.20.0.13** from doc2's host namespace; the old `192.168.100.13` address times out. The service authenticates with a password from `/run/secrets/discogs-pgpass` in `POSTGRES_PASSWORD=...` env format. To run a verification query from doc2:
```
ssh doc2 'export PGPASSWORD=$(sudo cat /run/secrets/discogs-pgpass | grep -oP "POSTGRES_PASSWORD=\K.*"); psql -h 10.20.0.13 -U discogs -d discogs -c "..."'
```
SQL query changes must first pass `nix-shell -p cargo rustc pkg-config openssl postgresql --run "cargo test"`. The suite starts a real ephemeral Nix-provided PostgreSQL instance and never skips or substitutes the live database. Live queries remain the post-deploy evidence, not the development test harness. See [[project_681_discogs_artist_wire_contract]] for the artist-catalogue contract.

**VA search:** `/api/search` takes `artist_id=N` (added #199) — an EXISTS on `release_artist`. VA artist is id 194, name row absent from the dump. `web/discogs.py::search_releases` pins `artist_id=194`. See [[project_full_library_backfill]] context for the curation frame.
