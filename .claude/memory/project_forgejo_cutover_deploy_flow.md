---
name: forgejo-cutover-deploy-flow
description: "Nixosconfig writes come from Forgejo; doc1 triggers verified sibling deploys with fleet-deploy. Push with the nixbot token header; commits must be SSH-signed."
metadata: 
  node_type: memory
  type: project
  originSessionId: d59e70a5-df4b-4e6e-9a5e-24c881e81595
---

As of the 2026-06-10 signed-fleet-deploys cutover (#235 in nixosconfig), the nixosconfig write+fetch root is **Forgejo** (`git.ablz.au/abl030/nixosconfig`). GitHub's copy is **frozen** at the cutover commit — never deploy `github:abl030/nixosconfig`. The cratedigger repo itself still lives on GitHub (only the nixosconfig leg changed).

Deploy flow for cratedigger changes:
1. Push cratedigger to GitHub (unchanged).
2. On doc1 in `~/nixosconfig`: `nix flake update cratedigger-src`, commit — must be **SSH-signed** (`commit.gpgsign=true` already set; fleet-update verifies signatures against hosts.nix allowed signers).
3. Push to Forgejo with the token header (gh credential helper is github.com-only; never echo the token):
   `TOKEN=$(cat /run/secrets/forgejo/nixbot-token) && git -c "http.extraHeader=Authorization: token ${TOKEN}" push origin master`
4. From doc1, run `fleet-deploy doc2`. The locked sibling trigger starts doc2's `nixos-upgrade` service; its internal `fleet-update` verifies and builds from the root-owned clone at `/var/lib/fleet-update/repo`.

Break-glass (e.g. when the deployed fleet-update itself is broken): `sudo fleet-update --dry-run` (fetches + verifies + checks out the clone) then `sudo nixos-rebuild switch --flake /var/lib/fleet-update/repo#doc2 --no-write-lock-file --option accept-flake-config true`.

**Bug fixed 2026-06-11** (nixosconfig `05783b8f`): `verify.nix` passed `system.autoUpgrade.flags` through to fleet-update's REBUILD_FLAGS; nixpkgs appends `--refresh --flake <github>` to that option, and the trailing `--flake` overrode the verified-clone flake ref — every enforced deploy silently rebuilt the frozen GitHub rev while reporting success. doc2 was fixed by break-glass; **other enforced fleet hosts (igpu etc.) may still run the buggy wrapper until they get one manual verified-clone switch** — their nightly fleet-update keeps them pinned to (or reverts them to) the cutover closure. Related: [[deploy-via-master-worktree]].
