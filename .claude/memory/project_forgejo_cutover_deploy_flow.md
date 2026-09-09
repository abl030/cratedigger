---
name: forgejo-cutover-deploy-flow
description: "Since 2026-06-10, nixosconfig deploys come from Forgejo via fleet-update; GitHub is frozen. Pushes go through ~/nixosconfig/scripts/forgejo-auth.sh (token as a header); commits must be SSH-signed. Since 2026-09-09 scripts/deploy.sh drives the whole leg."
metadata: 
  node_type: memory
  type: project
  originSessionId: d59e70a5-df4b-4e6e-9a5e-24c881e81595
---

As of the 2026-06-10 signed-fleet-deploys cutover (#235 in nixosconfig), the nixosconfig write+fetch root is **Forgejo** (`git.ablz.au/abl030/nixosconfig`). GitHub's copy is **frozen** at the cutover commit — never deploy `github:abl030/nixosconfig`. The cratedigger repo itself still lives on GitHub (only the nixosconfig leg changed).

Deploy flow for cratedigger changes ([[project-deploy-trim-2026-09]]): merge to main. Merged main ships on the nightly roll (doc1 23:00 pin, doc2 04:00 switch). To ship now, run `scripts/deploy.sh` on doc1 from the shared checkout: exact `--override-input` pin in a detached temp worktree, SSH-signed commit (`commit.gpgsign=true`, `gpg.format=ssh`, key file `~/.ssh/id_ed25519_git_sign`; fleet-update verifies signatures against hosts.nix allowed signers), push and read-back through `~/nixosconfig/scripts/forgejo-auth.sh` (never `git -c http.extraHeader=...` by hand any more, and never echo the token), `fleet-deploy doc2`, wait, anchor check.

Break-glass (e.g. when the deployed fleet-update itself is broken): on doc2 `sudo fleet-update --dry-run` (fetches + verifies + checks out the clone) then `sudo nixos-rebuild switch --flake /var/lib/fleet-update/repo#doc2 --no-write-lock-file --option accept-flake-config true`.

**Bug fixed 2026-06-11** (nixosconfig `05783b8f`): `verify.nix` passed `system.autoUpgrade.flags` through to fleet-update's REBUILD_FLAGS; nixpkgs appends `--refresh --flake <github>` to that option, and the trailing `--flake` overrode the verified-clone flake ref — every enforced deploy silently rebuilt the frozen GitHub rev while reporting success. This is why `scripts/deploy.sh` keeps the one `last-verified-rev` anchor check. Related: [[deploy-via-master-worktree]].
