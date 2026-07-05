---
name: nixosconfig-wrapper-tracks-module-options
description: "cratedigger nix/module.nix option-surface refactors must update the nixosconfig wrapper in lockstep; moduleVm can't catch wrapper drift, so it's a latent deploy break that only surfaces on the next fleet-update"
metadata: 
  node_type: memory
  type: project
  originSessionId: 694f77c4-3beb-46a5-bb35-bc94ebb68d7e
---

cratedigger's `nix/module.nix` DECLARES `services.cratedigger.*` options; the downstream nixosconfig wrapper (`~/nixosconfig/modules/nixos/services/cratedigger.nix` + `hosts/doc2/configuration.nix`) SETS them. When a cratedigger PR renames/moves an option, the wrapper is NOT auto-updated, and the `moduleVm` check tests the module against a SYNTHETIC config — so it passes. The break is invisible until the next `sudo fleet-update` tries to deploy any change past the renaming commit, failing eval with `The option 'services.cratedigger.X' does not exist`.

Canonical instance (2026-07-05): cratedigger commit 604da00 ("consolidate beets option surface") moved `beets.discogsMirrorUrl`/`beets.lrclibUrl`/`beets.discogsTokenFile` → `beets.package.*`, `beetsDirectory` → `beets.directory`, `beetsValidation.*` → `beets.validation.*`. It merged AFTER the last deploy (v2026.07.04) so it sat undeployed; the #501/#507/#508/#509/#510 deploy was the first to include it and blocked on it. Fixed in nixosconfig 98837f06 (a pure value-preserving path rename of the wrapper + the doc2 host override).

**Why:** module and wrapper live in different repos with nothing gating the contract between them; a module option refactor is a latent deploy break until the wrapper catches up.

**How to apply:**
1. When reviewing/merging a cratedigger PR that touches `nix/module.nix` option *declarations*, check whether the nixosconfig wrapper sets the changed option and flag a lockstep wrapper update in the same window.
2. When `fleet-update` fails with "option does not exist": it's wrapper drift. Find the renaming commit with `git log <last-deployed-rev>..origin/main -- nix/module.nix` then `git show <commit> -- nix/module.nix` for the old→new mapping. Remap the wrapper paths (values preserved), and ALWAYS verify locally on doc1 with `nix build .#nixosConfigurations.doc2.config.system.build.toplevel --no-link` (evaluation + assertions) before signing/pushing to Forgejo.

See [[project_forgejo_cutover_deploy_flow]], [[feedback_deploy_via_master_worktree]].
