---
name: deploy-via-master-worktree
description: "How to land flake bumps + downstream-wrapper edits for a cratedigger deploy without disturbing the operator's in-progress nixosconfig checkout"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 92afd350-c68a-4438-a444-759e7ed54714
---

doc2 deploys from the **`master`** branch of `~/nixosconfig` (`nixos-rebuild switch --flake github:abl030/nixosconfig#doc2 --refresh` pulls origin/master). But the operator's `~/nixosconfig` working tree on doc1 is usually checked out on a **dirty feature branch** (e.g. unrelated in-progress work) — so do NOT `git checkout master` or commit in place.

**Why:** on 2026-05-28 the checkout was on `feat/handsfree-agent-voice-input` with uncommitted voice-input edits. The user confirmed "yeah just deploy us on main."

**How to apply:** for every flake bump (`nix flake update cratedigger-src`) and downstream-wrapper edit (`modules/nixos/services/cratedigger.nix`), use a throwaway detached worktree off origin/master:
```
git fetch origin master && git worktree add --detach /tmp/cd-deploy origin/master
# edit + nix flake update + commit inside the worktree
git push origin HEAD:master
git worktree remove /tmp/cd-deploy && git update-ref refs/heads/master refs/remotes/origin/master
```
This leaves the operator's feature-branch checkout and dirty files untouched. Flake bumps still MUST originate on doc1 (only doc1 has push creds), per [[deploy]] in CLAUDE.md. Related: [[ci-only-runs-gitguardian]] (verify locally before deploying).
