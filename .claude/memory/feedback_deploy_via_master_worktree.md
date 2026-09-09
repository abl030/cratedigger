---
name: deploy-via-master-worktree
description: "The operator's ~/nixosconfig checkout may be on a dirty feature branch; never checkout or commit in it. scripts/deploy.sh pins from a detached temp worktree off origin/master for exactly this reason"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 92afd350-c68a-4438-a444-759e7ed54714
---

doc2 deploys from the **`master`** branch of nixosconfig, fetched from **Forgejo** (`git.ablz.au`) by doc2's verified `nixos-upgrade.service`. The operator's `~/nixosconfig` working tree on doc1 is sometimes checked out on a **dirty feature branch** — do NOT `git checkout master` or commit in place.

**Why:** on 2026-05-28 the checkout was on `feat/handsfree-agent-voice-input` with uncommitted voice-input edits. The user confirmed "yeah just deploy us on main."

**How to apply:** `scripts/deploy.sh` ([[project-deploy-trim-2026-09]]) already does the right thing: it fetches `origin/master`, adds a detached temporary worktree at that commit, updates only `flake.lock`, commits signed there, pushes through the nixosconfig token helper, and removes the worktree, leaving the operator's checkout untouched. For a nixosconfig-only change (a wrapper option), edit and commit in a throwaway detached worktree the same way if the main checkout is dirty, push to Forgejo master with the token helper, then run `scripts/deploy.sh` (it finds cratedigger already pinned and just triggers and waits). Flake bumps still MUST originate on doc1 (only doc1 has the Forgejo token + signing key). Related: [[forgejo-cutover-deploy-flow]].
