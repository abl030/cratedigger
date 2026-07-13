---
name: deploy-via-master-worktree
description: "How to land flake bumps + downstream-wrapper edits for a cratedigger deploy without disturbing the operator's in-progress nixosconfig checkout (Forgejo-era)"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 92afd350-c68a-4438-a444-759e7ed54714
---

doc2 deploys from the **`master`** branch of nixosconfig fetched from **Forgejo** (`git.ablz.au`). From doc1 the normal trigger is `fleet-deploy doc2`; do not SSH to the sibling to invoke its internal `fleet-update` directly. See [[forgejo-cutover-deploy-flow]] for the push/auth mechanics. The operator's `~/nixosconfig` working tree on doc1 is sometimes checked out on a **dirty feature branch** — if so, do NOT `git checkout master` or commit in place.

**Why:** on 2026-05-28 the checkout was on `feat/handsfree-agent-voice-input` with uncommitted voice-input edits. The user confirmed "yeah just deploy us on main."

**How to apply:** check `git status --short --branch` first — if the tree is clean and on master (as on 2026-06-11), commit in place. Otherwise use a throwaway detached worktree off origin/master:
```
git fetch origin master && git worktree add --detach /tmp/cd-deploy origin/master
# edit + nix flake update + commit (SSH-signed) inside the worktree
TOKEN=$(cat /run/secrets/forgejo/nixbot-token) && git -c "http.extraHeader=Authorization: token ${TOKEN}" push origin HEAD:master
git worktree remove /tmp/cd-deploy && git update-ref refs/heads/master refs/remotes/origin/master
```
This leaves the operator's feature-branch checkout and dirty files untouched. Flake bumps still MUST originate on doc1 (only doc1 has the Forgejo token + signing key), per [[deploy]] in CLAUDE.md. Related: [[ci-only-runs-gitguardian]] (verify locally before deploying).
