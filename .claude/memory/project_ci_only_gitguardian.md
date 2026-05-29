---
name: ci-only-runs-gitguardian
description: "cratedigger GitHub CI runs ONLY GitGuardian — the test suite and dead-code gate are NOT enforced in CI, so a green PR check is not a green suite"
metadata: 
  node_type: memory
  type: project
  originSessionId: 92afd350-c68a-4438-a444-759e7ed54714
---

cratedigger's GitHub PR checks are **GitGuardian only**. There is no CI job that runs `scripts/run_tests.sh`, pyright, or the vulture dead-code gate. A "CLEAN / mergeable" PR with passing checks says nothing about test health.

**Why this matters:** on 2026-05-28, PR #386 (youtube rescue ingest) merged with the dead-code gate RED (un-whitelisted findings) AND 10 failing tests. The failing tests were further masked locally because `run_tests.sh` has `set -e` and runs the dead-code sweep *before* the Python suite — a red sweep aborts the run, leaving a stale "OK" in `/tmp/cratedigger-test-output.txt`.

**How to apply:** never trust a green PR check to mean the suite passes. Always run `nix-shell --run "bash scripts/run_tests.sh"` + `nix-shell --run "pyright"` locally before merging/deploying, and assume freshly-merged main may carry latent test/dead-code breakage. Also: the moduleVm check (`nix build .#checks.x86_64-linux.moduleVm`) uses a trust-auth local Postgres, so it does NOT catch DB-auth/EnvironmentFile gaps (e.g. the youtube-ingest unit's missing pgpass injection that only surfaced live on doc2). See [[deploy-via-master-worktree]].
