---
name: project-445-complete-dual-load-killed
description: "2026-06-12: issue #445 fully closed (PR #451, item 3 dual-module-load); refactor reflection — gdb live probe technique + AST audit beats regex ratchets"
metadata: 
  node_type: memory
  type: project
  originSessionId: 50e135e6-3474-4d07-bad9-4aea60c1c7fb
---

Issue #445 item 3 shipped and deployed 2026-06-12 (PR #451, merged + fleet-updated): the dual-module-load ambiguity is structurally dead. `web/server.py` strips its own script-dir `sys.path` entry (realpath) before any import; `tests/web/_harness.py` no longer inserts `web/`/`lib/`; `TestSysPathAudit` is an **AST resolver** (folds os.path chains + simple variables — variable-target inserts and `append` are no longer evasions, zero exemptions); `tests/test_no_dual_load.py` has a script-mode boot pin with a sentinel (never over-strips) and walks subpackages. That closes ALL items of #445 — the issue is done.

**Refactor reflection (what to reuse next time):**
- **Live probe technique that worked:** `gdb -p <pid> -batch` + `call (int)PyGILState_Ensure()` / `call (int)PyRun_SimpleString("...dump to /tmp...")` / `PyGILState_Release($1)` against the production web process (gdb from `nix build nixpkgs#gdb --no-link --print-out-paths` on doc2). Clean attach/detach, no service disruption. Probe BEFORE and AFTER deploy — the after-probe caught a `"/"` (cwd) sys.path entry from the module wrappers' trailing `:''${PYTHONPATH:-}` colon, fixed with `''${PYTHONPATH:+:$PYTHONPATH}`.
- **Adversarial agent vs Codex were disjoint again:** adversarial found the audit-evasion class (variable-target insert, `append`), realpath/symlink gap, subpackage blind spot, 4 stale docs; Codex found nothing (clean pass, "no discrete regression"). Keep running both, adversarial first.
- **The reviewer undercounts; the structural fix counts for you.** Adversarial flagged 2 surviving dead sys.path mutations; rewriting the audit as an AST resolver mechanically flushed out 13 across 10 files. When a review finds N instances of a pattern, build the detector and let it find the rest — same lesson as the #430 ratchet.
- **Script-mode `sys.path[0]` is the hidden dual-load source:** `python path/to/script.py` (and `coverage run`) puts the script's DIRECTORY on sys.path. Any repo script whose dir contains importable modules has the latent hazard. web/server.py now self-strips; [[project-430-fakedb-migration-done]] playbook still applies for ratchets.
