---
name: project-mutmut-evaluation
description: mutmut ADOPTED 2026-09-01 as implementer-side breadth pass — PR #1318 merged; runbook docs/mutation-testing.md; decision + follow-up gaps in issue #1317
metadata:
  type: project
---

2026-09-01: mutmut 3.7.0 ADOPTED as the implementer-side catalog mutation breadth pass. PR #1318 merged to main (merge commit); decision record + evaluation evidence in issue #1317; runbook `docs/mutation-testing.md`; committed tooling `nix/mutmut-shell.nix` (includes the private-tmpfs shellHook — without it path-authority tests fail 16/83 under /tmp). Rules amended in `.claude/rules/code-quality.md` + PR template: author reports machine tally + survivor dismissals in Fault injection; reader audits dismissals; runner keeps aimed mutants (incl. survivor-killing tests — catalog tally discharges none of the aimed quota).

Operational facts that will bite:
- Per-PR `[tool.mutmut]` pyproject.toml is materialized in the worktree and DELETED after — never committed; the final gate refuses untracked files immediately. `mutants/` is excluded from ruff + both pyright configs (defense vs #1208) but deliberately NOT .gitignored.
- `source_paths` + `only_mutate` scoping; also_copy needs `migrations` (conftest applies them inside mutants/). In 3.7.0 also_copy copies BEFORE mutant generation — a duplicate source entry is redundant, NOT a neutralization trap (measured).
- Real-PG selections need `--max-children 1` (forked children share the one ephemeral PG); FakePipelineDB selections parallelize fully.
- Nothing CI-builds the mutmut shell; a nixpkgs bump breaking uv-build/libcst surfaces at next use. nixpkgs mutmut 3.2.0 is unusable (hard-guesses lib/, no test-selection config).

Evaluation numbers: 460 mutants/20.5s vs web/wrong_match_queue_view.py (399 killed, 60 survived → real gaps the manual #1304 round missed); catalog cannot express `to_json_dict() → {}` (that round's one survivor). Review lesson relived: my first draft minted two false mechanisms (also_copy ordering from reading 3.2.0 while shipping 3.7.0; "displacing ruff.toml") — both caught by the independent read, and the correction round's re-read found no new false claim.

Open follow-ups (in #1317): pin the surface-outcomes 2xx upper boundary (HTTP 300 → exit 0 survives); assert the JS-consumed Wrong Matches `latest_import` sub-fields + `verified_lossless` falsy path.

2026-09-01 later: #1317 CLOSED. PR #1320 (merged) shipped the gap closures — first full exercise of the workflow: implementer mutmut convergence (surface 22/22 killed; view 60 survivors → 14, +46, none lost), reader audited all 14 dismissals (rewrote mutant-38's mechanism: the SQL filter admits '' — producer behavior is what makes it safe), sonnet runner 20/20 aimed kills. Lessons: mutmut count buckets must reconcile (killed+survived+no-tests=total; Protocol-declaration mutants land in "no tests"); cross-module test-class imports double-collect under BOTH unittest's loader (measured 78 extras) and pytest — import the module, not the class; the fake enforces migration 001's plain UNIQUE on mb_release_id (no status carve-out), so multi-request test worlds need distinct MBIDs; seed_visible_wrong_match's default folder name is shared across calls.

2026-09-01 latest: covering issue #1321 opened — whole-repo mutation convergence register (family-by-family batches, priority: lib/quality first with fable review + held merge; test-infra scripts excluded). De-dupe batch work there; update its register row on merge.

2026-09-01 tooling registers: #1321 gained a pre-batch section (trial mutmut type_check_command + mutate_only_covered_lines on the first batch, promote to the runbook template on a measured win). #1322 opened — EXPLORATORY tooling-evaluation register (Ruff family expansion with DTZ top pick, import-linter layer contracts, CrossHair replay on lib/quality, Hypothesis stateful/target/order-shuffle), each candidate gets the replay-experiment treatment with a recorded verdict; skip-list (bandit/semgrep/mypy/coverage-gates/beartype/schemathesis/atheris/radon) recorded there with reasons — don't re-litigate.
