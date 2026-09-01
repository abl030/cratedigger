---
name: project-mutmut-evaluation
description: 2026-09-01 mutmut 3.7.0 evaluated as mutant-runner breadth pass — viable, fast, complementary to aimed mutants; recipe on branch worktree-mutmut-explore
metadata:
  type: project
---

2026-09-01: evaluated mutmut 3.7.0 for the review-gate mutant-runner role. Recipe pushed on branch `worktree-mutmut-explore` (mutmut-shell.nix + pyproject.toml).

Key facts:
- nixpkgs only packages mutmut 3.2.0, which hard-guesses `lib/` as mutate root and cannot scope — useless here. 3.7.0 built from GitHub inside the flake-locked env (needs uv-build bound relaxed from `<0.10.0`; hash sha256-jqJWFEYXVA6WizDO34iiyUmElGUBqsqPPyKS8AUJ7ZY=).
- Scoping pattern: `source_paths = ["<pkg>"]` + `only_mutate = ["<pkg>/<file>.py"]` + `pytest_add_cli_args_test_selection = [test modules]` + `also_copy` for the other packages + `migrations`. NEVER put a source_paths member in also_copy — the verbatim copy silently overwrites the mutated file.
- `tests/conftest.py` already makes the whole tree pytest-runnable (ephemeral PG + migrations at import). mutmut forks parallel children sharing that ONE PG — real-PG-backed targets need `--max-children 1`; FakePipelineDB targets are safe at full parallelism.
- Measured on doc1: 460 mutants vs `web/wrong_match_queue_view.py` in 20.5s wall, 399 killed / 60 survived; 22 mutants vs `lib/surface_outcomes.py` in 6.7s, 2 survivors.
- Found REAL gaps the manual 40-mutant #1304 round missed: `actual_filetype`/`latest_import` sub-fields JS-consumed (wrong-matches.js:1142) but never asserted; `verified_lossless` falsy→True survives; `continue→break` survives; HTTP 300 counted as CLI success (surface_outcomes 2xx boundary unpinned).
- mutmut's catalog CANNOT express the manual round's one real survivor (`to_json_dict() → {}` — verified: all 508 occurrences in the mutants file verbatim). Aimed mutants and catalog breadth are complementary, not substitutive.
- Noise is real but tractable: ~60 survivors collapse to ~a dozen distinct questions; string-literal operators inflate one unasserted field into 5 rows; `float("inf")→float("INF")` is equivalent.

Recommended role (operator decision pending): runner's FIRST pass, diff-scoped, config materialized per-PR in the runner's own worktree (no committed machinery, consistent with scope.md); aimed mutants (past-fix reverts, adapter/argument swaps, JS) and survivor triage stay manual. Do not run concurrently with a suite (outside the admission lock).
