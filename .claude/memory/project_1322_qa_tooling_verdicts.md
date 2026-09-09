---
name: project-1322-qa-tooling-verdicts
description: "#1322 QA-tooling register evaluated 2026-09-09: only the order-shuffle burst found a defect; shipped as nightly shuffled_suite stage (PR #1391, merged 61df2b4b); Ruff DTZ+B+BLE001 adopted (PR #1393); CrossHair/target()/import-linter rejected with numbers; #1389 lib→web layering, #1390 tsc gate, #1392 gate-timing flakes filed; doc1 needs a rebuild before the stage runs"
metadata:
  type: project
---

Issue #1322 (exploratory QA-tooling register) was evaluated candidate by candidate on
2026-09-09, every one run rather than reasoned about; verdicts are on the issue.

- Ruff: DTZ 0 findings, full B 3, BLE001 0 new (230 pre-existing `# noqa: BLE001` for a
  never-enabled rule; 546 dead noqa directives total), SIM/C4/RET/PERF ~196 style nits,
  TRY 3151 (3084 TRY003). Tree already clean of bug-shaped rules. DTZ+B+BLE001 = free
  legislation, shipped as PR #1393 (merged 01d5f58e); rest rejected.
- import-linter rejected (not in nixpkgs; both stated contracts wrong: import_one.py
  imports lib by design, lib→web already at 12 lazy sites in 6 modules because web/mb.py
  and web/discogs.py are misfiled mirror clients). Finding filed as #1389.
- CrossHair: works via `hypothesis-crosshair` backend in a venv with `--system-site-packages`
  over `python313Packages.z3-solver` (pip's z3 wheel can't load libstdc++ on NixOS and
  shadows the nixpkgs one; uninstall it; don't clobber PYTHONPATH). Found one synthetic
  interior needle Hypothesis missed, missed another, 20-400x slower, and on the #812 tie
  mutant plain Hypothesis kills it at 150 derandomized examples (boundary bias). Rejected.
- RuleBasedStateMachine already adopted (request + processing lifecycles). `target()`
  rejected: suite tier derandomized; nightly 20k fuzz already ~7 draws per interior value.
- Order shuffle: 2 seeds x ~11.8k tests found ONE real coupling (`tests/test_util.py`,
  `lib.util._ffmpeg_version` one-slot lru_cache warmed by MagicMock through bare `sp.run`
  mocks; `lib.util.sp` IS subprocess so `check_output` reaches the same mock). Shipped as
  nightly `shuffled_suite` stage (PR #1391, 9 commits, merged 61df2b4b): `--shuffle-seed` /
  `CRATEDIGGER_SHUFFLE_SEED` on scripts/run_python_tests.py, Hypothesis stays derandomized,
  seed printed with a per-MODULE replay (hotspot `module::batch` names are not selectors),
  hotspot exact-ID guard compares sorted (multiset). Shared helper
  `tests/helpers.py::cold_ffmpeg_version_probe/_cache`.
- tsc over web/js: 48 errors in 30/31 `// @ts-check` modules, 0 runtime bugs; filed #1390.

**Why:** the operator's frame was "does it buy anything useful, not test-number-goes-up";
that frame rejected four of five tools and kept the one that found a defect. Review
(opus reader + sonnet runner, 7 rounds) found: the replay handle was unrunnable for
sharded targets; the fuzz runner shares the child but keeps an ordered guard; the
seed leaks into fixture-spawned scripts (gate scripts + `inherited_environment` scrub
it); `tests/test_daily_flake_update.py` timing tests flake under load ~30 (3-second
"reached gate" window, resource receipt), seen 3x during concurrent suites.

**How to apply:** de-dupe new QA-tooling ideas against the #1322 verdict comment first.
Any suite-wide env knob that children honour must be scrubbed by scripts the suite's
own tests spawn. A nightly shuffled red that is green fixed-order is a test-isolation
defect, never a production finding. The nightly gate script is consumed from doc1's
pinned `cratedigger-src` store copy, and doc1 has no auto-switch: a gate-script change
goes live only after doc1 is rebuilt against a nixosconfig pin containing it (the
23:00 rolling bot re-pins; the rebuild is manual). Suite-side changes apply at once
because the gate clones `main` fresh.
