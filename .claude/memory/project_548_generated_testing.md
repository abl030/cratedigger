---
name: project-548-generated-testing
description: "Issue #548 generated/property testing: PRs #553 + #554 merged (Hypothesis, parity fuzz, event-stream fuzz, fuzz-on-push hook); remaining targets on issue"
metadata: 
  node_type: memory
  type: project
  originSessionId: 0b22cb5a-2427-470e-bf56-fe9d619ef487
---

Issue #548 (hardware-style generated testing), status as of 2026-07-08:

- **PR #553 merged**: `hypothesis` in dev shell; `tests/_hypothesis_profiles.py` (`suite` deterministic / `fuzz` 20k burst); `tests/test_quality_generated.py` (wild-space invariants + **twin parity property** over shared builders `tests/helpers.py::build_parity_candidate_evidence/current` + evidence integrity/fail-closed properties + known-bad self-tests); `tests/test_evidence_generated.py` (V0-evidence lifecycle, RED world pinned as `@example`); `docs/generated-testing.md`.
- **PR #554 merged**: `tests/test_slskd_events_generated.py` (stamping oracle / totality+exactly-once / duplicate-id invariance for `ingest_download_file_events`); classification-coherence property (found via coverage steering — the ONE unreached decision-policy layer was cleanup eligibility); **`push` profile (2k examples) wired into `scripts/pre-push`** so fuzzing runs on every push (~42s) before `nix flake check`; `coverage` in dev shell + steering recipe in the doc; event builders shared via `tests/helpers.py`.
- Coverage finding worth remembering: generators saturate the decision core fast (95% of `decisions.py` at 20k examples); marginal value comes from new strategies/invariants, not more examples. Non-decision misses (config parsing, wire helpers) belong to other tests.
- Promotion policy: shrunk failures become named `@example` pins or album-test-set scenarios — never JSON artifacts (seeds aren't stable across generator versions).
- **2026-07-08: THE METHOD FOUND A REAL PRODUCTION BUG** — #550 defect #1 (multi-disc partial-manifest shrink, unreproducible by static analysis/forensics): generated harness driving REAL try_multi_enqueue + REAL check_for_match reproduced the exact signature (16 entries/11 unique, CD01+CD03), RCA'd it (per-disc match loop never excludes an assigned folder; (username,filename) keying collapses duplicates), fix + coverage property merged as PR #557. Operator is sold on the method.
- **PR #555 merged (2026-07-08): fault-injection qualification.** 13 hand-picked mutants vs generated tests only: 10 killed as-is (incl. reverting the real V0 fix `6cf26a4` — the generated lifecycle property rediscovers the production bug), 1 killed at push-tier entropy, 2 true gaps fixed (below-gate never-stop-searching invariant for fresh requests; `_SPECTRAL_OVERRIDE_DECISIVE_WORLD` parity `@example` pin). All 13 now die. When the operator doubts the harness again, mutation testing is the demonstrator — the driver is a one-shot script (not committed), shape recorded in the issue comment.

**PR #556 merged (2026-07-08): invariant-first TDD codified + lifecycle machine.** `.claude/rules/code-quality.md` § Red/Green TDD now requires: invariants written first on new features; generated properties in the same PR for generated-testable surfaces; known-bad self-test per checker; fault-injection to qualify harnesses. `tests/test_request_lifecycle_generated.py` = `RuleBasedStateMachine` over real `finalize_request` + `supersede_request_mbid` vs FakePipelineDB (replaced-frozen, identity, guards, linked descendants); qualified at birth (3 transitions.py mutants killed at suite tier).

Remaining targets (on the issue): metamorphic decision properties (dominance/monotonicity/self-comparison), search-plan determinism properties, extending the lifecycle machine into the dispatch/outcome-action layer. Rejected: web/CLI differential (thin adapters over one service — tests a function against itself).

**Gotchas:** (1) pyright in the shared checkout excludes `.claude/worktrees`, so IDE diagnostics inside a worktree show bogus `reportMissingImports` — trust `nix-shell --run pyright` from the worktree root. (2) A git-hook script's `BASH_SOURCE` is the `.git/hooks` symlink path, NOT the resolved target — derive repo root via `git rev-parse --show-toplevel`.
