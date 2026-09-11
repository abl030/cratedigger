---
name: project-1322-followup-batch
description: "2026-09-09 meta-orchestrated batch of the #1322 follow-ups: #1389 mirror clients moved into lib (PR #1403), #1390 tsc gate for web/js (PR #1399 + sweep #1408), #1392 daily-gate timing flakes (PR #1406); all merged, no hand deploy (nightly rolling update ships main); a reviewer wrote into a live implementer worktree again"
metadata:
  type: project
---

The three issues filed from the #1322 evaluation were meta-orchestrated on 2026-09-09
(two opus agents at a time, own worktrees, hands-off briefs, agents merge themselves):

- #1389 (PR #1403, merge 5e4dfb1b): `web/mb.py`, `web/discogs.py`, `web/api_bases.py`,
  `web/artist_search.py`, `web/parallel_fanout.py`, `web/cache.py` moved to `lib/`
  (`lib/mb_api.py`, `lib/discogs_api.py`, `lib/api_bases.py`, `lib/artist_search.py`,
  `lib/parallel_fanout.py`, `lib/redis_cache.py`); `evidence_column_accusation_flags` to
  `lib/accusation_flags.py`. lib→web imports: 12 → 0. Ruff `banned-api` on `web` with
  per-file ignores for `web/**`, `tests/**` and three named scripts (the agent narrowed the
  issue's `scripts/**`/`harness/**` grant because TID251 ignores also relax the `tests`
  ban). New `tests/test_harness_interpreter_boundary.py` pins the three Beets-interpreter
  harness files import nothing from lib or web.
- #1390 (PR #1399, merge 2dd46110): 48 tsc errors → 0; `web/js/jsconfig.json`,
  `scripts/run_tsc.sh`, `scripts/phase_parsers/tsc.py`, `pkgs.typescript_5` in the shells;
  tsc phase runs after js-unit in 1-3 s. Sweep PR #1408 (merge ea7df8ec): `bigLabel` flag
  deleted; `web/static_assets.py` owns the static-file rule for both servers (the dev
  server had been serving Python module source with 200 for `/server.py`-shaped paths);
  the `tests/_harness_fixtures` nix-eval race was already fixed by #1394.
- #1392 (PR #1406, merge d20a6445): daily-gate module's wall-clock caps became one
  liveness-gated 120 s bound; receipt tests print the monitor's failure reason; neither
  flake reproduced in 300 runs up to load 170; monitor tolerance rejected on measurement.

**Why:** operator asked to "orchestrate fixes o your filed issues". Every agent's
two-agent review found real defects in the agent's own first cut (unbounded pipe drain
in #1406; unrunnable-adapter parity gaps and a `JS_CONTENT_TYPE` survivor in #1408; two
uncovered default lookups in the Replace path in #1403), which is the point of the split.
Load hit 156 when two suites and a world-model burst overlapped; a bounded, niced load
harness is the rule for reproductions.

**How to apply:** no hand deploy for non-user-facing merges since the deploy trim
(#1387): merged main ships on the nightly rolling update. Agent-tool worktrees stay
locked by the harness after the agent finishes; leave them. Seen again: a reviewer
subagent wrote a stray line into the implementer's live worktree during the review
window despite snapshot instructions; implementers must `git diff` before committing
after a review round. Shared `$CLAUDE_JOB_DIR/tmp` filenames collided again
(`pr_body.md`); give each agent its own subdirectory in the brief.
