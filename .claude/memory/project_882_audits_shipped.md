---
name: project-882-audits-shipped
description: 2026-07-26 - #882 shipped: fuzz burst was structurally unrunnable, classify copy keyed on a scenario no producer emits, property-input audit
metadata:
  node_type: memory
  type: project
---

Issue #882 (post-#868 reflection) delivered 2026-07-26 as PRs #883/#884/#885; cratedigger `5a8ae6d7`, nixosconfig `ee25e37b`, cycle `265c2d8e`. Item 2 split to #887 (operator decision owed); reflection in #888.

**The fuzz burst had never once completed.** `run_fuzz_tests.py` rejects any Hypothesis test whose deadline isn't `None`; two modules never imported `tests._hypothesis_profiles`, so discovery aborted for the WHOLE burst. Not slow, not flaky - structurally impossible, so it was silently skipped or hand-scoped. First completed runs: 88 modules / 946 tests / 2,704 targets / ~820s.

**Non-obvious traps this surfaced, worth remembering:**
- A source audit for the profile import is NOT enough: `@given`/`@settings` snapshot `settings.default` at DECORATION time, so an import at the bottom of the file passes an existence check and still leaves the stock 200 ms deadline. Needs a position check AND the runtime assertion (now in `scripts/run_python_tests.py`, scoped to `deadline` only - `world_model_burst.sh` legitimately flips `derandomize` via `CRATEDIGGER_WORLD_RANDOMIZED=1`).
- **A property can be structurally incapable of depth.** `test_force_import_service_generated` drew 2 booleans = 4 worlds against a 20,000 budget; a mutant widening `beets_staging_dir` to authorize `wrong_matches` (a real quarantine-authority widening) survived 215 tests. Widened to 36 worlds; mutant now dies. Generalization filed as #888 item 1.
- **A bare `return` in a `@given` body spends budget as a PASS** - measured 31.3% (suite) / 28.7% (fuzz) vacuous. `assume()` retries instead. Four sites found by manual pass; a scanner is the wrong tool (can't distinguish oracle early-outs).
- #882's own proposed criterion for item 5 was wrong: "property must reference a production symbol" false-positives 44/352 (12.5%) - properties reach production via node subprocesses with paths in STRINGS, subprocess'd scripts, and `self.<attr>` in state machines. Adopted criterion: every drawn input must be used. Allowlist permanently EMPTY (the one deliberate known-bad self-test was changed so its planted decider RECEIVES the world it ignores).

**Live defect found:** `web/classify.py` rendered "No MusicBrainz match found" behind `no_candidates` - a scenario no producer has ever emitted, 0 live rows - while the real literal `mbid_not_found` (50 rows) fell through to a raw machine token. Copy now splits by evidence: 32 rows "Requested release ID not among the match candidates", 18 zero-candidate rows "Beets returned no match candidates for the requested release ID". Says **Beets**, not MusicBrainz, because two of those rows requested DISCOGS ids. Says **requested release ID**, not "this folder", because `import.search_ids` makes beets skip the text search entirely - the folder cannot affect whether candidates exist.

Related: [[feedback-test-fidelity-meta-pattern]], [[ui-screenshot-loop]], [[project-859-sidecar-manifest-poisoning]], [[pin-and-fuzz-pair]].
