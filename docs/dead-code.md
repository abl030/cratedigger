# Finding dead code

Two complementary static checks protect production liveness:

- Ruff `F401`/`F811` runs source-locally, so a name used in another module
  cannot hide an unused import. Explicit redundant aliases mark the exact
  pre-existing legacy export baseline in `cratedigger.py`,
  `lib/pipeline_db/_shared.py`, and `scripts/pipeline_cli/__init__.py`.
- Vulture keeps the aggregate repository view for dead functions, classes,
  attributes, and cross-module APIs.

Both consume `tools/production_python_sources.txt`; tests are deliberately
absent from that one authored root list.

**Static (fast, noisy):** vulture flags unreferenced functions / classes / variables.

```bash
nix-shell --run "bash scripts/find_dead_code.sh"             # diff vs whitelist — only new findings
nix-shell --run "bash scripts/find_dead_code.sh --baseline"  # all candidates (initial hunt)
```

The whitelist at `tools/vulture/whitelist.py` masks the known aggregate
false positives on main (msgspec Struct fields, beets ImportSession overrides,
route handler dispatch, SQL DictRow attribute access). After deleting
genuinely-dead code, regenerate the aggregate baseline:

```bash
nix-shell --run 'mapfile -t sources < <(sed "/^[[:space:]]*#/d; /^[[:space:]]*$/d" tools/production_python_sources.txt); vulture --make-whitelist "${sources[@]}" > tools/vulture/whitelist.py'
```

Both scans are intentionally **production-only**. Tests are evidence that a
surface behaves as expected, not evidence that production still calls it; if
tests were included, a test-only reference could silently preserve a dead API
forever. A production field consumed only through serialization, framework
reflection, or an external client therefore needs a narrow entry in
`tools/vulture/whitelist.py` with its reason on the same line. Intentional
unused imports use an explicit redundant alias at that import, never a
whole-file ignore; this keeps every module ratcheted against new F401 debt.
The suite pins this boundary in `tests/test_issue_573_boundaries.py` and
`tests/test_unused_import_audit.py`; do not add `tests/` to the source roots.

**Why no runtime coverage?** We tried it (issue #352): production-instrumented coverage.py on the long-running services, diffed against test coverage to surface "tested but never run in prod." It was removed 2026-07-01. It never produced an actionable signal — collection silently broke three times on Nix store-path renames, and once fixed the diff was dominated by noise it can't see past: `pipeline-cli` / the beets-interpreter harness / the deploy-time migrator aren't instrumented at all (so they always look dead), and a branch not hit in a bounded window is a "rare operator path" (Replace, YouTube-rescue, ban-source), not dead. For a single-operator system the CPU overhead bought nothing that vulture plus judgement didn't already give. Don't re-add it without a fundamentally different design.

## Cascading orphans — regen the whitelist after every deletion

Deleting a vulture-flagged helper frequently exposes a **deeper orphan** that was kept alive solely by the thing you just deleted. The pattern has now bitten this audit four times:

- **PR #358** — deleting `finalize_request_if_plan_current` orphaned `PipelineDB.is_request_plan_current` (its only reader)
- **PR #360** — deleting `move_album` orphaned `BeetsOpResult.new_path` (only set by `move_album`)
- **PR #363 → #364** — deleting `_select_variant_for_album` (in #356) exposed `build_query` and `select_variant`; deleting those exposed `strip_short_tokens`
- **PR #355** — deleting all three public exports of the former import-service
  module left `_apply_request_spectral_fields` as a cascading orphan that
  justified deleting the whole module

The mechanic is simple but easy to miss: vulture's whitelist contains an entry per known orphan, keyed by name. When a helper goes away, its callees lose their last reference but aren't in the whitelist yet (they were "used" before). The next `vulture --make-whitelist` run will surface them, and `bash scripts/find_dead_code.sh` will go red until you either delete them or accept them as new whitelist entries.

**Workflow per deletion PR:**
1. Make the deletion + delete the tests that exercised it.
2. Regenerate the whitelist with the roots from
   `tools/production_python_sources.txt` (the command above), then prepend the
   header from the previous version.
3. Diff the whitelist (`git diff tools/vulture/whitelist.py`). New entries are cascading orphans you exposed.
4. For each new entry, decide:
   - **Fold into this PR** if the orphan is small, contained, and the test cleanup is bounded (~50 LOC). Best when the orphan is structurally tied to the deletion (`strip_short_tokens` was the canonical example — its body matched what only the deleted callers needed).
   - **Park in the whitelist** if it'd double the PR. Document the cascade in the PR description and the umbrella issue so the next pass picks it up.

**Anti-pattern:** deleting a helper, leaving the cascading orphans in the whitelist, and forgetting about them. The whitelist grows silently and the audit goes stale.
