# Issue #290 — resume note

This is the **entry point** for picking up the stateful-MagicMock removal effort with fresh context. Read this first; the long-form context lives in issues #290 (master plan) and #301 (deferred items + refactor ideas).

## Current state

Baseline last measured at **4 findings across 3 files** (the documented-as-deferred remainder). To check the live count:

```bash
nix-shell --run "python3 tests/_rebuild_mock_audit_baseline.py"
# Then:
nix-shell --run "python3 -c '
import json
b = json.load(open(\"tests/mock_audit_baseline.json\"))
print(f\"baseline: {sum(sum(v.values()) for v in b.values())}, files: {len(b)}\")
for f, kinds in sorted(b.items(), key=lambda kv: sum(kv[1].values())):
    print(f\"  {sum(kinds.values()):>3}  {f}\")
'"
```

If the live count differs from this doc, this doc is stale — trust the live count and update this file as part of your next PR.

## Mental model

A finding is one of two things:
1. **`stateful_mock_assign:NAME`** — a variable assignment like `db = MagicMock()` for a name implying a stateful collaborator. Replacement: `FakePipelineDB` / `FakePipelineDBSource` / `FakeSlskdAPI` / a `Fake*` subclass.
2. **`patch:lib.x.y`** — a `patch()` call against a function the audit considers "ours, not a seam." Either migrate to drive real code with a fake, or — if the function is genuinely a thin boundary wrapper — add to `tests/_mock_audit_scanner.py::_LEAF_SEAM_PATTERNS` with a rationale comment.

The audit is in `tests/test_mock_audit.py`; the scanner heuristic lives in `tests/_mock_audit_scanner.py`; the frozen call-site count is in `tests/mock_audit_baseline.json`.

## Next moves (in order of value-per-effort)

### ~~1. Item N in #301 — DI refactor for `try_enqueue` match function~~ (LANDED)

Shipped: `try_enqueue` / `try_multi_enqueue` / `_iter_wave_matches` now take
`match_fn: MatchFn = check_for_match` (keyword-only). Migrated all wave-shape
tests in `test_enqueue_fanout.py`, plus three sites in
`test_integration_slices.py` and three in `test_integration.py`. Dropped 34
findings (160 → 126).

### ~~2. Item K in #301 — DI refactor for `_check_quality_gate_core`~~ (LANDED)

Shipped: `dispatch_import_core` takes `quality_gate_fn: QualityGateFn =
_check_quality_gate_core` (keyword-only). Threaded as an optional
`quality_gate_fn` kwarg through `dispatch_import_from_db` and
`_handle_valid_result` so tests that hit those entry points can inject the
stub too. Test helpers `noop_quality_gate` + `RecordingQualityGate` added
to `tests/helpers.py`. Migrated `test_dispatch_core.py` (4),
`test_dispatch_from_db.py` (5), `test_import_dispatch.py` (3). Dropped 11
findings (126 → 115).

### ~~3. Item M in #301 — `_execute` support on `FakePipelineDB`~~ (LANDED)

Shipped: `FakePipelineDB.queue_execute_results(*cursors)` registers a
deterministic cursor sequence; `_execute(sql, params)` records calls in
`db.execute_calls` and pops the next entry (raising it if it's an
`Exception` instance). Migrated 5 sites in `test_pipeline_cli.py`
(`TestCmdQuery` 4, `TestCmdRepairSpectral` 1). Other test_pipeline_cli
sites use different MagicMock patterns that aren't `_execute` queues —
those remain for separate migration. Dropped 5 findings (115 → 110).

### ~~4. Web routes `finalize_request` DI (the long pole)~~ (LANDED)

Shipped: `web/routes/pipeline.py` now binds `finalize_request =
transitions.finalize_request` at module scope. Routes call the local
name (not `transitions.finalize_request` directly), so tests can swap
the dependency with `patch("web.routes.pipeline.finalize_request")` —
allowlisted as a route-scope DI seam in the same way
`web.server.db` is. Migrated all 26 patch sites in
`tests/test_web_server.py`. Dropped 26 findings (110 → 84).

### ~~5. Allowlist remaining web.routes DI seams + migrate match_folders_to_requests~~ (LANDED)

Shipped in PR #323. `web.routes.imports.cleanup_all_wrong_matches` (3
sites) and `web.server.compute_library_rank` (1 site) allowlisted as
route-scope DI seams. `web.routes.imports.match_folders_to_requests` (1
site) migrated to drive the real fuzzy matcher with shared
artist/album tokens. Dropped 5 findings (84 → 79).

### ~~6. Item P — per-module `finalize_request` DI seams for non-web tests~~ (LANDED)

Shipped in PR #324. Same shape as PR #322 applied to four more
modules: `lib.import_dispatch`, `harness.import_one`,
`scripts.pipeline_cli`, `scripts.repair`. Each binds
`finalize_request = transitions.finalize_request` at module scope and
exposes it as an allowlisted seam. Migrated 10 test patches. Dropped
10 findings (79 → 69).

### ~~7. test_import_one_stages.py harness migration~~ (LANDED)

Shipped in PR #325. Built `FakeBeetsDB` in `tests/fakes.py` (minimal
surface: `album_exists`, `get_album_info(mb_release_id, cfg)`,
`get_all_album_ids_for_release`, `get_item_paths`, `close` +
seed-helpers + a context-manager) with five self-tests in
`tests/test_fakes.py`. Migrated `TestPipelineDbUpdate` (4 sites,
`MagicMock()` → `FakePipelineDB()`) and the four
`beets = MagicMock()` sites to `FakeBeetsDB`. Dropped 8 findings
(69 → 61).

### ~~8. test_download.py harness migration~~ (LANDED)

Shipped in PR #326. Allowlisted four in-module DI seams in `lib.download`
(`dispatch_import_core`, `process_completed_album`, `_process_beets_validation`,
`log_validation_result`) plus the `cleanup_wrong_match` service-layer seam.
Migrated `slskd = MagicMock()` in `_make_ctx` to `FakeSlskdAPI()`.
Dropped 19 findings via allowlist + 1 migration (61 → 42).

### ~~9. test_repair_cli.py harness migration~~ (LANDED)

Shipped in PR #327. Migrated 4 `db = MagicMock()` sites in
`TestCollectIssues.test_auto_import_in_progress_*` to `FakePipelineDB`
using `queue_execute_results` from PR #321. Allowlisted three
`scripts.repair` in-module DI seams (`_collect_issues`,
`find_orphaned_downloads`, `find_blocked_recovery_issues`). Dropped 8
findings (42 → 34).

### ~~10. test_import_dispatch.py harness migration~~ (LANDED)

Shipped in PR #328. Lifted `quality_gate_decision` to a module-level
binding in `lib.import_dispatch` (same shape as `finalize_request` in
#322); migrated 5 patches to the new seam. Rewrote `_make_ctx()` to use
`make_ctx_with_fake_db` with a seeded `FakePipelineDB`. Allowlisted
`stage_to_ai_path` as a staging-destination seam. Dropped 7 findings
(34 → 27).

### ~~11. test_pipeline_cli.py / test_import_queue.py residuals~~ (LANDED)

Shipped in PR #329. Allowlisted `preview_import_from_values` /
`full_pipeline_decision` (pure-decision DI seams matching the
`quality_gate_decision` rationale), `MbidReplaceService`
(constructor-replacement seam), `import_preview_worker.run_once`
(worker tick stub), and `lib.download._handle_valid_result` (in-module
seam). Migrated remaining `db = MagicMock()` (test_pipeline_cli) and
`beets = MagicMock()` (test_import_queue). Fixed docstring noise where
helper docstrings tripped the audit regex. Dropped 17 findings
(27 → 10).

### ~~12. Residual cleanup~~ (LANDED)

Shipped in PR #330. Added `FakeBeetsDB.get_album_path_by_id` + seed
helper; migrated `test_beets_album_op.py::TestMoveAlbum` (5 sites)
and `test_dispatch_core.py::_patch_beets_album`. Broadened
`resolve_failed_path` allowlist to `lib.\\w+.resolve_failed_path`.
Allowlisted three RED-guard patches in `test_import_one_stages.py`
(harness measurement helpers — patches assert NONE are called when
pre-recorded evidence is supplied). Dropped 6 findings (10 → 4).

## Remaining 4 findings (deferred per #301)

| File | Item | Why |
|------|------|-----|
| `test_cycle_summary.py` | J | `album_match` exception-injection. Needs production refactor (extract try/finally credit helper). |
| `test_matching.py` | A | `_track_titles_cross_check` migration. Needs tight input tuning to construct passing-but-failing match cases. |
| `test_web_server.py` (×2) | (harness) | `mock_db = MagicMock()` + `failing_db = MagicMock()` — the shared file-level harness backing hundreds of contract tests. Migrating would mean rewriting every `mock_db.foo.return_value = ...` to per-test `FakePipelineDB` seeding. Worth its own multi-PR effort. |

## What's NOT next

- **Don't migrate one-off var-name sites** until the three DI refactors land. They're already on the residual list and individually low-yield.
- **Don't extend the allowlist further** unless you find a genuine thin seam wrapper I missed. The current allowlist is already covering all the obvious subprocess/HTTP/filesystem wrappers.
- **Don't try to delete `mock_audit_baseline.json`** (Phase 3 of #290) until the baseline reaches 0. The grandfather approach is the whole point.

## How to run the workflow

Each migration PR follows the same shape:

```bash
git checkout -b feat/mock-migrate-WHAT main
# Make changes
nix-shell --run "python3 -m unittest tests.test_WHAT"    # target file passes
nix-shell --run "bash scripts/run_tests.sh"               # full suite passes
nix-shell --run pyright                                   # 0 errors on full repo
python3 tests/_rebuild_mock_audit_baseline.py             # baseline shrinks
git add -A && git commit -m "test(WHAT): migrate ..."
git push -u origin feat/mock-migrate-WHAT
gh pr create --base main --title "test(WHAT): ..." --body "..."
gh pr merge <PR> --merge --delete-branch
```

The pre-commit hook + audit + skip-audit gates all enforce themselves. If the audit fails on a PR, either:
- You added a new anti-pattern site → fix or use a typed fake
- You removed sites but didn't re-snapshot → run `python3 tests/_rebuild_mock_audit_baseline.py`

## Pointers

- **Master plan**: issue #290
- **Deferred items / refactor ideas**: issue #301
- **Rule**: `.claude/rules/code-quality.md` § "MOCKS: LEAF-SEAM ONLY"
- **Audit**: `tests/test_mock_audit.py`, `tests/_mock_audit_scanner.py`
- **Fakes**: `tests/fakes.py` (FakePipelineDB, FakePipelineDBSource, FakeSlskdAPI)
- **Skip-test ban (related)**: `tests/test_skip_audit.py`, CLAUDE.md § "Skipped tests are an anti-pattern"

## Maintenance

Update this file whenever:
- Baseline drops or grows significantly (every PR is fine)
- An item on the "three next moves" list lands — replace with the new next-best
- The mental model changes (new heuristic, new fake category)

Don't update for individual migrations that don't change the strategic picture.
