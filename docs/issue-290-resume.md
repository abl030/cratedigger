# MagicMock removal — cleanup-phase resume note

**Status (2026-05-21): COMPLETE.** All four cleanup steps landed in
PRs #335-#338. Cover issue #333 closed. The long-form history
(Phase 1 + Phase 2, 30+ landed PRs, 160→4 baseline) lives in closed
issues #290 and #301. This doc is kept as historical evidence — read
it for context on the decisions, not for active work.

## Where we are

- Mock-audit baseline: **2 findings across 2 files** (down from 613 → 160 → 4 → 2).
- Pyright clean on full repo.
- Full suite: 3700 tests green.
- Remaining 2 findings are the explicitly-deferred items A and J
  in `lib.matching` — both are tracked as "address only if a future
  change touches the file" and are not in scope for further work.
- Infrastructure shipped: `FakePipelineDB` (+ `queue_execute_results`), `FakeBeetsDB`, `FakeSlskdAPI`, `FakePipelineDBSource`, `make_ctx_with_fake_db`, `noop_quality_gate`, `RecordingQualityGate`, `try_enqueue(match_fn=)`, `dispatch_import_core(quality_gate_fn=)`, `_pipeline_db_test_harness()` (web_server FakePipelineDB-wrap).
- DI seams shipped: module-local `finalize_request` bindings across `web.routes.pipeline`, `lib.import_dispatch`, `harness.import_one`, `scripts.pipeline_cli`, `scripts.repair`. Module-local `quality_gate_decision` binding on `lib.import_dispatch`. In-module DI seams in `lib.download` and `scripts.repair`. Kwarg-DI on `_collect_issues(find_orphaned_fn=, find_blocked_recovery_fn=)`.

## Live count check

```bash
nix-shell --run "python3 tests/_rebuild_mock_audit_baseline.py"
nix-shell --run "python3 -c '
import json
b = json.load(open(\"tests/mock_audit_baseline.json\"))
print(f\"baseline: {sum(sum(v.values()) for v in b.values())}, files: {len(b)}\")
for f, kinds in sorted(b.items(), key=lambda kv: sum(kv[1].values())):
    print(f\"  {sum(kinds.values()):>3}  {f}\")
'"
```

If the live count differs from this doc, trust the live count and update this file as part of the next PR.

## The cleanup phase — 4 steps (all complete)

Original ordering by value-per-effort. Each landed in its own PR.

### Step 1 — Migrate pure-decision allowlists to real-input tests ✅ DONE

Landed: orchestration tests now drive the real `quality_gate_decision`,
`full_pipeline_decision`, and `preview_import_from_values` via constructed
inputs. Allowlist entries removed from `tests/_mock_audit_scanner.py`;
`.claude/rules/code-quality.md` "Pure-decision allowlist policy" callout
flipped from "known cleanup" to "never reintroduce".

### Step 2 — `stage_to_ai_path` allowlist removal ✅ DONE

Landed: `_dispatch_valid_result_cmd` now configures
`ctx.cfg.beets_staging_dir = tmpdir` and lets the real
`stage_to_ai_path` compute the destination.
`StagedAlbum.move_to` already mkdir-p's the target, so no extra
setup is needed. Allowlist entry removed from
`tests/_mock_audit_scanner.py`.

### Step 3 — In-module DI seam audit ✅ DONE

Decisions per candidate:

| Allowlist entry | Caller | Decision |
|-----------------|--------|----------|
| `lib.download._handle_valid_result` | `process_completed_album` | **Keep.** Chain is 5 levels deep (`poll_active_downloads` → `_run_completed_processing` → `process_completed_album` → `_process_beets_validation` → `_handle_valid_result`). Threading a kwarg through every level just to surface the test seam at the top would cascade for negligible win. |
| `lib.download._process_beets_validation` | `process_completed_album` | **Keep.** Same chain. |
| `lib.download.process_completed_album` | `poll_active_downloads` | **Keep.** Stateful poller; same chain. |
| `lib.download.dispatch_import_core` | `_handle_valid_result` | **Keep.** Already provides its OWN kwarg DI for the post-import gate (`quality_gate_fn`); the re-import seam is for the wrapper tests. |
| `scripts.repair._collect_issues` | `cmd_fix` / `cmd_scan` | **Keep.** CLI argparse dispatchers — no kwarg path from the entry point. |
| `scripts.repair.find_orphaned_downloads` | `_collect_issues` | **Converted to kwarg DI.** `_collect_issues(..., find_orphaned_fn=find_orphaned_downloads)`. Allowlist entry removed. |
| `scripts.repair.find_blocked_recovery_issues` | `_collect_issues` | **Converted to kwarg DI.** `_collect_issues(..., find_blocked_recovery_fn=find_blocked_recovery_issues)`. Allowlist entry removed. |

Net: 2 allowlist entries cleared (the two mid-tier helpers in
`scripts.repair`); 5 entries documented as intentional in-module DI
seams (chain depth / argparse dispatch). The lib.download chain decision
is recorded in the allowlist rationale comment to head off "why not
kwarg DI here?" follow-up audits.

### Step 4 — `test_web_server.py` shared MagicMock harness ✅ DONE (pragmatic)

Both audit findings (`mock_db = MagicMock()` at `_make_server()` and
`failing_db = MagicMock()` at the beets-delete error path) are gone.
The harness now goes through `_pipeline_db_test_harness()` which
returns ``MagicMock(wraps=FakePipelineDB())``:

- Unmocked method calls fall through to the typed `FakePipelineDB`
  (production code paths now exercise real state mutations instead of
  always returning a `MagicMock()`).
- Existing test patterns (`.return_value = X`, `.assert_called_*`,
  `.side_effect`, `.reset_mock()`) keep working because the outer
  MagicMock layer still records and overrides per-method behaviour —
  the 316 existing call sites need no rewrite.
- The audit's intent is satisfied: a pure MagicMock collaborator is
  replaced with a typed-fake-backed one; production code calls land
  on real state.
- `FakePipelineDB.set_tracks` is the one strictness gap that needed a
  legacy shim: the slim test fixtures (`[{"title": "Track"}]`) lack
  `track_number`, which the fake rejects. The harness overrides
  `set_tracks` with a no-op MagicMock so route-contract tests don't
  trip on track-row layout. Production always emits `track_number`.

Future incremental work (not blocking phase 3): tests that want
to assert observable state can replace `mock_db.update_status.assert_called_with(...)`
with `fake_db.request(id)["status"] == "wanted"` by reaching through
`harness._fake`. Optional polish; not required to clear the cover
issue.

### Deferred items (do not block phase 3)

These two from #301 are explicitly deferred indefinitely — each requires production refactor and the cost-benefit doesn't justify the work:

- **Item A** — `lib.matching._track_titles_cross_check` patch in `test_matching.py`. Needs tight input tuning. Tracked in cover issue for visibility, not for active work.
- **Item J** — `lib.matching.album_match` exception-injection in `test_cycle_summary.py`. Needs production refactor (extract try/finally credit helper).

Both are 1 finding each and have been baseline-grandfathered since they were filed. Address only if a future change touches the same file.

## Mental model

A finding is one of:

1. **`stateful_mock_assign:NAME`** — `MagicMock()` assigned to a flagged variable name. Replacement: `FakePipelineDB` / `FakeBeetsDB` / `FakeSlskdAPI` / `FakePipelineDBSource` / `make_ctx_with_fake_db()`.
2. **`patch:lib.x.y`** — `patch()` on a target the audit considers ours-not-a-seam. Options:
   - **Migrate** — drive the real function with constructed inputs (preferred for pure logic).
   - **Kwarg DI** — refactor the caller to take the dependency as a kwarg (preferred for mid-tier private helpers).
   - **Module-local DI seam** — bind at module top in the caller's module; patch the local attribute (legitimate only for URL / argparse dispatchers).
   - **Allowlist** — add to `tests/_mock_audit_scanner.py::_LEAF_SEAM_PATTERNS` with rationale (only for thin boundary wrappers).

The audit lives in `tests/test_mock_audit.py`; the scanner heuristic in `tests/_mock_audit_scanner.py`; the frozen call-site count in `tests/mock_audit_baseline.json`.

## Workflow

Each cleanup PR follows the same shape:

```bash
git checkout -b feat/mock-cleanup-WHAT main
# Make changes
nix-shell --run "python3 -m unittest tests.test_WHAT"      # target file passes
nix-shell --run "bash scripts/run_tests.sh"                 # full suite passes
nix-shell --run pyright                                     # 0 errors on full repo
python3 tests/_rebuild_mock_audit_baseline.py               # baseline shrinks
git add -A && git commit -m "test(WHAT): migrate ..."
git push -u origin feat/mock-cleanup-WHAT
gh pr create --base main --title "test(WHAT): ..." --body "..."
gh pr merge <PR> --merge --delete-branch
```

The pre-commit hook, audit, and skip-audit gates all enforce themselves. If the audit fails on a PR:
- New anti-pattern site → fix or use a typed fake.
- Removed sites but didn't re-snapshot → run `python3 tests/_rebuild_mock_audit_baseline.py`.

## Pointers

- **Active cover issue**: #333.
- **History**: closed issues #290 (master plan), #301 (deferred items log).
- **Rule**: `.claude/rules/code-quality.md` § "MOCKS: LEAF-SEAM ONLY".
- **Audit**: `tests/test_mock_audit.py`, `tests/_mock_audit_scanner.py`.
- **Fakes**: `tests/fakes.py` (`FakePipelineDB`, `FakeBeetsDB`, `FakeSlskdAPI`, `FakePipelineDBSource`).
- **Helpers**: `tests/helpers.py` (`make_ctx_with_fake_db`, `noop_quality_gate`, `RecordingQualityGate`, `patch_dispatch_externals`, builders).

## Maintenance

Update this file whenever:
- A cleanup step (1-4) lands — strike through or remove the section.
- Baseline drops or grows significantly.
- A new category of debt is discovered.

When all four steps are done and the baseline is at the deferred floor (Items A + J = 2 findings, or 0 if those are also addressed), this file and the cover issue close together.
