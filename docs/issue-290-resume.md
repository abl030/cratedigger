# MagicMock removal — cleanup-phase resume note

This is the **entry point** for picking up the cleanup phase of the stateful-MagicMock removal effort with fresh context. Read this first. The long-form history (Phase 1 + Phase 2, 30+ landed PRs, 160→4 baseline) lives in **closed** issues #290 and #301. The remaining work is tracked in the **active cover issue #333**.

## Where we are

- Mock-audit baseline: **4 findings across 3 files** (down from 613 → 160 → 4).
- Pyright clean on full repo.
- Full suite: 3700 tests green.
- Infrastructure shipped: `FakePipelineDB` (+ `queue_execute_results`), `FakeBeetsDB`, `FakeSlskdAPI`, `FakePipelineDBSource`, `make_ctx_with_fake_db`, `noop_quality_gate`, `RecordingQualityGate`, `try_enqueue(match_fn=)`, `dispatch_import_core(quality_gate_fn=)`.
- DI seams shipped: module-local `finalize_request` bindings across `web.routes.pipeline`, `lib.import_dispatch`, `harness.import_one`, `scripts.pipeline_cli`, `scripts.repair`. Module-local `quality_gate_decision` binding on `lib.import_dispatch`. In-module DI seams in `lib.download` and `scripts.repair`.

The 4-finding floor sounds great but masks **allowlist debt**. The cleanup phase below is about repaying that debt and addressing the documented deferred items, not chasing the baseline further per se.

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

## The cleanup phase — 4 steps

These are the four open work items, ordered by value-per-effort. Each is a separate PR.

### Step 1 — Migrate pure-decision allowlists to real-input tests ✅ DONE

Landed: orchestration tests now drive the real `quality_gate_decision`,
`full_pipeline_decision`, and `preview_import_from_values` via constructed
inputs. Allowlist entries removed from `tests/_mock_audit_scanner.py`;
`.claude/rules/code-quality.md` "Pure-decision allowlist policy" callout
flipped from "known cleanup" to "never reintroduce".

### Step 2 — `stage_to_ai_path` allowlist removal (~30 min, smallest)

`lib.download.stage_to_ai_path` is pure path-construction with no I/O. Currently allowlisted as a "staging-destination seam" — that rationale is a stretch.

**The migration:** `test_import_dispatch.py::_dispatch_valid_result_cmd` patches it to return `dest_dir = os.path.join(tmpdir, "dest")`. Compute the real path: configure the test's `ctx.cfg.beets_staging_dir = tmpdir` and call the real `stage_to_ai_path` to compute the destination, then create that directory.

- Remove the allowlist entry.
- Update the helper to drive the real function with a tempdir staging root.
- Verify file moves still land where the test expects.

### Step 3 — In-module DI seam audit (~half day)

Module-local DI seams are legitimate when production is dispatched by URL or argparse (no kwarg path). For mid-tier private helpers called from other production functions, kwarg DI would be cleaner.

**Candidates to audit:**

| Current allowlist entry | Caller | Could be kwarg DI? |
|-------------------------|--------|---------------------|
| `lib.download._handle_valid_result` | `process_completed_album` | Likely yes — `process_completed_album` could take `handle_valid_fn=_handle_valid_result` |
| `lib.download._process_beets_validation` | `process_completed_album` | Likely yes — same shape |
| `lib.download.process_completed_album` | `poll_active_downloads` | Maybe — `poll_active_downloads` already wraps a stateful loop |
| `lib.download.dispatch_import_core` | `_handle_valid_result` | Already wraps `dispatch_import_core`'s kwarg `quality_gate_fn`; the re-import is for testing the wrapper |
| `scripts.repair._collect_issues` | `cmd_fix` / `cmd_scan` | CLI dispatch — keep as module-local seam |
| `scripts.repair.find_orphaned_downloads` | `_collect_issues` | Mid-tier — could be kwarg DI |

For each candidate that could be kwarg DI, prototype the change in a single tests/file pair, measure how the test reads vs the module-attribute version, and decide. Mid-tier wins might be small but they're more architecturally honest.

**Approach for each:**
- Add a kwarg to the calling function with the seam fn as the default.
- Update tests to pass the stub by value.
- Remove the allowlist entry.

### Step 4 — `test_web_server.py` shared MagicMock harness (multi-PR effort)

The two remaining audit findings (`mock_db = MagicMock()` at line ~149 and `failing_db = MagicMock()` around line 6875) are the shared file-level harness backing hundreds of contract tests in `test_web_server.py::_make_server()`. Plus the existing `make_db.foo.return_value = ...` chains throughout the file.

**Why deferred:** migrating `_make_server()` to use `FakePipelineDB` requires rewriting every `self.mock_db.foo.return_value = ...` in every test that uses `_WebServerCase`. That's ~300+ tests. Each one needs:
- Real seeded request rows via `seed_request(make_request_row(...))`
- Domain-state assertions via `db.request(id)["status"]` instead of `mock_db.update_status.assert_called_with(...)`
- Production-shape data for JSONB / datetime / UUID columns (see `.claude/rules/code-quality.md` § API Contract Tests)

**Approach (multi-PR):**
- PR A: Rebuild `_make_server()` to construct a `FakePipelineDB` with a sensible base seed. Keep the `self.mock_db` attribute pointing at it for now (introduce `self.fake_db` alias). Migrate ~10 simple tests as a proof.
- PR B-N: Per test class, rewrite the per-test seed + assertion patterns. Each PR should drop the count of tests still relying on `mock_db.*.return_value` patterns.
- Final PR: `mock_db` attribute is gone, only `fake_db`. Remove the 2 remaining audit findings.

This is genuinely 5-10 PRs of work, but each one is small and ships independently.

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
