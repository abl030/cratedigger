# Finding dead code

Two complementary tools — use them together when the test suite starts feeling like a tax on code nobody runs in production.

**Static (fast, noisy):** vulture flags unreferenced functions / classes / variables.

```bash
nix-shell --run "bash scripts/find_dead_code.sh"             # diff vs whitelist — only new findings
nix-shell --run "bash scripts/find_dead_code.sh --baseline"  # all candidates (initial hunt)
```

The whitelist at `tools/vulture/whitelist.py` masks the 166 known false positives on main (msgspec Struct fields, beets ImportSession overrides, route handler dispatch, SQL DictRow attribute access). After deleting genuinely-dead code, regenerate the baseline:

```bash
nix-shell --run "vulture --make-whitelist lib/ web/ harness/ scripts/ cratedigger.py album_source.py > tools/vulture/whitelist.py"
```

**Runtime (slow, unambiguous):** coverage.py against production traffic, then diff against test coverage.

1. Enable in `~/nixosconfig/modules/nixos/services/cratedigger.nix` (downstream wrapper): `services.cratedigger.coverage.enable = true;`. Deploy. Data accumulates at `/var/lib/cratedigger/coverage/` from the cratedigger oneshot, importer, preview worker, and web server. ~5-10% CPU overhead per process. The subprocess `.pth` shim (`nix/coverage-subprocess.nix`) makes `import_one.py` runs participate too.
2. After a representative window (a week, including at least one operator action: Replace, force-import, ban-source — otherwise rare operator paths look dead):
   ```bash
   nix-shell --run "bash scripts/run_tests_with_coverage.sh"   # populates build/test-coverage/
   nix-shell --run "bash scripts/coverage_report.sh doc2"      # rsyncs from prod, builds build/coverage-html/
   nix-shell --run "python3 scripts/coverage_diff.py"          # the test-only-lines report
   ```
3. `build/test-only-lines.txt` lists every line that tests cover but production never executed. That's the actionable dead-code candidates — either delete or demote from unit tests to a manual smoke procedure.

**Caveat:** coverage.py traces only Python; the `beet` subprocess is third-party and unmeasured. Anything reachable only via `beet` plugins won't appear in either side of the diff.

## Cascading orphans — regen the whitelist after every deletion

Deleting a vulture-flagged helper frequently exposes a **deeper orphan** that was kept alive solely by the thing you just deleted. The pattern has now bitten this audit four times:

- **PR #358** — deleting `finalize_request_if_plan_current` orphaned `PipelineDB.is_request_plan_current` (its only reader)
- **PR #360** — deleting `move_album` orphaned `BeetsOpResult.new_path` (only set by `move_album`)
- **PR #363 → #364** — deleting `_select_variant_for_album` (in #356) exposed `build_query` and `select_variant`; deleting those exposed `strip_short_tokens`
- **PR #355** — deleting all three public exports of `lib/import_service.py` left `_apply_request_spectral_fields` as a cascading orphan that justified deleting the whole module

The mechanic is simple but easy to miss: vulture's whitelist contains an entry per known orphan, keyed by name. When a helper goes away, its callees lose their last reference but aren't in the whitelist yet (they were "used" before). The next `vulture --make-whitelist` run will surface them, and `bash scripts/find_dead_code.sh` will go red until you either delete them or accept them as new whitelist entries.

**Workflow per deletion PR:**
1. Make the deletion + delete the tests that exercised it.
2. Regenerate the whitelist: `nix-shell --run "vulture --make-whitelist lib/ web/ harness/ scripts/ cratedigger.py album_source.py" 2>/dev/null > tools/vulture/whitelist.py` (then prepend the 22-line header back from the previous version).
3. Diff the whitelist (`git diff tools/vulture/whitelist.py`). New entries are cascading orphans you exposed.
4. For each new entry, decide:
   - **Fold into this PR** if the orphan is small, contained, and the test cleanup is bounded (~50 LOC). Best when the orphan is structurally tied to the deletion (`strip_short_tokens` was the canonical example — its body matched what only the deleted callers needed).
   - **Park in the whitelist** if it'd double the PR. Document the cascade in the PR description and the umbrella issue so the next pass picks it up.

**Anti-pattern:** deleting a helper, leaving the cascading orphans in the whitelist, and forgetting about them. The whitelist grows silently and the audit goes stale.
