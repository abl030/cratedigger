# #1278 point 9 — one parameterized root-coverage rule (SHIPPED 2026-08-31)

PR #1294, merged 31b5de46 (merge commit), gate-verified on combined main. No
deploy: dev tooling only (scripts/ + tests/ + docs), same precedent as point 6.

What landed:
- `ROOT_COVERAGE_RULES` table in `scripts/targeted_test_selection.py` replaced
  the three fail-closed branches; byte-identical port (reader's independent
  1561-path differential) except the deliberate change: `scripts/**/*.sh` now
  fail closed at zero neighbours. 13 of 16 wrappers previously selected
  nothing; 5 resolve via the new `.sh` basename probe, 6 gained verified
  entries, 2 admitted gaps (`lint.sh`, `mcp-playwright.sh`).
- `tests/test_selection_coverage_audit.py` (28 tests) replaced the two twin
  audits; contracts A (named neighbours exist, `*_NEIGHBOURS` by
  introspection), B (no entry redundant with fallback-only resolution — two
  entries deleted), C (`MASKABLE_ENTRY_PINS`, 16 entries whose deletion the
  fallback would mask; single-place deletion is now loud).
- The audit's own scope is externally anchored (`EXPECTED_ADMITTED_SELECTS_
  NOTHING`, `EXPECTED_SUFFIXES`, registry-label `assertIs`) — review round 1
  found the audit SELF-VACATED (deriving scope from the column it constrains);
  the anchor pattern is the fix and the lesson.

Process lessons (this series re-proved them):
- Correction rounds minted a fresh false claim THREE times; each caught only
  by the next independent read — twice by the implementer's own pre-commit
  re-read after being warned. The `lint.sh` grep-rationale was falsified by
  its own cited grep TWICE.
- New EXACT_PATH_NEIGHBOURS entries with fallback neighbours now need a
  `MASKABLE_ENTRY_PINS` row (the audit's failure message says what to add) —
  concurrent sessions were warned; item 7's `lib/pipeline_db/decisions.py`
  entry is pinned.
- Integration: #1292's wire_types entry was measured maskable mid-flight; the
  pin was folded into the merge commit deliberately so no RED commit exists
  for bisect.

Residuals recorded on issue #1278 (none cleared the bar for a new issue).
See [[project_1278_pt4_beets_child_shipped]], [[feedback_correction_rounds_mint_false_claims]].
