---
name: feedback-test-fidelity-meta-pattern
description: "The \"fix lands in code but production never sees it\" smell — fake/test infrastructure is more permissive than production, so passing tests hide divergence at the real adapter or DB boundary"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 740dc87f-bc63-476a-b5b2-3cb425f27455
---

When reviewing or implementing changes that involve external adapters (MB mirror, Discogs mirror, ytmusicapi, slskd-api) OR database write paths (PipelineDB, BeetsDB), the failure mode to watch for is: **the fix is claimed and the tests confirm it, but production never actually sees it, because the test infrastructure is more permissive than production.**

**Why:** On 2026-05-27, two back-to-back ce-code-review rounds on the YouTube Music album resolver PR each turned up a P0 of exactly this shape:

1. **Round 1 #1:** `lib/youtube_album_service.py::_resolve_mb_group` expected `mb_get_release(rg_mbid)` to return `None` on 404, but the real `web/mb.py::get_release` raises `urllib.error.HTTPError`. Every test used `lambda m: None` so the production crash never surfaced — the fakes were silently looser than the real adapter's exception contract.

2. **Round 2 P0:** `lib/youtube_album_service.py` writes `album_title` into the per-row dict passed to `PipelineDB.upsert_youtube_album_mapping`. The SQL `INSERT` column list does not include `album_title`. `psycopg2.extras.execute_values` silently ignores extra dict keys, so production writes drop the field on the floor. `FakePipelineDB` stores the entire dict verbatim, so the round-trip test passed despite production being broken — the fake was silently looser than the actual schema.

Both bugs shipped with passing tests, passing pyright, passing vulture, and clean ce-doc-review and ce-code-review-round-1 passes. They were only caught by an adversarial / api-contract / correctness reviewer reading the SQL by hand and noticing the column-set vs dict-set drift. That detection method does not scale.

**How to apply:**

When implementing or reviewing changes that involve either kind of boundary, force the test to be production-faithful before declaring done:

- For DB writes: at least one **real-PG round-trip test** must assert every key in the input payload survives the write+read cycle. `FakePipelineDB` tests don't count for this check — they store dicts verbatim and miss schema drift. Pattern:
  ```python
  rows_in = [self._row(...)]
  self.db.upsert_X("k", rows_in)
  rows_out = self.db.get_X("k")
  for key in rows_in[0]:
      self.assertEqual(rows_out[0][key], rows_in[0][key],
                       f"field {key} was dropped at the PG boundary")
  ```

- For external adapter fakes: the failure-case fake must raise the same exception class the real adapter raises, not return `None` or a synthetic stand-in. If you find yourself writing `lambda m: None` to simulate a mirror miss, that is a smell — check what the real adapter does. `web/mb.py::get_release` raises `urllib.error.HTTPError`; `web/discogs.py::get_release` does similar; `ytmusicapi.YTMusic` has its own taxonomy (`YTMusicServerError` / `requests.Timeout`).

Codified as `.claude/rules/test-fidelity.md`. Stronger enforcement layers (Struct-typed write interface, audit tests, adapter contract tests) are documented as future work in the same file.

Related: [[feedback-finish-the-job]] — the meta-frame is the same, "fix is incomplete unless production observes it." The test-fidelity rules are the boundary-specific enforcement of that principle.

Also related — three existing solution docs captured fragments of this lesson but didn't codify it as a forbidden pattern:
- `docs/solutions/testing/contract-test-mocks-must-mirror-production-shape.md`
- `docs/solutions/testing/mocked-contract-tests-miss-helper-mirror-integration-bugs.md`
- `docs/solutions/testing/cohort-filter-must-match-production-column-shape.md`

The rule file turns the lessons into actionable guardrails.
