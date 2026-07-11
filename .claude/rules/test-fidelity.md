# Test Fidelity Rules

The smell: **a fix lands in code that the tests confirm, but production never actually sees it, because the test infrastructure is more permissive than production.** Two PRs back-to-back tripped on the same shape — round 1 had `_resolve_mb_group` expecting `None` on 404 when the real `web/mb.py::get_release` raises `HTTPError`, and round 2 had `album_title` written into the row dict by the service but silently dropped by `psycopg2.extras.execute_values` because the SQL INSERT column list didn't include it (`FakePipelineDB` stored the whole dict so the test passed).

These rules codify the meta-pattern as forbidden anti-patterns.

## Rule A — Production-shape write contract

**Every `PipelineDB` write method (`upsert_*`, `add_*`, `update_*`, etc.) must have at least one real-PG round-trip test that asserts every key in the input dict is readable back via the corresponding `get_*` method.**

A test that uses `FakePipelineDB` is not enough — fakes store the input dict verbatim, so schema drift between the Python payload and the SQL column list is invisible. The contract under test is "what the operator's production database actually preserves," and the only fixture that captures that is the ephemeral PG conftest fixture.

**Concrete pattern (canonical: `tests/test_pipeline_db.py::TestYoutubeAlbumMappings`):**

```python
def test_upsert_round_trip_preserves_every_field(self):
    rows_in = [self._row(yt_browse_id="MPREb_a", yt_year=1996, ...)]
    self.db.upsert_youtube_album_mapping("rg-1", "mb", rows_in)
    rows_out = self.db.get_youtube_album_mapping("rg-1", "mb")
    # EVERY input key must round-trip — not just the obvious ones.
    for key in rows_in[0]:
        self.assertEqual(rows_out[0][key], rows_in[0][key],
                         f"field {key} was dropped at the PG boundary")
```

**What this catches:** the album_title bug from round 2. The fix landed in the service, the `PersistedYoutubeRow` Struct had the field, the `FakePipelineDB` stored it, but the SQL INSERT column list didn't include `album_title` — so production writes silently dropped the field and reads always returned `None`. A real-PG round-trip would have failed instantly.

**Side effect:** when this test fails, you know exactly which field drifted. The error names the column.

## Rule B — Fakes must mirror real-adapter exception contracts

**When a test fakes an external dependency that has a documented exception contract, the failure-case fake MUST use the real exception classes the production code can encounter. Do not return `None` (or any synthetic stand-in) where the real adapter raises.**

External dependencies in scope:
- `web/mb.py::get_release` and `get_release_group_releases` — raises `urllib.error.HTTPError` on 404, `urllib.error.URLError` on transport failure
- `web/discogs.py::get_release` and `get_master_releases` — same exception shape plus `requests.HTTPError` paths
- `ytmusicapi.YTMusic.search` and `get_album` — raises `YTMusicServerError` / `YTMusicUserError` / `requests.Timeout` / `requests.ConnectionError` / `KeyError`
- `lib/slskd_client.py` — `requests.HTTPError` with structural `.response.text`

**Forbidden anti-pattern:**

```python
# WRONG — production raises HTTPError on 404, this hides the divergence
result = resolve_youtube_album(
    rg_mbid,
    mb_get_release=lambda m: None,  # 404 path simulated as None
    ...
)
```

**Required pattern:**

```python
# RIGHT — fake mirrors the real adapter's exception contract
import urllib.error
def _mb_404(_mbid):
    raise urllib.error.HTTPError(
        url="...", code=404, msg="Not Found", hdrs=None, fp=None)

result = resolve_youtube_album(
    rg_mbid,
    mb_get_release=_mb_404,
    ...
)
```

**What this catches:** the round 1 #1 bug. `_resolve_mb_group` expected `mb_get_release(rg_mbid)` to return `None` on 404, but the real adapter raised `HTTPError`. Every test used `lambda: None` so the production crash never surfaced.

**Helper rule:** if you find yourself writing `lambda m: None` to fake a mirror lookup, that is a smell — the production adapter doesn't return None on the documented failure mode. Either:
- Use a documented stand-in (`tests/fakes/__init__.py::FakeMBLookup(raises_on_404=True)`), or
- Inline the real exception class via `lambda m: (_ for _ in ()).throw(urllib.error.HTTPError(...))`.

The first form is preferred — if the helper doesn't exist yet, add it to
`tests/fakes/` with the exception contract documented in its docstring.

## Stronger enforcement (future work)

The two rules above are guidance. Three layers of stronger enforcement, in order of ROI:

### 1. Struct-typed write interface (high ROI, moderate effort)

Make `PipelineDB.upsert_*` methods accept typed `msgspec.Struct` instances instead of `list[dict]`. The Struct's field names become the canonical write contract. Add an init-time assertion (or migration test) that the Struct's field names are a subset of the table's columns via `information_schema.columns`. Then:
- Adding a field to the Struct without a corresponding migration fails at write time, not silently
- The IDE / pyright sees the type and prevents dict typos
- The fake can no longer "store anything" — it has to type-check too

The album_title bug becomes impossible to express.

### 2. `tests/test_pipeline_db_write_audit.py` (medium ROI, low effort)

A test that:
- Introspects `PipelineDB` via `inspect` for every `upsert_*` / `add_*` / `update_*` method
- Asserts each has at least one matching real-PG test in `TestX::test_upsert_round_trip_preserves_every_field` shape
- Fails CI if a write method ships without a round-trip guard

Doesn't catch the bug directly, but forces every new write method to have the round-trip test from Rule A.

### 3. Adapter contract tests + forbidden-pattern audit (medium ROI, low effort)

For each external dependency, a "contract test" that documents what the real adapter raises:

```python
# tests/test_mirror_contracts.py
def test_web_mb_get_release_raises_HTTPError_on_404():
    with self.assertRaises(urllib.error.HTTPError):
        web.mb.get_release("00000000-0000-0000-0000-000000000000")
```

Plus a `tests/_lambda_audit.py` scanner that grep's for `mb_get_release=lambda` / `discogs_get_release=lambda` patterns in test files and fails on any not in the allowlist. Same pattern as the existing `_mock_audit_scanner.py`.

When a new mirror adapter is added, the contract test forces the author to document the exception types; the lambda-audit forces tests to use the canonical fake instead of raw lambdas.

## Why these matter

Both bugs from rounds 1 and 2 shipped with passing tests, passing pyright, passing vulture, and a clean review pass. They were only caught by adversarial / api-contract / correctness reviewers reading the code by hand against the migration SQL. That's not a sustainable detection method — the next bug of the same shape will ship.

These rules don't replace review; they make the smell harder to introduce in the first place. If a future PR violates Rule A, the real-PG test will fail at PR time. If a future PR violates Rule B, the fake won't compile against the production exception contract.

## Related memory

- [[feedback-test-fidelity-meta-pattern]] — the meta-pattern the rules codify
- See also: `docs/solutions/testing/contract-test-mocks-must-mirror-production-shape.md` — the original lesson that captured part of Rule A
- See also: `docs/solutions/testing/mocked-contract-tests-miss-helper-mirror-integration-bugs.md` — the original lesson that captured part of Rule B
