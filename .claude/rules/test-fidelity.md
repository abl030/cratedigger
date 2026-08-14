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

A fake must also mirror *when* the real edge fails, not only what it raises. An operator-facing claim about process behaviour owes a real-subprocess test. A fake that fails earlier than the real producer can manufacture a passing test for a false claim (a write-failing stdout fake vs a block-buffered pipe that raises EPIPE at shutdown).

External dependencies in scope:
- `web/mb.py::get_release` and `get_release_group_releases` — raises `urllib.error.HTTPError` on 404, `urllib.error.URLError` on transport failure
- `web/discogs.py::get_release` and `get_master_releases` — same exception shape plus `requests.HTTPError` paths
- `ytmusicapi.YTMusic.search` and `get_album` — raises `YTMusicServerError` / `YTMusicUserError` / `requests.Timeout` / `requests.ConnectionError` / `KeyError`; either method may also propagate `requests.exceptions.RetryError` when the injected production Session exhausts its configured status retries
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

## Rule C — A copy pin's input comes from the producer, never from a literal

**When a test pins operator-facing copy, the string that TRIGGERS that copy
must be derived from the code that produces it — the producing function
called for real, or the producing module's own exported constant. A
hand-typed literal in a fixture is not evidence that anything can produce
it.**

This is Rule B one layer up. Rule B stops a fake from returning a shape
production never returns; Rule C stops a fixture from feeding an *input*
production never emits. Both failures look identical from inside the test:
green, fluent, and describing a world that does not exist.

Rule C also applies when a docstring or PR claims a generated-test guard or
carve-out is load-bearing: prove it with a producible generated world or named
live evidence, not a hand-written impossible world.

**Forbidden anti-pattern:**

```python
# WRONG — the trigger is a string the test author invented. It exists
# nowhere in the codebase, so the copy behind it is unreachable in
# production and the assertion is unfalsifiable.
presentation = present_failure(_evidence(
    outcome="measurement_failed",
    error_message="FilesystemAuthorityError: path is outside the library root"))
self.assertEqual(
    presentation.verdict,
    "Measurement failed: installed path is outside the library root")
```

**Required pattern:**

```python
# RIGHT — the real authority function raises the real exception, composed
# exactly the way lib/import_preview.py composes the persisted detail.
with self.assertRaises(FilesystemAuthorityError) as caught:
    with open_configured_quarantine_directory(outside, roots):
        pass
detail = f"{type(caught.exception).__name__}: {caught.exception}"
presentation = present_failure(_evidence(
    outcome="measurement_failed", error_message=detail))
```

**What this catches:** issue #868's shipped defect. The copy asserted that
the *installed* path was outside the *library root*; the only string
production can raise there is about the CANDIDATE's quarantine roots. Every
fixture fed the invented literal by hand, so the wrong fact shipped fluent
and nothing failed. Issue #882 found the same shape in `web/classify.py`:
copy keyed on `no_candidates`, a scenario no producer has ever written,
while the producing string (`mbid_not_found`, 50 live rows) fell through to
the raw-token fallback.

**When the trigger genuinely cannot be produced in-process** — a persisted
enum, a DB column value, a decision name another worker writes — a literal
is allowed only if a producer audit traces it. The audit owes three things:

- the trigger is **SPELLED** as a string literal by a named production file
  (parse, don't grep: a mention in a comment or docstring is not a
  spelling, and that is exactly where a fabricated trigger hides);
- the set of triggers to trace is **DERIVED** from the module under test by
  introspection, never hand-listed, and an unrecognised match target
  **fails closed**;
- a trigger with no producer is legitimate only as a **HISTORICAL** one,
  registered as such with the live-row evidence written down.

Canonical implementations: `tests/test_failure_presentation.py::TestEveryTriggerHasAProducer`
and `tests/test_classify_producer_audit.py`.

**Side effect:** when the audit fails it names the literal and the producer
that cannot emit it, which is the whole diagnosis.

## Rule D — Changed derived operator-facing presentation owes live-corpus evidence

**A PR that changes derived operator-facing presentation or output — a verdict,
summary, badge, renderer-computed string, or primitive JavaScript turns into a
visible state — must measure the change against the real corpus before it
ships: the old renderer and the new one, over every live row, reporting
changed-row counts BY CHANGED FIELD. Put the numbers in the PR body.**

Rules A–C keep a test from describing a world production never produces.
Rule D answers the question no test can: on the rows that actually exist,
what does this change? A presentation PR makes a claim about a live
population, and the only instrument that settles it is the real renderer over
real rows.

**Forbidden anti-pattern:**

```
# WRONG — the blast radius is asserted from reading the diff.
# "This only touches the mbid_not_found branch, so only those rows move,
#  and the badge can't be affected."
```

Reasoning from the diff is how all five #882 review rounds shipped a new
falsehood while correcting an old one — each with whole-repo Pyright
green, the full suite green, and adversarial review finding nothing.

**Required pattern.** Export the corpus first — one JSON row object per
line, read-only, from doc2. Exact SQL for the classify target plus the
batching note for corpora too large for one payload: `docs/debugging-cli.md`
§ "Live-corpus render differential". Then three commands, using
`scripts/render_differential.py`:

```bash
# 1. Render the base ref, with the base ref's own renderer.
git worktree add /tmp/rd-base <base-ref>     # e.g. origin/main
nix-shell --run "python3 /tmp/rd-base/scripts/render_differential.py \
  render --corpus /tmp/corpus.jsonl --out /tmp/base.jsonl"

# 2. Render the working tree.
nix-shell --run "python3 scripts/render_differential.py \
  render --corpus /tmp/corpus.jsonl --out /tmp/current.jsonl"

# 3. Diff. Every field is reported, zeros included.
nix-shell --run "python3 scripts/render_differential.py diff \
  --base /tmp/base.jsonl --current /tmp/current.jsonl"
```

The harness never does git surgery: the two-render dance is the runbook's
job, which is also what keeps both modes directly testable. `--target
module:attribute` renders through something other than the default classify
target. If the base ref predates the harness, copy
`scripts/render_differential.py` into the base worktree first — each side
still renders with its own tree's renderer. An agent working in an isolated
worktree can materialize the base tree with
`git archive <base-ref> | tar -x -C /tmp/rd-base` instead of `git worktree
add`; the render step is identical.

**When the PR adds or removes an output field**, the run correctly fails
closed: the base tree cannot produce the new field, so the two field sets
differ. That is not a reason to skip the differential. Re-run the `diff`
with `--allow-field-drift`: the shared fields are compared as usual and the
unshared ones are printed under `NOT COMPARED` and carried in the report's
`base_only_fields` / `current_only_fields`. Say in the PR body which field
was added and that it had no base value to compare.

**Read the zeros, but only after checking what produced them.** A zero is
evidence only if the field was actually watched and actually rendered by
the production path. Both halves have failed in review:

- The watched set is derived from the render target's output type and
  **fails closed** — a field is unwatched only when its declared type is
  provably numeric/boolean/null. An earlier fail-open version skipped
  `comparison_basis` (`dict[str, object]`, eight operator-visible strings
  behind the card's "Compared" row), so nulling every basis on the whole
  live corpus reported **0 changed rows**. Every render now also checks the
  converse — no unwatched field may hold text at runtime — and fails
  closed if one does.
- The render target must be the **whole** production render path, not its
  first stage. Recents continues past `classify_log_entry` through
  `_project_current_library_have` and `_project_linked_import_evidence`,
  which overwrite watched text fields on thousands of live rows. A target
  that stops early reports zeros measured against values production never
  shows. If you add a render target, call every production stage; do not
  reimplement one.

#885's differential is the shape to copy: 36,303 rows, 173 changed, every
one in `verdict` + `summary`, with `badge` / `badge_class` /
`border_color` / `downloaded_label` byte-identical. Half that evidence is
in the four zeros — which is exactly why a zero has to be earned.

**What this catches:** presentation keyed on a scenario no producer emits
reports 0 changed rows — a fluent sentence nothing can reach, which is exactly
the `no_candidates` defect #885 found. Presentation whose blast radius was
mis-reasoned reports a row count that contradicts the PR description. A
one-line fallback change made alongside it moves rows the PR never mentioned —
#885's `_humanize_token` unification moved 123 rows on top of the 50 under
discussion, and the differential is what made that visible rather than a
surprise in production.

**Cost, honestly:** the corpus export takes minutes and is reusable across
rounds; each render is one command; the diff is instant. Writing the
harness was the expensive part and it is already written.

**Primitive fields that JavaScript turns into semantics are also operator-facing
output.** Rule D applies when a boolean, enum, number, or nullable primitive is
consumed by JavaScript to choose visible text, colour, icon, visibility, or an
accusation. A Python render-differential zero for such a field does **not**
cover the JavaScript mapping: the renderer can report its primitive unchanged
while the browser changes what it says or shows.

For each affected surface, the PR body must therefore include a live-corpus
tally of the resulting visible states (or old-to-new visible states when they
change), not merely the primitive values. It must also include live-db
screenshots of changed cases and must-still-work controls. Use the existing
recipe in `docs/solutions/ui-dev-server-screenshot-loop.md`; it is the visual
evidence leg of Rule D, not a second differential runbook.

## Executable coverage and its boundary

Rule A coverage is enforced by `tests/test_pipeline_db_write_audit.py` plus
real-PostgreSQL round trips; typed write payloads also have table-column
contracts in `tests/test_pipeline_db_column_contract.py`. Rule B's narrow
adapter-fake pattern is guarded by `tests/test_mirror_contracts.py` and
`tests/test_lambda_audit.py`. Rule C's producer audits name an unproducible
literal — `tests/test_copy_marker_producer_audit.py` covers Rule C's inverse
direction: within its two bounds (the `*_COPY`/`*_QUALIFIER` name grammar,
and an explicit registered list of participating test modules), it fails
closed on a copy-pin marker constant that is not a substring of any string
literal a registered production file (Python or JS) actually spells. Its
evidence is FILE-level, not sentence-level, so it does NOT catch a marker
matching a different, already-correct sentence in the same producer file
while the specific sentence it was written to police is wrong (issue #1111
item 2's own founding incident, #1086 — tightening to site-level attribution
would require inferring intent from control flow, the prohibited
semantic-scanner shape). Rule D remains a PR-time live-corpus procedure
because no test can decide which derived operator-facing presentation or
output matters.

These gates enforce their declared shapes, not every semantic equivalent. The
rules above retain the judgement the executable checks cannot supply.
