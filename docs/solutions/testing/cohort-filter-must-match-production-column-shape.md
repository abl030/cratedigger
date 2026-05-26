---
title: "Cohort-filter tests must seed production column shape, not the cohort name"
date: 2026-05-26
category: testing
problem_type: pattern
component: triage-service
tags:
  - production-shape-mocks
  - cli-api-symmetry
  - filter-parser
  - column-vs-value
related_prs:
  - "#381 (PR4 — operator triage surface)"
related_rules:
  - .claude/rules/code-quality.md § "Contract test mocks must mirror production shape"
related_solutions:
  - testing/contract-test-mocks-must-mirror-production-shape.md
---

# Cohort-filter tests must seed production column shape

## What happened

PR #381 shipped `pipeline-cli triage list --filter=data_quality:reason=unresolved_4xx_client`
as the operator-facing handle for the #374 sticky-4xx cohort. The
implementation parsed `data_quality:reason=<value>` as
`WHERE fr.reason_code = %s`. Tests passed green. Production returned
zero rows.

The bug: **`unresolved_4xx_client` is a status enum value, not a
reason code.** `lib/field_resolver_service.py::_classify_lookup_exception`
emits `(status='unresolved_4xx_client', reason_code='http_400')` for
a 400 Bad Request. The `reason_code` column carries the concrete HTTP
code (`http_400`, `http_410`, `http_422`). The `status` column carries
the bucket the operator pages on.

Test fixtures in `tests/test_triage_service.py::_seed_three`,
`tests/test_pipeline_cli.py::_seed_cohort`, and the
`TestTriageRouteContracts` seed-by-MagicMock-forwarder pattern all
faked the wrong shape:

```python
# Wrong — what the tests had
db.record_field_resolution(
    request_id=4, field_name="catalog_number",
    status="unresolved",                   # too generic
    reason_code="unresolved_4xx_client",   # this is a status value!
)

# Right — production shape per lib/field_resolver_service.py:202-223
db.record_field_resolution(
    request_id=4, field_name="catalog_number",
    status="unresolved_4xx_client",        # the bucket
    reason_code="http_400",                # the concrete HTTP code
)
```

Caught by the multi-agent `ce-code-review` correctness reviewer
(Opus). The reviewer verified by reading
`lib/field_resolver_service.py::_classify_lookup_exception` directly
and cross-referencing with `tests/test_integration_slices.py`, which
already used the correct shape (`status='unresolved_404',
reason_code='http_404'`). Only the cohort-filter fixtures diverged.

## Why it slipped past green tests

1. **The filter parser, the SQL, and the fixtures all agreed with
   each other** — they were internally consistent but disagreed with
   production. A symmetric error: producer (fixture) and consumer
   (SQL `WHERE reason_code = %s`) both used `reason_code` to carry the
   bucket name, so the round-trip matched.
2. **The pre-PR4 cohort already had real data** with the correct
   shape. The PR's contract tests never queried that shape because
   they seeded synthetic rows in test isolation.
3. **`reason_code` was a free-form text column** — there's no CHECK
   constraint at the DB level enforcing the vocabulary. Anything
   typeable into a fixture passes the round-trip.
4. **The integration slice already had it right** but covered a
   different concern (cross-domain composition, not filter matching).
   The slice and the cohort tests evolved separately and the slice's
   correctness didn't propagate.

## The fix that landed

Two additive changes plus a fixture rewrite:

1. **New filter form**: `data_quality:status=<status>` (additive
   alongside the existing `data_quality:reason=<code>` and
   `data_quality:<field>`). The operator's primary handle for the
   #374 cohort is now `triage list --filter=data_quality:status=unresolved_4xx_client`.
2. **Fixture rewrite**: every cohort-filter test seeds the production
   shape (`status='unresolved_4xx_client', reason_code='http_400'`).
3. **`VALID_DATA_QUALITY_FIELD_NAMES` frozenset** in
   `lib/triage_service.py` imported from `lib/field_resolver_service.py`'s
   `FIELD_*` constants — prevents the parser from silently
   accepting unknown field names and pushes the vocabulary into one
   source of truth.

Confirmed live on 2026-05-26: `curl '/api/triage/list?filter=data_quality:status=unresolved_4xx_client'`
returns **75** stuck requests — exact match to #374's reported count.

## The rule

**When you write a cohort-filter fixture, the seed values must
match what production code actually writes — not what the operator
*calls* the bucket.** Operator vocabulary (the cohort's name) and
column-level vocabulary (the enum value) are different things. If
they happen to coincide, the test catches drift accidentally. If
they diverge — and they usually diverge, because operators name
things in workflow terms while writers name things in data-shape
terms — the test passes while production breaks.

Concrete heuristics:

1. **Before writing a cohort fixture, grep the writer.** If your
   filter is `WHERE column = 'X'`, find every place in `lib/` that
   inserts into that column. Read what value it writes. Use that
   value in the fixture. If the production writer writes 'A' but the
   operator calls the cohort 'X', the filter is in the wrong column —
   not the fixture.
2. **Cross-reference the integration slice** before writing a contract
   test. If a slice already round-trips the column, its shape is the
   contract. New tests inherit it; they don't re-invent.
3. **DB CHECK constraints would have caught this.** The status enum
   is enforced at the application layer (Literal in
   `lib/field_resolver_service.py`); `reason_code` is free-form.
   When a column carries a vocabulary, push the vocabulary into a
   CHECK constraint or an explicit ANY-of clause — not freeform TEXT.
   (This wasn't fixed in PR4; future work.)
4. **The operator-facing filter name is allowed to differ from the
   column name.** `data_quality:status=unresolved_4xx_client` is
   correct *and* `data_quality:reason=http_400` is correct — they
   target different columns. The names matter; the mismatch between
   filter-form and column is the bug to avoid.

## Related

- `docs/solutions/testing/contract-test-mocks-must-mirror-production-shape.md`
  — the broader rule (datetime, UUID, typed Structs). This case is
  the cohort-filter specialisation.
- `lib/field_resolver_service.py::_classify_lookup_exception` — the
  canonical writer of `status` and `reason_code`. Read it before
  writing any field-resolution test fixture.
- `tests/test_integration_slices.py` — uses production shape; lean
  on slices when wiring new cohort tests.
