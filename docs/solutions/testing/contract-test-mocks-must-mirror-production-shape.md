---
title: "Contract test mocks must mirror production row shape — synthetic int/str dicts hide serialization bugs"
date: 2026-05-09
category: testing
problem_type: test-pyramid-gap
component: web
tags:
  - testing
  - mocks
  - contract-tests
  - serialization
  - datetime
  - jsonb
  - integration-boundaries
related_plans:
  - docs/plans/2026-05-09-002-feat-search-plan-per-request-dashboard-plan.md
related_solutions:
  - docs/solutions/testing/mocked-contract-tests-miss-helper-mirror-integration-bugs.md
---

# Contract test mocks must mirror production row shape — synthetic int/str dicts hide serialization bugs

## Context

PR `feat/search-plan-dashboard` added `GET /api/pipeline/<id>/search-plan/history`. Eight contract test cases in `TestPipelineSearchPlanHistoryContract` verified the route's status codes, body shape, and field presence. All passed. Pyright clean. ce-code-review's correctness reviewer caught the bug:

> "GET /search-plan/history route 500s on real DB rows: datetime not JSON-serializable. The current mocked-rows-only tests cannot catch this regression."

The route returned raw `search_log` rows from the DB. Each row's `created_at` is a `datetime.datetime`. `web/server.py::_json` does `json.dumps(data).encode()` with no `default=...` callable — so `datetime` raises `TypeError` and the route 500s. **Every other GET in `web/routes/pipeline.py` maps rows through `_server()._serialize_row(r)` first** (see `get_pipeline_recents`, `get_pipeline_all`, `get_pipeline_downloading`); this new route did not.

The contract test fixture looked like:

```python
mock_db.get_search_history_page.return_value = SearchLogHistoryPage(
    rows=[{"id": 1, "request_id": 100, "outcome": "found",
           "created_at": "2026-05-09T10:23Z", ...}],
    next_before_id=None,
)
```

Every field a `str` or `int`. No `datetime`. No JSONB blob. No types from `psycopg2.extras.DictRow`. The mock was the test author's mental model of "a search_log row" — not what `PipelineDB.get_search_history_page` actually returns.

Pyright passed because `Dict[str, Any]` is type-compatible with `Dict[str, datetime|int|str|None]`. The contract test passed because the mock matched the assertion shape. **Production would have 500'd on the first real call.**

This is a recurrence of the anti-pattern documented in `docs/solutions/testing/mocked-contract-tests-miss-helper-mirror-integration-bugs.md`, but with a sharper failure mode: that doc warned about helper-to-service drift; this one names the helper-to-serializer drift directly. We have shipped this class of bug more than once. The rule below makes it impossible to ship again without explicit override.

## Guidance

**Mock data in contract tests must mirror production row shape — including `datetime`, JSONB, msgspec-validated structs, and any other non-primitive type. Synthetic int/str dicts are not acceptable.**

For any contract test that mocks a DB-row producer (a `PipelineDB` method, a `BeetsDB` method, anything reading from `psycopg2.extras.DictRow`):

- Timestamps must be `datetime.datetime` objects (not strings)
- UUIDs must be `uuid.UUID` objects (not strings)
- JSONB columns must be the typed dataclass / `msgspec.Struct` shape (not synthetic dicts)
- Any field that crosses through `_serialize_row`, `msgspec.to_builtins`, or any custom JSON encoder must be tested with a value that exercises the serialization path
- The mock row must be one a real `psycopg2` query for that table could plausibly return

**Alternative when row-shape mocking is genuinely impractical:** pair the contract test with an integration slice in `tests/test_integration_slices.py` that round-trips through real serialization (against a fixture-backed DB or `apply_migrations()` on ephemeral PG). The slice replaces the production-shape requirement only when the slice exercises the full serialization path the mocked test would have skipped.

The minimum bar: at least one scenario per contract test class that returns DB rows must use production-shaped mock data, OR be explicitly justified as a primitives-only path with no DB row, datetime, UUID, or JSONB involvement.

## Why This Matters

Three reasons synthetic int/str mock data systematically misses bugs:

1. **The mock encodes the test author's mental model of "a row," not the row's actual type.** When the author writes `{"created_at": "2026-05-09T10:23Z"}`, they're thinking "a timestamp string." `psycopg2` returns a `datetime` object. The mock and the database agree on the *field name*; they disagree on the *populated type*. The serializer cares about the type, not the name.

2. **Pyright accepts `Dict[str, Any]` for both shapes.** The type system covers "values can be anything" but doesn't enforce "production values look like X." Pyright's job is type integrity at boundaries; behavioral correctness at the serialization boundary is the test author's responsibility.

3. **Serialization failure is silent until the first real call.** No exception in development, no warning at deploy, no degraded badge. The first user with a normal-shaped row hits a 500. Mocks that match the assertion shape but not the production shape are worse than no mock at all — they manufacture false confidence.

This is the same boundary-drift pattern as `int`-vs-`str` wire types (the `mbid_not_found` regression in PR #98 captured in `.claude/rules/code-quality.md` § "Wire-boundary types"). Names match; populated types diverge; nothing catches it until production.

## When To Apply

Apply this rule whenever a contract test mocks a function that touches the DB. Specifically:

- New route handlers that read DB rows and forward them to JSON
- Existing route handlers whose response is being extended with a new field that hasn't been serialization-tested
- Service-layer methods whose typed result includes datetime, UUID, JSONB, or msgspec-validated fields

Skip the production-shape requirement only when:

- The route handler does its own typed serialization through a known-good helper (e.g. `msgspec.to_builtins(struct)` on a fully-typed `Struct`) AND a separate test of that helper exists
- The mock is for a pure-primitive function (an int counter, a string lookup, a bool flag) with no DB or datetime involvement

## Example

The fix in this PR was twofold:

1. **Map `result.rows` through `_server()._serialize_row(r)` before assigning to the response payload.** This is the same pattern every other GET in `web/routes/pipeline.py` uses. See the neighbor routes for the canonical shape.
2. **Add a contract test scenario** `test_history_datetime_rows_are_serialized_to_strings` that injects a `datetime.datetime` into the mock row and asserts the response's `created_at` is a string AND the full payload round-trips through `json.loads(json.dumps(...))` cleanly.

The first fix makes the production code correct. The second makes the regression impossible — any future change to the route that drops the `_serialize_row` call will fail the test before it ships. Without the second fix, a refactor of the route is one careless deletion away from re-introducing the bug.

## Action

For future contract tests, apply this rule from the start:

> When mocking a DB-row producer, populate at least one mock row with production-shaped values: `datetime.datetime` for timestamps, `uuid.UUID` for IDs, the typed dataclass / `msgspec.Struct` for JSONB. If the route's serialization path is via a known-good helper (e.g. `_serialize_row`, `msgspec.to_builtins`), the mock can use simpler shapes — but only if a separate test of that helper exists.

Codified in `.claude/rules/code-quality.md` § "API Contract Tests" as a hard requirement.
