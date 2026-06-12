---
title: "Service first, glue follows — typed result, guardrail before IO, one cache, deferred wrappers"
date: 2026-05-19
category: architecture
problem_type: pattern
component: replace-picker / beets-distance
tags:
  - service-layer
  - cli-api-symmetry
  - msgspec
  - wire-boundary
  - guardrail
  - cache-protocol
  - typed-result
related_prs:
  - "#285 (service + API + CLI)"
  - "#286 (UI overlay)"
  - "#287 (Nix pythonEnv hotfix)"
related_rules:
  - .claude/rules/code-quality.md § "CLI ⇄ API Surface Symmetry"
  - .claude/rules/code-quality.md § "Wire-boundary types — use `msgspec.Struct`"
---

# Service first, glue follows

## What we did

The Replace picker needed a "show me the real beets distance between this
download and that MBID" signal. Several shapes were possible — distance
math in the route handler, distance math in the frontend (with a
fingerprints endpoint feeding it), beets-via-harness-subprocess, batch
endpoints that took an MBID list, etc.

What we shipped instead was a single service function:

```python
compute_beets_distance(
    download_log_id: int,
    mbid: str,
    *,
    pdb: PipelineDB,
    mb_get_release: Callable[[str], dict],
    cache: BeetsDistanceCache | None = None,
    resolve_failed_path: Callable | None = None,
) -> BeetsDistanceResult
```

Then the HTTP route and the CLI command became 30–40 lines of mapping
each, and the UI overlay became ~50 lines of `runWithConcurrency` over
N×M `(pressing × wrong-matches-folder)` calls.

Total downstream surface, once the service existed:

| Layer | Lines |
|-------|-------|
| `web/routes/pipeline.py::get_beets_distance` | ~30 |
| `scripts/pipeline_cli.py::cmd_beets_distance` | ~80 (with text + JSON output) |
| `web/js/replace_picker.js` distance overlay | ~50 |
| Tests for each wrapper | small contract tests, no logic |

The integration slice (`tests/test_beets_distance.py`) — beets imported,
real audio fixture tagged via `music_tag`, real `match.assign_items` +
`distance.distance` round-trip, real cache write/read — caught the
real-world things that frontend tests can't see.

## Why the order matters

The order ended up:

1. Pure pipeline decision (we don't have one yet for "what's the
   distance" — it's beets', but we treat it like a pure function).
2. Typed result (`BeetsDistanceResult: msgspec.Struct`) — wire boundary
   from day one. Encodes to JSON for the HTTP response, encodes to JSON
   for the CLI's `--json` output, the same Struct round-trips through
   tests.
3. **Guardrail before IO**: candidate MBID's release group must match
   the request's release group. This check happens *before* we
   `resolve_failed_path`, *before* we walk the folder, *before* we
   read any tags. Cross-RG calls are operator slips; failing fast
   keeps the API hard to misuse and makes the contract test for
   `wrong_release_group` trivial (just an MB lookup, no FS setup).
4. One cache protocol (`BeetsDistanceCache.get/set`) and an in-process
   `DictCache` for tests; production wires Redis through a tiny
   adapter that bypasses `web/cache.py`'s JSON wrapping because our
   fingerprints are msgspec bytes. The adapter is 20 lines next to the
   route handler — *not* an abstraction shared across the codebase.
5. Service first, **wrappers deferred** to a follow-up commit. The
   service shipped in PR #285a, the HTTP + CLI wrappers in #285b. The
   service's tests went green before any wrapper code existed.

Each step constrained the next. If we'd reversed any of them:

- Building the route first → distance math leaks into route module,
  `wrong_release_group` ends up half in the route and half in a helper.
- Building the UI first → fingerprint reading runs in the frontend, the
  cache key shape is bound to whatever the picker happens to know, and
  the CLI never gets the same capability.
- No typed result → we end up wrapping `dict.get()` calls everywhere,
  the same drift that PR #98 / issue #99 originally proved bites you
  for the same reasons.
- Cache as part of the service body, not a protocol → tests need a
  Redis fixture, integration slice can't pin determinism.

## The checklist

For the next "the API is the hard part" feature:

1. **Name the inputs and the result.** Write the type signature and
   the `msgspec.Struct` result *before* any implementation. If you
   can't enumerate the outcomes as a fixed string set, you don't
   understand the problem yet — that includes the "everything fine"
   outcome.

2. **List the guardrails.** Which inputs are nonsensical together?
   What semantic-violation outcomes does that produce? Make them
   short-circuit before any expensive work (FS, subprocess, network).
   Test each guardrail with a fixture-light test that proves no IO
   ran.

3. **Inject collaborators, don't import them.** `pdb`, `mb_get_release`,
   `resolve_failed_path`, `cache`. Production wires real
   implementations; tests inject fakes. The service body never imports
   `web.mb` or `lib.util` or Redis directly — those are caller
   concerns.

4. **One cache protocol with a `DictCache` for tests.** A 10-line
   `Protocol` is enough. Don't reuse the project's broader cache
   helper if it serialises differently — write an adapter at the
   caller, not at the service.

5. **Make the result a `msgspec.Struct`.** It's the wire boundary even
   if you only think you need it for the CLI today. The HTTP response
   uses the same struct. The frontend types its decoded payload
   against the same field names. Outcome strings are the API contract,
   not the prose.

6. **Service ships green before the wrappers exist.** The integration
   slice (real beets, real audio fixture, real cache round-trip) is
   the authority. Wrappers are exit-code / status-code mapping tests
   only — they don't re-test the service.

7. **CLI + HTTP wrappers in the same PR or the next one — never
   later.** Symmetry is a property of the operator surface. Drifting
   "we'll add the CLI later" turns into the CLI never existing. The
   wrappers are short; just write both.

8. **Frontend consumes the typed contract directly.** No
   `result.distance || 0` shimming — the JS asserts `outcome === 'ok'`
   first, then reads `distance` knowing it's a number. The frontend's
   pure helpers (`pickBestDistance`, `formatDistanceBadge`,
   `runWithConcurrency`) are testable in `node` without a DOM.

## Where this pattern fits in the project

- `lib/mbid_replace_service.py` — same shape, predates this doc.
- `lib/search_plan_service.py` — same shape.
- `lib/beets_distance.py` — this PR's example, with the guardrail-
  before-IO and the injectable cache being the new bits worth noting.

Anything operator-visible that crosses the CLI ⇄ API boundary should
look like this by default. If it doesn't, that's the smell.

## Caveats

- The eager `from beets import library` at module load time is
  non-obvious. Historically it guarded against the web test harness
  adding `lib/` to sys.path early in the test session, which shadowed
  the upstream `beets` package with `lib/beets.py` whenever something
  lazy-imported it later. Those inserts were removed with #445 item 3
  (every module now loads under exactly one canonical name —
  `tests/test_no_dual_load.py` and `TestSysPathAudit` enforce it), but
  the eager import stays: it pins the upstream package once at load
  time. The fix that actually mattered was adding `ps.beets` to the
  production pythonEnv in `nix/package.nix` so the eager import works
  in prod too — see PR #287.

- The cross-RG guardrail passes through when the request's release
  group is null (legacy rows). Documented as `_StubPDB`-driven test
  `test_wrong_release_group_passes_when_request_rg_is_null`. The
  guardrail is "refuse when both sides know their RG and disagree",
  not "require both sides to know their RG".

- `BeetsDistanceCache.set(key, value, ttl)` takes a TTL parameter even
  though the `DictCache` ignores it. Production needs Redis TTLs;
  tests don't. The protocol keeps the call sites identical and lets
  the implementation decide.
