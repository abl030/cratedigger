---
title: "Mocked contract tests miss helper-to-mirror integration bugs — run a Playwright smoke before declaring done"
date: 2026-05-01
category: testing
problem_type: test-pyramid-gap
component: web
tags:
  - testing
  - mocks
  - playwright
  - smoke-tests
  - integration-boundaries
  - musicbrainz-mirror
related_plans:
  - docs/plans/2026-05-01-002-feat-search-by-id-plan.md
related_prs:
  - "#203"
  - "#204"
---

# Mocked contract tests miss helper-to-mirror integration bugs — run a Playwright smoke before declaring done

## Context

Search-by-ID (#203) shipped with 13 contract tests on the new `/api/browse/resolve` endpoint and 36 unit tests on the `parsePastedId` parser. Pyright clean. 2719/2719 Python tests passed. Code review by two parallel persona agents found six findings, all addressed in a follow-up commit before merge.

The merged feature was broken on the MB happy path. The Playwright smoke caught it within five minutes of deploy: pasting a bare MB release MBID dropped into the artist view, but the parent release-group never auto-expanded and the leaf release never got the `.search-target` ring. Hotfix in #204 was a one-line change to `web/mb.py` adding `release-groups` to the upstream `inc` parameter.

The contract test that *should* have caught this looked like:

```python
def test_mb_release_resolved(self):
    with patch("web.server.mb_api") as mock_mb:
        mock_mb.get_release.return_value = {
            "id": self.MB_RELEASE_ID,
            "title": "Test Release",
            "artist_id": self.MB_ARTIST_ID,
            "artist_name": "Test Artist",
            "release_group_id": self.MB_RG_ID,  # ← assumed populated
        }
        ...
        self.assertEqual(data["expand_id"], self.MB_RG_ID)
```

The mock returned a fully-populated `release_group_id` because the *test author* knew that field was needed. The live `mb.get_release()` helper, on the other hand, returned `release_group_id: None` because its upstream MB query omitted `inc=release-groups`. The mock and the helper agreed on the *type signature* but disagreed on the *populated content*. The test passed because the mock was correct; the production code failed because the helper wasn't.

## Guidance

For any feature where a route handler depends on a helper that hits an external service (MB mirror, Discogs mirror, slskd, beets-the-subprocess), at least one of the following must be true before declaring the feature done:

1. **A live integration test exists** that exercises the helper-to-service path end-to-end against the real (or a fixture-backed) service.
2. **A Playwright / browser smoke test runs against a deployed instance** and exercises the user-facing flow that depends on the helper.

Mocked contract tests are necessary but not sufficient. They prove the route layer's logic given a stipulated helper response, not that the helper actually produces that response in production.

For Cratedigger specifically, the cheapest second line of defense is the Playwright agent against `music.ablz.au` after each deploy. Tokens spent on the smoke are cheaper than tokens spent diagnosing why a "fully tested" feature looks broken in the browser.

## Why This Matters

Three reasons mocked contract tests systematically miss helper-to-service bugs:

1. **The mock encodes the test author's mental model of the helper, not the helper's actual behavior.** When the author wrote the mock, they were already thinking about what the route needed downstream. A bug in the helper's *upstream* call (a missing `inc` parameter, a wrong header, a query-string typo) has no signal in the mock.

2. **The Pyright type-check passes either way.** `release_group_id: str | None` covers both "always populated" and "always None"; the type system doesn't enforce when each happens. Pyright's job is type integrity, not behavioral correctness across the boundary.

3. **The boundary is silent on success and silent on failure.** When `mb.get_release()` returns a release with `release_group_id: None`, no exception fires, no log line emits, no badge degrades. The downstream resolver's `data.get("release_group_id") or raw_id` fallback turns the bug into the *same shape* of valid response — just with the wrong values. The tests assert on shape, not on values.

This is the same class of bug as wire-boundary type drift (the lesson in `.claude/rules/code-quality.md` on `msgspec.Struct` boundaries — the `int`-vs-`str` Discogs `mbid_not_found` regression in PR #98). The shape is a contract; the *meaning* of the populated fields is not. Drift on meaning slips through unless something exercises the live path.

## When To Apply

Run a Playwright smoke (or equivalent live integration test) before declaring done when:

- A new route's behavior is conditional on fields populated by an existing helper (`mb.py`, `discogs.py`, `beets_db.py`).
- The helper's upstream query is being adjusted in the same PR (or was recently adjusted).
- The route's contract test mocks the helper rather than calling it.
- The user-visible behavior is "did anything happen?" rather than "did the right number come back?" — e.g. did the ring appear, did the modal open, did the cleanup run.

Skip the smoke when:

- The helper is pure (no I/O, no upstream service) and well-covered by unit tests.
- The change is internal-only (refactor, type narrowing) with no behavioral surface change.
- The change is in a route handler that doesn't read from a helper at all.

## Example

For #203, the resolver's contract test was correctly shaped — it asserted `expand_id` was the release-group MBID, distinct from the release MBID. The bug was upstream of the mock boundary, in `mb.get_release()`'s `inc` parameter. No mocked test would have caught it without explicitly asserting against a live MB response.

The Playwright agent's smoke prompt was 12 scenarios covering happy paths, edge cases, and error paths. Token cost for the smoke was ~89K agent tokens (one run, ~5 minutes wall time). Token cost of the would-have-been bug report from a user, the bisect, and the rollback would have been substantially higher — and the user would have lost trust in the deploy in the meantime.

The cheapest version of this defense is to write the Playwright prompt *with the plan*, not after the merge. The plan already enumerated the smoke scenarios in U4's and U5's `test scenarios` blocks (Plan U4: "Happy path (Playwright smoke)..."). They were correctly scoped during planning; we just didn't run them until after the merge. Running them as the *last* gate before merge — not the *first* gate after deploy — would have caught #204 pre-merge with no production exposure.

## Action

For future features that touch helper-to-mirror or helper-to-subprocess boundaries, add a step to the pre-merge checklist:

> If the plan's test scenarios include "(Playwright smoke)" or equivalent, run them against the deployed branch before the rebase-merge. If the work is on `feat/*`, this means: deploy to a staging or via temporary direct-push, smoke, then merge. If the work is on a hotfix small enough that staging round-trip isn't worth it, accept the post-merge smoke gate but be ready to revert on failure.

The smoke is cheap. The cost of skipping it is paid in production.
