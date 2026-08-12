"""Generated patrol for the widened submit-retry status semantics
(``lib.search_exec.SearchSubmitRetryPolicy.retryable_statuses``, issue
#1112).

Pre-#1112 ``SearchSubmitRetryPolicy`` retried exactly one hardcoded HTTP
status (409); ``tests/test_search_exec.py`` pinned that fixed shape
deterministically. #1112 generalised the field to an arbitrary
``retryable_statuses`` set so the main pipeline's own submit-retry loop
(``cratedigger.py::_submit_plan_search``, which retries 409+429) could
share the exact mechanism instead of maintaining a second copy. These
properties patrol the WORLD SPACE of that generalisation — arbitrary
retryable-status sets and arbitrary non-member statuses, not just the two
concrete pairs (``{409}`` for the probe, ``{409, 429}`` for the main
pipeline) the deterministic pins in ``tests/test_search_exec.py`` and
``tests/test_slskd_searches.py`` cover.

Three invariants, all driven through the REAL production entry point
(``lib.search_exec.submit_search_with_retry`` — the function both
``execute_search`` and ``cratedigger.py::_submit_plan_search`` now share):

* **P-A** — a status OUTSIDE the policy's ``retryable_statuses`` never
  consumes budget or retries: it raises ``SearchSubmitError`` with
  ``retry_exhausted=False`` after exactly ONE ``search_text`` call,
  regardless of ``max_attempts``, and mints no retry id.
* **P-B** — a status INSIDE ``retryable_statuses``, failing on every
  attempt, exhausts the FULL ``max_attempts`` budget: exactly
  ``max_attempts`` calls, exactly ``max_attempts - 1`` minted retry ids,
  and ``SearchSubmitError.retry_exhausted=True``.
* **P-C** — generalises the #1090-review "mixed final attempt" pin
  (409, 409, then a non-retried 429) to the whole generated domain: every
  attempt before the last is a retryable-status failure (so the loop
  keeps retrying), but the LAST attempt fails with a status OUTSIDE the
  set. ``retry_exhausted`` must be False even though every prior attempt
  was retryable and the budget was fully consumed — this is the world
  that kills a mutant dropping the ``is_retryable`` term from
  ``retry_exhausted``'s formula, for ANY policy, not just the hardcoded
  409/429 pair the pre-#1112 deterministic pin covered.

Fakes are leaf-seam only: ``FakeSlskdAPI`` (external slskd boundary) via
its documented ``search_text_error`` / ``search_text_error_by_query``
injection. The policy, the retry loop, and the exception classification
are all real production code.
"""

from __future__ import annotations

import unittest
from collections.abc import Callable

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.search_exec import (
    SearchSubmitError,
    SearchSubmitRetryPolicy,
    submit_search_with_retry,
)
from tests.fakes import FakeSlskdAPI
from tests.helpers import make_requests_http_error

# A small, fixed pool of real HTTP status codes a submit failure could
# plausibly carry. Large enough that Hypothesis explores varied
# retryable/non-retryable splits; small enough to keep the "at least one
# status left outside the drawn set" constraint cheap to satisfy.
_CANDIDATE_STATUSES: tuple[int, ...] = (
    400, 401, 403, 404, 408, 409, 425, 429, 500, 502, 503, 504,
)


@st.composite
def _status_split(draw) -> tuple[frozenset[int], int]:
    """Draw ``(retryable_statuses, non_retryable_status)`` -- a nonempty,
    non-full subset of the candidate pool, plus one status guaranteed to
    sit outside it."""
    retryable = draw(st.sets(
        st.sampled_from(_CANDIDATE_STATUSES),
        min_size=1, max_size=len(_CANDIDATE_STATUSES) - 1,
    ))
    remaining = [s for s in _CANDIDATE_STATUSES if s not in retryable]
    non_retryable = draw(st.sampled_from(remaining))
    return frozenset(retryable), non_retryable


def _counting_minter() -> tuple[Callable[[], str], list[str]]:
    minted: list[str] = []

    def _mint() -> str:
        new_id = f"retry-{len(minted)}"
        minted.append(new_id)
        return new_id

    return _mint, minted


class TestWidenedRetryableStatusSemanticsGenerated(unittest.TestCase):

    @given(split=_status_split(), max_attempts=st.integers(min_value=1, max_value=6))
    def test_status_outside_policy_set_never_consumes_budget(
        self, split: tuple[frozenset[int], int], max_attempts: int,
    ) -> None:
        retryable_statuses, injected_status = split
        api = FakeSlskdAPI()
        api.searches.search_text_error = make_requests_http_error(
            "not retried", status_code=injected_status)
        mint, minted = _counting_minter()
        policy = SearchSubmitRetryPolicy(
            mint_ledgered_search_id=mint,
            max_attempts=max_attempts,
            retryable_statuses=retryable_statuses,
        )
        with self.assertRaises(SearchSubmitError) as caught:
            submit_search_with_retry(
                api, {"id": "initial-id", "searchText": "q"},
                submit_retry=policy, sleep_fn=lambda _s: None,
            )
        self.assertFalse(caught.exception.retry_exhausted)
        self.assertEqual(len(api.searches.search_text_calls), 1)
        self.assertEqual(minted, [])

    @given(split=_status_split(), max_attempts=st.integers(min_value=1, max_value=6))
    def test_status_inside_policy_set_exhausts_full_budget(
        self, split: tuple[frozenset[int], int], max_attempts: int,
    ) -> None:
        retryable_statuses, _non_retryable = split
        injected_status = min(retryable_statuses)
        api = FakeSlskdAPI()
        api.searches.search_text_error = make_requests_http_error(
            "always retryable", status_code=injected_status)
        mint, minted = _counting_minter()
        policy = SearchSubmitRetryPolicy(
            mint_ledgered_search_id=mint,
            max_attempts=max_attempts,
            retryable_statuses=retryable_statuses,
        )
        with self.assertRaises(SearchSubmitError) as caught:
            submit_search_with_retry(
                api, {"id": "initial-id", "searchText": "q"},
                submit_retry=policy, sleep_fn=lambda _s: None,
            )
        self.assertTrue(caught.exception.retry_exhausted)
        self.assertEqual(len(api.searches.search_text_calls), max_attempts)
        self.assertEqual(len(minted), max_attempts - 1)

    @given(split=_status_split(), max_attempts=st.integers(min_value=2, max_value=6))
    def test_nonretryable_final_attempt_never_sets_retry_exhausted(
        self, split: tuple[frozenset[int], int], max_attempts: int,
    ) -> None:
        retryable_statuses, non_retryable_status = split
        leading_status = min(retryable_statuses)
        api = FakeSlskdAPI()
        queue: list[Exception | None] = []
        for _ in range(max_attempts - 1):
            queue.append(make_requests_http_error(
                "retryable", status_code=leading_status))
        queue.append(make_requests_http_error(
            "terminal", status_code=non_retryable_status))
        api.searches.search_text_error_by_query["q"] = queue
        mint, _minted = _counting_minter()
        policy = SearchSubmitRetryPolicy(
            mint_ledgered_search_id=mint,
            max_attempts=max_attempts,
            retryable_statuses=retryable_statuses,
        )
        with self.assertRaises(SearchSubmitError) as caught:
            submit_search_with_retry(
                api, {"id": "initial-id", "searchText": "q"},
                submit_retry=policy, sleep_fn=lambda _s: None,
            )
        self.assertFalse(caught.exception.retry_exhausted)
        self.assertEqual(len(api.searches.search_text_calls), max_attempts)


if __name__ == "__main__":
    unittest.main()
