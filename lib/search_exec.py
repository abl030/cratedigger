"""Unified slskd search lifecycle: submit → poll-state → settle → harvest → delete.

Issue #466 consolidated three drifted copies of this lifecycle
(``cratedigger.search_for_album``, ``cratedigger._collect_search_results``,
``lib.unfindable_detection_service.run_artist_probe``, plus the
``scripts/bench_parallel_search.py`` copy) into ``execute_search``. The three
copies had already diverged on correctness-relevant behaviour: only the
parallel-collect copy had the #212 progress watchdog, and only the two
cratedigger copies had the #242 response-settle. The unfindable probe — which
runs unattended on a daily systemd timer — had NEITHER, so a wedged slskd
search would hang the service indefinitely and an immediate harvest could
silently drop the response list it uses to decide "is this artist absent".

``execute_search`` owns the whole lifecycle exactly once; the four call sites
are thin adapters that build their own caller-specific result objects
(``lib.search.SearchResult`` for the pipeline, ``ArtistProbeResult`` for the
probe, a bench dataclass for the benchmark) from the returned
``SearchExecutionResult``.

Watchdog + settle constants are hardcoded by design (R12 — they are internal
failure-mode tuning, not operator knobs) and live here with the lifecycle they
govern.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable

logger = logging.getLogger("cratedigger.search_exec")


# === Per-search progress watchdog + response-settle constants ===
# Hardcoded by design (R12) — not exposed via config.ini or the NixOS module.
# If empirical data argues for a different value, that is a code-level edit +
# deploy, not a runtime tunable.
#
# SEARCH_WATCHDOG_DEADLINE_S — a search whose responseCount has not advanced
#   for this many seconds (and is still InProgress / Queued) trips the
#   watchdog. 90s catches the 8h-hang failure mode (issue #212) while leaving
#   slow-but-receiving searches alone.
# SEARCH_CANCEL_WAIT_DEADLINE_S — after stop(), wait at most this long for
#   slskd's async response-persistence cleanup to complete. Reading responses
#   before slskd flushes the response list silently degrades the harvest.
# SEARCH_CANCEL_WAIT_POLL_S — inner poll cadence during the post-cancel wait.
# SEARCH_RESPONSE_SETTLE_DEADLINE_S — after slskd reports a terminal state
#   (Completed, FileLimitReached / ResponseLimitReached / TimedOut), wait at
#   most this long for slskd's async response-store commit to stabilise before
#   reading. Issue #242: the response writer and the state writer are separate
#   threads on slskd's side, so an immediate ``search_responses`` after
#   ``"Completed" in state`` can return [] while the writer is still flushing.
#   2.0s is shorter than the 5.0s post-cancel deadline because natural
#   completion is the happy path — responses are usually already settled and
#   the helper exits after one confirmatory call. The cancel path is the worst
#   case (slskd just got interrupted) so it earns more headroom.
# SEARCH_RESPONSE_SETTLE_POLL_S — inner poll cadence during settle.
SEARCH_WATCHDOG_DEADLINE_S = 90.0
SEARCH_CANCEL_WAIT_DEADLINE_S = 5.0
SEARCH_CANCEL_WAIT_POLL_S = 0.2
SEARCH_RESPONSE_SETTLE_DEADLINE_S = 2.0
SEARCH_RESPONSE_SETTLE_POLL_S = 0.2


class SearchSubmitError(Exception):
    """Raised when ``searches.search_text`` fails before slskd accepts a search.

    Distinct from a poll/harvest failure so callers can classify a
    pre-accept failure as *non-consuming* (the search slot was never taken)
    while a post-accept collection failure is consuming. ``execute_search``
    only ever raises this from the submit phase; poll/harvest transport
    errors propagate as their original exception type. The underlying slskd
    exception is preserved as ``__cause__`` via ``raise ... from``.
    """


@dataclass
class SearchExecutionResult:
    """Outcome of one full ``execute_search`` lifecycle.

    Plain ``@dataclass`` (not ``msgspec.Struct``): constructed entirely from
    our own typed Python code and never crossing a JSON/DB wire boundary. The
    ``responses`` list stays in whatever shape the slskd client returns today
    — retyping slskd search responses is explicitly out of scope (issue #468).

    Fields:
      * ``responses`` — the settled harvest (the caller filters/caches it).
      * ``final_state`` — slskd's terminal state string, or ``None`` if no
        state poll succeeded before the loop broke.
      * ``response_count_terminal`` — slskd's uncapped ``responseCount`` from
        the terminal state poll. Diverges from ``len(responses)`` when slskd
        truncated the harvested array at responseLimit/fileLimit. ``None``
        when no state poll succeeded.
      * ``watchdog_fired`` — True iff the #212 progress watchdog cancelled the
        search. Diagnostic only; harvest classification reflects the responses
        actually collected, not the watchdog.
      * ``state_poll_error`` — True iff a ``searches.state`` poll raised and the
        loop broke early to a best-effort harvest (so ``final_state`` /
        ``response_count_terminal`` may be stale or ``None``). Together with
        ``watchdog_fired`` this marks a *degraded* execution: the harvest is
        best-effort and any terminal-state-derived signal is untrustworthy.
        The unfindable probe uses it to refuse to record a low match count
        from a degraded poll (which would corrupt categorisation).
      * ``elapsed_s`` — wall time of the ``execute_search`` call.
    """

    responses: list[dict[str, Any]] = field(default_factory=list)
    final_state: str | None = None
    response_count_terminal: int | None = None
    watchdog_fired: bool = False
    state_poll_error: bool = False
    elapsed_s: float = 0.0


def _fetch_search_responses_settled(
    slskd_client: Any,
    search_id: Any,
    *,
    deadline_s: float,
    poll_s: float,
    clock_fn: Callable[[], float] | None = None,
    sleep_fn: Callable[[float], None] | None = None,
) -> list[dict[str, Any]]:
    """Fetch slskd ``search_responses`` after waiting for the response store
    to commit. Mitigates the issue #242 race between slskd's terminal-state
    update and its response-store flush.

    Polls ``search_responses`` until two consecutive calls return the same
    length, or ``deadline_s`` elapses. Returns the most-recently-fetched
    list. In the happy path (responses already settled), this costs exactly
    one extra HTTP call vs. a naive single-shot fetch — a small price for
    eliminating the empirically-observed 56% zero-rate on
    ``Completed, FileLimitReached``.

    The two-consecutive-same-length condition is the natural stability
    signal: slskd's writer flushes responses incrementally, and consecutive
    same-length reads mean the writer is no longer making progress.

    On deadline expiry, returns the last list seen rather than raising — the
    caller already wraps the harvest in try/except for transport errors; a
    short list is better than a crash.

    ``clock_fn`` / ``sleep_fn`` are injected for test determinism; production
    callers omit them (default to ``time.monotonic`` / ``time.sleep``,
    resolved at call time so a module-level ``time.sleep`` patch still lands).
    """
    clock = clock_fn or time.monotonic
    sleep = sleep_fn or time.sleep
    deadline = clock() + deadline_s
    prev: list[dict[str, Any]] | None = None
    current = slskd_client.searches.search_responses(search_id)
    while clock() < deadline:
        if prev is not None and len(prev) == len(current):
            return current
        prev = current
        sleep(poll_s)
        current = slskd_client.searches.search_responses(search_id)
    return current


def execute_search(
    slskd_client: Any,
    *,
    search_id: Any = None,
    submit_kwargs: dict[str, Any] | None = None,
    delete: bool,
    clock_fn: Callable[[], float] | None = None,
    sleep_fn: Callable[[float], None] | None = None,
) -> SearchExecutionResult:
    """Run the full slskd search lifecycle for one search.

    Two entry modes, one lifecycle:

      * **Submit mode** (``search_id=None`` + ``submit_kwargs``): submits via
        ``searches.search_text(**submit_kwargs)`` and derives the id from the
        response, then polls/settles/harvests/deletes. Used by the serial
        pipeline, the unfindable probe, and the bench script.
      * **Pre-submitted mode** (``search_id`` given): skips submit and runs
        poll/settle/harvest/delete against the already-accepted id. Used by
        the parallel pipeline, whose submit phase is sequential under slskd's
        ``SemaphoreSlim(1,1)`` and therefore lives outside this call.

    Exception contract:
      * Submit failure raises :class:`SearchSubmitError` before any poll runs,
        so the caller can classify it as non-consuming.
      * A state-poll exception is absorbed by the watchdog loop (it breaks and
        proceeds to a best-effort harvest) — the loop must be resilient to run
        the #212 watchdog at all.
      * A harvest transport error propagates to the caller unchanged.
      * A failed ``delete`` is swallowed and logged — cleanup never discards a
        good harvest.

    The poll loop carries the #212 progress watchdog and the harvest carries
    the #242 settle. ``delete`` is honoured (best-effort) after the harvest.

    ``clock_fn`` / ``sleep_fn`` are injected for test determinism; production
    callers omit them.
    """
    clock = clock_fn or time.monotonic
    sleep = sleep_fn or time.sleep
    t0 = time.time()

    if search_id is None:
        if submit_kwargs is None:
            raise ValueError(
                "execute_search requires submit_kwargs when search_id is None"
            )
        try:
            submitted = slskd_client.searches.search_text(**submit_kwargs)
        except Exception as exc:
            raise SearchSubmitError(
                f"slskd search submission failed: {exc}"
            ) from exc
        search_id = submitted["id"]

    # Wait for slskd to process the search. Searches go through:
    #   Queued -> InProgress -> Completed, (TimedOut|ResponseLimitReached|Errored)
    # We wait while state is Queued OR InProgress. slskd's searchTimeout drives
    # the move to a terminal state; we do not impose our own wall-time poll cap
    # (it starves legitimately slow searches). The progress watchdog below is
    # the only cratedigger-side kill, and it measures *progress*, not
    # wall-time-from-submission (issue #212; the 8h53m hung-cycle case).
    final_state: str | None = None
    watchdog_fired = False
    state_poll_error = False
    prev_count = 0
    last_progress_at = clock()
    response_count_terminal: int | None = None
    while True:
        try:
            state_resp = slskd_client.searches.state(search_id, False)
            state = state_resp["state"]
            final_state = state
            count = state_resp.get("responseCount", 0)
            response_count_terminal = count
            if count > prev_count:
                prev_count = count
                last_progress_at = clock()
            # State-transition exit MUST be checked BEFORE the watchdog
            # deadline so a search that completes on the deadline poll exits
            # naturally and never calls stop().
            if "Completed" in state or (
                "InProgress" not in state and "Queued" not in state
            ):
                break
        except Exception:
            # A state-poll failure breaks to a best-effort harvest (the loop
            # must be resilient to run the #212 watchdog at all), but it
            # marks the execution *degraded* so callers that can't trust a
            # partial harvest — the unfindable probe — can refuse to record.
            logger.warning("Failed to poll search state for %s", search_id)
            state_poll_error = True
            break

        if clock() - last_progress_at >= SEARCH_WATCHDOG_DEADLINE_S:
            logger.info(
                "watchdog firing for search_id=%s after %ss of no progress",
                search_id, SEARCH_WATCHDOG_DEADLINE_S,
            )
            try:
                slskd_client.searches.stop(search_id)
            except Exception:
                logger.info(
                    "searches.stop(%s) failed; proceeding with harvest anyway",
                    search_id,
                )
            watchdog_fired = True
            break

        sleep(1)

    # Bridge slskd's state→responses race (issue #242). The cancel path needs
    # a longer budget because slskd just got interrupted; the natural path is
    # the happy case where responses are usually already settled and the
    # helper exits after one confirmatory call.
    settle_deadline = (
        SEARCH_CANCEL_WAIT_DEADLINE_S
        if watchdog_fired
        else SEARCH_RESPONSE_SETTLE_DEADLINE_S
    )
    settle_poll = (
        SEARCH_CANCEL_WAIT_POLL_S
        if watchdog_fired
        else SEARCH_RESPONSE_SETTLE_POLL_S
    )
    # The cleanup delete lives in a ``finally`` so it runs even when the
    # harvest raises a transport error. The pre-#466 probe deleted its search
    # in a ``finally`` for exactly this reason (a failed ``search_responses``
    # must not leak the search on slskd's side); the unified lifecycle keeps
    # that guarantee for every caller. The delete is itself best-effort: slskd
    # GCs an undeleted search on its own, so a failed DELETE must never discard
    # a successful harvest (the pre-#466 serial pipeline path had that latent
    # bug — delete lived inside the collection try/except and a failed delete
    # rolled a good harvest into ``collection_crash``) nor fail a good probe.
    elapsed = 0.0
    try:
        responses = _fetch_search_responses_settled(
            slskd_client, search_id,
            deadline_s=settle_deadline, poll_s=settle_poll,
            clock_fn=clock, sleep_fn=sleep,
        )
        elapsed = time.time() - t0
    finally:
        if delete:
            try:
                slskd_client.searches.delete(search_id)
            except Exception:
                logger.warning(
                    "searches.delete(%s) failed; slskd will GC it", search_id,
                )

    return SearchExecutionResult(
        responses=responses,
        final_state=final_state,
        response_count_terminal=response_count_terminal,
        watchdog_fired=watchdog_fired,
        state_poll_error=state_poll_error,
        elapsed_s=elapsed,
    )
