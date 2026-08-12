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

Issue #1112 did the same consolidation one layer down, for just the
submit phase: ``cratedigger.py::_submit_plan_search`` (the parallel
pipeline's own submit step — it never runs poll/harvest itself, so it
never called ``execute_search``) carried a second, bespoke 409+429
submit-retry loop. ``submit_search_with_retry`` now owns that submit
phase exactly once; ``execute_search``'s own submit branch and
``_submit_plan_search`` both call it, with different
``SearchSubmitRetryPolicy`` configurations (statuses retried, backoff,
whether a readiness probe floors the wait).

Watchdog + settle constants are hardcoded by design (R12 — they are internal
failure-mode tuning, not operator knobs) and live here with the lifecycle they
govern.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger("cratedigger.search_exec")

# Untyped external boundaries — issue #468 explicitly keeps retyping slskd's
# client and search-response shapes out of scope. Every function in this
# module that touches one of these three shapes shares ONE alias instead of
# repeating the ``Any`` escape hatch at every signature (issue #765's
# typing-ratchet counts every literal occurrence; #1112 split one function
# into two sharing the same untyped boundary, so this collects the token
# count behind one definition per shape rather than letting it grow with the
# number of call sites).
_SlskdClient = Any  # mirrors lib.slskd_client.SlskdClient / FakeSlskdAPI
_SlskdSearchId = Any  # whatever id shape slskd's own response carries
_SlskdJson = dict[str, Any]  # a raw slskd request/response JSON object


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

# Floor (not zero) for a submit-retry wait when ``server_ready`` reports
# ready (issue #1090 NON-BLOCKING-3). A "ready" reading is advisory and can
# race the actual reconnect; collapsing straight to a 0s wait makes a WRONG
# reading load-bearing (3 POSTs within milliseconds instead of the intended
# backoff). Flooring to this value degrades gracefully instead.
SUBMIT_RETRY_READY_FLOOR_S = 0.5


class SearchSubmitError(Exception):
    """Raised when ``searches.search_text`` fails before slskd accepts a search.

    Distinct from a poll/harvest failure so callers can classify a
    pre-accept failure as *non-consuming* (the search slot was never taken)
    while a post-accept collection failure is consuming. ``execute_search``
    only ever raises this from the submit phase; poll/harvest transport
    errors propagate as their original exception type. The underlying slskd
    exception is preserved as ``__cause__`` via ``raise ... from``.

    ``retry_exhausted`` (issue #1090 BLOCKING-1; generalised issue #1112) is
    a typed discriminator a caller uses to decide whether this failure
    represents a TRANSIENT slskd-connectivity condition worth counting
    toward a circuit breaker, as opposed to a DETERMINISTIC per-row
    rejection (a status outside the policy's own ``retryable_statuses``, a
    network error, or any submit with no retry policy at all) that will
    recur identically on every future run. True ONLY when a status in the
    configured ``SearchSubmitRetryPolicy.retryable_statuses`` (see
    ``_submit_is_retryable_status``) persisted through every attempt of
    that policy's budget — i.e. "exhausted the policy's own budget", not
    "exhausted a 409-only budget" specifically. A caller that never widens
    ``retryable_statuses`` past its ``{409}`` default (the unfindable
    probe) therefore keeps the original #1090 meaning unchanged: only a
    409 that survived the whole budget ever sets this True. Reconstructing
    this from ``str(exc)`` is exactly the fragile-string-matching this
    field replaces — callers must read the typed attribute, never the
    message.
    """

    def __init__(self, message: str, *, retry_exhausted: bool = False) -> None:
        super().__init__(message)
        self.retry_exhausted = retry_exhausted


@dataclass(frozen=True)
class SearchSubmitRetryPolicy:
    """Bounded retry for a transient slskd search-SUBMIT rejection (#1090).

    2026-08-12 root cause: a ~3s burst of 49/50 unfindable-probe submits
    got HTTP 409 when slskd's underlying Soulseek connection reset and
    reconnected — while slskd sits in ``Connected, LoggingIn``,
    Soulseek.NET's ``SearchAsync`` guard throws, and slskd's
    ``SearchesController`` maps that to 409. This is NOT slskd's rate
    limiter (``SearchRequestLimiter`` returns 429, never 409) — a
    genuinely different failure mode that a caller opts into retrying (see
    ``retryable_statuses``) rather than one this policy assumes by default.

    ``max_attempts`` counts the FIRST attempt: ``max_attempts=3`` allows up
    to 2 retries. ``backoff_s`` supplies the wait before each retry (index
    0 = wait before retry #1); the last entry repeats once attempts exceed
    the schedule length.

    ``retryable_statuses`` (issue #1112) is the set of HTTP status codes
    this policy retries; any other status (or any exception with no HTTP
    status at all, e.g. a network error) raises immediately on the FIRST
    attempt, consuming none of the retry budget. Defaults to ``{409}`` —
    the unfindable probe's original #1090 policy — so an unwidened caller
    is byte-identical to pre-#1112 behaviour. The main pipeline
    (``cratedigger.py::_submit_plan_search``, #1112) widens this to
    ``{409, 429}``: its pre-#1112 bespoke loop already retried slskd's real
    rate limiter (``SearchRequestLimiter``, which DOES return 429) alongside
    the same mid-reconnect 409 this class was written for.

    ``mint_ledgered_search_id`` is called before every RETRY (never the
    first attempt — the caller already ledgered the id in its
    ``submit_kwargs``) to mint a fresh search id and record it via
    ``db.record_search_id`` BEFORE the retried POST. This preserves the
    write-ahead invariant (issue #576, I2) per attempt, mirroring
    ``cratedigger.py::_submit_plan_search``'s own per-attempt ledger write
    (#1112 folded that bespoke loop into this same shared policy).

    ``server_ready`` is an ADVISORY pre-retry check (typically
    ``SlskdClient.server.state()``) that FLOORS (never zeroes) the wait once
    slskd reports it reconnected — a "ready" reading shortens the wait to a
    small fixed floor rather than skipping it outright, so a WRONG readiness
    reading (a race between the probe and the actual reconnect) degrades to
    "retried a bit sooner than the full backoff" instead of "retried
    immediately, 3 POSTs within milliseconds" (issue #1090 NON-BLOCKING-3).
    An absent, false, or raising probe falls back to the full fixed backoff.
    The bounded retry — not the probe — is the load-bearing mechanism; a
    SLOW probe still adds its own latency per retry, so production gives it
    a short dedicated timeout independent of the main HTTP client's (see
    ``lib.slskd_client.SlskdServerApi.state``). The main pipeline's policy
    leaves this unset (``None``) — its pre-#1112 loop had no readiness
    probe, and folding it in was not part of the #1112 consolidation.
    """

    mint_ledgered_search_id: Callable[[], str]
    max_attempts: int = 3
    # Paired with the default max_attempts=3 -- exactly 2 retries ever
    # happen (indices 0, 1), so the default schedule has exactly 2
    # entries. A caller raising max_attempts should also extend
    # backoff_s; the last entry repeats past its own length rather than
    # padding with an unreachable value here (issue #1090 F5).
    backoff_s: tuple[float, ...] = (2.0, 5.0)
    server_ready: Callable[[], bool] | None = None
    retryable_statuses: frozenset[int] = frozenset({409})


def _submit_is_retryable_status(
    exc: BaseException, retryable_statuses: frozenset[int],
) -> bool:
    """True when ``exc`` carries an HTTP status in ``retryable_statuses``.

    Deliberately narrow: checks the real ``requests.HTTPError.response
    .status_code`` shape via ``getattr`` (no ``requests`` import needed
    here) so a network error, or any status the caller's policy didn't
    opt into, is never silently retried.
    """
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    return status in retryable_statuses


# Human-readable cause noted alongside a bare status code in the submit-retry
# warning log (issue #1090's mid-reconnect root cause, #1112's generalised
# 429 rate-limit cause). An unrecognised status (only reachable if a future
# caller widens ``retryable_statuses`` further) logs the bare number.
_SUBMIT_RETRY_STATUS_NOTES: dict[int, str] = {
    409: "mid-reconnect",
    429: "rate limited",
}


def _describe_submit_retry_status(exc: BaseException) -> str:
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    note = _SUBMIT_RETRY_STATUS_NOTES.get(status) if status is not None else None
    return f"{status} ({note})" if note else str(status)


def submit_search_with_retry(
    slskd_client: _SlskdClient,
    submit_kwargs: _SlskdJson,
    *,
    submit_retry: SearchSubmitRetryPolicy | None,
    sleep_fn: Callable[[float], None] | None = None,
) -> _SlskdJson:
    """Submit one slskd search, honouring an optional bounded retry policy.

    Issue #1112 extracted this from ``execute_search``'s submit branch so
    ``cratedigger.py::_submit_plan_search`` (the main pipeline's own,
    previously-bespoke, 409+429 submit-retry loop) can share the exact
    same retry/backoff/ledger/logging mechanism as the unfindable probe's
    ``SearchSubmitRetryPolicy`` (issue #1090) instead of maintaining a
    second copy. ``execute_search`` itself now calls this helper for its
    submit phase; pre-submitted-mode callers never reach it.

    Returns slskd's raw submit response (``{"id": ..., ...}``) on success.

    Exception contract:
      * ``submit_retry=None`` submits exactly once and raises
        :class:`SearchSubmitError` on ANY exception — byte-identical to
        pre-#1090 behaviour.
      * A status outside ``submit_retry.retryable_statuses`` (or any
        exception with no HTTP status, e.g. a network error) raises
        :class:`SearchSubmitError` immediately, consuming none of the
        retry budget.
      * A retryable status exhausts the full ``max_attempts`` budget before
        raising :class:`SearchSubmitError` with ``retry_exhausted=True``
        (see that class's docstring for exactly when this is set).
      * ``submit_retry.mint_ledgered_search_id`` is called OUTSIDE the
        try/except that classifies slskd submit exceptions — a DB failure
        while minting a retry id propagates UNWRAPPED, never as
        ``SearchSubmitError`` (the write-ahead ledger's "DB-down is
        cycle-fatal, never silently swallowed" contract).
    """
    sleep = sleep_fn or time.sleep
    kwargs = dict(submit_kwargs)
    attempts = 1 if submit_retry is None else max(1, submit_retry.max_attempts)
    for attempt in range(attempts):
        try:
            return slskd_client.searches.search_text(**kwargs)
        except Exception as exc:
            is_last_attempt = attempt >= attempts - 1
            is_retryable = submit_retry is not None and _submit_is_retryable_status(
                exc, submit_retry.retryable_statuses,
            )
            if submit_retry is None or is_last_attempt or not is_retryable:
                # retry_exhausted (issue #1090 BLOCKING-1, generalised
                # #1112): True ONLY when a retryable status persisted
                # through the FULL attempt budget -- the one
                # TRANSIENT-connectivity shape a circuit breaker should
                # ever count. Every other raise here (a status outside the
                # policy's set, a network error, or any submit with no
                # policy at all) is a DETERMINISTIC per-row rejection that
                # will recur identically on every future run and must NOT
                # accumulate toward a breaker trip.
                raise SearchSubmitError(
                    f"slskd search submission failed: {exc}",
                    retry_exhausted=(
                        submit_retry is not None
                        and is_last_attempt
                        and is_retryable
                    ),
                ) from exc
            wait = submit_retry.backoff_s[
                min(attempt, len(submit_retry.backoff_s) - 1)
            ]
            ready = False
            if submit_retry.server_ready is not None:
                try:
                    ready = bool(submit_retry.server_ready())
                except Exception:  # noqa: BLE001 - advisory probe never blocks the retry
                    ready = False
            # A "ready" reading FLOORS the wait rather than zeroing it
            # (issue #1090 NON-BLOCKING-3) -- a race between this probe
            # and the actual reconnect must degrade to "sooner than the
            # full backoff", never to "no wait at all".
            effective_wait = (
                min(wait, SUBMIT_RETRY_READY_FLOOR_S) if ready else wait
            )
            logger.warning(
                "slskd search submit got %s; retrying attempt %d/%d for "
                "%r (server_ready=%s, wait=%.1fs)",
                _describe_submit_retry_status(exc), attempt + 2, attempts,
                kwargs.get("searchText", ""), ready, effective_wait,
            )
            if effective_wait > 0:
                sleep(effective_wait)
            kwargs = dict(kwargs)
            kwargs["id"] = submit_retry.mint_ledgered_search_id()
    raise AssertionError(
        "submit_search_with_retry loop exited without returning or raising"
    )


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

    responses: list[_SlskdJson] = field(
        default_factory=lambda: [],  # noqa: PIE807 - preserves contextual generic type
    )
    final_state: str | None = None
    response_count_terminal: int | None = None
    watchdog_fired: bool = False
    state_poll_error: bool = False
    elapsed_s: float = 0.0


def _fetch_search_responses_settled(
    slskd_client: _SlskdClient,
    search_id: _SlskdSearchId,
    *,
    deadline_s: float,
    poll_s: float,
    clock_fn: Callable[[], float] | None = None,
    sleep_fn: Callable[[float], None] | None = None,
) -> list[_SlskdJson]:
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
    prev: list[_SlskdJson] | None = None
    current = slskd_client.searches.search_responses(search_id)
    while clock() < deadline:
        if prev is not None and len(prev) == len(current):
            return current
        prev = current
        sleep(poll_s)
        current = slskd_client.searches.search_responses(search_id)
    return current


def execute_search(
    slskd_client: _SlskdClient,
    *,
    search_id: _SlskdSearchId = None,
    submit_kwargs: _SlskdJson | None = None,
    delete: bool,
    clock_fn: Callable[[], float] | None = None,
    sleep_fn: Callable[[float], None] | None = None,
    submit_retry: SearchSubmitRetryPolicy | None = None,
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
        so the caller can classify it as non-consuming — see
        :func:`submit_search_with_retry` (the actual submit implementation
        this delegates to) for the full submit-phase contract, including
        the write-ahead-ledger DB-failure-propagates carve-out.
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

    ``submit_retry`` (issue #1090) is an OPTIONAL bounded retry for a
    transient slskd search-submit 409 — see :class:`SearchSubmitRetryPolicy`.
    Defaults to ``None``, which submits exactly once (byte-identical
    pre-#1090 behaviour); only the unfindable probe opts in today. Ignored
    entirely in pre-submitted mode (``search_id`` given) — there is no
    submit phase to retry.
    """
    clock = clock_fn or time.monotonic
    sleep = sleep_fn or time.sleep
    t0 = time.time()

    if search_id is None:
        if submit_kwargs is None:
            raise ValueError(
                "execute_search requires submit_kwargs when search_id is None"
            )
        # Bounded submit retry (issue #1090; extracted to a shared helper
        # in #1112 so the main pipeline's own submit-retry loop could opt
        # into the exact same mechanism): ``submit_retry=None`` submits
        # exactly once and raises ``SearchSubmitError`` on any exception —
        # byte-identical to pre-#1090 behaviour. A status outside the
        # policy's ``retryable_statuses`` (first attempt or any retry) also
        # raises immediately without consuming the remaining attempt
        # budget.
        submitted = submit_search_with_retry(
            slskd_client, submit_kwargs,
            submit_retry=submit_retry, sleep_fn=sleep,
        )
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
        except Exception:  # noqa: BLE001 - boundary converts or isolates collaborator failures
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
            except Exception:  # noqa: BLE001 - boundary converts or isolates collaborator failures
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
            except Exception:  # noqa: BLE001 - boundary converts or isolates collaborator failures
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
