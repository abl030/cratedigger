"""Unfindable detection service (U13 of search-plan iteration 2 — R18-R20).

Runs as a dedicated systemd oneshot (`cratedigger-unfindable.service`)
on a daily timer, **completely separate** from the 5-min
`cratedigger.service` loop. The structural separation is load-bearing:
R20 ("the system never stops searching") forbids the regular search
cadence from ever being throttled by detection state, so detection
lives in its own process where it cannot reach any code path that
mutates search-plan cursors.

Architecture
------------

Per cohort member (currently: every ``status='wanted'`` request whose
``last_artist_probe_at`` is NULL or older than `PROBE_INTERVAL_DAYS`):

1. Run an artist-only Soulseek probe (``slskd.searches.search_text(
   searchText=<artist>)``). Capture the responseCount as the
   "match_count" signal.
2. Write the probe observation: ``last_artist_probe_at = NOW()``,
   ``last_artist_probe_match_count = <count>``. The cursor / cycle
   / failure_class are never touched.
3. Read recent probe + search-log signal for that request, feed the
   pure ``classify_unfindable_from_state`` decision function.
4. If the verdict is one of the 4 categories, write it. If the
   verdict is ``None`` AND the request was previously categorised,
   clear the column (re-categorisation downgrade). If the verdict
   is ``None`` AND the request had no prior category, leave both
   columns alone.

The four categories (mirror migration 028's CHECK constraint):

- ``artist_absent`` — last K probes returned low match counts AND
  no peer's username/dir contains a fuzzy artist match.
- ``album_absent_artist_present`` — recent probes show the artist
  IS on the network (match_count above threshold), but the request's
  last M plan cycles produced zero ``found`` outcomes.
- ``one_track_structural`` — request has exactly one track.
  Set without any probe; the structural shape alone determines this
  category. The probe still runs (cheap signal that may downgrade
  the row later if the artist becomes scarce).
- ``wrong_pressing_available`` — recent search-log rows show
  ``rejection_reason='strict_count_mismatch'`` with a high
  ``matcher_score_top1`` — peers ARE serving the album, just not in
  the operator's exact pressing.

See:
- ``docs/brainstorms/2026-05-25-search-plan-iteration-2-requirements.md``
  (R18-R20)
- ``docs/plans/2026-05-25-001-feat-search-plan-iteration-2-plan.md``
  (U13)
"""

from __future__ import annotations

from collections.abc import Mapping

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Callable, Protocol

if TYPE_CHECKING:
    from lib.pipeline_db.rows import AlbumRequestRow


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tunable constants. Operators tune in code (single source of truth) — these
# are deliberately not in config.ini because the detection job is internal
# infrastructure rather than a routine knob.
# ---------------------------------------------------------------------------

# How often a single cohort member is probed. Each detection run picks the
# K oldest probes; a request will not be probed twice within this window.
PROBE_INTERVAL_DAYS: int = 7

# Match-count threshold for the artist-only probe. responseCount strictly
# less than this counts as a "low" probe. 5 is the conservative default —
# below five peers globally is overwhelmingly "the artist isn't on the
# network right now" rather than "a quiet day on the artist".
ARTIST_MATCH_THRESHOLD: int = 5

# How many consecutive low probes are required before the
# ``artist_absent`` verdict fires. Two weeks of low probes (default K=2 at
# PROBE_INTERVAL_DAYS=7) gives enough signal to be confident the artist
# isn't surfacing on the network; a single low day is too noisy.
REQUIRED_LOW_PROBES: int = 2

# How many recent plan cycles must show zero ``found`` outcomes before
# ``album_absent_artist_present`` fires. Plan cycles roll over when the
# whole strategy list wraps, so M=3 covers ~3 generator passes.
REQUIRED_ZERO_FIND_CYCLES: int = 3

# Wrong-pressing detection: at least this many search_log rows in the
# observation window must show the wrong-pressing signature
# (rejection_reason='strict_count_mismatch' with a high matcher score).
WRONG_PRESSING_MIN_HITS: int = 3

# Matcher score threshold for the wrong-pressing signature. The score is
# the avg_ratio of the top candidate (0..1). 0.85 mirrors the wrong-
# pressing pattern documented in the U13 plan ("matched_tracks >= 0.85 *
# expected_track_count AND avg_ratio >= 0.85").
WRONG_PRESSING_MATCHER_THRESHOLD: float = 0.85

# Recent search_log window for the wrong-pressing / zero-find signal.
# Bounded so an old wrong-pressing row that's since been cleared by a
# regenerate doesn't keep the verdict pinned forever.
SEARCH_LOG_WINDOW_DAYS: int = 30

# Default cohort batch size. The detection job processes the K oldest
# probe candidates per run; cohort members not picked up this run roll
# over to the next daily run.
DEFAULT_BATCH_SIZE: int = 100


# ---------------------------------------------------------------------------
# Category constants. Mirror migration 028's CHECK constraint.
# ---------------------------------------------------------------------------

CATEGORY_ARTIST_ABSENT = "artist_absent"
CATEGORY_ALBUM_ABSENT_ARTIST_PRESENT = "album_absent_artist_present"
CATEGORY_ONE_TRACK_STRUCTURAL = "one_track_structural"
CATEGORY_WRONG_PRESSING_AVAILABLE = "wrong_pressing_available"

ALL_CATEGORIES: tuple[str, ...] = (
    CATEGORY_ARTIST_ABSENT,
    CATEGORY_ALBUM_ABSENT_ARTIST_PRESENT,
    CATEGORY_ONE_TRACK_STRUCTURAL,
    CATEGORY_WRONG_PRESSING_AVAILABLE,
)


# ---------------------------------------------------------------------------
# Result outcomes — service-layer status strings.
# ---------------------------------------------------------------------------

RESULT_CATEGORISED = "categorised"
RESULT_DOWNGRADED = "downgraded"
RESULT_NO_CHANGE = "no_change"
RESULT_REQUEST_NOT_FOUND = "request_not_found"
RESULT_PROBE_FAILED = "probe_failed"
RESULT_NOT_DUE = "not_due"


# ---------------------------------------------------------------------------
# Internal types. Plain ``@dataclass(frozen=True)`` because these are
# pure-internal Python types constructed entirely from our own typed
# code — never crossing JSON, never re-encoded. Per
# `.claude/rules/code-quality.md` § "Wire-boundary types", Struct is
# reserved for types that actually traverse a wire boundary.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class UnfindableSearchLogSignal:
    """Aggregated search-log evidence read for one request.

    Pre-computed by the service's DB query so the pure classifier
    doesn't need to reason about row windowing or status filtering.
    """

    # Of the distinct ``plan_cycle_snapshot`` values observed in the
    # recent window, how many produced ZERO ``found`` outcomes for
    # this request. ``zero_find_cycles >= REQUIRED_ZERO_FIND_CYCLES``
    # is one of the inputs to the ``album_absent_artist_present`` rule.
    zero_find_cycles: int
    # Count of search_log rows in the window whose
    # ``rejection_reason='strict_count_mismatch'`` AND
    # ``matcher_score_top1 >= WRONG_PRESSING_MATCHER_THRESHOLD``. Drives
    # the ``wrong_pressing_available`` rule.
    wrong_pressing_hits: int


@dataclass(frozen=True)
class UnfindableInputs:
    """All decision-relevant state for one request, in one struct."""

    # Operator-visible context. Used for ``one_track_structural``.
    total_tracks: int | None
    # Recent probe history: most-recent first. Each entry is the
    # ``last_artist_probe_match_count`` recorded at that observation.
    # The classifier looks at ``REQUIRED_LOW_PROBES`` rows for the
    # artist_absent rule.
    probe_match_counts: tuple[int, ...]
    # True when the most recent probe's slskd response surfaced a peer
    # whose username (or directory) fuzzy-matches the artist name. When
    # False with low match counts, the artist is genuinely absent;
    # when True with low match counts, it's a quiet day for a known
    # artist and we don't escalate to ``artist_absent``.
    probe_observed_artist_match: bool
    # Aggregated search-log signal (see ``UnfindableSearchLogSignal``).
    search_log_signal: UnfindableSearchLogSignal


@dataclass(frozen=True)
class UnfindableCategorisation:
    """Pure classifier result: the category + the reason it fired.

    ``reason`` is operator-facing — a short human string that explains
    which evidence drove the verdict. The service logs it alongside the
    probe write so operators can spot drift between the persisted
    column and what the classifier thought it was doing.
    """

    category: str
    reason: str


@dataclass(frozen=True)
class UnfindableServiceResult:
    """Outcome of ``UnfindableDetectionService.categorise_request``.

    ``outcome`` is one of the ``RESULT_*`` constants.

    - ``RESULT_CATEGORISED`` — wrote a new category (or refreshed an
      existing one). ``new_category`` / ``previous_category`` populated.
    - ``RESULT_DOWNGRADED`` — cleared a previously-set category back to
      NULL because the new probe evidence no longer supports it.
      ``new_category=None``, ``previous_category`` populated.
    - ``RESULT_NO_CHANGE`` — no signal yet, nothing to write.
    - ``RESULT_REQUEST_NOT_FOUND`` — no such request id.
    - ``RESULT_PROBE_FAILED`` — slskd raised; left state unchanged. The
      detection job continues with the next cohort member.
    - ``RESULT_NOT_DUE`` — request's probe is still within
      ``PROBE_INTERVAL_DAYS``; skipped without firing slskd.
    """

    outcome: str
    request_id: int
    previous_category: str | None = None
    new_category: str | None = None
    probe_match_count: int | None = None
    reason: str | None = None
    error_message: str | None = None


# ---------------------------------------------------------------------------
# Pure classifier — testable in isolation, no IO.
# ---------------------------------------------------------------------------


def classify_unfindable_from_state(
    inputs: UnfindableInputs,
    *,
    artist_match_threshold: int = ARTIST_MATCH_THRESHOLD,
    required_low_probes: int = REQUIRED_LOW_PROBES,
    required_zero_find_cycles: int = REQUIRED_ZERO_FIND_CYCLES,
    wrong_pressing_min_hits: int = WRONG_PRESSING_MIN_HITS,
) -> UnfindableCategorisation | None:
    """Pure decision function — see module docstring for the rules.

    Decision order (intentional, earlier branches dominate):

    1. ``one_track_structural`` — ``total_tracks == 1``. The structural
       shape is the strongest signal we have; probe results are
       irrelevant.
    2. ``wrong_pressing_available`` — the wrong-pressing signature in
       the search-log is direct evidence that peers are serving the
       album, just not in this pressing. Outranks artist-absence
       because the network DOES have the album in some form.
    3. ``artist_absent`` — ``required_low_probes`` consecutive recent
       probes were below the match threshold AND no fuzzy artist match
       observed on the most recent probe. The two-signal AND prevents
       a quiet day on a known artist from being mis-categorised.
    4. ``album_absent_artist_present`` — probe(s) show the artist is on
       the network (most recent match_count >= threshold OR a fuzzy
       artist match was observed) AND the last
       ``required_zero_find_cycles`` plan cycles produced zero
       ``found`` outcomes.
    5. Otherwise → ``None`` (no signal yet / downgrade candidate).

    Returns ``None`` when no rule matches. The service interprets
    ``None`` against the row's previous ``unfindable_category`` to
    decide whether to clear the column (downgrade) or leave it alone.
    The classifier itself is stateless — the prior category does NOT
    feed any rule, so it's not on the inputs struct.
    """
    # Branch 1: structural shape dominates.
    if inputs.total_tracks is not None and inputs.total_tracks <= 1:
        return UnfindableCategorisation(
            category=CATEGORY_ONE_TRACK_STRUCTURAL,
            reason=f"total_tracks={inputs.total_tracks}",
        )

    sig = inputs.search_log_signal

    # Branch 2: wrong-pressing signature.
    if sig.wrong_pressing_hits >= wrong_pressing_min_hits:
        return UnfindableCategorisation(
            category=CATEGORY_WRONG_PRESSING_AVAILABLE,
            reason=(
                f"wrong_pressing_hits={sig.wrong_pressing_hits} "
                f">= {wrong_pressing_min_hits} "
                f"(strict_count_mismatch with matcher_score >= "
                f"{WRONG_PRESSING_MATCHER_THRESHOLD})"
            ),
        )

    # Branch 3: artist_absent. Requires K consecutive low probes AND no
    # fuzzy artist match on the most recent probe. The ``<`` boundary
    # is intentional: a count of exactly ``artist_match_threshold``
    # does NOT count as low, so the threshold is the inclusive "this
    # many peers means the artist is on the network".
    if len(inputs.probe_match_counts) >= required_low_probes:
        recent = inputs.probe_match_counts[:required_low_probes]
        all_low = all(c < artist_match_threshold for c in recent)
        if all_low and not inputs.probe_observed_artist_match:
            return UnfindableCategorisation(
                category=CATEGORY_ARTIST_ABSENT,
                reason=(
                    f"last {required_low_probes} probes all had "
                    f"match_count < {artist_match_threshold} "
                    f"({list(recent)}) and no fuzzy artist match"
                ),
            )

    # Branch 4: album_absent_artist_present. Requires evidence the
    # artist IS on the network AND ``required_zero_find_cycles``
    # consecutive plan cycles with zero ``found``.
    artist_present = (
        (len(inputs.probe_match_counts) > 0
         and inputs.probe_match_counts[0] >= artist_match_threshold)
        or inputs.probe_observed_artist_match
    )
    if (artist_present
            and sig.zero_find_cycles >= required_zero_find_cycles):
        return UnfindableCategorisation(
            category=CATEGORY_ALBUM_ABSENT_ARTIST_PRESENT,
            reason=(
                f"artist_present=True; zero_find_cycles="
                f"{sig.zero_find_cycles} >= {required_zero_find_cycles}"
            ),
        )

    return None


# ---------------------------------------------------------------------------
# Fuzzy artist name matcher. Keep narrow — production fuzz matching is
# in ``lib.artist_compare``; we don't need that surface here. A
# whitespace + punctuation strip + case-fold + substring is enough to
# answer "did any peer surface mention this artist". False positives
# downgrade the verdict to "leave alone", which is the safer side.
# ---------------------------------------------------------------------------


_FUZZ_STRIP = re.compile(r"[^a-z0-9]+")


def _fuzz(name: str) -> str:
    return _FUZZ_STRIP.sub("", name.lower())


def fuzzy_artist_observed_in_probe(
    artist_name: str,
    slskd_responses: list[dict[str, Any]],
) -> bool:
    """True when any peer's username or directory contains the artist.

    The slskd artist-only probe returns a list of ``response`` dicts;
    each carries a ``username`` and a ``files`` list of dicts with
    a ``filename`` key. A peer surfacing the artist's name in either
    their username or any filename is enough to call this a positive
    fuzzy match.

    Heuristic only. The classifier uses this as the ``AND`` clause
    that prevents a quiet day on a known artist from triggering
    ``artist_absent``. False positives keep the request in the safer
    "no verdict" state.
    """
    if not artist_name:
        return False
    needle = _fuzz(artist_name)
    if not needle:
        return False
    for resp in slskd_responses:
        username = str(resp.get("username") or "")
        if needle in _fuzz(username):
            return True
        for f in resp.get("files") or []:
            fname = str(f.get("filename") or "")
            if needle in _fuzz(fname):
                return True
    return False


# ---------------------------------------------------------------------------
# Probe runner — narrow seam, swappable for tests.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ArtistProbeResult:
    """Outcome of one slskd artist-only probe."""

    match_count: int
    artist_observed: bool


class ProbeDegradedError(Exception):
    """The probe search did not complete cleanly (watchdog cancel or a
    state-poll error), so its ``responseCount`` / harvest cannot be trusted.

    Raised by :func:`run_artist_probe` so the service treats a degraded probe
    exactly like a slskd failure — ``RESULT_PROBE_FAILED``, nothing recorded.
    This restores the pre-#466 contract: a hung or errored probe never writes
    a (likely low) ``last_artist_probe_match_count``, which would otherwise
    accumulate toward a spurious ``artist_absent`` categorisation. Recording a
    fabricated absence signal from a broken probe is a categorisation-
    corrupting bug for a service whose whole job is deciding "is this absent".
    """


def run_artist_probe(
    slskd_client: Any,
    *,
    artist_name: str,
    db: "_PipelineDBProto",
    request_id: int | None = None,
    search_timeout_ms: int = 30000,
    response_limit: int = 100,
    file_limit: int = 1000,
    poll_sleep: Callable[[float], None] | None = None,
    clock: Callable[[], float] | None = None,
    delete_after: bool = True,
) -> ArtistProbeResult:
    """Run one artist-only probe via slskd and return the signal.

    Narrow surface: just ``responseCount`` (peer-count) + the fuzzy
    artist observation. No browse, no candidate scoring, no download.
    Bypasses the entire matcher / plan / cursor surface — the search
    is a fire-and-forget telemetry probe.

    Thin adapter over the unified slskd lifecycle
    (``lib.search_exec.execute_search``, issue #466). Consolidation gained
    this probe the #212 progress watchdog (a wedged probe search no longer
    hangs the daily ``cratedigger-unfindable.service`` indefinitely — it is
    cancelled after 90s of no progress) and the #242 response-settle (the
    response list used for the fuzzy artist observation is now the settled
    harvest rather than an immediate read that could race to empty and
    spuriously report the artist absent).

    Write-ahead ledger (issue #576, I2): mints a fresh search id and
    records it via ``db.record_search_id`` BEFORE the submit — the daily
    unattended timer is exactly the kind of process a kill can hit
    mid-search, and a probe search that leaks is otherwise invisible
    (there's no per-request row watching it).

    Exception contract:
      * A submit failure or a harvest transport error propagates; the caller
        (the service) records ``RESULT_PROBE_FAILED``.
      * A *degraded* execution — the #212 watchdog cancelled the search, or a
        ``searches.state`` poll raised and the loop fell back to a best-effort
        harvest — raises :class:`ProbeDegradedError`. This restores the
        pre-#466 contract: a hung/errored probe records NOTHING rather than a
        fabricated low ``responseCount`` that would accumulate toward a
        spurious ``artist_absent``. (Conservative by design: even a
        watchdog-cancelled search that happened to harvest a high count is
        treated as failed — categorisation fidelity over an extra data point.)
      * A failed cleanup ``delete`` is swallowed and never fails the probe.

    ``poll_sleep`` / ``clock`` are injected for test determinism (forwarded to
    ``execute_search`` as ``sleep_fn`` / ``clock_fn``); production omits them.
    """
    import uuid

    from lib.search_exec import execute_search

    search_id = str(uuid.uuid4())
    db.record_search_id(search_id, purpose="artist_probe", request_id=request_id)

    exec_result = execute_search(
        slskd_client,
        submit_kwargs={
            "id": search_id,
            "searchText": artist_name,
            "searchTimeout": search_timeout_ms,
            "filterResponses": True,
            "responseLimit": response_limit,
            "fileLimit": file_limit,
        },
        delete=delete_after,
        sleep_fn=poll_sleep,
        clock_fn=clock,
    )
    if exec_result.watchdog_fired or exec_result.state_poll_error:
        raise ProbeDegradedError(
            f"probe for {artist_name!r} degraded "
            f"(watchdog_fired={exec_result.watchdog_fired}, "
            f"state_poll_error={exec_result.state_poll_error}); "
            f"refusing to record an untrustworthy match count"
        )
    return ArtistProbeResult(
        match_count=int(exec_result.response_count_terminal or 0),
        artist_observed=fuzzy_artist_observed_in_probe(
            artist_name, exec_result.responses,
        ),
    )


# ---------------------------------------------------------------------------
# DB Protocol — narrow surface so the service is mocked-fake friendly
# and the AST guard test can enumerate exactly what we touch.
# ---------------------------------------------------------------------------


class _PipelineDBProto(Protocol):
    """Subset of PipelineDB methods the service touches.

    Deliberately narrow: every method here is one the AST guard would
    reject if it were a cursor-mutator. Methods like
    ``record_consumed_search_attempt`` / ``advance_search_plan_cursor``
    are NOT on this protocol so a typo can't accidentally pull them in
    through ``self.db.<x>``.
    """

    def get_request(self, request_id: int) -> "AlbumRequestRow | None": ...
    def get_tracks(self, request_id: int) -> list[dict[str, Any]]: ...
    def list_unfindable_probe_candidates(
        self, *, limit: int, probe_interval_days: int,
    ) -> list[dict[str, Any]]: ...
    def record_search_id(
        self,
        search_id: str,
        purpose: str,
        request_id: int | None,
    ) -> None: ...
    def record_artist_probe(
        self,
        request_id: int,
        *,
        match_count: int,
        observed_at: datetime,
    ) -> None: ...
    def set_unfindable_category(
        self,
        request_id: int,
        *,
        category: str | None,
        categorised_at: datetime,
    ) -> None: ...
    def get_unfindable_search_log_signal(
        self,
        request_id: int,
        *,
        window_days: int,
        matcher_score_threshold: float,
    ) -> UnfindableSearchLogSignal: ...


# ---------------------------------------------------------------------------
# Service.
# ---------------------------------------------------------------------------


class UnfindableDetectionService:
    """Orchestrates probe → classify → write for the unfindable cohort.

    Constructed with a ``PipelineDB`` and a slskd client. The slskd
    client is injected (not constructed inline) so tests can drive the
    full categorisation with ``FakeSlskdAPI`` and the production run
    script wires the real one.

    The service is stateless beyond its ``db`` / ``slskd_client``
    references; it does not cache anything between calls.
    """

    def __init__(
        self,
        db: _PipelineDBProto,
        slskd_client: Any,
        *,
        probe_runner: Callable[..., ArtistProbeResult] | None = None,
        now_fn: Callable[[], datetime] | None = None,
    ) -> None:
        self.db = db
        self.slskd_client = slskd_client
        # Kwarg-DI seam (.claude/rules/code-quality.md § Mocks). Tests
        # inject a synchronous fake; production uses ``run_artist_probe``.
        self._probe_runner = probe_runner or run_artist_probe
        self._now_fn = now_fn or (lambda: datetime.now(timezone.utc))

    # ---------- public surface ----------

    def categorise_request(
        self,
        request_id: int,
        *,
        force_probe: bool = False,
    ) -> UnfindableServiceResult:
        """Probe + classify + write for one request.

        ``force_probe=True`` bypasses the ``PROBE_INTERVAL_DAYS`` check
        (operator-driven override). The default flow respects the
        cadence so the daily systemd timer doesn't re-probe rows it
        just touched.

        Always safe to call against any request; if the request doesn't
        qualify (no row, status != wanted) the outcome reports that
        without firing slskd.
        """
        row = self.db.get_request(request_id)
        if row is None:
            return UnfindableServiceResult(
                outcome=RESULT_REQUEST_NOT_FOUND,
                request_id=request_id,
                error_message=f"request {request_id} not found",
            )

        previous = row.get("unfindable_category")
        now = self._now_fn()

        # Probe-due check. Respects the 7-day cadence so operators can
        # call this without needing to think about cooldown windows.
        last_probe = _as_datetime(row.get("last_artist_probe_at"))
        cutoff = now - timedelta(days=PROBE_INTERVAL_DAYS)
        if (not force_probe
                and last_probe is not None
                and last_probe > cutoff):
            return UnfindableServiceResult(
                outcome=RESULT_NOT_DUE,
                request_id=request_id,
                previous_category=previous,
            )

        # Probe. Failure mode: log, return RESULT_PROBE_FAILED, leave
        # row state untouched. The daily run continues with the next
        # cohort member.
        artist_name = str(row.get("artist_name") or "")
        try:
            probe = self._probe_runner(
                self.slskd_client, artist_name=artist_name,
                db=self.db, request_id=request_id,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "unfindable_detection: probe failed for request %s "
                "(artist=%r)", request_id, artist_name,
            )
            return UnfindableServiceResult(
                outcome=RESULT_PROBE_FAILED,
                request_id=request_id,
                previous_category=previous,
                error_message=f"{type(exc).__name__}: {exc}",
            )

        self.db.record_artist_probe(
            request_id,
            match_count=probe.match_count,
            observed_at=now,
        )

        # Pull aggregated search-log signal + track count for the
        # classifier. The probe match counts come from the row +
        # the just-recorded probe; we layer the new probe on the
        # front so the classifier sees the latest observation as
        # ``probe_match_counts[0]``.
        probe_history = self._build_probe_history(row, probe.match_count)
        signal = self.db.get_unfindable_search_log_signal(
            request_id,
            window_days=SEARCH_LOG_WINDOW_DAYS,
            matcher_score_threshold=WRONG_PRESSING_MATCHER_THRESHOLD,
        )
        tracks = self.db.get_tracks(request_id) or []
        inputs = UnfindableInputs(
            total_tracks=len(tracks) if tracks else None,
            probe_match_counts=tuple(probe_history),
            probe_observed_artist_match=probe.artist_observed,
            search_log_signal=signal,
        )
        verdict = classify_unfindable_from_state(inputs)

        if verdict is None:
            # No rule fires. If the row was previously categorised, the
            # operator deserves to see that the rescue (or generator
            # regeneration) cleared the verdict — clear the column. If
            # the row was never categorised, leave both columns alone.
            if previous is not None:
                self.db.set_unfindable_category(
                    request_id, category=None, categorised_at=now,
                )
                return UnfindableServiceResult(
                    outcome=RESULT_DOWNGRADED,
                    request_id=request_id,
                    previous_category=previous,
                    new_category=None,
                    probe_match_count=probe.match_count,
                    reason="probe evidence no longer supports prior category",
                )
            return UnfindableServiceResult(
                outcome=RESULT_NO_CHANGE,
                request_id=request_id,
                previous_category=None,
                new_category=None,
                probe_match_count=probe.match_count,
            )

        # Verdict fires — write it. We always refresh
        # ``unfindable_categorised_at`` so operators can see how fresh
        # the categorisation is, even when the category itself hasn't
        # changed since the last run.
        self.db.set_unfindable_category(
            request_id,
            category=verdict.category,
            categorised_at=now,
        )
        return UnfindableServiceResult(
            outcome=RESULT_CATEGORISED,
            request_id=request_id,
            previous_category=previous,
            new_category=verdict.category,
            probe_match_count=probe.match_count,
            reason=verdict.reason,
        )

    def categorise_due_batch(
        self,
        *,
        limit: int = DEFAULT_BATCH_SIZE,
    ) -> list[UnfindableServiceResult]:
        """Process the K oldest cohort members and return per-row results.

        Cohort definition: requests with status ``wanted`` whose
        ``last_artist_probe_at`` is NULL or older than
        ``PROBE_INTERVAL_DAYS``. The DB query picks ``limit`` rows
        ordered by oldest probe first; rows not picked roll over to
        the next daily run.
        """
        candidates = self.db.list_unfindable_probe_candidates(
            limit=int(limit),
            probe_interval_days=PROBE_INTERVAL_DAYS,
        )
        results: list[UnfindableServiceResult] = []
        for cand in candidates:
            rid = int(cand["id"])
            try:
                result = self.categorise_request(rid)
            except Exception as exc:  # noqa: BLE001
                # Defence-in-depth: a single bad row cannot poison the
                # rest of the batch. We log + record a probe-failed-
                # equivalent outcome so the operator surface still sees
                # one row per attempt.
                logger.exception(
                    "unfindable_detection: categorise_request crashed "
                    "for request %s", rid,
                )
                result = UnfindableServiceResult(
                    outcome=RESULT_PROBE_FAILED,
                    request_id=rid,
                    error_message=f"{type(exc).__name__}: {exc}",
                )
            results.append(result)
        return results

    # ---------- internal ----------

    @staticmethod
    def _build_probe_history(
        row: Mapping[str, Any], latest_match_count: int,
    ) -> list[int]:
        """Construct the probe history vector for the classifier.

        Persisted columns only carry the LATEST probe count, not a
        rolling window. To get classifier inputs that mirror "the last
        K probes", we layer the just-recorded probe on top of the
        previous probe count. The pure classifier still sees a list,
        so future schema work (per-probe table) can extend the
        history without rewriting the classifier surface.
        """
        history: list[int] = [int(latest_match_count)]
        prior = row.get("last_artist_probe_match_count")
        if prior is not None:
            try:
                history.append(int(prior))
            except (TypeError, ValueError):
                pass
        return history


# ---------------------------------------------------------------------------
# Helpers (private).
# ---------------------------------------------------------------------------


def _as_datetime(value: Any) -> datetime | None:
    """Coerce a row column to a UTC-aware datetime, or None.

    The DB row is the same shape ``make_request_row`` produces — a
    ``datetime``. Test fakes sometimes hand back tz-naive values; we
    coerce to UTC so the cadence comparison is unambiguous.
    """
    if value is None:
        return None
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


# Re-export the names the AST guard banlists so the test file imports
# them from one source (and so a `from lib.search_plan_service import
# advance_for_request` typo in this module triggers an ImportError
# rather than silently shadowing).
__all__ = (
    "ARTIST_MATCH_THRESHOLD",
    "ALL_CATEGORIES",
    "CATEGORY_ARTIST_ABSENT",
    "CATEGORY_ALBUM_ABSENT_ARTIST_PRESENT",
    "CATEGORY_ONE_TRACK_STRUCTURAL",
    "CATEGORY_WRONG_PRESSING_AVAILABLE",
    "DEFAULT_BATCH_SIZE",
    "PROBE_INTERVAL_DAYS",
    "REQUIRED_LOW_PROBES",
    "REQUIRED_ZERO_FIND_CYCLES",
    "RESULT_CATEGORISED",
    "RESULT_DOWNGRADED",
    "RESULT_NO_CHANGE",
    "RESULT_NOT_DUE",
    "RESULT_PROBE_FAILED",
    "RESULT_REQUEST_NOT_FOUND",
    "SEARCH_LOG_WINDOW_DAYS",
    "ArtistProbeResult",
    "ProbeDegradedError",
    "UnfindableCategorisation",
    "UnfindableDetectionService",
    "UnfindableInputs",
    "UnfindableSearchLogSignal",
    "UnfindableServiceResult",
    "WRONG_PRESSING_MATCHER_THRESHOLD",
    "WRONG_PRESSING_MIN_HITS",
    "classify_unfindable_from_state",
    "fuzzy_artist_observed_in_probe",
    "run_artist_probe",
)
