"""Generated contracts on what ``try_enqueue`` returns.

Two, both driving the real entry point: a selected manifest is admitted iff
every advertised size is positive (issue #1301), and whatever the outcome,
the returned attempt reports every match result it consumed (issue #1313).
"""

from __future__ import annotations

import unittest
from collections.abc import Sequence
from unittest.mock import patch

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from cratedigger import SlskdFile
from lib.enqueue import EnqueueAttempt, try_enqueue
from lib.grab_list import DownloadFile
from lib.matching import MatchResult
from lib.slskd_transfers import SlskdEnqueueOutcome
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row
from tests.test_enqueue_fanout import (
    _candidate_score,
    _const_match,
    _ctx_with_download_ownership,
    _make_cfg,
    _make_tracks,
    run_forensics_world,
)


def _run_size_world(
    sizes: list[int | None],
) -> tuple[EnqueueAttempt, FakePipelineDB, int]:
    cfg = _make_cfg()
    db = FakePipelineDB()
    db.seed_request(make_request_row(id=1, status="wanted"))
    ctx = _ctx_with_download_ownership(cfg=cfg, db=db)
    username = "peer"
    file_dir = "Music\\peer\\Album"
    ctx.user_upload_speed[username] = 10_000
    files: list[SlskdFile] = []
    for ordinal, size in enumerate(sizes, start=1):
        file: SlskdFile = {
            "filename": f"{ordinal:02d} - Track {ordinal}.mp3",
            "bitRate": 216,
        }
        if size is not None:
            file["size"] = size
        files.append(file)
    match = MatchResult(
        matched=True,
        directory={"directory": file_dir, "files": files},
        file_dir=file_dir,
        candidates=[],
    )
    enqueue_calls = 0

    def accept_enqueue(
        *,
        username: str,
        files: Sequence[SlskdFile],
        file_dir: str,
        **_kwargs: object,
    ) -> SlskdEnqueueOutcome:
        nonlocal enqueue_calls
        enqueue_calls += 1
        downloads: list[DownloadFile] = []
        for ordinal, file in enumerate(files, start=1):
            size = file.get("size")
            assert isinstance(size, int) and not isinstance(size, bool)
            downloads.append(DownloadFile(
                filename=file["filename"],
                id=f"transfer-{ordinal}",
                file_dir=file_dir,
                username=username,
                size=size,
            ))
        return SlskdEnqueueOutcome(
            status="accepted",
            downloads=downloads,
        )

    with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
         patch(
             "lib.enqueue.slskd_enqueue_with_outcome",
             side_effect=accept_enqueue,
         ):
        attempt = try_enqueue(
            _make_tracks(),
            {username: {"mp3": [file_dir]}},
            "mp3",
            ctx,
            match_fn=_const_match(match),
        )
    return attempt, db, enqueue_calls


class TestAdvertisedSizeAdmissionProperty(unittest.TestCase):
    """A selected audio manifest is admitted iff every size is positive."""

    @given(
        sizes=st.lists(
            st.one_of(st.none(), st.integers(min_value=-1, max_value=2)),
            min_size=1,
            max_size=6,
        ),
    )
    @example(sizes=[1, 0])
    @example(sizes=[1, None])
    def test_non_positive_or_missing_size_never_reaches_slskd(
        self, *, sizes: list[int | None],
    ) -> None:
        attempt, db, enqueue_calls = _run_size_world(sizes)
        should_admit = all(size is not None and size > 0 for size in sizes)

        self.assertEqual(attempt.matched, should_admit)
        self.assertEqual(enqueue_calls, int(should_admit))
        self.assertEqual(
            db.request(1)["status"],
            "downloading" if should_admit else "wanted",
        )
        if not should_admit:
            self.assertEqual(db.record_transfer_enqueue_calls, [])


def forensics_violations(
    attempt: EnqueueAttempt,
    consumed: list[tuple[int, int]],
) -> list[str]:
    """Every way a returned attempt can misreport what matching cost.

    ``consumed`` is one ``(candidates, skips)`` pair per ``check_for_match``
    the attempt actually made — recorded by the injected ``match_fn``, so
    the expectation follows the real iteration rather than assuming the
    attempt walked every peer. Accumulating rather than short-circuiting so
    a candidate-count violation cannot hide a skip-count one.

    Both counting clauses are equalities, not floors: over-reporting is as
    wrong as under-reporting, and a stray second ``record_match`` call is
    the producible defect that makes it so.
    """
    violations: list[str] = []
    expected_candidates = sum(candidates for candidates, _ in consumed)
    expected_skips = sum(skips for _, skips in consumed)
    if len(attempt.candidates) != expected_candidates:
        violations.append(
            f"candidate scores dropped: reported {len(attempt.candidates)}, "
            f"consumed {expected_candidates}"
        )
    if attempt.pre_filter_skip_count != expected_skips:
        violations.append(
            f"pre-filter skips dropped: reported "
            f"{attempt.pre_filter_skip_count}, consumed {expected_skips}"
        )
    if attempt.matched and attempt.enqueue_failed:
        violations.append("an attempt cannot both keep a candidate and fail")
    return violations


class TestAttemptForensicsProperty(unittest.TestCase):
    """An attempt reports every match result it consumed, whatever it decides."""

    @given(
        plan=st.lists(
            st.tuples(
                st.booleans(),
                st.integers(min_value=0, max_value=3),
                st.integers(min_value=0, max_value=3),
            ),
            min_size=1,
            max_size=5,
        ),
        enqueue_succeeds=st.booleans(),
        lane=st.sampled_from(("single", "multi")),
        empty_after_filter=st.booleans(),
    )
    # The world the deterministic pin names: a peer walked past before the
    # one that matched, whose skips would vanish if any return read the
    # winner's counters instead of the attempt's.
    @example(plan=[(False, 2, 3), (True, 1, 1)], enqueue_succeeds=True,
             lane="single", empty_after_filter=False)
    # Nothing matches at all: the whole walk is forensics and nothing else.
    @example(plan=[(False, 1, 2), (False, 3, 0)], enqueue_succeeds=True,
             lane="single", empty_after_filter=False)
    # A match whose enqueue is refused — the failure return path.
    @example(plan=[(True, 2, 5)], enqueue_succeeds=False, lane="single", empty_after_filter=False)
    # The multi lane: fifteen of the nineteen return sites live there,
    # and the single-lane-only property reached none of them.
    @example(plan=[(False, 1, 2), (True, 2, 1)], enqueue_succeeds=True,
             lane="multi", empty_after_filter=False)
    # A match whose directory filters down to nothing — each lane's own
    # "nothing remained after filtering and admission" return, reachable
    # from no generated world before this dimension existed.
    @example(plan=[(True, 1, 1)], enqueue_succeeds=True,
             lane="multi", empty_after_filter=True)
    @example(plan=[(True, 1, 1)], enqueue_succeeds=True,
             lane="single", empty_after_filter=True)
    def test_every_consumed_match_is_reported(
        self,
        *,
        plan: list[tuple[bool, int, int]],
        enqueue_succeeds: bool,
        lane: str,
        empty_after_filter: bool,
    ) -> None:
        attempt, consumed = run_forensics_world(
            plan, enqueue_succeeds=enqueue_succeeds, lane=lane,
            empty_after_filter=empty_after_filter,
        )
        self.assertEqual(forensics_violations(attempt, consumed), [])


class TestForensicsCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-test per clause: each one fires, with its own message.

    Both counting clauses are tested in BOTH directions. Only testing the
    under-report direction leaves ``!=`` free to weaken to ``<`` unnoticed
    (issue #1313, mutant runner findings 14 and 15), and over-reporting is a
    producible defect, not a hypothetical one: recording the same match twice
    is one stray ``record_match`` call away.
    """

    _CLEAN = EnqueueAttempt(
        matched=False, candidates=(), pre_filter_skip_count=0,
    )

    def _attempt(self, *, candidates: int, skips: int) -> EnqueueAttempt:
        return EnqueueAttempt(
            matched=False,
            candidates=tuple(
                _candidate_score("peer", f"dir-{i}") for i in range(candidates)
            ),
            pre_filter_skip_count=skips,
        )

    def test_dropped_candidate_scores_trip_their_own_clause(self) -> None:
        violations = forensics_violations(self._CLEAN, [(2, 0)])
        self.assertEqual(len(violations), 1)
        self.assertIn("candidate scores dropped", violations[0])

    def test_double_counted_candidate_scores_trip_it_too(self) -> None:
        violations = forensics_violations(
            self._attempt(candidates=4, skips=0), [(2, 0)])
        self.assertEqual(len(violations), 1)
        self.assertIn("candidate scores dropped", violations[0])
        self.assertIn("reported 4, consumed 2", violations[0])

    def test_dropped_skips_trip_their_own_clause(self) -> None:
        violations = forensics_violations(self._CLEAN, [(0, 3)])
        self.assertEqual(len(violations), 1)
        self.assertIn("pre-filter skips dropped", violations[0])

    def test_double_counted_skips_trip_it_too(self) -> None:
        violations = forensics_violations(
            self._attempt(candidates=0, skips=6), [(0, 3)])
        self.assertEqual(len(violations), 1)
        self.assertIn("pre-filter skips dropped", violations[0])
        self.assertIn("reported 6, consumed 3", violations[0])

    def test_matched_and_failed_trips_its_own_clause(self) -> None:
        violations = forensics_violations(
            EnqueueAttempt(
                matched=True, enqueue_failed=True,
                candidates=(), pre_filter_skip_count=0,
            ),
            [],
        )
        self.assertEqual(len(violations), 1)
        self.assertIn("cannot both keep a candidate and fail", violations[0])

    def test_a_clean_attempt_trips_nothing(self) -> None:
        self.assertEqual(forensics_violations(self._CLEAN, [(0, 0)]), [])
