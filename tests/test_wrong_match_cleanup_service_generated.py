"""Generated patrol: Wrong Matches bulk cleanup cancellation never leaves
a partially-deleted album directory (issue #1083).

Invariant: cancellation is a plain boolean check-and-break BETWEEN rows
in ``cleanup_all_wrong_matches`` — never a raise, never mid-delete. This
drives the REAL per-row delete (``cleanup_wrong_match`` ->
``cleanup_wrong_match_source`` -> ``shutil.rmtree``) against a real
temporary quarantine root, over generated queue sizes and cancellation
points, and asserts every seeded directory ends up either fully intact
(cancelled/kept before it started) or fully gone (deleted) — never a
partial subset of its original files.

Two cancellation-timing arms are exercised, both over the same real
production entry points:

- ``mid_row_race=False`` (between rows): the token is cancelled
  immediately AFTER a target row's real delete returns — proves the
  between-rows checkpoint itself behaves, but a cancel that only ever
  lands once a row is already done can never observe (or catch a
  regression inside) that row's own delete.
- ``mid_row_race=True`` (in-flight): a REAL second thread cancels the
  token WHILE the target row's own ``shutil.rmtree`` call is still on
  the stack, proven by a started/may-proceed handshake rather than a
  sleep — the cancel is observably concurrent with the row's in-flight
  delete, not merely "before" or "after" it. Production never reads the
  token again once a row starts, so every generated example in this arm
  still finds the row fully deleted.

  This proves exactly ONE claim, not the general class of "a checkpoint
  moved inside the per-row delete": cancellation delivered AT THE
  INSTANT ``shutil.rmtree`` is entered does not produce a partial
  directory. The harness can only synchronize a race at that one call
  site — a checkpoint that runs BEFORE ``shutil.rmtree`` is ever
  reached (for example a per-file removal loop that only calls
  ``shutil.rmtree`` once it has already finished, or been interrupted)
  is invisible to this arm's handshake and can still leave a partial
  directory undetected. The known-bad self-test below plants a
  mid-delete checkpoint AT that one instant this harness controls and
  proves the property fails there; it is not evidence the property
  catches every mid-delete-checkpoint shape. The wider gap — patrolling
  cancellation delivered at arbitrary points inside a row, not just at
  ``shutil.rmtree`` entry — is real test engineering tracked separately
  (issue #1095), deliberately not attempted here.
"""

from __future__ import annotations

import os
import shutil
import tempfile
import threading
import unittest
from contextlib import contextmanager
from unittest.mock import patch

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib import wrong_match_cleanup_service
from lib.import_execution import CancellationToken
from lib.wrong_match_cleanup_service import (
    WrongMatchCleanupSummary,
    cleanup_all_wrong_matches,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row
from tests.test_wrong_match_cleanup_service import (
    _cfg,
    _evidence,
    _log_wrong_match,
    _make_source,
    _patch_current_evidence_helper,
    _store_evidence,
)

_REQUEST_ID = 1


def _partial_deletion_violations(
    seeded: dict[int, tuple[str, tuple[str, ...]]],
) -> list[str]:
    """Classify each seeded row's directory as intact / deleted / PARTIAL.

    A directory is legitimate only in two states: still present with
    EXACTLY its original file set (untouched — kept, or never reached),
    or entirely absent (deleted). Anything else — present with a
    different file set, including empty — is evidence that a delete was
    interrupted partway through.
    """
    violations: list[str] = []
    for log_id, (path, filenames) in seeded.items():
        if not os.path.isdir(path):
            continue  # fully deleted -- fine
        present = tuple(sorted(os.listdir(path)))
        if present == filenames:
            continue  # untouched -- fine
        violations.append(
            f"log_id={log_id} path={path}: partial delete, "
            f"{len(present)}/{len(filenames)} original files remain "
            f"({sorted(set(filenames) - set(present))} missing)"
        )
    return violations


@contextmanager
def _cancel_while_row_is_in_flight(
    token: CancellationToken,
    target_path: str,
    *,
    mutant: bool = False,
):
    """Cancel ``token`` from a SECOND thread while ``target_path``'s own
    ``shutil.rmtree`` call is genuinely on the stack — proven by a
    started/may-proceed handshake, not a sleep, so the cancel is
    observably concurrent with the row's in-flight delete rather than
    merely "before" or "after" it.

    ``mutant=True`` (self-test only, #1083 known-bad) swaps the single
    atomic ``rmtree`` for the exact regression this issue warns against:
    file-by-file removal that checks ``token.cancelled`` between
    individual removals instead of only between rows. The generated
    property never sets this — production has no such checkpoint, so
    every generated example uses the real atomic ``rmtree`` and the row
    always ends up intact or fully gone, never partial.
    """
    real_rmtree = shutil.rmtree
    started = threading.Event()
    may_proceed = threading.Event()
    _HARNESS_STALL = (
        "harness stall in _cancel_while_row_is_in_flight ({who}): the "
        "{what} within 5s. This names a TEST synchronization timeout, "
        "not a production failure -- 60 concurrent runs (60 more under "
        "120 CPU burners) never reproduced it, so treat this as the "
        "harness wedging (or the wait budget genuinely no longer being "
        "enough), not evidence against the code under test."
    )

    def wrapped_rmtree(path: str) -> None:
        # Production only ever calls ``shutil.rmtree(resolved_path)`` --
        # one positional argument, no options -- so the stand-in matches
        # that exact call shape rather than a generic ``*args/**kwargs``
        # passthrough (which pyright cannot resolve against rmtree's
        # overloaded ``ignore_errors``/``onerror``/``onexc`` signature).
        if path != target_path:
            real_rmtree(path)
            return
        started.set()
        assert may_proceed.wait(timeout=5), _HARNESS_STALL.format(
            who="wrapped_rmtree",
            what="canceller thread never landed its cancel",
        )
        if not mutant:
            real_rmtree(path)
            return
        # MUTANT (self-test only): checkpoint moved INSIDE the per-row
        # delete -- checks cancellation between individual file removals
        # instead of only between rows, exactly the trap #1083 warns
        # against.
        for name in sorted(os.listdir(path)):
            os.remove(os.path.join(path, name))
            if token.cancelled:
                return
        os.rmdir(path)

    def canceller() -> None:
        # Runs on a background thread -- an ``assert`` raised here
        # would never reach the test runner (bare thread exceptions are
        # swallowed), so this only RECORDS the wait's outcome into
        # ``landed``; the actual assertion happens after ``join()``
        # below, back on the test's own thread. Still cancels and
        # unblocks ``wrapped_rmtree`` even after a stall, so a
        # slow-but-not-dead target row doesn't hang the sweep forever.
        landed[0] = started.wait(timeout=5)
        token.cancel("generated-stop-mid-row")
        may_proceed.set()

    landed: list[bool] = [False]
    thread = threading.Thread(target=canceller, daemon=True)
    thread.start()
    with patch("lib.wrong_matches.shutil.rmtree", side_effect=wrapped_rmtree):
        yield
    thread.join(timeout=5)
    assert landed[0], _HARNESS_STALL.format(
        who="canceller",
        what="target row's delete never reached shutil.rmtree",
    )


def _run_cancellation_sweep(
    *,
    count: int,
    cancel_after: int,
    mid_row_race: bool = False,
    mid_row_mutant: bool = False,
) -> tuple[WrongMatchCleanupSummary, list[str]]:
    """Drive the real per-row delete over a generated queue; cancel
    ``cancel_after`` rows in (0 = before any row). Returns the summary
    and the partial-deletion violations found once the sweep settles."""
    cancel_after = min(cancel_after, count)
    with tempfile.TemporaryDirectory() as root:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=_REQUEST_ID, status="wanted", mb_release_id="mbid-1",
        ))

        seeded: dict[int, tuple[str, tuple[str, ...]]] = {}
        log_ids: list[int] = []
        for index in range(count):
            source = _make_source(root, f"row-{index}")
            # A few extra files so a mid-tree interruption would be
            # structurally observable, not just "file present/absent".
            for extra in range(2):
                with open(
                    os.path.join(source, f"extra-{extra}.bin"), "wb",
                ) as handle:
                    handle.write(b"data")
            filenames = tuple(sorted(os.listdir(source)))
            log_id = _log_wrong_match(db, _REQUEST_ID, source)
            db.set_download_log_candidate_evidence(
                log_id,
                _store_evidence(
                    db,
                    _evidence(
                        source,
                        mb_release_id=f"mbid-row-{index}",
                        matched_bad_audio_hash=True,
                    ),
                ),
            )
            seeded[log_id] = (source, filenames)
            log_ids.append(log_id)

        # ``get_wrong_matches()`` sorts newest-first within a request
        # (mirrors the real DISTINCT ON projection) -- never assume
        # insertion order matches processing order.
        ordered_ids = [
            row["download_log_id"] for row in db.get_wrong_matches()
        ]
        assert len(ordered_ids) == count, (ordered_ids, log_ids)

        token = CancellationToken()
        with _patch_current_evidence_helper():
            if cancel_after <= 0:
                token.cancel("generated-stop")
                summary = cleanup_all_wrong_matches(
                    db, confirm_all_wrong_matches=True, cfg=_cfg(),
                    cancellation_token=token,
                )
            elif mid_row_race:
                target_id = ordered_ids[cancel_after - 1]
                target_path = seeded[target_id][0]
                with _cancel_while_row_is_in_flight(
                    token, target_path, mutant=mid_row_mutant,
                ):
                    summary = cleanup_all_wrong_matches(
                        db, confirm_all_wrong_matches=True, cfg=_cfg(),
                        cancellation_token=token,
                    )
            else:
                real_cleanup_wrong_match = (
                    wrong_match_cleanup_service.cleanup_wrong_match
                )
                target_id = ordered_ids[cancel_after - 1]

                def maybe_cancel(db_arg, download_log_id, **kwargs):
                    result = real_cleanup_wrong_match(
                        db_arg, download_log_id, **kwargs,
                    )
                    if download_log_id == target_id:
                        token.cancel("generated-stop")
                    return result

                with patch(
                    "lib.wrong_match_cleanup_service.cleanup_wrong_match",
                    side_effect=maybe_cancel,
                ):
                    summary = cleanup_all_wrong_matches(
                        db, confirm_all_wrong_matches=True, cfg=_cfg(),
                        cancellation_token=token,
                    )

        return summary, _partial_deletion_violations(seeded)


def assert_cancellation_never_leaves_partial_delete(
    *, count: int, cancel_after: int, mid_row_race: bool = False,
) -> None:
    """Assert the invariant holds for one generated ``(count,
    cancel_after, mid_row_race)`` world: no partial directory, and the
    summary tells the truth about how much actually ran."""
    cancel_after = min(cancel_after, count)
    summary, violations = _run_cancellation_sweep(
        count=count, cancel_after=cancel_after, mid_row_race=mid_row_race,
    )
    if violations:
        raise AssertionError(
            "cancellation left a partially-deleted album directory:\n"
            + "\n".join(violations)
        )

    if cancel_after >= count:
        if summary.cancelled or summary.processed != count:
            raise AssertionError(
                "cancel landing after the last row must not claim "
                f"work it did not do: cancelled={summary.cancelled} "
                f"processed={summary.processed} count={count}"
            )
    else:
        if not summary.cancelled or summary.processed != cancel_after:
            raise AssertionError(
                "mid-queue cancellation must stop after exactly the "
                f"rows before the stop: cancelled={summary.cancelled} "
                f"processed={summary.processed} "
                f"cancel_after={cancel_after}"
            )


class TestWrongMatchCancellationNeverPartialGenerated(unittest.TestCase):
    @given(
        count=st.integers(min_value=1, max_value=4),
        cancel_after=st.integers(min_value=0, max_value=4),
        mid_row_race=st.booleans(),
    )
    @example(count=1, cancel_after=0, mid_row_race=False)  # cancel before the first row
    @example(count=3, cancel_after=1, mid_row_race=False)  # cancel mid-queue, between rows
    @example(count=3, cancel_after=3, mid_row_race=False)  # cancel races the tail, between rows
    @example(count=3, cancel_after=1, mid_row_race=True)   # cancel races an IN-FLIGHT row
    @example(count=3, cancel_after=3, mid_row_race=True)   # in-flight race on the tail row
    def test_no_cancellation_ever_leaves_a_partial_directory(
        self, *, count: int, cancel_after: int, mid_row_race: bool,
    ) -> None:
        assert_cancellation_never_leaves_partial_delete(
            count=count, cancel_after=cancel_after,
            mid_row_race=mid_row_race,
        )


class TestCheckerRejectsPartialDeleteState(unittest.TestCase):
    """Known-bad self-tests (#1083): the checker and the property must
    actually trip on the world they claim to rule out."""

    def test_checker_flags_a_directory_missing_only_some_of_its_files(
        self,
    ) -> None:
        """Planted violating state: manually thin a directory to a
        strict, non-empty subset of its original files -- the shape a
        checkpoint firing mid-``rmtree`` would leave behind."""
        with tempfile.TemporaryDirectory() as root:
            source = _make_source(root, "half-deleted")
            with open(os.path.join(source, "extra.bin"), "wb") as handle:
                handle.write(b"data")
            original = tuple(sorted(os.listdir(source)))
            os.remove(os.path.join(source, original[0]))

            violations = _partial_deletion_violations({1: (source, original)})

        self.assertTrue(
            violations, "checker failed to flag a partial delete")
        self.assertIn("half-deleted", violations[0])

    def test_checker_accepts_untouched_and_fully_deleted_directories(self) -> None:
        """Converse guard: the checker must not be trigger-happy on the
        two legitimate end states."""
        with tempfile.TemporaryDirectory() as root:
            untouched = _make_source(root, "untouched")
            untouched_files = tuple(sorted(os.listdir(untouched)))
            deleted = _make_source(root, "deleted")
            deleted_files = tuple(sorted(os.listdir(deleted)))
            shutil.rmtree(deleted)

            violations = _partial_deletion_violations({
                1: (untouched, untouched_files),
                2: (deleted, deleted_files),
            })

        self.assertEqual(violations, [])

    def test_checkpoint_moved_inside_the_per_row_delete_trips_the_property(
        self,
    ) -> None:
        """Known-bad self-test for the CHECKER, not a production-mutant
        kill. The mid-delete checkpoint is planted as THIS test's own
        stand-in at the exact ``shutil.rmtree`` call the harness already
        controls (``mid_row_mutant=True`` inside
        ``_cancel_while_row_is_in_flight``) -- it is driven through the
        real production entry points
        (``cleanup_all_wrong_matches`` -> ``cleanup_wrong_match`` ->
        ``cleanup_wrong_match_source``) up to that call, with a genuine
        concurrent race, per test-fidelity Rule C (a producible world,
        not a hand-written violation). That is narrower than "the
        property catches a mid-delete-checkpoint regression": swapping
        in the SAME file-by-file loop as a real edit to
        ``lib/wrong_matches.py`` (removing the ``shutil.rmtree`` call
        this harness hooks, so the loop runs and finishes BEFORE
        ``shutil.rmtree`` is ever reached) makes the harness's
        synchronization miss the race entirely -- the property still
        fails on that real mutant, but via the cancelled/processed
        mismatch in ``assert_cancellation_never_leaves_partial_delete``,
        never via ``_partial_deletion_violations``, and only once both
        handshake waits in ``_cancel_while_row_is_in_flight`` time out.
        ``_partial_deletion_violations`` — the checker this whole module
        is named for — has only ever been observed to trip here and on
        the hand-planted directory in
        ``test_checker_flags_a_directory_missing_only_some_of_its_files``
        above, never on an actual production-code mutant reached end to
        end. See the module docstring and issue #1095 for the real gap
        this leaves."""
        _summary, violations = _run_cancellation_sweep(
            count=3, cancel_after=1, mid_row_race=True, mid_row_mutant=True,
        )

        self.assertTrue(
            violations,
            "known-bad self-test's own stand-in mutant failed to trip "
            "the checker -- see the docstring above for what this "
            "test does and does not prove",
        )


if __name__ == "__main__":
    unittest.main()
