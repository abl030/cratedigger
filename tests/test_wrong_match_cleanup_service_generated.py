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
"""

from __future__ import annotations

import os
import tempfile
import unittest
from unittest.mock import patch

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib import wrong_match_cleanup_service
from lib.import_execution import CancellationToken
from lib.wrong_match_cleanup_service import cleanup_all_wrong_matches
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


def assert_cancellation_never_leaves_partial_delete(
    *, count: int, cancel_after: int,
) -> None:
    """Drive the real per-row delete over a generated queue; cancel
    ``cancel_after`` rows in (0 = before any row); assert no directory
    ends up partially deleted, and that the summary tells the truth
    about how much actually ran."""
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

        violations = _partial_deletion_violations(seeded)
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
    )
    @example(count=1, cancel_after=0)   # cancel before the first row
    @example(count=3, cancel_after=1)   # cancel mid-queue
    @example(count=3, cancel_after=3)   # cancel races the tail
    def test_no_cancellation_ever_leaves_a_partial_directory(
        self, *, count: int, cancel_after: int,
    ) -> None:
        assert_cancellation_never_leaves_partial_delete(
            count=count, cancel_after=cancel_after,
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
            import shutil
            shutil.rmtree(deleted)

            violations = _partial_deletion_violations({
                1: (untouched, untouched_files),
                2: (deleted, deleted_files),
            })

        self.assertEqual(violations, [])

    def test_checkpoint_moved_inside_the_per_row_delete_trips_the_property(
        self,
    ) -> None:
        """If a future regression moved the cancellation check INSIDE the
        per-row delete (checking between individual file removals
        instead of only between rows, the exact trap this issue warns
        against), the result is a partially-emptied directory. The
        property's checker must catch it."""
        with tempfile.TemporaryDirectory() as root:
            source = _make_source(root, "mid-row-checkpoint")
            for extra in range(3):
                with open(
                    os.path.join(source, f"extra-{extra}.bin"), "wb",
                ) as handle:
                    handle.write(b"data")
            original = tuple(sorted(os.listdir(source)))
            token = CancellationToken()

            # Mutant per-row delete: checks cancellation between
            # individual file removals -- the bug this test guards
            # against -- instead of only between rows.
            for index, name in enumerate(original):
                if token.cancelled:
                    break
                os.remove(os.path.join(source, name))
                if index == 1:
                    token.cancel("mid-row-stop")

            violations = _partial_deletion_violations({1: (source, original)})

        self.assertTrue(
            violations,
            "property failed to detect a directory partially deleted by "
            "a checkpoint moved inside the per-row delete",
        )


if __name__ == "__main__":
    unittest.main()
