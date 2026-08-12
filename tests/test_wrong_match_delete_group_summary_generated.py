"""Generated patrol: issue #1086 item 3 — the group summary must partition.

Invariant: every candidate ``delete_wrong_match_group`` processes lands in
EXACTLY ONE of its ``WrongMatchDeleteSummary`` outcome buckets — ``deleted``,
``cleared_missing``, ``unavailable``, ``skipped``, ``errors`` — and the
bucket totals sum to the number of candidates. Before this fix,
``OUTCOME_SKIPPED_PATH_UNAVAILABLE`` set both ``skipped=True`` and a
non-``None`` ``error`` on the same ``WrongMatchDeleteResult`` (the
unavailable reason doubles as the error text), so counting ``skipped`` and
``errors`` independently double-counted that one candidate: the toast read
``deleted 1 · skipped 1 · errors 1`` for two real outcomes, not three.

``OUTCOME_SKIPPED_UNSAFE_PATH`` sets the identical shape and is the same
pre-existing convention (predates #1063, left alone then to keep that PR's
blast radius honest). It is fixed the same way here — no longer also
counted toward ``errors`` — but it stays in the ``skipped`` bucket rather
than joining the new ``unavailable`` one: an unsafe-path candidate WAS
positively observed and refused on containment grounds, which is nothing
like "the server never learned whether this folder is even there" (the
fact #1084 keeps ``unavailable`` distinct for).

The property drives the REAL ``delete_wrong_match_group`` over composed
real quarantine folders — one independent candidate per sampled world,
seeded under the same request so they form one group — and checks the
returned summary against the bucket each world must land in.
"""

from __future__ import annotations

import os
import shutil
import tempfile
import unittest
from collections.abc import Sequence
from typing import TYPE_CHECKING

from hypothesis import HealthCheck, example, given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.wrong_match_delete_service import (
    OUTCOME_SKIPPED_PATH_UNAVAILABLE,
    WrongMatchDeleteResult,
    WrongMatchDeleteSummary,
    delete_wrong_match_group,
)
from tests.fakes import FakePipelineDB
from tests.helpers import SeededWrongMatch, seed_visible_wrong_match

if TYPE_CHECKING:
    from lib.pipeline_db.rows import DownloadLogWithEvidenceRow

#: One real quarantine candidate per named world, all reachable end-to-end
#: through ``delete_wrong_match_group``. Deliberately excludes
#: "lock_contended" (covered on its own in
#: ``tests/test_protected_path_truth_generated.py``): ``FakePipelineDB``'s
#: advisory-lock stub is a single DB-wide flag, not a per-candidate one, so
#: composing it with other worlds in the same group would fail every OTHER
#: candidate's lock too — an artifact of the fake, not of this invariant.
#:
#: "invalid_row" is NOT the same producer as "download_log_missing" for
#: FakePipelineDB's OWN reasons: the fake's ``get_wrong_matches()`` drops a
#: falsy ``failed_path`` before the row is ever visible, so
#: ``_delete_wrong_match``'s ``failed_path_missing`` branch is unreachable
#: through this fake (issue #1095 tracks the drift from the real SQL, which
#: only filters ``IS NOT NULL`` and so would let ``""`` survive). This world
#: instead reaches the OTHER ``OUTCOME_SKIPPED_INVALID_ROW`` producer,
#: ``download_log_missing``: the row is listed, then its own entry lookup
#: returns ``None`` — the exact race the early-return guards against.
WORLDS: tuple[str, ...] = (
    "present",
    "genuinely_missing",
    "unreadable_parent",
    "unreadable_album",
    "unsafe_root",
    "active_job",
    "delete_error",
    "invalid_row",
)

#: The bucket each world's real outcome must land in.
BUCKET_BY_WORLD: dict[str, str] = {
    "present": "deleted",
    "genuinely_missing": "cleared_missing",
    "unreadable_parent": "unavailable",
    "unreadable_album": "errors",
    "unsafe_root": "skipped",
    "active_job": "skipped",
    "delete_error": "errors",
    "invalid_row": "skipped",
}

BUCKETS: tuple[str, ...] = (
    "deleted", "cleared_missing", "unavailable", "skipped", "errors",
)


class _VanishingEntryDB(FakePipelineDB):
    """A ``FakePipelineDB`` that can make ONE entry disappear from lookup.

    ``delete_wrong_match_group`` lists a row via ``get_wrong_matches()``,
    then (per candidate) calls ``get_download_log_entry(log_id)`` to fetch
    it again. In production these two reads can race — the row is deleted
    between them — and ``_delete_wrong_match`` guards that exact race with
    its ``download_log_missing`` early return. The plain fake has no seam
    for it because its two reads never naturally disagree; this subclass
    manufactures the one entry this module's WORLDS list needs, without
    touching ``get_wrong_matches()`` (the row stays listed, matching the
    real race).
    """

    def __init__(self) -> None:
        super().__init__()
        # A set, not a single id: a group can compose more than one
        # "invalid_row" candidate (the generated property samples with
        # repeats), and every one of them must vanish independently.
        self.vanished_log_ids: set[int] = set()

    def get_download_log_entry(
        self, log_id: int,
    ) -> DownloadLogWithEvidenceRow | None:
        if log_id in self.vanished_log_ids:
            return None
        return super().get_download_log_entry(log_id)


def _build_candidate(
    db: FakePipelineDB, root: str, world: str,
) -> SeededWrongMatch:
    """Seed one real quarantine candidate under request 1, in ``world``.

    Mirrors ``tests/test_protected_path_truth_generated.py::_build_world``'s
    per-world disk mutations for the worlds this module needs; kept as its
    own small copy rather than an import so this file's scope stays clear
    of that file's absence-vocabulary invariant (issue #1086 item 1).
    """
    quarantine = "elsewhere" if world == "unsafe_root" else "wrong_matches"
    source = seed_visible_wrong_match(db, root, quarantine=quarantine)
    if world == "genuinely_missing":
        shutil.rmtree(source.path)
    elif world == "unreadable_parent":
        os.chmod(source.parent, 0o000)
    elif world == "unreadable_album":
        os.chmod(source.path, 0o000)
    elif world == "delete_error":
        # Readable and listable, but its child cannot be unlinked: a
        # genuine mid-delete failure, distinct from an unobservable path.
        os.chmod(source.path, 0o500)
    elif world == "active_job":
        db.enqueue_import_job(
            "force_import",
            request_id=source.request_id,
            payload={
                "download_log_id": source.download_log_id,
                "failed_path": source.path,
            },
        )
    elif world == "invalid_row":
        assert isinstance(db, _VanishingEntryDB)
        db.vanished_log_ids.add(source.download_log_id)
    return source


def _restore(source: SeededWrongMatch) -> None:
    """Undo every permission world so the tmp tree can be removed."""
    for path in (source.parent, source.path):
        try:
            os.chmod(path, 0o700)
        except OSError:
            continue


def bucket_totals_violations(
    *, worlds: Sequence[str], summary: WrongMatchDeleteSummary,
) -> list[str]:
    """Every clause independent, so one violating world trips every clause it hits."""
    violations: list[str] = []
    expected: dict[str, int] = {}
    for world in worlds:
        bucket = BUCKET_BY_WORLD[world]
        expected[bucket] = expected.get(bucket, 0) + 1
    actual = {
        "deleted": summary.deleted,
        "cleared_missing": summary.cleared_missing,
        "unavailable": summary.unavailable,
        "skipped": summary.skipped,
        "errors": summary.errors,
    }
    for bucket in BUCKETS:
        want = expected.get(bucket, 0)
        if actual[bucket] != want:
            violations.append(
                f"bucket {bucket!r} counted {actual[bucket]}, expected "
                f"{want} for worlds {list(worlds)}"
            )
    total = sum(actual.values())
    if total != len(worlds):
        violations.append(
            f"bucket totals summed to {total}, expected {len(worlds)} "
            f"candidates (worlds {list(worlds)}) — a candidate was "
            "double-counted or lost"
        )
    return violations


def _build_group(
    db: FakePipelineDB, root: str, worlds: Sequence[str],
) -> list[SeededWrongMatch]:
    sources: list[SeededWrongMatch] = []
    for index, world in enumerate(worlds):
        candidate_root = os.path.join(root, f"candidate{index}")
        os.makedirs(candidate_root, exist_ok=True)
        sources.append(_build_candidate(db, candidate_root, world))
    return sources


class TestGroupSummaryBucketsPinned(unittest.TestCase):
    """Deterministic pin: one real candidate per bucket, in one group."""

    def test_each_reachable_outcome_lands_in_exactly_one_bucket(self) -> None:
        worlds = list(WORLDS)
        db = _VanishingEntryDB()
        sources: list[SeededWrongMatch] = []
        with tempfile.TemporaryDirectory() as root:
            try:
                sources = _build_group(db, root, worlds)
                summary = delete_wrong_match_group(db, 1)
            finally:
                for source in sources:
                    _restore(source)

        self.assertEqual(summary.deleted, 1, summary)
        self.assertEqual(summary.cleared_missing, 1, summary)
        self.assertEqual(summary.unavailable, 1, summary)
        # unreadable_album + delete_error: both genuine delete failures.
        self.assertEqual(summary.errors, 2, summary)
        # unsafe_root + active_job + invalid_row: all refused, none an error.
        self.assertEqual(summary.skipped, 3, summary)
        self.assertEqual(
            summary.deleted + summary.cleared_missing + summary.unavailable
            + summary.skipped + summary.errors,
            len(worlds),
        )
        self.assertEqual(
            bucket_totals_violations(worlds=worlds, summary=summary), [])


class TestGroupSummaryBucketsGenerated(unittest.TestCase):
    """The real service, composed over generated world mixes."""

    @example(worlds=["unreadable_parent"])
    @example(worlds=["unsafe_root"])
    @example(worlds=["invalid_row"])
    @example(worlds=["invalid_row", "invalid_row"])
    @example(worlds=["present", "unreadable_parent"])
    @example(worlds=["unreadable_parent", "unreadable_album"])
    @example(worlds=["unreadable_parent", "unsafe_root"])
    @example(worlds=["unsafe_root", "unreadable_album"])
    @example(worlds=["invalid_row", "unreadable_parent"])
    @example(worlds=list(WORLDS))
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    @given(worlds=st.lists(st.sampled_from(WORLDS), min_size=1, max_size=3))
    def test_every_candidate_lands_in_exactly_one_bucket(
        self, worlds: list[str],
    ) -> None:
        db = _VanishingEntryDB()
        sources: list[SeededWrongMatch] = []
        with tempfile.TemporaryDirectory() as root:
            try:
                sources = _build_group(db, root, worlds)
                summary = delete_wrong_match_group(db, 1)
            finally:
                for source in sources:
                    _restore(source)
            violations = bucket_totals_violations(worlds=worlds, summary=summary)
            self.assertEqual(violations, [], "\n".join(violations))

    def test_known_bad_checker_trips_on_the_pre_fix_double_count(self) -> None:
        """The exact pre-#1086 shape: one candidate landing in two buckets."""
        result = WrongMatchDeleteResult(
            download_log_id=1,
            outcome=OUTCOME_SKIPPED_PATH_UNAVAILABLE,
            skipped=True,
            error="path_unavailable[EACCES]: denied",
        )
        pre_fix = WrongMatchDeleteSummary(
            request_id=1, outcome="failed", success=False, processed=1,
            deleted=0, cleared_missing=0, deleted_paths=0, cleared=0,
            unavailable=0,
            # The actual pre-fix double count: both buckets claim the one
            # OUTCOME_SKIPPED_PATH_UNAVAILABLE candidate.
            skipped=1, errors=1,
            remaining=1, group_empty=False, results=(result,),
        )
        violations = bucket_totals_violations(
            worlds=["unreadable_parent"], summary=pre_fix)
        self.assertTrue(
            any("'errors'" in v for v in violations),
            f"expected an errors-bucket violation, got {violations!r}")
        self.assertTrue(
            any("'unavailable'" in v for v in violations),
            f"expected an unavailable-bucket violation, got {violations!r}")
        self.assertTrue(
            any("summed to 2" in v for v in violations),
            f"expected a total-mismatch violation, got {violations!r}")

        # Must still work, the other direction: a candidate silently LOST
        # from every bucket trips the same total-mismatch clause.
        lost = WrongMatchDeleteSummary(
            request_id=1, outcome="partial", success=False, processed=1,
            deleted=0, cleared_missing=0, deleted_paths=0, cleared=0,
            unavailable=0, skipped=0, errors=0,
            remaining=1, group_empty=False, results=(result,),
        )
        violations = bucket_totals_violations(
            worlds=["unreadable_parent"], summary=lost)
        self.assertTrue(any("summed to 0" in v for v in violations))

        # Must still work: the FIXED shape — one bucket, one candidate —
        # trips nothing.
        fixed = WrongMatchDeleteSummary(
            request_id=1, outcome="partial", success=False, processed=1,
            deleted=0, cleared_missing=0, deleted_paths=0, cleared=0,
            unavailable=1, skipped=0, errors=0,
            remaining=1, group_empty=False, results=(result,),
        )
        self.assertEqual(
            bucket_totals_violations(
                worlds=["unreadable_parent"], summary=fixed),
            [])
