"""Outcome coverage for the operator complete-mark service."""

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.complete_mark_service import (
    COMPLETE_MARK_HTTP_STATUS,
    CompleteMarkResult,
    canonical_findings,
    set_complete_mark,
)
from lib.library_completeness import (
    CompletenessAlbum,
    CompletenessCounts,
    CompletenessFinding,
    CompletenessReport,
)
from lib.library_completeness_snapshot import (
    LibraryCompletenessSnapshot,
    library_completeness_snapshot_path,
    write_library_completeness_snapshot,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row

UNKLE_FINDING = CompletenessFinding(
    "missing_source_audio",
    "Nursery Rhyme / Breather: installed composite is one continuous audio segment",
)


def _write_snapshot(directory: str, *albums: CompletenessAlbum) -> str:
    path = library_completeness_snapshot_path(directory)
    write_library_completeness_snapshot(path, LibraryCompletenessSnapshot(
        "2026-10-22T00:00:00+00:00", 1.0,
        CompletenessReport(
            "incomplete", CompletenessCounts(len(albums), 0, len(albums), 0, 0), albums,
        ),
    ))
    return path


def _album(release_id: str, *findings: CompletenessFinding) -> CompletenessAlbum:
    return CompletenessAlbum(19925, "UNKLE", "Psyence Fiction", release_id, findings, 13, 12, 12)


class TestSetCompleteMark(unittest.TestCase):
    """Authoritative branch coverage — both operator surfaces wrap this."""

    def setUp(self) -> None:
        self.db = FakePipelineDB()
        # Production Discogs rows duplicate the numeric id into both columns.
        self.db.seed_request(make_request_row(
            id=5096, status="imported", mb_release_id="18004",
            discogs_release_id="18004",
        ))
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def _mark(self, path: str | None, *, marked: bool = True) -> CompleteMarkResult:
        return set_complete_mark(self.db, 5096, marked=marked, snapshot_path=path)

    def test_mark_records_the_current_findings(self) -> None:
        path = _write_snapshot(self.tmp.name, _album("18004", UNKLE_FINDING))
        self.assertEqual(self._mark(path), CompleteMarkResult("marked", 5096))
        row = self.db.get_request(5096)
        assert row is not None
        self.assertIsNotNone(row["marked_complete_at"])
        self.assertEqual(row["marked_complete_findings"], [
            {"kind": UNKLE_FINDING.kind, "detail": UNKLE_FINDING.detail},
        ])

    def test_remark_with_same_findings_is_a_noop(self) -> None:
        path = _write_snapshot(self.tmp.name, _album("18004", UNKLE_FINDING))
        self._mark(path)
        self.assertEqual(self._mark(path).outcome, "already_marked")

    def test_remark_after_findings_change_records_the_new_findings(self) -> None:
        path = _write_snapshot(self.tmp.name, _album("18004", UNKLE_FINDING))
        self._mark(path)
        drift = CompletenessFinding("catalog_drift", "uncatalogued=1 catalogued_missing=0")
        path = _write_snapshot(self.tmp.name, _album("18004", UNKLE_FINDING, drift))
        self.assertEqual(self._mark(path).outcome, "marked")
        row = self.db.get_request(5096)
        assert row is not None
        self.assertEqual(len(row["marked_complete_findings"] or []), 2)

    def test_marking_complete_withdraws_the_incomplete_mark(self) -> None:
        self.db.set_marked_incomplete(5096, marked=True)
        path = _write_snapshot(self.tmp.name, _album("18004", UNKLE_FINDING))
        self._mark(path)
        row = self.db.get_request(5096)
        assert row is not None
        self.assertIsNone(row["marked_incomplete_at"])

    def test_clear_needs_no_census(self) -> None:
        path = _write_snapshot(self.tmp.name, _album("18004", UNKLE_FINDING))
        self._mark(path)
        self.assertEqual(self._mark(None, marked=False).outcome, "cleared")
        self.assertEqual(self._mark(None, marked=False).outcome, "already_clear")

    def test_request_without_census_findings_is_not_listed(self) -> None:
        path = _write_snapshot(self.tmp.name, _album("99999", UNKLE_FINDING))
        self.assertEqual(self._mark(path).outcome, "not_listed")
        row = self.db.get_request(5096)
        assert row is not None
        self.assertIsNone(row["marked_complete_at"])

    def test_census_unavailable(self) -> None:
        self.assertEqual(self._mark(None).outcome, "census_unavailable")
        missing = library_completeness_snapshot_path(self.tmp.name)
        self.assertEqual(self._mark(missing).outcome, "census_unavailable")
        with open(missing, "w", encoding="ascii") as fh:
            fh.write("not json")
        self.assertEqual(self._mark(missing).outcome, "census_unavailable")

    def test_unknown_and_replaced_requests(self) -> None:
        path = _write_snapshot(self.tmp.name, _album("18004", UNKLE_FINDING))
        self.assertEqual(
            set_complete_mark(self.db, 999, marked=True, snapshot_path=path).outcome,
            "not_found",
        )
        self.db.seed_request(make_request_row(
            id=8, status="replaced", mb_release_id="18004-old"))
        path = _write_snapshot(self.tmp.name, _album("18004-old", UNKLE_FINDING))
        self.assertEqual(
            set_complete_mark(self.db, 8, marked=True, snapshot_path=path).outcome,
            "replaced",
        )

    def test_canonical_findings_ignore_order(self) -> None:
        a = {"kind": "unknown", "detail": "x"}
        b = {"kind": "catalog_drift", "detail": "y"}
        self.assertEqual(canonical_findings([a, b]), canonical_findings([b, a]))

    def test_outcome_maps(self) -> None:
        self.assertEqual(COMPLETE_MARK_HTTP_STATUS["not_listed"], 409)
        self.assertEqual(COMPLETE_MARK_HTTP_STATUS["census_unavailable"], 503)


if __name__ == "__main__":
    unittest.main()
