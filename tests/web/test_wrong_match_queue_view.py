"""Direct tests for web/wrong_match_queue_view.py (#1278 extraction).

The queue projection's non-HTTP interface: ``build_wrong_match_groups``
drives a ``FakePipelineDB`` and recorder collaborators directly — no HTTP
server, no patches; path observation runs the REAL primitive against real
tmp directories. The /api/wrong-matches HTTP contract itself stays pinned
by tests/web/test_routes_imports.py.
"""
import datetime
import os
import shutil
import sys
import tempfile
import unittest
from typing import ClassVar

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row, seed_visible_wrong_match

# Module-qualified on purpose: importing the class itself would make
# unittest's loader (and pytest's collector) collect its tests a second
# time from this module's namespace.
from tests.web import test_routes_imports as _routes_contract
from web.wrong_match_queue_view import (
    _entry_sort_key,
    _latest_import_summary,
    _quality_summary,
    _row_presence,
    build_wrong_match_groups,
)

_MBID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
_RG_MBID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
# Second pressing for two-request worlds: the fake mirrors migration
# 001's plain UNIQUE on album_requests.mb_release_id (no status carve-out
# — even a replaced sibling may not reuse an MBID), so sibling requests
# need their own release identity.
_MBID_B = "cccccccc-cccc-cccc-cccc-cccccccccccc"


class _RankRecorder:
    """Recorder standing in for ``web.overlay.compute_library_rank``."""

    def __init__(self, result: str = "unknown") -> None:
        self.result = result
        self.calls: list[tuple[str | None, int | None]] = []

    def __call__(self, fmt: str | None, avg: int | None) -> str:
        self.calls.append((fmt, avg))
        return self.result


class _BeetsDetailRecorder:
    """Recorder standing in for ``WebRuntime.check_beets_library_detail``."""

    def __init__(self, result: dict[str, dict[str, object]] | None = None) -> None:
        self.result = result or {}
        self.calls: list[list[str]] = []

    def __call__(self, mbids: list[str]) -> dict[str, dict[str, object]]:
        self.calls.append(list(mbids))
        return self.result


class TestBuildWrongMatchGroupsInterface(unittest.TestCase):
    """The projection runs against explicit collaborators, no server."""

    def setUp(self) -> None:
        self.db = FakePipelineDB()
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)

    def _build(
        self,
        *,
        beets: _BeetsDetailRecorder | None = None,
        rank: _RankRecorder | None = None,
        include_replaced: bool = False,
    ) -> list[dict[str, object]]:
        return build_wrong_match_groups(
            db=self.db,
            check_beets_library_detail=beets or _BeetsDetailRecorder(),
            compute_library_rank=rank or _RankRecorder(),
            include_replaced=include_replaced,
        )

    def test_groups_assemble_without_http(self) -> None:
        seeded = seed_visible_wrong_match(self.db, self.root, request_id=7)
        groups = self._build()
        self.assertEqual(len(groups), 1)
        group = groups[0]
        self.assertEqual(group["request_id"], 7)
        entries = group["entries"]
        assert isinstance(entries, list)
        self.assertEqual(
            [e["download_log_id"] for e in entries],
            [seeded.download_log_id],
        )
        self.assertTrue(entries[0]["files_exist"])
        self.assertFalse(entries[0]["path_unavailable"])
        self.assertIsNone(entries[0]["import_job"])
        self.assertEqual(group["import_jobs"], [])

    def test_active_import_jobs_serialize_into_group_and_entry(self) -> None:
        """The job payloads carry the real serialized job, not a husk.

        Review survivor (#1278 wx4 mutant runner): nothing constrained
        the ``import_jobs``/``import_job`` field CONTENT — a mutant
        emitting ``{}`` at both sites outlived the whole suite.
        """
        seeded = seed_visible_wrong_match(self.db, self.root, request_id=7)
        job = self.db.enqueue_import_job(
            "force_import", request_id=7,
            dedupe_key=f"force_import:download_log:{seeded.download_log_id}",
            payload={"download_log_id": seeded.download_log_id,
                     "failed_path": seeded.path},
        )
        group = self._build()[0]
        jobs = group["import_jobs"]
        assert isinstance(jobs, list)
        self.assertEqual([j["id"] for j in jobs], [job.id])
        self.assertEqual(jobs[0]["job_type"], "force_import")
        entries = group["entries"]
        assert isinstance(entries, list)
        entry_job = entries[0]["import_job"]
        assert isinstance(entry_job, dict)
        self.assertEqual(entry_job["id"], job.id)
        self.assertEqual(entry_job["job_type"], "force_import")

    def test_beets_lookup_receives_exactly_the_row_mbids(self) -> None:
        seed_visible_wrong_match(self.db, self.root, request_id=7)
        beets = _BeetsDetailRecorder()
        self._build(beets=beets)
        self.assertEqual(beets.calls, [[_MBID]])

    def test_beets_lookup_skipped_when_no_row_has_an_mbid(self) -> None:
        self.db.seed_request(make_request_row(
            id=7, status="wanted", mb_release_id=None,
        ))
        seed_visible_wrong_match(self.db, self.root, request_id=7)
        beets = _BeetsDetailRecorder()
        groups = self._build(beets=beets)
        self.assertEqual(beets.calls, [])
        self.assertEqual(len(groups), 1)
        self.assertFalse(groups[0]["in_library"])

    def test_entry_rank_comes_from_injected_rank_producer(self) -> None:
        seed_visible_wrong_match(self.db, self.root, request_id=7)
        rank = _RankRecorder(result="lossless")
        groups = self._build(rank=rank)
        entries = groups[0]["entries"]
        assert isinstance(entries, list)
        self.assertEqual(entries[0]["quality_rank"], "lossless")
        # The seeded row carries no candidate evidence: the producer saw
        # the (None, None) evidence pair, proving the entry rank flows
        # through the injected callable rather than any module global.
        self.assertIn((None, None), rank.calls)

    def test_in_library_header_uses_beets_detail_and_injected_rank(self) -> None:
        seed_visible_wrong_match(self.db, self.root, request_id=7)
        beets = _BeetsDetailRecorder(result={_MBID: {
            "beets_format": "FLAC",
            "beets_bitrate": 900,
            "beets_avg_bitrate": 1000,
        }})
        rank = _RankRecorder(result="lossless")
        group = self._build(beets=beets, rank=rank)[0]
        self.assertTrue(group["in_library"])
        self.assertEqual(group["format"], "FLAC")
        self.assertEqual(group["min_bitrate"], 900)
        self.assertEqual(group["avg_bitrate"], 1000)
        self.assertEqual(group["quality_rank"], "lossless")
        self.assertIn(("FLAC", 1000), rank.calls)

    def test_replaced_request_hidden_by_default_shown_on_opt_in(self) -> None:
        self.db.seed_request(make_request_row(
            id=7, status="replaced", mb_release_id=_MBID,
        ))
        seed_visible_wrong_match(self.db, self.root, request_id=7)
        self.assertEqual(self._build(), [])
        shown = self._build(include_replaced=True)
        self.assertEqual([g["request_id"] for g in shown], [7])

    def test_latest_import_filled_from_history_batch(self) -> None:
        seed_visible_wrong_match(self.db, self.root, request_id=7)
        self.db.log_download(
            7, outcome="success", soulseek_username="goodpeer",
        )
        group = self._build()[0]
        latest = group["latest_import"]
        assert isinstance(latest, dict)
        self.assertEqual(latest["outcome"], "success")
        self.assertEqual(latest["soulseek_username"], "goodpeer")

    def test_proven_absent_entry_leaves_the_worklist(self) -> None:
        seeded = seed_visible_wrong_match(self.db, self.root, request_id=7)
        shutil.rmtree(seeded.path)
        self.assertEqual(self._build(), [])

    def test_group_payload_key_set_and_identity_fields(self) -> None:
        """The group dict carries exactly the contract keys, values intact.

        pending_count is the browser's visibility filter and mb ids feed
        the release links (web/js/wrong-matches.js); a renamed key leaves
        a stray sibling behind while the real reader goes blank (#1317
        mutmut survivors). Exact equality against the route contract's
        GROUP_REQUIRED_FIELDS — one hand-maintained key list, two
        strictness levels (the contract test checks presence, this pins
        the whole set).
        """
        self.db.seed_request(make_request_row(
            id=7, status="wanted",
            mb_release_id=_MBID, mb_release_group_id=_RG_MBID,
        ))
        seed_visible_wrong_match(self.db, self.root, request_id=7)
        group = self._build()[0]
        self.assertEqual(
            set(group.keys()),
            set(_routes_contract.TestWrongMatchesContract
                .GROUP_REQUIRED_FIELDS))
        self.assertEqual(group["mb_release_id"], _MBID)
        self.assertEqual(group["mb_release_group_id"], _RG_MBID)
        self.assertEqual(group["pending_count"], 1)
        self.assertIsNone(group["latest_import"])

    def test_skipped_replaced_row_does_not_stop_later_rows(self) -> None:
        """One filtered row must not end the scan — and the SIGNATURE
        default (replaced hidden) is exercised by OMITTING the kwarg,
        which no other caller in the repo does: the route always passes
        the query-derived value explicitly, and the route's own default
        is pinned separately in test_routes_imports (#1317 mutmut
        survivors: the include_replaced default flip and
        continue→break)."""
        self.db.seed_request(make_request_row(
            id=6, status="replaced", mb_release_id=_MBID_B,
        ))
        seed_visible_wrong_match(self.db, self.root, request_id=6)
        seed_visible_wrong_match(self.db, self.root, request_id=7)
        groups = build_wrong_match_groups(
            db=self.db,
            check_beets_library_detail=_BeetsDetailRecorder(),
            compute_library_rank=_RankRecorder(),
        )
        self.assertEqual([g["request_id"] for g in groups], [7])

    def test_proven_absent_row_does_not_stop_later_rows(self) -> None:
        self.db.seed_request(make_request_row(
            id=6, status="wanted", mb_release_id=_MBID_B,
        ))
        # Distinct folder name: the seeder's default path is SHARED, so
        # with the default both rows would point at one directory and
        # deleting request 6's folder would prove request 7's absent too.
        gone = seed_visible_wrong_match(
            self.db, self.root, request_id=6,
            name="Gone - Album (2024) [dead1234]",
        )
        seed_visible_wrong_match(self.db, self.root, request_id=7)
        shutil.rmtree(gone.path)
        groups = self._build()
        self.assertEqual([g["request_id"] for g in groups], [7])

    def test_entry_username_prefers_row_then_validation_result(self) -> None:
        """download_log.soulseek_username wins; the envelope is fallback.

        Both worlds are needed, for different mutants: the row-set world
        kills a wrong-key read (which silently falls through to the
        envelope) AND or→and on the chain (row-and-envelope yields the
        envelope); the row-null world proves the envelope fallback is
        consulted at all — a mutant deleting the ``or`` arm is invisible
        to the first world (#1317 mutmut survivors).
        """
        self.db.seed_request(make_request_row(
            id=6, status="wanted", mb_release_id=_MBID_B,
        ))
        seed_visible_wrong_match(
            self.db, self.root, request_id=6,
            name="Row - Peer (2024) [beef1234]",
            soulseek_username="rowpeer",
        )
        # World 2: the seeder leaves the row-level username unset, so its
        # entry must surface the envelope's "peer".
        seed_visible_wrong_match(self.db, self.root, request_id=7)
        by_request = {
            g["request_id"]: g["entries"] for g in self._build()
        }
        row_set = by_request[6]
        assert isinstance(row_set, list)
        self.assertEqual(row_set[0]["soulseek_username"], "rowpeer")
        row_null = by_request[7]
        assert isinstance(row_null, list)
        self.assertEqual(row_null[0]["soulseek_username"], "peer")


class TestEntrySortKey(unittest.TestCase):
    """Best-quality first; ties by distance asc then id desc."""

    CASES: ClassVar = [
        ("lossless outranks transparent",
         {"quality_rank": "lossless", "distance": 0.5, "download_log_id": 1},
         {"quality_rank": "transparent", "distance": 0.1, "download_log_id": 2}),
        ("equal rank breaks on distance asc",
         {"quality_rank": "good", "distance": 0.1, "download_log_id": 1},
         {"quality_rank": "good", "distance": 0.2, "download_log_id": 2}),
        ("equal rank+distance breaks on id desc",
         {"quality_rank": "good", "distance": 0.1, "download_log_id": 9},
         {"quality_rank": "good", "distance": 0.1, "download_log_id": 3}),
        ("missing distance sorts after any real distance",
         {"quality_rank": "good", "distance": 0.9, "download_log_id": 1},
         {"quality_rank": "good", "distance": None, "download_log_id": 2}),
        ("unknown rank string sorts below the rank table",
         {"quality_rank": "unknown", "distance": 0.1, "download_log_id": 1},
         {"quality_rank": "not-a-rank", "distance": 0.1, "download_log_id": 2}),
    ]

    def test_ordering_table(self) -> None:
        for desc, first, second in self.CASES:
            with self.subTest(desc=desc):
                self.assertLess(
                    _entry_sort_key(first), _entry_sort_key(second))

    def test_boolean_distance_is_not_a_number(self) -> None:
        # bool is an int subclass; the key must treat it as missing.
        entry = {"quality_rank": "good", "distance": True, "download_log_id": 1}
        self.assertEqual(_entry_sort_key(entry)[1], float("inf"))


class TestLatestImportSummary(unittest.TestCase):
    """Newest-first scan for the first success-shaped outcome."""

    def test_empty_history_is_none(self) -> None:
        self.assertIsNone(_latest_import_summary([]))

    def test_no_success_row_is_none(self) -> None:
        rows = [{"outcome": "rejected"}, {"outcome": "timeout"}]
        self.assertIsNone(_latest_import_summary(rows))

    def test_picks_first_success_shaped_row(self) -> None:
        rows = [
            {"outcome": "rejected", "id": 3},
            {"outcome": "force_import", "id": 2},
            {"outcome": "success", "id": 1},
        ]
        summary = _latest_import_summary(rows)
        assert summary is not None
        self.assertEqual(summary["id"], 2)
        self.assertEqual(summary["outcome"], "force_import")

    def test_datetime_created_at_renders_isoformat(self) -> None:
        stamp = datetime.datetime(
            2026, 8, 31, 12, 0, 0, tzinfo=datetime.UTC)
        summary = _latest_import_summary(
            [{"outcome": "success", "created_at": stamp}])
        assert summary is not None
        self.assertEqual(summary["created_at"], stamp.isoformat())

    def test_string_created_at_passes_through(self) -> None:
        summary = _latest_import_summary(
            [{"outcome": "success", "created_at": "2026-08-31"}])
        assert summary is not None
        self.assertEqual(summary["created_at"], "2026-08-31")

    def test_summary_payload_is_exactly_its_declared_fields(self) -> None:
        """Full-dict pin: the browser renders actual_filetype /
        actual_min_bitrate by name (web/js/wrong-matches.js), so a
        renamed or wrong-key field ships green without this equality
        (#1317 mutmut survivors)."""
        stamp = datetime.datetime(
            2026, 8, 31, 12, 0, 0, tzinfo=datetime.UTC)
        summary = _latest_import_summary([{
            "id": 41, "outcome": "success", "created_at": stamp,
            "soulseek_username": "peer", "actual_filetype": "flac",
            "actual_min_bitrate": 900, "beets_scenario": "clean",
            "download_path": "ignored-by-the-summary",
        }])
        self.assertEqual(summary, {
            "id": 41,
            "outcome": "success",
            "created_at": stamp.isoformat(),
            "soulseek_username": "peer",
            "actual_filetype": "flac",
            "actual_min_bitrate": 900,
            "beets_scenario": "clean",
        })


class TestQualitySummary(unittest.TestCase):
    """Full-dict pins for the group-header quality block.

    verified_lossless, min_bitrate and current_spectral_bitrate are
    browser-consumed by name (web/js/wrong-matches.js); the falsy
    verified_lossless world and the request-floor min_bitrate fallback
    had no assertion at all (#1317 mutmut survivors).
    """

    def test_exact_presence_with_unmeasured_beets_detail_full_payload(
            self) -> None:
        rank = _RankRecorder(result="lossless")
        row = {
            "request_status": "wanted",
            "mb_release_id": _MBID,
            "request_min_bitrate": 320,
            "request_verified_lossless": None,
            "request_current_spectral_grade": "genuine",
            "request_current_spectral_bitrate": 999,
        }
        # The producer (BeetsDB.check_mbids_detail) always returns all
        # six keys — a bare {} is a shape it never emits and would
        # short-circuit the guarded reads this pin exists to exercise.
        unmeasured_detail: dict[str, object] = {
            "beets_tracks": 10,
            "beets_format": None,
            "beets_bitrate": None,
            "beets_avg_bitrate": None,
            "beets_samplerate": None,
            "beets_bitdepth": None,
        }
        summary = _quality_summary(
            row, {_MBID: unmeasured_detail}, "exact", rank)
        self.assertEqual(summary, {
            "status": "wanted",
            "min_bitrate": 320,
            "avg_bitrate": None,
            "format": None,
            "verified_lossless": False,
            "current_spectral_grade": "genuine",
            "current_spectral_bitrate": 999,
            "current_spectral_accusation_admissible": None,
            "current_spectral_accusation_withheld": None,
            "quality_label": None,
            "quality_rank": None,
        })
        # No format → the injected rank producer is never consulted.
        self.assertEqual(rank.calls, [])

    def test_absent_presence_full_payload(self) -> None:
        rank = _RankRecorder(result="lossless")
        summary = _quality_summary(
            {"request_status": "processing"}, {}, "absent", rank)
        self.assertEqual(summary, {
            "status": "processing",
            "min_bitrate": None,
            "avg_bitrate": None,
            "format": None,
            "verified_lossless": False,
            "current_spectral_grade": None,
            "current_spectral_bitrate": None,
            "current_spectral_accusation_admissible": None,
            "current_spectral_accusation_withheld": None,
            "quality_label": None,
            "quality_rank": None,
        })
        self.assertEqual(rank.calls, [])


class TestRowPresence(unittest.TestCase):
    """'In library' means exact-ID hit in the batched lookup, period."""

    def test_exact_hit(self) -> None:
        self.assertEqual(
            _row_presence({"mb_release_id": _MBID}, {_MBID: {}}), "exact")

    def test_miss_and_missing_and_nonstring_are_absent(self) -> None:
        for desc, row in (
            ("lookup miss", {"mb_release_id": "other"}),
            ("mbid missing", {}),
            ("mbid empty", {"mb_release_id": ""}),
            ("mbid non-string", {"mb_release_id": 42}),
        ):
            with self.subTest(desc=desc):
                self.assertEqual(_row_presence(row, {_MBID: {}}), "absent")


if __name__ == "__main__":
    unittest.main()
