import unittest
from typing import TYPE_CHECKING

import msgspec

from lib.beets_db import CurrentBeetsResolution
from lib.disk_coverage_service import (
    DiskCoverageAmbiguousResolution,
    disk_coverage,
)
from lib.release_identity import ReleaseIdentity
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import make_request_row


class TestDiskCoverageService(unittest.TestCase):
    def test_classifies_exact_identities_in_one_batched_resolution(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="imported",
            mb_release_id="00000000-0000-4000-8000-000000000001",
        ))
        db.seed_request(make_request_row(
            id=2, status="wanted",
            mb_release_id="00000000-0000-4000-8000-000000000002",
        ))
        db.seed_request(make_request_row(
            id=3, status="downloading", mb_release_id="12856590",
            discogs_release_id="12856590",
        ))
        db.seed_request(make_request_row(
            id=4, status="replaced",
            mb_release_id="00000000-0000-4000-8000-000000000004",
        ))

        class RecordingBeetsDB(FakeBeetsDB):
            def __init__(self) -> None:
                super().__init__()
                self.batch_calls: list[list[ReleaseIdentity]] = []

            def resolve_current_releases(
                self, identities: list[ReleaseIdentity],
            ) -> dict[ReleaseIdentity, CurrentBeetsResolution]:
                self.batch_calls.append(identities)
                return super().resolve_current_releases(identities)

        beets = RecordingBeetsDB()
        beets.set_album_exists("00000000-0000-4000-8000-000000000001", True)
        beets.set_album_exists("12856590", True)

        result = disk_coverage(db, beets)

        self.assertEqual(result.counts.active_total, 3)
        self.assertEqual(result.counts.on_disk_total, 2)
        self.assertEqual(result.counts.off_disk_total, 1)
        self.assertEqual(result.counts.by_status, {
            "downloading": 1,
            "imported": 1,
            "wanted": 1,
        })
        self.assertEqual(result.counts.off_disk_by_status, {"wanted": 1})
        assert result.off_disk is not None
        self.assertEqual([row.id for row in result.off_disk], [2])
        self.assertEqual(
            [identity.release_id for identity in beets.batch_calls[0]],
            [
                "00000000-0000-4000-8000-000000000001",
                "00000000-0000-4000-8000-000000000002",
                "12856590",
            ],
        )
        self.assertEqual(len(beets.batch_calls), 1)

    def test_off_disk_rows_preserve_missing_and_ambiguous_resolutions(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="imported",
            mb_release_id="00000000-0000-4000-8000-000000000001",
        ))
        db.seed_request(make_request_row(
            id=2, status="wanted",
            mb_release_id="00000000-0000-4000-8000-000000000002",
        ))
        db.seed_request(make_request_row(
            id=3, status="imported",
            mb_release_id="00000000-0000-4000-8000-000000000003",
        ))
        db.seed_request(make_request_row(
            id=4, status="imported",
            mb_release_id="00000000-0000-4000-8000-000000000004",
            discogs_release_id="42",
        ))
        beets = FakeBeetsDB()
        beets.set_album_exists("00000000-0000-4000-8000-000000000001", True)
        beets.set_album_ids_for_release(
            "00000000-0000-4000-8000-000000000003", [77, 91],
        )

        result = disk_coverage(db, beets)

        self.assertEqual(result.counts.on_disk_total, 1)
        self.assertEqual(result.counts.off_disk_total, 3)
        self.assertEqual(result.counts.off_disk_by_status, {
            "imported": 2, "wanted": 1,
        })
        assert result.off_disk is not None
        by_id = {row.id: row for row in result.off_disk}
        self.assertEqual(by_id[2].resolution.kind, "missing")
        self.assertEqual(by_id[4].resolution.kind, "missing")
        self.assertIsNone(by_id[4].source)
        ambiguous = by_id[3].resolution
        assert isinstance(ambiguous, DiskCoverageAmbiguousResolution)
        self.assertEqual(ambiguous.album_ids, (77, 91))
        self.assertEqual(ambiguous.reason, "multiple_matches")

    def test_off_disk_rows_carry_the_real_exact_release_source(self) -> None:
        """#1089 MAJOR-A (review round 3): ``source`` is derived from the
        VALUE's shape, never column presence — production Discogs rows
        duplicate the numeric id into BOTH ``mb_release_id`` and
        ``discogs_release_id`` (``ReleaseIdentity.from_strict_fields``'s own
        docstring), so a column-truthiness gate would misclassify every one
        of them as MusicBrainz-sourced.

        #1089 N4 (review round 4): the derivation is STRICT
        (``from_strict_fields``), not the lenient ``from_fields`` — a row
        with a real MB UUID PLUS a conflicting, different numeric Discogs
        id must resolve to no source at all, exactly matching
        ``MergeRekeyService.rekey_request``'s own admission test. The
        lenient derivation would have picked ``mb_release_id`` and shown a
        button the service refuses.
        """
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="wanted",
            mb_release_id="d990b8af-01db-46f1-a2cb-d9ca19f57e94",
        ))
        db.seed_request(make_request_row(
            id=2, status="wanted",
            # The real production shape: the numeric Discogs id duplicated
            # into BOTH columns.
            mb_release_id="12856590", discogs_release_id="12856590",
        ))
        db.seed_request(make_request_row(
            id=3, status="wanted", mb_release_id=None,
            discogs_release_id=None,
        ))
        db.seed_request(make_request_row(
            id=4, status="wanted",
            # A real MB UUID plus a CONFLICTING, different numeric Discogs
            # id — the service's own from_strict_fields admission test
            # fails this closed to None; the lenient from_fields would
            # have picked mb_release_id and shown a button the service
            # refuses (#1089 N4).
            mb_release_id="e1000000-0000-0000-0000-000000000000",
            discogs_release_id="99999999",
        ))
        beets = FakeBeetsDB()

        result = disk_coverage(db, beets)

        assert result.off_disk is not None
        by_id = {row.id: row.source for row in result.off_disk}
        self.assertEqual(by_id, {
            1: "musicbrainz",
            2: "discogs",
            3: None,
            4: None,
        })

    def test_counts_only_suppresses_rows(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, mb_release_id="00000000-0000-4000-8000-000000000001",
        ))
        beets = FakeBeetsDB()

        result = disk_coverage(db, beets, include_rows=False)

        self.assertEqual(result.counts.off_disk_total, 1)
        self.assertIsNone(result.off_disk)

    def test_include_inverse_lists_beets_albums_without_active_pipeline_row(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, mb_release_id="00000000-0000-4000-8000-000000000001",
        ))
        db.seed_request(make_request_row(
            id=2, status="replaced",
            mb_release_id="00000000-0000-4000-8000-000000000002",
        ))
        beets = FakeBeetsDB()
        beets.set_album_exists("00000000-0000-4000-8000-000000000001", True)
        beets.set_release_identities([
            {
                "id": 10,
                "album": "Tracked",
                "albumartist": "Artist",
                "mb_albumid": "00000000-0000-4000-8000-000000000001",
                "discogs_albumid": None,
            },
            {
                "id": 11,
                "album": "Long Tail",
                "albumartist": "Artist",
                "mb_albumid": "00000000-0000-4000-8000-000000000003",
                "discogs_albumid": None,
            },
            {
                "id": 12,
                "album": "Old",
                "albumartist": "Artist",
                "mb_albumid": "00000000-0000-4000-8000-000000000002",
                "discogs_albumid": None,
            },
        ])

        result = disk_coverage(db, beets, include_inverse=True)

        assert result.inverse is not None
        self.assertEqual([row.id for row in result.inverse], [11, 12])
        self.assertEqual(result.counts.inverse_total, 2)

    def test_result_is_json_serializable(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, mb_release_id="00000000-0000-4000-8000-000000000001",
        ))

        payload = msgspec.to_builtins(disk_coverage(db, FakeBeetsDB()))

        self.assertEqual(payload["counts"]["off_disk_total"], 1)
        self.assertEqual(payload["off_disk"][0]["id"], 1)


if TYPE_CHECKING:
    from typing import cast

    from lib.beets_db import BeetsDB
    from lib.disk_coverage_service import (
        DiskCoverageBeetsDB as _CovBeetsDB,
    )
    from lib.disk_coverage_service import (
        DiskCoveragePipelineDB as _CovPipelineDB,
    )
    from lib.pipeline_db import PipelineDB

    # Static parity proof (#409) — see the matching block in
    # tests/test_wrong_match_cleanup_service.py for the rationale.
    _pipeline_db_satisfies_coverage_protocol: _CovPipelineDB = cast("PipelineDB", None)
    _fake_db_satisfies_coverage_protocol: _CovPipelineDB = cast("FakePipelineDB", None)
    _beets_db_satisfies_coverage_protocol: _CovBeetsDB = cast("BeetsDB", None)
    _fake_beets_satisfies_coverage_protocol: _CovBeetsDB = cast("FakeBeetsDB", None)


class TestDiskCoverageDBProtocolParity(unittest.TestCase):
    """#409: both impl pairs must satisfy the disk-coverage protocols."""

    def test_pipeline_impls_satisfy_protocol(self) -> None:
        from lib.disk_coverage_service import DiskCoveragePipelineDB
        from lib.pipeline_db import PipelineDB

        self.assertTrue(issubclass(PipelineDB, DiskCoveragePipelineDB))
        self.assertTrue(issubclass(FakePipelineDB, DiskCoveragePipelineDB))

    def test_beets_impls_satisfy_protocol(self) -> None:
        from lib.beets_db import BeetsDB
        from lib.disk_coverage_service import DiskCoverageBeetsDB

        self.assertTrue(issubclass(BeetsDB, DiskCoverageBeetsDB))
        self.assertTrue(issubclass(FakeBeetsDB, DiskCoverageBeetsDB))


if __name__ == "__main__":
    unittest.main()
