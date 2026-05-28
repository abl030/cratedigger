import unittest

import msgspec

from lib.disk_coverage_service import disk_coverage
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import make_request_row


class TestDiskCoverageService(unittest.TestCase):
    def test_counts_off_disk_active_rows_by_exact_beets_identity(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="imported", mb_release_id="mbid-on-disk"))
        db.seed_request(make_request_row(
            id=2, status="wanted", mb_release_id="mbid-missing"))
        db.seed_request(make_request_row(
            id=3, status="downloading", mb_release_id=None,
            discogs_release_id="12856590"))
        db.seed_request(make_request_row(
            id=4, status="replaced", mb_release_id="old-mbid"))
        beets = FakeBeetsDB()
        beets.set_album_exists("mbid-on-disk", True)
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
            beets.check_mbids_calls,
            [["mbid-on-disk", "mbid-missing", "12856590"]],
        )

    def test_counts_only_suppresses_rows(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, mb_release_id="missing"))
        beets = FakeBeetsDB()

        result = disk_coverage(db, beets, include_rows=False)

        self.assertEqual(result.counts.off_disk_total, 1)
        self.assertIsNone(result.off_disk)

    def test_include_inverse_lists_beets_albums_without_active_pipeline_row(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, mb_release_id="tracked-mbid"))
        db.seed_request(make_request_row(
            id=2, status="replaced", mb_release_id="old-mbid"))
        beets = FakeBeetsDB()
        beets.set_album_exists("tracked-mbid", True)
        beets.set_release_identities([
            {
                "id": 10,
                "album": "Tracked",
                "albumartist": "Artist",
                "mb_albumid": "tracked-mbid",
                "discogs_albumid": None,
            },
            {
                "id": 11,
                "album": "Long Tail",
                "albumartist": "Artist",
                "mb_albumid": "untracked-mbid",
                "discogs_albumid": None,
            },
            {
                "id": 12,
                "album": "Old",
                "albumartist": "Artist",
                "mb_albumid": "old-mbid",
                "discogs_albumid": None,
            },
        ])

        result = disk_coverage(db, beets, include_inverse=True)

        assert result.inverse is not None
        self.assertEqual([row.id for row in result.inverse], [11, 12])
        self.assertEqual(result.counts.inverse_total, 2)

    def test_result_is_json_serializable(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, mb_release_id="missing"))

        payload = msgspec.to_builtins(disk_coverage(db, FakeBeetsDB()))

        self.assertEqual(payload["counts"]["off_disk_total"], 1)
        self.assertEqual(payload["off_disk"][0]["id"], 1)


if __name__ == "__main__":
    unittest.main()
