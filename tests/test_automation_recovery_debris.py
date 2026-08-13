"""Orchestration tests for recovery-side Beets crash-debris removal (#1089)."""

from __future__ import annotations

import unittest

from lib.automation_recovery_debris import (
    RecoveryDebrisReport,
    remove_recovery_debris,
)
from lib.beets_delete import BeetsDeleteCompleted, BeetsDeleteFailed, BeetsDeleteRequest
from tests.fakes.beets import FakeBeetsDB

RELEASE = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
SOURCE = "/mnt/virtio/cratedigger/processing/albums/8df204a3"


def _detail(*paths: str) -> dict[str, object]:
    return {"tracks": [{"path": path} for path in paths]}


class _RecordingBeetsDbFactory:
    """Yields the same seeded ``FakeBeetsDB`` and records close()."""

    def __init__(self, beets_db: FakeBeetsDB) -> None:
        self.beets_db = beets_db
        self.calls = 0

    def __call__(self) -> FakeBeetsDB:
        self.calls += 1
        return self.beets_db


class _StubBeetsDelete:
    def __init__(self, outcome) -> None:
        self.outcome = outcome
        self.requests: list[BeetsDeleteRequest] = []

    def __call__(self, request: BeetsDeleteRequest):
        self.requests.append(request)
        return self.outcome


class TestRemoveRecoveryDebris(unittest.TestCase):
    def _beets(self) -> FakeBeetsDB:
        return FakeBeetsDB(
            library_db_path="/mnt/virtio/cratedigger/beets-db/beets-library.db",
            library_root="/mnt/virtio/Music/Beets",
        )

    def test_no_launch_short_circuits_without_opening_beets(self) -> None:
        opened = 0

        def factory() -> FakeBeetsDB:
            nonlocal opened
            opened += 1
            return self._beets()

        report = remove_recovery_debris(
            launch_release_id=None,
            launch_source_path=None,
            beets_db_factory=factory,
        )

        self.assertEqual(report, RecoveryDebrisReport(outcome="no_launch"))
        self.assertEqual(opened, 0)

    def test_not_found_when_no_beets_album_matches_the_release(self) -> None:
        beets = self._beets()
        report = remove_recovery_debris(
            launch_release_id=RELEASE,
            launch_source_path=SOURCE,
            beets_db_factory=_RecordingBeetsDbFactory(beets),
        )

        self.assertEqual(report, RecoveryDebrisReport(outcome="not_found"))

    def test_not_confined_when_item_is_outside_launch_source(self) -> None:
        beets = self._beets()
        beets.set_album_ids_for_release(RELEASE, [19823])
        beets.set_album_detail(19823, _detail(f"{SOURCE}/01.flac", "/mnt/virtio/Music/Beets/Artist/Album/01.flac"))

        report = remove_recovery_debris(
            launch_release_id=RELEASE,
            launch_source_path=SOURCE,
            beets_db_factory=_RecordingBeetsDbFactory(beets),
        )

        self.assertEqual(report.outcome, "not_confined")
        self.assertIn(f"{SOURCE}/01.flac", report.item_paths)

    def test_not_confined_when_candidate_has_no_items(self) -> None:
        beets = self._beets()
        beets.set_album_ids_for_release(RELEASE, [19823])
        beets.set_album_detail(19823, _detail())

        report = remove_recovery_debris(
            launch_release_id=RELEASE,
            launch_source_path=SOURCE,
            beets_db_factory=_RecordingBeetsDbFactory(beets),
        )

        self.assertEqual(report.outcome, "not_confined")

    def test_ambiguous_when_two_candidates_are_both_confined(self) -> None:
        beets = self._beets()
        beets.set_album_ids_for_release(RELEASE, [19823, 19824])
        beets.set_album_detail(19823, _detail(f"{SOURCE}/01.flac"))
        beets.set_album_detail(19824, _detail(f"{SOURCE}/02.flac"))

        report = remove_recovery_debris(
            launch_release_id=RELEASE,
            launch_source_path=SOURCE,
            beets_db_factory=_RecordingBeetsDbFactory(beets),
        )

        self.assertEqual(report.outcome, "ambiguous")

    def test_removed_calls_the_admitted_delete_lane_with_debris_confinement(self) -> None:
        beets = self._beets()
        beets.set_album_ids_for_release(RELEASE, [19823])
        beets.set_album_detail(19823, _detail(f"{SOURCE}/01.flac", f"{SOURCE}/02.flac"))
        stub = _StubBeetsDelete(BeetsDeleteCompleted(
            album_id=19823, album_name="Frozen", artist_name="Idina Menzel",
            former_album_path=SOURCE, deleted_tracks=0, deleted_artifacts=0,
            preserved_paths=(), metadata_only=True,
        ))

        report = remove_recovery_debris(
            launch_release_id=RELEASE,
            launch_source_path=SOURCE,
            beets_db_factory=_RecordingBeetsDbFactory(beets),
            beets_delete_fn=stub,
        )

        self.assertEqual(report.outcome, "removed")
        self.assertEqual(report.album_id, 19823)
        self.assertCountEqual(report.item_paths, (f"{SOURCE}/01.flac", f"{SOURCE}/02.flac"))
        # Issue #1089 review m6: propagated straight from the admitted
        # delete lane's own outcome, not hardcoded — proven below by a
        # planted mutant on the propagation, not merely asserted here.
        self.assertTrue(report.metadata_only)
        self.assertEqual(len(stub.requests), 1)
        request = stub.requests[0]
        self.assertEqual(request.album_id, 19823)
        self.assertEqual(request.expected_release_id, RELEASE)
        self.assertEqual(request.library_db_path, beets.library_db_path)
        self.assertEqual(request.library_root, beets.library_root)
        self.assertEqual(request.debris_confinement_root, SOURCE)

    def test_removed_metadata_only_is_propagated_not_hardcoded(self) -> None:
        """Issue #1089 review m6: feed the admitted lane's own outcome with
        ``metadata_only=False`` and confirm the report reflects it — proves
        this is a genuine field propagation, not a hardcoded ``True``
        (production's own delete request always sets
        ``debris_confinement_root``, so a real ``removed`` outcome is
        always ``metadata_only=True`` in practice; this test isolates the
        propagation itself from that production-side invariant)."""
        beets = self._beets()
        beets.set_album_ids_for_release(RELEASE, [19823])
        beets.set_album_detail(19823, _detail(f"{SOURCE}/01.flac"))
        stub = _StubBeetsDelete(BeetsDeleteCompleted(
            album_id=19823, album_name="Frozen", artist_name="Idina Menzel",
            former_album_path=SOURCE, deleted_tracks=0, deleted_artifacts=0,
            preserved_paths=(), metadata_only=False,
        ))

        report = remove_recovery_debris(
            launch_release_id=RELEASE,
            launch_source_path=SOURCE,
            beets_db_factory=_RecordingBeetsDbFactory(beets),
            beets_delete_fn=stub,
        )

        self.assertEqual(report.outcome, "removed")
        self.assertFalse(report.metadata_only)

    def test_removal_failed_surfaces_the_delete_lane_reason(self) -> None:
        beets = self._beets()
        beets.set_album_ids_for_release(RELEASE, [19823])
        beets.set_album_detail(19823, _detail(f"{SOURCE}/01.flac"))
        stub = _StubBeetsDelete(BeetsDeleteFailed(
            album_id=19823, reason="postcondition_failed",
            detail="Beets album row survived metadata removal",
            album_still_present=True,
        ))

        report = remove_recovery_debris(
            launch_release_id=RELEASE,
            launch_source_path=SOURCE,
            beets_db_factory=_RecordingBeetsDbFactory(beets),
            beets_delete_fn=stub,
        )

        self.assertEqual(report.outcome, "removal_failed")
        self.assertEqual(report.album_id, 19823)
        self.assertIn("postcondition_failed", report.detail)

    def test_beets_unavailable_is_reported_never_raised(self) -> None:
        """A known-expected Beets-unavailability failure (issue #1089's fix
        for the wide test blast radius, and CLAUDE.md invariant 11: recovery
        must keep moving) is reported, not propagated."""

        def factory():
            raise FileNotFoundError("Beets DB not found: /nonexistent.db")

        report = remove_recovery_debris(
            launch_release_id=RELEASE,
            launch_source_path=SOURCE,
            beets_db_factory=factory,
        )

        self.assertEqual(report.outcome, "beets_unavailable")
        self.assertIn("FileNotFoundError", report.detail)

    def test_unclassified_beets_failure_still_propagates(self) -> None:
        """Only KNOWN unavailability categories are swallowed — an
        unclassified exception is a genuine unexpected failure and must
        still surface (never silently treated as 'nothing to remove')."""

        class _Unclassified(RuntimeError):
            pass

        def factory():
            raise _Unclassified("planted unexpected failure")

        with self.assertRaises(_Unclassified):
            remove_recovery_debris(
                launch_release_id=RELEASE,
                launch_source_path=SOURCE,
                beets_db_factory=factory,
            )


if __name__ == "__main__":
    unittest.main()
