"""Deterministic pins for the cross-engine world invariant bank (#743)."""

from __future__ import annotations

import os
import tempfile
import unittest

from lib.world_invariants import (
    DenylistAuthoritySnapshot,
    EvidenceDiskSnapshot,
    LifecycleTransitionSnapshot,
    LibraryAlbumSnapshot,
    RequestMembershipSnapshot,
    assert_replaced_row_frozen,
    check_denylist_authority,
    check_evidence_disk_coherence,
    check_folder_exclusivity,
    check_library_filesystem,
    check_no_lossy_tier_widening,
    check_proof_lock_terminality,
    check_status_membership,
    derive_denylist_authorities,
)


class TestWorldInvariantPins(unittest.TestCase):
    def test_distinct_folders_and_imported_membership_are_coherent(self) -> None:
        albums = (
            LibraryAlbumSnapshot(
                album_id=1,
                release_id="release-a",
                album_path="/library/Artist/2001 - Album",
                item_paths=(
                    "/library/Artist/2001 - Album/01 First.flac",
                    "/library/Artist/2001 - Album/02 Second.flac",
                ),
            ),
            LibraryAlbumSnapshot(
                album_id=2,
                release_id="release-b",
                album_path="/library/Artist/2001 - Album [2002]",
                item_paths=(
                    "/library/Artist/2001 - Album [2002]/01 First.mp3",
                ),
            ),
        )
        requests = (
            RequestMembershipSnapshot(
                request_id=10,
                release_id="release-a",
                status="imported",
                imported_path="/library/Artist/2001 - Album",
            ),
            # Backfill/upgrade worlds legitimately remain wanted while an
            # exact pressing is already installed.
            RequestMembershipSnapshot(
                request_id=11,
                release_id="release-b",
                status="wanted",
                imported_path="/library/Artist/2001 - Album [2002]",
            ),
        )

        self.assertEqual(check_folder_exclusivity(albums), ())
        self.assertEqual(check_status_membership(requests, albums), ())

    def test_evidence_proof_policy_and_authority_are_coherent(self) -> None:
        evidence = EvidenceDiskSnapshot(
            request_id=10,
            release_id="release-a",
            status="imported",
            album_path="/library/A",
            current_evidence_id=4,
            evidence_id=4,
            evidence_release_id="release-a",
            evidence_source_path="/library/A",
            evidence_fingerprint="sha256:a",
            actual_fingerprint="sha256:a",
        )
        transition = LifecycleTransitionSnapshot(
            request_id=10,
            operation="force_import",
            before_status="imported",
            after_status="imported",
            before_release_id="release-a",
            after_release_id="release-a",
            before_override=None,
            after_override=None,
            before_album_fingerprint="sha256:a",
            after_album_fingerprint="sha256:a",
            before_verified_lossless=True,
        )
        authority = DenylistAuthoritySnapshot(
            request_id=10,
            username="peer",
            authorizing_decisions=("curator_ban",),
        )

        self.assertEqual(check_evidence_disk_coherence((evidence,)), ())
        self.assertEqual(check_proof_lock_terminality((transition,)), ())
        self.assertEqual(check_no_lossy_tier_widening((transition,)), ())
        self.assertEqual(check_denylist_authority((authority,)), ())

    def test_denylist_authority_is_derived_from_persisted_decisions(self) -> None:
        self.assertEqual(
            derive_denylist_authorities(
                username="peer",
                reason="manual curator action",
                history=[{
                    "outcome": "curator_ban",
                    "soulseek_username": "peer",
                }],
            ),
            ("curator_ban",),
        )
        self.assertEqual(
            derive_denylist_authorities(
                username="quality-peer",
                reason="quality gate: lossless-only candidate rejected",
                history=[],
            ),
            ("requeue_lossless",),
        )


class TestWorldInvariantCheckersTripOnKnownBad(unittest.TestCase):
    @staticmethod
    def _transition(**overrides: object) -> LifecycleTransitionSnapshot:
        fields: dict[str, object] = {
            "request_id": 10,
            "operation": "upgrade_import",
            "before_status": "imported",
            "after_status": "wanted",
            "before_release_id": "release-a",
            "after_release_id": "release-a",
            "before_override": "lossless",
            "after_override": None,
            "before_album_fingerprint": "sha256:before",
            "after_album_fingerprint": "sha256:after",
            "before_verified_lossless": True,
        }
        fields.update(overrides)
        return LifecycleTransitionSnapshot(**fields)  # type: ignore[arg-type]

    def test_folder_checker_trips_on_passenger_shared_folder(self) -> None:
        folder = "/library/Lisa Hannigan/2011 - Passenger"
        violations = check_folder_exclusivity((
            LibraryAlbumSnapshot(
                album_id=1,
                release_id="old-pressing",
                album_path=folder,
                item_paths=(f"{folder}/01 Home.flac",),
            ),
            LibraryAlbumSnapshot(
                album_id=2,
                release_id="new-pressing",
                album_path=folder,
                item_paths=(f"{folder}/02 Passenger.mp3",),
            ),
        ))

        self.assertIn("folder_shared", {v.code for v in violations})

    def test_folder_checker_trips_when_item_escapes_album_folder(self) -> None:
        violations = check_folder_exclusivity((
            LibraryAlbumSnapshot(
                album_id=1,
                release_id="release-a",
                album_path="/library/Artist/Album",
                item_paths=("/library/Artist/Other/01 Track.flac",),
            ),
        ))

        self.assertIn("item_outside_album_folder", {v.code for v in violations})

    def test_folder_checker_trips_on_empty_album(self) -> None:
        violations = check_folder_exclusivity((
            LibraryAlbumSnapshot(
                album_id=1,
                release_id="release-a",
                album_path="/library/Artist/Album",
                item_paths=(),
            ),
        ))

        self.assertIn("album_empty", {v.code for v in violations})

    def test_filesystem_checker_trips_on_missing_folder_and_item(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            missing_folder = os.path.join(tmpdir, "missing-album")
            violations = check_library_filesystem((LibraryAlbumSnapshot(
                album_id=1,
                release_id="release-a",
                album_path=missing_folder,
                item_paths=(os.path.join(missing_folder, "01 Track.flac"),),
            ),))

        self.assertEqual(
            {v.code for v in violations},
            {"album_folder_missing", "album_item_missing"},
        )

    def test_replaced_checker_trips_on_thawed_audit_row(self) -> None:
        before = {"id": 41, "status": "replaced", "updated_at": "t0"}
        after = {"id": 41, "status": "wanted", "updated_at": "t1"}

        with self.assertRaisesRegex(AssertionError, "mutated after supersede"):
            assert_replaced_row_frozen(before, after)

    def test_membership_checker_trips_on_missing_imported_release(self) -> None:
        violations = check_status_membership((
            RequestMembershipSnapshot(
                request_id=10,
                release_id="missing-release",
                status="imported",
                imported_path="/library/Artist/Album",
            ),
        ), ())

        self.assertIn("imported_release_missing", {v.code for v in violations})

    def test_membership_checker_trips_on_duplicate_exact_release(self) -> None:
        albums = (
            LibraryAlbumSnapshot(1, "release-a", "/library/A", ("/library/A/1.flac",)),
            LibraryAlbumSnapshot(2, "release-a", "/library/B", ("/library/B/1.flac",)),
        )
        violations = check_status_membership((
            RequestMembershipSnapshot(10, "release-a", "imported", "/library/A"),
        ), albums)

        self.assertIn("imported_release_duplicate", {v.code for v in violations})

    def test_membership_checker_trips_on_imported_path_drift(self) -> None:
        albums = (
            LibraryAlbumSnapshot(1, "release-a", "/library/Actual", ("/library/Actual/1.flac",)),
        )
        violations = check_status_membership((
            RequestMembershipSnapshot(10, "release-a", "imported", "/library/Stale"),
        ), albums)

        self.assertIn("imported_path_mismatch", {v.code for v in violations})

    def test_evidence_checker_trips_on_stale_disk_fingerprint(self) -> None:
        violations = check_evidence_disk_coherence((EvidenceDiskSnapshot(
            request_id=10,
            release_id="release-a",
            status="imported",
            album_path="/library/A",
            current_evidence_id=4,
            evidence_id=4,
            evidence_release_id="release-a",
            evidence_source_path="/library/A",
            evidence_fingerprint="sha256:stale",
            actual_fingerprint="sha256:current",
        ),))

        self.assertIn("evidence_fingerprint_mismatch", {v.code for v in violations})

    def test_evidence_checker_trips_on_dangling_link(self) -> None:
        violations = check_evidence_disk_coherence((EvidenceDiskSnapshot(
            request_id=10,
            release_id="release-a",
            status="wanted",
            album_path="/library/A",
            current_evidence_id=99,
            evidence_id=None,
            evidence_release_id=None,
            evidence_source_path=None,
            evidence_fingerprint=None,
            actual_fingerprint="sha256:current",
        ),))

        self.assertIn("current_evidence_dangling", {v.code for v in violations})

    def test_proof_checker_trips_on_replaced_bytes(self) -> None:
        violations = check_proof_lock_terminality((self._transition(),))

        self.assertIn("proof_lock_broken", {v.code for v in violations})

    def test_search_checker_trips_on_lossless_to_full_widening(self) -> None:
        violations = check_no_lossy_tier_widening((self._transition(
            before_verified_lossless=False,
        ),))

        self.assertIn("lossy_tier_widened", {v.code for v in violations})

    def test_denylist_checker_trips_without_decision_authority(self) -> None:
        violations = check_denylist_authority((DenylistAuthoritySnapshot(
            request_id=10,
            username="peer",
        ),))

        self.assertIn("denylist_without_authority", {v.code for v in violations})


if __name__ == "__main__":
    unittest.main()
