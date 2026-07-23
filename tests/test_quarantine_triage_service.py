"""Deterministic contracts for the read-only quarantine lifecycle surface."""

from __future__ import annotations

import os
import tempfile
import unittest
from typing import TYPE_CHECKING
from unittest.mock import patch

from lib.quarantine_triage_service import (
    QuarantineScanError,
    list_unreferenced_quarantine_folders,
)
from tests.fakes import FakePipelineDB

if TYPE_CHECKING:
    from lib.pipeline_db import WrongMatchCandidateRow


def _seed_wrong_match(
    db: FakePipelineDB,
    failed_path: str,
    *,
    request_status: str = "wanted",
) -> None:
    request_id = db.add_request(
        artist_name="Referenced",
        album_title=failed_path,
        source="request",
        status=request_status,
    )
    db.log_download(
        request_id,
        outcome="rejected",
        validation_result={
            "failed_path": failed_path,
            "scenario": "high_distance",
        },
    )


class _FailingWrongMatchesDB(FakePipelineDB):
    def get_wrong_matches(self) -> "list[WrongMatchCandidateRow]":
        raise RuntimeError("database unavailable")


class TestQuarantineTriageService(unittest.TestCase):
    def test_lists_only_unreferenced_immediate_album_folders(self) -> None:
        """Relative/absolute refs protect album roots; special buckets do not list."""
        with tempfile.TemporaryDirectory() as root:
            quarantine = os.path.join(root, "failed_imports")
            wrong_matches = os.path.join(root, "wrong_matches")
            os.makedirs(quarantine)
            os.makedirs(wrong_matches)
            relative = os.path.join(quarantine, "Relative Album")
            absolute = os.path.join(quarantine, "Absolute Album")
            orphan_z = os.path.join(quarantine, "Zulu Orphan")
            orphan_a = os.path.join(quarantine, "Alpha Orphan")
            wrong_referenced = os.path.join(wrong_matches, "Wrong Referenced")
            wrong_orphan = os.path.join(wrong_matches, "Wrong Orphan")
            for path in (
                relative,
                absolute,
                orphan_z,
                orphan_a,
                wrong_referenced,
                wrong_orphan,
            ):
                os.makedirs(os.path.join(path, "Disc 1"))

            # These are code-owned category roots, not album folders. Their
            # children must not be recursively surfaced by this immediate-root
            # lifecycle view.
            os.makedirs(os.path.join(quarantine, "bad_files", "Corrupt Album"))
            os.makedirs(os.path.join(quarantine, "untracked_audio", "Leftovers"))
            with open(os.path.join(quarantine, "README.txt"), "w", encoding="utf-8") as f:
                f.write("not a folder")
            os.symlink(orphan_a, os.path.join(quarantine, "Album Symlink"))

            mtime_ns = 1_700_000_000_123_456_789
            os.utime(orphan_a, ns=(mtime_ns, mtime_ns))

            db = FakePipelineDB()
            _seed_wrong_match(db, "failed_imports/Relative Album")
            # A reference to a descendant still makes the immediate album root
            # visible in Wrong Matches and therefore not orphaned.
            _seed_wrong_match(db, os.path.join(absolute, "Disc 1"))
            _seed_wrong_match(db, wrong_referenced)

            result = list_unreferenced_quarantine_folders(db, root)

            self.assertEqual(
                [folder.name for folder in result.folders],
                ["Alpha Orphan", "Wrong Orphan", "Zulu Orphan"],
            )
            self.assertEqual(result.quarantine_root, quarantine)
            self.assertEqual(result.wrong_matches_root, wrong_matches)
            self.assertEqual(
                result.special_buckets,
                ["bad_files", "untracked_audio"],
            )
            alpha = result.folders[0]
            self.assertEqual(alpha.path, orphan_a)
            self.assertEqual(alpha.mtime_ns, mtime_ns)

    def test_reference_outside_configured_quarantine_does_not_claim_folder(self) -> None:
        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as other:
            quarantine = os.path.join(root, "failed_imports")
            album = os.path.join(quarantine, "Same Name")
            os.makedirs(album)
            db = FakePipelineDB()
            _seed_wrong_match(
                db,
                os.path.join(other, "failed_imports", "Same Name"),
            )

            result = list_unreferenced_quarantine_folders(db, root)

            self.assertEqual([folder.path for folder in result.folders], [album])

    def test_replaced_audit_reference_does_not_claim_live_folder(self) -> None:
        """Default Wrong Matches hides replaced rows, so triage must too."""
        with tempfile.TemporaryDirectory() as root:
            quarantine = os.path.join(root, "failed_imports")
            album = os.path.join(quarantine, "Frozen Audit Album")
            os.makedirs(album)
            db = FakePipelineDB()
            _seed_wrong_match(
                db,
                "failed_imports/Frozen Audit Album",
                request_status="replaced",
            )

            result = list_unreferenced_quarantine_folders(db, root)

            self.assertEqual(
                [folder.path for folder in result.folders],
                [album],
            )

    def test_missing_failed_imports_root_is_valid_empty_state(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            result = list_unreferenced_quarantine_folders(
                FakePipelineDB(), root,
            )
        self.assertEqual(result.folders, [])

    def test_empty_download_dir_fails_closed(self) -> None:
        with self.assertRaisesRegex(
            QuarantineScanError, "slskd download directory is not configured",
        ):
            list_unreferenced_quarantine_folders(FakePipelineDB(), "")

    def test_database_failure_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            os.makedirs(os.path.join(root, "failed_imports", "Album"))
            with self.assertRaisesRegex(
                QuarantineScanError, "read visible Wrong Matches references",
            ):
                list_unreferenced_quarantine_folders(
                    _FailingWrongMatchesDB(), root,
                )

    def test_filesystem_failure_fails_closed_instead_of_returning_empty(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            # A non-directory at the quarantine path makes scandir fail. The
            # operator must see an unavailable result, never a false empty list.
            with open(os.path.join(root, "failed_imports"), "w", encoding="utf-8") as f:
                f.write("not a directory")
            with self.assertRaisesRegex(
                QuarantineScanError, "scan quarantine directory",
            ):
                list_unreferenced_quarantine_folders(
                    FakePipelineDB(), root,
                )

    def test_wrong_matches_filesystem_failure_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with open(os.path.join(root, "wrong_matches"), "w", encoding="utf-8") as f:
                f.write("not a directory")
            with self.assertRaisesRegex(
                QuarantineScanError, "scan quarantine directory",
            ):
                list_unreferenced_quarantine_folders(
                    FakePipelineDB(), root,
                )

    def test_entry_disappearing_mid_scan_fails_closed(self) -> None:
        """Only an absent root is empty; a racy partial scan is unavailable."""
        class _DisappearingEntry:
            name = "Vanishing Album"

            def is_dir(self, *, follow_symlinks: bool) -> bool:
                return True

            def stat(self, *, follow_symlinks: bool):
                raise FileNotFoundError("entry disappeared")

        class _ScandirResult:
            def __enter__(self):
                return iter([_DisappearingEntry()])

            def __exit__(self, *_args) -> None:
                return None

        with tempfile.TemporaryDirectory() as root, patch(
            "lib.quarantine_triage_service.os.scandir",
            return_value=_ScandirResult(),
        ):
            with self.assertRaisesRegex(
                QuarantineScanError, "scan quarantine directory",
            ):
                list_unreferenced_quarantine_folders(
                    FakePipelineDB(), root,
                )


if __name__ == "__main__":
    unittest.main()
