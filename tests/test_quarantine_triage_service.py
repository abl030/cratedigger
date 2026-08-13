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
    def get_wrong_matches(self) -> list[WrongMatchCandidateRow]:
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

            processing_dir = os.path.join(root, "unused-processing")
            result = list_unreferenced_quarantine_folders(
                db, root, processing_dir=processing_dir,
            )

            self.assertEqual(
                [folder.name for folder in result.folders],
                ["Alpha Orphan", "Wrong Orphan", "Zulu Orphan"],
            )
            self.assertEqual(result.quarantine_root, quarantine)
            self.assertEqual(result.wrong_matches_root, wrong_matches)
            self.assertEqual(
                result.processing_wrong_matches_root,
                os.path.join(processing_dir, "albums", "wrong_matches"),
            )
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

            result = list_unreferenced_quarantine_folders(
                db, root, processing_dir=os.path.join(root, "unused-processing"),
            )

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

            result = list_unreferenced_quarantine_folders(
                db, root, processing_dir=os.path.join(root, "unused-processing"),
            )

            self.assertEqual(
                [folder.path for folder in result.folders],
                [album],
            )

    def test_missing_failed_imports_root_is_valid_empty_state(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            result = list_unreferenced_quarantine_folders(
                FakePipelineDB(), root,
                processing_dir=os.path.join(root, "unused-processing"),
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
                    processing_dir=os.path.join(root, "unused-processing"),
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
                    processing_dir=os.path.join(root, "unused-processing"),
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
                    processing_dir=os.path.join(root, "unused-processing"),
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
        ), self.assertRaisesRegex(
            QuarantineScanError, "scan quarantine directory",
        ):
            list_unreferenced_quarantine_folders(
                FakePipelineDB(), root,
                processing_dir=os.path.join(root, "unused-processing"),
            )

    def test_empty_processing_dir_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as root, self.assertRaisesRegex(
            QuarantineScanError, "processing directory is not configured",
        ):
            list_unreferenced_quarantine_folders(
                FakePipelineDB(), root, processing_dir="",
            )

    def test_processing_wrong_matches_orphan_is_listed(self) -> None:
        """#1122 item 1: the live processing-side wrong_matches root is scanned.

        Post-#1077, every current kept rejection lands under
        ``<processing_dir>/albums/wrong_matches/`` — never a bare
        ``<processing_dir>/wrong_matches/`` and never a processing-side
        ``failed_imports/`` (``lib.import_manifest._allocate_target`` always
        targets ``wrong_matches/`` now; see its docstring).
        """
        with tempfile.TemporaryDirectory() as root, \
                tempfile.TemporaryDirectory() as proc_root:
            processing_wrong_matches = os.path.join(
                proc_root, "albums", "wrong_matches",
            )
            orphan = os.path.join(processing_wrong_matches, "Processing Orphan")
            os.makedirs(orphan)

            result = list_unreferenced_quarantine_folders(
                FakePipelineDB(), root, processing_dir=proc_root,
            )

            self.assertEqual(
                result.processing_wrong_matches_root, processing_wrong_matches,
            )
            self.assertEqual(
                [folder.path for folder in result.folders], [orphan],
            )

    def test_processing_wrong_matches_referenced_folder_is_not_listed(self) -> None:
        """A queue-referenced processing-root folder is never surfaced as an
        orphan. Processing-sourced rejections always store an ABSOLUTE
        ``failed_path`` (``move_failed_import_whole`` returns
        ``os.path.abspath(...)`` unconditionally), so the reference is seeded
        as an absolute path here.
        """
        with tempfile.TemporaryDirectory() as root, \
                tempfile.TemporaryDirectory() as proc_root:
            processing_wrong_matches = os.path.join(
                proc_root, "albums", "wrong_matches",
            )
            referenced = os.path.join(
                processing_wrong_matches, "Processing Referenced",
            )
            os.makedirs(referenced)
            db = FakePipelineDB()
            _seed_wrong_match(db, referenced)

            result = list_unreferenced_quarantine_folders(
                db, root, processing_dir=proc_root,
            )

            self.assertEqual(result.folders, [])

    def test_processing_wrong_matches_filesystem_failure_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as root, \
                tempfile.TemporaryDirectory() as proc_root:
            processing_albums = os.path.join(proc_root, "albums")
            os.makedirs(processing_albums)
            with open(
                os.path.join(processing_albums, "wrong_matches"),
                "w", encoding="utf-8",
            ) as f:
                f.write("not a directory")
            with self.assertRaisesRegex(
                QuarantineScanError, "scan quarantine directory",
            ):
                list_unreferenced_quarantine_folders(
                    FakePipelineDB(), root, processing_dir=proc_root,
                )

    def test_processing_failed_imports_orphan_is_listed(self) -> None:
        """#1122 F3: the review proved a LIVE processing-side
        ``failed_imports/`` folder (``.../failed_imports/bad_files/Celer -
        Evening (2011) [d19841d5]``, referenced by download_log 39702) that
        the original PR1 fix never scanned. There IS a processing-side
        ``failed_imports/`` root — the false claim was ``_allocate_target``
        has no producer for it any more, not that the directory can't hold
        orphans from before that change.
        """
        with tempfile.TemporaryDirectory() as root, \
                tempfile.TemporaryDirectory() as proc_root:
            processing_failed_imports = os.path.join(
                proc_root, "albums", "failed_imports",
            )
            orphan = os.path.join(
                processing_failed_imports, "Processing Failed Orphan",
            )
            os.makedirs(orphan)

            result = list_unreferenced_quarantine_folders(
                FakePipelineDB(), root, processing_dir=proc_root,
            )

            self.assertEqual(
                result.processing_failed_imports_root,
                processing_failed_imports,
            )
            self.assertEqual(
                [folder.path for folder in result.folders], [orphan],
            )

    def test_processing_failed_imports_referenced_folder_is_not_listed(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as root, \
                tempfile.TemporaryDirectory() as proc_root:
            processing_failed_imports = os.path.join(
                proc_root, "albums", "failed_imports",
            )
            referenced = os.path.join(
                processing_failed_imports, "Processing Failed Referenced",
            )
            os.makedirs(referenced)
            db = FakePipelineDB()
            _seed_wrong_match(db, referenced)

            result = list_unreferenced_quarantine_folders(
                db, root, processing_dir=proc_root,
            )

            self.assertEqual(result.folders, [])

    def test_processing_failed_imports_bad_files_bucket_is_excluded(
        self,
    ) -> None:
        """#1122 F3 deliberate decision: ``bad_files`` gets the SAME
        code-owned-category exclusion under the processing-side
        ``failed_imports/`` root as it already gets under the download-dir
        one (``SPECIAL_QUARANTINE_BUCKETS``) — it is not an album folder,
        and this immediate-children-only view never recurses into it.
        This is the exact shape of the review's live example (a real
        referenced entry living two levels deep, inside ``bad_files``):
        the folder stays invisible to this sweep by design, not by bug —
        recursing into code-owned category buckets is out of scope for an
        immediate-root lifecycle view.
        """
        with tempfile.TemporaryDirectory() as root, \
                tempfile.TemporaryDirectory() as proc_root:
            processing_failed_imports = os.path.join(
                proc_root, "albums", "failed_imports",
            )
            db = FakePipelineDB()
            # A referenced entry INSIDE bad_files (the review's live shape) —
            # never even reaches the referenced-vs-orphan decision, because
            # the immediate-children walk skips bad_files entirely.
            referenced_inside_bad_files = os.path.join(
                processing_failed_imports, "bad_files", "Celer - Evening",
            )
            os.makedirs(referenced_inside_bad_files)
            _seed_wrong_match(db, referenced_inside_bad_files)
            # An UNREFERENCED entry inside bad_files must ALSO stay hidden —
            # proves this is bucket exclusion, not reference-driven luck.
            os.makedirs(os.path.join(
                processing_failed_imports, "bad_files", "Unreferenced Junk",
            ))
            # A genuine orphan sitting directly under failed_imports/ (a
            # sibling of bad_files, not inside it) still surfaces normally.
            real_orphan = os.path.join(
                processing_failed_imports, "Real Orphan Album",
            )
            os.makedirs(real_orphan)

            result = list_unreferenced_quarantine_folders(
                db, root, processing_dir=proc_root,
            )

            self.assertEqual(
                [folder.path for folder in result.folders], [real_orphan],
            )

    def test_processing_failed_imports_filesystem_failure_fails_closed(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as root, \
                tempfile.TemporaryDirectory() as proc_root:
            processing_albums = os.path.join(proc_root, "albums")
            os.makedirs(processing_albums)
            with open(
                os.path.join(processing_albums, "failed_imports"),
                "w", encoding="utf-8",
            ) as f:
                f.write("not a directory")
            with self.assertRaisesRegex(
                QuarantineScanError, "scan quarantine directory",
            ):
                list_unreferenced_quarantine_folders(
                    FakePipelineDB(), root, processing_dir=proc_root,
                )

    def test_default_download_dir_and_processing_dir_read_config_once(
        self,
    ) -> None:
        """#1122 F5: both defaults are resolved from ONE
        ``read_runtime_config()`` call, not one call per field."""
        with tempfile.TemporaryDirectory() as root:
            config_path = os.path.join(root, "config.ini")
            with open(config_path, "w", encoding="utf-8") as f:
                f.write(f"[Slskd]\ndownload_dir = {root}\n")
            calls: list[None] = []
            from lib.config import read_runtime_config as _real_read_config

            real_config = _real_read_config(config_path)

            def _counting_read_runtime_config(*_args: object, **_kwargs: object):
                calls.append(None)
                return real_config

            with patch(
                "lib.config.read_runtime_config",
                _counting_read_runtime_config,
            ):
                list_unreferenced_quarantine_folders(FakePipelineDB())

            self.assertEqual(
                len(calls), 1,
                "download_dir and processing_dir defaults must share one "
                "runtime config read, not one each",
            )

    def test_unreadable_runtime_config_fails_closed(self) -> None:
        """#1122 review NEW-1: ``_read_runtime_config``'s own
        ``except -> QuarantineScanError`` branch (hit when BOTH
        ``download_dir`` and ``processing_dir`` default and the runtime
        config itself cannot be read) had no direct coverage — the removed
        CLI-level ``test_quarantine_main_maps_runtime_config_failure_and_closes_db``
        exercised this indirectly through ``main()``, but nothing replaced
        it at the service layer once quarantine stopped constructing a
        ``PipelineDB`` in the CLI process at all.
        """
        with self.assertRaisesRegex(
            QuarantineScanError, "runtime configuration",
        ), patch(
            "lib.config.read_runtime_config",
            side_effect=PermissionError("runtime config unreadable"),
        ):
            list_unreferenced_quarantine_folders(FakePipelineDB())


if __name__ == "__main__":
    unittest.main()
