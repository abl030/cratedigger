"""The pre-conversion library-folder probe (issue #1465).

A lossless download is converted (and its lossless files removed) before
the real Beets session runs the folder exclusivity guard. For a staged
folder holding lossless audio, ``run_import_one`` first runs the same
session as a probe; a reported collision refuses the import before any
conversion, so the copy held in Wrong Matches is the download as fetched.
"""

from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from lib.quality import FolderExclusivityGuardInfo

_MBID = "3b2e8f63-0c3c-4d2e-9b55-1465aaaa0002"


def _guard() -> FolderExclusivityGuardInfo:
    return FolderExclusivityGuardInfo(
        folder="/Beets/Guard/2004 - Foo_",
        message="planned folder '/Beets/Guard/2004 - Foo_' already holds beets albums id:7",
    )


class _Run:
    """One ``run_import_one`` with the Beets child and conversion recorded."""

    def __init__(self, *, lossless: bool, probe_collides: bool) -> None:
        from harness import import_one
        from lib.beets_db import AlbumInfo
        from lib.evidence_action_file import (
            remove_quality_evidence_action_file,
            write_quality_evidence_action_file,
        )
        from lib.quality import AudioQualityMeasurement, QualityEvidenceActionPayload
        from lib.quality_evidence import snapshot_audio_files
        from tests.evidence_helpers import make_album_quality_evidence
        from tests.fakes import FakeBeetsDB

        self.calls: list[str] = []
        self._tmp = tempfile.TemporaryDirectory()
        root = self._tmp.name
        self.album = os.path.join(root, "staging", "album")
        os.makedirs(self.album)
        name = "01 - One.flac" if lossless else "01 - One.mp3"
        self.track = os.path.join(self.album, name)
        if lossless:
            # Real audio: conversion genuinely runs (no owned-function mock).
            subprocess.run([
                "ffmpeg", "-loglevel", "error", "-f", "lavfi", "-i",
                "sine=frequency=440:duration=0.2", "-c:a", "flac", self.track,
            ], check=True)
        else:
            with open(self.track, "wb") as handle:
                handle.write(b"cratedigger test bytes")
        library = os.path.join(root, "library", "Guard", "Foo")
        os.makedirs(library)
        fmt = "FLAC" if lossless else "MP3"
        evidence = make_album_quality_evidence(
            mb_release_id=_MBID,
            source_path=self.album,
            files=snapshot_audio_files(self.album),
            measurement=AudioQualityMeasurement(min_bitrate_kbps=320, format=fmt),
            codec=fmt.lower(),
            container=fmt.lower(),
            storage_format=fmt,
        )
        action_file = write_quality_evidence_action_file(
            QualityEvidenceActionPayload(
                candidate=evidence,
                decision={"imported": True, "stage2_import": "import"},
                decision_name="import",
            )
        )
        beets = FakeBeetsDB()
        beets.set_album_exists(_MBID, False)
        beets.set_item_paths(_MBID, [(1, os.path.join(library, "01 - One.mp3"))])
        beets.set_album_info(_MBID, AlbumInfo(
            album_id=77, track_count=1, min_bitrate_kbps=320, is_cbr=True,
            album_path=library,
        ))

        def recording_run_import(
            path: str, mb_release_id: str, *, max_distance: float,
            probe_folder: bool = False, **_kwargs: object,
        ) -> import_one.RunImportOutcome:
            del path, mb_release_id, max_distance
            # Whether the lossless original still exists at each Beets call.
            lossless_present = any(
                f.endswith(".flac") for f in os.listdir(self.album))
            if probe_folder:
                self.calls.append(
                    "probe" + (" (flac present)" if lossless_present else ""))
                return import_one.RunImportOutcome(
                    8 if probe_collides else 0, [],
                    folder_exclusivity_guard=_guard() if probe_collides else None,
                    folder_probe_reported=True,
                )
            self.calls.append(
                "import" + (" (flac present)" if lossless_present else ""))
            return import_one.RunImportOutcome(
                0, [], applied_distance=0.01,
                admitted_audio_count=1, applied_audio_count=1,
            )

        request = import_one.ImportOneRequest(
            path=self.album,
            mb_release_id=_MBID,
            quality_evidence_action_file=action_file,
        )
        try:
            with patch("harness.import_one.BeetsDB", return_value=beets), \
                 patch("harness.import_one.run_import", recording_run_import), \
                 patch("harness.import_one._log"):
                self.result = import_one.run_import_one(request)
        finally:
            remove_quality_evidence_action_file(action_file)

    def close(self) -> None:
        self._tmp.cleanup()


class TestFolderProbe(unittest.TestCase):
    def run_one(self, *, lossless: bool, probe_collides: bool = False) -> _Run:
        run = _Run(lossless=lossless, probe_collides=probe_collides)
        self.addCleanup(run.close)
        return run

    def test_lossless_collision_is_refused_before_any_conversion(self) -> None:
        run = self.run_one(lossless=True, probe_collides=True)
        self.assertEqual(
            run.calls, ["probe (flac present)"], "no real import")
        self.assertEqual(run.result.exit_code, 8)
        self.assertEqual(run.result.decision, "folder_exclusivity_guard_failed")
        guard = run.result.postflight.folder_exclusivity_guard
        assert guard is not None
        self.assertEqual(guard.folder, "/Beets/Guard/2004 - Foo_")
        self.assertEqual(run.result.postflight.replaced_albums, [])
        # The download is exactly as fetched: the lossless original remains.
        self.assertTrue(os.path.isfile(run.track))

    def test_lossless_free_folder_probes_then_converts_then_imports(self) -> None:
        run = self.run_one(lossless=True)
        # The probe saw the original; the real import saw the conversion.
        self.assertEqual(run.calls, ["probe (flac present)", "import"])

    def test_lossy_source_is_not_probed(self) -> None:
        run = self.run_one(lossless=False)
        self.assertEqual(run.calls, ["import"])


if __name__ == "__main__":
    unittest.main()
