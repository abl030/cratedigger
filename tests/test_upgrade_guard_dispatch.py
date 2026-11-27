"""The upgrade guard through the real dispatch core (issue #1445 item 3).

Production-shaped automation dispatch over ``FakePipelineDB``: a candidate
whose track lengths disagree with the installed copy is held before Beets
launches; the folder moves whole into ``wrong_matches/``; the audit row
names it under ``upgrade_held`` with the stored comparison; the peer is
banned for this request without a cooldown strike; the request requeues.
The Fairways numbers are the real request 1649.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import unittest
from collections.abc import Callable
from datetime import datetime
from unittest.mock import patch

import msgspec

from lib.import_execution import CancellationToken
from lib.quality import DownloadInfo
from lib.quality.decisions import DECISION_SUSPECT_LOSSLESS_DOWNGRADE
from lib.quality.installed_copy_guard import (
    CandidateTrackLength,
    InstalledTrackLength,
    compare_installed_copy,
)
from lib.quality_evidence import snapshot_audio_files, snapshot_fingerprint
from tests.dispatch_helpers import (
    finalize_claimed_dispatch,
    make_dispatch_request,
    noop_quality_gate,
    patch_dispatch_externals,
    pinned_dispatch_authority,
)
from tests.evidence_helpers import make_album_quality_evidence
from tests.fakes import FakePipelineDB
from tests.helpers import make_download_file, make_import_result, make_request_row
from tests.test_import_dispatch import (
    _HARNESS,
    _claim_dispatch_job,
    _full_dispatch_config,
    _owned_test_runner,
)
from tests.test_installed_copy_guard import fairways

_RELEASE = "4c6f4a7b-c093-4006-a50e-3d6dfbdd2645"
_FOLDER = "The Fairways - Is Everything All Right"

_SHORT_OF_RELEASE_REASON = (
    "upgrade held: track lengths short of the release "
    "(installed copy marked incomplete)"
)

InstalledLoader = Callable[..., tuple[InstalledTrackLength, ...] | None]


def _installed(tracks: list[InstalledTrackLength]) -> InstalledLoader:
    def load(**_kwargs: object) -> tuple[InstalledTrackLength, ...] | None:
        return tuple(tracks)
    return load


def _not_installed(**_kwargs: object) -> None:
    return None


def _matches_release() -> tuple[CandidateTrackLength, ...]:
    """The Fairways candidate with every file as long as the release says."""
    candidate, _ = fairways()
    return tuple(
        msgspec.structs.replace(t, candidate_seconds=t.declared_seconds)
        for t in candidate
    )


class _World:
    """One dispatch of the Fairways candidate through the real core."""

    def __init__(
        self,
        *,
        force: bool = False,
        installed_loader: InstalledLoader | None = None,
        guard_candidate: tuple[CandidateTrackLength, ...] | None = None,
        marked_incomplete: bool = False,
        audio_corrupt: bool = False,
        cancel_during_move: bool = False,
    ) -> None:
        candidate_tracks, installed_tracks = fairways()
        self.db = FakePipelineDB()
        self.db.seed_request(make_request_row(
            id=42,
            status="unsearchable" if force else "wanted",
            mb_release_id=_RELEASE,
            search_filetype_override="lossless,mp3 v0,mp3 320",
            min_bitrate=320,
            marked_incomplete_at=(
                "2026-11-01T00:00:00+00:00" if marked_incomplete else None
            ),
        ))
        self.tmp = tempfile.TemporaryDirectory()
        albums = os.path.join(self.tmp.name, "albums")
        self.path = os.path.join(albums, _FOLDER)
        os.makedirs(self.path)
        with open(os.path.join(self.path, "02 - Secretive Girl.flac"), "wb") as fh:
            fh.write(b"fixture audio")
        audio = os.path.join(self.path, "02 - Secretive Girl.flac")
        os.rename(audio, audio[:-5] + ".mp3")
        self.wrong_matches = os.path.join(albums, "wrong_matches")
        # No current evidence is loaded below, so the quality decision
        # admits this candidate as into an empty slot; the guard reads the
        # installed copy straight from Beets (the injected loader).
        files = snapshot_audio_files(self.path)
        candidate = make_album_quality_evidence(
            mb_release_id=_RELEASE, source_path=self.path, files=files,
        )
        candidate = msgspec.structs.replace(
            candidate, files=candidate.files,
            snapshot_fingerprint=snapshot_fingerprint(files),
        )
        self.claimed, candidate_result, execution_lease = _claim_dispatch_job(
            self.db, path=self.path, release_id=_RELEASE, force=force,
            candidate_evidence=None if audio_corrupt else candidate,
            evidence_kwargs=(
                {"audio_corrupt": True, "audio_error": "decode failed"}
                if audio_corrupt else None
            ),
        )
        cancellation_token = (
            CancellationToken() if execution_lease is not None else None
        )
        if guard_candidate is None and not force:
            guard_candidate = tuple(candidate_tracks)
        loader = installed_loader or _installed(installed_tracks)
        from lib import import_manifest

        real_move = import_manifest.move_failed_import_whole

        def move_then_cancel(
            src_path: str,
            *,
            scenario: str | None = None,
            quarantine_root: str | None = None,
            before_mutation: import_manifest.MutationCheckpoint | None = None,
        ) -> str | None:
            moved = real_move(
                src_path, scenario=scenario, quarantine_root=quarantine_root,
                before_mutation=before_mutation,
            )
            if cancel_during_move and cancellation_token is not None:
                cancellation_token.cancel("operator stop during the move")
            return moved

        with patch_dispatch_externals() as self.ext, patch(
            "lib.dispatch.subprocess_runner.parse_import_result",
            return_value=make_import_result(decision="import"),
        ), patch.object(
            import_manifest, "move_failed_import_whole", move_then_cancel,
        ), pinned_dispatch_authority(
            self.db, execution_lease, cancellation_token=cancellation_token,
        ) as (cancellation_token, owner_session_identity):
            from lib.dispatch import dispatch_import_core

            self.outcome = dispatch_import_core(
                make_dispatch_request(
                    path=self.path,
                    mb_release_id=_RELEASE,
                    request_id=42,
                    label="The Fairways - Is Everything All Right?",
                    force=force,
                    beets_harness_path=_HARNESS,
                    dl_info=DownloadInfo(filetype="flac", username="natetheneet"),
                    files=[make_download_file(username="natetheneet")],
                    scenario="force_import" if force else "strong_match",
                    requeue_on_failure=not force,
                    candidate_import_job_id=self.claimed.id,
                    prevalidated_candidate_result=candidate_result,
                    execution_lease=execution_lease,
                    owner_session_identity=owner_session_identity,
                    upgrade_guard_candidate=guard_candidate,
                ),
                self.db,
                cfg=_full_dispatch_config(),
                quality_gate_fn=noop_quality_gate,
                current_evidence_loader=lambda *_a, **_k: None,
                installed_tracks_loader=loader,
                cancellation_token=cancellation_token,
                run_import_fn=(
                    _owned_test_runner if execution_lease is not None else None
                ),
            )

    def beets_launched(self) -> bool:
        return self.ext.run.call_args is not None

    def close(self) -> None:
        self.tmp.cleanup()


class TestUpgradeGuardDispatch(unittest.TestCase):
    def world(
        self,
        *,
        force: bool = False,
        installed_loader: InstalledLoader | None = None,
        marked_incomplete: bool = False,
        audio_corrupt: bool = False,
        cancel_during_move: bool = False,
        guard_candidate: tuple[CandidateTrackLength, ...] | None = None,
    ) -> _World:
        world = _World(
            force=force,
            installed_loader=installed_loader,
            guard_candidate=guard_candidate,
            marked_incomplete=marked_incomplete,
            audio_corrupt=audio_corrupt,
            cancel_during_move=cancel_during_move,
        )
        self.addCleanup(world.close)
        return world

    def test_fairways_automation_upgrade_is_held(self) -> None:
        world = self.world()
        outcome = world.outcome
        self.assertFalse(outcome.success)
        self.assertEqual(outcome.code, "upgrade_held")
        self.assertEqual(outcome.post_commit_wrong_match_scenario, "upgrade_held")
        self.assertIsNone(outcome.post_commit_cleanup)
        self.assertFalse(world.beets_launched(), "Beets must never start")
        # The folder moved WHOLE into Wrong Matches.
        self.assertFalse(os.path.exists(world.path))
        kept = os.path.join(world.wrong_matches, _FOLDER)
        self.assertTrue(os.path.isfile(
            os.path.join(kept, "02 - Secretive Girl.mp3")))

        finalize_claimed_dispatch(world.db, world.claimed, outcome)
        log = world.db.download_logs[-1]
        self.assertEqual(log.outcome, "rejected")
        payload = json.loads(log.validation_result)
        self.assertEqual(payload["scenario"], "upgrade_held")
        self.assertEqual(payload["failed_path"], kept)
        self.assertEqual(payload["denylisted_users"], ["natetheneet"])
        hold = payload["upgrade_hold"]
        self.assertTrue(hold["holds"])
        self.assertEqual(
            [row["medium_index"] for row in hold["rows"] if row["tripped"]],
            [2, 5])
        self.assertIn("so the new copy is the odd one out", payload["detail"])
        # Banned for this request, with no cooldown strike.
        self.assertEqual(
            [(e.request_id, e.username) for e in world.db.denylist],
            [(42, "natetheneet")])
        self.assertEqual(world.db.cooldowns_applied, [])
        self.assertEqual(world.db.request(42)["status"], "wanted")
        # Visible in the worklist, so the operator can act on it.
        visible = world.db.get_wrong_matches()
        self.assertEqual([row["download_log_id"] for row in visible], [log.id])

    def test_matching_copies_import(self) -> None:
        candidate, _ = fairways()
        agreeing = [
            InstalledTrackLength(
                key=t.key, installed_seconds=t.candidate_seconds)
            for t in candidate
        ]
        world = self.world(installed_loader=_installed(agreeing))
        self.assertTrue(world.beets_launched())
        self.assertNotEqual(world.outcome.code, "upgrade_held")

    def test_force_import_is_the_operator_approval_and_never_held(self) -> None:
        world = self.world(force=True)
        self.assertTrue(world.beets_launched())
        self.assertNotEqual(world.outcome.code, "upgrade_held")

    def test_operator_incomplete_mark_turns_the_guard_off(self) -> None:
        """The mark turns the installed-copy comparison off: a candidate the
        release listing agrees with imports over a disagreeing copy."""
        world = self.world(
            marked_incomplete=True, guard_candidate=_matches_release(),
        )
        self.assertTrue(world.beets_launched())

    def test_marked_incomplete_holds_a_candidate_short_of_the_release(
        self,
    ) -> None:
        """Issue #1471 gap 2. Fairways track 5 runs 3:18 where the release
        says 4:20: under the mark it is short of the declared program, so it
        is held instead of replacing the marked copy and clearing the mark.
        """
        calls: list[dict[str, object]] = []
        _, installed = fairways()

        def recording(**kwargs: object) -> tuple[InstalledTrackLength, ...]:
            calls.append(kwargs)
            return tuple(installed)

        world = self.world(marked_incomplete=True, installed_loader=recording)
        self.assertEqual(world.outcome.code, "upgrade_held")
        self.assertFalse(world.beets_launched(), "Beets must never start")
        self.assertEqual(len(calls), 1, "installed copy measured for display")

        finalize_claimed_dispatch(world.db, world.claimed, world.outcome)
        payload = json.loads(world.db.download_logs[-1].validation_result)
        hold = payload["upgrade_hold"]
        self.assertEqual(hold["basis"], "declared_program")
        self.assertEqual(
            [row["medium_index"] for row in hold["rows"] if row["tripped"]],
            [5])
        tripped = next(row for row in hold["rows"] if row["tripped"])
        self.assertEqual(tripped["installed_seconds"], 259.9)
        self.assertTrue(payload["detail"].startswith(
            "Kept your current copy, which you marked incomplete."))
        self.assertEqual(
            [(e.request_id, e.username, e.reason) for e in world.db.denylist],
            [(42, "natetheneet", _SHORT_OF_RELEASE_REASON)])
        self.assertIsNotNone(world.db.request(42)["marked_incomplete_at"])

    def test_marked_but_nothing_installed_imports_a_short_candidate(
        self,
    ) -> None:
        """Bad Rip removes the copy and leaves the mark: with nothing to
        keep, a short candidate fills the empty slot instead of being held
        under a sentence about a copy that does not exist."""
        world = self.world(
            marked_incomplete=True, installed_loader=_not_installed,
        )
        self.assertTrue(world.beets_launched())
        self.assertNotEqual(world.outcome.code, "upgrade_held")

    def test_no_installed_copy_imports(self) -> None:
        world = self.world(installed_loader=_not_installed)
        self.assertTrue(world.beets_launched())

    def test_nothing_keyed_fails_open_with_one_journal_line(self) -> None:
        _, installed = fairways()
        renamed = [
            InstalledTrackLength(
                key=f"merged-{t.key}", installed_seconds=t.installed_seconds)
            for t in installed
        ]
        with self.assertLogs("cratedigger", level=logging.WARNING) as logs:
            world = self.world(installed_loader=_installed(renamed))
        self.assertTrue(world.beets_launched())
        self.assertEqual(
            sum("UPGRADE GUARD COMPARED NOTHING" in line for line in logs.output),
            1)

    def test_a_rejected_candidate_opens_no_installed_file(self) -> None:
        """C3: the installed copy is read and measured only when the
        decision would otherwise import over it."""
        calls: list[dict[str, object]] = []

        def recording(**kwargs: object) -> None:
            calls.append(kwargs)

        world = self.world(installed_loader=recording, audio_corrupt=True)
        self.assertFalse(world.outcome.success)
        self.assertEqual(calls, [], "an installed file was opened")

    def test_marked_incomplete_opens_no_installed_file(self) -> None:
        calls: list[dict[str, object]] = []

        def recording(**kwargs: object) -> None:
            calls.append(kwargs)

        world = self.world(
            installed_loader=recording, marked_incomplete=True,
            guard_candidate=_matches_release(),
        )
        self.assertTrue(world.beets_launched())
        self.assertEqual(calls, [], "an installed file was opened")

    def test_no_cancellation_checkpoint_after_the_folder_moved(self) -> None:
        """C2: once the folder has moved the hold runs to its terminal
        outcome, so the moved folder can never be left without its row."""
        world = self.world(cancel_during_move=True)
        self.assertEqual(world.outcome.code, "upgrade_held")
        self.assertIsNotNone(world.outcome.terminal_outcome)
        self.assertTrue(os.path.isdir(os.path.join(world.wrong_matches, _FOLDER)))

    def test_unreadable_library_never_imports(self) -> None:
        def broken(**_kwargs: object) -> None:
            raise OSError("beets library unreadable")

        world = self.world(installed_loader=broken)
        self.assertFalse(world.outcome.success)
        self.assertFalse(world.beets_launched())


class TestInstalledLengthsAreMeasuredFromFiles(unittest.TestCase):
    """The production installed-side loader, end to end on real audio.

    WHY this exists: Beets' ``items.length`` is NOT the installed file's
    length — it holds the release's declared value. On doc2, Kurt Vile
    "So Outta Reach" item 195486 "02 It's Alright.opus" stores 289.9s (the
    MusicBrainz value) while the file reads 336.3s. A guard reading that
    column would compare the candidate with the release and the "new copy
    is closer to the release" exemption could never fire. Do not
    "simplify" the loader back to the column.
    """

    _FIXTURES = os.path.join(
        os.path.dirname(__file__), "fixtures", "audio_hash")

    def setUp(self) -> None:
        import shutil
        import sqlite3

        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.library = os.path.join(self.tmp.name, "library")
        album = os.path.join(self.library, "Kurt Vile", "So Outta Reach")
        os.makedirs(album)
        self.mp3 = os.path.join(album, "01 Track.mp3")
        self.ogg = os.path.join(album, "02 It's Alright.ogg")
        shutil.copy(os.path.join(self._FIXTURES, "sine_440.mp3"), self.mp3)
        shutil.copy(os.path.join(self._FIXTURES, "sine_440.ogg"), self.ogg)
        self.missing = os.path.join(album, "03 Gone.mp3")
        self.db_path = os.path.join(self.tmp.name, "library.db")
        conn = sqlite3.connect(self.db_path)
        conn.executescript("""
            CREATE TABLE albums (
                id INTEGER PRIMARY KEY, mb_albumid TEXT,
                discogs_albumid INTEGER);
            CREATE TABLE items (
                id INTEGER PRIMARY KEY, album_id INTEGER, path BLOB,
                title TEXT, track INTEGER, disc INTEGER, length REAL,
                format TEXT, bitrate INTEGER, samplerate INTEGER,
                bitdepth INTEGER, mb_releasetrackid TEXT, mb_trackid TEXT);
        """)
        conn.execute("INSERT INTO albums VALUES (1, ?, NULL)", (_RELEASE,))
        for item_id, path, declared, key in (
            (10, self.mp3, 227.4, "rt-1"),
            (11, self.ogg, 289.9, "rt-2"),
            (12, self.missing, 196.9, "rt-3"),
        ):
            conn.execute(
                "INSERT INTO items VALUES "
                "(?, 1, ?, 'x', 1, 1, ?, 'MP3', 0, 0, 0, ?, '')",
                (item_id, path.encode(), declared, key),
            )
        conn.commit()
        conn.close()

    def test_lengths_come_from_the_files_not_the_beets_column(self) -> None:
        from lib.dispatch.core import _load_installed_track_lengths

        with self.assertLogs("cratedigger", level=logging.WARNING) as logs:
            tracks = _load_installed_track_lengths(
                beets_library_db_path=self.db_path,
                beets_library_root=self.library,
                mb_release_id=_RELEASE,
                label="Kurt Vile - So Outta Reach",
            )
        assert tracks is not None
        by_key = {t.key: t.installed_seconds for t in tracks}
        # Header-level mediafile reads of the real fixtures — the same
        # reader the beets harness uses for the candidate's item.length.
        self.assertAlmostEqual(by_key["rt-1"], 0.5486, places=3)
        self.assertAlmostEqual(by_key["rt-2"], 0.5, places=3)
        self.assertNotIn(289.9, by_key.values())
        # A missing file is skipped (0.0), with one line naming it.
        self.assertEqual(by_key["rt-3"], 0.0)
        self.assertEqual(
            sum("cannot measure installed file" in line for line in logs.output),
            1)

    def test_ambiguous_installed_copy_skips_with_one_journal_line(self) -> None:
        """C4: two Beets albums claim the release — nothing to key against,
        and the journal says why."""
        import sqlite3

        from lib.dispatch.core import _load_installed_track_lengths

        conn = sqlite3.connect(self.db_path)
        conn.execute("INSERT INTO albums VALUES (2, ?, NULL)", (_RELEASE,))
        conn.execute(
            "INSERT INTO items VALUES "
            "(20, 2, ?, 'x', 1, 1, 1.0, 'MP3', 0, 0, 0, 'rt-1', '')",
            (os.path.join(self.library, "other", "01.mp3").encode(),),
        )
        conn.commit()
        conn.close()
        with self.assertLogs("cratedigger", level=logging.WARNING) as logs:
            tracks = _load_installed_track_lengths(
                beets_library_db_path=self.db_path,
                beets_library_root=self.library,
                mb_release_id=_RELEASE,
                label="Kurt Vile - So Outta Reach",
            )
        self.assertIsNone(tracks)
        self.assertEqual(
            sum("UPGRADE GUARD SKIPPED" in line for line in logs.output), 1)

    def test_comparison_records_that_lengths_were_measured(self) -> None:
        comparison = compare_installed_copy(*fairways())
        self.assertEqual(
            comparison.installed_lengths_source, "measured_from_files")

    def test_non_positive_measurement_is_skipped_with_a_log_line(self) -> None:
        from lib.dispatch.core import _measure_installed_track_lengths
        from lib.quality.installed_copy_guard import InstalledTrackFile

        with self.assertLogs("cratedigger", level=logging.WARNING) as logs:
            tracks = _measure_installed_track_lengths(
                [InstalledTrackFile(key="k", path="/x.mp3")],
                measure_fn=lambda _path: float("nan"),
                label="x",
            )
        self.assertEqual(tracks[0].installed_seconds, 0.0)
        self.assertEqual(len(logs.output), 1)


class TestHoldLeavesSearchStateLikeAnOrdinaryReject(unittest.TestCase):
    """Correction 2: a hold may not narrow, widen or reset the search.

    The same seeded request goes through the hold helper and through the
    existing persisted-evidence reject helper with a non-narrowing reject
    decision; the request rows afterwards must be identical.
    """

    def _row_after(self, hold: bool) -> dict[str, object]:
        from lib.dispatch.outcome_actions import (
            _hold_upgrade_from_evidence_decision,
            _reject_import_from_evidence_decision,
        )
        from lib.dispatch.types import ImportAttemptResult

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="downloading", mb_release_id=_RELEASE,
            search_filetype_override="lossless,mp3 v0,mp3 320",
            min_bitrate=320, validation_attempts=2,
        ))
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "albums", _FOLDER)
            os.makedirs(path)
            request = make_dispatch_request(
                path=path, mb_release_id=_RELEASE, request_id=42,
                label="Fairways", beets_harness_path=_HARNESS,
                dl_info=DownloadInfo(filetype="flac", username="natetheneet"),
                scenario="strong_match", requeue_on_failure=True,
            )
            attempt = ImportAttemptResult(None)
            attempt.merge(make_import_result(decision="import"))
            if hold:
                _hold_upgrade_from_evidence_decision(
                    request, db, attempt_result=attempt,
                    comparison=compare_installed_copy(*fairways()),
                )
            else:
                _reject_import_from_evidence_decision(
                    request, db, attempt_result=attempt,
                    decision=DECISION_SUSPECT_LOSSLESS_DOWNGRADE,
                    detail="ordinary reject",
                )
        row = dict(db.request(42))
        # Wall-clock stamps differ between the two runs by microseconds;
        # what must match is whether each was set.
        return {
            key: (type(value).__name__ if isinstance(value, datetime) else value)
            for key, value in row.items()
        }

    def test_request_row_matches_the_existing_rejection_path(self) -> None:
        self.assertEqual(self._row_after(hold=True), self._row_after(hold=False))


if __name__ == "__main__":
    unittest.main()
