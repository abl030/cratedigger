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

InstalledLoader = Callable[..., tuple[InstalledTrackLength, ...] | None]


def _installed(tracks: list[InstalledTrackLength]) -> InstalledLoader:
    def load(**_kwargs: object) -> tuple[InstalledTrackLength, ...] | None:
        return tuple(tracks)
    return load


def _not_installed(**_kwargs: object) -> None:
    return None


class _World:
    """One dispatch of the Fairways candidate through the real core."""

    def __init__(
        self,
        *,
        force: bool = False,
        installed_loader: InstalledLoader | None = None,
        guard_candidate: tuple[CandidateTrackLength, ...] | None = None,
        marked_incomplete: bool = False,
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
        candidate = make_album_quality_evidence(
            mb_release_id=_RELEASE, source_path=self.path,
        )
        files = snapshot_audio_files(self.path)
        candidate = msgspec.structs.replace(
            candidate, files=files,
            snapshot_fingerprint=snapshot_fingerprint(files),
        )
        self.claimed, candidate_result, execution_lease = _claim_dispatch_job(
            self.db, path=self.path, release_id=_RELEASE, force=force,
            candidate_evidence=candidate,
        )
        cancellation_token = (
            CancellationToken() if execution_lease is not None else None
        )
        if guard_candidate is None and not force:
            guard_candidate = tuple(candidate_tracks)
        loader = installed_loader or _installed(installed_tracks)
        with patch_dispatch_externals() as self.ext, patch(
            "lib.dispatch.subprocess_runner.parse_import_result",
            return_value=make_import_result(decision="import"),
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
    def world(self, **kwargs: object) -> _World:
        world = _World(**kwargs)  # type: ignore[arg-type]
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
        world = self.world(marked_incomplete=True)
        self.assertTrue(world.beets_launched())

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

    def test_unreadable_library_never_imports(self) -> None:
        def broken(**_kwargs: object) -> None:
            raise OSError("beets library unreadable")

        world = self.world(installed_loader=broken)
        self.assertFalse(world.outcome.success)
        self.assertFalse(world.beets_launched())


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
