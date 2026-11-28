"""The folder-collision hold through the real dispatch core (issue #1465).

The harness refused an import because the library folder it would occupy
already holds another album: Beets removed and moved nothing. Automation
dispatch holds the intact copy in Wrong Matches exactly like a held
upgrade (moved whole into ``wrong_matches/``, ``failed_path`` under the
``folder_exclusivity_guard_failed`` scenario with the stored collision),
denylists the peer for this request with NO cooldown strike, and requeues.
A force import of an already held folder that still collides is not
re-held: the folder stays where it is and stays held.
"""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from unittest.mock import patch

from lib.import_execution import CancellationToken
from lib.quality import (
    DownloadInfo,
    FolderExclusivityGuardInfo,
    FolderOccupantInfo,
    ImportResult,
)
from tests.dispatch_helpers import (
    finalize_claimed_dispatch,
    make_dispatch_request,
    noop_quality_gate,
    patch_dispatch_externals,
    pinned_dispatch_authority,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_download_file, make_import_result, make_request_row
from tests.test_import_dispatch import (
    _HARNESS,
    _claim_dispatch_job,
    _full_dispatch_config,
    _owned_test_runner,
)

_RELEASE = "3b2e8f63-0c3c-4d2e-9b55-1465aaaa0002"
_FOLDER = "Guard Artist - Foo"
_LIBRARY_FOLDER = "/Beets/Guard Artist/2004 - Foo_"


def _guard() -> FolderExclusivityGuardInfo:
    return FolderExclusivityGuardInfo(
        folder=_LIBRARY_FOLDER,
        planned_paths=[f"{_LIBRARY_FOLDER}/01 Track 1.mp3"],
        occupants=[FolderOccupantInfo(
            beets_album_id=7,
            mb_albumid="3b2e8f63-0c3c-4d2e-9b55-1465aaaa0001",
            albumartist="Guard Artist",
            album="Foo_",
            item_paths=[f"{_LIBRARY_FOLDER}/01 Other.mp3"],
        )],
        excluded_album_ids=[9],
        beets_album_id=9,
        message=f"planned folder {_LIBRARY_FOLDER!r} already holds beets albums id:7",
    )


def _refusal() -> ImportResult:
    result = make_import_result(decision="folder_exclusivity_guard_failed")
    result.exit_code = 8
    result.error = _guard().message
    result.postflight.folder_exclusivity_guard = _guard()
    return result


class _World:
    def __init__(
        self, *, force: bool = False, held_source: bool = False,
        local_import: bool = False,
    ) -> None:
        self.db = FakePipelineDB()
        self.db.seed_request(make_request_row(
            id=42,
            status="unsearchable" if force else "wanted",
            mb_release_id=_RELEASE,
        ))
        self.tmp = tempfile.TemporaryDirectory()
        albums = os.path.join(self.tmp.name, "albums")
        self.wrong_matches = os.path.join(albums, "wrong_matches")
        parent = self.wrong_matches if held_source else albums
        # The operator's folder. A force import runs Beets on a private
        # action copy (scripts/importer.py ``force-action-<job>``), which the
        # importer deletes after the terminal commit; the operator's folder
        # travels as ``launch_authority_path`` (None for a local import).
        self.source = os.path.join(parent, _FOLDER)
        os.makedirs(self.source)
        with open(os.path.join(self.source, "01 Track 1.mp3"), "wb") as fh:
            fh.write(b"fixture audio")
        if force:
            self.path = os.path.join(albums, "force-action-77")
            os.makedirs(self.path)
            with open(os.path.join(self.path, "01 Track 1.mp3"), "wb") as fh:
                fh.write(b"fixture audio")
        else:
            self.path = self.source
        launch_authority = (
            self.source if force and not local_import else None)
        self.claimed, candidate_result, execution_lease = _claim_dispatch_job(
            # A local import's job names its own action copy (there is no
            # separate pipeline source); a force retry's names the folder.
            self.db, path=self.path if local_import else self.source,
            release_id=_RELEASE, force=force,
            action_path=self.path if force and not local_import else None,
        )
        cancellation_token = (
            CancellationToken() if execution_lease is not None else None
        )
        with patch_dispatch_externals() as self.ext, patch(
            "lib.dispatch.subprocess_runner.parse_import_result",
            return_value=_refusal(),
        ), pinned_dispatch_authority(
            self.db, execution_lease, cancellation_token=cancellation_token,
        ) as (cancellation_token, owner_session_identity):
            from lib.dispatch import dispatch_import_core

            self.outcome = dispatch_import_core(
                make_dispatch_request(
                    path=self.path,
                    mb_release_id=_RELEASE,
                    request_id=42,
                    label="Guard Artist - Foo?",
                    force=force,
                    beets_harness_path=_HARNESS,
                    dl_info=DownloadInfo(filetype="mp3", username="peer1"),
                    files=[make_download_file(username="peer1")],
                    scenario="force_import" if force else "strong_match",
                    requeue_on_failure=not force,
                    candidate_import_job_id=self.claimed.id,
                    prevalidated_candidate_result=candidate_result,
                    execution_lease=execution_lease,
                    owner_session_identity=owner_session_identity,
                    cooled_down_users=set(),
                    launch_authority_path=launch_authority,
                ),
                self.db,
                cfg=_full_dispatch_config(),
                quality_gate_fn=noop_quality_gate,
                current_evidence_loader=lambda *_a, **_k: None,
                cancellation_token=cancellation_token,
                run_import_fn=(
                    _owned_test_runner if execution_lease is not None else None
                ),
            )
        finalize_claimed_dispatch(self.db, self.claimed, self.outcome)

    def close(self) -> None:
        self.tmp.cleanup()


class TestFolderCollisionHoldDispatch(unittest.TestCase):
    def world(self, **kwargs: bool) -> _World:
        world = _World(**kwargs)
        self.addCleanup(world.close)
        return world

    def test_automation_collision_is_held_in_wrong_matches(self) -> None:
        world = self.world()
        outcome = world.outcome
        self.assertFalse(outcome.success)
        self.assertEqual(outcome.code, "folder_collision_held")
        self.assertEqual(
            outcome.post_commit_wrong_match_scenario,
            "folder_exclusivity_guard_failed",
        )
        self.assertIsNone(outcome.post_commit_cleanup)
        # Moved WHOLE into Wrong Matches, never deleted.
        self.assertFalse(os.path.exists(world.path))
        kept = os.path.join(world.wrong_matches, _FOLDER)
        self.assertTrue(os.path.isfile(os.path.join(kept, "01 Track 1.mp3")))

        log = world.db.download_logs[-1]
        self.assertEqual(log.outcome, "rejected")
        self.assertEqual(log.beets_scenario, "folder_exclusivity_guard_failed")
        payload = json.loads(log.validation_result)
        self.assertEqual(payload["scenario"], "folder_exclusivity_guard_failed")
        self.assertEqual(payload["failed_path"], kept)
        self.assertEqual(payload["folder_collision"]["folder"], _LIBRARY_FOLDER)
        self.assertEqual(
            payload["folder_collision"]["occupants"][0]["beets_album_id"], 7)
        self.assertIn(_LIBRARY_FOLDER, payload["detail"])
        self.assertIn("Guard Artist - Foo_ (beets id 7)", payload["detail"])
        # Denylisted for this request only; no cooldown strike.
        self.assertEqual(
            [(e.request_id, e.username, e.reason) for e in world.db.denylist],
            [(42, "peer1", "library folder occupied")])
        self.assertEqual(world.db.cooldowns_applied, [])
        # Searching continues.
        self.assertEqual(world.db.request(42)["status"], "wanted")
        # Held in the worklist, sticky to its folder.
        (row,) = world.db.get_wrong_matches()
        self.assertEqual(row["download_log_id"], log.id)
        self.assertEqual(row["held_download_log_id"], log.id)
        self.assertEqual(
            world.db.get_held_upgrade_download_log_id(42, kept), log.id)

    def test_force_retry_that_still_collides_is_held_on_the_operators_folder(
        self,
    ) -> None:
        world = self.world(force=True, held_source=True)
        self.assertEqual(world.outcome.code, "folder_collision_held")
        # The operator's held folder stays exactly where it is.
        self.assertTrue(
            os.path.isfile(os.path.join(world.source, "01 Track 1.mp3")))
        self.assertFalse(os.path.exists(
            os.path.join(world.wrong_matches, "wrong_matches")))
        world.ext.cleanup.assert_not_called()
        log = world.db.download_logs[-1]
        payload = json.loads(log.validation_result)
        self.assertEqual(payload["scenario"], "folder_exclusivity_guard_failed")
        # The hold names the OPERATOR'S folder, never the private action
        # copy Beets ran on (the importer deletes that copy).
        self.assertEqual(payload["failed_path"], world.source)
        self.assertNotIn("force-action", payload["failed_path"])
        self.assertEqual(payload["folder_collision"]["folder"], _LIBRARY_FOLDER)
        # The operator's retry denylists nobody, strikes nobody, and leaves
        # the request's status alone.
        self.assertEqual(world.db.denylist, [])
        self.assertEqual(world.db.cooldowns_applied, [])
        self.assertEqual(world.db.request(42)["status"], "unsearchable")
        # The folder is held, by this newest row, so its card is current.
        (row,) = world.db.get_wrong_matches()
        self.assertEqual(row["held_download_log_id"], log.id)
        self.assertEqual(
            world.db.get_held_upgrade_download_log_id(42, world.source), log.id)

    def test_local_import_collision_settles_without_a_hold(self) -> None:
        """A local import has no pipeline folder to hold: its action copy is
        disposable and the operator's folder is outside the pipeline."""
        world = self.world(force=True, local_import=True)
        self.assertNotEqual(world.outcome.code, "folder_collision_held")
        self.assertEqual(world.db.get_wrong_matches(), [])
        log = world.db.download_logs[-1]
        self.assertEqual(log.beets_scenario, "folder_exclusivity_guard_failed")
        self.assertIsNone(json.loads(log.validation_result or "{}").get("failed_path"))
        # No cooldown strike here either; the operator's folder is untouched.
        self.assertEqual(world.db.cooldowns_applied, [])
        self.assertTrue(
            os.path.isfile(os.path.join(world.source, "01 Track 1.mp3")))
        for entry in world.db.denylist:
            self.assertEqual(entry.reason, "library folder occupied")


if __name__ == "__main__":
    unittest.main()
