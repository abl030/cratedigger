"""Issue #1176 PR3 — local-import lane hazards and seam contracts.

Two hazards this module exists to prove, per CLAUDE.md's PR3 brief:

* **Hazard A** — a successful (or failed) local import must never delete or
  otherwise touch the operator's real folder. ``_force_job_wrong_match_
  payload`` excludes every job whose ``job_type`` is not ``force_import``
  BY CONSTRUCTION, so ``_dismiss_successful_force_import`` /
  ``_cleanup_failed_force_import`` are no-ops for a ``local_import`` job.
* **Hazard B** — no Wrong Matches row a local-import job produces may ever
  carry the operator's real path as ``failed_path``; every audit surface
  this lane writes names the disposable private action copy instead.

Plus the seam contracts introduced by this PR: ``dispatch_import_from_db``'s
``distance_threshold``/``scenario`` parameters default to force's exact
historical behavior, and ``execute_import_job``'s new ``local_import``
branch passes the three documented deviations (no operator-path audit
exposure, the strict distance threshold, its own attempt scenario).
"""

from __future__ import annotations

import os
import shutil
import tempfile
import unittest
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

from lib.config import CratediggerConfig
from lib.dispatch.manifest_guard import _guard_force_import_audio_manifest
from lib.dispatch.types import DispatchOutcome, ImportAttemptResult
from lib.import_execution import CancellationToken, OwnerSessionIdentity
from lib.import_queue import (
    IMPORT_JOB_FORCE,
    IMPORT_JOB_LOCAL,
    IMPORT_JOB_YOUTUBE,
    ImportJob,
    force_import_dedupe_key,
    force_import_payload,
    local_import_dedupe_key,
    local_import_payload,
    youtube_import_dedupe_key,
    youtube_import_payload,
)
from lib.preview_snapshot import LOCAL_IMPORT_ACTION_PREFIX, force_action_copy_path
from scripts import importer
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


def _local_job(db: FakePipelineDB, request_id: int = 42, source_path: str = "/operator/real/Album"):
    db.seed_request(make_request_row(
        id=request_id, status="wanted", mb_release_id="mb-42",
    ))
    return db.enqueue_import_job(
        IMPORT_JOB_LOCAL,
        request_id=request_id,
        dedupe_key=local_import_dedupe_key(request_id),
        payload=local_import_payload(
            source_path=source_path, request_id=request_id,
        ),
    )


class TestHazardANeverConsumeOperatorFolder(unittest.TestCase):
    """A local import never deletes/moves the operator's real folder."""

    def test_wrong_match_payload_is_none_for_every_non_force_job_type(self) -> None:
        """subTest table: only force_import ever yields a wrong-match
        payload. Proves Hazard A by construction (#1176 PR3) for local,
        and pins the SAME invariant for the two other non-force types.
        """
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted", mb_release_id="mb-1"))
        cases = [
            ("local_import", _local_job(db, request_id=1)),
            (
                "force_import",
                db.enqueue_import_job(
                    IMPORT_JOB_FORCE,
                    request_id=1,
                    dedupe_key=force_import_dedupe_key(1),
                    payload=force_import_payload(
                        download_log_id=1, failed_path="/wrong_matches/Album",
                    ),
                ),
            ),
            (
                "youtube_import",
                db.enqueue_import_job(
                    IMPORT_JOB_YOUTUBE,
                    request_id=1,
                    dedupe_key=youtube_import_dedupe_key(1),
                    payload=youtube_import_payload(
                        staged_path="/staged/Album", request_id=1,
                        browse_id="MPREb_x", download_log_id=1,
                    ),
                ),
            ),
        ]
        for label, job in cases:
            with self.subTest(job_type=label):
                payload = importer._force_job_wrong_match_payload(job)
                if label == "force_import":
                    self.assertIsNotNone(payload)
                else:
                    self.assertIsNone(payload)

    def test_dismiss_successful_is_a_noop_for_local_import(self) -> None:
        db = FakePipelineDB()
        job = _local_job(db)
        self.assertIsNone(importer._dismiss_successful_force_import(db, job))

    def test_cleanup_failed_is_a_noop_for_local_import_even_on_audio_corrupt(
        self,
    ) -> None:
        """``audio_corrupt`` is the ONE force-failure scenario that also
        bans+deletes the source (D3). A local-import job must not inherit
        that even when its outcome carries the identical scenario label.
        """
        db = FakePipelineDB()
        job = _local_job(db)
        outcome = DispatchOutcome(
            success=False,
            message="rejected",
            post_commit_wrong_match_scenario="audio_corrupt",
        )
        self.assertIsNone(importer._cleanup_failed_force_import(db, job, outcome))


class TestHazardBNeverExposeOperatorPathInAudit(unittest.TestCase):
    """No Wrong Matches row a local-import job writes carries the
    operator's real path as ``failed_path`` — every audit row names the
    disposable action copy instead."""

    def test_manifest_guard_reject_names_the_action_copy_not_the_operator_path(
        self,
    ) -> None:
        db = FakePipelineDB()
        # FakePipelineDB satisfies every Protocol these production
        # functions actually depend on; it is not the concrete PipelineDB
        # class pyright sees in their signatures. One cast per test method
        # (not one per call site) mirrors the established pattern in
        # tests/test_import_operation_fence.py.
        db_arg = cast(Any, db)
        request_id = 7
        db.seed_request(make_request_row(
            id=request_id, status="wanted", mb_release_id="mb-7",
        ))
        # Two tracks expected; the action copy on disk has three — an
        # ordinary "untracked_audio" manifest-guard reject, exactly what a
        # local-import strict-validation flow can hit.
        db.set_tracks(request_id, [
            {"track_number": 1, "title": "One"},
            {"track_number": 2, "title": "Two"},
        ])
        action_copy = self._make_album_dir(files=3)
        operator_path = "/home/operator/MyRip/Album"  # never opened; must never be persisted

        reject = _guard_force_import_audio_manifest(
            db_arg,
            request_id=request_id,
            failed_path=action_copy,
            # This is the exact seam Hazard B guards: local-import's real
            # call site always passes ``audit_source_path=None`` — never the
            # operator's path (CLAUDE.md decision 2 for #1176).
            audit_source_path=None,
            download_log_id=None,
            source_username=None,
            attempt_result=ImportAttemptResult.from_import_job(db_arg, None),
            import_job_id=None,
        )
        self.assertIsNotNone(reject)
        self.assertEqual(len(db.download_logs), 1)
        failed_path = self._failed_path(db.download_logs[0])
        self.assertEqual(failed_path, action_copy)
        self.assertNotEqual(failed_path, operator_path)

    def test_planted_mutant_exposing_operator_path_is_caught(self) -> None:
        """Known-bad self-test (proves the PRECEDING test actually
        falsifies the hazard, not merely agrees by construction): passing
        the operator's real path as ``audit_source_path`` — the exact
        mutant Hazard B forbids in production — makes ``failed_path``
        diverge from the action copy, which the assertion above would
        catch.
        """
        db = FakePipelineDB()
        db_arg = cast(Any, db)
        request_id = 8
        db.seed_request(make_request_row(
            id=request_id, status="wanted", mb_release_id="mb-8",
        ))
        db.set_tracks(request_id, [{"track_number": 1, "title": "One"}])
        action_copy = self._make_album_dir(files=2)
        operator_path = "/home/operator/MyRip/Album"

        _guard_force_import_audio_manifest(
            db_arg,
            request_id=request_id,
            failed_path=action_copy,
            # The mutant: a caller that leaks the operator path here.
            audit_source_path=operator_path,
            download_log_id=None,
            source_username=None,
            attempt_result=ImportAttemptResult.from_import_job(db_arg, None),
            import_job_id=None,
        )
        failed_path = self._failed_path(db.download_logs[0])
        self.assertEqual(
            failed_path, operator_path,
            "sanity check: the mutant world really does leak the operator "
            "path when audit_source_path is set — proving the preceding "
            "test would fail (not vacuously pass) against that mutant",
        )

    @staticmethod
    def _failed_path(entry: Any) -> str | None:
        import json

        raw = entry.validation_result
        if isinstance(raw, str):
            raw = json.loads(raw)
        if isinstance(raw, dict):
            return raw.get("failed_path")
        return getattr(raw, "failed_path", None)

    def _make_album_dir(self, *, files: int) -> str:
        import tempfile

        root = tempfile.mkdtemp()
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        for index in range(files):
            with open(os.path.join(root, f"{index:02d}.flac"), "wb") as handle:
                handle.write(b"\x00")
        return root


class TestExecuteImportJobLocalBranchSeam(unittest.TestCase):
    """``execute_import_job``'s ``local_import`` branch passes the three
    documented deviations from force-import to ``dispatch_import_from_db``.

    This is the seam test that would catch the Hazard-B mutant a unit test
    on ``_guard_force_import_audio_manifest`` alone cannot: a regression
    that changed the LOCAL branch's own ``source_reference_path=None`` call
    argument (e.g. to the operator's payload path) would flip
    ``captured["source_reference_path"]`` here without touching the guard
    function at all.
    """

    def _prepared_job(self, db: FakePipelineDB, cfg: SimpleNamespace):
        # SimpleNamespace satisfies force_action_copy_path's/execute_import_
        # job's structural needs (it only reads named attributes) but is
        # not the concrete CratediggerConfig class pyright sees.
        cfg_arg = cast(Any, cfg)
        request_id = 51
        db.seed_request(make_request_row(
            id=request_id, status="wanted", mb_release_id="mb-51",
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_LOCAL,
            request_id=request_id,
            dedupe_key=local_import_dedupe_key(request_id),
            payload=local_import_payload(
                source_path="/operator/real/Album", request_id=request_id,
            ),
        )
        action_path = force_action_copy_path(
            cfg_arg, job.id, prefix=LOCAL_IMPORT_ACTION_PREFIX,
        )
        os.makedirs(action_path)
        self.addCleanup(
            lambda: __import__("shutil").rmtree(cfg.processing_dir, ignore_errors=True)
        )
        db.mark_import_job_preview_importable(
            job.id, preview_result={"action_path": action_path},
        )
        claimed = db.claim_local_import_job_under_lock(
            job.id, request_id=request_id, worker_id="w",
        )
        assert claimed is not None
        return claimed, action_path

    def test_local_branch_passes_the_three_documented_deviations(self) -> None:
        db = FakePipelineDB()
        root = tempfile.mkdtemp()
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        cfg = SimpleNamespace(
            processing_dir=os.path.join(root, "processing"),
            slskd_download_dir=os.path.join(root, "slskd"),
            beets_distance_threshold=0.15,
        )
        job, action_path = self._prepared_job(db, cfg)

        captured: dict[str, object] = {}

        def recorder(_db: object, **kwargs: object) -> DispatchOutcome:
            captured.update(kwargs)
            return DispatchOutcome(success=True, message="ok")

        outcome = importer.execute_import_job(
            cast(Any, db),
            job,
            force_dispatch_fn=recorder,
            force_runtime_config=cast(Any, cfg),
        )

        self.assertTrue(outcome.success)
        self.assertEqual(captured["failed_path"], action_path)
        # Decision 2: never the operator's real path.
        self.assertIsNone(captured["source_reference_path"])
        # Decision 3: the ordinary automation threshold, never the force
        # override (which stays None, meaning "use FORCE_IMPORT_DISTANCE_
        # THRESHOLD", for a force job's own call — see the sibling force
        # test below).
        self.assertEqual(captured["distance_threshold"], 0.15)
        # Decision 4: the local-import attempt scenario.
        self.assertEqual(captured["scenario"], "local_import")
        self.assertIsNone(captured["source_username"])
        self.assertIsNone(captured["source_dirs"])
        self.assertIsNone(captured["download_log_id"])

    def test_force_branch_stays_byte_identical(self) -> None:
        """Must-still-work guard: force's own branch is untouched by the
        shared ``_execute_action_copy_dispatch`` refactor."""
        db = FakePipelineDB()
        root = tempfile.mkdtemp()
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        cfg = SimpleNamespace(
            processing_dir=os.path.join(root, "processing"),
            slskd_download_dir=os.path.join(root, "slskd"),
            beets_distance_threshold=0.15,
        )
        request_id = 52
        db.seed_request(make_request_row(
            id=request_id, status="wanted", mb_release_id="mb-52",
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=request_id,
            dedupe_key=force_import_dedupe_key(1),
            payload=force_import_payload(
                download_log_id=1,
                failed_path="/wrong_matches/Album",
                source_username="peer",
                source_dirs=["peer\\Album"],
            ),
        )
        action_path = force_action_copy_path(cast(Any, cfg), job.id)
        os.makedirs(action_path)
        db.mark_import_job_preview_importable(
            job.id, preview_result={"action_path": action_path},
        )
        claimed = db.claim_force_import_job_under_lock(
            job.id, request_id=request_id, worker_id="w",
        )
        assert claimed is not None

        captured: dict[str, object] = {}

        def recorder(_db: object, **kwargs: object) -> DispatchOutcome:
            captured.update(kwargs)
            return DispatchOutcome(success=True, message="ok")

        importer.execute_import_job(
            cast(Any, db), claimed, force_dispatch_fn=recorder,
            force_runtime_config=cast(Any, cfg),
        )

        self.assertEqual(captured["source_reference_path"], "/wrong_matches/Album")
        self.assertIsNone(captured["distance_threshold"])
        self.assertEqual(captured["scenario"], "force_import")
        self.assertEqual(captured["source_username"], "peer")
        self.assertEqual(captured["source_dirs"], ["peer\\Album"])
        self.assertEqual(captured["download_log_id"], 1)


class TestProcessClaimedJobForwardsPinnedSessionToLocal(unittest.TestCase):
    """``process_claimed_job``'s pinned-session/cancellation forwarding
    (the ``is_force`` flag) must cover local-import — it claims through the
    SAME pinned-IMPORT-session path as force (``_process_force_claim``,
    ``claim_fn=_claim_local_import``). Narrowing ``is_force`` back to
    force-only silently drops local-import's real pinned
    ``cancellation_token`` / ``owner_session_identity`` on every dispatch —
    the job still runs, just with no graceful-shutdown cancellation
    support and no pinned session forwarded to ``execute_fn``.
    """

    def test_local_import_job_receives_the_pinned_session(self) -> None:
        db = FakePipelineDB()
        job = _local_job(db)
        token = CancellationToken()
        identity = OwnerSessionIdentity(connection_object_id=1, backend_pid=2)

        captured: dict[str, object] = {}

        def execute_fn(_db: object, _job: object, **kwargs: object) -> DispatchOutcome:
            captured.update(kwargs)
            return DispatchOutcome(success=True, message="ok")

        importer.process_claimed_job(
            cast(Any, db),
            job,
            execute_fn=execute_fn,
            cancellation_token=token,
            owner_session_identity=identity,
        )

        self.assertIs(captured.get("cancellation_token"), token)
        self.assertIs(captured.get("owner_session_identity"), identity)


class TestRunOnceClaimsLocalImportCandidate(unittest.TestCase):
    """Issue #1211 PR3 — no test anywhere drove ``importer.run_once`` all
    the way through the ``IMPORT_JOB_LOCAL`` claim branch (since issue #1278
    the local kind's own ``_claim_route_local_import``, which calls
    ``_process_force_claim(..., claim_fn=_claim_local_import)``
    -> ``process_claimed_job``). #1210 covered ``process_claimed_job`` in
    isolation once already claimed; this proves the claim-loop wiring one
    level up actually reaches and claims a queued local-import candidate.
    """

    def setUp(self) -> None:
        # Mirrors tests/test_import_queue.py::TestImporterWorker.setUp. The
        # terminal cleanup this test exercises (_record_terminal_force_
        # action_cleanup -> cleanup_force_action_copy_for_job) requires the
        # private processing root to be mode 0700 (lib/fs_authority.py) for
        # the reap to SUCCEED -- _cleanup_terminal_force_action's blanket
        # except Exception (scripts/importer.py) means a wrong mode does not
        # fail this test; it silently folds into a logged
        # {"removed": False, "error": "FilesystemAuthorityError: ..."}
        # instead. The mode is asserted here (test_run_once_claims_and_
        # completes_a_local_import_candidate below reads "removed": True and
        # confirms the action copy no longer exists on disk) so that
        # assertion is checking real cleanup, not a silently-failed one.
        # Plain os.makedirs (as TestExecuteImportJobLocalBranchSeam uses,
        # which never reaches terminal cleanup) would leave that assertion
        # false.
        self._root = tempfile.mkdtemp(prefix="cratedigger-local-import-run-once-")
        self.addCleanup(lambda: shutil.rmtree(self._root, ignore_errors=True))
        downloads = os.path.join(self._root, "downloads")
        processing = os.path.join(self._root, "processing")
        os.mkdir(downloads, 0o700)
        os.mkdir(processing, 0o700)
        os.mkdir(os.path.join(processing, "albums"), 0o700)
        os.mkdir(os.path.join(processing, "preview"), 0o700)
        self._cfg = CratediggerConfig(
            slskd_download_dir=downloads,
            processing_dir=processing,
            audio_check_mode="off",
        )
        patcher = patch(
            "lib.config.read_runtime_config", return_value=self._cfg,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_run_once_claims_and_completes_a_local_import_candidate(self) -> None:
        db = FakePipelineDB()
        job = _local_job(db, request_id=90, source_path="/operator/real/Album")
        action_path = force_action_copy_path(
            self._cfg, job.id, prefix=LOCAL_IMPORT_ACTION_PREFIX,
        )
        os.mkdir(action_path, 0o700)
        marked = db.mark_import_job_preview_importable(
            job.id, preview_result={"action_path": action_path}, message="ready",
        )
        assert marked is not None

        dispatch_calls: list[dict[str, object]] = []

        def _recording_dispatch(_db: object, **kwargs: object) -> DispatchOutcome:
            dispatch_calls.append(kwargs)
            return DispatchOutcome(success=True, message="imported")

        def _execute_fn(
            db_arg: object,
            job_arg: ImportJob,
            *,
            ctx: object = None,
            cancellation_token: CancellationToken | None = None,
            owner_session_identity: OwnerSessionIdentity | None = None,
        ) -> DispatchOutcome:
            # The REAL execute_import_job, driven through ITS OWN
            # force_dispatch_fn/force_runtime_config kwarg-DI seam
            # (code-quality.md's preferred strategy over patching
            # lib.dispatch.dispatch_import_from_db, which is not leaf-seam
            # allowlisted) -- process_claimed_job's own execute_fn(...) call
            # never forwards those two kwargs, so this wrapper (injected via
            # run_once's own execute_fn= seam) supplies them instead.
            return importer.execute_import_job(
                db_arg,  # pyright: ignore[reportArgumentType]
                job_arg,
                ctx=ctx,
                cancellation_token=cancellation_token,
                owner_session_identity=owner_session_identity,
                force_dispatch_fn=_recording_dispatch,
                force_runtime_config=self._cfg,
            )

        result = importer.run_once(
            db,  # pyright: ignore[reportArgumentType]
            worker_id="worker",
            stage_db_factory=lambda _dsn: db,
            execute_fn=_execute_fn,
        )

        assert result is not None
        self.assertEqual(result.id, job.id)
        self.assertEqual(result.job_type, IMPORT_JOB_LOCAL)
        # Domain-state assertion (Test Taxonomy #3): the job outcome
        # persisted by the claim-loop, not merely "it ran without raising".
        self.assertEqual(result.status, "completed")
        stored = db.get_import_job(job.id)
        assert stored is not None
        self.assertEqual(stored.status, "completed")
        # ImportJob.result is already dict[str, Any] | None -- no cast
        # needed to index it.
        assert stored.result is not None
        force_action_cleanup = stored.result["force_action_cleanup"]
        assert isinstance(force_action_cleanup, dict)
        # The action copy really was reaped, not silently left behind
        # a logged {"removed": False} (see setUp's docstring).
        self.assertTrue(force_action_cleanup["removed"])
        self.assertFalse(os.path.exists(action_path))

        self.assertEqual(len(dispatch_calls), 1)
        dispatch_kwargs = dispatch_calls[0]
        self.assertEqual(dispatch_kwargs["failed_path"], action_path)
        # Decision 2 (#1176 PR3): local-import never audits the operator's
        # real path.
        self.assertIsNone(dispatch_kwargs["source_reference_path"])
        self.assertEqual(dispatch_kwargs["scenario"], "local_import")


if __name__ == "__main__":
    unittest.main()
