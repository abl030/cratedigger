"""Fail-stop cancellation pins for multi-step processor primitives."""

import errno
import os
import pathlib
import tempfile
import threading
import unittest
from collections.abc import Callable
from types import SimpleNamespace
from unittest.mock import patch

from lib.dispatch.subprocess_runner import run_import_one
from lib.download_materialization import _materialize_processing_dir
from lib.download_rejection import _handle_rejected_result
from lib.download_validation import _process_beets_validation
from lib.fs_authority import copy_opened_file, remove_relative_tree
from lib.grab_list import GrabListEntry
from lib.import_execution import (
    CancellationToken,
    ExecutionCancelled,
)
from lib.import_preview import (
    _remove_preview_tree,
    _snapshot_authorized_directory,
    measure_and_persist_candidate_evidence,
)
from lib.processing_paths import canonical_folder_for_row, processing_albums_dir
from lib.quality import ValidationResult
from lib.quality_evidence import EvidenceBuildResult
from lib.staged_album import StagedAlbum
from tests.fakes import FakePipelineDB
from tests.helpers import (
    handoff_automation_owner,
    make_download_file,
    make_grab_list_entry,
    make_request_row,
)


class _CancelAtCheckpoint(CancellationToken):
    def __init__(self, checkpoint: int) -> None:
        super().__init__()
        self._checkpoint = checkpoint
        self._observed = 0

    def raise_if_cancelled(self) -> None:
        self._observed += 1
        if self._observed == self._checkpoint:
            self.cancel(f"cancel_at_{self._checkpoint}")
        super().raise_if_cancelled()


def _private_roots(raw: str) -> tuple[str, str]:
    download_root = os.path.join(raw, "slskd")
    processing_root = os.path.join(raw, "processing")
    os.mkdir(download_root)
    os.mkdir(processing_root, 0o700)
    os.mkdir(os.path.join(processing_root, "albums"), 0o700)
    os.mkdir(os.path.join(processing_root, "preview"), 0o700)
    return download_root, processing_root


def _materialize_world(
    raw: str,
) -> tuple[GrabListEntry, SimpleNamespace, StagedAlbum, str, str]:
    download_root, processing_root = _private_roots(raw)
    source_dir = os.path.join(download_root, "peer", "album")
    os.makedirs(source_dir)
    source = os.path.join(source_dir, "01.flac")
    pathlib.Path(source).write_bytes(b"audio")
    file = make_download_file(
        filename="peer\\album\\01.flac",
        file_dir="peer\\album",
        username="peer",
    )
    file.local_path = source
    album = make_grab_list_entry(
        files=[file],
        db_request_id=42,
        db_source="request",
    )
    cfg = SimpleNamespace(
        processing_dir=processing_root,
        slskd_download_dir=download_root,
        beets_staging_dir=os.path.join(raw, "staging"),
    )
    db = FakePipelineDB()
    source_adapter = SimpleNamespace(_get_db=lambda: db)
    ctx = SimpleNamespace(cfg=cfg, pipeline_db_source=source_adapter)
    canonical = canonical_folder_for_row(
        album,
        processing_albums_dir(processing_root),
    )
    return album, ctx, StagedAlbum(canonical, request_id=42), source, canonical


class TestMaterializationCancellation(unittest.TestCase):
    def test_cancellation_before_publish_leaves_source_and_private_transaction(
        self,
    ) -> None:
        token = CancellationToken()
        with tempfile.TemporaryDirectory() as raw:
            album, ctx, staged, source, canonical = _materialize_world(raw)

            def cancel_before_publish(_parent_fd: int, _name: str) -> None:
                token.cancel("owner_session_lost")

            with self.assertRaisesRegex(ExecutionCancelled, "owner_session_lost"):
                _materialize_processing_dir(
                    album,
                    staged,
                    ctx,  # pyright: ignore[reportArgumentType]
                    before_publish=cancel_before_publish,
                    cancellation_token=token,
            )

            self.assertTrue(os.path.exists(source))
            self.assertFalse(os.path.exists(canonical))
            self.assertTrue(any(
                name.startswith(".materialize-tmp-")
                for name in os.listdir(processing_albums_dir(ctx.cfg.processing_dir))
            ))

    def test_partial_write_observes_cancellation_before_the_next_write(self) -> None:
        token = CancellationToken()
        with tempfile.TemporaryDirectory() as raw:
            source = pathlib.Path(raw, "source")
            destination = pathlib.Path(raw, "destination")
            source.write_bytes(b"abcdefgh")
            real_write = os.write
            writes = 0

            def partial_write(fd: int, data: memoryview) -> int:
                nonlocal writes
                writes += 1
                count = real_write(fd, data[:4])
                token.cancel("lost_after_partial_write")
                return count

            with source.open("rb") as source_file, destination.open(
                "wb",
            ) as destination_file, patch(
                "lib.fs_authority.os.write",
                side_effect=partial_write,
            ), self.assertRaisesRegex(
                ExecutionCancelled,
                "lost_after_partial_write",
            ):
                copy_opened_file(
                    source_file.fileno(),
                    destination_file.fileno(),
                    before_write=lambda _count: token.raise_if_cancelled(),
                )

            self.assertEqual(writes, 1)
            self.assertEqual(destination.read_bytes(), b"abcd")

    def test_recursive_cleanup_stops_before_the_next_child_mutation(self) -> None:
        token = _CancelAtCheckpoint(2)
        with tempfile.TemporaryDirectory() as raw:
            owned = pathlib.Path(raw, "owned")
            nested = owned / "nested"
            nested.mkdir(parents=True)
            (nested / "a.flac").write_bytes(b"a")
            (nested / "b.flac").write_bytes(b"b")
            parent_fd = os.open(raw, os.O_RDONLY | os.O_DIRECTORY)
            try:
                with self.assertRaisesRegex(ExecutionCancelled, "cancel_at_2"):
                    remove_relative_tree(
                        parent_fd,
                        "owned",
                        before_mutation=token.raise_if_cancelled,
                    )
            finally:
                os.close(parent_fd)

            self.assertFalse((nested / "a.flac").exists())
            self.assertTrue((nested / "b.flac").exists())
            self.assertTrue(owned.is_dir())

    def test_cancellation_after_publish_prevents_source_unlink(self) -> None:
        token = CancellationToken()
        with tempfile.TemporaryDirectory() as raw:
            album, ctx, staged, source, canonical = _materialize_world(raw)
            from lib import download_materialization as materialization

            real_fsync = materialization._fsync_private_directory

            def fsync_then_cancel(fd: int, subject: str) -> None:
                real_fsync(fd, subject)
                if subject == "albums directory":
                    token.cancel("lost_after_publish")

            with patch.object(
                materialization,
                "_fsync_private_directory",
                side_effect=fsync_then_cancel,
            ), self.assertRaisesRegex(ExecutionCancelled, "lost_after_publish"):
                _materialize_processing_dir(
                    album,
                    staged,
                    ctx,  # pyright: ignore[reportArgumentType]
                    cancellation_token=token,
                )

            self.assertTrue(os.path.isfile(os.path.join(canonical, "01.flac")))
            self.assertTrue(os.path.exists(source))


class TestPreviewCancellation(unittest.TestCase):
    def test_snapshot_cancellation_before_first_write_keeps_partial_tree(
        self,
    ) -> None:
        token = CancellationToken()
        with tempfile.TemporaryDirectory() as raw:
            download_root, processing_root = _private_roots(raw)
            source = os.path.join(download_root, "candidate")
            os.mkdir(source)
            pathlib.Path(source, "01.flac").write_bytes(b"audio")
            cfg = SimpleNamespace(
                processing_dir=processing_root,
                slskd_download_dir=download_root,
            )

            def cancel_copy(
                _source_fd: int,
                _destination_fd: int,
                *,
                max_bytes: int,
                before_write: Callable[[int], None],
            ) -> int:
                del max_bytes
                token.cancel("lost_before_copy_write")
                before_write(1)
                raise AssertionError("cancelled write callback returned")

            with self.assertRaisesRegex(
                ExecutionCancelled,
                "lost_before_copy_write",
            ):
                _snapshot_authorized_directory(
                    source,
                    cfg,  # pyright: ignore[reportArgumentType] - minimal path config
                    copy_fn=cancel_copy,
                    cancellation_token=token,
                )

            snapshots = os.listdir(os.path.join(processing_root, "preview"))
            self.assertEqual(len(snapshots), 1)
            self.assertTrue(snapshots[0].startswith("preview-"))

    def test_repair_cancellation_prevents_measurement_and_persistence(self) -> None:
        token = CancellationToken()
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="wanted",
            mb_release_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        ))
        with tempfile.TemporaryDirectory() as raw:
            download_root, processing_root = _private_roots(raw)
            path = os.path.join(processing_root, "albums", "owned")
            os.mkdir(path)
            pathlib.Path(path, "01.mp3").write_bytes(b"audio")
            handoff_automation_owner(
                db,
                42,
                state={
                    "filetype": "mp3",
                    "enqueued_at": "2026-07-29T00:00:00+00:00",
                    "current_path": path,
                    "files": [],
                },
                canonical_path=path,
            )
            cfg = SimpleNamespace(
                processing_dir=processing_root,
                slskd_download_dir=download_root,
                quality_ranks=SimpleNamespace(),
                beets_directory="",
            )
            measured = False

            def repair(_path: str) -> None:
                token.cancel("lost_during_repair")

            def current_loader(*_args: object, **_kwargs: object) -> EvidenceBuildResult:
                return EvidenceBuildResult(None, "empty_current")

            def forbidden_measure(*_args: object, **_kwargs: object) -> object:
                nonlocal measured
                measured = True
                raise AssertionError("measurement ran after cancellation")

            with patch(
                "lib.import_preview.measure_preimport_state",
                side_effect=forbidden_measure,
            ), self.assertRaisesRegex(ExecutionCancelled, "lost_during_repair"):
                measure_and_persist_candidate_evidence(
                    db,
                    request_id=42,
                    path=path,
                    runtime_config=cfg,  # pyright: ignore[reportArgumentType] - minimal config
                    repair_fn=repair,
                    current_evidence_loader=current_loader,
                    cancellation_token=token,
                )

            self.assertFalse(measured)

    def test_cancelled_cleanup_leaves_snapshot_for_recovery(self) -> None:
        token = CancellationToken()
        token.cancel("lost_before_cleanup")
        with tempfile.TemporaryDirectory() as raw:
            download_root, processing_root = _private_roots(raw)
            snapshot = os.path.join(processing_root, "preview", "preview-owned")
            os.mkdir(snapshot)
            pathlib.Path(snapshot, "01.flac").write_bytes(b"audio")
            cfg = SimpleNamespace(
                processing_dir=processing_root,
                slskd_download_dir=download_root,
            )

            with self.assertRaisesRegex(ExecutionCancelled, "lost_before_cleanup"):
                _remove_preview_tree(
                    snapshot,
                    cfg,  # pyright: ignore[reportArgumentType] - minimal path config
                    cancellation_token=token,
                )

            self.assertTrue(os.path.exists(snapshot))


def _refuse_db_use(token: CancellationToken) -> object:
    """A pipeline-DB handle that must never be used in this stage."""
    if token.cancelled:
        raise AssertionError("DB used after cancellation")

    class _Unusable:
        def __getattr__(self, name: str) -> object:
            raise AssertionError(f"DB used during validation: {name}")

    return _Unusable()


class TestValidationAndRejectionCancellation(unittest.TestCase):
    def test_cancellation_after_validation_prevents_the_next_stage(self) -> None:
        token = CancellationToken()
        with tempfile.TemporaryDirectory() as raw:
            path = os.path.join(raw, "album")
            os.mkdir(path)
            pathlib.Path(path, "01.flac").write_bytes(b"audio")
            file = make_download_file(filename="peer\\album\\01.flac")
            album = make_grab_list_entry(
                files=[file],
                db_request_id=42,
                db_source="request",
            )
            ctx = SimpleNamespace(
                cfg=SimpleNamespace(
                    beets_harness_path="/fake/harness",
                    beets_distance_threshold=0.15,
                ),
                pipeline_db_source=SimpleNamespace(
                    # Refuses once the token is cancelled, and hands back a
                    # handle whose every attribute raises before that — so the
                    # test still proves no DB WORK happens after cancellation,
                    # and no DB work happens before it either.
                    _get_db=lambda: _refuse_db_use(token),
                ),
            )

            def validate(*_args: object, **_kwargs: object) -> ValidationResult:
                token.cancel("lost_during_validation")
                return ValidationResult(
                    valid=True,
                    distance=0.01,
                    scenario="strong_match",
                )

            with patch(
                "lib.beets.beets_validate",
                side_effect=validate,
            ), self.assertRaisesRegex(
                ExecutionCancelled,
                "lost_during_validation",
            ):
                _process_beets_validation(
                    album,
                    StagedAlbum(path, request_id=42),
                    ctx,  # pyright: ignore[reportArgumentType] - stage-only context
                    import_job_id=7,
                    cancellation_token=token,
                )

            self.assertTrue(os.path.exists(path))

    def test_cancellation_during_validation_never_reaches_the_merge_seam(
        self,
    ) -> None:
        """An execution that lost its owner mid-harness retags nothing.

        The harness is the long pole in a validation, so an owner can be lost
        while it runs. The checkpoint that catches that sits between the
        harness and the merge seam — the first mutation — because everything
        after it is durable: a MusicBrainz lookup, a retag against the
        SHARED Beets library, and an identity write. Without it, an execution
        with no authority left would still move another process's library.
        """
        token = CancellationToken()
        mirror_calls: list[str] = []
        retag_calls: list[tuple[str, str]] = []

        def canonical(release_id: str) -> str | None:
            mirror_calls.append(release_id)
            return "9b59f78b-3ca6-41e1-8025-6ed4bcfad4e4"

        def retag(_cfg: object, **kwargs: object) -> object:
            retag_calls.append((
                str(kwargs["old_identity"]), str(kwargs["new_identity"]),
            ))
            raise AssertionError("the shared library was retagged after cancel")

        with tempfile.TemporaryDirectory() as raw:
            path = os.path.join(raw, "album")
            os.mkdir(path)
            pathlib.Path(path, "01.flac").write_bytes(b"audio")
            album = make_grab_list_entry(
                files=[make_download_file(filename="peer\\album\\01.flac")],
                db_request_id=42,
                db_source="request",
                mb_release_id="6b209cc5-62b0-4ef7-9336-c2dbd876301a",
            )
            ctx = SimpleNamespace(
                cfg=SimpleNamespace(
                    beets_harness_path="/fake/harness",
                    beets_distance_threshold=0.15,
                ),
                pipeline_db_source=SimpleNamespace(
                    _get_db=lambda: _refuse_db_use(token),
                ),
            )

            def validate(*_args: object, **_kwargs: object) -> ValidationResult:
                # The owner is lost while the harness runs, and the harness
                # reports the one scenario that reaches the merge seam.
                token.cancel("lost_during_validation")
                return ValidationResult(
                    valid=False,
                    scenario="mbid_not_found",
                    detail="Target MBID not in candidates",
                    target_mbid="6b209cc5-62b0-4ef7-9336-c2dbd876301a",
                )

            with patch(
                "lib.beets.beets_validate",
                side_effect=validate,
            ), self.assertRaisesRegex(
                ExecutionCancelled,
                "lost_during_validation",
            ):
                _process_beets_validation(
                    album,
                    StagedAlbum(path, request_id=42),
                    ctx,  # pyright: ignore[reportArgumentType] - stage-only context
                    import_job_id=7,
                    cancellation_token=token,
                    canonical_release_fn=canonical,
                    retag_fn=retag,  # pyright: ignore[reportArgumentType]
                )

            self.assertEqual(mirror_calls, [])
            self.assertEqual(retag_calls, [])

    def test_cancellation_before_quarantine_keeps_source_untouched(self) -> None:
        token = CancellationToken()
        token.cancel("lost_before_quarantine")
        with tempfile.TemporaryDirectory() as raw:
            path = os.path.join(raw, "album")
            os.mkdir(path)
            pathlib.Path(path, "01.flac").write_bytes(b"audio")
            album = make_grab_list_entry(
                files=[make_download_file(filename="peer\\album\\01.flac")],
                db_request_id=42,
                db_source="request",
            )

            with self.assertRaisesRegex(
                ExecutionCancelled,
                "lost_before_quarantine",
            ):
                _handle_rejected_result(
                    album,
                    ValidationResult(
                        valid=False,
                        distance=0.5,
                        scenario="wrong_release",
                    ),
                    StagedAlbum(path, request_id=42),
                    SimpleNamespace(),  # pyright: ignore[reportArgumentType] - unused
                    import_job_id=7,
                    cancellation_token=token,
                )

            self.assertTrue(os.path.isfile(os.path.join(path, "01.flac")))
            self.assertFalse(os.path.exists(os.path.join(raw, "wrong_matches")))

    def test_cancellation_mid_quarantine_leaves_exact_partial_manifest(self) -> None:
        token = _CancelAtCheckpoint(5)
        with tempfile.TemporaryDirectory() as raw:
            path = os.path.join(raw, "album")
            os.mkdir(path)
            pathlib.Path(path, "01.flac").write_bytes(b"one")
            pathlib.Path(path, "02.flac").write_bytes(b"two")
            album = make_grab_list_entry(
                files=[
                    make_download_file(filename="peer\\album\\01.flac"),
                    make_download_file(filename="peer\\album\\02.flac"),
                ],
                db_request_id=42,
                db_source="request",
            )

            with self.assertRaisesRegex(ExecutionCancelled, "cancel_at_5"):
                _handle_rejected_result(
                    album,
                    ValidationResult(
                        valid=False,
                        distance=0.5,
                        scenario="wrong_release",
                    ),
                    StagedAlbum(path, request_id=42),
                    SimpleNamespace(),  # pyright: ignore[reportArgumentType]
                    import_job_id=7,
                    cancellation_token=token,
                )

            target = pathlib.Path(raw, "wrong_matches", "album")
            self.assertEqual(
                sorted(item.name for item in target.iterdir()),
                ["01.flac"],
            )
            self.assertEqual(os.listdir(path), ["02.flac"])


class TestStagedAlbumCancellation(unittest.TestCase):
    def test_nested_entry_moves_atomically_then_cancellation_keeps_both_roots(
        self,
    ) -> None:
        token = _CancelAtCheckpoint(3)
        with tempfile.TemporaryDirectory() as raw:
            source = pathlib.Path(raw, "source")
            nested = source / "Disc 1"
            nested.mkdir(parents=True)
            (nested / "01.flac").write_bytes(b"audio")
            destination = pathlib.Path(raw, "destination")

            with self.assertRaisesRegex(ExecutionCancelled, "cancel_at_3"):
                StagedAlbum(str(source)).move_to(
                    str(destination),
                    cancellation_token=token,
                )

            self.assertTrue(source.is_dir())
            self.assertEqual(os.listdir(source), [])
            self.assertEqual(
                (destination / "Disc 1" / "01.flac").read_bytes(),
                b"audio",
            )

    def test_monitored_move_creates_missing_destination_parents(
        self,
    ) -> None:
        token = CancellationToken()
        with tempfile.TemporaryDirectory() as raw:
            source = pathlib.Path(raw, "source")
            source.mkdir()
            (source / "01.flac").write_bytes(b"audio")
            destination = pathlib.Path(
                raw,
                "Incoming",
                "auto-import",
                "Artist",
                "Album",
            )

            result = StagedAlbum(str(source)).move_to(
                str(destination),
                cancellation_token=token,
            )

            self.assertEqual(result, str(destination))
            self.assertEqual((destination / "01.flac").read_bytes(), b"audio")
            self.assertFalse(source.exists())

    def test_monitored_move_supports_cross_filesystem_fallback(
        self,
    ) -> None:
        token = CancellationToken()
        with tempfile.TemporaryDirectory() as raw:
            source = pathlib.Path(raw, "source")
            source.mkdir()
            (source / "01.flac").write_bytes(b"audio")
            destination = pathlib.Path(raw, "destination")

            with patch(
                "lib.staged_album.os.rename",
                side_effect=OSError(errno.EXDEV, "cross-device link"),
            ):
                result = StagedAlbum(str(source)).move_to(
                    str(destination),
                    cancellation_token=token,
                )

            self.assertEqual(result, str(destination))
            self.assertEqual((destination / "01.flac").read_bytes(), b"audio")
            self.assertFalse(source.exists())


class TestMonitoredImportOneCancellation(unittest.TestCase):
    def test_cancellation_terminates_and_reaps_import_process_group(self) -> None:
        token = CancellationToken()
        with tempfile.TemporaryDirectory() as raw:
            root = pathlib.Path(raw)
            harness = root / "run_beets_harness.sh"
            harness.write_text("# test path anchor\n")
            pid_path = root / "child.pid"
            script = root / "import_one.py"
            script.write_text(
                "import os,pathlib,time\n"
                f"pathlib.Path({str(pid_path)!r}).write_text(str(os.getpid()))\n"
                "time.sleep(30)\n",
            )

            def cancel_when_started() -> None:
                while not pid_path.exists():
                    threading.Event().wait(0.01)
                token.cancel("owner_session_lost")

            canceller = threading.Thread(target=cancel_when_started)
            canceller.start()
            try:
                with self.assertRaisesRegex(
                    ExecutionCancelled,
                    "owner_session_lost",
                ):
                    run_import_one(
                        path=str(root / "source"),
                        mb_release_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                        beets_harness_path=str(harness),
                        cancellation_token=token,
                        timeout=10,
                    )
            finally:
                canceller.join(timeout=2)

            self.assertFalse(canceller.is_alive())
            child_pid = int(pid_path.read_text())
            with self.assertRaises(ProcessLookupError):
                os.kill(child_pid, 0)


if __name__ == "__main__":
    unittest.main()
