"""Filesystem contracts for resumable exact-owner processing cleanup."""

from __future__ import annotations

import copy
import os
import tempfile
import unittest
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from lib.import_execution import CancellationToken, ExecutionCancelled
from lib.pipeline_db import (
    CleanupJournalReceipt,
    ProcessingCleanupJournalRow,
)
from lib.processing_cleanup import (
    PROCESSING_CLEANUP_NO_OP,
    PROCESSING_CLEANUP_QUARANTINE_SOURCE,
    PROCESSING_CLEANUP_REMOVE_SOURCE,
    ProcessingCleanupError,
    cleanup_manifest_builtins,
    cleanup_manifest_hash,
    execute_processing_cleanup,
    inspect_processing_cleanup_source,
)


class _JournalStore:
    def __init__(self, row: ProcessingCleanupJournalRow) -> None:
        self.row = copy.deepcopy(row)
        self.checkpoints: list[dict[str, object]] = []

    def checkpoint_processing_cleanup_journal(
        self,
        *,
        request_id: int,
        job_id: int,
        expected_revision: int,
        step_progress: Mapping[str, object],
    ) -> ProcessingCleanupJournalRow:
        if (
            request_id != self.row["request_id"]
            or job_id != self.row["job_id"]
            or expected_revision != self.row["revision"]
        ):
            raise AssertionError("test journal lost exact owner/revision CAS")
        old_progress = self.row["step_progress"]
        if any(
            key not in step_progress or step_progress[key] != value
            for key, value in old_progress.items()
        ):
            raise AssertionError("test journal progress was rewritten")
        updated = copy.deepcopy(self.row)
        updated["revision"] += 1
        updated["step_progress"] = dict(step_progress)
        updated["updated_at"] = datetime.now(UTC)
        self.row = updated
        self.checkpoints.append(dict(step_progress))
        return copy.deepcopy(updated)

    def complete_processing_cleanup_journal(
        self,
        *,
        request_id: int,
        job_id: int,
        expected_revision: int,
        receipt: CleanupJournalReceipt,
    ) -> ProcessingCleanupJournalRow:
        if (
            request_id != self.row["request_id"]
            or job_id != self.row["job_id"]
            or expected_revision != self.row["revision"]
        ):
            raise AssertionError("test completion lost exact owner/revision CAS")
        if receipt.step_progress != self.row["step_progress"]:
            raise AssertionError("test receipt lost exact progress")
        updated = copy.deepcopy(self.row)
        updated["revision"] += 1
        updated["completed_receipt"] = receipt
        updated["completed_at"] = datetime.now(UTC)
        updated["updated_at"] = datetime.now(UTC)
        self.row = updated
        return copy.deepcopy(updated)


def _journal_row(
    *,
    action: str,
    source_path: str,
    destination_path: str | None = None,
) -> ProcessingCleanupJournalRow:
    inspection = inspect_processing_cleanup_source(source_path)
    if action == PROCESSING_CLEANUP_NO_OP:
        if inspection.status != "missing":
            raise AssertionError("no-op fixture source must be absent")
        manifest = ()
        manifest_hash = cleanup_manifest_hash(manifest)
    else:
        if inspection.status != "complete" or inspection.manifest_hash is None:
            raise AssertionError(f"fixture inspection failed: {inspection}")
        manifest = inspection.manifest
        manifest_hash = inspection.manifest_hash
    now = datetime.now(UTC)
    manifest_json = list(cleanup_manifest_builtins(manifest))
    return ProcessingCleanupJournalRow(
        job_id=17,
        request_id=11,
        revision=1,
        action=action,
        source_path=source_path,
        source_manifest=manifest_json,
        source_manifest_hash=manifest_hash,
        destination_path=destination_path,
        destination_manifest=(
            copy.deepcopy(manifest_json)
            if destination_path is not None
            else None
        ),
        destination_manifest_hash=(
            manifest_hash if destination_path is not None else None
        ),
        selected_destination_path=destination_path,
        step_progress={},
        completed_receipt=None,
        created_at=now,
        updated_at=now,
        completed_at=None,
    )


def _write_tree(root: Path) -> None:
    root.mkdir()
    (root / "01.flac").write_bytes(b"track-one")
    (root / "Disc 2").mkdir()
    (root / "Disc 2" / "02.flac").write_bytes(b"track-two")
    (root / "Disc 2" / "booklet").mkdir()
    (root / "empty").mkdir()


class TestProcessingCleanupInspection(unittest.TestCase):
    def test_complete_missing_and_uninspectable_remain_distinct(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            source = base / "source"
            _write_tree(source)

            complete = inspect_processing_cleanup_source(str(source))
            self.assertEqual(complete.status, "complete")
            self.assertEqual(
                {entry.relative_path for entry in complete.manifest},
                {
                    "01.flac",
                    "Disc 2",
                    "Disc 2/02.flac",
                    "Disc 2/booklet",
                    "empty",
                },
            )
            self.assertEqual(
                complete.manifest_hash,
                cleanup_manifest_hash(complete.manifest),
            )

            missing = inspect_processing_cleanup_source(
                str(base / "missing")
            )
            self.assertEqual(missing.status, "missing")
            self.assertEqual(missing.error_code, "missing")

            unsafe = base / "unsafe"
            unsafe.symlink_to(source, target_is_directory=True)
            uninspectable = inspect_processing_cleanup_source(str(unsafe))
            self.assertEqual(uninspectable.status, "uninspectable")
            self.assertIn(
                uninspectable.error_code,
                {"unsafe_symlink", "not_a_directory"},
            )


class TestProcessingCleanupExecutor(unittest.TestCase):
    def test_cancellation_during_unlink_hashing_prevents_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            source = Path(raw) / "source"
            _write_tree(source)
            store = _JournalStore(
                _journal_row(
                    action=PROCESSING_CLEANUP_REMOVE_SOURCE,
                    source_path=str(source),
                )
            )
            token = CancellationToken()
            checkpoints = 0

            def cancel_after_hash() -> None:
                nonlocal checkpoints
                checkpoints += 1
                if checkpoints == 3:
                    token.cancel("cancelled_during_unlink_hashing")
                token.raise_if_cancelled()

            with self.assertRaises(ExecutionCancelled):
                execute_processing_cleanup(
                    store,
                    store.row,
                    owner_checkpoint=cancel_after_hash,
                )

            self.assertEqual(checkpoints, 3)
            self.assertTrue(source.is_dir())
            self.assertTrue((source / "01.flac").is_file())
            self.assertTrue((source / "Disc 2" / "02.flac").is_file())

    def test_cancellation_during_quarantine_hashing_prevents_rename(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            source = base / "source"
            destination_parent = base / "quarantine"
            destination_parent.mkdir()
            destination = destination_parent / "selected"
            _write_tree(source)
            store = _JournalStore(
                _journal_row(
                    action=PROCESSING_CLEANUP_QUARANTINE_SOURCE,
                    source_path=str(source),
                    destination_path=str(destination),
                )
            )
            token = CancellationToken()
            checkpoints = 0

            def cancel_at_rename_hash() -> None:
                nonlocal checkpoints
                checkpoints += 1
                if checkpoints == 5:
                    token.cancel("cancelled_during_quarantine_hashing")
                token.raise_if_cancelled()

            with self.assertRaises(ExecutionCancelled):
                execute_processing_cleanup(
                    store,
                    store.row,
                    owner_checkpoint=cancel_at_rename_hash,
                )

            self.assertEqual(checkpoints, 5)
            self.assertTrue(source.is_dir())
            self.assertFalse(destination.exists())

    def test_remove_source_uses_exact_manifest_and_completes_receipt(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            source = Path(raw) / "source"
            _write_tree(source)
            store = _JournalStore(
                _journal_row(
                    action=PROCESSING_CLEANUP_REMOVE_SOURCE,
                    source_path=str(source),
                )
            )
            owner_checks = 0

            def owner_checkpoint() -> None:
                nonlocal owner_checks
                owner_checks += 1

            completed = execute_processing_cleanup(
                store,
                store.row,
                owner_checkpoint=owner_checkpoint,
            )

            self.assertFalse(source.exists())
            self.assertGreater(owner_checks, 0)
            receipt = completed["completed_receipt"]
            self.assertIsNotNone(receipt)
            assert receipt is not None
            self.assertEqual(receipt.outcome, "completed")
            self.assertEqual(
                receipt.action,
                PROCESSING_CLEANUP_REMOVE_SOURCE,
            )
            self.assertTrue(
                all(value is True for value in receipt.step_progress.values())
            )

    def test_uninterrupted_remove_hashes_each_source_byte_at_most_twice(
        self,
    ) -> None:
        """Initial authority plus target verification stays linear in bytes."""
        from lib import processing_cleanup

        with tempfile.TemporaryDirectory() as raw:
            source = Path(raw) / "source"
            source.mkdir()
            for track in range(1, 13):
                (source / f"{track:02d}.flac").write_bytes(
                    bytes([track]) * (track * 257)
                )
            source_bytes = sum(
                path.stat().st_size
                for path in source.iterdir()
                if path.is_file()
            )
            store = _JournalStore(
                _journal_row(
                    action=PROCESSING_CLEANUP_REMOVE_SOURCE,
                    source_path=str(source),
                )
            )
            hashed_bytes = 0
            read_opened_file = processing_cleanup._read_opened_file

            def counting_read(opened_fd: int) -> tuple[int, str]:
                nonlocal hashed_bytes
                hashed_bytes += os.fstat(opened_fd).st_size
                return read_opened_file(opened_fd)

            with patch.object(
                processing_cleanup,
                "_read_opened_file",
                side_effect=counting_read,
            ):
                execute_processing_cleanup(
                    store,
                    store.row,
                    owner_checkpoint=lambda: None,
                )

            self.assertFalse(source.exists())
            self.assertLessEqual(hashed_bytes, source_bytes * 2)

    def test_corrupt_and_wrong_match_quarantine_share_atomic_exact_tree_action(
        self,
    ) -> None:
        for quarantine_name in ("bad_files", "wrong_matches"):
            with (
                self.subTest(quarantine=quarantine_name),
                tempfile.TemporaryDirectory() as raw,
            ):
                    base = Path(raw)
                    source = base / "source"
                    destination_parent = base / quarantine_name
                    destination_parent.mkdir()
                    destination = destination_parent / "selected-once"
                    _write_tree(source)
                    expected_files = {
                        path.relative_to(source): path.read_bytes()
                        for path in source.rglob("*")
                        if path.is_file()
                    }
                    store = _JournalStore(
                        _journal_row(
                            action=PROCESSING_CLEANUP_QUARANTINE_SOURCE,
                            source_path=str(source),
                            destination_path=str(destination),
                        )
                    )

                    completed = execute_processing_cleanup(
                        store,
                        store.row,
                        owner_checkpoint=lambda: None,
                    )

                    self.assertFalse(source.exists())
                    self.assertTrue(destination.is_dir())
                    self.assertEqual(
                        {
                            path.relative_to(destination): path.read_bytes()
                            for path in destination.rglob("*")
                            if path.is_file()
                        },
                        expected_files,
                    )
                    receipt = completed["completed_receipt"]
                    self.assertIsNotNone(receipt)
                    assert receipt is not None
                    self.assertEqual(
                        receipt.selected_destination_path,
                        str(destination),
                    )

    def test_recovery_close_no_op_requires_positive_absence(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            missing = Path(raw) / "missing"
            store = _JournalStore(
                _journal_row(
                    action=PROCESSING_CLEANUP_NO_OP,
                    source_path=str(missing),
                )
            )
            completed = execute_processing_cleanup(
                store,
                store.row,
                owner_checkpoint=lambda: None,
            )
            receipt = completed["completed_receipt"]
            self.assertIsNotNone(receipt)
            assert receipt is not None
            self.assertEqual(receipt.outcome, "no_op")

            source = Path(raw) / "appeared"
            source.mkdir()
            existing_store = _JournalStore(
                ProcessingCleanupJournalRow(
                    **{
                        **dict(store.row),
                        "source_path": str(source),
                        "completed_receipt": None,
                        "completed_at": None,
                        "revision": 1,
                        "step_progress": {},
                    }
                )
            )
            with self.assertRaises(ProcessingCleanupError) as conflict:
                execute_processing_cleanup(
                    existing_store,
                    existing_store.row,
                    owner_checkpoint=lambda: None,
                )
            self.assertEqual(conflict.exception.code, "manifest_drift")

    def test_manifest_drift_and_destination_collision_fail_before_mutation(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            source = base / "source"
            quarantine = base / "quarantine"
            quarantine.mkdir()
            destination = quarantine / "selected"
            _write_tree(source)
            drift_store = _JournalStore(
                _journal_row(
                    action=PROCESSING_CLEANUP_REMOVE_SOURCE,
                    source_path=str(source),
                )
            )
            (source / "01.flac").write_bytes(b"changed")
            with self.assertRaises(ProcessingCleanupError) as drift:
                execute_processing_cleanup(
                    drift_store,
                    drift_store.row,
                    owner_checkpoint=lambda: None,
                )
            self.assertEqual(drift.exception.code, "manifest_drift")
            self.assertTrue(source.exists())
            self.assertEqual(drift_store.row["revision"], 1)

            # Re-plan the now-current source, then prove the persisted selected
            # destination is never recomputed around a collision.
            collision_store = _JournalStore(
                _journal_row(
                    action=PROCESSING_CLEANUP_QUARANTINE_SOURCE,
                    source_path=str(source),
                    destination_path=str(destination),
                )
            )
            destination.mkdir()
            (destination / "foreign").write_text("occupied")
            with self.assertRaises(ProcessingCleanupError) as collision:
                execute_processing_cleanup(
                    collision_store,
                    collision_store.row,
                    owner_checkpoint=lambda: None,
                )
            self.assertEqual(
                collision.exception.code,
                "destination_collision",
            )
            self.assertTrue(source.exists())
            self.assertEqual(collision_store.row["revision"], 1)

    def test_uninspectable_source_fails_closed_not_as_missing(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            source = Path(raw) / "source"
            _write_tree(source)
            store = _JournalStore(
                _journal_row(
                    action=PROCESSING_CLEANUP_REMOVE_SOURCE,
                    source_path=str(source),
                )
            )
            held_source = Path(raw) / "held-source"
            source.rename(held_source)
            source.symlink_to(held_source, target_is_directory=True)
            with self.assertRaises(ProcessingCleanupError) as blocked:
                execute_processing_cleanup(
                    store,
                    store.row,
                    owner_checkpoint=lambda: None,
                )
            self.assertEqual(
                blocked.exception.code,
                "source_uninspectable",
            )
            self.assertTrue(source.exists())
            self.assertTrue(source.is_symlink())
            self.assertEqual(store.row["revision"], 1)

    def test_crash_after_every_boundary_resumes_without_path_inference(
        self,
    ) -> None:
        def run_once(
            raw: str,
            *,
            fail_label: str | None,
        ) -> tuple[list[str], _JournalStore, Path]:
            source = Path(raw) / "source"
            _write_tree(source)
            store = _JournalStore(
                _journal_row(
                    action=PROCESSING_CLEANUP_REMOVE_SOURCE,
                    source_path=str(source),
                )
            )
            boundaries: list[str] = []

            def after_boundary(label: str) -> None:
                boundaries.append(label)
                if label == fail_label:
                    raise RuntimeError(f"crash after {label}")

            if fail_label is None:
                execute_processing_cleanup(
                    store,
                    store.row,
                    owner_checkpoint=lambda: None,
                    after_boundary=after_boundary,
                )
            else:
                with self.assertRaisesRegex(RuntimeError, "crash after"):
                    execute_processing_cleanup(
                        store,
                        store.row,
                        owner_checkpoint=lambda: None,
                        after_boundary=after_boundary,
                    )
            return boundaries, store, source

        with tempfile.TemporaryDirectory() as baseline_raw:
            boundaries, _store, _source = run_once(
                baseline_raw,
                fail_label=None,
            )
        self.assertEqual(len(boundaries), len(set(boundaries)))

        for boundary in boundaries:
            with (
                self.subTest(boundary=boundary),
                tempfile.TemporaryDirectory() as raw,
            ):
                    _seen, store, source = run_once(
                        raw,
                        fail_label=boundary,
                    )
                    completed = execute_processing_cleanup(
                        store,
                        store.row,
                        owner_checkpoint=lambda: None,
                    )
                    self.assertFalse(source.exists())
                    self.assertIsNotNone(completed["completed_receipt"])

    def test_cancellation_never_rolls_back_an_atomic_quarantine(self) -> None:
        for cancel_phase in ("journaled:rename", "mutated:rename"):
            with (
                self.subTest(cancel_phase=cancel_phase),
                tempfile.TemporaryDirectory() as raw,
            ):
                    base = Path(raw)
                    source = base / "source"
                    destination_parent = base / "quarantine"
                    destination_parent.mkdir()
                    destination = destination_parent / "selected"
                    _write_tree(source)
                    store = _JournalStore(
                        _journal_row(
                            action=PROCESSING_CLEANUP_QUARANTINE_SOURCE,
                            source_path=str(source),
                            destination_path=str(destination),
                        )
                    )
                    token = CancellationToken()

                    def cancel_at_boundary(
                        label: str,
                        *,
                        expected: str = cancel_phase,
                        current_token: CancellationToken = token,
                    ) -> None:
                        if label == expected:
                            current_token.cancel(f"test:{label}")

                    with self.assertRaises(ExecutionCancelled):
                        execute_processing_cleanup(
                            store,
                            store.row,
                            owner_checkpoint=token.raise_if_cancelled,
                            after_boundary=cancel_at_boundary,
                        )
                    if cancel_phase == "journaled:rename":
                        self.assertTrue(source.exists())
                        self.assertFalse(destination.exists())
                    else:
                        self.assertFalse(source.exists())
                        self.assertTrue(destination.exists())

                    resumed = execute_processing_cleanup(
                        store,
                        store.row,
                        owner_checkpoint=lambda: None,
                    )
                    self.assertFalse(source.exists())
                    self.assertTrue(destination.exists())
                    self.assertIsNotNone(resumed["completed_receipt"])

    def test_quarantine_creates_only_its_journaled_immediate_parent(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            source = base / "source"
            destination_parent = base / "quarantine"
            destination = destination_parent / "selected"
            _write_tree(source)
            store = _JournalStore(
                _journal_row(
                    action=PROCESSING_CLEANUP_QUARANTINE_SOURCE,
                    source_path=str(source),
                    destination_path=str(destination),
                )
            )
            boundaries: list[str] = []

            completed = execute_processing_cleanup(
                store,
                store.row,
                owner_checkpoint=lambda: None,
                after_boundary=boundaries.append,
            )

            self.assertFalse(source.exists())
            self.assertTrue(destination.is_dir())
            self.assertEqual(
                boundaries[:3],
                [
                    "journaled:destination_parent",
                    "mutated:destination_parent",
                    "checkpointed:destination_parent",
                ],
            )
            self.assertEqual(
                store.checkpoints[0],
                {"before:destination_parent": True},
            )
            self.assertEqual(
                store.checkpoints[1],
                {
                    "before:destination_parent": True,
                    "after:destination_parent": True,
                },
            )
            self.assertIsNotNone(completed["completed_receipt"])

    def test_quarantine_parent_creation_resumes_after_every_boundary(self) -> None:
        for cancel_phase in (
            "journaled:destination_parent",
            "mutated:destination_parent",
            "checkpointed:destination_parent",
        ):
            with (
                self.subTest(cancel_phase=cancel_phase),
                tempfile.TemporaryDirectory() as raw,
            ):
                base = Path(raw)
                source = base / "source"
                destination_parent = base / "quarantine"
                destination = destination_parent / "selected"
                _write_tree(source)
                store = _JournalStore(
                    _journal_row(
                        action=PROCESSING_CLEANUP_QUARANTINE_SOURCE,
                        source_path=str(source),
                        destination_path=str(destination),
                    )
                )

                def crash(
                    label: str,
                    *,
                    expected: str = cancel_phase,
                ) -> None:
                    if label == expected:
                        raise RuntimeError(f"crash after {label}")

                with self.assertRaisesRegex(RuntimeError, "crash after"):
                    execute_processing_cleanup(
                        store,
                        store.row,
                        owner_checkpoint=lambda: None,
                        after_boundary=crash,
                    )
                self.assertTrue(source.exists())
                self.assertEqual(
                    destination_parent.exists(),
                    cancel_phase != "journaled:destination_parent",
                )

                completed = execute_processing_cleanup(
                    store,
                    store.row,
                    owner_checkpoint=lambda: None,
                )
                self.assertFalse(source.exists())
                self.assertTrue(destination.exists())
                self.assertIsNotNone(completed["completed_receipt"])

    def test_quarantine_refuses_unsafe_or_missing_journaled_parent(self) -> None:
        for parent_state in ("symlink", "removed_after_checkpoint"):
            with (
                self.subTest(parent_state=parent_state),
                tempfile.TemporaryDirectory() as raw,
            ):
                base = Path(raw)
                source = base / "source"
                destination_parent = base / "quarantine"
                destination = destination_parent / "selected"
                _write_tree(source)
                store = _JournalStore(
                    _journal_row(
                        action=PROCESSING_CLEANUP_QUARANTINE_SOURCE,
                        source_path=str(source),
                        destination_path=str(destination),
                    )
                )
                if parent_state == "symlink":
                    foreign = base / "foreign"
                    foreign.mkdir()
                    destination_parent.symlink_to(foreign, target_is_directory=True)
                else:
                    destination_parent.mkdir()

                    def crash_after_parent(label: str) -> None:
                        if label == "checkpointed:destination_parent":
                            raise RuntimeError("planted parent checkpoint crash")

                    with self.assertRaisesRegex(
                        RuntimeError,
                        "planted parent checkpoint crash",
                    ):
                        execute_processing_cleanup(
                            store,
                            store.row,
                            owner_checkpoint=lambda: None,
                            after_boundary=crash_after_parent,
                        )
                    destination_parent.rmdir()

                with self.assertRaises(ProcessingCleanupError) as raised:
                    execute_processing_cleanup(
                        store,
                        store.row,
                        owner_checkpoint=lambda: None,
                    )
                self.assertEqual(
                    raised.exception.code,
                    (
                        "destination_collision"
                        if parent_state == "symlink"
                        else "destination_uninspectable"
                    ),
                )
                self.assertTrue(source.exists())
                self.assertFalse(destination.exists())

    def test_quarantine_and_no_op_resume_after_every_boundary(self) -> None:
        for action in (
            PROCESSING_CLEANUP_QUARANTINE_SOURCE,
            PROCESSING_CLEANUP_NO_OP,
        ):
            with self.subTest(action=action):
                with tempfile.TemporaryDirectory() as baseline_raw:
                    baseline = Path(baseline_raw)
                    source = baseline / "source"
                    destination: Path | None = None
                    if action == PROCESSING_CLEANUP_QUARANTINE_SOURCE:
                        _write_tree(source)
                        quarantine = baseline / "quarantine"
                        quarantine.mkdir()
                        destination = quarantine / "selected"
                    store = _JournalStore(
                        _journal_row(
                            action=action,
                            source_path=str(source),
                            destination_path=(
                                str(destination)
                                if destination is not None
                                else None
                            ),
                        )
                    )
                    boundaries: list[str] = []
                    execute_processing_cleanup(
                        store,
                        store.row,
                        owner_checkpoint=lambda: None,
                        after_boundary=boundaries.append,
                    )

                for crash_boundary in boundaries:
                    with (
                        self.subTest(
                            action=action,
                            boundary=crash_boundary,
                        ),
                        tempfile.TemporaryDirectory() as raw,
                    ):
                        base = Path(raw)
                        source = base / "source"
                        destination = None
                        if action == PROCESSING_CLEANUP_QUARANTINE_SOURCE:
                            _write_tree(source)
                            quarantine = base / "quarantine"
                            quarantine.mkdir()
                            destination = quarantine / "selected"
                        store = _JournalStore(
                            _journal_row(
                                action=action,
                                source_path=str(source),
                                destination_path=(
                                    str(destination)
                                    if destination is not None
                                    else None
                                ),
                            )
                        )

                        def crash(
                            label: str,
                            *,
                            expected: str = crash_boundary,
                        ) -> None:
                            if label == expected:
                                raise RuntimeError(
                                    f"crash after {label}"
                                )

                        with self.assertRaisesRegex(
                            RuntimeError,
                            "crash after",
                        ):
                            execute_processing_cleanup(
                                store,
                                store.row,
                                owner_checkpoint=lambda: None,
                                after_boundary=crash,
                            )
                        completed = execute_processing_cleanup(
                            store,
                            store.row,
                            owner_checkpoint=lambda: None,
                        )
                        self.assertFalse(source.exists())
                        if destination is not None:
                            self.assertTrue(destination.exists())
                        self.assertIsNotNone(
                            completed["completed_receipt"]
                        )

    def test_collision_after_quarantine_precheckpoint_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            source = base / "source"
            quarantine = base / "quarantine"
            quarantine.mkdir()
            destination = quarantine / "selected"
            _write_tree(source)
            store = _JournalStore(
                _journal_row(
                    action=PROCESSING_CLEANUP_QUARANTINE_SOURCE,
                    source_path=str(source),
                    destination_path=str(destination),
                )
            )

            def crash_after_plan(label: str) -> None:
                if label == "journaled:rename":
                    raise RuntimeError("crash after journaled rename")

            with self.assertRaisesRegex(RuntimeError, "journaled rename"):
                execute_processing_cleanup(
                    store,
                    store.row,
                    owner_checkpoint=lambda: None,
                    after_boundary=crash_after_plan,
                )
            destination.mkdir()
            (destination / "foreign").write_text("collision")

            with self.assertRaises(ProcessingCleanupError) as collision:
                execute_processing_cleanup(
                    store,
                    store.row,
                    owner_checkpoint=lambda: None,
                )
            self.assertEqual(
                collision.exception.code,
                "destination_collision",
            )
            self.assertTrue(source.exists())
            self.assertTrue(destination.exists())


if __name__ == "__main__":
    unittest.main()
