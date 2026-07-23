"""Generated companion for #663's descriptor-path authority pins.

The deterministic pins in ``test_path_authority.py`` cover the named attack
shapes.  This property ranges over arbitrary safe leaf names and both regular
and symlink targets: only the same descriptor-rooted regular file is readable.
"""

from __future__ import annotations

import os
import tempfile
import unittest
from collections.abc import Callable
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import tests._hypothesis_profiles  # noqa: F401
from hypothesis import example, given
from hypothesis import strategies as st

from lib.fs_authority import (
    FilesystemAuthorityError,
    open_directory_path,
    open_private_processing_root,
    open_regular_relative,
)
from lib.import_preview import (
    _snapshot_authorized_directory,
    remove_preview_snapshot,
)
from lib.download_materialization import (
    MaterializeGuarded,
    Materialized,
    _materialize_processing_dir,
    _materialize_token,
)
from lib.grab_list import DownloadFile
from lib.import_queue import IMPORT_JOB_FORCE, force_import_dedupe_key, force_import_payload
from lib.processing_paths import canonical_folder_for_row, processing_albums_dir
from lib.quality_evidence import EvidenceBuildResult
from lib.staged_album import StagedAlbum
from tests.fakes import FakePipelineDB
from tests.helpers import make_ctx_with_fake_db, make_grab_list_entry
from web.wrong_match_file_service import build_wrong_match_explorer


_SAFE_COMPONENTS = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyz0123456789_-",
    min_size=1,
    max_size=32,
)


class TestGeneratedDescriptorAuthority(unittest.TestCase):
    @given(name=_SAFE_COMPONENTS, symlink_target=st.booleans())
    def test_only_regular_file_at_the_authorized_descriptor_is_readable(
        self,
        name: str,
        symlink_target: bool,
    ) -> None:
        with tempfile.TemporaryDirectory() as parent:
            root = os.path.join(parent, "root")
            outside = os.path.join(parent, "outside")
            os.mkdir(root)
            with open(outside, "wb") as handle:
                handle.write(b"outside")
            candidate = os.path.join(root, name)
            if symlink_target:
                os.symlink(outside, candidate)
            else:
                with open(candidate, "wb") as handle:
                    handle.write(b"owned")

            with open_directory_path(root) as root_fd:
                if symlink_target:
                    with self.assertRaises(FilesystemAuthorityError):
                        open_regular_relative(root_fd, name)
                else:
                    opened = open_regular_relative(root_fd, name)
                    try:
                        self.assertEqual(os.read(opened.fd, 16), b"owned")
                    finally:
                        opened.close()

    @given(unsafe_ancestor=st.booleans())
    def test_private_root_acceptance_tracks_ancestor_writability(
        self, unsafe_ancestor: bool,
    ) -> None:
        with tempfile.TemporaryDirectory(dir=os.getcwd()) as parent:
            source = os.path.join(parent, "source")
            container = os.path.join(parent, "container")
            processing = os.path.join(container, "processing")
            os.mkdir(source)
            os.mkdir(container, 0o777 if unsafe_ancestor else 0o755)
            os.chmod(container, 0o777 if unsafe_ancestor else 0o755)
            os.mkdir(processing, 0o700)
            if unsafe_ancestor:
                with self.assertRaises(FilesystemAuthorityError):
                    with open_private_processing_root(processing, source):
                        pass
            else:
                with open_private_processing_root(processing, source):
                    pass

    @given(entry_count=st.integers(min_value=0, max_value=6))
    def test_preview_snapshot_total_entry_limit_is_global(
        self, entry_count: int,
    ) -> None:
        """Nested traversal has one total ceiling, not per-directory limits."""
        with tempfile.TemporaryDirectory(dir=os.getcwd()) as parent:
            source = os.path.join(parent, "source")
            processing = os.path.join(parent, "processing")
            os.mkdir(source)
            os.mkdir(processing, 0o700)
            os.mkdir(os.path.join(processing, "albums"), 0o700)
            preview = os.path.join(processing, "preview")
            os.mkdir(preview, 0o700)
            for index in range(entry_count):
                nested = os.path.join(source, f"nested-{index}")
                os.mkdir(nested)
                with open(os.path.join(nested, "track.mp3"), "wb") as handle:
                    handle.write(b"audio")
            cfg = MagicMock()
            cfg.slskd_download_dir = source
            cfg.processing_dir = processing
            with patch("lib.import_preview._PREVIEW_MAX_ENTRIES", 3):
                if entry_count * 2 > 3:
                    with self.assertRaisesRegex(
                        FilesystemAuthorityError, "entry limit",
                    ):
                        _snapshot_authorized_directory(source, cfg)
                    self.assertEqual(os.listdir(preview), [])
                else:
                    snapshot = _snapshot_authorized_directory(source, cfg)
                    try:
                        self.assertEqual(len(os.listdir(snapshot)), entry_count)
                    finally:
                        remove_preview_snapshot(snapshot, cfg)


def assert_generated_publication_invariant(
    *,
    result: object,
    source_exists: bool,
    expected_source_exists: bool,
    destination_names: set[str],
    expected_names: set[str],
    artifact_names: list[str],
    name_max: int,
) -> None:
    """Publication proof checker, deliberately independent of its writer."""
    if source_exists != expected_source_exists:
        raise AssertionError("source deletion ordering was violated")
    if destination_names != expected_names:
        raise AssertionError("canonical destination was overwritten or incomplete")
    if not isinstance(result, (Materialized, MaterializeGuarded)):
        raise AssertionError(f"unexpected materialize result {result!r}")
    if any(len(name.encode("utf-8", "surrogateescape")) > name_max for name in artifact_names):
        raise AssertionError("a materialize artifact exceeds NAME_MAX")
    if any(name.startswith(".materialize-tmp-") for name in artifact_names):
        raise AssertionError("an unpublished materialize temp survived")
    lock_names = [name for name in artifact_names if name.startswith(".materialize-lock-")]
    if any(not name.startswith(".materialize-lock-shard-") for name in lock_names):
        raise AssertionError("materialize lock is not a bounded shard lock")
    if len({name.rsplit("-", 1)[-1] for name in lock_names}) > 256:
        raise AssertionError("materialize used more than 256 lock shards")


def assert_generated_preview_invariant(
    *,
    succeeded: bool,
    preview_children: list[str],
    copied_bytes: int,
    expected_bytes: int,
    lock_path: str,
) -> None:
    """Private preview snapshots either copy exact bytes or clean up fully."""
    if not os.path.isfile(lock_path):
        raise AssertionError("preview lock escaped its stable private root")
    if not succeeded and preview_children:
        raise AssertionError("failed preview copy left a snapshot behind")
    if succeeded and copied_bytes != expected_bytes:
        raise AssertionError("preview copy bytes diverged from the source manifest")


def assert_explorer_entry_invariant(
    *,
    entry_count: int,
    entry_cap: int,
    payload: dict[str, object],
) -> None:
    """A total-entry overflow must never present itself as a complete view."""
    partial = payload["partial"]
    reason = payload["truncated_reason"]
    if entry_count > entry_cap and (partial is not True or reason != "entry_limit"):
        raise AssertionError("over-budget explorer result was presented as complete")
    scanned_file_count = payload["scanned_file_count"]
    if not isinstance(scanned_file_count, int):
        raise AssertionError("explorer did not return an integer scanned_file_count")
    if scanned_file_count > entry_cap:
        raise AssertionError("explorer scanned more regular files than its entry budget")


def assert_force_front_gate_invariant(
    *,
    lookup_path: str,
    db_failed_path: str,
    payload_failed_path: str,
    snapshot_root: str,
    preview_children: list[str],
) -> None:
    """Force evidence lookup may consume only the DB-authorized snapshot."""
    if lookup_path == payload_failed_path or lookup_path == db_failed_path:
        raise AssertionError("force evidence lookup consumed an unisolated path")
    if os.path.commonpath([lookup_path, snapshot_root]) != snapshot_root:
        raise AssertionError("force evidence lookup escaped private preview")
    if preview_children:
        raise AssertionError("force front gate leaked its private snapshot")


def assert_generated_relocation_invariant(
    *,
    result: object,
    source_exists: bool,
    replacement_has_canonical: bool,
) -> None:
    """The descriptor-held old root may publish, but the replacement may not."""
    if not isinstance(result, MaterializeGuarded) or result.detail != "processing_root_relocated":
        raise AssertionError("root relocation did not produce the guarded result")
    if not source_exists or replacement_has_canonical:
        raise AssertionError("root relocation lost source bytes or wrote replacement root")


def _private_world() -> tuple[tempfile.TemporaryDirectory[str], str, str, MagicMock]:
    parent = tempfile.TemporaryDirectory(dir=os.getcwd())
    source = os.path.join(parent.name, "downloads")
    processing = os.path.join(parent.name, "processing")
    os.mkdir(source)
    os.mkdir(processing, 0o700)
    os.mkdir(os.path.join(processing, "albums"), 0o700)
    os.mkdir(os.path.join(processing, "preview"), 0o700)
    cfg = MagicMock()
    cfg.slskd_download_dir = source
    cfg.processing_dir = processing
    cfg.beets_staging_dir = os.path.join(parent.name, "Incoming")
    return parent, source, processing, cfg


class TestGeneratedMaterializePublication(unittest.TestCase):
    @given(
        canonical_bytes=st.integers(min_value=23, max_value=255),
        destination_state=st.sampled_from(("absent", "empty", "complete", "incomplete")),
    )
    @example(canonical_bytes=255, destination_state="absent")
    @example(canonical_bytes=255, destination_state="empty")
    def test_real_materialization_never_overwrites_or_reorders_source_deletion(
        self,
        canonical_bytes: int,
        destination_state: str,
    ) -> None:
        """Generated NAME_MAX/destination worlds drive the real publisher."""
        parent, source, processing, cfg = _private_world()
        with parent:
            source_path = os.path.join(source, "track.mp3")
            with open(source_path, "wb") as handle:
                handle.write(b"audio")
            file = DownloadFile(
                filename="peer\\\\track.mp3", username="peer", id="1",
                file_dir="peer", size=5,
            )
            file.local_path = source_path
            # The fixed suffix/format is stable, so this directly ranges the
            # canonical basename from its practical minimum through NAME_MAX.
            base = make_grab_list_entry(files=[file], artist="A", title="T", year="2020")
            fixed = len(os.path.basename(canonical_folder_for_row(
                base, processing_albums_dir(processing),
            )).encode())
            artist = "A" * (canonical_bytes - fixed + 1)
            album = make_grab_list_entry(files=[file], artist=artist, title="T", year="2020")
            canonical = canonical_folder_for_row(album, processing_albums_dir(processing))
            self.assertEqual(len(os.path.basename(canonical).encode()), canonical_bytes)
            if destination_state != "absent":
                os.mkdir(canonical)
                if destination_state == "complete":
                    with open(os.path.join(canonical, "track.mp3"), "wb") as handle:
                        handle.write(b"existing")
                elif destination_state == "incomplete":
                    with open(os.path.join(canonical, "foreign.mp3"), "wb") as handle:
                        handle.write(b"foreign")
            staged = StagedAlbum.from_entry(album, default_path=canonical)
            result = _materialize_processing_dir(
                album, staged, make_ctx_with_fake_db(FakePipelineDB(), cfg=cfg),
            )
            albums = processing_albums_dir(processing)
            expected_names = (
                {"track.mp3"} if destination_state in ("absent", "complete")
                else ({"foreign.mp3"} if destination_state == "incomplete" else set())
            )
            assert_generated_publication_invariant(
                result=result,
                source_exists=os.path.exists(source_path),
                expected_source_exists=destination_state != "absent",
                destination_names=set(os.listdir(canonical)),
                expected_names=expected_names,
                artifact_names=os.listdir(albums),
                name_max=os.pathconf(albums, "PC_NAME_MAX"),
            )

    @given(name=_SAFE_COMPONENTS)
    def test_real_materialize_artifacts_use_only_fixed_bounded_shards(self, name: str) -> None:
        parent, source, processing, cfg = _private_world()
        with parent:
            source_path = os.path.join(source, "track.mp3")
            with open(source_path, "wb") as handle:
                handle.write(b"audio")
            file = DownloadFile(
                filename="peer\\\\track.mp3", username="peer", id="1",
                file_dir="peer", size=5,
            )
            file.local_path = source_path
            album = make_grab_list_entry(files=[file], artist=name * 16, title=name, year="2020")
            canonical = canonical_folder_for_row(album, processing_albums_dir(processing))
            result = _materialize_processing_dir(
                album, StagedAlbum.from_entry(album, default_path=canonical),
                make_ctx_with_fake_db(FakePipelineDB(), cfg=cfg),
            )
            artifacts = os.listdir(processing_albums_dir(processing))
            lock_names = [entry for entry in artifacts if entry.startswith(".materialize-lock-")]
            self.assertEqual(lock_names, [
                f".materialize-lock-shard-{_materialize_token(os.path.basename(canonical))[:2]}",
            ])
            assert_generated_publication_invariant(
                result=result,
                source_exists=os.path.exists(source_path),
                expected_source_exists=False,
                destination_names=set(os.listdir(canonical)),
                expected_names={"track.mp3"},
                artifact_names=artifacts,
                name_max=os.pathconf(processing_albums_dir(processing), "PC_NAME_MAX"),
            )


class TestGeneratedPreviewCopyBounds(unittest.TestCase):
    @given(
        declared_bytes=st.integers(min_value=0, max_value=5),
        growth_bytes=st.integers(min_value=0, max_value=2),
        available_bytes=st.integers(min_value=0, max_value=7),
    )
    @example(declared_bytes=4, growth_bytes=1, available_bytes=6)
    @example(declared_bytes=4, growth_bytes=0, available_bytes=5)
    def test_real_preview_copy_obeys_caps_growth_and_reserve(
        self,
        declared_bytes: int,
        growth_bytes: int,
        available_bytes: int,
    ) -> None:
        parent, source, processing, cfg = _private_world()
        with parent:
            source_path = os.path.join(source, "track.mp3")
            with open(source_path, "wb") as handle:
                handle.write(b"a" * declared_bytes)
            from lib.fs_authority import copy_opened_file as real_copy_opened_file

            def grow_before_real_copy(
                source_fd: int,
                destination_fd: int,
                *,
                max_bytes: int | None = None,
                before_write: Callable[[int], None] | None = None,
            ) -> int:
                if growth_bytes:
                    with open(source_path, "ab") as handle:
                        handle.write(b"g" * growth_bytes)
                return real_copy_opened_file(
                    source_fd,
                    destination_fd,
                    max_bytes=max_bytes,
                    before_write=before_write,
                )

            statvfs = MagicMock(f_bavail=available_bytes, f_frsize=1)
            snapshot: str | None = None
            with patch("lib.import_preview._PREVIEW_MAX_BYTES", 4), patch(
                "lib.import_preview._PREVIEW_FREE_RESERVE_BYTES", 2,
            ), patch("lib.import_preview.os.fstatvfs", return_value=statvfs), patch(
                "lib.import_preview.copy_opened_file", side_effect=grow_before_real_copy,
            ):
                try:
                    snapshot = _snapshot_authorized_directory(source, cfg)
                except FilesystemAuthorityError:
                    snapshot = None
            preview = os.path.join(processing, "preview")
            if snapshot is None:
                assert_generated_preview_invariant(
                    succeeded=False,
                    preview_children=os.listdir(preview),
                    copied_bytes=0,
                    expected_bytes=0,
                    lock_path=os.path.join(processing, ".preview-snapshot.lock"),
                )
            else:
                try:
                    copied = os.path.join(snapshot, "track.mp3")
                    assert_generated_preview_invariant(
                        succeeded=True,
                        preview_children=os.listdir(preview),
                        copied_bytes=os.path.getsize(copied),
                        expected_bytes=declared_bytes,
                        lock_path=os.path.join(processing, ".preview-snapshot.lock"),
                    )
                finally:
                    remove_preview_snapshot(snapshot, cfg)


class TestGeneratedWrongMatchExplorerBounds(unittest.TestCase):
    @given(kinds=st.lists(st.sampled_from(("audio", "other", "directory", "fifo")), min_size=0, max_size=6))
    @example(kinds=["directory", "directory", "directory", "directory"])
    def test_real_explorer_has_deterministic_total_entry_limit(self, kinds: list[str]) -> None:
        parent, source, processing, cfg = _private_world()
        del processing
        with parent:
            failed = os.path.join(source, "failed_imports", "Album")
            os.makedirs(failed)
            for index, kind in enumerate(kinds):
                path = os.path.join(failed, f"{index:02}-{kind}")
                if kind == "directory":
                    os.mkdir(path)
                elif kind == "fifo":
                    os.mkfifo(path)
                else:
                    suffix = ".mp3" if kind == "audio" else ".txt"
                    with open(f"{path}{suffix}", "wb") as handle:
                        handle.write(b"audio")
            entry = {"validation_result": {"failed_path": failed}}
            runtime = SimpleNamespace(
                slskd_download_dir=source,
                beets_staging_dir=os.path.join(source, "Incoming"),
                processing_dir=os.path.join(source, "processing"),
            )
            with patch("web.wrong_match_file_service._EXPLORER_MAX_ENTRIES", 3), patch(
                "web.wrong_match_file_service.read_runtime_config", return_value=runtime,
            ):
                first = build_wrong_match_explorer(download_log_id=1, entry=entry)
                second = build_wrong_match_explorer(download_log_id=1, entry=entry)
            self.assertEqual(
                (first["partial"], first["truncated_reason"], first["scanned_file_count"], first["other_file_count"], first["files"]),
                (second["partial"], second["truncated_reason"], second["scanned_file_count"], second["other_file_count"], second["files"]),
            )
            assert_explorer_entry_invariant(entry_count=len(kinds), entry_cap=3, payload=first)


class TestGeneratedForceFrontGateAuthority(unittest.TestCase):
    @given(payload_leaf=_SAFE_COMPONENTS)
    def test_front_gate_snapshots_only_db_failed_path(self, payload_leaf: str) -> None:
        from scripts import import_preview_worker

        parent, source, processing, cfg = _private_world()
        with parent:
            db_path = os.path.join(source, "failed_imports", "Database")
            payload_path = os.path.join(parent.name, f"payload-{payload_leaf}")
            os.makedirs(db_path)
            os.makedirs(payload_path)
            with open(os.path.join(db_path, "01.mp3"), "wb") as handle:
                handle.write(b"database")
            with open(os.path.join(payload_path, "01.mp3"), "wb") as handle:
                handle.write(b"payload")
            db = FakePipelineDB()
            log_id = db.log_download(
                42,
                outcome="rejected",
                validation_result={"scenario": "high_distance", "failed_path": db_path},
            )
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(download_log_id=log_id, failed_path=payload_path),
            )
            captured: list[str] = []

            def capture_lookup(*args: object, **kwargs: object) -> EvidenceBuildResult:
                lookup = str(kwargs["source_path"])
                captured.append(lookup)
                self.assertTrue(os.path.exists(os.path.join(lookup, "01.mp3")))
                return EvidenceBuildResult(None, "missing")

            with patch(
                "scripts.import_preview_worker.read_runtime_config", return_value=cfg,
            ), patch(
                "scripts.import_preview_worker.load_candidate_evidence_for_source",
                side_effect=capture_lookup,
            ):
                result, display = import_preview_worker._front_gate_check(db, job)
            self.assertIsNotNone(result)
            self.assertEqual(display, db_path)
            self.assertEqual(len(captured), 1)
            assert_force_front_gate_invariant(
                lookup_path=captured[0],
                db_failed_path=db_path,
                payload_failed_path=payload_path,
                snapshot_root=os.path.join(processing, "preview"),
                preview_children=os.listdir(os.path.join(processing, "preview")),
            )


class TestGeneratedRootRelocation(unittest.TestCase):
    @given(replacement_extra=st.booleans())
    def test_real_publish_relocation_never_commits_to_replacement(self, replacement_extra: bool) -> None:
        parent, source, processing, cfg = _private_world()
        with parent:
            source_path = os.path.join(source, "track.mp3")
            with open(source_path, "wb") as handle:
                handle.write(b"audio")
            file = DownloadFile(
                filename="peer\\\\track.mp3", username="peer", id="1",
                file_dir="peer", size=5,
            )
            file.local_path = source_path
            album = make_grab_list_entry(files=[file], artist="Artist", title="Album", year="2020")
            canonical = canonical_folder_for_row(album, processing_albums_dir(processing))
            relocated = f"{processing}-relocated"
            from lib.fs_authority import rename_relative_noreplace as real_rename

            def relocate_before_real_publish(albums_fd: int, temp: str, destination: str) -> bool:
                os.rename(processing, relocated)
                os.mkdir(processing, 0o700)
                os.mkdir(os.path.join(processing, "albums"), 0o700)
                os.mkdir(os.path.join(processing, "preview"), 0o700)
                if replacement_extra:
                    with open(os.path.join(processing, "replacement-marker"), "wb") as handle:
                        handle.write(b"replacement")
                return real_rename(albums_fd, temp, destination)

            with patch(
                "lib.download_materialization.rename_relative_noreplace",
                side_effect=relocate_before_real_publish,
            ):
                result = _materialize_processing_dir(
                    album,
                    StagedAlbum.from_entry(album, default_path=canonical),
                    make_ctx_with_fake_db(FakePipelineDB(), cfg=cfg),
                )
            assert_generated_relocation_invariant(
                result=result,
                source_exists=os.path.exists(source_path),
                replacement_has_canonical=os.path.exists(canonical),
            )


class TestPathAuthorityProofCheckers(unittest.TestCase):
    """Known-bad proof checks: every invariant checker must reject a lie."""

    def test_publication_checker_rejects_overwrite_source_loss(self) -> None:
        with self.assertRaises(AssertionError):
            assert_generated_publication_invariant(
                result=Materialized(), source_exists=False, expected_source_exists=True,
                destination_names={"foreign.mp3"}, expected_names={"track.mp3"},
                artifact_names=[".materialize-tmp-leaked"], name_max=255,
            )

    def test_preview_checker_rejects_residue(self) -> None:
        with self.assertRaises(AssertionError):
            assert_generated_preview_invariant(
                succeeded=False, preview_children=["preview-leaked"], copied_bytes=0,
                expected_bytes=0, lock_path=__file__,
            )

    def test_explorer_checker_rejects_complete_over_budget_payload(self) -> None:
        with self.assertRaises(AssertionError):
            assert_explorer_entry_invariant(
                entry_count=4, entry_cap=3,
                payload={"partial": False, "truncated_reason": None, "scanned_file_count": 3},
            )

    def test_force_checker_rejects_payload_path(self) -> None:
        with self.assertRaises(AssertionError):
            assert_force_front_gate_invariant(
                lookup_path="/payload", db_failed_path="/db", payload_failed_path="/payload",
                snapshot_root="/preview", preview_children=[],
            )

    def test_relocation_checker_rejects_replacement_commit(self) -> None:
        with self.assertRaises(AssertionError):
            assert_generated_relocation_invariant(
                result=MaterializeGuarded(detail="processing_root_relocated"),
                source_exists=True, replacement_has_canonical=True,
            )


if __name__ == "__main__":
    unittest.main()
