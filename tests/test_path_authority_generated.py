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

import tests._hypothesis_profiles  # noqa: F401
from hypothesis import example, given
from hypothesis import strategies as st

from lib.config import CratediggerConfig
from lib.fs_authority import (
    FilesystemAuthorityError,
    copy_opened_file,
    open_directory_path,
    open_private_processing_root,
    open_regular_relative,
)
from lib.import_preview import (
    PreviewSnapshotLimits,
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
from web.wrong_match_file_service import (
    WrongMatchExplorerLimits,
    build_wrong_match_explorer,
)


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
            cfg = CratediggerConfig(
                slskd_download_dir=source,
                processing_dir=processing,
            )
            if entry_count * 2 > 3:
                with self.assertRaisesRegex(
                    FilesystemAuthorityError, "entry limit",
                ):
                    _snapshot_authorized_directory(
                        source,
                        cfg,
                        limits=PreviewSnapshotLimits(max_entries=3),
                    )
                self.assertEqual(os.listdir(preview), [])
            else:
                snapshot = _snapshot_authorized_directory(
                    source,
                    cfg,
                    limits=PreviewSnapshotLimits(max_entries=3),
                )
                try:
                    self.assertEqual(len(os.listdir(snapshot)), entry_count)
                finally:
                    remove_preview_snapshot(snapshot, cfg)


def assert_generated_publication_invariant(
    *,
    result: object,
    expected_result_type: type[Materialized] | type[MaterializeGuarded],
    expected_detail: str | None,
    source_exists: bool,
    expected_source_exists: bool,
    destination_names: set[str],
    expected_names: set[str],
    artifact_names: list[str],
    name_max: int,
) -> None:
    """Publication proof checker, deliberately independent of its writer."""
    if type(result) is not expected_result_type:
        raise AssertionError(
            f"materialize result was {type(result).__name__}, expected "
            f"{expected_result_type.__name__}",
        )
    if isinstance(result, MaterializeGuarded) and result.detail != expected_detail:
        raise AssertionError(
            f"guard detail was {result.detail!r}, expected {expected_detail!r}",
        )
    if source_exists != expected_source_exists:
        raise AssertionError("source deletion ordering was violated")
    if destination_names != expected_names:
        raise AssertionError("canonical destination was overwritten or incomplete")
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
    expected_audio_paths: list[str],
    expected_other_file_count: int,
    expected_scanned_file_count: int,
    expected_scanned_bytes: int,
) -> None:
    """Explorer limits are inclusive and complete through the exact cap."""
    partial = payload["partial"]
    reason = payload["truncated_reason"]
    if entry_count <= entry_cap:
        if partial is not False or reason is not None:
            raise AssertionError("at-cap explorer result was truncated")
        files = payload["files"]
        if not isinstance(files, list):
            raise AssertionError("explorer files were not a list")
        actual_audio_paths = [
            row.get("relative_path")
            for row in files
            if isinstance(row, dict)
        ]
        if actual_audio_paths != expected_audio_paths:
            raise AssertionError("complete explorer audio paths were not exact")
        if payload["other_file_count"] != expected_other_file_count:
            raise AssertionError("complete explorer other-file count was not exact")
        if payload["scanned_file_count"] != expected_scanned_file_count:
            raise AssertionError("complete explorer scanned-file count was not exact")
        if payload["scanned_bytes"] != expected_scanned_bytes:
            raise AssertionError("complete explorer scanned-byte count was not exact")
        return
    if partial is not True or reason != "entry_limit":
        raise AssertionError("over-budget explorer result was presented as complete")
    scanned_file_count = payload["scanned_file_count"]
    if not isinstance(scanned_file_count, int):
        raise AssertionError("explorer did not return an integer scanned_file_count")
    if scanned_file_count > entry_cap:
        raise AssertionError("explorer scanned more regular files than its entry budget")
    files = payload["files"]
    if not isinstance(files, list) or len(files) > scanned_file_count:
        raise AssertionError("truncated explorer output exceeded its scanned-file budget")


def assert_force_front_gate_invariant(
    *,
    lookup_path: str,
    db_failed_path: str,
    payload_failed_path: str,
    lookup_bytes: bytes,
    expected_db_bytes: bytes,
    snapshot_root: str,
    preview_children: list[str],
) -> None:
    """Force evidence lookup may consume only the DB-authorized snapshot."""
    if lookup_path == payload_failed_path or lookup_path == db_failed_path:
        raise AssertionError("force evidence lookup consumed an unisolated path")
    if os.path.commonpath([lookup_path, snapshot_root]) != snapshot_root:
        raise AssertionError("force evidence lookup escaped private preview")
    if lookup_bytes != expected_db_bytes:
        raise AssertionError("force evidence lookup did not contain DB-authorized bytes")
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


def _private_world() -> tuple[tempfile.TemporaryDirectory[str], str, str, CratediggerConfig]:
    parent = tempfile.TemporaryDirectory(dir=os.getcwd())
    source = os.path.join(parent.name, "downloads")
    processing = os.path.join(parent.name, "processing")
    incoming = os.path.join(parent.name, "Incoming")
    os.mkdir(source)
    os.mkdir(processing, 0o700)
    os.mkdir(os.path.join(processing, "albums"), 0o700)
    os.mkdir(os.path.join(processing, "preview"), 0o700)
    os.mkdir(incoming)
    cfg = CratediggerConfig(
        slskd_download_dir=source,
        processing_dir=processing,
        beets_staging_dir=incoming,
        audio_check_mode="off",
    )
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
            if destination_state == "absent":
                expected_result_type: type[Materialized] | type[MaterializeGuarded] = Materialized
                expected_detail = None
                expected_source_exists = False
                expected_names = {"track.mp3"}
                expected_bytes = b"audio"
            elif destination_state == "complete":
                # Existing exact manifests converge without a second source
                # unlink: they may already be owned by an earlier attempt.
                expected_result_type = Materialized
                expected_detail = None
                expected_source_exists = True
                expected_names = {"track.mp3"}
                expected_bytes = b"existing"
            else:
                expected_result_type = MaterializeGuarded
                expected_detail = "incomplete_or_unsafe_canonical"
                expected_source_exists = True
                expected_names = {"foreign.mp3"} if destination_state == "incomplete" else set()
                expected_bytes = None
            assert_generated_publication_invariant(
                result=result,
                expected_result_type=expected_result_type,
                expected_detail=expected_detail,
                source_exists=os.path.exists(source_path),
                expected_source_exists=expected_source_exists,
                destination_names=set(os.listdir(canonical)),
                expected_names=expected_names,
                artifact_names=os.listdir(albums),
                name_max=os.pathconf(albums, "PC_NAME_MAX"),
            )
            if expected_bytes is not None:
                with open(os.path.join(canonical, "track.mp3"), "rb") as handle:
                    self.assertEqual(handle.read(), expected_bytes)

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
                expected_result_type=Materialized,
                expected_detail=None,
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
    @example(declared_bytes=4, growth_bytes=0, available_bytes=6)
    @example(declared_bytes=5, growth_bytes=0, available_bytes=7)
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
                return copy_opened_file(
                    source_fd,
                    destination_fd,
                    max_bytes=max_bytes,
                    before_write=before_write,
                )

            snapshot: str | None = None
            expected_success = (
                growth_bytes == 0
                and declared_bytes <= 4
                and available_bytes >= declared_bytes + 2
            )
            try:
                snapshot = _snapshot_authorized_directory(
                    source,
                    cfg,
                    limits=PreviewSnapshotLimits(
                        max_bytes=4,
                        free_reserve_bytes=2,
                    ),
                    available_bytes_fn=lambda _preview_fd: available_bytes,
                    copy_fn=grow_before_real_copy,
                )
            except FilesystemAuthorityError as exc:
                snapshot = None
                if expected_success:
                    self.fail(f"expected preview copy success, got {exc}")
                if available_bytes < 2:
                    self.assertEqual(str(exc), "insufficient private preview space")
                elif declared_bytes > 4:
                    self.assertEqual(str(exc), "preview snapshot limit exceeded")
                elif growth_bytes:
                    self.assertEqual(str(exc), "source grew beyond copy limit")
                else:
                    self.assertEqual(str(exc), "insufficient private preview space")
            else:
                if not expected_success:
                    self.fail("preview copy succeeded outside the exact bounded world")
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
                    with open(copied, "rb") as handle:
                        self.assertEqual(handle.read(), b"a" * declared_bytes)
                finally:
                    remove_preview_snapshot(snapshot, cfg)


class TestGeneratedWrongMatchExplorerBounds(unittest.TestCase):
    @given(kinds=st.lists(st.sampled_from(("audio", "other", "directory", "fifo")), min_size=0, max_size=6))
    @example(kinds=["audio", "other", "directory"])
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
            runtime = CratediggerConfig(
                slskd_download_dir=source,
                beets_staging_dir=source,
                processing_dir=os.path.join(source, "processing"),
            )
            limits = WrongMatchExplorerLimits(max_entries=3)
            first = build_wrong_match_explorer(
                download_log_id=1,
                entry=entry,
                cfg=runtime,
                limits=limits,
            )
            second = build_wrong_match_explorer(
                download_log_id=1,
                entry=entry,
                cfg=runtime,
                limits=limits,
            )
            self.assertEqual(
                (first["partial"], first["truncated_reason"], first["scanned_file_count"], first["other_file_count"], first["files"]),
                (second["partial"], second["truncated_reason"], second["scanned_file_count"], second["other_file_count"], second["files"]),
            )
            expected_audio_paths = [
                f"{index:02}-audio.mp3"
                for index, kind in enumerate(kinds)
                if kind == "audio"
            ]
            expected_regular_count = sum(kind in {"audio", "other"} for kind in kinds)
            assert_explorer_entry_invariant(
                entry_count=len(kinds),
                entry_cap=3,
                payload=first,
                expected_audio_paths=expected_audio_paths,
                expected_other_file_count=sum(kind == "other" for kind in kinds),
                expected_scanned_file_count=expected_regular_count,
                expected_scanned_bytes=5 * expected_regular_count,
            )


class TestGeneratedForceFrontGateAuthority(unittest.TestCase):
    @given(
        db_leaf=_SAFE_COMPONENTS,
        payload_leaf=_SAFE_COMPONENTS,
        payload_outside_authority=st.booleans(),
    )
    def test_front_gate_snapshots_only_db_failed_path(
        self,
        db_leaf: str,
        payload_leaf: str,
        payload_outside_authority: bool,
    ) -> None:
        from scripts import import_preview_worker

        parent, source, processing, cfg = _private_world()
        with parent:
            incoming = cfg.beets_staging_dir
            db_path = os.path.join(
                incoming,
                "auto-import",
                f"Database-{db_leaf}",
                "failed_imports",
                "Album",
            )
            payload_root = (
                parent.name
                if payload_outside_authority
                else incoming
            )
            payload_path = os.path.join(
                payload_root,
                "manual",
                f"Payload-{payload_leaf}",
                "failed_imports",
                "Album",
            )
            os.makedirs(db_path)
            os.makedirs(payload_path)
            db_bytes = f"database:{db_leaf}".encode()
            payload_bytes = f"payload:{payload_leaf}".encode()
            self.assertNotEqual(db_bytes, payload_bytes)
            with open(os.path.join(db_path, "01.mp3"), "wb") as handle:
                handle.write(db_bytes)
            with open(os.path.join(payload_path, "01.mp3"), "wb") as handle:
                handle.write(payload_bytes)
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
            captured: list[tuple[str, bytes]] = []

            def capture_lookup(*args: object, **kwargs: object) -> EvidenceBuildResult:
                lookup = str(kwargs["source_path"])
                with open(os.path.join(lookup, "01.mp3"), "rb") as handle:
                    captured.append((lookup, handle.read()))
                return EvidenceBuildResult(None, "missing")

            result, display = import_preview_worker._front_gate_check(
                db,
                job,
                runtime_config=cfg,
                candidate_evidence_loader=capture_lookup,
            )
            self.assertIsNotNone(result)
            self.assertEqual(display, db_path)
            self.assertEqual(len(captured), 1)
            assert_force_front_gate_invariant(
                lookup_path=captured[0][0],
                db_failed_path=db_path,
                payload_failed_path=payload_path,
                lookup_bytes=captured[0][1],
                expected_db_bytes=db_bytes,
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

            def relocate_before_publish(_albums_fd: int, _destination: str) -> None:
                os.rename(processing, relocated)
                os.mkdir(processing, 0o700)
                os.mkdir(os.path.join(processing, "albums"), 0o700)
                os.mkdir(os.path.join(processing, "preview"), 0o700)
                if replacement_extra:
                    with open(os.path.join(processing, "replacement-marker"), "wb") as handle:
                        handle.write(b"replacement")

            result = _materialize_processing_dir(
                album,
                StagedAlbum.from_entry(album, default_path=canonical),
                make_ctx_with_fake_db(FakePipelineDB(), cfg=cfg),
                before_publish=relocate_before_publish,
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
                result=Materialized(),
                expected_result_type=MaterializeGuarded,
                expected_detail="incomplete_or_unsafe_canonical",
                source_exists=True, expected_source_exists=True,
                destination_names={"foreign.mp3"}, expected_names={"track.mp3"},
                artifact_names=[".materialize-tmp-leaked"], name_max=255,
            )

    def test_preview_checker_rejects_residue(self) -> None:
        with self.assertRaises(AssertionError):
            assert_generated_preview_invariant(
                succeeded=False, preview_children=["preview-leaked"], copied_bytes=0,
                expected_bytes=0, lock_path=__file__,
            )

    def test_explorer_checker_rejects_truncation_at_exact_cap(self) -> None:
        with self.assertRaises(AssertionError):
            assert_explorer_entry_invariant(
                entry_count=3,
                entry_cap=3,
                payload={
                    "partial": True,
                    "truncated_reason": "entry_limit",
                    "scanned_file_count": 0,
                    "other_file_count": 0,
                    "scanned_bytes": 0,
                    "files": [],
                },
                expected_audio_paths=[],
                expected_other_file_count=0,
                expected_scanned_file_count=0,
                expected_scanned_bytes=0,
            )

    def test_force_checker_rejects_payload_path(self) -> None:
        with self.assertRaises(AssertionError):
            assert_force_front_gate_invariant(
                lookup_path="/payload", db_failed_path="/db", payload_failed_path="/payload",
                lookup_bytes=b"payload",
                expected_db_bytes=b"database",
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
