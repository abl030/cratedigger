"""Generated companion for #663's descriptor-path authority pins.

The deterministic pins in ``test_path_authority.py`` cover the named attack
shapes.  This property ranges over arbitrary safe leaf names and both regular
and symlink targets: only the same descriptor-rooted regular file is readable.
"""

from __future__ import annotations

import os
import re
import tempfile
import unittest
from collections.abc import Callable, Sequence
from functools import partial
from itertools import product

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.config import CratediggerConfig
from lib.download_materialization import (
    Materialized,
    MaterializeFailed,
    MaterializeGuarded,
    _materialize_processing_dir,
    _materialize_token,
)
from lib.fs_authority import (
    FilesystemAuthorityError,
    copy_opened_file,
    local_import_owned_subtrees,
    open_configured_local_import_directory,
    open_directory_path,
    open_private_processing_root,
    open_regular_relative,
)
from lib.grab_list import DownloadFile
from lib.import_preview import (
    PreviewSnapshotLimits,
    _snapshot_authorized_directory,
    remove_preview_snapshot,
)
from lib.import_queue import (
    IMPORT_JOB_FORCE,
    force_import_dedupe_key,
    force_import_payload,
)
from lib.processing_paths import canonical_folder_for_row, processing_albums_dir
from lib.quality_evidence import EvidenceBuildResult
from lib.staged_album import StagedAlbum
from tests.fakes import FakePipelineDB
from tests.helpers import (
    make_ctx_with_fake_db,
    make_grab_list_entry,
    make_request_row,
)
from tests.test_path_authority import assert_publication_invariant
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

    def test_private_root_acceptance_tracks_ancestor_writability(self) -> None:
        for unsafe_ancestor in (False, True):
            with (
                self.subTest(unsafe_ancestor=unsafe_ancestor),
                tempfile.TemporaryDirectory() as parent,
            ):
                source = os.path.join(parent, "source")
                container = os.path.join(parent, "container")
                processing = os.path.join(container, "processing")
                os.mkdir(source)
                os.mkdir(container, 0o777 if unsafe_ancestor else 0o755)
                os.chmod(container, 0o777 if unsafe_ancestor else 0o755)
                os.mkdir(processing, 0o700)
                if unsafe_ancestor:
                    with (
                        self.assertRaises(FilesystemAuthorityError),
                        open_private_processing_root(processing, source),
                    ):
                        pass
                else:
                    with open_private_processing_root(processing, source):
                            pass

    def test_preview_snapshot_total_entry_limit_is_global(
        self,
    ) -> None:
        """Nested traversal has one total ceiling, not per-directory limits."""
        cases = (
            ("empty tree", (), True),
            ("one directory and file below limit", (1,), True),
            ("one directory and two files at limit", (2,), True),
            ("first directory and file pair above limit", (1, 1), False),
        )
        for case, nested_file_counts, expected_success in cases:
            with (
                self.subTest(case=case),
                tempfile.TemporaryDirectory() as parent,
            ):
                source = os.path.join(parent, "source")
                processing = os.path.join(parent, "processing")
                os.mkdir(source)
                os.mkdir(processing, 0o700)
                os.mkdir(os.path.join(processing, "albums"), 0o700)
                preview = os.path.join(processing, "preview")
                os.mkdir(preview, 0o700)
                for index, file_count in enumerate(nested_file_counts):
                    nested = os.path.join(source, f"nested-{index}")
                    os.mkdir(nested)
                    for track_index in range(file_count):
                        with open(
                            os.path.join(nested, f"track-{track_index}.mp3"),
                            "wb",
                        ) as handle:
                            handle.write(b"audio")
                cfg = CratediggerConfig(
                    slskd_download_dir=source,
                    processing_dir=processing,
                )
                if not expected_success:
                    with self.assertRaisesRegex(
                        FilesystemAuthorityError,
                        "entry limit",
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
                        self.assertEqual(
                            len(os.listdir(snapshot)),
                            len(nested_file_counts),
                        )
                    finally:
                        remove_preview_snapshot(snapshot, cfg)


def assert_generated_publication_invariant(
    *,
    result: object,
    expected_result_type: type[Materialized | MaterializeGuarded],
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


_RESULT_TYPES: dict[str, type] = {
    "materialized": Materialized,
    "guarded": MaterializeGuarded,
    "failed": MaterializeFailed,
}

_RESULT_FACTORIES: dict[str, Callable[[], object]] = {
    "materialized": Materialized,
    "guarded": lambda: MaterializeGuarded(detail="incomplete_or_unsafe_canonical"),
    "failed": lambda: MaterializeFailed(reason="slskd_root_missing"),
}

# Stated here rather than read off the production default, for the same
# reason as #868 I3: an assertion derived from the thing under test cannot
# detect that thing widening.
DEFAULT_PUBLICATION_RESULT_KINDS = frozenset({"materialized", "guarded"})


def assert_result_type_gate(
    *,
    accepted: bool,
    result_kind: str,
    allowed_kinds: frozenset[str] | None,
) -> None:
    """The publication checker's result-type gate, stated independently.

    ``allowed_kinds is None`` means the call site took the DEFAULT tuple,
    which admits exactly a publication and a guard — never a refusal (#882
    item 6: the default was the one arm no known-bad pin exercised).
    """
    effective = (
        DEFAULT_PUBLICATION_RESULT_KINDS
        if allowed_kinds is None
        else allowed_kinds
    )
    if accepted != (result_kind in effective):
        verdict = "accepted" if accepted else "rejected"
        raise AssertionError(
            f"materialize result {result_kind!r} was {verdict} against "
            f"allowed kinds {sorted(effective)!r}",
        )


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
    expected_unreadable_count: int = 0,
) -> None:
    """Explorer limits are inclusive and complete through the exact cap.

    ``expected_unreadable_count`` covers a REFUSED entry that has nothing
    to do with the cap — a FIFO answers ``not_regular_file`` at open and
    is counted since issue #1086 (previously silently dropped). An
    at-cap listing holding one is honestly ``partial`` for THAT reason,
    never a truncation reason: the two causes of an incomplete listing
    stay distinguishable (``truncated_reason`` is LIMITS only).
    """
    partial = payload["partial"]
    reason = payload["truncated_reason"]
    if entry_count <= entry_cap:
        if expected_unreadable_count:
            if partial is not True or reason is not None:
                raise AssertionError(
                    "an at-cap listing with a refused (non-cap) entry was "
                    "not honestly flagged partial"
                )
            if payload.get("unreadable_entry_count") != expected_unreadable_count:
                raise AssertionError(
                    "unreadable_entry_count did not match the refused "
                    "(non-cap) entries"
                )
        elif partial is not False or reason is not None:
            raise AssertionError("at-cap explorer result was truncated")
        # The refusal above changes ``partial``, never what was actually
        # read: the readable portion must still be exact either way.
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
        raise AssertionError(  # noqa: TRY004 - generated invariant failure
            "explorer did not return an integer scanned_file_count"
        )
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
    """Force evidence lookup may consume only the DB-authorized action copy."""
    if lookup_path == payload_failed_path or lookup_path == db_failed_path:
        raise AssertionError("force evidence lookup consumed an unisolated path")
    if os.path.commonpath([lookup_path, snapshot_root]) != snapshot_root:
        raise AssertionError("force evidence lookup escaped private action root")
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
    parent = tempfile.TemporaryDirectory()
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


def _publish_race_winner(destination_state: str) -> Callable[[int, str], None]:
    """Build a competing writer that publishes first, under our own lock.

    ``_materialize_processing_dir`` only leaves an unpublished transaction
    directory behind when its ``renameat2(RENAME_NOREPLACE)`` loses, so this
    is the only shape that can prove the transaction is reclaimed.
    """
    name = "track.mp3" if destination_state == "race_exact" else "foreign.mp3"
    payload = b"existing" if destination_state == "race_exact" else b"foreign"

    def win_the_publish(albums_fd: int, destination: str) -> None:
        os.mkdir(destination, 0o700, dir_fd=albums_fd)
        winner_fd = os.open(
            destination,
            os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
            dir_fd=albums_fd,
        )
        try:
            written = os.open(
                name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
                0o600,
                dir_fd=winner_fd,
            )
            try:
                os.write(written, payload)
            finally:
                os.close(written)
        finally:
            os.close(winner_fd)

    return win_the_publish


class TestGeneratedMaterializePublication(unittest.TestCase):
    @given(
        canonical_bytes=st.integers(min_value=23, max_value=255),
        destination_state=st.sampled_from((
            "absent",
            "empty",
            "complete",
            "incomplete",
            # A writer that bypassed this process's shard lock wins between
            # our preflight and our publish. Only these two states create a
            # real unpublished transaction directory, so they are the only
            # worlds that can prove it is reclaimed (issue #1094).
            "race_exact",
            "race_foreign",
        )),
    )
    @example(canonical_bytes=255, destination_state="absent")
    @example(canonical_bytes=255, destination_state="empty")
    @example(canonical_bytes=255, destination_state="race_exact")
    @example(canonical_bytes=255, destination_state="race_foreign")
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
            if destination_state in ("empty", "complete", "incomplete"):
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
                before_publish=(
                    _publish_race_winner(destination_state)
                    if destination_state.startswith("race_")
                    else None
                ),
            )
            albums = processing_albums_dir(processing)
            if destination_state == "absent":
                expected_result_type: type[Materialized | MaterializeGuarded] = Materialized
                expected_detail = None
                expected_source_exists = False
                expected_names = {"track.mp3"}
                expected_bytes = b"audio"
            elif destination_state in ("complete", "race_exact"):
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
                expected_names = (
                    {"foreign.mp3"}
                    if destination_state in ("incomplete", "race_foreign")
                    else set()
                )
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

    def test_real_lock_shards_stay_bounded_across_many_albums(self) -> None:
        """One album per world can never exceed the shard ceiling.

        The bound is a property of the ACCUMULATED ``albums/`` root, so it
        is only observable once many distinct canonical names have taken
        their locks in the same private tree (issue #1094).
        """
        album_count = 300
        parent, source, processing, cfg = _private_world()
        albums = processing_albums_dir(processing)
        with parent:
            result: object = None
            canonical = albums
            for index in range(album_count):
                source_path = os.path.join(source, f"track-{index}.mp3")
                with open(source_path, "wb") as handle:
                    handle.write(b"audio")
                file = DownloadFile(
                    filename=f"peer\\\\track-{index}.mp3", username="peer",
                    id=str(index), file_dir="peer", size=5,
                )
                file.local_path = source_path
                album = make_grab_list_entry(
                    files=[file], artist="Artist", title=f"Album {index}",
                    year="2020",
                )
                canonical = canonical_folder_for_row(album, albums)
                result = _materialize_processing_dir(
                    album,
                    StagedAlbum.from_entry(album, default_path=canonical),
                    make_ctx_with_fake_db(FakePipelineDB(), cfg=cfg),
                )
            artifacts = os.listdir(albums)
            self.assertEqual(
                len([name for name in artifacts if not name.startswith(".")]),
                album_count,
                "every generated album must have published its own folder",
            )
            assert_generated_publication_invariant(
                result=result,
                expected_result_type=Materialized,
                expected_detail=None,
                source_exists=os.path.exists(
                    os.path.join(source, f"track-{album_count - 1}.mp3"),
                ),
                expected_source_exists=False,
                destination_names=set(os.listdir(canonical)),
                expected_names={f"track-{album_count - 1}.mp3"},
                artifact_names=artifacts,
                name_max=os.pathconf(albums, "PC_NAME_MAX"),
            )


class TestGeneratedPreviewCopyBounds(unittest.TestCase):
    def test_real_preview_copy_obeys_caps_growth_and_reserve(
        self,
    ) -> None:
        cases = (
            (
                "initial reserve one byte short",
                0,
                0,
                1,
                "insufficient private preview space",
            ),
            ("empty source at exact reserve", 0, 0, 2, None),
            ("below cap at exact write reserve", 3, 0, 5, None),
            ("at cap and exact write reserve", 4, 0, 6, None),
            (
                "declared cap one byte over",
                5,
                0,
                7,
                "preview snapshot limit exceeded",
            ),
            (
                "growth one byte over preflight",
                4,
                1,
                6,
                "source grew beyond copy limit",
            ),
            (
                "write reserve one byte short",
                4,
                0,
                5,
                "insufficient private preview space",
            ),
        )
        for (
            case,
            declared_bytes,
            growth_bytes,
            available_bytes,
            expected_error,
        ) in cases:
            with self.subTest(case=case):
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
                        _growth_bytes: int = growth_bytes,
                        _source_path: str = source_path,
                    ) -> int:
                        if _growth_bytes:
                            with open(_source_path, "ab") as handle:
                                handle.write(b"g" * _growth_bytes)
                        return copy_opened_file(
                            source_fd,
                            destination_fd,
                            max_bytes=max_bytes,
                            before_write=before_write,
                        )

                    snapshot: str | None = None
                    try:
                        snapshot = _snapshot_authorized_directory(
                            source,
                            cfg,
                            limits=PreviewSnapshotLimits(
                                max_bytes=4,
                                free_reserve_bytes=2,
                            ),
                            available_bytes_fn=(
                                lambda _preview_fd, _available=available_bytes: (
                                    _available
                                )
                            ),
                            copy_fn=grow_before_real_copy,
                        )
                    except FilesystemAuthorityError as exc:
                        snapshot = None
                        if expected_error is None:
                            self.fail(
                                f"expected preview copy success, got {exc}",
                            )
                        self.assertEqual(str(exc), expected_error)
                    else:
                        if expected_error is not None:
                            self.fail(
                                "preview copy succeeded outside the exact "
                                f"bounded world: expected {expected_error}",
                            )
                    preview = os.path.join(processing, "preview")
                    if snapshot is None:
                        assert_generated_preview_invariant(
                            succeeded=False,
                            preview_children=os.listdir(preview),
                            copied_bytes=0,
                            expected_bytes=0,
                            lock_path=os.path.join(
                                processing,
                                ".preview-snapshot.lock",
                            ),
                        )
                    else:
                        try:
                            copied = os.path.join(snapshot, "track.mp3")
                            assert_generated_preview_invariant(
                                succeeded=True,
                                preview_children=os.listdir(preview),
                                copied_bytes=os.path.getsize(copied),
                                expected_bytes=declared_bytes,
                                lock_path=os.path.join(
                                    processing,
                                    ".preview-snapshot.lock",
                                ),
                            )
                            with open(copied, "rb") as handle:
                                self.assertEqual(
                                    handle.read(),
                                    b"a" * declared_bytes,
                                )
                        finally:
                            remove_preview_snapshot(snapshot, cfg)


class TestGeneratedWrongMatchExplorerBounds(unittest.TestCase):
    @given(kinds=st.lists(st.sampled_from(("audio", "other", "directory", "fifo")), min_size=0, max_size=6))
    @example(kinds=["audio", "other", "directory"])
    @example(kinds=["directory", "directory", "directory", "directory"])
    def test_real_explorer_has_deterministic_total_entry_limit(self, kinds: list[str]) -> None:
        parent, source, processing, _cfg = _private_world()
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
                # A FIFO answers ``not_regular_file`` at open — a
                # containment refusal counted since issue #1086, not a
                # cap-truncation reason.
                expected_unreadable_count=sum(kind == "fifo" for kind in kinds),
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

        parent, _source, processing, cfg = _private_world()
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
            db.seed_request(make_request_row(id=42, status="wanted"))
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

            result, display, action_path = import_preview_worker._front_gate_check(
                db,
                job,
                runtime_config=cfg,
                candidate_evidence_loader=capture_lookup,
            )
            self.assertIsNotNone(result)
            self.assertEqual(display, db_path)
            self.assertIsNotNone(action_path)
            self.assertEqual(len(captured), 1)
            assert_force_front_gate_invariant(
                lookup_path=captured[0][0],
                db_failed_path=db_path,
                payload_failed_path=payload_path,
                lookup_bytes=captured[0][1],
                expected_db_bytes=db_bytes,
                snapshot_root=os.path.join(processing, "albums"),
                preview_children=os.listdir(os.path.join(processing, "preview")),
            )

    @given(
        db_leaf=_SAFE_COMPONENTS,
        payload_leaf=_SAFE_COMPONENTS,
        payload_outside_authority=st.booleans(),
    )
    def test_real_execute_path_keeps_configured_db_authority(
        self,
        db_leaf: str,
        payload_leaf: str,
        payload_outside_authority: bool,
    ) -> None:
        """The public worker reaches real force execution without a preview hook."""
        from scripts import import_preview_worker

        parent, source, processing, cfg = _private_world()
        with parent:
            db_path = os.path.join(source, "failed_imports", db_leaf, "Album")
            payload_root = parent.name if payload_outside_authority else source
            payload_path = os.path.join(
                payload_root,
                "payload",
                payload_leaf,
                "Album",
            )
            os.makedirs(db_path)
            os.makedirs(payload_path)
            with open(os.path.join(db_path, "01.mp3"), "wb") as handle:
                handle.write(b"database authority")
            with open(os.path.join(payload_path, "01.mp3"), "wb") as handle:
                handle.write(b"payload metadata")

            db = FakePipelineDB()
            setattr(db, "dsn", "postgresql://generated")  # noqa: B010
            db.seed_request(make_request_row(
                id=42,
                status="wanted",
                mb_release_id="",
            ))
            log_id = db.log_download(
                42,
                outcome="rejected",
                validation_result={"scenario": "high_distance", "failed_path": db_path},
            )
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=payload_path,
                ),
            )

            # No preview_fn: this reaches execute_preview_job through the
            # public worker pathway, and exercises its configured snapshot.
            updated = import_preview_worker.run_once(
                db,
                worker_id="generated-preview",
                runtime_config=cfg,
                stage_db_factory=lambda _dsn: db,
                heartbeat_db_factory=lambda _dsn: db,
            )

            self.assertIsNotNone(updated)
            assert updated is not None and updated.preview_result is not None
            self.assertEqual(updated.status, "failed")
            self.assertEqual(updated.preview_status, "measurement_failed")
            self.assertEqual(
                updated.preview_result["reason"],
                "missing_release_id",
            )
            self.assertEqual(updated.preview_result["source_path"], db_path)
            self.assertNotEqual(updated.preview_result["source_path"], payload_path)
            self.assertEqual(os.listdir(os.path.join(processing, "preview")), [])


class TestGeneratedRootRelocation(unittest.TestCase):
    def test_real_publish_relocation_never_commits_to_replacement(self) -> None:
        for replacement_extra in (False, True):
            with self.subTest(replacement_extra=replacement_extra):
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
                    album = make_grab_list_entry(
                        files=[file], artist="Artist", title="Album", year="2020",
                    )
                    canonical = canonical_folder_for_row(
                        album, processing_albums_dir(processing),
                    )
                    relocated = f"{processing}-relocated"

                    def relocate_before_publish(
                        _albums_fd: int, _destination: str,
                        processing_path: str = processing,
                        relocated_path: str = relocated,
                        replacement_extra_value: bool = replacement_extra,
                    ) -> None:
                        os.rename(processing_path, relocated_path)
                        os.mkdir(processing_path, 0o700)
                        os.mkdir(
                            os.path.join(processing_path, "albums"), 0o700,
                        )
                        os.mkdir(
                            os.path.join(processing_path, "preview"), 0o700,
                        )
                        if replacement_extra_value:
                            with open(
                                os.path.join(
                                    processing_path, "replacement-marker",
                                ),
                                "wb",
                            ) as handle:
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


class TestGeneratedPublicationResultTypeGate(unittest.TestCase):
    """#882 item 6: patrol every (result kind × allowed tuple) world.

    The deterministic pin in ``test_path_authority.py`` proves the default
    rejects a ``MaterializeFailed``; this ranges over the whole cross product
    so a widening of the default — or of any explicit tuple — is killed
    wherever it lands.
    """

    def test_only_allowed_result_kinds_pass_the_publication_checker(
        self,
    ) -> None:
        result_kinds = tuple(sorted(_RESULT_FACTORIES))
        explicit_allowed_kinds = tuple(
            frozenset(
                kind
                for kind, included in zip(
                    result_kinds,
                    included_kinds,
                    strict=True,
                )
                if included
            )
            for included_kinds in product((False, True), repeat=len(result_kinds))
            if any(included_kinds)
        )
        # Exhaustive result-kind × default-or-nonempty-explicit-allowlist
        # table. The default failed/materialized/guarded decisive worlds are
        # retained.
        allowed_worlds: tuple[frozenset[str] | None, ...] = (
            None,
            *explicit_allowed_kinds,
        )
        for result_kind, allowed_kinds in product(
            result_kinds,
            allowed_worlds,
        ):
            with self.subTest(
                result_kind=result_kind,
                allowed_kinds=allowed_kinds,
            ):
                # Every other arm of the checker is satisfied, so the
                # result-type gate is the only thing that can refuse this
                # world.
                result = _RESULT_FACTORIES[result_kind]()
                try:
                    if allowed_kinds is None:
                        assert_publication_invariant(
                            result=result,
                            source_exists=True,
                            expected_source_exists=True,
                            destination_names=set(),
                            expected_names=set(),
                            artifact_names=[],
                            name_max=255,
                        )
                    else:
                        assert_publication_invariant(
                            result=result,
                            source_exists=True,
                            expected_source_exists=True,
                            destination_names=set(),
                            expected_names=set(),
                            artifact_names=[],
                            name_max=255,
                            allowed_result_types=tuple(
                                _RESULT_TYPES[kind]
                                for kind in sorted(allowed_kinds)
                            ),
                        )
                except AssertionError:
                    accepted = False
                else:
                    accepted = True

                assert_result_type_gate(
                    accepted=accepted,
                    result_kind=result_kind,
                    allowed_kinds=allowed_kinds,
                )


def _publication_world(
    *,
    result: object = None,
    expected_result_type: type[Materialized | MaterializeGuarded] = Materialized,
    expected_detail: str | None = None,
    source_exists: bool = False,
    expected_source_exists: bool = False,
    destination_names: set[str] | None = None,
    expected_names: set[str] | None = None,
    artifact_names: list[str] | None = None,
    name_max: int = 255,
) -> None:
    """Drive the publication checker; only the overridden facts lie."""
    assert_generated_publication_invariant(
        result=Materialized() if result is None else result,
        expected_result_type=expected_result_type,
        expected_detail=expected_detail,
        source_exists=source_exists,
        expected_source_exists=expected_source_exists,
        destination_names=(
            {"track.mp3"} if destination_names is None else destination_names
        ),
        expected_names={"track.mp3"} if expected_names is None else expected_names,
        artifact_names=(
            [".materialize-lock-shard-ab"]
            if artifact_names is None
            else artifact_names
        ),
        name_max=name_max,
    )


def _preview_world(
    *,
    succeeded: bool = True,
    preview_children: list[str] | None = None,
    copied_bytes: int = 5,
    expected_bytes: int = 5,
    lock_path: str = __file__,
) -> None:
    """Drive the preview checker; only the overridden facts lie."""
    assert_generated_preview_invariant(
        succeeded=succeeded,
        preview_children=[] if preview_children is None else preview_children,
        copied_bytes=copied_bytes,
        expected_bytes=expected_bytes,
        lock_path=lock_path,
    )


def _explorer_world(
    *,
    entry_count: int = 1,
    entry_cap: int = 3,
    partial_result: object = False,
    truncated_reason: object = None,
    files: object = None,
    other_file_count: object = 0,
    scanned_file_count: object = 1,
    scanned_bytes: object = 5,
    expected_audio_paths: list[str] | None = None,
    expected_other_file_count: int = 0,
    expected_scanned_file_count: int = 1,
    expected_scanned_bytes: int = 5,
) -> None:
    """Drive the explorer checker; only the overridden facts lie."""
    assert_explorer_entry_invariant(
        entry_count=entry_count,
        entry_cap=entry_cap,
        payload={
            "partial": partial_result,
            "truncated_reason": truncated_reason,
            "files": (
                [{"relative_path": "01-audio.mp3"}] if files is None else files
            ),
            "other_file_count": other_file_count,
            "scanned_file_count": scanned_file_count,
            "scanned_bytes": scanned_bytes,
        },
        expected_audio_paths=(
            ["01-audio.mp3"]
            if expected_audio_paths is None
            else expected_audio_paths
        ),
        expected_other_file_count=expected_other_file_count,
        expected_scanned_file_count=expected_scanned_file_count,
        expected_scanned_bytes=expected_scanned_bytes,
    )


_ACTION_ROOT = "/processing/albums"
_ACTION_COPY = "/processing/albums/force-action-7"
_DB_QUARANTINE = "/Incoming/auto-import/Database/failed_imports/Album"
_PAYLOAD_QUARANTINE = "/elsewhere/manual/failed_imports/Album"


def _force_world(
    *,
    lookup_path: str = _ACTION_COPY,
    lookup_bytes: bytes = b"database",
    expected_db_bytes: bytes = b"database",
    preview_children: list[str] | None = None,
) -> None:
    """Drive the force front-gate checker; only the overrides lie."""
    assert_force_front_gate_invariant(
        lookup_path=lookup_path,
        db_failed_path=_DB_QUARANTINE,
        payload_failed_path=_PAYLOAD_QUARANTINE,
        lookup_bytes=lookup_bytes,
        expected_db_bytes=expected_db_bytes,
        snapshot_root=_ACTION_ROOT,
        preview_children=[] if preview_children is None else preview_children,
    )


def _relocation_world(
    *,
    result: object = None,
    source_exists: bool = True,
    replacement_has_canonical: bool = False,
) -> None:
    """Drive the relocation checker; only the overridden facts lie."""
    assert_generated_relocation_invariant(
        result=(
            MaterializeGuarded(detail="processing_root_relocated")
            if result is None
            else result
        ),
        source_exists=source_exists,
        replacement_has_canonical=replacement_has_canonical,
    )


class TestPathAuthorityProofCheckers(unittest.TestCase):
    """Known-bad proof checks: EVERY clause of every checker rejects a lie.

    A checker raises at its FIRST failing clause, so one world violating
    several clauses proves only that first one — the shape issue #1094
    exists to remove. Each row below plants exactly one clause, on a world
    where every earlier clause passes, and asserts that clause's own
    message.
    """

    def _assert_each_clause(
        self, cases: tuple[tuple[str, str, Callable[[], None]], ...],
    ) -> None:
        for clause, message, plant in cases:
            with (
                self.subTest(clause=clause),
                self.assertRaisesRegex(AssertionError, re.escape(message)),
            ):
                plant()

    def test_honest_worlds_pass_every_checker(self) -> None:
        """The base worlds the clause rows perturb must themselves pass."""
        _publication_world()
        _preview_world()
        _explorer_world()
        _explorer_world(
            entry_count=4,
            partial_result=True,
            truncated_reason="entry_limit",
            scanned_file_count=1,
        )
        _force_world()
        _relocation_world()
        assert_result_type_gate(
            accepted=True, result_kind="materialized", allowed_kinds=None,
        )
        assert_quarantine_verdict_is_earned(
            world="outside",
            code="unspecified",
            message="path is outside configured quarantine roots",
        )
        assert_local_import_authorization_is_earned(
            world="present", root="/root", owned_subtrees=("/root/owned",),
            held_display_path="/root/cd-rip/album",
        )
        assert_local_import_authorization_is_earned(
            world="outside", root="/root", owned_subtrees=("/root/owned",),
            held_display_path=None,
        )

    def test_result_type_gate_rejects_every_lie(self) -> None:
        self._assert_each_clause((
            (
                "default admits a refusal",
                (
                    "materialize result 'failed' was accepted against allowed "
                    "kinds ['guarded', 'materialized']"
                ),
                partial(
                    assert_result_type_gate,
                    accepted=True, result_kind="failed", allowed_kinds=None,
                ),
            ),
            (
                "default refuses a publication",
                (
                    "materialize result 'materialized' was rejected against "
                    "allowed kinds ['guarded', 'materialized']"
                ),
                partial(
                    assert_result_type_gate,
                    accepted=False,
                    result_kind="materialized",
                    allowed_kinds=None,
                ),
            ),
            (
                "explicit tuple refuses its own kind",
                (
                    "materialize result 'failed' was rejected against allowed "
                    "kinds ['failed']"
                ),
                partial(
                    assert_result_type_gate,
                    accepted=False,
                    result_kind="failed",
                    allowed_kinds=frozenset({"failed"}),
                ),
            ),
            (
                "explicit tuple admits an excluded kind",
                (
                    "materialize result 'failed' was accepted against allowed "
                    "kinds ['materialized']"
                ),
                partial(
                    assert_result_type_gate,
                    accepted=True,
                    result_kind="failed",
                    allowed_kinds=frozenset({"materialized"}),
                ),
            ),
        ))

    def test_publication_checker_rejects_every_clause(self) -> None:
        self._assert_each_clause((
            (
                "result type",
                "materialize result was MaterializeFailed, expected Materialized",
                partial(
                    _publication_world,
                    result=MaterializeFailed(reason="slskd_root_missing"),
                ),
            ),
            (
                "guard detail",
                (
                    "guard detail was 'published_manifest_mismatch', expected "
                    "'incomplete_or_unsafe_canonical'"
                ),
                partial(
                    _publication_world,
                    result=MaterializeGuarded(detail="published_manifest_mismatch"),
                    expected_result_type=MaterializeGuarded,
                    expected_detail="incomplete_or_unsafe_canonical",
                    source_exists=True,
                    expected_source_exists=True,
                ),
            ),
            (
                "source deletion ordering",
                "source deletion ordering was violated",
                partial(
                    _publication_world,
                    source_exists=True, expected_source_exists=False,
                ),
            ),
            (
                "destination overwritten",
                "canonical destination was overwritten or incomplete",
                partial(_publication_world, destination_names={"foreign.mp3"}),
            ),
            (
                "artifact over NAME_MAX",
                "a materialize artifact exceeds NAME_MAX",
                partial(_publication_world, artifact_names=["a" * 256]),
            ),
            (
                "unpublished transaction survived",
                "an unpublished materialize temp survived",
                partial(
                    _publication_world,
                    artifact_names=[".materialize-tmp-0011223344-abcd"],
                ),
            ),
            (
                "unbounded lock name",
                "materialize lock is not a bounded shard lock",
                partial(
                    _publication_world,
                    artifact_names=[".materialize-lock-Artist Album 2020"],
                ),
            ),
            (
                "more than 256 shards",
                "materialize used more than 256 lock shards",
                partial(
                    _publication_world,
                    artifact_names=[
                        f".materialize-lock-shard-{index:04x}"
                        for index in range(257)
                    ],
                ),
            ),
        ))

    def test_preview_checker_rejects_every_clause(self) -> None:
        self._assert_each_clause((
            (
                "lock outside its stable private root",
                "preview lock escaped its stable private root",
                partial(_preview_world, lock_path=f"{__file__}.absent"),
            ),
            (
                "failed copy left residue",
                "failed preview copy left a snapshot behind",
                partial(
                    _preview_world,
                    succeeded=False, preview_children=["preview-leaked"],
                ),
            ),
            (
                "short copy",
                "preview copy bytes diverged from the source manifest",
                partial(_preview_world, copied_bytes=4, expected_bytes=5),
            ),
        ))

    def test_explorer_checker_rejects_every_clause(self) -> None:
        truncated = "at-cap explorer result was truncated"
        complete = "over-budget explorer result was presented as complete"
        self._assert_each_clause((
            (
                "at cap but flagged partial",
                truncated,
                partial(_explorer_world, entry_count=3, partial_result=True),
            ),
            (
                "at cap but carries a truncation reason",
                truncated,
                partial(
                    _explorer_world, entry_count=3, truncated_reason="entry_limit",
                ),
            ),
            (
                "complete files not a list",
                "explorer files were not a list",
                partial(
                    _explorer_world, files=({"relative_path": "01-audio.mp3"},),
                ),
            ),
            (
                "complete audio paths inexact",
                "complete explorer audio paths were not exact",
                partial(_explorer_world, expected_audio_paths=["02-audio.mp3"]),
            ),
            (
                "complete other-file count inexact",
                "complete explorer other-file count was not exact",
                partial(_explorer_world, other_file_count=1),
            ),
            (
                "complete scanned-file count inexact",
                "complete explorer scanned-file count was not exact",
                partial(_explorer_world, scanned_file_count=2),
            ),
            (
                "complete scanned-byte count inexact",
                "complete explorer scanned-byte count was not exact",
                partial(_explorer_world, scanned_bytes=9),
            ),
            (
                "over cap presented as complete",
                complete,
                partial(_explorer_world, entry_count=4),
            ),
            (
                "over cap truncated for the wrong reason",
                complete,
                partial(
                    _explorer_world,
                    entry_count=4,
                    partial_result=True,
                    truncated_reason="depth_limit",
                ),
            ),
            (
                "truncated scanned count not an integer",
                "explorer did not return an integer scanned_file_count",
                partial(
                    _explorer_world,
                    entry_count=4,
                    partial_result=True,
                    truncated_reason="entry_limit",
                    scanned_file_count="1",
                ),
            ),
            (
                "truncated scan exceeded the entry budget",
                "explorer scanned more regular files than its entry budget",
                partial(
                    _explorer_world,
                    entry_count=4,
                    partial_result=True,
                    truncated_reason="entry_limit",
                    scanned_file_count=4,
                ),
            ),
            (
                "truncated output exceeded its scanned files",
                "truncated explorer output exceeded its scanned-file budget",
                partial(
                    _explorer_world,
                    entry_count=4,
                    partial_result=True,
                    truncated_reason="entry_limit",
                    scanned_file_count=1,
                    files=[
                        {"relative_path": "01-audio.mp3"},
                        {"relative_path": "02-audio.mp3"},
                    ],
                ),
            ),
            (
                "truncated files not a list",
                "truncated explorer output exceeded its scanned-file budget",
                partial(
                    _explorer_world,
                    entry_count=4,
                    partial_result=True,
                    truncated_reason="entry_limit",
                    scanned_file_count=1,
                    files=({"relative_path": "01-audio.mp3"},),
                ),
            ),
        ))

    def test_force_checker_rejects_every_clause(self) -> None:
        unisolated = "force evidence lookup consumed an unisolated path"
        self._assert_each_clause((
            (
                "lookup read the payload path",
                unisolated,
                partial(
                    _force_world,
                    lookup_path=_PAYLOAD_QUARANTINE, lookup_bytes=b"payload",
                ),
            ),
            (
                "lookup read the DB quarantine path",
                unisolated,
                partial(_force_world, lookup_path=_DB_QUARANTINE),
            ),
            (
                "lookup escaped the action root",
                "force evidence lookup escaped private action root",
                partial(
                    _force_world, lookup_path="/processing/preview/preview-abc",
                ),
            ),
            (
                "lookup held foreign bytes",
                "force evidence lookup did not contain DB-authorized bytes",
                partial(_force_world, lookup_bytes=b"payload"),
            ),
            (
                "private snapshot leaked",
                "force front gate leaked its private snapshot",
                partial(_force_world, preview_children=["preview-abc"]),
            ),
        ))

    def test_relocation_checker_rejects_every_clause(self) -> None:
        unguarded = "root relocation did not produce the guarded result"
        lost = "root relocation lost source bytes or wrote replacement root"
        self._assert_each_clause((
            (
                "relocation published instead of guarding",
                unguarded,
                partial(_relocation_world, result=Materialized()),
            ),
            (
                "relocation guarded for another reason",
                unguarded,
                partial(
                    _relocation_world,
                    result=MaterializeGuarded(
                        detail="incomplete_or_unsafe_canonical",
                    ),
                ),
            ),
            (
                "relocation lost the source bytes",
                lost,
                partial(_relocation_world, source_exists=False),
            ),
            (
                "relocation wrote the replacement root",
                lost,
                partial(_relocation_world, replacement_has_canonical=True),
            ),
        ))

    def test_quarantine_verdict_checker_rejects_every_clause(self) -> None:
        containment = "path is outside configured quarantine roots"
        laundered = (
            "was refused with a containment verdict the resolver never evaluated"
        )
        self._assert_each_clause((
            # Fails closed on a world it has no rule for (issue #1063 F5).
            (
                "unclassified world",
                "world='brand_new_world' has no verdict rule",
                partial(
                    assert_quarantine_verdict_is_earned,
                    world="brand_new_world", code="missing", message="gone",
                ),
            ),
            (
                "uncontained path refused without the containment verdict",
                "an uncontained path was refused as 'unspecified': gone",
                partial(
                    assert_quarantine_verdict_is_earned,
                    world="outside", code="unspecified", message="gone",
                ),
            ),
            (
                "uncontained path refused as missing",
                f"an uncontained path was refused as 'missing': {containment}",
                partial(
                    assert_quarantine_verdict_is_earned,
                    world="outside", code="missing", message=containment,
                ),
            ),
            (
                "unreadable root laundered into a containment verdict",
                f"world='unreadable_root' {laundered}",
                partial(
                    assert_quarantine_verdict_is_earned,
                    world="unreadable_root",
                    code="unspecified",
                    message=containment,
                ),
            ),
            (
                "absent name laundered into a containment verdict",
                f"world='absent' {laundered}",
                partial(
                    assert_quarantine_verdict_is_earned,
                    world="absent", code="unspecified", message=containment,
                ),
            ),
            (
                "unreadable root refused as something other than storage",
                "an unreadable root was refused as 'missing', not a storage failure",
                partial(
                    assert_quarantine_verdict_is_earned,
                    world="unreadable_root", code="missing", message="gone",
                ),
            ),
            (
                "absent name refused as something other than missing",
                "an absent candidate was refused as 'open_failed', not missing",
                partial(
                    assert_quarantine_verdict_is_earned,
                    world="absent", code="open_failed", message="boom",
                ),
            ),
        ))

    def test_local_import_authorization_checker_rejects_every_clause(self) -> None:
        """Issue #1176 PR2: the core invariant checker for the local-import
        lane. Each row plants exactly one clause on a world where every
        earlier clause passes (issue #1094's per-clause discipline)."""
        self._assert_each_clause((
            (
                "unclassified world",
                "world='brand_new_world' has no verdict rule",
                partial(
                    assert_local_import_authorization_is_earned,
                    world="brand_new_world", root="/root", owned_subtrees=(),
                    held_display_path=None,
                ),
            ),
            (
                "authorized a path outside the configured root",
                (
                    "authorized a path outside the configured local-import "
                    "root '/root'"
                ),
                partial(
                    assert_local_import_authorization_is_earned,
                    world="present", root="/root", owned_subtrees=(),
                    held_display_path="/elsewhere/album",
                ),
            ),
            (
                "authorized a path inside an owned subtree",
                (
                    "authorized a path inside the Cratedigger-owned subtree "
                    "'/root/cratedigger-processing/albums'"
                ),
                partial(
                    assert_local_import_authorization_is_earned,
                    world="present", root="/root",
                    owned_subtrees=("/root/cratedigger-processing/albums",),
                    held_display_path=(
                        "/root/cratedigger-processing/albums/leak"),
                ),
            ),
        ))


#: The worlds that reach a refusal and therefore owe a verdict rule. The
#: present-* worlds never get here — they succeed.
_REFUSING_WORLDS: frozenset[str] = frozenset(
    {"outside", "unreadable_root", "absent"},
)


def assert_quarantine_verdict_is_earned(
    *, world: str, code: str, message: str,
) -> None:
    """A quarantine refusal must name the fact it actually established.

    Only a candidate that is genuinely NOT under a configured root may be
    refused for containment; a root the resolver could not open, and a
    name that is genuinely absent, each owe their own code (issue #1063).

    Fails closed on an unknown world: a checker that silently passes
    input it has no rule for is unfalsifiable for exactly the cases most
    likely to be new.
    """
    if world not in _REFUSING_WORLDS:
        raise AssertionError(
            f"world={world!r} has no verdict rule — add one rather than "
            f"letting an unclassified world pass (code={code!r})"
        )
    containment_verdict = "outside configured quarantine roots" in message
    if world == "outside":
        if not containment_verdict or code == "missing":
            raise AssertionError(
                f"an uncontained path was refused as {code!r}: {message}")
        return
    if containment_verdict:
        raise AssertionError(
            f"world={world!r} was refused with a containment verdict the "
            f"resolver never evaluated: {message}")
    if world == "unreadable_root" and code != "open_failed":
        raise AssertionError(
            f"an unreadable root was refused as {code!r}, not a storage failure")
    if world == "absent" and code != "missing":
        raise AssertionError(
            f"an absent candidate was refused as {code!r}, not missing")


class TestGeneratedQuarantineVerdicts(unittest.TestCase):
    """Every quarantine refusal states the fact it actually observed.

    All three configured roots are live here, and a legacy RELATIVE name
    is lexically contained by every one of them — the only shape that
    exercises the ``contained_refusal`` / ``contained_missing``
    precedence added for issue #1063.
    """

    @example(world="present_third_root", leaf="album", relative=False)
    @example(world="present_third_root", leaf="album", relative=True)
    @example(world="unreadable_earlier_root", leaf="album", relative=True)
    @example(world="unreadable_later_root", leaf="album", relative=True)
    @example(world="unreadable_root", leaf="album", relative=False)
    @example(world="unreadable_root", leaf="album", relative=True)
    @example(world="absent", leaf="album", relative=False)
    @example(world="absent", leaf="album", relative=True)
    @example(world="outside", leaf="album", relative=False)
    @given(
        world=st.sampled_from((
            "present_third_root",
            "present_first_root",
            "unreadable_earlier_root",
            "unreadable_later_root",
            "unreadable_root",
            "absent",
            "outside",
        )),
        leaf=_SAFE_COMPONENTS,
        relative=st.booleans(),
    )
    def test_real_resolver_never_invents_a_containment_verdict(
        self, world: str, leaf: str, relative: bool,
    ) -> None:
        from lib.fs_authority import open_configured_quarantine_directory

        if world in ("unreadable_earlier_root", "unreadable_later_root"):
            # An absolute candidate is lexically contained by exactly one
            # root, so "another configured root also refused / also held
            # it" only exists for the relative legacy shape.
            relative = True

        with tempfile.TemporaryDirectory() as parent:
            slskd = os.path.join(parent, "slskd")
            incoming = os.path.join(parent, "Incoming")
            processing = os.path.join(parent, "processing")
            for directory in (slskd, incoming, processing):
                os.mkdir(directory, 0o700)
            albums = os.path.join(processing, "albums")
            os.mkdir(albums, 0o700)
            os.mkdir(os.path.join(processing, "preview"), 0o700)
            cfg = CratediggerConfig.from_ini(_quarantine_ini(
                slskd=slskd, incoming=incoming, processing=processing))
            self.assertEqual(
                [cfg.slskd_download_dir, cfg.beets_staging_dir,
                 cfg.processing_dir],
                [slskd, incoming, processing],
                "fixture config must populate every configured root",
            )

            # Resolver root order: slskd, staging, processing/albums.
            first_quarantine = os.path.join(slskd, "wrong_matches")
            third_quarantine = os.path.join(albums, "wrong_matches")
            os.makedirs(first_quarantine, exist_ok=True)
            os.makedirs(third_quarantine, exist_ok=True)
            first_album = os.path.join(first_quarantine, leaf)
            third_album = os.path.join(third_quarantine, leaf)

            expected_album: str | None = None
            if world in ("present_third_root", "unreadable_earlier_root"):
                os.makedirs(third_album, exist_ok=True)
                expected_album = third_album
            elif world in ("present_first_root", "unreadable_later_root"):
                os.makedirs(first_album, exist_ok=True)
                expected_album = first_album
            elif world == "unreadable_root":
                os.makedirs(third_album, exist_ok=True)

            candidate = os.path.join("wrong_matches", leaf)
            if not relative:
                candidate = (
                    expected_album if expected_album is not None else third_album
                )
            if world == "outside":
                candidate = os.path.join(parent, "unconfigured", leaf)
                os.makedirs(candidate, exist_ok=True)

            if world in ("unreadable_earlier_root", "unreadable_root"):
                os.chmod(first_quarantine, 0o000)
            if world in ("unreadable_later_root", "unreadable_root"):
                os.chmod(third_quarantine, 0o000)

            try:
                if expected_album is not None:
                    # A root that cannot be READ must never deny a name a
                    # DIFFERENT configured root positively holds — and the
                    # descriptor handed back must be that exact folder.
                    with open_configured_quarantine_directory(
                        candidate, cfg,
                    ) as opened:
                        self.assertEqual(
                            os.fstat(opened.fd).st_ino,
                            os.stat(expected_album).st_ino,
                        )
                    return
                with self.assertRaises(FilesystemAuthorityError) as refused, \
                        open_configured_quarantine_directory(candidate, cfg):
                    pass
            finally:
                os.chmod(first_quarantine, 0o700)
                os.chmod(third_quarantine, 0o700)

            assert_quarantine_verdict_is_earned(
                world=world,
                code=refused.exception.code,
                message=str(refused.exception),
            )


#: The worlds the local-import authorization property drives. Fail-closed:
#: a world outside this set gets no verdict rule at all (see
#: :func:`assert_local_import_authorization_is_earned`).
_LOCAL_IMPORT_WORLDS: frozenset[str] = frozenset({
    "present",
    "outside",
    "owned_processing",
    "owned_staging",
    "owned_slskd",
    "owned_beets_directory",
    "not_configured_disabled",
    "not_configured_no_dir",
    "missing",
    "unreadable",
})


def assert_local_import_authorization_is_earned(
    *,
    world: str,
    root: str,
    owned_subtrees: Sequence[str],
    held_display_path: str | None,
) -> None:
    """Issue #1176 PR2's core invariant: authorization implies containment.

    A candidate may come back as an authorized :class:`~lib.fs_authority.HeldDirectory`
    ONLY if its resolved path is (a) lexically under the configured
    local-import root and (b) lexically outside every Cratedigger-owned
    subtree. Every REFUSAL world is fine as far as this checker is
    concerned — the deterministic pins in ``tests/test_path_authority.py``
    own each refusal's own structured code/message; this checker owns only
    the "was this authorization earned" half, driven end to end by the
    generated property below.

    Deliberately does NOT reuse :func:`lib.fs_authority.paths_overlap` (the
    same helper the production resolver calls) for its own containment
    math — an independent ``os.path.commonpath`` check here means a defect
    in ``paths_overlap`` itself would still be caught, rather than the
    checker and the code under test silently agreeing by construction.

    Fails closed on an unrecognised world so a new world added to the
    property's ``st.sampled_from`` cannot silently skip verification
    (mirrors :func:`assert_quarantine_verdict_is_earned`'s issue #1063
    fail-closed doctrine).
    """
    if world not in _LOCAL_IMPORT_WORLDS:
        raise AssertionError(
            f"world={world!r} has no verdict rule — add one rather than "
            f"letting an unclassified world pass "
            f"(held_display_path={held_display_path!r})"
        )
    if held_display_path is None:
        return
    root_abs = os.path.abspath(root)
    if os.path.commonpath([root_abs, held_display_path]) != root_abs:
        raise AssertionError(
            f"world={world!r} authorized a path outside the configured "
            f"local-import root {root_abs!r}: {held_display_path!r}")
    for owned in owned_subtrees:
        owned_abs = os.path.abspath(owned)
        if os.path.commonpath([owned_abs, held_display_path]) == owned_abs:
            raise AssertionError(
                f"world={world!r} authorized a path inside the "
                f"Cratedigger-owned subtree {owned_abs!r}: "
                f"{held_display_path!r}")


class TestGeneratedLocalImportAuthorization(unittest.TestCase):
    """Issue #1176 PR2: no path outside the configured root, and no path
    inside a Cratedigger-owned subtree, is EVER returned as authorized.

    Driven end to end through real ``CratediggerConfig.from_ini`` plumbing
    (test-fidelity Rule C — a hand-built stub config would only prove the
    resolver's own attribute-name assumptions, not that the nix-module ->
    config.ini -> ``CratediggerConfig`` -> ``lib.fs_authority`` pipeline is
    wired correctly) and the real no-follow resolver, over a BROAD
    configured root that genuinely contains the owned subtrees as siblings
    of a legitimate import source — the realistic ``/mnt/virtio``-shaped
    deployment the owned-subtree carve-out exists for.
    """

    @example(world="present", leaf="album")
    @example(world="outside", leaf="album")
    @example(world="owned_processing", leaf="album")
    @example(world="owned_staging", leaf="album")
    @example(world="owned_slskd", leaf="album")
    @example(world="owned_beets_directory", leaf="album")
    @example(world="not_configured_disabled", leaf="album")
    @example(world="not_configured_no_dir", leaf="album")
    @example(world="missing", leaf="album")
    @example(world="unreadable", leaf="album")
    @given(
        world=st.sampled_from(sorted(_LOCAL_IMPORT_WORLDS)),
        leaf=_SAFE_COMPONENTS,
    )
    def test_real_resolver_never_authorizes_outside_or_owned(
        self, world: str, leaf: str,
    ) -> None:
        with tempfile.TemporaryDirectory() as parent:
            root = os.path.join(parent, "local-import-root")
            processing = os.path.join(root, "cratedigger-processing")
            staging = os.path.join(root, "incoming")
            slskd = os.path.join(root, "slskd-downloads")
            beets_directory = os.path.join(root, "beets-library")
            for directory in (root, processing, staging, slskd, beets_directory):
                os.makedirs(directory, exist_ok=True)
            albums = processing_albums_dir(processing)
            os.makedirs(albums, exist_ok=True)

            enabled = world != "not_configured_disabled"
            configured_dir = "" if world == "not_configured_no_dir" else root
            cfg = CratediggerConfig.from_ini(_local_import_ini(
                enabled=enabled, local_dir=configured_dir,
                processing=processing, staging=staging, slskd=slskd,
                beets_directory=beets_directory,
            ))
            self.assertEqual(cfg.local_import_enabled, enabled)
            self.assertEqual(cfg.local_import_dir, configured_dir)
            self.assertEqual(cfg.processing_dir, processing)
            self.assertEqual(cfg.beets_staging_dir, staging)
            self.assertEqual(cfg.slskd_download_dir, slskd)
            self.assertEqual(cfg.beets_directory, beets_directory)

            owned = local_import_owned_subtrees(cfg)

            unreadable_parent: str | None = None
            if world == "present":
                candidate = os.path.join(root, "cd-rip", leaf)
                os.makedirs(candidate, exist_ok=True)
            elif world == "outside":
                candidate = os.path.join(parent, "elsewhere", leaf)
                os.makedirs(candidate, exist_ok=True)
            elif world == "owned_processing":
                candidate = os.path.join(albums, leaf)
                os.makedirs(candidate, exist_ok=True)
            elif world == "owned_staging":
                candidate = os.path.join(staging, leaf)
                os.makedirs(candidate, exist_ok=True)
            elif world == "owned_slskd":
                candidate = os.path.join(slskd, leaf)
                os.makedirs(candidate, exist_ok=True)
            elif world == "owned_beets_directory":
                candidate = os.path.join(beets_directory, leaf)
                os.makedirs(candidate, exist_ok=True)
            elif world in ("not_configured_disabled", "not_configured_no_dir"):
                candidate = os.path.join(root, "cd-rip", leaf)
                os.makedirs(candidate, exist_ok=True)
            elif world == "missing":
                candidate = os.path.join(root, "cd-rip", leaf)
            elif world == "unreadable":
                unreadable_parent = os.path.join(root, "cd-rip")
                os.makedirs(unreadable_parent, exist_ok=True)
                candidate = os.path.join(unreadable_parent, leaf)
                os.makedirs(candidate, exist_ok=True)
                os.chmod(unreadable_parent, 0o000)
            else:  # pragma: no cover - _LOCAL_IMPORT_WORLDS is exhaustive here
                raise AssertionError(f"unhandled world {world!r}")

            held_display_path: str | None = None
            try:
                try:
                    with open_configured_local_import_directory(
                        candidate, cfg,
                    ) as opened:
                        held_display_path = opened.display_path
                except FilesystemAuthorityError:
                    held_display_path = None
            finally:
                if unreadable_parent is not None:
                    os.chmod(unreadable_parent, 0o700)

            assert_local_import_authorization_is_earned(
                world=world, root=root, owned_subtrees=owned,
                held_display_path=held_display_path,
            )


def _local_import_ini(
    *, enabled: bool, local_dir: str, processing: str, staging: str,
    slskd: str, beets_directory: str,
):
    """Build config through the sections ``CratediggerConfig`` really reads.

    Mirrors ``_quarantine_ini``'s Rule-C rationale: ``local_import_enabled``
    / ``local_import_dir`` come from ``[Local Import]``, ``processing_dir``
    from ``[Paths]``, ``beets_staging_dir`` from ``[Beets Validation]``,
    ``slskd_download_dir`` from ``[Slskd]``, and ``beets_directory`` from
    ``[Beets]`` — the same sections the nix module renders into
    ``config.ini``.
    """
    import configparser

    parser = configparser.RawConfigParser()
    parser.read_string(
        "[Local Import]\n"
        f"enabled = {'True' if enabled else 'False'}\n"
        f"dir = {local_dir}\n"
        "[Paths]\n"
        f"processing_dir = {processing}\n"
        "[Beets Validation]\n"
        f"staging_dir = {staging}\n"
        "[Slskd]\n"
        f"download_dir = {slskd}\n"
        "[Beets]\n"
        f"directory = {beets_directory}\n"
    )
    return parser


def _quarantine_ini(*, slskd: str, incoming: str, processing: str):
    """Build config through the sections ``CratediggerConfig`` really reads.

    ``slskd_download_dir`` comes from ``[Slskd] download_dir`` and
    ``beets_staging_dir`` from ``[Beets Validation] staging_dir``; only
    ``processing_dir`` lives under ``[Paths]``. Putting all three under
    ``[Paths]`` silently left TWO of the three quarantine roots empty, so
    the multi-root precedence this property exists to exercise never ran
    (test-fidelity Rule C — issue #1063 review T1.2).
    """
    import configparser

    parser = configparser.RawConfigParser()
    parser.read_string(
        "[Slskd]\n"
        f"download_dir = {slskd}\n"
        "[Beets Validation]\n"
        f"staging_dir = {incoming}\n"
        "[Paths]\n"
        f"processing_dir = {processing}\n"
    )
    return parser


if __name__ == "__main__":
    unittest.main()
