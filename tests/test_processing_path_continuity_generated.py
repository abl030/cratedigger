"""Generated patrol for processing-owner filesystem path continuity."""

from __future__ import annotations

import os
import tempfile
import unittest
from collections.abc import Callable
from dataclasses import replace

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib import download_validation
from lib.config import CratediggerConfig
from lib.context import CratediggerContext
from lib.dispatch import DispatchCoreFn, DispatchOutcome
from lib.dispatch import core as dispatch_core
from lib.dispatch.types import DispatchDB, DispatchRequest
from lib.grab_list import GrabListEntry
from lib.processing_paths import stage_to_ai_path
from lib.quality import ValidationResult
from lib.staged_album import StagedAlbum
from tests.fakes import FakePipelineDB, RecordingDispatchCore
from tests.helpers import make_ctx_with_fake_db, make_request_row

HandleFn = Callable[..., DispatchOutcome | None]
CleanupRootFn = Callable[[str, CratediggerConfig], str]
_MBID = "11111111-2222-3333-4444-555555555555"


def _known_bad_relocate_before_dispatch(
    album_data: GrabListEntry,
    bv_result: ValidationResult,
    staged_album: StagedAlbum,
    ctx: CratediggerContext,
    *,
    dispatch_fn: DispatchCoreFn | None = None,
) -> DispatchOutcome | None:
    """Mutant: splits the live folder from its durable owner path."""
    dest = stage_to_ai_path(
        artist=album_data.artist,
        title=album_data.title,
        staging_dir=ctx.cfg.beets_staging_dir,
        request_id=album_data.db_request_id,
        auto_import=True,
    )
    staged_album.move_to(dest)
    return download_validation._handle_valid_result(
        album_data,
        bv_result,
        staged_album,
        ctx,
        dispatch_fn=dispatch_fn,
    )


def _known_bad_swap_identity_before_dispatch(
    album_data: GrabListEntry,
    bv_result: ValidationResult,
    staged_album: StagedAlbum,
    ctx: CratediggerContext,
    *,
    dispatch_fn: DispatchCoreFn | None = None,
) -> DispatchOutcome | None:
    """Mutant: hands dispatch the operator label as the release identity.

    Models an argument inversion at the auto lane's single
    ``DispatchRequest`` construction (issue #1277) — the exact shape a
    review mutant used, and the one that would send Beets at the wrong
    release under the wrong RELEASE lock.
    """

    def swapping(
        request: DispatchRequest,
        db: DispatchDB,
        **kwargs: object,
    ) -> DispatchOutcome:
        assert dispatch_fn is not None
        return dispatch_fn(
            replace(
                request,
                mb_release_id=request.label,
                label=request.mb_release_id,
            ),
            db,
            **kwargs,  # pyright: ignore[reportArgumentType]
        )

    return download_validation._handle_valid_result(
        album_data,
        bv_result,
        staged_album,
        ctx,
        dispatch_fn=swapping,
    )


def assert_processing_owner_keeps_one_path(
    *,
    file_count: int,
    processing_depth: int,
    staging_depth: int,
    handle_fn: HandleFn,
) -> None:
    """The imported folder, durable owner path, and cleanup path stay equal."""
    with tempfile.TemporaryDirectory() as raw:
        processing_path = os.path.join(
            raw,
            "processing",
            *(f"level-{index}" for index in range(processing_depth)),
            "album",
        )
        staging_root = os.path.join(
            raw,
            "incoming",
            *(f"level-{index}" for index in range(staging_depth)),
        )
        os.makedirs(processing_path)
        expected_files = {f"{index:02d}.flac" for index in range(file_count)}
        for name in expected_files:
            with open(os.path.join(processing_path, name), "wb") as handle:
                handle.write(name.encode())

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id=_MBID,
            status="processing",
            active_automation_import_job_id=99,
            active_download_state={
                "filetype": "flac",
                "enqueued_at": "2026-07-30T12:00:00+00:00",
                "files": [],
                "current_path": processing_path,
            },
        ))
        ctx = make_ctx_with_fake_db(
            db,
            cfg=CratediggerConfig(
                beets_harness_path="/harness",
                pipeline_db_enabled=True,
                beets_distance_threshold=0.15,
                processing_dir=os.path.join(raw, "processing"),
                beets_staging_dir=staging_root,
                beets_tracking_file=os.path.join(raw, "tracking.log"),
            ),
        )
        album_data = GrabListEntry(
            album_id=42,
            artist="Generated Artist",
            title="Generated Album",
            year="2026",
            files=[],
            filetype="flac",
            mb_release_id=_MBID,
            db_source="request",
            db_request_id=42,
            import_folder=processing_path,
        )
        staged_album = StagedAlbum.from_entry(
            album_data,
            default_path=processing_path,
        )
        dispatch = RecordingDispatchCore()

        outcome = handle_fn(
            album_data,
            ValidationResult(
                valid=True,
                distance=0.05,
                scenario="strong_match",
            ),
            staged_album,
            ctx,
            dispatch_fn=dispatch,
        )

        if outcome is None or not outcome.success:
            raise AssertionError("processing owner did not reach import dispatch")
        durable_path = db.request(42)["active_download_state"]["current_path"]
        dispatched_paths = [call.request.path for call in dispatch.calls]
        if durable_path != processing_path:
            raise AssertionError("processing mutated its immutable path provenance")
        if staged_album.current_path != processing_path:
            raise AssertionError("live folder diverged from its durable owner path")
        if dispatched_paths != [processing_path]:
            raise AssertionError("Beets was dispatched against a relocated path")
        # The request dispatch is handed must describe the album that was
        # just validated — one construction, so a swapped or dropped value
        # is invisible to a path-only check (issue #1277).
        dispatched_identity = tuple(
            (call.request.mb_release_id, call.request.label,
             call.request.request_id, call.request.distance)
            for call in dispatch.calls
        )
        expected_identity = ((
            _MBID,
            f"{album_data.artist} - {album_data.title}",
            42,
            0.05,
        ),)
        if dispatched_identity != expected_identity:
            raise AssertionError(
                "dispatch was handed a different album than the one "
                f"validated: {dispatched_identity!r} != {expected_identity!r}"
            )
        if not os.path.isdir(processing_path):
            raise AssertionError("canonical processing folder was relocated")
        if set(os.listdir(processing_path)) != expected_files:
            raise AssertionError("canonical processing manifest changed")


def _known_bad_external_cleanup_root(
    _source_path: str,
    cfg: CratediggerConfig,
) -> str:
    """Mutant: sends owned cleanup back across a configured mount boundary."""
    return cfg.beets_staging_dir


def assert_processing_quarantine_stays_on_owner_filesystem(
    *,
    processing_depth: int,
    staging_depth: int,
    root_fn: CleanupRootFn,
) -> None:
    """Owned quarantine is rooted beside canonical albums, not in Incoming."""
    with tempfile.TemporaryDirectory() as raw:
        processing_root = os.path.join(
            raw,
            "private",
            *(f"level-{index}" for index in range(processing_depth)),
        )
        albums_root = os.path.join(processing_root, "albums")
        source_path = os.path.join(albums_root, "candidate")
        staging_root = os.path.join(
            raw,
            "incoming",
            *(f"level-{index}" for index in range(staging_depth)),
        )
        os.makedirs(source_path)
        os.makedirs(staging_root)
        cfg = CratediggerConfig(
            processing_dir=processing_root,
            beets_staging_dir=staging_root,
        )

        selected = root_fn(source_path, cfg)
        if selected != albums_root:
            raise AssertionError(
                "processing quarantine crossed out of its owner filesystem"
            )


class TestProcessingPathContinuityGenerated(unittest.TestCase):
    @given(
        file_count=st.integers(min_value=1, max_value=5),
        processing_depth=st.integers(min_value=0, max_value=3),
        staging_depth=st.integers(min_value=0, max_value=3),
    )
    @example(file_count=1, processing_depth=0, staging_depth=3)
    def test_processing_owner_keeps_one_path_in_every_layout(
        self,
        *,
        file_count: int,
        processing_depth: int,
        staging_depth: int,
    ) -> None:
        assert_processing_owner_keeps_one_path(
            file_count=file_count,
            processing_depth=processing_depth,
            staging_depth=staging_depth,
            handle_fn=download_validation._handle_valid_result,
        )

    def test_checker_rejects_relocation_known_bad_mutant(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            "live folder diverged",
        ):
            assert_processing_owner_keeps_one_path(
                file_count=1,
                processing_depth=1,
                staging_depth=2,
                handle_fn=_known_bad_relocate_before_dispatch,
            )

    def test_checker_rejects_swapped_identity_known_bad_mutant(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            "different album than the one validated",
        ):
            assert_processing_owner_keeps_one_path(
                file_count=1,
                processing_depth=1,
                staging_depth=2,
                handle_fn=_known_bad_swap_identity_before_dispatch,
            )

    @given(
        processing_depth=st.integers(min_value=0, max_value=3),
        staging_depth=st.integers(min_value=0, max_value=3),
    )
    @example(processing_depth=3, staging_depth=0)
    def test_owned_quarantine_ignores_every_external_staging_layout(
        self,
        *,
        processing_depth: int,
        staging_depth: int,
    ) -> None:
        assert_processing_quarantine_stays_on_owner_filesystem(
            processing_depth=processing_depth,
            staging_depth=staging_depth,
            root_fn=dispatch_core._processing_quarantine_root,
        )

    def test_checker_rejects_external_quarantine_known_bad_mutant(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            "crossed out",
        ):
            assert_processing_quarantine_stays_on_owner_filesystem(
                processing_depth=1,
                staging_depth=2,
                root_fn=_known_bad_external_cleanup_root,
            )
