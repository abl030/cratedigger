"""Generated producer-to-consumer authority contract for force import."""

from __future__ import annotations

import os
import shutil
import tempfile
import unittest
from dataclasses import dataclass
from functools import partial
from itertools import product
from pathlib import Path
from unittest.mock import patch

import msgspec

from album_source import DatabaseSource
from lib.config import CratediggerConfig
from lib.context import CratediggerContext
from lib.download_processing import CompletionDispatched
from lib.force_import_service import (
    RESULT_QUEUED,
    ForceImportEnqueueResult,
    enqueue_force_import,
)
from lib.grab_list import DownloadFile, GrabListEntry
from lib.import_queue import (
    IMPORT_JOB_FORCE,
    IMPORT_JOB_YOUTUBE,
    ForceImportPayload,
    youtube_import_dedupe_key,
    youtube_import_payload,
)
from lib.processing_paths import processing_albums_dir
from lib.quality import ValidationResult
from lib.staged_album import StagedAlbum
from lib.validation_envelope import decode_validation_envelope
from tests.fakes import FakePipelineDB
from tests.helpers import (
    claim_next_import_job,
    make_download_file,
    make_grab_list_entry,
    make_request_row,
)

# Independently authored producer inventory. These are the rejection lanes that
# can persist a force-importable quarantine path; importing the authority table
# from lib.fs_authority would make this contract agree by construction. The
# four folder/integrity rejects deliberately name the plain
# ``failed_imports/<album>`` shape that the original inventory omitted.
PRODUCER_LANES = ("slskd", "youtube_staging", "private_processing")
REJECTION_CLASSES: dict[str, tuple[str, ...]] = {
    "high_distance": ("wrong_matches",),
    "spectral_reject": ("failed_imports", "bad_files"),
    "bad_audio_hash": ("failed_imports",),
    "nested_layout": ("failed_imports",),
    "empty_fileset": ("failed_imports",),
    "mixed_source": ("failed_imports",),
}
PATH_CASES = ("existing", "missing", "lookalike")


@dataclass(frozen=True)
class ProducedRejection:
    db: FakePipelineDB
    cfg: CratediggerConfig
    download_log_id: int
    failed_path: str


def assert_force_import_authority_invariant(
    *, authorized: bool, outcome: str, job_count: int,
) -> None:
    """Exactly one real produced quarantine source may create a queue job."""
    if authorized and (outcome != RESULT_QUEUED or job_count != 1):
        raise AssertionError("produced quarantine source did not enqueue exactly once")
    if not authorized and job_count != 0:
        raise AssertionError("non-produced source created an import job")


def _configured_world(tmp: str) -> tuple[CratediggerConfig, dict[str, str]]:
    staging = os.path.join(tmp, "Incoming")
    slskd = os.path.join(tmp, "slskd")
    processing = os.path.join(tmp, "processing")
    os.makedirs(staging)
    os.makedirs(slskd)
    os.makedirs(processing_albums_dir(processing))
    cfg = CratediggerConfig(
        beets_staging_dir=staging,
        slskd_download_dir=slskd,
        processing_dir=processing,
        beets_tracking_file=os.path.join(tmp, "validation.jsonl"),
        pipeline_db_enabled=True,
    )
    return cfg, {
        "slskd": os.path.join(slskd, "peer"),
        "youtube_staging": os.path.join(staging, "auto-import"),
        "private_processing": processing_albums_dir(processing),
    }


def _source_album(
    lane_root: str,
    *,
    scenario: str,
    nested: bool,
    album_name: str = "Artist - Album",
) -> tuple[str, list[DownloadFile]]:
    source_parent = os.path.join(lane_root, "nested") if nested else lane_root
    source = os.path.join(source_parent, album_name)
    os.makedirs(source)
    if scenario == "empty_fileset":
        return source, []
    with open(os.path.join(source, "01 - Track.flac"), "wb") as handle:
        handle.write(b"audio")
    return source, [make_download_file(
        filename="01 - Track.flac",
        file_dir="peer\\Artist\\Album",
        username="peer",
    )]


def _rejection_result(scenario: str) -> ValidationResult:
    return ValidationResult(
        valid=False,
        distance=0.9,
        scenario=scenario,
        detail=f"generated {scenario}",
    )


def _runtime_context(
    db: FakePipelineDB,
    cfg: CratediggerConfig,
) -> CratediggerContext:
    source = DatabaseSource(
        "unused-producer-contract-dsn",
        musicbrainz_ws2_base="http://musicbrainz.test/ws/2",
        discogs_api_base="http://discogs.test",
        borrowed_db=db,  # pyright: ignore[reportArgumentType]
    )
    return CratediggerContext(
        cfg=cfg,
        slskd=None,
        pipeline_db_source=source,
    )


def _persisted_rejection(db: FakePipelineDB) -> tuple[int, str]:
    row = db.download_logs[-1]
    validation = decode_validation_envelope(row.validation_result)
    if not validation.failed_path:
        raise AssertionError("rejection adapter did not persist failed_path")
    return row.id, validation.failed_path


def _produce_download_rejection(
    cfg: CratediggerConfig,
    lane_root: str,
    *,
    scenario: str,
    nested: bool,
    album_name: str = "Artist - Album",
) -> ProducedRejection:
    """Drive the shared slskd/private rejection mover and DB adapter."""
    from lib.download_rejection import _handle_rejected_result

    source, files = _source_album(
        lane_root,
        scenario=scenario,
        nested=nested,
        album_name=album_name,
    )
    db = FakePipelineDB()
    db.seed_request(make_request_row(
        id=867,
        status="downloading",
        artist_name="Artist",
        album_title="Album",
        mb_release_id="mb-867",
    ))
    ctx = _runtime_context(db, cfg)
    album = make_grab_list_entry(
        files=files,
        artist="Artist",
        title="Album",
        mb_release_id="mb-867",
        db_request_id=867,
        db_source="request",
    )
    _handle_rejected_result(
        album,
        _rejection_result(scenario),
        StagedAlbum(current_path=source, request_id=867),
        ctx,
    )
    if files:
        if db.get_denylisted_users(867) != [{
            "username": "peer",
            "reason": "beets validation rejected",
            "created_at": None,
        }]:
            raise AssertionError("slskd rejection did not persist its peer denylist")
        if db.cooldowns_applied != ["peer"]:
            raise AssertionError("slskd rejection did not evaluate its peer cooldown")
    log_id, failed_path = _persisted_rejection(db)
    return ProducedRejection(db, cfg, log_id, failed_path)


def _produce_youtube_rejection(
    cfg: CratediggerConfig,
    lane_root: str,
    *,
    scenario: str,
    nested: bool,
    album_name: str = "Artist - Album",
) -> ProducedRejection:
    """Drive importer payload reconstruction into the real rejection adapter."""
    from lib.download_rejection import _handle_rejected_result
    from scripts import importer

    staged_path, _files = _source_album(
        lane_root,
        scenario=scenario,
        nested=nested,
        album_name=album_name,
    )
    db = FakePipelineDB()
    db.seed_request(make_request_row(
        id=867,
        status="wanted",
        active_download_state=None,
        artist_name="Artist",
        album_title="Album",
        mb_release_id="mb-867",
    ))
    job = db.enqueue_import_job(
        IMPORT_JOB_YOUTUBE,
        request_id=867,
        dedupe_key=youtube_import_dedupe_key(39310),
        payload=youtube_import_payload(
            staged_path=staged_path,
            request_id=867,
            browse_id="MPREb_generated",
            download_log_id=39310,
        ),
    )
    ready = db.mark_import_job_preview_importable(
        job.id,
        preview_result={"verdict": "would_import"},
        message="ready",
    )
    if ready is None:
        raise AssertionError("youtube producer fixture was not importable")
    claimed = claim_next_import_job(db, worker_id="producer-contract")
    if claimed is None:
        raise AssertionError("youtube producer fixture was not claimable")
    ctx = _runtime_context(db, cfg)

    def reject_staged_entry(
        album_data: GrabListEntry,
        ctx: CratediggerContext,
        *,
        import_job_id: int,
        **_kwargs: object,
    ) -> CompletionDispatched:
        outcome = _handle_rejected_result(
            album_data,
            _rejection_result(scenario),
            StagedAlbum.from_entry(album_data, default_path=staged_path),
            ctx,
            import_job_id=import_job_id,
        )
        return CompletionDispatched(outcome=outcome)

    terminal = importer.process_claimed_job(
        db,  # pyright: ignore[reportArgumentType]
        claimed,
        ctx=ctx,
        execute_fn=partial(
            importer.execute_youtube_import_job,
            process_album_fn=reject_staged_entry,
        ),
    )
    if terminal is None or terminal.status != "failed":
        raise AssertionError("youtube rejection did not reach terminal persistence")
    if db.list_denylist_rows():
        raise AssertionError("youtube rejection persisted a peer denylist")
    if db.cooldowns_applied or db.user_cooldowns:
        raise AssertionError("youtube rejection evaluated or persisted a cooldown")
    log_id, failed_path = _persisted_rejection(db)
    return ProducedRejection(db, cfg, log_id, failed_path)


def _produce_rejection(
    lane: str,
    cfg: CratediggerConfig,
    lane_root: str,
    *,
    scenario: str,
    nested: bool,
    album_name: str = "Artist - Album",
) -> ProducedRejection:
    if lane == "youtube_staging":
        return _produce_youtube_rejection(
            cfg,
            lane_root,
            scenario=scenario,
            nested=nested,
            album_name=album_name,
        )
    return _produce_download_rejection(
        cfg,
        lane_root,
        scenario=scenario,
        nested=nested,
        album_name=album_name,
    )


def _replace_persisted_path(produced: ProducedRejection, path: str) -> None:
    row = next(
        row
        for row in produced.db.download_logs
        if row.id == produced.download_log_id
    )
    validation = decode_validation_envelope(row.validation_result)
    payload: dict[str, object] = msgspec.to_builtins(validation)
    payload["failed_path"] = path
    row.validation_result = payload


def _enqueue(produced: ProducedRejection) -> ForceImportEnqueueResult:
    return enqueue_force_import(
        produced.db,
        produced.cfg,
        produced.download_log_id,
    )


def _force_job_count(db: FakePipelineDB) -> int:
    return sum(
        job.job_type == IMPORT_JOB_FORCE
        for job in db.list_import_jobs()
    )


class TestForceImportProducerAuthorityGenerated(unittest.TestCase):
    def test_live_youtube_staging_wrong_match_is_force_importable(self) -> None:
        """Pin #1016: a provenance-free staged Wrong Match is authorized."""
        with tempfile.TemporaryDirectory() as tmp:
            cfg, lane_roots = _configured_world(tmp)
            produced = _produce_rejection(
                "youtube_staging",
                cfg,
                lane_roots["youtube_staging"],
                scenario="high_distance",
                nested=False,
                album_name="Loon_Lake-Low_Res-playlist-request-111-log-39310",
            )
            self.assertEqual(
                produced.failed_path,
                os.path.join(
                    cfg.beets_staging_dir,
                    "auto-import",
                    "wrong_matches",
                    "Loon_Lake-Low_Res-playlist-request-111-log-39310",
                ),
            )
            result = _enqueue(produced)
            assert_force_import_authority_invariant(
                authorized=True,
                outcome=result.outcome,
                job_count=_force_job_count(produced.db),
            )
            assert result.job is not None
            self.assertIsInstance(result.job.payload, ForceImportPayload)
            assert isinstance(result.job.payload, ForceImportPayload)
            self.assertIsNone(result.job.payload.source_username)
            self.assertEqual(result.job.payload.source_dirs, [])

    def test_every_produced_quarantine_path_matches_force_import_authority(self) -> None:
        """Exhaust all 108 producer lane/rejection/layout/path worlds."""
        worlds = product(
            PRODUCER_LANES,
            sorted(REJECTION_CLASSES.items()),
            (False, True),
            PATH_CASES,
        )
        for lane, (scenario, expected_components), nested, path_case in worlds:
            with self.subTest(
                lane=lane,
                scenario=scenario,
                nested=nested,
                path_case=path_case,
            ), tempfile.TemporaryDirectory() as tmp:
                cfg, lane_roots = _configured_world(tmp)
                produced = _produce_rejection(
                    lane,
                    cfg,
                    lane_roots[lane],
                    scenario=scenario,
                    nested=nested,
                )
                produced_parts = Path(produced.failed_path).parts
                self.assertEqual(
                    produced_parts[-1 - len(expected_components):-1],
                    expected_components,
                )

                if path_case == "missing":
                    shutil.rmtree(produced.failed_path)
                elif path_case == "lookalike":
                    authority_marker = expected_components[0]
                    marker_dir = next(
                        parent
                        for parent in Path(produced.failed_path).parents
                        if parent.name == authority_marker
                    )
                    relative_below_marker = Path(produced.failed_path).relative_to(
                        marker_dir
                    )
                    lookalike_dir = marker_dir.with_name(
                        f"{authority_marker}-lookalike"
                    )
                    os.rename(marker_dir, lookalike_dir)
                    _replace_persisted_path(
                        produced,
                        str(lookalike_dir / relative_below_marker),
                    )

                result = _enqueue(produced)
                assert_force_import_authority_invariant(
                    authorized=path_case == "existing",
                    outcome=result.outcome,
                    job_count=_force_job_count(produced.db),
                )

    def test_produced_escape_outside_every_configured_root_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg, _lane_roots = _configured_world(tmp)
            outside = os.path.join(tmp, "outside")
            produced = _produce_rejection(
                "slskd",
                cfg,
                outside,
                scenario="high_distance",
                nested=True,
            )
            result = _enqueue(produced)
            assert_force_import_authority_invariant(
                authorized=False,
                outcome=result.outcome,
                job_count=_force_job_count(produced.db),
            )

    def test_produced_path_replaced_by_symlink_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg, lane_roots = _configured_world(tmp)
            produced = _produce_rejection(
                "slskd",
                cfg,
                lane_roots["slskd"],
                scenario="high_distance",
                nested=False,
            )
            moved = os.path.join(tmp, "moved-produced-album")
            os.rename(produced.failed_path, moved)
            os.symlink(moved, produced.failed_path)
            result = _enqueue(produced)
            assert_force_import_authority_invariant(
                authorized=False,
                outcome=result.outcome,
                job_count=_force_job_count(produced.db),
            )

    def test_faulted_rejection_path_adapter_is_detected(self) -> None:
        """Known-bad: bypassing download_rejection's mover cannot survive."""
        with tempfile.TemporaryDirectory() as tmp:
            cfg, lane_roots = _configured_world(tmp)
            with patch(
                "lib.download_rejection.move_failed_import_curated",
                side_effect=RuntimeError("producer adapter mutant"),
            ), self.assertRaisesRegex(RuntimeError, "producer adapter mutant"):
                _produce_rejection(
                    "slskd",
                    cfg,
                    lane_roots["slskd"],
                    scenario="high_distance",
                    nested=False,
                )

    def test_staging_wrong_match_authorization_mutant_trips_checker(self) -> None:
        """Known-bad: removing staging Wrong Matches authority is detected."""
        with self.assertRaisesRegex(
            AssertionError,
            "did not enqueue exactly once",
        ):
            assert_force_import_authority_invariant(
                authorized=True,
                outcome="unauthorized_path",
                job_count=0,
            )

    def test_invariant_checker_rejects_non_produced_job(self) -> None:
        with self.assertRaisesRegex(AssertionError, "non-produced"):
            assert_force_import_authority_invariant(
                authorized=False,
                outcome=RESULT_QUEUED,
                job_count=1,
            )
