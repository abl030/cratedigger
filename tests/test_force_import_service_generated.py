"""Generated producer-to-consumer authority contract for force import."""

from __future__ import annotations

import os
import shutil
import tempfile
import unittest
from itertools import product
from pathlib import Path
from types import SimpleNamespace

from lib.force_import_service import RESULT_QUEUED, enqueue_force_import
from lib.import_manifest import move_failed_import_curated
from lib.processing_paths import processing_albums_dir
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row

# Independently authored producer inventory. These are the rejection lanes that
# can persist a force-importable quarantine path; importing the authority table
# from lib.fs_authority would make this contract agree by construction.
PRODUCER_LANES = ("slskd", "youtube_staging", "private_processing")
REJECTION_CLASSES: dict[str, tuple[str, ...]] = {
    "high_distance": ("wrong_matches",),
    "spectral_reject": ("failed_imports", "bad_files"),
}
PATH_CASES = ("existing", "missing", "lookalike")


def assert_force_import_authority_invariant(
    *, authorized: bool, outcome: str, job_count: int,
) -> None:
    """Exactly one real produced quarantine source may create a queue job."""
    if authorized and (outcome != RESULT_QUEUED or job_count != 1):
        raise AssertionError("produced quarantine source did not enqueue exactly once")
    if not authorized and job_count != 0:
        raise AssertionError("non-produced source created an import job")


def _configured_world(tmp: str) -> tuple[SimpleNamespace, dict[str, str]]:
    staging = os.path.join(tmp, "Incoming")
    slskd = os.path.join(tmp, "slskd")
    processing = os.path.join(tmp, "processing")
    os.makedirs(staging)
    os.makedirs(slskd)
    os.makedirs(processing_albums_dir(processing))
    cfg = SimpleNamespace(
        beets_staging_dir=staging,
        slskd_download_dir=slskd,
        processing_dir=processing,
    )
    return cfg, {
        "slskd": os.path.join(slskd, "peer"),
        "youtube_staging": os.path.join(staging, "auto-import"),
        "private_processing": processing_albums_dir(processing),
    }


def _produce_rejection(
    lane_root: str,
    *,
    scenario: str,
    nested: bool,
    album_name: str = "Artist - Album",
) -> str:
    source_parent = os.path.join(lane_root, "nested") if nested else lane_root
    source = os.path.join(source_parent, album_name)
    os.makedirs(source)
    track = os.path.join(source, "01 - Track.flac")
    with open(track, "wb") as handle:
        handle.write(b"audio")
    produced = move_failed_import_curated(
        source,
        allowed_audio=["01 - Track.flac"],
        scenario=scenario,
    )
    assert produced is not None
    return produced


def _enqueue(produced_path: str, cfg: object) -> tuple[str, int]:
    db = FakePipelineDB()
    db.seed_request(make_request_row(id=867, mb_release_id="mb-867"))
    log_id = db.log_download(
        request_id=867,
        outcome="rejected",
        soulseek_username="",
        validation_result={
            "failed_path": produced_path,
            "source_dirs": [],
        },
    )
    result = enqueue_force_import(db, cfg, log_id)
    return result.outcome, len(db.list_import_jobs())


class TestForceImportProducerAuthorityGenerated(unittest.TestCase):
    def test_live_youtube_staging_wrong_match_is_force_importable(self) -> None:
        """Pin #1016: a provenance-free staged Wrong Match is authorized."""
        with tempfile.TemporaryDirectory() as tmp:
            cfg, lane_roots = _configured_world(tmp)
            produced = _produce_rejection(
                lane_roots["youtube_staging"],
                scenario="high_distance",
                nested=False,
                album_name="Loon_Lake-Low_Res-playlist-request-111-log-39310",
            )
            self.assertEqual(
                produced,
                os.path.join(
                    cfg.beets_staging_dir,
                    "auto-import",
                    "wrong_matches",
                    "Loon_Lake-Low_Res-playlist-request-111-log-39310",
                ),
            )
            outcome, job_count = _enqueue(produced, cfg)
            assert_force_import_authority_invariant(
                authorized=True,
                outcome=outcome,
                job_count=job_count,
            )

    def test_every_produced_quarantine_path_matches_force_import_authority(self) -> None:
        """Exhaust all 36 producer lane/rejection/layout/path worlds."""
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
                    lane_roots[lane],
                    scenario=scenario,
                    nested=nested,
                )
                produced_parts = Path(produced).parts
                self.assertEqual(
                    produced_parts[-1 - len(expected_components):-1],
                    expected_components,
                )

                candidate = produced
                if path_case == "missing":
                    shutil.rmtree(produced)
                elif path_case == "lookalike":
                    authority_marker = expected_components[0]
                    marker_dir = next(
                        parent
                        for parent in Path(produced).parents
                        if parent.name == authority_marker
                    )
                    relative_below_marker = Path(produced).relative_to(marker_dir)
                    lookalike_dir = marker_dir.with_name(
                        f"{authority_marker}-lookalike"
                    )
                    os.rename(marker_dir, lookalike_dir)
                    candidate = str(lookalike_dir / relative_below_marker)

                outcome, job_count = _enqueue(candidate, cfg)
                assert_force_import_authority_invariant(
                    authorized=path_case == "existing",
                    outcome=outcome,
                    job_count=job_count,
                )

    def test_produced_escape_outside_every_configured_root_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg, _lane_roots = _configured_world(tmp)
            outside = os.path.join(tmp, "outside")
            produced = _produce_rejection(
                outside,
                scenario="high_distance",
                nested=True,
            )
            outcome, job_count = _enqueue(produced, cfg)
            assert_force_import_authority_invariant(
                authorized=False,
                outcome=outcome,
                job_count=job_count,
            )

    def test_produced_path_replaced_by_symlink_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg, lane_roots = _configured_world(tmp)
            produced = _produce_rejection(
                lane_roots["slskd"],
                scenario="high_distance",
                nested=False,
            )
            moved = os.path.join(tmp, "moved-produced-album")
            os.rename(produced, moved)
            os.symlink(moved, produced)
            outcome, job_count = _enqueue(produced, cfg)
            assert_force_import_authority_invariant(
                authorized=False,
                outcome=outcome,
                job_count=job_count,
            )

    def test_invariant_checker_rejects_non_produced_job(self) -> None:
        with self.assertRaisesRegex(AssertionError, "non-produced"):
            assert_force_import_authority_invariant(
                authorized=False,
                outcome=RESULT_QUEUED,
                job_count=1,
            )

    def test_invariant_checker_rejects_a_refused_produced_source(self) -> None:
        with self.assertRaisesRegex(AssertionError, "did not enqueue exactly once"):
            assert_force_import_authority_invariant(
                authorized=True,
                outcome="unauthorized_path",
                job_count=0,
            )
