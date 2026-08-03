"""Generated authority worlds for force-import preflight."""

from __future__ import annotations

import os
import tempfile
import unittest
from itertools import product
from types import SimpleNamespace

from lib.force_import_service import RESULT_QUEUED, enqueue_force_import
from lib.processing_paths import processing_albums_dir
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row

# Stated here rather than imported from lib.fs_authority: an oracle derived
# from the production table cannot detect that table widening. The three
# configured quarantine roots all authorize the two exact quarantine markers.
# This includes YouTube rejects under staging/auto-import/wrong_matches.
MARKERS_BY_ROOT: dict[str, frozenset[str]] = {
    "slskd": frozenset({"failed_imports", "wrong_matches"}),
    "staging": frozenset({"failed_imports", "wrong_matches"}),
    "processing": frozenset({"failed_imports", "wrong_matches"}),
}

_MARKERS = ("failed_imports", "wrong_matches", "failed_imports-lookalike")


def assert_force_import_authority_invariant(
    *, authorized: bool, outcome: str, job_count: int,
) -> None:
    """Only a configured quarantine directory may produce a queue job."""
    if authorized and (outcome != RESULT_QUEUED or job_count != 1):
        raise AssertionError("authorized quarantine source did not enqueue exactly once")
    if not authorized and job_count != 0:
        raise AssertionError("unauthorized source created an import job")


class TestForceImportAuthorityGenerated(unittest.TestCase):
    def test_only_existing_configured_quarantine_sources_enqueue(self) -> None:
        # Exhaustive finite authority table. It includes both staging
        # quarantine markers and component-lookalike worlds.
        worlds = product(
            sorted(MARKERS_BY_ROOT),
            _MARKERS,
            (False, True),
            (False, True),
        )
        for root, marker, missing, nested in worlds:
            with self.subTest(
                root=root,
                marker=marker,
                missing=missing,
                nested=nested,
            ), tempfile.TemporaryDirectory() as tmp:
                staging = os.path.join(tmp, "Incoming")
                slskd = os.path.join(tmp, "slskd")
                processing = os.path.join(tmp, "processing")
                os.makedirs(staging)
                os.makedirs(slskd)
                # The processing quarantine root is the albums/ child, not the
                # processing dir itself.
                os.makedirs(processing_albums_dir(processing))
                root_path = {
                    "slskd": slskd,
                    "staging": staging,
                    "processing": processing_albums_dir(processing),
                }[root]
                prefix = ("auto-import", "Artist") if nested else ()
                path = os.path.join(root_path, *prefix, marker, "Album")
                if not missing:
                    os.makedirs(path)
                db = FakePipelineDB()
                db.seed_request(make_request_row(id=867, mb_release_id="mb-867"))
                log_id = db.log_download(
                    request_id=867,
                    outcome="rejected",
                    validation_result={"failed_path": path},
                )
                cfg = SimpleNamespace(
                    beets_staging_dir=staging,
                    slskd_download_dir=slskd,
                    processing_dir=processing,
                )
                result = enqueue_force_import(db, cfg, log_id)
                assert_force_import_authority_invariant(
                    authorized=marker in MARKERS_BY_ROOT[root] and not missing,
                    outcome=result.outcome,
                    job_count=len(db.list_import_jobs()),
                )

    def test_invariant_checker_rejects_unauthorized_job(self) -> None:
        with self.assertRaisesRegex(AssertionError, "unauthorized"):
            assert_force_import_authority_invariant(
                authorized=False, outcome=RESULT_QUEUED, job_count=1,
            )

    def test_invariant_checker_rejects_a_refused_authorized_source(self) -> None:
        with self.assertRaisesRegex(AssertionError, "did not enqueue exactly once"):
            assert_force_import_authority_invariant(
                authorized=True, outcome="not_found", job_count=0,
            )
