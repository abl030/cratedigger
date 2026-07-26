"""Generated authority worlds for force-import preflight."""

from __future__ import annotations

import os
import tempfile
import unittest
from types import SimpleNamespace

from hypothesis import given, strategies as st

from lib.force_import_service import RESULT_QUEUED, enqueue_force_import
import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


def assert_force_import_authority_invariant(
    *, authorized: bool, outcome: str, job_count: int,
) -> None:
    """Only a configured quarantine directory may produce a queue job."""
    if authorized and (outcome != RESULT_QUEUED or job_count != 1):
        raise AssertionError("authorized quarantine source did not enqueue exactly once")
    if not authorized and job_count != 0:
        raise AssertionError("unauthorized source created an import job")


class TestForceImportAuthorityGenerated(unittest.TestCase):
    @given(authorized=st.booleans(), missing=st.booleans())
    def test_only_existing_configured_quarantine_sources_enqueue(
        self, authorized: bool, missing: bool,
    ) -> None:
        with tempfile.TemporaryDirectory() as root:
            staging = os.path.join(root, "Incoming")
            slskd = os.path.join(root, "slskd")
            processing = os.path.join(root, "processing")
            os.makedirs(staging)
            os.makedirs(slskd)
            os.makedirs(processing)
            path = os.path.join(
                staging,
                "failed_imports" if authorized else "failed_imports-lookalike",
                "Album",
            )
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
                authorized=authorized and not missing,
                outcome=result.outcome,
                job_count=len(db.list_import_jobs()),
            )

    def test_invariant_checker_rejects_unauthorized_job(self) -> None:
        with self.assertRaisesRegex(AssertionError, "unauthorized"):
            assert_force_import_authority_invariant(
                authorized=False, outcome=RESULT_QUEUED, job_count=1,
            )
