"""Generated strict payload-boundary tests for CD-SEC-19."""

from __future__ import annotations

from dataclasses import replace
import unittest

import msgspec
from hypothesis import example, given, strategies as st

from lib.import_queue import (
    AutomationImportPayload,
    ForceImportPayload,
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_YOUTUBE,
    ImportJob,
    YoutubeImportPayload,
)
import tests._hypothesis_profiles  # noqa: F401


_TEXT = st.text(
    alphabet=st.characters(blacklist_categories=("Cs", "Cc")),
    min_size=1,
    max_size=30,
)


def _row(job_type: str, payload: dict[str, object]) -> dict[str, object]:
    return {
        "id": 1,
        "job_type": job_type,
        "status": "queued",
        "payload": payload,
    }


@st.composite
def valid_payload_rows(draw: st.DrawFn) -> tuple[str, dict[str, object]]:
    job_type = draw(st.sampled_from((
        IMPORT_JOB_FORCE,
        IMPORT_JOB_AUTOMATION,
        IMPORT_JOB_YOUTUBE,
    )))
    if job_type == IMPORT_JOB_FORCE:
        payload: dict[str, object] = {
            "download_log_id": draw(st.integers(min_value=1, max_value=1_000_000)),
            "failed_path": "/tmp/" + draw(_TEXT),
        }
        source_username = draw(st.one_of(st.none(), _TEXT))
        if source_username is not None:
            payload["source_username"] = source_username
        source_dirs = draw(st.lists(_TEXT.map(lambda value: "/tmp/" + value), max_size=3))
        if source_dirs:
            payload["source_dirs"] = source_dirs
        return job_type, payload
    if job_type == IMPORT_JOB_AUTOMATION:
        return job_type, {}
    payload = {
        "staged_path": "/tmp/" + draw(_TEXT),
        "request_id": draw(st.integers(min_value=1, max_value=1_000_000)),
        "browse_id": draw(_TEXT),
    }
    download_log_id = draw(st.one_of(
        st.none(), st.integers(min_value=1, max_value=1_000_000),
    ))
    if download_log_id is not None:
        payload["download_log_id"] = download_log_id
    return job_type, payload


def assert_job_payload_matches_type(job: ImportJob) -> None:
    """Every decoded payload uses the Struct selected by its discriminator."""
    expected = {
        IMPORT_JOB_FORCE: ForceImportPayload,
        IMPORT_JOB_AUTOMATION: AutomationImportPayload,
        IMPORT_JOB_YOUTUBE: YoutubeImportPayload,
    }[job.job_type]
    if not isinstance(job.payload, expected):
        raise AssertionError(
            f"{job.job_type} decoded to {type(job.payload).__name__}, "
            f"not {expected.__name__}",
        )


class TestPayloadCheckerTripsOnKnownBadJobs(unittest.TestCase):
    def test_rejects_payload_struct_from_a_different_job_type(self) -> None:
        job = ImportJob.from_row(_row(
            IMPORT_JOB_FORCE,
            {"download_log_id": 37206, "failed_path": "/tmp/failed"},
        ))
        with self.assertRaisesRegex(AssertionError, "force_import decoded"):
            assert_job_payload_matches_type(
                replace(job, payload=AutomationImportPayload()),
            )


class TestImportJobPayloadBoundaryGenerated(unittest.TestCase):
    @given(row=valid_payload_rows())
    @example(row=(IMPORT_JOB_FORCE, {
        "download_log_id": 37206,
        "failed_path": "/tmp/failed",
    }))
    def test_every_valid_job_payload_decodes_to_its_declared_struct(
        self,
        row: tuple[str, dict[str, object]],
    ) -> None:
        job_type, payload = row
        job = ImportJob.from_row(_row(job_type, payload))
        assert_job_payload_matches_type(job)

    @given(
        job_type=st.sampled_from((
            IMPORT_JOB_FORCE,
            IMPORT_JOB_AUTOMATION,
            IMPORT_JOB_YOUTUBE,
        )),
        unexpected=_TEXT,
    )
    @example(job_type=IMPORT_JOB_FORCE, unexpected="unexpected")
    def test_extra_fields_fail_closed_for_every_job_type(
        self,
        job_type: str,
        unexpected: str,
    ) -> None:
        payload: dict[str, object]
        if job_type == IMPORT_JOB_FORCE:
            payload = {"download_log_id": 1, "failed_path": "/tmp/failed"}
        elif job_type == IMPORT_JOB_AUTOMATION:
            payload = {}
        else:
            payload = {
                "staged_path": "/tmp/staged",
                "request_id": 1,
                "browse_id": "MPREb_generated",
            }
        payload[unexpected] = True
        with self.assertRaises(msgspec.ValidationError):
            ImportJob.from_row(_row(job_type, payload))
