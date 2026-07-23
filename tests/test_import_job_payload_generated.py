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
    validate_payload,
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
        "download_log_id": draw(st.integers(min_value=1, max_value=1_000_000)),
    }
    return job_type, payload


@st.composite
def invalid_payload_rows(draw: st.DrawFn) -> tuple[str, dict[str, object]]:
    job_type, payload = draw(valid_payload_rows())
    if job_type == IMPORT_JOB_AUTOMATION:
        payload["unexpected"] = draw(st.booleans())
        return job_type, payload

    if job_type == IMPORT_JOB_FORCE:
        defect = draw(st.sampled_from((
            "download_missing",
            "download_none",
            "download_zero",
            "download_negative",
            "download_bool",
            "download_wrong_type",
            "path_missing",
            "path_none",
            "path_empty",
            "path_wrong_type",
            "extra",
        )))
        if defect == "download_missing":
            payload.pop("download_log_id")
        elif defect == "download_none":
            payload["download_log_id"] = None
        elif defect == "download_zero":
            payload["download_log_id"] = 0
        elif defect == "download_negative":
            payload["download_log_id"] = draw(st.integers(max_value=-1))
        elif defect == "download_bool":
            payload["download_log_id"] = draw(st.booleans())
        elif defect == "download_wrong_type":
            payload["download_log_id"] = draw(_TEXT)
        elif defect == "path_missing":
            payload.pop("failed_path")
        elif defect == "path_none":
            payload["failed_path"] = None
        elif defect == "path_empty":
            payload["failed_path"] = ""
        elif defect == "path_wrong_type":
            payload["failed_path"] = draw(st.integers())
        else:
            payload["unexpected"] = draw(st.booleans())
        return job_type, payload

    defect = draw(st.sampled_from((
        "request_missing",
        "request_none",
        "request_zero",
        "request_negative",
        "request_bool",
        "request_wrong_type",
        "download_missing",
        "download_none",
        "download_zero",
        "download_negative",
        "download_bool",
        "download_wrong_type",
        "path_missing",
        "path_none",
        "path_empty",
        "path_wrong_type",
        "browse_missing",
        "browse_none",
        "browse_empty",
        "browse_wrong_type",
        "extra",
    )))
    field, _, failure = defect.partition("_")
    key = {
        "request": "request_id",
        "download": "download_log_id",
        "path": "staged_path",
        "browse": "browse_id",
    }.get(field)
    if defect == "extra":
        payload["unexpected"] = draw(st.booleans())
    elif failure == "missing":
        assert key is not None
        payload.pop(key)
    elif failure == "none":
        assert key is not None
        payload[key] = None
    elif failure == "zero":
        assert key is not None
        payload[key] = 0
    elif failure == "negative":
        assert key is not None
        payload[key] = draw(st.integers(max_value=-1))
    elif failure == "bool":
        assert key is not None
        payload[key] = draw(st.booleans())
    elif failure == "empty":
        assert key is not None
        payload[key] = ""
    elif failure == "wrong_type":
        assert key is not None
        payload[key] = (
            draw(_TEXT)
            if key in ("request_id", "download_log_id")
            else draw(st.integers())
        )
    else:
        raise AssertionError(f"unhandled generated defect: {defect}")
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


def assert_payload_rejected_at_read_and_prewrite(
    job_type: str,
    payload: dict[str, object],
) -> None:
    """Malformed payloads fail at both canonical boundary entry points."""
    boundaries = (
        ("ImportJob.from_row", lambda: ImportJob.from_row(_row(job_type, payload))),
        ("validate_payload", lambda: validate_payload(job_type, payload)),
    )
    for name, invoke in boundaries:
        try:
            invoke()
        except msgspec.ValidationError:
            continue
        raise AssertionError(f"{name} accepted malformed {job_type} payload")


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

    def test_rejects_a_valid_payload_planted_as_known_bad(self) -> None:
        with self.assertRaisesRegex(AssertionError, "accepted malformed"):
            assert_payload_rejected_at_read_and_prewrite(
                IMPORT_JOB_FORCE,
                {"download_log_id": 1, "failed_path": "/tmp/valid"},
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
        self.assertEqual(
            validate_payload(job_type, payload),
            job.to_dict()["payload"],
        )

    @given(row=invalid_payload_rows())
    @example(row=(IMPORT_JOB_FORCE, {
        "download_log_id": "37206",
        "failed_path": "/tmp/failed",
    }))
    @example(row=(IMPORT_JOB_YOUTUBE, {
        "staged_path": "/tmp/staged",
        "request_id": True,
        "browse_id": "MPREb_generated",
        "download_log_id": 1,
    }))
    def test_malformed_values_fail_at_read_and_prewrite_boundaries(
        self,
        row: tuple[str, dict[str, object]],
    ) -> None:
        assert_payload_rejected_at_read_and_prewrite(*row)
