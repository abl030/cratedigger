#!/usr/bin/env python3
"""Generated evidence-lifecycle tests — issue #548.

Property-based port of the local fuzzer that found the V0-evidence bug
fixed in ``6cf26a4`` (require source V0 for converted current evidence):
a current-evidence row representing a lossless-source transcode must never
become action-ready without its source V0 metric.

For each generated world the test builds the real on-disk + DB state — a
staged album folder, a ``FakePipelineDB`` request row, and a converted current
evidence row with no linked V0 metric — then runs the production action loader
(``ensure_current_evidence_for_action``) and asserts:

1. the stale transcode row is never accepted as ``current_status=loaded``;
2. mutating the request-row V0 stamps cannot change the action result;
3. missing linked acquisition evidence fails closed (not available).

Profiles and promotion policy: tests/_hypothesis_profiles.py and
docs/generated-testing.md. The exact minimized cases from the original
RED run are committed in tests/test_import_evidence.py; the ``@example``
pin below keeps the original failing shape replaying here forever.
"""

import configparser
import os
import shutil
import sys
import tempfile
import unittest
from dataclasses import dataclass
from typing import Any, Literal, cast
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

from hypothesis import example, given
from hypothesis import strategies as st

from lib.beets_db import AlbumInfo
from lib.import_evidence import ensure_current_evidence_for_action
from lib.import_preview import measure_and_persist_candidate_evidence
from lib.measurement import ExistingSpectralAuditLookup
from lib.quality import (
    AlbumQualityEvidence,
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    SpectralAnalysisDetail,
    VerifiedLosslessProof,
)
from lib.quality_evidence import (
    backfill_current_evidence_from_album_info,
    snapshot_audio_files,
)
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import make_album_quality_evidence, make_request_row


@dataclass(frozen=True)
class EvidenceLifecycleWorld:
    """One stale-converted-current-evidence world."""
    extension: str            # on-disk transcode container: "opus" | "mp3"
    was_converted_from: str   # lossless source lineage: "flac"|"alac"|"wav"
    source_v0_avg: int
    source_v0_min: int
    stale_min_bitrate: int
    stale_avg_bitrate: int

    @property
    def storage_format(self) -> str:
        return "Opus" if self.extension == "opus" else "MP3"


@st.composite
def evidence_lifecycle_worlds(draw) -> EvidenceLifecycleWorld:
    avg = draw(st.integers(min_value=1, max_value=400))
    stale_min = draw(st.integers(min_value=1, max_value=400))
    return EvidenceLifecycleWorld(
        extension=draw(st.sampled_from(("opus", "mp3"))),
        was_converted_from=draw(st.sampled_from(("flac", "alac", "wav"))),
        source_v0_avg=avg,
        source_v0_min=max(avg - draw(st.integers(min_value=0, max_value=50)), 1),
        stale_min_bitrate=stale_min,
        stale_avg_bitrate=stale_min + draw(st.integers(min_value=0, max_value=50)),
    )


def assert_lifecycle_outcome(
    *,
    current_status: str | None,
    available: bool,
    result_v0_avg: int | None,
) -> None:
    """Missing linked acquisition evidence always fails closed."""
    if current_status == "loaded":
        raise AssertionError(
            "lossless-source transcode current evidence loaded without "
            "V0 metric")
    if available or result_v0_avg is not None:
        raise AssertionError(
            "request stamps resurrected a missing linked V0 fact")


def _run_world(
    world: EvidenceLifecycleWorld,
    *,
    request_has_v0_scalar: bool,
) -> tuple[str | None, bool, int | None]:
    """Build the world's on-disk + DB state and run the action loader."""
    root = tempfile.mkdtemp(prefix="cratedigger-evidence-gen-")
    try:
        audio_path = os.path.join(root, f"01 - Track.{world.extension}")
        with open(audio_path, "wb") as handle:
            handle.write(b"generated-audio")

        request_id = 1
        mbid = "evidence-generated-mbid"
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=request_id, mb_release_id=mbid))
        if request_has_v0_scalar:
            db.update_request_fields(
                request_id,
                current_spectral_grade="likely_transcode",
                current_spectral_bitrate=128,
                current_lossless_source_v0_probe_min_bitrate=world.source_v0_min,
                current_lossless_source_v0_probe_avg_bitrate=world.source_v0_avg,
                current_lossless_source_v0_probe_median_bitrate=world.source_v0_avg,
            )

        stale_current = make_album_quality_evidence(
            mb_release_id=mbid,
            files=snapshot_audio_files(root),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=world.stale_min_bitrate,
                avg_bitrate_kbps=world.stale_avg_bitrate,
                median_bitrate_kbps=world.stale_avg_bitrate,
                format=world.storage_format,
                is_cbr=False,
                spectral_grade=None,
                spectral_bitrate_kbps=None,
                was_converted_from=world.was_converted_from,
            ),
            v0_metric=None,
            codec=world.extension,
            container=world.extension,
            storage_format=world.storage_format,
        )
        db.upsert_album_quality_evidence(stale_current)
        persisted = db.find_album_quality_evidence(
            mb_release_id=mbid,
            snapshot_fingerprint=stale_current.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_request_current_evidence(request_id, persisted.id)

        result = ensure_current_evidence_for_action(
            db,
            request_id=request_id,
            mb_release_id=mbid,
            current_album_path=root,
            album_info=AlbumInfo(
                album_id=1,
                track_count=1,
                min_bitrate_kbps=world.stale_min_bitrate,
                avg_bitrate_kbps=world.stale_avg_bitrate,
                median_bitrate_kbps=world.stale_avg_bitrate,
                is_cbr=False,
                album_path=root,
                format=world.storage_format,
            ),
        )
        result_v0_avg = (
            result.evidence.v0_metric.avg_bitrate_kbps
            if result.evidence is not None and result.evidence.v0_metric is not None
            else None
        )
        return result.provenance.current_status, result.available, result_v0_avg
    finally:
        shutil.rmtree(root, ignore_errors=True)


# The exact world shape of the original RED run (seed 548 case 0 of the
# pre-Hypothesis fuzzer): an opus transcode from flac with the legacy
# request scalar present. Fix 6cf26a4; exact minimized twins live in
# tests/test_import_evidence.py.
_ORIGINAL_RED_WORLD = EvidenceLifecycleWorld(
    extension="opus",
    was_converted_from="flac",
    source_v0_avg=171,
    source_v0_min=171,
    stale_min_bitrate=108,
    stale_avg_bitrate=114,
)


class TestGeneratedEvidenceLifecycle(unittest.TestCase):
    """Action-loader invariants over generated stale-current worlds."""

    @given(world=evidence_lifecycle_worlds())
    @example(world=_ORIGINAL_RED_WORLD)
    def test_request_scalar_cannot_resurrect_source_v0(self, world):
        without_scalar = _run_world(world, request_has_v0_scalar=False)
        with_scalar = _run_world(world, request_has_v0_scalar=True)
        self.assertEqual(with_scalar, without_scalar)
        current_status, available, result_v0_avg = with_scalar
        assert_lifecycle_outcome(
            current_status=current_status,
            available=available,
            result_v0_avg=result_v0_avg,
        )


@dataclass(frozen=True)
class BlankPathWorld:
    """One current-evidence world varying source_path recordability."""
    source_path_kind: str      # "blank" | "whitespace" | "real"
    spectral_grade: str | None
    min_bitrate: int
    avg_bitrate: int
    # Converted-from-lossless rows interact with the lossless-source-V0
    # fail-closed branch. Request stamps never rescue the missing linked fact.
    was_converted_from: str | None = None
    request_has_v0_scalar: bool = False


@st.composite
def blank_path_worlds(draw) -> BlankPathWorld:
    min_bitrate = draw(st.integers(min_value=1, max_value=400))
    return BlankPathWorld(
        source_path_kind=draw(
            st.sampled_from(("blank", "whitespace", "real"))
        ),
        spectral_grade=draw(
            st.sampled_from((None, "genuine", "likely_transcode"))
        ),
        min_bitrate=min_bitrate,
        avg_bitrate=min_bitrate + draw(st.integers(min_value=0, max_value=100)),
        was_converted_from=draw(st.sampled_from((None, "flac"))),
        request_has_v0_scalar=draw(st.booleans()),
    )


def assert_blank_path_outcome(
    *,
    source_path_kind: str,
    requires_lossless_v0: bool,
    current_status: str | None,
    available: bool,
    result_source_path: str | None,
) -> None:
    """A blank-source_path row is never authoritative for an action.

    The invariant behind download_log 37206 (French Quarter): a row whose
    recorded path is blank can never be re-verified against disk nor
    enriched with HAVE spectral, so the loader must rebuild it — never
    hand it to the decision as ``loaded``. The one legitimate non-rebuild
    outcome is the lossless-source-V0 guard: a converted row missing the linked
    acquisition fact fails closed instead (a disk rebuild would fabricate
    provenance). Request stamps cannot change that outcome. Here it only
    shapes which blank-path outcome is legal.
    """
    if source_path_kind == "real":
        if not requires_lossless_v0 and current_status != "loaded":
            raise AssertionError(
                "complete current evidence with a real source_path must "
                f"load as authoritative (got {current_status})")
        return
    if current_status == "loaded":
        raise AssertionError(
            "blank-source_path current evidence was loaded as authoritative")
    if requires_lossless_v0:
        if available:
            raise AssertionError(
                "lossless-source row without a V0 backfill source "
                "must fail closed, not become available")
        return
    if not available:
        raise AssertionError(
            "blank-source_path row must rebuild from album_info, "
            "not fail closed")
    if not (result_source_path or "").strip():
        raise AssertionError(
            "rebuilt action evidence still carries a blank source_path")


def _run_blank_path_world(
    world: BlankPathWorld,
) -> tuple[str | None, bool, str | None]:
    """Build the world's on-disk + DB state and run the action loader."""
    root = tempfile.mkdtemp(prefix="cratedigger-blankpath-gen-")
    try:
        with open(os.path.join(root, "01 - Track.mp3"), "wb") as handle:
            handle.write(b"generated-audio")

        request_id = 1
        mbid = "blank-path-generated-mbid"
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=request_id, mb_release_id=mbid))
        if world.request_has_v0_scalar:
            db.update_request_fields(
                request_id,
                current_lossless_source_v0_probe_min_bitrate=190,
                current_lossless_source_v0_probe_avg_bitrate=200,
                current_lossless_source_v0_probe_median_bitrate=200,
            )

        source_path = {
            "blank": "",
            "whitespace": "   ",
            "real": root,
        }[world.source_path_kind]
        current = make_album_quality_evidence(
            mb_release_id=mbid,
            source_path=source_path,
            files=snapshot_audio_files(root),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=world.min_bitrate,
                avg_bitrate_kbps=world.avg_bitrate,
                median_bitrate_kbps=world.avg_bitrate,
                format="MP3",
                is_cbr=False,
                spectral_grade=world.spectral_grade,
                spectral_bitrate_kbps=(
                    96 if world.spectral_grade == "likely_transcode" else None
                ),
                was_converted_from=world.was_converted_from,
            ),
            v0_metric=None,
        )
        db.upsert_album_quality_evidence(current)
        persisted = db.find_album_quality_evidence(
            mb_release_id=mbid,
            snapshot_fingerprint=current.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_request_current_evidence(request_id, persisted.id)

        result = ensure_current_evidence_for_action(
            db,
            request_id=request_id,
            mb_release_id=mbid,
            current_album_path=root,
            album_info=AlbumInfo(
                album_id=1,
                track_count=1,
                min_bitrate_kbps=world.min_bitrate,
                avg_bitrate_kbps=world.avg_bitrate,
                median_bitrate_kbps=world.avg_bitrate,
                is_cbr=False,
                album_path=root,
                format="MP3",
            ),
        )
        return (
            result.provenance.current_status,
            result.available,
            result.evidence.source_path if result.evidence is not None else None,
        )
    finally:
        shutil.rmtree(root, ignore_errors=True)


# The exact French Quarter shape (download_log 37206): a 2026-05-16
# library-backfill row with min 186 / avg 194 and no spectral, whose blank
# source_path kept every HAVE enrichment guard refusing forever.
_FRENCH_QUARTER_WORLD = BlankPathWorld(
    source_path_kind="blank",
    spectral_grade=None,
    min_bitrate=186,
    avg_bitrate=194,
)

# The fail-closed seam: a blank-path row that is ALSO converted-from-
# lossless with no linked source V0 fact must fail closed, not rebuild.
_BLANK_LOSSLESS_NO_SCALAR_WORLD = BlankPathWorld(
    source_path_kind="blank",
    spectral_grade=None,
    min_bitrate=108,
    avg_bitrate=114,
    was_converted_from="flac",
    request_has_v0_scalar=False,
)


class TestGeneratedBlankSourcePath(unittest.TestCase):
    """Action-loader invariants over generated source_path worlds."""

    @given(world=blank_path_worlds())
    @example(world=_FRENCH_QUARTER_WORLD)
    @example(world=_BLANK_LOSSLESS_NO_SCALAR_WORLD)
    def test_blank_source_path_is_never_authoritative(self, world):
        current_status, available, result_source_path = (
            _run_blank_path_world(world)
        )
        assert_blank_path_outcome(
            source_path_kind=world.source_path_kind,
            requires_lossless_v0=world.was_converted_from is not None,
            current_status=current_status,
            available=available,
            result_source_path=result_source_path,
        )


class TestBlankPathCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests for the blank-path invariant checker."""

    def test_trips_on_loaded_blank_path(self):
        with self.assertRaises(AssertionError):
            assert_blank_path_outcome(
                source_path_kind="blank", requires_lossless_v0=False,
                current_status="loaded",
                available=True, result_source_path="")

    def test_trips_on_fail_closed_instead_of_rebuild(self):
        with self.assertRaises(AssertionError):
            assert_blank_path_outcome(
                source_path_kind="blank", requires_lossless_v0=False,
                current_status="failed",
                available=False, result_source_path=None)

    def test_trips_on_rebuilt_row_still_blank(self):
        with self.assertRaises(AssertionError):
            assert_blank_path_outcome(
                source_path_kind="whitespace", requires_lossless_v0=False,
                current_status="backfilled",
                available=True, result_source_path="   ")

    def test_trips_on_real_path_not_loading(self):
        with self.assertRaises(AssertionError):
            assert_blank_path_outcome(
                source_path_kind="real", requires_lossless_v0=False,
                current_status="backfilled",
                available=True, result_source_path="/library/album")

    def test_trips_on_lossless_no_scalar_becoming_available(self):
        with self.assertRaises(AssertionError):
            assert_blank_path_outcome(
                source_path_kind="blank", requires_lossless_v0=True,
                current_status="backfilled",
                available=True, result_source_path="/library/album")


LosslessSpectralFailureKind = Literal[
    "absent",
    "not_attempted",
    "error",
    "grade_none",
    "grade_error",
]


def assert_lossless_spectral_failure_lifecycle(
    *,
    request_status: str,
    expected_request_status: str,
    job_status: str,
    preview_status: str | None,
    harness_calls: int,
) -> None:
    """Unusable lossless spectral evidence always fails before the harness."""

    if request_status != expected_request_status:
        raise AssertionError(
            "lossless spectral failure changed the force-import request from "
            f"{expected_request_status!r} to {request_status!r}"
        )
    if job_status != "failed" or preview_status != "measurement_failed":
        raise AssertionError(
            "lossless spectral failure did not terminate the preview job"
        )
    if harness_calls:
        raise AssertionError("harness ran without usable lossless spectral evidence")


def _lossless_spectral_detail(
    kind: LosslessSpectralFailureKind,
) -> SpectralAnalysisDetail | None:
    if kind == "absent":
        return None
    if kind == "not_attempted":
        return SpectralAnalysisDetail(attempted=False)
    if kind == "error":
        return SpectralAnalysisDetail(
            attempted=True,
            error="RuntimeError: generated analyzer failure",
        )
    if kind == "grade_none":
        return SpectralAnalysisDetail(attempted=True, grade=None)
    return SpectralAnalysisDetail(attempted=True, grade="error")


def _run_lossless_spectral_failure_world(
    kind: LosslessSpectralFailureKind,
) -> tuple[str, str, str | None, int]:
    from lib.config import CratediggerConfig
    from lib.import_queue import (
        IMPORT_JOB_FORCE,
        force_import_dedupe_key,
        force_import_payload,
    )
    from scripts import import_preview_worker

    root = tempfile.mkdtemp(prefix="cratedigger-lossless-spectral-gen-")
    try:
        source = os.path.join(root, "album")
        os.makedirs(source)
        with open(os.path.join(source, "01.flac"), "wb") as handle:
            handle.write(b"generated-lossless-audio")

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=71,
            status="downloading",
            mb_release_id="generated-lossless-mbid",
        ))
        log_id = db.log_download(71, outcome="rejected")
        db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=71,
            dedupe_key=force_import_dedupe_key(log_id),
            payload=force_import_payload(
                download_log_id=log_id,
                failed_path=source,
                source_username="generated-peer",
            ),
        )
        claimed = db.claim_next_import_preview_job(worker_id="preview")
        assert claimed is not None

        harness_calls = 0

        def run_import(**_kwargs: Any):
            nonlocal harness_calls
            harness_calls += 1
            raise AssertionError("harness must not run")

        detail = _lossless_spectral_detail(kind)

        def analyzer(_path: str) -> SpectralAnalysisDetail:
            return cast(SpectralAnalysisDetail, detail)

        def preview(db_arg: Any, _job: Any):
            return measure_and_persist_candidate_evidence(
                db_arg,
                request_id=71,
                path=source,
                force=True,
                download_log_id=log_id,
                import_job_id=claimed.id,
                run_import_fn=run_import,
                spectral_detail_analyzer=analyzer,
                existing_spectral_resolver=(
                    lambda _release_id: ExistingSpectralAuditLookup()
                ),
            )

        ini = configparser.ConfigParser()
        ini["Beets Validation"] = {
            "harness_path": "/fake/harness/run_beets_harness.sh",
            "audio_check": "off",
        }
        ini["Pipeline DB"] = {"enabled": "true"}
        cfg = CratediggerConfig.from_ini(ini)
        fake_beets = FakeBeetsDB()
        with patch(
            "lib.config.read_runtime_config",
            return_value=cfg,
        ), patch(
            "lib.beets_db.BeetsDB",
            lambda **_kwargs: fake_beets,
        ):
            updated = import_preview_worker.process_claimed_preview_job(
                cast(Any, db),
                claimed,
                preview_fn=preview,
            )
        assert updated is not None
        return (
            str(db.request(71)["status"]),
            updated.status,
            updated.preview_status,
            harness_calls,
        )
    finally:
        shutil.rmtree(root, ignore_errors=True)


class TestGeneratedLosslessSpectralFailureLifecycle(unittest.TestCase):
    @given(kind=st.sampled_from((
        "absent",
        "not_attempted",
        "error",
        "grade_none",
        "grade_error",
    )))
    @example(kind="absent")
    def test_unusable_lossless_spectral_never_reaches_harness(self, kind):
        request_status, job_status, preview_status, harness_calls = (
            _run_lossless_spectral_failure_world(kind)
        )
        assert_lossless_spectral_failure_lifecycle(
            request_status=request_status,
            expected_request_status="downloading",
            job_status=job_status,
            preview_status=preview_status,
            harness_calls=harness_calls,
        )


class TestLosslessSpectralFailureCheckerTripsOnViolations(unittest.TestCase):
    def test_trips_when_failed_preview_changes_force_request_status(self):
        with self.assertRaises(AssertionError):
            assert_lossless_spectral_failure_lifecycle(
                request_status="imported",
                expected_request_status="downloading",
                job_status="failed",
                preview_status="measurement_failed",
                harness_calls=0,
            )

    def test_trips_when_harness_runs(self):
        with self.assertRaises(AssertionError):
            assert_lossless_spectral_failure_lifecycle(
                request_status="wanted",
                expected_request_status="wanted",
                job_status="failed",
                preview_status="measurement_failed",
                harness_calls=1,
            )

class TestLifecycleCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests for the lifecycle invariant checker."""

    def test_trips_on_loaded_without_v0(self):
        with self.assertRaises(AssertionError):
            assert_lifecycle_outcome(
                current_status="loaded", available=True, result_v0_avg=171)

    def test_trips_when_scalar_fact_is_resurrected(self):
        with self.assertRaises(AssertionError):
            assert_lifecycle_outcome(
                current_status="rebuilt", available=False, result_v0_avg=171)

    def test_trips_on_not_failing_closed(self):
        with self.assertRaises(AssertionError):
            assert_lifecycle_outcome(
                current_status="rebuilt", available=True, result_v0_avg=None)


def assert_fingerprint_flip_two_axis_carry(
    *,
    original_subject: str,
    evidence: AlbumQualityEvidence,
) -> None:
    """Only source facts survive a content fingerprint change.

    A changed fingerprint means the installed files are a new subject. Source
    facts remain meaningful but become carried; installed facts must be
    measured again from the new files rather than copied from the old row.
    """
    measurement = evidence.measurement
    if original_subject == "source":
        if measurement.spectral_grade is None:
            raise AssertionError("source spectral fact was dropped")
        if (
            measurement.spectral_subject,
            measurement.spectral_provenance,
        ) != ("source", "carried"):
            raise AssertionError("source spectral fact was not marked carried")
        if evidence.v0_metric is None:
            raise AssertionError("source V0 fact was dropped")
        if (
            evidence.v0_metric.subject,
            evidence.v0_metric.provenance,
        ) != ("source", "carried"):
            raise AssertionError("source V0 fact was not marked carried")
    else:
        if measurement.spectral_grade is not None:
            raise AssertionError("installed spectral fact crossed fingerprints")
        if evidence.v0_metric is not None:
            raise AssertionError("installed V0 fact crossed fingerprints")

    if (
        measurement.spectral_subject == "installed"
        and measurement.spectral_provenance == "carried"
    ):
        raise AssertionError("installed spectral fact cannot be carried")
    if (
        evidence.v0_metric is not None
        and evidence.v0_metric.subject == "installed"
        and evidence.v0_metric.provenance == "carried"
    ):
        raise AssertionError("installed V0 fact cannot be carried")
    if evidence.verified_lossless_proof is None:
        raise AssertionError("verified-lossless proof was dropped")
    if evidence.verified_lossless_proof.provenance != "carried":
        raise AssertionError("verified-lossless proof was not marked carried")


def _run_fingerprint_flip_world(
    subject: Literal["source", "installed"],
) -> AlbumQualityEvidence:
    root = tempfile.mkdtemp(prefix="cratedigger-two-axis-gen-")
    try:
        audio_path = os.path.join(root, "01 - Track.mp3")
        with open(audio_path, "wb") as handle:
            handle.write(b"original-audio")

        request_id = 1
        mbid = "two-axis-generated-mbid"
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=request_id, mb_release_id=mbid))
        current = make_album_quality_evidence(
            mb_release_id=mbid,
            source_path=root,
            files=snapshot_audio_files(root),
            lineage_version=3,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=128,
                avg_bitrate_kbps=130,
                median_bitrate_kbps=129,
                format="MP3",
                spectral_grade="genuine",
                spectral_subject=subject,
                spectral_provenance="measured",
            ),
            v0_metric=AlbumQualityV0Metric(
                subject=subject,
                provenance="measured",
                avg_bitrate_kbps=245,
            ),
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="measured",
                source="flac",
                classifier="spectral_verified_lossless",
            ),
        )
        db.upsert_album_quality_evidence(current)
        persisted = db.find_album_quality_evidence(
            mb_release_id=mbid,
            snapshot_fingerprint=current.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_request_current_evidence(request_id, persisted.id)

        with open(audio_path, "ab") as handle:
            handle.write(b"-changed")
        result = backfill_current_evidence_from_album_info(
            db,
            request_id=request_id,
            mb_release_id=mbid,
            album_info=AlbumInfo(
                album_id=1,
                track_count=1,
                min_bitrate_kbps=190,
                avg_bitrate_kbps=196,
                median_bitrate_kbps=195,
                is_cbr=False,
                album_path=root,
                format="MP3",
            ),
        )
        assert result.evidence is not None
        return result.evidence
    finally:
        shutil.rmtree(root, ignore_errors=True)


class TestGeneratedTwoAxisFingerprintCarry(unittest.TestCase):
    @given(subject=st.sampled_from(("source", "installed")))
    @example(subject="source")
    @example(subject="installed")
    def test_fingerprint_flip_carries_only_source_facts(self, subject):
        evidence = _run_fingerprint_flip_world(subject)
        assert_fingerprint_flip_two_axis_carry(
            original_subject=subject,
            evidence=evidence,
        )


class TestTwoAxisCarryCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests prove the generated checker has teeth."""

    def test_trips_when_installed_facts_cross_fingerprints(self):
        known_bad = make_album_quality_evidence(
            lineage_version=3,
            measurement=AudioQualityMeasurement(
                spectral_grade="genuine",
                spectral_subject="installed",
                spectral_provenance="carried",
            ),
            v0_metric=AlbumQualityV0Metric(
                subject="installed",
                provenance="carried",
                avg_bitrate_kbps=245,
            ),
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="carried",
                source="flac",
                classifier="spectral_verified_lossless",
            ),
        )
        with self.assertRaises(AssertionError):
            assert_fingerprint_flip_two_axis_carry(
                original_subject="installed",
                evidence=known_bad,
            )

    def test_trips_when_source_fact_is_not_marked_carried(self):
        known_bad = make_album_quality_evidence(
            lineage_version=3,
            measurement=AudioQualityMeasurement(
                spectral_grade="genuine",
                spectral_subject="source",
                spectral_provenance="measured",
            ),
            v0_metric=AlbumQualityV0Metric(
                subject="source",
                provenance="measured",
                avg_bitrate_kbps=245,
            ),
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="measured",
                source="flac",
                classifier="spectral_verified_lossless",
            ),
        )
        with self.assertRaises(AssertionError):
            assert_fingerprint_flip_two_axis_carry(
                original_subject="source",
                evidence=known_bad,
            )


# ---------------------------------------------------------------------------
# 2026-07-18 proof-mint incident (Passenger / request 8877) — two invariants:
#
# 1. Proof minting is total over its input space: it never raises, mints a
#    proof exactly when the attempt is verified lossless, and the minted
#    source is a normalised non-empty token.
# 2. A crashed harness result (decision="crash") NEVER becomes candidate
#    evidence, however complete its partial measurements look — the live
#    crash fired one line after source_measurement was set, so the partial
#    result persisted proof-less and the proof lock silently never engaged.
# ---------------------------------------------------------------------------


def assert_minted_proof_consistent(
    will_be: bool,
    was_converted_from: Any,
    detected_source_format: Any,
    spectral_grade: Any,
    proof: Any,
) -> None:
    """Checker: mint output obeys the proof-construction contract."""
    if not will_be:
        assert proof is None, "unverified attempt must not mint a proof"
        return
    assert proof is not None, "verified attempt must mint a proof"
    assert proof.provenance == "measured", proof.provenance
    assert proof.classifier == "spectral_verified_lossless", proof.classifier
    assert proof.source, "minted proof source must be non-empty"
    assert proof.source == proof.source.strip().lower(), proof.source
    assert proof.detail == spectral_grade


def assert_crashed_result_never_persists(
    decision: Any,
    build_result: Any,
) -> None:
    """Checker: decision='crash' never yields buildable evidence."""
    if decision == "crash":
        assert build_result.evidence is None, (
            "a crashed ImportResult must never become candidate evidence"
        )
        assert build_result.status == "crashed_result", build_result.status


_filetype_token = st.one_of(
    st.none(),
    st.sampled_from(["flac", "FLAC", "alac", "wav", "m4a", "UNKNOWN", "  "]),
    st.text(max_size=8),
)

_grade_token = st.sampled_from(
    [None, "genuine", "marginal", "suspect", "likely_transcode", "error"]
)


class TestGeneratedProofMint(unittest.TestCase):
    @given(
        will_be=st.booleans(),
        was_converted_from=_filetype_token,
        detected=_filetype_token,
        grade=_grade_token,
    )
    @example(  # the live Passenger world that crashed on args.filetype
        will_be=True, was_converted_from="flac", detected="FLAC",
        grade="genuine",
    )
    def test_mint_is_total_and_consistent(
        self, will_be, was_converted_from, detected, grade,
    ):
        from lib.quality import mint_verified_lossless_proof

        proof = mint_verified_lossless_proof(
            will_be,
            was_converted_from=was_converted_from,
            detected_source_format=detected,
            spectral_grade=grade,
        )
        assert_minted_proof_consistent(
            will_be, was_converted_from, detected, grade, proof)


class TestGeneratedCrashedResultPersistGate(unittest.TestCase):
    @given(
        decision=st.sampled_from(
            ["crash", "import", "reject", "conversion_failed", None]
        ),
        with_measurement=st.booleans(),
        error=st.one_of(st.none(), st.text(max_size=40)),
    )
    @example(  # the live 2026-07-18 shape
        decision="crash", with_measurement=True,
        error="AttributeError: 'Namespace' object has no attribute 'filetype'",
    )
    def test_crashed_results_never_build_evidence(
        self, decision, with_measurement, error,
    ):
        from lib.quality import AlbumQualityEvidenceFile as EvidenceFile
        from lib.quality import ImportResult
        from lib.quality_evidence import evidence_from_import_result

        measurement = (
            AudioQualityMeasurement(
                min_bitrate_kbps=767, avg_bitrate_kbps=851,
                median_bitrate_kbps=847, format="FLAC",
                spectral_grade="genuine", spectral_subject="source",
                spectral_provenance="measured",
            )
            if with_measurement else None
        )
        result = evidence_from_import_result(
            mb_release_id="mbid-crash-gate",
            source_path="/nonexistent/crash-gate",
            import_result=ImportResult(
                decision=decision,
                error=error,
                source_measurement=measurement,
            ),
            files=[
                EvidenceFile(
                    relative_path="01.mp3", size_bytes=47, mtime_ns=1,
                    extension="mp3", container="mp3", codec="mp3",
                )
            ],
        )
        assert_crashed_result_never_persists(decision, result)


@dataclass(frozen=True)
class EvidenceBuildResultForTest:
    """Planted stand-in for the known-bad checker self-test only."""

    evidence: Any
    status: str


class TestProofMintCheckersTripOnViolations(unittest.TestCase):
    def test_trips_on_missing_proof_for_verified_attempt(self):
        with self.assertRaises(AssertionError):
            assert_minted_proof_consistent(
                True, "flac", "FLAC", "genuine", None)

    def test_trips_on_phantom_proof_for_unverified_attempt(self):
        planted = VerifiedLosslessProof(
            provenance="measured", source="flac",
            classifier="spectral_verified_lossless",
        )
        with self.assertRaises(AssertionError):
            assert_minted_proof_consistent(
                False, "flac", "FLAC", "genuine", planted)

    def test_trips_on_unnormalised_source(self):
        planted = VerifiedLosslessProof(
            provenance="measured", source="FLAC ",
            classifier="spectral_verified_lossless", detail="genuine",
        )
        with self.assertRaises(AssertionError):
            assert_minted_proof_consistent(
                True, "FLAC ", None, "genuine", planted)

    def test_trips_when_crashed_result_builds_evidence(self):
        planted = EvidenceBuildResultForTest(
            evidence=object(), status="ready")
        with self.assertRaises(AssertionError):
            assert_crashed_result_never_persists("crash", planted)


if __name__ == "__main__":
    unittest.main()
