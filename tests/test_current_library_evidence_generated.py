"""Generated properties for the current-library (HAVE) evidence lane.

Patrols the world space around `tests/test_import_queue.py`'s deterministic
front-gate pins by driving the REAL `process_claimed_preview_job` reuse path
over generated HAVE worlds (issue #1313). Three invariants:

- **V1 ordering** — if a freshly measured HAVE spectral fact is persisted at
  all, it is persisted BEFORE the job is marked importable. An audit-only
  scan left the importer's decision spectrally blind (download_log 37206);
  so does persisting after the job becomes importable.
- **V2 authority** — a HAVE authority the loader could not resolve
  (anything but `ready`-with-a-row or `empty_current`) never reaches
  `mark_import_job_preview_importable`.
- **V3 absence** — an authoritative absence has no row to write onto, so
  nothing is persisted.

- **V4 R19** — a preserved lossless-source row is never overwritten with a
  scan of its own installed derivative. Such a row wears its SOURCE's
  spectral; rescanning the lossy copy can rewrite a transcode-like source as
  apparently genuine.

**V4 is fail-closed legislation, not a satisfied guard — say so plainly.**
No world this lane can build violates it, and that was established by
measurement rather than by reading. R19 identity is pure lineage and says
nothing about the carried grade, so the strategy now varies that grade on
the preserved-source arm; before, it was hardcoded usable, which made every
preserved-source world *reusable* and left the other branch unproducible and
therefore unmeasurable. Both branches were then run with BOTH R19 guards
removed (`persist_measured_have_spectral`'s and
`persist_exact_current_spectral_from_attempt`'s), and the property stayed
GREEN:

- **Reusable** (a policy-usable carried grade): the worker passes
  `existing_detail`, so `collect_release_attempt_spectral_audit` returns an
  empty `ExistingSpectralAuditLookup` — no installed path, nothing to
  persist against.
- **Not reusable** (a non-policy carried grade): the audit's own preserve
  branch (`lib/measurement.py`) hands back the CARRIED detail instead of
  scanning the derivative, and the persist helper then declines on that
  detail's unusable grade.

The second branch holds only because the producible non-policy grades are
exactly the closed set the evidence row admits. Widen `_POLICY_USABLE_
SPECTRAL_GRADES`, or change what decides reuse, and V4 becomes reachable —
which is the point of keeping it. Removing ONLY the caller-side guard is
also GREEN; the callee's guard, and the audit above it, are the real floor.
Deterministic siblings:
`TestPersistMeasuredHaveSpectral.test_declines_for_a_preserved_lossless_source_row`
and the callee's own R19 tests.
"""

from __future__ import annotations

import dataclasses
import os
import tempfile
import unittest
from unittest.mock import patch

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.beets_db import AlbumInfo
from lib.current_library_evidence import preserve_existing_source_spectral
from lib.measurement import ExistingSpectralAuditLookup
from lib.quality import (
    EVIDENCE_SUBJECT_SOURCE,
    AudioQualityMeasurement,
    SpectralAnalysisDetail,
)
from lib.quality_evidence import EvidenceBuildResult, snapshot_audio_files
from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION
from tests.dispatch_helpers import claim_next_import_preview_job
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import make_request_row
from tests.test_import_queue import (
    _force_download_log,
    _force_preview_source,
    _seed_candidate_for_download_log,
    _seed_current_for_request,
)

PERSIST_HAVE = "persist_have_spectral"
MARK_IMPORTABLE = "mark_importable"

# Loader statuses this lane can see. Only the first two are resolvable;
# the rest are authority failures the lane must refuse to proceed on.
RESOLVABLE_STATUSES = ("ready", "empty_current")
UNRESOLVABLE_STATUSES = ("stale", "failed", "missing", "incomplete")


@dataclasses.dataclass(frozen=True)
class HaveWorld:
    """One generated world for the front-gate HAVE reuse lane."""

    loader_status: str
    have_spectral_grade: str | None
    preserve_source: bool
    have_lookup_resolves: bool
    fresh_scan_grade: str

    @property
    def resolvable(self) -> bool:
        return self.loader_status in RESOLVABLE_STATUSES

    @property
    def has_row(self) -> bool:
        return self.loader_status == "ready"


have_worlds = st.builds(
    HaveWorld,
    loader_status=st.sampled_from(RESOLVABLE_STATUSES + UNRESOLVABLE_STATUSES),
    have_spectral_grade=st.sampled_from([None, "genuine", "suspect", "error"]),
    preserve_source=st.booleans(),
    have_lookup_resolves=st.booleans(),
    fresh_scan_grade=st.sampled_from(["genuine", "suspect", "likely_transcode"]),
)


class _OrderRecordingDB(FakePipelineDB):
    """Records the two writes whose ORDER V1 is about."""

    def __init__(self) -> None:
        super().__init__()
        self.write_order: list[str] = []

    def persist_current_spectral_measurement(self, **kwargs):
        self.write_order.append(PERSIST_HAVE)
        return super().persist_current_spectral_measurement(**kwargs)

    def mark_import_job_preview_importable(self, *args, **kwargs):
        self.write_order.append(MARK_IMPORTABLE)
        return super().mark_import_job_preview_importable(*args, **kwargs)


def reuse_lane_violations(world: HaveWorld, order: list[str]) -> list[str]:
    """Every clause evaluates, so ordering cannot mask one."""
    violations: list[str] = []
    persisted = PERSIST_HAVE in order
    marked = MARK_IMPORTABLE in order

    if persisted and marked and order.index(PERSIST_HAVE) > order.index(
        MARK_IMPORTABLE
    ):
        violations.append(
            f"V1: HAVE spectral persisted after the job was marked "
            f"importable: {order}"
        )
    if persisted and not marked:
        violations.append(
            f"V1: HAVE spectral persisted for a job never marked "
            f"importable: {order}"
        )
    if not world.resolvable and marked:
        violations.append(
            f"V2: unresolved HAVE authority {world.loader_status!r} still "
            f"marked the job importable: {order}"
        )
    if world.loader_status == "empty_current" and persisted:
        violations.append(
            f"V3: an authoritative absence persisted a HAVE fact: {order}"
        )
    if world.preserve_source and world.has_row and persisted:
        violations.append(
            f"V4: a preserved lossless-source row was overwritten with an "
            f"installed-derivative scan (R19): {order}"
        )
    return violations


def _drive_world(world: HaveWorld) -> list[str]:
    """Run the real preview worker over one generated HAVE world."""
    from scripts import import_preview_worker

    # R19's world is a recorded lossless conversion into a known lossy
    # installed codec; its trigger is the same shape
    # `test_wav_conversion_preserves_source_spectral` pins, and the
    # assertion below proves this world really produces it.
    installed_name = "01.opus" if world.preserve_source else "01.mp3"
    with _force_preview_source() as (source, cfg), \
            tempfile.TemporaryDirectory() as existing:
        with open(os.path.join(source, "01.mp3"), "wb") as handle:
            handle.write(b"audio")
        with open(os.path.join(existing, installed_name), "wb") as handle:
            handle.write(b"audio")
        db = _OrderRecordingDB()
        db.seed_request(make_request_row(id=42, mb_release_id="mbid-42"))
        if world.preserve_source:
            # R19 identity is pure lineage — a recorded lossless source
            # converted into one known lossy installed codec — and does not
            # depend on the CARRIED GRADE. Varying that grade is what makes
            # the NOT-reusable preserved-source arm producible at all: with
            # a hardcoded usable grade every such world was reusable, so V4
            # below could only ever be exercised on one of its two branches.
            # A row carrying spectral markers must carry a grade (the
            # evidence row rejects markers without one), so the strategy's
            # `None` becomes the non-policy grade "error" here — still
            # unusable, which is the property that decides reuse.
            carried_grade = world.have_spectral_grade or "error"
            measurement = AudioQualityMeasurement(
                min_bitrate_kbps=128,
                avg_bitrate_kbps=128,
                median_bitrate_kbps=128,
                format="Opus",
                spectral_grade=carried_grade,
                spectral_bitrate_kbps=160,
                spectral_subject=EVIDENCE_SUBJECT_SOURCE,
                spectral_provenance="carried",
                spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
                was_converted_from="wav",
            )
            codec = container = "opus"
            storage_format = "Opus"
        else:
            measurement = AudioQualityMeasurement(
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                format="MP3",
                is_cbr=True,
                spectral_grade=world.have_spectral_grade,
                spectral_bitrate_kbps=(
                    160 if world.have_spectral_grade else None
                ),
                spectral_measurement_version=(
                    SPECTRAL_MEASUREMENT_VERSION
                    if world.have_spectral_grade
                    else None
                ),
            )
            codec = container = "mp3"
            storage_format = "MP3"
        current = _seed_current_for_request(
            db,
            42,
            mb_release_id="mbid-42",
            source_path=existing,
            files=snapshot_audio_files(existing),
            measurement=measurement,
            codec=codec,
            container=container,
            storage_format=storage_format,
        )
        # Rule C: the R19 world must be the one production recognises, not a
        # literal the strategy invented.
        assert preserve_existing_source_spectral(current) is world.preserve_source
        fake_beets = FakeBeetsDB()
        fake_beets.set_album_info("mbid-42", AlbumInfo(
            album_id=1,
            track_count=1,
            min_bitrate_kbps=320,
            avg_bitrate_kbps=320,
            median_bitrate_kbps=320,
            is_cbr=True,
            album_path=existing,
            format="MP3",
        ))
        download_log_id = _force_download_log(db, 42, source)
        db.enqueue_import_job(
            "force_import",
            request_id=42,
            dedupe_key=f"force:{download_log_id}",
            payload={
                "download_log_id": download_log_id,
                "failed_path": source,
                "source_username": "alice",
            },
        )
        claimed = claim_next_import_preview_job(db, worker_id="generated")
        assert claimed is not None
        _seed_candidate_for_download_log(
            db, download_log_id,
            mb_release_id="mbid-frontgate-generated",
            files=snapshot_audio_files(source),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=245,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=252,
                format="MP3",
                spectral_grade="genuine",
            ),
            codec="mp3",
            container="mp3",
            storage_format="MP3",
        )

        def loader(_db, **_kwargs):
            if world.loader_status == "ready":
                return EvidenceBuildResult(
                    current, "ready", current_album_path=existing,
                )
            if world.loader_status == "empty_current":
                return EvidenceBuildResult(
                    None, "empty_current", "exact album not in beets",
                )
            return EvidenceBuildResult(
                None, world.loader_status, "generated authority failure",
            )

        def analyze(path: str) -> SpectralAnalysisDetail:
            return SpectralAnalysisDetail(
                attempted=True,
                grade=(
                    world.fresh_scan_grade if path == existing else "genuine"
                ),
                bitrate_kbps=128 if path == existing else None,
                spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
            )

        lookup = (
            ExistingSpectralAuditLookup(path=existing)
            if world.have_lookup_resolves
            else ExistingSpectralAuditLookup()
        )
        with patch(
            "lib.beets_db.BeetsDB", lambda *_args, **_kwargs: fake_beets,
        ):
            import_preview_worker.process_claimed_preview_job(
                db,
                claimed,
                spectral_detail_analyzer=analyze,
                existing_spectral_resolver=lambda _mbid: lookup,
                current_evidence_loader=loader,
                runtime_config=cfg,
            )
        return list(db.write_order)


class TestCurrentLibraryEvidenceGenerated(unittest.TestCase):
    """The reuse lane's HAVE contract over generated worlds."""

    @given(world=have_worlds)
    def test_generated_reuse_lane_keeps_its_have_contract(self, world):
        order = _drive_world(world)
        violations = reuse_lane_violations(world, order)
        self.assertEqual(violations, [], f"world={world} order={order}")

    def test_resolvable_worlds_still_reach_the_importable_mark(self):
        """Must-still-work: the guard does not fail the happy path closed."""
        order = _drive_world(HaveWorld(
            loader_status="ready",
            have_spectral_grade=None,
            preserve_source=False,
            have_lookup_resolves=True,
            fresh_scan_grade="suspect",
        ))
        self.assertEqual(order, [PERSIST_HAVE, MARK_IMPORTABLE])


class TestReuseLaneCheckerTripsOnViolations(unittest.TestCase):
    """One known-bad self-test per clause of `reuse_lane_violations`."""

    RESOLVED = HaveWorld(
        loader_status="ready",
        have_spectral_grade=None,
        preserve_source=False,
        have_lookup_resolves=True,
        fresh_scan_grade="suspect",
    )

    def test_v1_trips_when_persistence_follows_the_importable_mark(self):
        violations = reuse_lane_violations(
            self.RESOLVED, [MARK_IMPORTABLE, PERSIST_HAVE],
        )
        self.assertTrue(
            any(v.startswith("V1: HAVE spectral persisted after") for v in violations),
            violations,
        )

    def test_v1_trips_when_persistence_happens_without_the_mark(self):
        violations = reuse_lane_violations(self.RESOLVED, [PERSIST_HAVE])
        self.assertTrue(
            any(
                v.startswith("V1: HAVE spectral persisted for a job never")
                for v in violations
            ),
            violations,
        )

    def test_v2_trips_when_an_unresolved_authority_marks_importable(self):
        world = dataclasses.replace(self.RESOLVED, loader_status="stale")
        violations = reuse_lane_violations(world, [MARK_IMPORTABLE])
        self.assertTrue(
            any(v.startswith("V2:") for v in violations), violations,
        )

    def test_v3_trips_when_an_absence_persists_a_have_fact(self):
        world = dataclasses.replace(self.RESOLVED, loader_status="empty_current")
        violations = reuse_lane_violations(
            world, [PERSIST_HAVE, MARK_IMPORTABLE],
        )
        self.assertTrue(
            any(v.startswith("V3:") for v in violations), violations,
        )

    def test_v4_trips_when_a_preserved_source_row_is_overwritten(self):
        """Clause 4: earlier clauses all pass; only R19 is violated."""
        world = dataclasses.replace(self.RESOLVED, preserve_source=True)
        violations = reuse_lane_violations(
            world, [PERSIST_HAVE, MARK_IMPORTABLE],
        )
        self.assertTrue(
            any(v.startswith("V4:") for v in violations), violations,
        )

    def test_a_clean_world_trips_nothing(self):
        self.assertEqual(
            reuse_lane_violations(self.RESOLVED, [PERSIST_HAVE, MARK_IMPORTABLE]),
            [],
        )


if __name__ == "__main__":
    unittest.main()
