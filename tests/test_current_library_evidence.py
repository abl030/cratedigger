"""Current-library (HAVE) evidence: outcomes, resolution, and persistence.

Covers `lib/current_library_evidence.py` — the one module that answers "what
does the library already have for this request?" (issue #1313). The generated
siblings live in `tests/test_current_library_evidence_generated.py`.
"""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
import unittest
from contextlib import nullcontext
from unittest.mock import patch

from lib.current_library_evidence import (
    CurrentLibraryAuthorityUnavailable,
    CurrentLibraryEvidence,
    CurrentLibraryEvidenceDB,
    HaveEnrichment,
    HavePreparation,
    enrich_incomplete_current_evidence_for_request,
    persist_measured_have_spectral,
    prepare_current_evidence_for_failure,
    preserve_existing_source_spectral,
    resolve_current_library_evidence,
)
from lib.quality import (
    CURRENT_EVIDENCE_LINEAGE_VERSION,
    EVIDENCE_SUBJECT_SOURCE,
    AudioQualityMeasurement,
    QualityRankConfig,
    SpectralAnalysisDetail,
    V0ProbeEvidence,
)
from lib.quality_evidence import EvidenceBuildResult, snapshot_audio_files
from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION
from tests.evidence_helpers import make_album_quality_evidence
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


class TestHaveOutcomeBudgetPolicy(unittest.TestCase):
    """Each outcome owns whether it spends a per-cycle enrichment unit."""

    PREPARATION_CASES = (
        ("a resolved row is free", HavePreparation.READY, False),
        (
            "an authoritative absence is free",
            HavePreparation.NO_CURRENT_EVIDENCE,
            False,
        ),
        ("a failure spends a unit", HavePreparation.FAILED, True),
    )

    ENRICHMENT_CASES = (
        ("nothing was missing", HaveEnrichment.COMPLETE, False),
        ("the files moved under the capture", HaveEnrichment.STALE, False),
        (
            "Beets authoritatively has nothing",
            HaveEnrichment.NO_CURRENT_EVIDENCE,
            False,
        ),
        ("measurement ran and resolved", HaveEnrichment.ENRICHED, True),
        ("measurement ran and did not resolve", HaveEnrichment.PARTIAL, True),
    )

    def test_preparation_budget_policy(self):
        for desc, outcome, charges in self.PREPARATION_CASES:
            with self.subTest(desc=desc):
                self.assertEqual(outcome.charges_budget, charges)

    def test_enrichment_budget_policy(self):
        for desc, outcome, charges in self.ENRICHMENT_CASES:
            with self.subTest(desc=desc):
                self.assertEqual(outcome.charges_budget, charges)

    def test_every_member_has_a_budget_policy(self):
        """No member may be added without deciding what it costs."""
        for member in (*HavePreparation, *HaveEnrichment):
            with self.subTest(member=member):
                self.assertIsInstance(member.charges_budget, bool)
        self.assertEqual(
            {m for m, _ in ((m, None) for m in HavePreparation)},
            {c[1] for c in self.PREPARATION_CASES},
        )
        self.assertEqual(
            {m for m in HaveEnrichment},
            {c[1] for c in self.ENRICHMENT_CASES},
        )


def _resolved(
    *,
    evidence=None,
    reuse: bool = False,
    preserve: bool = False,
) -> CurrentLibraryEvidence:
    return CurrentLibraryEvidence(
        evidence=evidence,
        existing_spectral_evidence=SpectralAnalysisDetail(attempted=False),
        reuse_have_evidence=reuse,
        preserve_have_source=preserve,
    )


def _link(db: FakePipelineDB, evidence):
    """Persist ``evidence`` and link it as the request's current row."""
    db.upsert_album_quality_evidence(evidence)
    stored = db.find_album_quality_evidence(
        mb_release_id=evidence.mb_release_id,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
    )
    assert stored is not None and stored.id is not None
    db.set_request_current_evidence(42, stored.id)
    return stored


class TestResolveCurrentLibraryEvidence(unittest.TestCase):
    """The one HAVE resolution both preview lanes and the worker run."""

    def _db(self) -> FakePipelineDB:
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        return db

    def _source_dir(self) -> str:
        source = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, source, ignore_errors=True)
        with open(os.path.join(source, "01.mp3"), "wb") as handle:
            handle.write(b"not real audio but never inspected in this test")
        return source

    def _resolve(self, db, loader):
        return resolve_current_library_evidence(
            db,
            request_id=42,
            mb_release_id="mb-release",
            quality_ranks=QualityRankConfig.defaults(),
            beets_library_root="/library",
            loader=loader,
        )

    def test_empty_current_is_an_absence_not_a_failure(self):
        db = self._db()

        resolved = self._resolve(
            db,
            lambda *_a, **_k: EvidenceBuildResult(
                None, "empty_current", "exact album not in beets",
            ),
        )

        self.assertIsInstance(resolved, CurrentLibraryEvidence)
        assert isinstance(resolved, CurrentLibraryEvidence)
        self.assertIsNone(resolved.evidence)
        self.assertFalse(resolved.existing_spectral_evidence.attempted)
        self.assertFalse(resolved.reuse_have_evidence)
        self.assertFalse(resolved.preserve_have_source)

    def test_non_ready_status_is_an_authority_failure_naming_its_reason(self):
        db = self._db()

        resolved = self._resolve(
            db,
            lambda *_a, **_k: EvidenceBuildResult(None, "stale", "snapshot moved"),
        )

        self.assertIsInstance(resolved, CurrentLibraryAuthorityUnavailable)
        assert isinstance(resolved, CurrentLibraryAuthorityUnavailable)
        self.assertEqual(resolved.detail, "stale: snapshot moved")

    def test_ready_without_evidence_is_also_an_authority_failure(self):
        """A "ready" status carrying no row must not be treated as resolved."""
        db = self._db()

        resolved = self._resolve(
            db, lambda *_a, **_k: EvidenceBuildResult(None, "ready"),
        )

        self.assertIsInstance(resolved, CurrentLibraryAuthorityUnavailable)
        assert isinstance(resolved, CurrentLibraryAuthorityUnavailable)
        self.assertEqual(resolved.detail, "ready: current authority unavailable")

    def test_ready_row_projects_its_persisted_spectral_and_reuse_decision(self):
        db = self._db()
        source = self._source_dir()
        evidence = make_album_quality_evidence(
            mb_release_id="mb-release",
            source_path=source,
            files=snapshot_audio_files(source),
            measurement=AudioQualityMeasurement(
                spectral_grade="genuine",
                spectral_bitrate_kbps=192,
                cliff_hz=19500,
                codec_family="mp3",
                ultrasonic_deficit_db=-7.25,
                spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
            ),
            lineage_version=CURRENT_EVIDENCE_LINEAGE_VERSION,
        )

        resolved = self._resolve(
            db, lambda *_a, **_k: EvidenceBuildResult(evidence, "ready"),
        )

        assert isinstance(resolved, CurrentLibraryEvidence)
        self.assertIs(resolved.evidence, evidence)
        # EVERY projected field, not just the two obvious ones: this detail
        # is what the decision reads as HAVE, so a field silently dropped
        # here is a spectrally blinder decision, not a cosmetic loss.
        projected = resolved.existing_spectral_evidence
        self.assertEqual(projected.grade, "genuine")
        self.assertEqual(projected.bitrate_kbps, 192)
        self.assertEqual(projected.cliff_hz, 19500)
        self.assertEqual(projected.codec_family, "mp3")
        self.assertEqual(projected.ultrasonic_deficit_db, -7.25)
        self.assertEqual(
            projected.spectral_measurement_version,
            SPECTRAL_MEASUREMENT_VERSION,
        )
        self.assertTrue(resolved.reuse_have_evidence)
        self.assertFalse(resolved.preserve_have_source)

    def test_preserved_lossless_source_row_sets_the_preserve_flag(self):
        """R19 lineage must reach the bundle the lanes branch on."""
        db = self._db()
        source = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, source, ignore_errors=True)
        with open(os.path.join(source, "01.opus"), "wb") as handle:
            handle.write(b"installed derivative")
        evidence = make_album_quality_evidence(
            mb_release_id="mb-release",
            source_path=source,
            files=snapshot_audio_files(source),
            measurement=AudioQualityMeasurement(
                format="Opus",
                spectral_grade="genuine",
                spectral_bitrate_kbps=160,
                spectral_subject=EVIDENCE_SUBJECT_SOURCE,
                spectral_provenance="carried",
                spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
                was_converted_from="wav",
            ),
            codec="opus",
            container="opus",
            storage_format="Opus",
        )
        assert preserve_existing_source_spectral(evidence)

        resolved = self._resolve(
            db, lambda *_a, **_k: EvidenceBuildResult(evidence, "ready"),
        )

        assert isinstance(resolved, CurrentLibraryEvidence)
        self.assertTrue(resolved.preserve_have_source)

    def test_loader_receives_the_caller_s_beets_authority(self):
        """The resolver forwards authority, it does not re-derive it."""
        db = self._db()
        ranks = QualityRankConfig.defaults()
        seen: list[dict[str, object]] = []

        def loader(_db, **kwargs):
            seen.append(dict(kwargs))
            return EvidenceBuildResult(None, "empty_current")

        resolve_current_library_evidence(
            db,
            request_id=42,
            mb_release_id="mb-release",
            quality_ranks=ranks,
            beets_library_root="/library",
            loader=loader,
        )

        self.assertEqual(len(seen), 1)
        self.assertIs(seen[0]["quality_ranks"], ranks)
        self.assertEqual(seen[0]["beets_library_root"], "/library")
        self.assertEqual(seen[0]["request_id"], 42)
        self.assertEqual(seen[0]["mb_release_id"], "mb-release")

    def test_loader_receives_the_linked_row_as_its_preloaded_evidence(self):
        db = self._db()
        source = self._source_dir()
        evidence = make_album_quality_evidence(
            mb_release_id="mb-release",
            source_path=source,
            files=snapshot_audio_files(source),
        )
        stored = _link(db, evidence)
        seen: list[object] = []

        def loader(_db, **kwargs):
            seen.append(kwargs.get("preloaded_evidence"))
            return EvidenceBuildResult(None, "empty_current")

        self._resolve(db, loader)

        self.assertEqual(len(seen), 1)
        preloaded = seen[0]
        self.assertIsNotNone(preloaded)
        assert preloaded is not None
        self.assertEqual(getattr(preloaded, "id"), stored.id)  # noqa: B009

    def test_an_unresolvable_link_hands_the_loader_no_preloaded_row(self):
        """Every way the link can fail reaches the loader as one absence.

        No link, a link naming a row that is gone, and either read raising
        are four different worlds, and the loader must not be able to tell
        them apart: it resolves Beets freshly and backfills from the files
        that are actually installed. Handing it a half-built row instead
        would let an unreadable link decide the HAVE facts (issue #1313).
        """
        boom = RuntimeError("evidence unavailable")
        for label in (
            "no link at all",
            "link names a row that is gone",
            "the id read raises",
            "the row read raises",
        ):
            with self.subTest(world=label):
                db = self._db()
                if label == "link names a row that is gone":
                    db.set_request_current_evidence(42, 999)
                seen: list[object] = []

                def loader(_db, seen=seen, **kwargs):
                    seen.append(kwargs.get("preloaded_evidence"))
                    return EvidenceBuildResult(None, "empty_current")

                if label == "the id read raises":
                    failing = patch.object(
                        db, "get_request_current_evidence_id", side_effect=boom,
                    )
                elif label == "the row read raises":
                    db.set_request_current_evidence(42, 999)
                    failing = patch.object(
                        db, "load_album_quality_evidence_by_id", side_effect=boom,
                    )
                else:
                    failing = nullcontext()

                with failing:
                    resolved = self._resolve(db, loader)

                self.assertEqual(seen, [None])
                assert isinstance(resolved, CurrentLibraryEvidence)
                self.assertIsNone(resolved.evidence)
                self.assertFalse(resolved.existing_spectral_evidence.attempted)

    def test_every_broken_link_says_which_link_broke(self):
        """The three worlds whose only trace is a log line.

        A vanished row, an unreadable id and an unreadable row all return
        None exactly like a request with no link at all, so nothing about
        the resolved bundle tells an operator that a HAVE link is broken.
        The warning is the entire signal in each case, and it is useless
        without the ids it names. These are the only assertions stopping any
        of the three branches from inverting or losing its arguments
        (issue #1313 mutmut pass, and its review round, which pointed out
        that the two raising branches earn the pin for the same reason the
        vanished-row branch does).
        """
        boom = RuntimeError("evidence unavailable")
        cases = (
            (
                "the row vanished",
                nullcontext(),
                "Current spectral evidence 999 is missing for request 42",
            ),
            (
                "the id read raised",
                lambda db: patch.object(
                    db, "get_request_current_evidence_id", side_effect=boom),
                "Unable to resolve current spectral evidence for request 42",
            ),
            (
                "the row read raised",
                lambda db: patch.object(
                    db, "load_album_quality_evidence_by_id", side_effect=boom),
                "Unable to load current spectral evidence 999 for request 42",
            ),
        )
        for label, failure, expected in cases:
            with self.subTest(world=label):
                db = self._db()
                db.set_request_current_evidence(42, 999)
                failing = failure(db) if callable(failure) else failure

                with self.assertLogs(
                    "cratedigger", level="WARNING",
                ) as captured, failing:
                    resolved = self._resolve(
                        db,
                        lambda *_a, **_k: EvidenceBuildResult(
                            None, "empty_current"),
                    )

                assert isinstance(resolved, CurrentLibraryEvidence)
                self.assertIsNone(resolved.evidence)
                self.assertEqual(len(captured.records), 1)
                self.assertEqual(captured.records[0].getMessage(), expected)

    def test_a_resolvable_link_warns_about_nothing(self):
        """Must-still-work: the healthy world must not accuse itself."""
        db = self._db()
        source = self._source_dir()
        evidence = make_album_quality_evidence(
            mb_release_id="mb-release",
            source_path=source,
            files=snapshot_audio_files(source),
        )
        _link(db, evidence)
        logger = logging.getLogger("cratedigger")

        with patch.object(logger, "warning") as warned:
            self._resolve(
                db, lambda *_a, **_k: EvidenceBuildResult(evidence, "ready"),
            )

        warned.assert_not_called()


class _PersistRecordingDB(FakePipelineDB):
    """Records the one write these declining guards must never reach."""

    def __init__(self) -> None:
        super().__init__()
        self.spectral_persist_calls: list[int] = []

    def persist_current_spectral_measurement(self, **kwargs) -> bool:
        self.spectral_persist_calls.append(int(kwargs["expected_evidence_id"]))
        return super().persist_current_spectral_measurement(**kwargs)


class TestPersistMeasuredHaveSpectral(unittest.TestCase):
    """The guard on making a fresh HAVE scan durable."""

    def _db(self) -> _PersistRecordingDB:
        db = _PersistRecordingDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        return db

    def _fresh_scan(self) -> SpectralAnalysisDetail:
        return SpectralAnalysisDetail(
            attempted=True,
            grade="suspect",
            bitrate_kbps=128,
            spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
        )

    def _installed_dir(self, name: str) -> str:
        source = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, source, ignore_errors=True)
        with open(os.path.join(source, name), "wb") as handle:
            handle.write(b"installed bytes")
        return source

    def _linked_installed_row(self, db: FakePipelineDB, source: str):
        """An ordinary linked installed row: every callee guard would pass."""
        return _link(db, make_album_quality_evidence(
            source_path=source,
            files=snapshot_audio_files(source),
            measurement=AudioQualityMeasurement(),
        ))

    def _linked_preserved_source_row(self, db: FakePipelineDB, source: str):
        """A row R19 protects — the predicate itself proves it (Rule C)."""
        stored = _link(db, make_album_quality_evidence(
            source_path=source,
            files=snapshot_audio_files(source),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=128,
                avg_bitrate_kbps=128,
                median_bitrate_kbps=128,
                format="Opus",
                spectral_grade="genuine",
                spectral_subject=EVIDENCE_SUBJECT_SOURCE,
                spectral_provenance="carried",
                spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
                was_converted_from="wav",
            ),
            codec="opus",
            container="opus",
            storage_format="Opus",
        ))
        assert preserve_existing_source_spectral(stored)
        return stored

    def test_declines_when_no_have_row_is_linked(self):
        """No row to write onto — and the DB is never touched."""
        db = self._db()
        result = persist_measured_have_spectral(
            db,
            request_id=42,
            resolved=_resolved(evidence=None),
            measured_existing=self._fresh_scan(),
            measured_existing_path=self._installed_dir("01.mp3"),
        )
        self.assertIsNone(result)
        self.assertEqual(db.spectral_persist_calls, [])

    def test_declines_for_a_preserved_lossless_source_row(self):
        """R19: an installed-derivative scan is never that row's grade.

        Every other guard on both sides passes for this world — the row is
        linked, its fingerprint matches the scanned path, and the fresh scan
        is usable — so the only thing that can decline is R19 itself.
        """
        db = self._db()
        source = self._installed_dir("01.opus")
        linked = self._linked_preserved_source_row(db, source)

        result = persist_measured_have_spectral(
            db,
            request_id=42,
            resolved=_resolved(evidence=linked, preserve=True),
            measured_existing=self._fresh_scan(),
            measured_existing_path=source,
        )

        self.assertIsNone(result)
        self.assertEqual(db.spectral_persist_calls, [])
        stored = db.load_album_quality_evidence_by_id(linked.id)
        assert stored is not None
        self.assertEqual(stored.measurement.spectral_grade, "genuine")

    def test_declines_when_the_scan_resolved_no_installed_path(self):
        """A linked, otherwise-persistable row still declines with no path."""
        db = self._db()
        source = self._installed_dir("01.mp3")
        linked = self._linked_installed_row(db, source)

        result = persist_measured_have_spectral(
            db,
            request_id=42,
            resolved=_resolved(evidence=linked),
            measured_existing=self._fresh_scan(),
            measured_existing_path=None,
        )

        self.assertIsNone(result)
        self.assertEqual(db.spectral_persist_calls, [])
        stored = db.load_album_quality_evidence_by_id(linked.id)
        assert stored is not None
        self.assertIsNone(stored.measurement.spectral_grade)

    def test_persists_a_fresh_scan_onto_the_exact_snapshot(self):
        db = self._db()
        source = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, source, ignore_errors=True)
        with open(os.path.join(source, "01.mp3"), "wb") as handle:
            handle.write(b"installed bytes")
        evidence = make_album_quality_evidence(
            source_path=source,
            files=snapshot_audio_files(source),
            measurement=AudioQualityMeasurement(),
        )
        linked = _link(db, evidence)

        result = persist_measured_have_spectral(
            db,
            request_id=42,
            resolved=_resolved(evidence=linked),
            measured_existing=self._fresh_scan(),
            measured_existing_path=source,
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.status, "ready")
        stored = db.load_album_quality_evidence_by_id(linked.id)
        assert stored is not None
        self.assertEqual(stored.measurement.spectral_grade, "suspect")
        self.assertEqual(stored.measurement.spectral_bitrate_kbps, 128)


class TestEnrichIncompleteCurrentEvidence(unittest.TestCase):
    """Failure-point HAVE enrichment fills only what's missing, once."""

    def _db(self) -> FakePipelineDB:
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        return db

    def _source_dir(self) -> str:
        source = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, source, ignore_errors=True)
        with open(os.path.join(source, "01.mp3"), "wb") as handle:
            handle.write(b"not real audio but never inspected in this test")
        return source

    def _seed_current(
        self,
        db: FakePipelineDB,
        source: str,
        *,
        spectral_present: bool,
        v0_attempted: bool = False,
    ):
        evidence = make_album_quality_evidence(
            mb_release_id="mbid-42",
            source_path=source,
            files=snapshot_audio_files(source),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                format="MP3",
                spectral_grade="genuine" if spectral_present else None,
                spectral_bitrate_kbps=96 if spectral_present else None,
                spectral_measurement_version=(
                    SPECTRAL_MEASUREMENT_VERSION
                    if spectral_present
                    else None
                ),
            ),
            v0_metric=None,
            on_disk_v0_research_attempted=v0_attempted,
        )
        db.upsert_album_quality_evidence(evidence)
        stored = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        db.set_request_current_evidence(42, stored.id)
        return stored

    def _spectral_recorder(self, detail: SpectralAnalysisDetail):
        calls: list[str] = []

        def analyzer(path: str) -> SpectralAnalysisDetail:
            calls.append(path)
            return detail

        return analyzer, calls

    def _probe_recorder(self):
        calls: list[str] = []

        def probe(path: str) -> V0ProbeEvidence:
            calls.append(path)
            return V0ProbeEvidence(
                kind="on_disk_research_v0",
                min_bitrate_kbps=201,
                avg_bitrate_kbps=259,
                median_bitrate_kbps=255,
            )

        return probe, calls

    def _good_scan(self) -> SpectralAnalysisDetail:
        return SpectralAnalysisDetail(
            attempted=True, grade="genuine", bitrate_kbps=96,
            spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
        )

    def _enrich(self, db, analyzer, probe):
        def load_current(db_arg, **_kwargs):
            evidence_id = db_arg.get_request_current_evidence_id(42)
            evidence = db_arg.load_album_quality_evidence_by_id(evidence_id)
            if evidence is None:
                return EvidenceBuildResult(
                    None,
                    "empty_current",
                    "exact album not in beets",
                )
            return EvidenceBuildResult(
                evidence,
                "ready",
                current_album_path=evidence.source_path,
            )

        return enrich_incomplete_current_evidence_for_request(
            db,
            request_id=42,
            mb_release_id="mbid-42",
            quality_ranks=QualityRankConfig.defaults(),
            beets_library_root="",
            spectral_analyzer=analyzer,
            probe_fn=probe,
            load_fn=load_current,
        )

    def test_complete_row_skips_all_measurement(self):
        db = self._db()
        source = self._source_dir()
        self._seed_current(db, source, spectral_present=True, v0_attempted=True)
        analyzer, spectral_calls = self._spectral_recorder(self._good_scan())
        probe, probe_calls = self._probe_recorder()

        outcome = self._enrich(db, analyzer, probe)

        self.assertEqual(outcome, HaveEnrichment.COMPLETE)
        self.assertEqual(spectral_calls, [])
        self.assertEqual(probe_calls, [])

    def test_preparation_preserves_an_existing_complete_current_row(self):
        db = self._db()
        source = self._source_dir()
        before = self._seed_current(
            db,
            source,
            spectral_present=True,
            v0_attempted=True,
        )

        calls: list[object] = []

        def load_current(*_args, **_kwargs):
            calls.append(_kwargs.get("preloaded_evidence"))
            return EvidenceBuildResult(before, "ready")

        outcome = prepare_current_evidence_for_failure(
            db,
            request_id=42,
            mb_release_id="mbid-42",
            quality_ranks=QualityRankConfig.defaults(),
            beets_library_root=source,
            load_fn=load_current,
        )

        current_id = db.get_request_current_evidence_id(42)
        self.assertEqual(outcome, HavePreparation.READY)
        self.assertEqual(calls, [before])
        self.assertEqual(current_id, before.id)
        self.assertEqual(
            db.load_album_quality_evidence_by_id(current_id),
            before,
        )

    def test_failure_refreshes_complete_v1_through_current_beets(self):
        from lib.beets_db import AlbumInfo
        from tests.fakes import FakeBeetsDB

        db = self._db()
        source = self._source_dir()
        evidence = make_album_quality_evidence(
            mb_release_id="mbid-42",
            source_path=source,
            files=snapshot_audio_files(source),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=256,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=256,
                format="AAC",
                is_cbr=True,
                spectral_grade="genuine",
            ),
            lineage_version=1,
            on_disk_v0_research_attempted=True,
        )
        db.upsert_album_quality_evidence(evidence)
        before = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert before is not None and before.id is not None
        db.set_request_current_evidence(42, before.id)
        fake_beets = FakeBeetsDB()
        fake_beets.set_album_info("mbid-42", AlbumInfo(
            album_id=1,
            track_count=1,
            min_bitrate_kbps=256,
            avg_bitrate_kbps=256,
            median_bitrate_kbps=256,
            is_cbr=True,
            album_path=source,
            format="AAC",
        ))

        db.update_status(42, "downloading", expected_status="wanted")
        with patch("lib.beets_db.BeetsDB", lambda **_kwargs: fake_beets):
            prepared = prepare_current_evidence_for_failure(
                db,
                request_id=42,
                mb_release_id="mbid-42",
                quality_ranks=QualityRankConfig.defaults(),
                beets_library_root=source,
            )

        self.assertEqual(prepared, HavePreparation.READY)
        current_id = db.get_request_current_evidence_id(42)
        self.assertEqual(current_id, before.id)
        current = db.load_album_quality_evidence_by_id(current_id)
        assert current is not None
        self.assertEqual(current.lineage_version, CURRENT_EVIDENCE_LINEAGE_VERSION)
        self.assertEqual(db.request(42)["status"], "downloading")

        db.update_status(42, "wanted", expected_status="downloading")
        with patch("lib.beets_db.BeetsDB", lambda **_kwargs: fake_beets):
            enriched = enrich_incomplete_current_evidence_for_request(
                db,
                request_id=42,
                mb_release_id="mbid-42",
                quality_ranks=QualityRankConfig.defaults(),
                beets_library_root=source,
                spectral_analyzer=lambda _path: self._good_scan(),
                probe_fn=lambda _path: None,
            )

        self.assertEqual(enriched, HaveEnrichment.ENRICHED)
        self.assertEqual(db.request(42)["status"], "wanted")
        current = db.load_album_quality_evidence_by_id(current_id)
        assert current is not None
        self.assertEqual(current.lineage_version, CURRENT_EVIDENCE_LINEAGE_VERSION)
        self.assertEqual(current.measurement.format, "AAC")
        self.assertEqual(current.measurement.avg_bitrate_kbps, 256)

    def test_fills_both_missing_pieces(self):
        db = self._db()
        source = self._source_dir()
        stored = self._seed_current(db, source, spectral_present=False)
        assert stored.id is not None
        analyzer, spectral_calls = self._spectral_recorder(self._good_scan())
        probe, probe_calls = self._probe_recorder()

        outcome = self._enrich(db, analyzer, probe)

        self.assertEqual(outcome, HaveEnrichment.ENRICHED)
        self.assertEqual(spectral_calls, [source])
        self.assertEqual(probe_calls, [source])
        persisted = db.load_album_quality_evidence_by_id(stored.id)
        assert persisted is not None
        self.assertEqual(persisted.measurement.spectral_grade, "genuine")
        self.assertEqual(persisted.measurement.spectral_bitrate_kbps, 96)
        self.assertTrue(persisted.on_disk_v0_research_attempted)
        assert persisted.v0_metric is not None
        self.assertEqual(persisted.v0_metric.avg_bitrate_kbps, 259)

    def test_fills_v0_only_when_spectral_present(self):
        db = self._db()
        source = self._source_dir()
        self._seed_current(db, source, spectral_present=True)
        analyzer, spectral_calls = self._spectral_recorder(self._good_scan())
        probe, probe_calls = self._probe_recorder()

        outcome = self._enrich(db, analyzer, probe)

        self.assertEqual(outcome, HaveEnrichment.ENRICHED)
        self.assertEqual(spectral_calls, [])
        self.assertEqual(probe_calls, [source])

    def test_fills_spectral_only_when_v0_already_attempted(self):
        db = self._db()
        source = self._source_dir()
        self._seed_current(
            db, source, spectral_present=False, v0_attempted=True,
        )
        analyzer, spectral_calls = self._spectral_recorder(self._good_scan())
        probe, probe_calls = self._probe_recorder()

        outcome = self._enrich(db, analyzer, probe)

        self.assertEqual(outcome, HaveEnrichment.ENRICHED)
        self.assertEqual(spectral_calls, [source])
        self.assertEqual(probe_calls, [])

    def test_stale_snapshot_measures_nothing(self):
        db = self._db()
        source = self._source_dir()
        self._seed_current(db, source, spectral_present=False)
        with open(os.path.join(source, "01.mp3"), "ab") as handle:
            handle.write(b"changed after snapshot")
        analyzer, spectral_calls = self._spectral_recorder(self._good_scan())
        probe, probe_calls = self._probe_recorder()

        outcome = self._enrich(db, analyzer, probe)

        self.assertEqual(outcome, HaveEnrichment.STALE)
        self.assertEqual(spectral_calls, [])
        self.assertEqual(probe_calls, [])

    def test_without_current_evidence_returns_no_current_evidence(self):
        db = self._db()
        analyzer, spectral_calls = self._spectral_recorder(self._good_scan())
        probe, probe_calls = self._probe_recorder()

        outcome = self._enrich(db, analyzer, probe)

        self.assertEqual(outcome, HaveEnrichment.NO_CURRENT_EVIDENCE)
        self.assertEqual(spectral_calls, [])
        self.assertEqual(probe_calls, [])

    def test_failed_backfill_is_not_classified_as_absent_library_copy(self):
        db = self._db()

        outcome = prepare_current_evidence_for_failure(
            db,
            request_id=42,
            mb_release_id="mbid-42",
            quality_ranks=QualityRankConfig.defaults(),
            beets_library_root="",
            load_fn=lambda *_args, **_kwargs: EvidenceBuildResult(
                None,
                "failed",
                "beets library unreadable",
            ),
        )

        self.assertEqual(outcome, HavePreparation.FAILED)

    def test_backfill_exception_is_not_classified_as_absent_library_copy(self):
        db = self._db()

        def broken_loader(*_args, **_kwargs):
            raise RuntimeError("beets adapter crashed")

        outcome = prepare_current_evidence_for_failure(
            db,
            request_id=42,
            mb_release_id="mbid-42",
            quality_ranks=QualityRankConfig.defaults(),
            beets_library_root="",
            load_fn=broken_loader,
        )

        self.assertEqual(outcome, HavePreparation.FAILED)

    def test_failed_download_backfills_unlinked_seabear_have(self):
        """We Built a Fire: an installed album cannot stay HAVE-less."""
        from lib.beets_db import AlbumInfo
        from tests.fakes import FakeBeetsDB

        db = self._db()
        source = self._source_dir()
        fake_beets = FakeBeetsDB()
        fake_beets.set_album_info("mbid-42", AlbumInfo(
            album_id=1,
            track_count=17,
            min_bitrate_kbps=183,
            avg_bitrate_kbps=190,
            median_bitrate_kbps=191,
            is_cbr=False,
            album_path=source,
            format="MP3",
        ))
        analyzer, spectral_calls = self._spectral_recorder(self._good_scan())
        probe, probe_calls = self._probe_recorder()

        with patch("lib.beets_db.BeetsDB", lambda **_kwargs: fake_beets):
            prepared = prepare_current_evidence_for_failure(
                db,
                request_id=42,
                mb_release_id="mbid-42",
                quality_ranks=QualityRankConfig.defaults(),
                beets_library_root=source,
            )
            outcome = enrich_incomplete_current_evidence_for_request(
                db,
                request_id=42,
                mb_release_id="mbid-42",
                quality_ranks=QualityRankConfig.defaults(),
                beets_library_root=source,
                spectral_analyzer=analyzer,
                probe_fn=probe,
            )

        self.assertEqual(prepared, HavePreparation.READY)
        self.assertEqual(outcome, HaveEnrichment.ENRICHED)
        self.assertEqual(spectral_calls, [source])
        self.assertEqual(probe_calls, [source])
        evidence_id = db.get_request_current_evidence_id(42)
        self.assertIsNotNone(evidence_id)
        persisted = db.load_album_quality_evidence_by_id(evidence_id)
        assert persisted is not None
        self.assertEqual(persisted.measurement.format, "MP3")
        self.assertEqual(persisted.measurement.avg_bitrate_kbps, 190)
        self.assertEqual(persisted.measurement.spectral_grade, "genuine")
        assert persisted.v0_metric is not None
        self.assertEqual(persisted.v0_metric.avg_bitrate_kbps, 259)

    def test_failed_spectral_scan_reports_partial(self):
        db = self._db()
        source = self._source_dir()
        stored = self._seed_current(
            db, source, spectral_present=False, v0_attempted=True,
        )
        assert stored.id is not None
        analyzer, spectral_calls = self._spectral_recorder(
            SpectralAnalysisDetail(attempted=True, error="sox exploded"),
        )
        probe, probe_calls = self._probe_recorder()

        outcome = self._enrich(db, analyzer, probe)

        self.assertEqual(outcome, HaveEnrichment.PARTIAL)
        self.assertEqual(spectral_calls, [source])
        self.assertEqual(probe_calls, [])
        persisted = db.load_album_quality_evidence_by_id(stored.id)
        assert persisted is not None
        self.assertIsNone(persisted.measurement.spectral_grade)
        self.assertIsNone(persisted.measurement.spectral_bitrate_kbps)


class TestCurrentLibraryEvidenceDBProtocolParity(unittest.TestCase):
    """#409 shape for this module's own port (#1313).

    ``ImportPreviewDB`` extends ``CurrentLibraryEvidenceDB``, so the preview
    module's parity tests cover this one only transitively — and nothing
    pinned that inheritance. These three assertions make the HAVE port's own
    contract explicit, so narrowing either side fails here rather than
    silently through a sibling.
    """

    def test_pipeline_db_satisfies_protocol(self) -> None:
        from lib.pipeline_db import PipelineDB

        self.assertTrue(issubclass(PipelineDB, CurrentLibraryEvidenceDB))

    def test_fake_pipeline_db_satisfies_protocol(self) -> None:
        self.assertTrue(issubclass(FakePipelineDB, CurrentLibraryEvidenceDB))

    def test_have_protocol_extends_evidence_protocol(self) -> None:
        """The HAVE writers sit on top of the candidate-evidence reads."""
        from lib.quality_evidence import QualityEvidenceDB

        self.assertTrue(
            issubclass(CurrentLibraryEvidenceDB, QualityEvidenceDB),
        )

    def test_preview_protocol_extends_the_have_protocol(self) -> None:
        """The preview lanes forward their handle into the HAVE persisters."""
        from lib.import_preview import ImportPreviewDB

        self.assertTrue(
            issubclass(ImportPreviewDB, CurrentLibraryEvidenceDB),
        )
