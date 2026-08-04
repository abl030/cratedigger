"""Real-PostgreSQL pins for the decision-corpus exporter (#999)."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import unittest
from contextlib import redirect_stderr
from copy import deepcopy
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal

from lib.json_narrow import is_str_object_dict
from lib.quality import (
    CD_RIP_BIT_VERIFIED_CLASSIFIER,
    AccurateRipBitMatch,
    AlbumQualityEvidenceFile,
    AudioQualityMeasurement,
    CdRipBitVerification,
    CdTocIdentity,
)
from lib.quality_evidence import snapshot_audio_files
from scripts.decision_differential import (
    _EVIDENCE_SCHEMA_TYPES,
    _FILE_SCHEMA_TYPES,
    _SOURCE_SCHEMA_TYPES,
    DecisionCorpusExportResult,
    RenderDifferentialError,
    _assert_live_decision_corpus_schema,
    _DecisionCorpusSnapshot,
    _materialize_transition_snapshot,
    assert_decision_corpus_schema,
    assert_export_output_exact,
    export_decision_corpus,
    main,
    replay_decision_transitions,
    verify_decision_corpus_pair,
)
from tests.helpers import make_album_quality_evidence
from tests.test_pipeline_db import TEST_DSN, make_db, requires_postgres


@requires_postgres
class TestDecisionCorpusExport(unittest.TestCase):
    """The public exporter reads the migrated PG boundary, not a helper."""

    def setUp(self) -> None:
        self.db = make_db()

    def tearDown(self) -> None:
        self.db.close()

    def _request(self, release: str) -> int:
        return self.db.add_request(
            artist_name="Decision corpus",
            album_title=release,
            source="request",
            mb_release_id=release,
        )

    def _evidence(self, release: str, ordinal: int) -> int:
        self.db.upsert_album_quality_evidence(
            make_album_quality_evidence(
                mb_release_id=release,
                source_path=f"/tmp/decision-corpus-{ordinal}",
                files=[
                    AlbumQualityEvidenceFile(
                        relative_path=f"{ordinal:02d}.mp3",
                        size_bytes=ordinal,
                        mtime_ns=ordinal,
                        extension="mp3",
                        container="mp3",
                        codec="mp3",
                    )
                ],
            )
        )
        stored = self.db.find_album_quality_evidence(
            mb_release_id=release,
            snapshot_fingerprint=make_album_quality_evidence(
                mb_release_id=release,
                files=[
                    AlbumQualityEvidenceFile(
                        relative_path=f"{ordinal:02d}.mp3",
                        size_bytes=ordinal,
                        mtime_ns=ordinal,
                        extension="mp3",
                        container="mp3",
                        codec="mp3",
                    )
                ],
            ).snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        return stored.id

    def _cd_evidence(self, release: str, ordinal: int) -> int:
        cd_rip = CdRipBitVerification(
            source_format="flac",
            toc=CdTocIdentity(
                track_offsets_sectors=[0],
                leadout_sector=470,
                accuraterip_id="000001d6-000003ac-02000601",
                musicbrainz_disc_id="base-archive-cd-disc",
            ),
            accuraterip=AccurateRipBitMatch(
                provider="accuraterip",
                url="https://www.accuraterip.com/base-archive.bin",
                checksum_version="arv2",
                read_offset_samples=0,
                track_confidences=[42],
                track_checksums=[0x12345678],
                response_sha256="a" * 64,
            ),
        )
        files = [
            AlbumQualityEvidenceFile(
                relative_path=f"{ordinal:02d}.flac",
                size_bytes=ordinal,
                mtime_ns=ordinal,
                extension="flac",
                container="flac",
                codec="flac",
            )
        ]
        evidence = make_album_quality_evidence(
            mb_release_id=release,
            source_path=f"/tmp/decision-corpus-{ordinal}",
            files=files,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=800,
                avg_bitrate_kbps=850,
                median_bitrate_kbps=840,
                format="FLAC",
            ),
            codec="flac",
            container="flac",
            storage_format="FLAC",
            verified_lossless_proof=cd_rip.verified_lossless_proof(),
            cd_rip_verification=cd_rip,
        )
        self.db.upsert_album_quality_evidence(evidence)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=release,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        return stored.id

    def _export(
        self, *, batch_size: int = 1
    ) -> tuple[
        DecisionCorpusExportResult,
        list[dict[str, object]],
        dict[str, object],
    ]:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = export_decision_corpus(
                TEST_DSN,
                root / "corpus.jsonl",
                root / "coverage.json",
                batch_size=batch_size,
            )
            corpus: list[dict[str, object]] = [
                json.loads(line)
                for line in (root / "corpus.jsonl").read_text().splitlines()
            ]
            coverage: dict[str, object] = json.loads(
                (root / "coverage.json").read_text()
            )
        return result, corpus, coverage

    def test_exact_source_links_collapse_and_export_current_once(self) -> None:
        """Both source arms remain counted while their identical link is one row."""
        release = "decision-corpus-export"
        request_id = self._request(release)
        candidate_id = self._evidence(release, 1)
        current_id = self._evidence(release, 2)
        self.assertTrue(self.db.set_request_current_evidence(request_id, current_id))
        job = self.db.enqueue_import_job(
            "force_import",
            request_id=request_id,
            payload={"download_log_id": 1, "failed_path": "/tmp/candidate"},
        )
        self.assertTrue(self.db.set_import_job_candidate_evidence(job.id, candidate_id))
        log_id = self.db.log_download(request_id=request_id, outcome="rejected")
        self.db.set_download_log_candidate_evidence(log_id, candidate_id)

        result, corpus, coverage = self._export()

        self.assertTrue(result.green)
        self.assertEqual(
            coverage["source_link_counts"],
            {
                "download_log": 1,
                "import_jobs": 1,
            },
        )
        self.assertEqual(coverage["identical_associations_collapsed"], 1)
        self.assertEqual(
            coverage["valid_candidates"],
            {
                "paired": 1,
                "unpaired": 0,
            },
        )
        self.assertEqual(coverage["referenced_current_ids"], [current_id])
        self.assertEqual([row["id"] for row in corpus], [candidate_id, current_id])
        self.assertTrue(corpus[0]["is_candidate"])
        self.assertFalse(corpus[1]["is_candidate"])
        self.assertEqual(corpus[0]["current_evidence_id"], current_id)

    def test_all_three_foreign_keys_classify_every_evidence_role_in_id_order(
        self,
    ) -> None:
        """The O(rows + links) census names candidate/current/dual/audit rows."""
        candidate_release = "role-candidate"
        candidate_request = self._request(candidate_release)
        candidate_id = self._evidence(candidate_release, 41)
        candidate_job = self.db.enqueue_import_job(
            "force_import",
            request_id=candidate_request,
            payload={"download_log_id": 41, "failed_path": "/tmp/candidate"},
        )
        self.assertTrue(
            self.db.set_import_job_candidate_evidence(candidate_job.id, candidate_id)
        )

        current_release = "role-current"
        current_request = self._request(current_release)
        current_id = self._evidence(current_release, 42)
        self.assertTrue(
            self.db.set_request_current_evidence(current_request, current_id)
        )

        dual_release = "role-dual"
        dual_request = self._request(dual_release)
        dual_id = self._evidence(dual_release, 43)
        self.assertTrue(self.db.set_request_current_evidence(dual_request, dual_id))
        dual_log = self.db.log_download(request_id=dual_request, outcome="rejected")
        self.db.set_download_log_candidate_evidence(dual_log, dual_id)

        audit_id = self._evidence("role-audit", 44)

        result, corpus, coverage = self._export()

        self.assertTrue(result.green)
        self.assertEqual(
            coverage["evidence_role_counts"],
            {"candidate": 1, "current": 1, "dual": 1, "audit_only": 1},
        )
        roles = coverage["evidence_roles"]
        assert isinstance(roles, list)
        self.assertEqual(
            [(row["evidence_id"], row["role"]) for row in roles],
            [
                (candidate_id, "candidate"),
                (current_id, "current"),
                (dual_id, "dual"),
                (audit_id, "audit_only"),
            ],
        )
        self.assertEqual(
            [row["id"] for row in corpus],
            [candidate_id, current_id, dual_id, audit_id],
        )
        owner_links = coverage["evidence_owner_links"]
        assert isinstance(owner_links, list)
        self.assertEqual(
            [
                (row["owner_kind"], row["owner_id"])
                for row in owner_links
            ],
            sorted(
                (row["owner_kind"], row["owner_id"])
                for row in owner_links
            ),
        )

    def test_transition_replay_uses_disposable_pg_and_never_writes_source(
        self,
    ) -> None:
        """A verified live-shaped pair is observation, not write authority."""
        from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION

        live_sources = TemporaryDirectory()
        self.addCleanup(live_sources.cleanup)

        def source_evidence(
            release: str,
            ordinal: int,
            *,
            provenance: Literal["measured", "carried"],
            was_converted_from: str | None,
        ) -> int:
            files = [AlbumQualityEvidenceFile(
                relative_path=f"{ordinal:02d}.mp3",
                size_bytes=ordinal,
                mtime_ns=ordinal,
                extension="mp3",
                container="mp3",
                codec="mp3",
            )]
            source = Path(live_sources.name) / str(ordinal)
            source.mkdir()
            _materialize_transition_snapshot(source, files)
            evidence = make_album_quality_evidence(
                mb_release_id=release,
                source_path=str(source),
                files=files,
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=128,
                    avg_bitrate_kbps=130,
                    median_bitrate_kbps=129,
                    format="MP3",
                    spectral_grade="suspect",
                    spectral_bitrate_kbps=96,
                    spectral_subject="source",
                    spectral_provenance=provenance,
                    spectral_measurement_version=None,
                    was_converted_from=was_converted_from,
                ),
                preserve_spectral_measurement_version=True,
                codec="mp3",
                container="mp3",
                storage_format="MP3",
            )
            self.db.upsert_album_quality_evidence(evidence)
            stored = self.db.find_album_quality_evidence(
                mb_release_id=release,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert stored is not None and stored.id is not None
            return stored.id

        dual_release = "transition-preserved-current-source"
        dual_request_id = self._request(dual_release)
        dual_id = source_evidence(
            dual_release,
            45,
            provenance="carried",
            was_converted_from="flac",
        )
        self.assertTrue(
            self.db.set_request_current_evidence(dual_request_id, dual_id)
        )
        job = self.db.enqueue_import_job(
            "force_import",
            request_id=dual_request_id,
            payload={"download_log_id": 45, "failed_path": "/tmp/dual"},
        )
        self.assertTrue(self.db.set_import_job_candidate_evidence(job.id, dual_id))

        native_release = "transition-remeasurable-current-source"
        native_request_id = self._request(native_release)
        native_id = source_evidence(
            native_release,
            46,
            provenance="measured",
            was_converted_from=None,
        )
        self.assertTrue(
            self.db.set_request_current_evidence(native_request_id, native_id)
        )
        audit_id = self._evidence("transition-audit", 46)

        def source_counts() -> tuple[int, int, int]:
            row = self.db._execute("""
                SELECT
                    (SELECT COUNT(*) FROM album_quality_evidence) AS evidence,
                    (SELECT COUNT(*) FROM import_jobs) AS jobs,
                    (SELECT COUNT(*) FROM album_requests) AS requests
            """).fetchone()
            assert row is not None
            return int(row["evidence"]), int(row["jobs"]), int(row["requests"])

        before = source_counts()
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            corpus_path = root / "corpus.jsonl"
            coverage_path = root / "coverage.json"
            report_path = root / "transition.json"
            result = export_decision_corpus(
                TEST_DSN,
                corpus_path,
                coverage_path,
            )
            self.assertTrue(result.green)
            report = replay_decision_transitions(
                corpus_path,
                coverage_path,
                report_path,
            )

        self.assertEqual(source_counts(), before)
        self.assertEqual(report.total_matrix_classes, 3)
        self.assertEqual(
            {row.evidence_id for row in report.representatives},
            {audit_id, dual_id, native_id},
        )
        self.assertTrue(
            all(row.exact_import_job_fk for row in report.representatives)
        )
        self.assertTrue(report.green)
        self.assertEqual(report.transition_violations, 0)
        by_role = {
            row.observed_role: row for row in report.representatives
        }
        self.assertEqual(
            by_role["current"].canonical_spectral_generation,
            SPECTRAL_MEASUREMENT_VERSION,
        )
        self.assertIsNone(
            by_role["dual"].canonical_spectral_generation,
        )
        self.assertEqual(
            report.decided
            + report.snapshot_refused
            + report.admission_refused
            + report.producer_refused,
            report.total_matrix_classes,
        )

    def test_source_snapshot_state_distinguishes_present_and_missing_candidates(
        self,
    ) -> None:
        """v3 binds observational source state without making path authority."""
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            present = root / "present"
            present.mkdir()
            (present / "01.mp3").write_bytes(b"candidate")
            missing = root / "missing"

            ids: dict[str, int] = {}
            for label, source in (("present", present), ("missing", missing)):
                release = f"source-state-{label}"
                request_id = self._request(release)
                files = snapshot_audio_files(str(present))
                evidence = make_album_quality_evidence(
                    mb_release_id=release,
                    source_path=str(source),
                    files=files,
                )
                self.db.upsert_album_quality_evidence(evidence)
                stored = self.db.find_album_quality_evidence(
                    mb_release_id=release,
                    snapshot_fingerprint=evidence.snapshot_fingerprint,
                )
                assert stored is not None and stored.id is not None
                ids[label] = stored.id
                job = self.db.enqueue_import_job(
                    "force_import",
                    request_id=request_id,
                    payload={
                        "download_log_id": stored.id,
                        "failed_path": str(source),
                    },
                )
                self.assertTrue(
                    self.db.set_import_job_candidate_evidence(job.id, stored.id)
                )

            corpus = root / "corpus.jsonl"
            coverage_path = root / "coverage.json"
            report_path = root / "transition.json"
            export_decision_corpus(TEST_DSN, corpus, coverage_path)
            coverage = json.loads(coverage_path.read_text())
            observed = {
                row["evidence_id"]: row["source_snapshot_state"]
                for row in coverage["observed_evidence"]
            }
            roles = {
                row["evidence_id"]: row["matrix_class"]
                for row in coverage["evidence_roles"]
            }

            self.assertEqual(observed[ids["present"]], "present_exact")
            self.assertEqual(observed[ids["missing"]], "missing")
            self.assertIn("source=present_exact", roles[ids["present"]])
            self.assertIn("source=missing", roles[ids["missing"]])

            report = replay_decision_transitions(
                corpus,
                coverage_path,
                report_path,
            )

        by_id = {row.evidence_id: row for row in report.representatives}
        self.assertEqual(by_id[ids["present"]].expected_outcome, "decided")
        self.assertEqual(by_id[ids["present"]].outcome, "decided")
        self.assertEqual(
            by_id[ids["missing"]].expected_outcome,
            "snapshot_refused",
        )
        self.assertEqual(by_id[ids["missing"]].outcome, "snapshot_refused")
        self.assertTrue(report.green)

    def test_transition_runs_from_filtered_runtime_without_tests(self) -> None:
        """The deployed -I wrapper owns every transition runtime import."""
        evidence_id = self._evidence("filtered-runtime-transition", 77)
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            corpus = root / "corpus.jsonl"
            coverage = root / "coverage.json"
            report = root / "transition.json"
            export_decision_corpus(TEST_DSN, corpus, coverage)

            runtime = root / "runtime"
            runtime.mkdir()
            repository = Path(__file__).parents[1]
            for directory in ("lib", "web", "harness", "scripts", "migrations"):
                shutil.copytree(
                    repository / directory,
                    runtime / directory,
                    ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
                )
            self.assertFalse((runtime / "tests").exists())

            completed = subprocess.run(
                [
                    sys.executable,
                    "-I",
                    str(runtime / "scripts" / "decision_differential.py"),
                    "transition",
                    "--corpus",
                    str(corpus),
                    "--coverage",
                    str(coverage),
                    "--out",
                    str(report),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            replay = json.loads(report.read_text(encoding="utf-8"))
            self.assertTrue(replay["green"])
            self.assertEqual(
                [row["evidence_id"] for row in replay["representatives"]],
                [evidence_id],
            )

    def test_transition_sparse_snapshot_restores_exported_mtime(self) -> None:
        """Replay files retain the complete stored manifest, including mtime."""
        mtime_ns = 1_700_000_000_123_456_789
        file = AlbumQualityEvidenceFile(
            relative_path="disc/01.flac",
            size_bytes=4096,
            mtime_ns=mtime_ns,
            extension="flac",
            container="flac",
            codec="flac",
        )
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            _materialize_transition_snapshot(root, [file])
            materialized = root / "disc" / "01.flac"
            stat = materialized.stat()

        self.assertEqual(stat.st_size, file.size_bytes)
        self.assertEqual(stat.st_mtime_ns, file.mtime_ns)

    def test_real_migrated_schema_passes_the_production_checker(self) -> None:
        """Deterministic outer PG pin for every consumed column contract."""
        with self.db.conn.cursor() as cursor:
            _assert_live_decision_corpus_schema(cursor)

    def test_production_checkers_trip_on_schema_and_output_mutants(self) -> None:
        """Known-bad qualification for the production, not test-local, guards."""
        with self.assertRaises(RenderDifferentialError):
            assert_decision_corpus_schema(
                {"album_quality_evidence": {}, "album_quality_evidence_files": {}}
            )
        with self.assertRaises(RenderDifferentialError):
            assert_export_output_exact(
                [{"id": 1, "is_candidate": True}, {"id": 1, "is_candidate": False}],
                [(1, None, "release")],
                [],
            )

    def test_output_exactness_rejects_swapped_pairs_and_keeps_dual_roles(self) -> None:
        """A candidate can also be current; association metadata is exact."""
        associations = [(1, 2, "one"), (3, 1, "three")]
        rows = [
            {
                "id": candidate,
                "is_candidate": True,
                "current_evidence_id": current,
                "request_mb_release_id": release,
                "files": [],
            }
            for candidate, current, release in associations
        ] + [
            {
                "id": 2,
                "is_candidate": False,
                "current_evidence_id": None,
                "request_mb_release_id": None,
                "files": [],
            }
        ]
        assert_export_output_exact(rows, associations, [1, 2])
        swapped = deepcopy(rows)
        swapped[0]["current_evidence_id"] = 1
        swapped[1]["current_evidence_id"] = 2
        with self.assertRaises(RenderDifferentialError):
            assert_export_output_exact(swapped, associations, [1, 2])

    def test_every_consumed_schema_column_rejects_each_named_mutant(self) -> None:
        """Deterministic companion to the generated schema-drift patrol."""
        for table, contract in (
            ("album_quality_evidence", _EVIDENCE_SCHEMA_TYPES),
            ("album_quality_evidence_files", _FILE_SCHEMA_TYPES),
            *_SOURCE_SCHEMA_TYPES.items(),
        ):
            for column, (kind, nullable) in contract.items():
                for mutation in ("missing", "type", "nullable"):
                    descriptions = {
                        "album_quality_evidence": deepcopy(_EVIDENCE_SCHEMA_TYPES),
                        "album_quality_evidence_files": deepcopy(_FILE_SCHEMA_TYPES),
                        **deepcopy(_SOURCE_SCHEMA_TYPES),
                    }
                    if mutation == "missing":
                        del descriptions[table][column]
                    elif mutation == "type":
                        descriptions[table][column] = (
                            "text" if kind != "text" else "integer",
                            nullable,
                        )
                    else:
                        descriptions[table][column] = (kind, not nullable)
                    with (
                        self.subTest(table=table, column=column, mutation=mutation),
                        self.assertRaises(RenderDifferentialError),
                    ):
                        assert_decision_corpus_schema(descriptions)

    def test_authorityless_link_is_debt_with_independently_published_outputs(
        self,
    ) -> None:
        """A historical source link never disappears just because it lacks a request."""
        candidate_id = self._evidence("authorityless", 3)
        job = self.db.enqueue_import_job(
            "force_import",
            request_id=None,
            payload={"download_log_id": 1, "failed_path": "/tmp/candidate"},
        )
        self.assertTrue(self.db.set_import_job_candidate_evidence(job.id, candidate_id))
        self.db._execute(
            "UPDATE album_quality_evidence SET snapshot_fingerprint = %s WHERE id = %s",
            ("f" * 64, candidate_id),
        )
        self.db.conn.commit()

        result, corpus, coverage = self._export()

        self.assertFalse(result.green)
        self.assertEqual(
            coverage["authorityless_source_links"],
            [
                {
                    "evidence_id": candidate_id,
                    "request_id": None,
                    "source": "import_jobs",
                    "source_id": job.id,
                    "reason": "null_request_id",
                }
            ],
        )
        self.assertEqual(coverage["authorityless_candidate_ids"], [candidate_id])
        mismatches = coverage["content_address_mismatches"]
        assert isinstance(mismatches, list)
        assert is_str_object_dict(mismatches[0])
        self.assertEqual(mismatches[0]["evidence_id"], candidate_id)
        self.assertEqual([row["id"] for row in corpus], [candidate_id])
        self.assertFalse(corpus[0]["is_candidate"])
        outputs = coverage["outputs"]
        assert is_str_object_dict(outputs)
        corpus_output = outputs["corpus"]
        assert is_str_object_dict(corpus_output)
        self.assertTrue(corpus_output["exact_match"])
        self.assertIsInstance(corpus_output["sha256"], str)

    def test_authorityless_request_current_is_observed_but_not_replayed(self) -> None:
        """A missing request release authority cannot hide its HAVE evidence."""
        request_id = self._request("authorityless-current")
        candidate_id = self._evidence("authorityless-current", 31)
        current_id = self._evidence("authorityless-current", 32)
        self.assertTrue(self.db.set_request_current_evidence(request_id, current_id))
        self.db._execute(
            "UPDATE album_requests SET mb_release_id = NULL WHERE id = %s",
            (request_id,),
        )
        job = self.db.enqueue_import_job(
            "force_import", request_id=request_id,
            payload={"download_log_id": 1, "failed_path": "/tmp/candidate"},
        )
        self.assertTrue(self.db.set_import_job_candidate_evidence(job.id, candidate_id))
        result, corpus, coverage = self._export()
        self.assertFalse(result.green)
        self.assertEqual(coverage["referenced_current_ids"], [current_id])
        self.assertEqual(coverage["missing_current_evidence_ids"], [])
        observed = coverage["observed_evidence"]
        assert isinstance(observed, list)
        self.assertEqual(
            {row["evidence_id"] for row in observed if is_str_object_dict(row)},
            {candidate_id, current_id},
        )
        self.assertEqual(
            [row["id"] for row in corpus], [candidate_id, current_id]
        )
        self.assertTrue(all(not row["is_candidate"] for row in corpus))
        outputs = coverage["outputs"]
        assert is_str_object_dict(outputs)
        corpus_output = outputs["corpus"]
        assert is_str_object_dict(corpus_output)
        self.assertEqual(corpus_output["expected_referenced_current_ids"], [])

    def test_verify_rejects_stale_pair_and_export_rejects_same_destination(
        self,
    ) -> None:
        """The executable verifier, not documentation, binds the artifacts."""
        release = "verified-pair"
        request_id = self._request(release)
        candidate_id = self._evidence(release, 41)
        job = self.db.enqueue_import_job(
            "force_import",
            request_id=request_id,
            payload={"download_log_id": 1, "failed_path": "/tmp/candidate"},
        )
        self.assertTrue(self.db.set_import_job_candidate_evidence(job.id, candidate_id))
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            corpus, coverage = root / "corpus.jsonl", root / "coverage.json"
            export_decision_corpus(TEST_DSN, corpus, coverage)
            verify_decision_corpus_pair(corpus, coverage)
            corpus.write_bytes(corpus.read_bytes() + b"\n")
            with self.assertRaises(RenderDifferentialError):
                verify_decision_corpus_pair(corpus, coverage)
            with self.assertRaises(RenderDifferentialError):
                export_decision_corpus(TEST_DSN, root / "same", root / "./same")

    def test_decide_verifies_pair_before_writing(self) -> None:
        release = "decide-coverage-gate"
        request_id = self._request(release)
        candidate_id = self._evidence(release, 81)
        job = self.db.enqueue_import_job(
            "force_import", request_id=request_id,
            payload={"download_log_id": 1, "failed_path": "/tmp/candidate"},
        )
        self.assertTrue(self.db.set_import_job_candidate_evidence(job.id, candidate_id))
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            corpus, coverage, out = root / "corpus", root / "coverage", root / "out"
            export_decision_corpus(TEST_DSN, corpus, coverage)
            self.assertEqual(
                main(["decide", "--corpus", str(corpus), "--coverage", str(coverage), "--out", str(out)]),
                0,
            )
            corpus.write_bytes(corpus.read_bytes() + b"\n")
            self.assertEqual(
                main(["decide", "--corpus", str(corpus), "--coverage", str(coverage), "--out", str(out)]),
                1,
            )

    def test_cli_returns_non_green_after_writing_authority_debt_outputs(self) -> None:
        """The public executable reports debt with status 2, never a green lie."""
        candidate_id = self._evidence("authorityless-cli", 33)
        job = self.db.enqueue_import_job(
            "force_import",
            request_id=None,
            payload={"download_log_id": 1, "failed_path": "/tmp/candidate"},
        )
        self.assertTrue(self.db.set_import_job_candidate_evidence(job.id, candidate_id))
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            with redirect_stderr(StringIO()):
                status = main(
                    [
                        "export",
                        "--dsn",
                        TEST_DSN or "",
                        "--corpus",
                        str(root / "corpus.jsonl"),
                        "--coverage",
                        str(root / "coverage.json"),
                    ]
                )
            self.assertEqual(status, 2)
            self.assertTrue((root / "corpus.jsonl").is_file())
            self.assertTrue((root / "coverage.json").is_file())

    def test_malformed_nested_pg_evidence_fails_at_the_export_wire_boundary(
        self,
    ) -> None:
        """A typed JSONB projection cannot be silently coerced into a corpus."""
        release = "malformed-export-wire"
        request_id = self._request(release)
        candidate_id = self._evidence(release, 34)
        job = self.db.enqueue_import_job(
            "force_import",
            request_id=request_id,
            payload={"download_log_id": 1, "failed_path": "/tmp/candidate"},
        )
        self.assertTrue(self.db.set_import_job_candidate_evidence(job.id, candidate_id))
        self.db._execute(
            "UPDATE album_quality_evidence SET aac_lattice_tracks = "
            '\'[{"filename": "01.m4a", "offset": "wrong", '
            '"z": null, "proba": null, "error": null}]\'::jsonb '
            "WHERE id = %s",
            (candidate_id,),
        )
        self.db.conn.commit()
        with (
            TemporaryDirectory() as tmp,
            self.assertRaisesRegex(RenderDifferentialError, "projection/wire drift"),
        ):
            export_decision_corpus(
                TEST_DSN,
                Path(tmp) / "corpus.jsonl",
                Path(tmp) / "coverage.json",
            )

    def test_file_content_address_mismatch_is_complete_named_debt(self) -> None:
        """A stored fingerprint is verified against the exported file content."""
        release = "content-address-debt"
        request_id = self._request(release)
        candidate_id = self._evidence(release, 35)
        job = self.db.enqueue_import_job(
            "force_import",
            request_id=request_id,
            payload={"download_log_id": 1, "failed_path": "/tmp/candidate"},
        )
        self.assertTrue(self.db.set_import_job_candidate_evidence(job.id, candidate_id))
        self.db._execute(
            "UPDATE album_quality_evidence SET snapshot_fingerprint = %s WHERE id = %s",
            ("0" * 64, candidate_id),
        )
        self.db.conn.commit()

        result, corpus, coverage = self._export()

        self.assertFalse(result.green)
        self.assertEqual([row["id"] for row in corpus], [candidate_id])
        self.assertFalse(corpus[0]["is_candidate"])
        mismatches = coverage["content_address_mismatches"]
        assert isinstance(mismatches, list) and mismatches
        assert is_str_object_dict(mismatches[0])
        self.assertEqual(mismatches[0]["evidence_id"], candidate_id)

    def test_conflict_is_named_without_suppressing_another_valid_candidate(
        self,
    ) -> None:
        """A conflicting candidate is debt; it never selects an arbitrary request."""
        release = "decision-corpus-conflict"
        first_request = self._request(release)
        second_release = "decision-corpus-conflict-other"
        second_request = self._request(second_release)
        conflicted_id = self._evidence(release, 30)
        current_id = self._evidence(second_release, 31)
        valid_id = self._evidence(release, 32)
        self.assertTrue(
            self.db.set_request_current_evidence(second_request, current_id)
        )
        for request_id in (first_request, second_request):
            job = self.db.enqueue_import_job(
                "force_import",
                request_id=request_id,
                payload={
                    "download_log_id": request_id,
                    "failed_path": f"/tmp/{request_id}",
                },
            )
            self.assertTrue(
                self.db.set_import_job_candidate_evidence(job.id, conflicted_id)
            )
        valid_job = self.db.enqueue_import_job(
            "force_import",
            request_id=first_request,
            payload={"download_log_id": 99, "failed_path": "/tmp/valid"},
        )
        self.assertTrue(
            self.db.set_import_job_candidate_evidence(valid_job.id, valid_id)
        )

        result, corpus, coverage = self._export()

        self.assertFalse(result.green)
        self.assertEqual(
            coverage["association_conflicts"],
            [
                {
                    "evidence_id": conflicted_id,
                    "associations": [
                        {
                            "candidate_evidence_id": conflicted_id,
                            "current_evidence_id": None,
                            "request_mb_release_id": release,
                        },
                        {
                            "candidate_evidence_id": conflicted_id,
                            "current_evidence_id": current_id,
                            "request_mb_release_id": second_release,
                        },
                    ],
                }
            ],
        )
        self.assertEqual(
            coverage["candidate_release_mismatches"],
            [
                {
                    "evidence_id": conflicted_id,
                    "evidence_mb_release_id": release,
                    "request_mb_release_id": second_release,
                }
            ],
        )
        self.assertEqual(
            [row["id"] for row in corpus],
            [conflicted_id, current_id, valid_id],
        )
        self.assertEqual(
            [row["is_candidate"] for row in corpus],
            [False, False, True],
        )

    def test_batch_size_and_source_order_do_not_change_bytes(self) -> None:
        """A repeatable snapshot has one deterministic JSONL/coverage form."""
        release = "decision-corpus-batches"
        request_id = self._request(release)
        candidate_id = self._evidence(release, 4)
        job = self.db.enqueue_import_job(
            "force_import",
            request_id=request_id,
            payload={"download_log_id": 1, "failed_path": "/tmp/candidate"},
        )
        self.assertTrue(self.db.set_import_job_candidate_evidence(job.id, candidate_id))
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            first_corpus, first_coverage = root / "first.jsonl", root / "first.json"
            second_corpus, second_coverage = root / "second.jsonl", root / "second.json"
            first = export_decision_corpus(
                TEST_DSN, first_corpus, first_coverage, batch_size=1
            )
            second = export_decision_corpus(
                TEST_DSN, second_corpus, second_coverage, batch_size=97
            )
            self.assertTrue(first.green)
            self.assertTrue(second.green)
            self.assertEqual(first_corpus.read_bytes(), second_corpus.read_bytes())
            self.assertEqual(first_coverage.read_bytes(), second_coverage.read_bytes())

    def test_export_holds_one_qualified_repeatable_read_snapshot_across_later_batches(
        self,
    ) -> None:
        """A concurrent writer after link collection is excluded from this export."""
        release = "repeatable-read-export"
        request_id = self._request(release)
        candidate_id = self._evidence(release, 51)
        original_path = "/tmp/decision-corpus-51"
        job = self.db.enqueue_import_job(
            "force_import",
            request_id=request_id,
            payload={"download_log_id": 1, "failed_path": original_path},
        )
        self.assertTrue(self.db.set_import_job_candidate_evidence(job.id, candidate_id))
        snapshots: list[_DecisionCorpusSnapshot] = []

        def mutate_after_links(snapshot: _DecisionCorpusSnapshot) -> None:
            snapshots.append(snapshot)
            self.assertEqual(snapshot.isolation, "repeatable read")
            self.assertEqual(snapshot.read_only, "on")
            self.assertTrue(snapshot.snapshot)
            self.db._execute(
                "UPDATE album_quality_evidence SET source_path = %s WHERE id = %s",
                ("/tmp/concurrent-mutation", candidate_id),
            )
            later = self.db.enqueue_import_job(
                "force_import",
                request_id=request_id,
                payload={"download_log_id": 2, "failed_path": "/tmp/later"},
            )
            self.assertTrue(self.db.set_import_job_candidate_evidence(later.id, candidate_id))
            self.db.conn.commit()

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            export_decision_corpus(
                TEST_DSN,
                root / "corpus.jsonl",
                root / "coverage.json",
                batch_size=1,
                _after_source_links=mutate_after_links,
            )
            coverage = json.loads((root / "coverage.json").read_text())
            corpus = [json.loads(line) for line in (root / "corpus.jsonl").read_text().splitlines()]
        self.assertEqual(len(snapshots), 1)
        self.assertEqual(coverage["total_source_links"], 1)
        self.assertEqual(corpus[0]["source_path"], original_path)

    def test_verifier_rejects_every_coverage_summary_and_nested_ledger_mutant(self) -> None:
        """The public artifact verifier recomputes every claimed summary field."""
        release = "strict-coverage-mutants"
        request_id = self._request(release)
        candidate_id = self._evidence(release, 52)
        job = self.db.enqueue_import_job(
            "force_import",
            request_id=request_id,
            payload={"download_log_id": 1, "failed_path": "/tmp/candidate"},
        )
        self.assertTrue(self.db.set_import_job_candidate_evidence(job.id, candidate_id))
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            corpus, coverage_path = root / "corpus.jsonl", root / "coverage.json"
            export_decision_corpus(TEST_DSN, corpus, coverage_path)
            original = json.loads(coverage_path.read_text())
            mutations = {
                "schema_version": lambda data: data.__setitem__("schema_version", 1),
                "green": lambda data: data.__setitem__("green", False),
                "debt_count": lambda data: data.__setitem__("debt_count", 1),
                "count": lambda data: data.__setitem__("total_source_links", 99),
                "role": lambda data: data["outputs"]["corpus"]["written_candidate_ids"].clear(),
                "address": lambda data: data["outputs"]["corpus"]["content_addresses"][0].__setitem__("files_sha256", "0" * 64),
                "source_arm": lambda data: data["source_links"][0].__setitem__("source", "download_log"),
                "unknown_source": lambda data: (
                    data["source_links"][0].__setitem__("source", "unknown"),
                    data.__setitem__(
                        "source_link_counts",
                        {"download_log": 0, "import_jobs": 0},
                    ),
                    data.__setitem__(
                        "source_distinct_candidate_id_counts",
                        {"download_log": 0, "import_jobs": 0},
                    ),
                ),
                "observed": lambda data: data["observed_evidence"][0].__setitem__("mb_release_id", "wrong"),
                "unknown": lambda data: data.__setitem__("unexpected", True),
            }
            for name, mutate in mutations.items():
                broken = deepcopy(original)
                mutate(broken)
                coverage_path.write_text(json.dumps(broken) + "\n")
                with self.subTest(name=name), self.assertRaises(RenderDifferentialError):
                    verify_decision_corpus_pair(corpus, coverage_path)

    def test_exact_base_archive_can_help_and_decide_with_the_copied_script(self) -> None:
        """Current CD corpus replays through pre-CD production boundaries."""
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            release = "base-archive-decision-corpus"
            request_id = self._request(release)
            candidate_id = self._cd_evidence(release, 71)
            job = self.db.enqueue_import_job(
                "force_import", request_id=request_id,
                payload={"download_log_id": 1, "failed_path": "/tmp/candidate"},
            )
            self.assertTrue(self.db.set_import_job_candidate_evidence(job.id, candidate_id))
            corpus = root / "corpus.jsonl"
            coverage = root / "coverage.json"
            export_decision_corpus(TEST_DSN, corpus, coverage)
            for base_ref in (
                "3fdf2748",
                "7adc9b115a0561e04cd6b1f212de4249de566f00",
            ):
                base_root = root / base_ref
                base_root.mkdir()
                archive = root / f"{base_ref}.tar"
                subprocess.run(
                    [
                        "git",
                        "archive",
                        "--format=tar",
                        "--output",
                        str(archive),
                        base_ref,
                    ],
                    check=True,
                )
                subprocess.run(
                    ["tar", "-xf", str(archive), "-C", str(base_root)],
                    check=True,
                )
                script = base_root / "scripts" / "decision_differential.py"
                shutil.copy2(
                    Path(__file__).parents[1] / "scripts" / script.name,
                    script,
                )
                completed = subprocess.run(
                    ["nix-shell", "--run", f"{sys.executable} {script} --help"],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                with self.subTest(base_ref=base_ref, command="help"):
                    self.assertEqual(completed.returncode, 0, completed.stderr)

                for counterfactual in (False, True):
                    out = root / f"decided-{base_ref}-{counterfactual}.jsonl"
                    args = (
                        "decide",
                        "--corpus",
                        str(corpus),
                        "--coverage",
                        str(coverage),
                        "--out",
                        str(out),
                        *(("--counterfactual",) if counterfactual else ()),
                    )
                    completed = subprocess.run(
                        [
                            "nix-shell",
                            "--run",
                            " ".join((sys.executable, str(script), *args)),
                        ],
                        check=False,
                        capture_output=True,
                        text=True,
                    )
                    with self.subTest(
                        base_ref=base_ref,
                        counterfactual=counterfactual,
                    ):
                        self.assertEqual(completed.returncode, 0, completed.stderr)
                        decided = json.loads(out.read_text(encoding="utf-8"))
                        self.assertIsNone(decided["fields"]["decision_error"])
                        if not counterfactual:
                            self.assertEqual(
                                decided["fields"][
                                    "verified_lossless_classifier"
                                ],
                                CD_RIP_BIT_VERIFIED_CLASSIFIER,
                            )
