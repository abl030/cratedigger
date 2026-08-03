"""Real-PostgreSQL pins for the decision-corpus exporter (#999)."""

from __future__ import annotations

import json
import unittest
from contextlib import redirect_stderr
from copy import deepcopy
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory

from lib.json_narrow import is_str_object_dict
from lib.quality import AlbumQualityEvidenceFile
from scripts.decision_differential import (
    _EVIDENCE_SCHEMA_TYPES,
    _FILE_SCHEMA_TYPES,
    _SOURCE_SCHEMA_TYPES,
    DecisionCorpusExportResult,
    RenderDifferentialError,
    _assert_live_decision_corpus_schema,
    assert_decision_corpus_schema,
    assert_export_output_exact,
    export_decision_corpus,
    main,
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
        self.assertEqual(corpus, [])
        outputs = coverage["outputs"]
        assert is_str_object_dict(outputs)
        corpus_output = outputs["corpus"]
        assert is_str_object_dict(corpus_output)
        self.assertTrue(corpus_output["exact_match"])
        self.assertIsInstance(corpus_output["sha256"], str)

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
        self.assertEqual(corpus, [])
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
                        {"current_evidence_id": None, "request_mb_release_id": release},
                        {
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
        self.assertEqual([row["id"] for row in corpus], [valid_id])

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
