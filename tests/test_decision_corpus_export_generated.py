"""Generated outer-boundary properties for the decision-corpus exporter."""

from __future__ import annotations

import json
import unittest
from copy import deepcopy
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory

import msgspec
from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.quality import AlbumQualityEvidenceFile
from scripts.decision_differential import (
    _EVIDENCE_SCHEMA_TYPES,
    _FILE_SCHEMA_TYPES,
    _SOURCE_SCHEMA_TYPES,
    RenderDifferentialError,
    _CoverageAddress,
    _decision_corpus_evidence_columns,
    _decision_corpus_evidence_file_columns,
    _evidence_from_corpus_row,
    assert_decision_corpus_schema,
    assert_export_output_exact,
    duplicate_content_addresses,
    export_decision_corpus,
    verify_decision_corpus_pair,
)
from tests.helpers import make_album_quality_evidence
from tests.test_decision_differential import _corpus_row
from tests.test_pipeline_db import TEST_DSN, make_db, requires_postgres


@requires_postgres
class TestDecisionCorpusExportGenerated(unittest.TestCase):
    @given(
        size_bytes=st.integers(min_value=1, max_value=10_000_000),
        first_mtime_ns=st.integers(min_value=0, max_value=10_000_000),
        mtime_delta=st.integers(min_value=1, max_value=10_000),
    )
    def test_real_writer_and_exporter_enforce_content_address_semantics(
        self,
        size_bytes: int,
        first_mtime_ns: int,
        mtime_delta: int,
    ) -> None:
        """Malformed claims fail; excluded mtime drift stays informational."""
        db = make_db()
        try:
            release = "generated-content-address"
            request_id = db.add_request(
                "Generated",
                release,
                "request",
                release,
            )
            first = make_album_quality_evidence(
                mb_release_id=release,
                source_path="/tmp/generated-first",
                files=[
                    AlbumQualityEvidenceFile(
                        relative_path="01.mp3",
                        size_bytes=size_bytes,
                        mtime_ns=first_mtime_ns,
                        extension="mp3",
                        container="mp3",
                        codec="mp3",
                    )
                ],
            )
            wrong_fingerprint = (
                "0" * 64
                if first.snapshot_fingerprint != "0" * 64
                else "1" * 64
            )
            malformed = msgspec.structs.replace(
                first,
                mb_release_id=f"{release}-malformed",
                snapshot_fingerprint=wrong_fingerprint,
            )
            with self.assertRaises(ValueError):
                db.upsert_album_quality_evidence(malformed)
            self.assertIsNone(
                db.find_album_quality_evidence(
                    mb_release_id=malformed.mb_release_id,
                    snapshot_fingerprint=wrong_fingerprint,
                )
            )

            db.upsert_album_quality_evidence(first)
            stored_first = db.find_album_quality_evidence(
                mb_release_id=release,
                snapshot_fingerprint=first.snapshot_fingerprint,
            )
            assert stored_first is not None and stored_first.id is not None
            db._execute(
                "UPDATE album_quality_evidence SET snapshot_fingerprint = %s "
                "WHERE id = %s",
                (wrong_fingerprint, stored_first.id),
            )
            second = make_album_quality_evidence(
                mb_release_id=release,
                source_path="/tmp/generated-second",
                files=[
                    AlbumQualityEvidenceFile(
                        relative_path="01.mp3",
                        size_bytes=size_bytes,
                        mtime_ns=first_mtime_ns + mtime_delta,
                        extension="mp3",
                        container="mp3",
                        codec="mp3",
                    )
                ],
            )
            db.upsert_album_quality_evidence(second)
            stored_second = db.find_album_quality_evidence(
                mb_release_id=release,
                snapshot_fingerprint=second.snapshot_fingerprint,
            )
            assert stored_second is not None and stored_second.id is not None
            job = db.enqueue_import_job(
                "force_import",
                request_id=request_id,
                payload={
                    "download_log_id": 1,
                    "failed_path": "/tmp/generated-first",
                },
            )
            self.assertTrue(
                db.set_import_job_candidate_evidence(job.id, stored_first.id)
            )
            db.conn.commit()
            with TemporaryDirectory() as tmp:
                root = Path(tmp)
                result = export_decision_corpus(
                    TEST_DSN,
                    root / "corpus.jsonl",
                    root / "coverage.json",
                )
                coverage = json.loads((root / "coverage.json").read_text())

            self.assertEqual(result.debt_count, 1)
            self.assertEqual(
                [item["evidence_id"] for item in coverage["content_address_mismatches"]],
                [stored_first.id],
            )
            self.assertEqual(
                coverage["duplicate_derived_addresses"],
                [
                    {
                        "mb_release_id": release,
                        "snapshot_fingerprint": first.snapshot_fingerprint,
                        "evidence_ids": [stored_first.id, stored_second.id],
                    }
                ],
            )
        finally:
            db.close()

    @given(
        keys=st.lists(
            st.tuples(st.text(min_size=1, max_size=8), st.text(min_size=1, max_size=8)),
            min_size=1, max_size=200,
        )
    )
    def test_linear_address_conflicts_match_reference(self, keys) -> None:
        addresses = [
            _CoverageAddress(
                id=index, mb_release_id=release,
                snapshot_fingerprint=fingerprint, files_sha256="x",
            )
            for index, (release, fingerprint) in enumerate(keys)
        ]
        expected = sorted({key for key in keys if keys.count(key) > 1})
        self.assertEqual(duplicate_content_addresses(addresses), expected)

    def test_linear_address_conflicts_handles_a_large_unique_input(self) -> None:
        """A large, mostly-unique ledger stays on the linear Counter path."""
        addresses = [
            _CoverageAddress(
                id=index,
                mb_release_id=f"release-{index}",
                snapshot_fingerprint=f"fingerprint-{index}",
                files_sha256="x",
            )
            for index in range(10_000)
        ]
        addresses.append(
            _CoverageAddress(
                id=10_000,
                mb_release_id="release-7777",
                snapshot_fingerprint="fingerprint-7777",
                files_sha256="x",
            )
        )
        self.assertEqual(
            duplicate_content_addresses(addresses),
            [("release-7777", "fingerprint-7777")],
        )

    @given(
        table=st.sampled_from(
            (
                "album_quality_evidence",
                "album_quality_evidence_files",
                *_SOURCE_SCHEMA_TYPES,
            )
        ),
        mutation=st.sampled_from(("missing", "type", "nullable")),
        column_index=st.integers(min_value=0, max_value=56),
    )
    def test_schema_mutations_trip_the_production_checker(
        self,
        table: str,
        mutation: str,
        column_index: int,
    ) -> None:
        """Every consumed schema column rejects one-field drift."""
        evidence = dict(_EVIDENCE_SCHEMA_TYPES)
        files = deepcopy(_FILE_SCHEMA_TYPES)
        descriptions = {
            "album_quality_evidence": evidence,
            "album_quality_evidence_files": files,
            **deepcopy(_SOURCE_SCHEMA_TYPES),
        }
        columns = (
            _decision_corpus_evidence_columns()
            if table == "album_quality_evidence"
            else (*_decision_corpus_evidence_file_columns(), "evidence_id", "ordinal")
            if table == "album_quality_evidence_files"
            else tuple(_SOURCE_SCHEMA_TYPES[table])
        )
        column = columns[column_index % len(columns)]
        if mutation == "missing":
            del descriptions[table][column]
        elif mutation == "type":
            kind, nullable = descriptions[table][column]
            descriptions[table][column] = (
                "text" if kind != "text" else "integer",
                nullable,
            )
        else:
            kind, nullable = descriptions[table][column]
            descriptions[table][column] = (kind, not nullable)
        with self.assertRaises(RenderDifferentialError):
            assert_decision_corpus_schema(descriptions)

    @given(
        ids=st.lists(
            st.integers(min_value=1, max_value=100), min_size=2, max_size=8, unique=True
        ),
        mutant=st.sampled_from(("omit", "duplicate", "substitute", "misrole")),
    )
    def test_output_role_mutants_trip_the_production_checker(
        self,
        ids: list[int],
        mutant: str,
    ) -> None:
        candidates, currents = sorted(ids[::2]), sorted(ids[1::2])
        associations = [
            (candidate, currents[index % len(currents)], "release")
            for index, candidate in enumerate(candidates)
        ]
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
                "id": value,
                "is_candidate": False,
                "current_evidence_id": None,
                "request_mb_release_id": None,
                "files": [],
            }
            for value in currents
        ]
        assert_export_output_exact(rows, associations, currents)
        broken = deepcopy(rows)
        if mutant == "omit":
            broken.pop(0)
        elif mutant == "duplicate":
            broken.append(dict(broken[0]))
        elif mutant == "substitute":
            broken[0]["id"] = 1000
        else:
            broken[0]["is_candidate"] = False
        with self.assertRaises(RenderDifferentialError):
            assert_export_output_exact(broken, associations, currents)

    @given(
        mutation=st.sampled_from(
            (
                "bool",
                "audio",
                "aac",
                "file",
                "unknown_top",
                "unknown_audio",
                "unknown_file",
            )
        )
    )
    def test_nested_wire_mutants_fail_before_production_coercion(
        self, mutation: str
    ) -> None:
        row = _corpus_row()
        if mutation == "bool":
            row["is_cbr"] = "false"
        elif mutation == "audio":
            audio = row["audio_validation"]
            assert isinstance(audio, dict)
            audio["files_checked"] = "0"
        elif mutation == "aac":
            row["aac_lattice_tracks"] = [
                {
                    "filename": "a",
                    "offset": "x",
                    "z": None,
                    "proba": None,
                    "error": None,
                }
            ]
        elif mutation == "file":
            files = row["files"]
            assert isinstance(files, list) and isinstance(files[0], dict)
            files[0]["size_bytes"] = "1"
        elif mutation == "unknown_top":
            row["unexpected"] = True
        elif mutation == "unknown_audio":
            audio = row["audio_validation"]
            assert isinstance(audio, dict)
            audio["unexpected"] = True
        else:
            files = row["files"]
            assert isinstance(files, list) and isinstance(files[0], dict)
            files[0]["unexpected"] = True
        with self.assertRaises(RenderDifferentialError):
            _evidence_from_corpus_row(row)

    @given(
        link_count=st.integers(min_value=1, max_value=4),
        paired=st.booleans(),
        mutation=st.sampled_from(
            (
                "schema_version",
                "green",
                "debt_count",
                "source_arm",
                "observed_release",
                "role",
                "address",
                "nested_unknown",
            )
        ),
    )
    def test_real_pg_export_is_exact_and_batch_invariant(
        self,
        link_count: int,
        paired: bool,
        mutation: str,
    ) -> None:
        db = make_db()
        try:
            release = f"generated-export-{link_count}-{paired}"
            request_id = db.add_request("Generated", release, "request", release)
            candidate = make_album_quality_evidence(
                mb_release_id=release,
                source_path="/tmp/generated-candidate",
                files=[
                    AlbumQualityEvidenceFile(
                        relative_path="candidate.mp3",
                        size_bytes=1,
                        mtime_ns=1,
                        extension="mp3",
                        container="mp3",
                        codec="mp3",
                    )
                ],
            )
            db.upsert_album_quality_evidence(candidate)
            candidate_row = db.find_album_quality_evidence(
                mb_release_id=release,
                snapshot_fingerprint=candidate.snapshot_fingerprint,
            )
            assert candidate_row is not None and candidate_row.id is not None
            expected_current_ids: list[int] = []
            if paired:
                current = make_album_quality_evidence(
                    mb_release_id=release,
                    source_path="/tmp/generated-current",
                    files=[
                        AlbumQualityEvidenceFile(
                            relative_path="current.mp3",
                            size_bytes=2,
                            mtime_ns=2,
                            extension="mp3",
                            container="mp3",
                            codec="mp3",
                        )
                    ],
                )
                db.upsert_album_quality_evidence(current)
                current_row = db.find_album_quality_evidence(
                    mb_release_id=release,
                    snapshot_fingerprint=current.snapshot_fingerprint,
                )
                assert current_row is not None and current_row.id is not None
                self.assertTrue(
                    db.set_request_current_evidence(request_id, current_row.id)
                )
                expected_current_ids.append(current_row.id)
            for offset in range(link_count):
                job = db.enqueue_import_job(
                    "force_import",
                    request_id=request_id,
                    payload={
                        "download_log_id": offset + 1,
                        "failed_path": f"/tmp/{offset}",
                    },
                )
                self.assertTrue(
                    db.set_import_job_candidate_evidence(job.id, candidate_row.id)
                )
            with TemporaryDirectory() as tmp:
                root = Path(tmp)
                corpus_path, coverage_path = (
                    root / "corpus.jsonl",
                    root / "coverage.json",
                )
                result = export_decision_corpus(
                    TEST_DSN, corpus_path, coverage_path, batch_size=link_count
                )
                corpus = [
                    json.loads(line) for line in corpus_path.read_text().splitlines()
                ]
                self.assertTrue(result.green)
                assert_export_output_exact(
                    corpus,
                    [(candidate_row.id, expected_current_ids[0], release)]
                    if paired
                    else [(candidate_row.id, None, release)],
                    expected_current_ids,
                )
                coverage = json.loads(coverage_path.read_text())
                self.assertEqual(
                    coverage["valid_candidates"]["paired" if paired else "unpaired"], 1
                )
                verify_decision_corpus_pair(corpus_path, coverage_path)
                broken = deepcopy(coverage)
                if mutation == "schema_version":
                    broken["schema_version"] = 1
                elif mutation == "green":
                    broken["green"] = False
                elif mutation == "debt_count":
                    broken["debt_count"] = 1
                elif mutation == "source_arm":
                    broken["source_links"][0]["source"] = "download_log"
                elif mutation == "observed_release":
                    broken["observed_evidence"][0]["mb_release_id"] = "wrong"
                elif mutation == "role":
                    broken["outputs"]["corpus"]["written_candidate_ids"] = []
                elif mutation == "address":
                    broken["outputs"]["corpus"]["content_addresses"][0][
                        "files_sha256"
                    ] = "0" * 64
                else:
                    broken["source_links"][0]["unknown"] = True
                coverage_path.write_text(json.dumps(broken) + "\n")
                with self.assertRaises(RenderDifferentialError):
                    verify_decision_corpus_pair(corpus_path, coverage_path)
        finally:
            db.close()

    @given(
        multiplicity=st.integers(min_value=1, max_value=3),
        reverse_insertion=st.booleans(),
        batch_sizes=st.tuples(
            st.integers(min_value=1, max_value=4),
            st.integers(min_value=1, max_value=4),
        ).filter(lambda pair: pair[0] != pair[1]),
    )
    def test_real_pg_multigraph_reconciles_all_export_debt(
        self,
        multiplicity: int,
        reverse_insertion: bool,
        batch_sizes: tuple[int, int],
    ) -> None:
        """One outer-PG graph overlaps all admitted debt classes.

        FKs forbid synthetic missing/dangling links; those remain qualified by
        strict-boundary helper mutations rather than falsely claimed here.
        """
        db = make_db()
        try:
            release = f"generated-graph-{multiplicity}-{reverse_insertion}"
            wrong_release = f"{release}-wrong"

            def evidence(label: str, release_id: str, size: int) -> int:
                item = make_album_quality_evidence(
                    mb_release_id=release_id,
                    source_path=f"/tmp/{label}",
                    files=[
                        AlbumQualityEvidenceFile(
                            relative_path=f"{label}.mp3",
                            size_bytes=size,
                            mtime_ns=size,
                            extension="mp3",
                            container="mp3",
                            codec="mp3",
                        )
                    ],
                )
                db.upsert_album_quality_evidence(item)
                stored = db.find_album_quality_evidence(
                    mb_release_id=release_id,
                    snapshot_fingerprint=item.snapshot_fingerprint,
                )
                assert stored is not None and stored.id is not None
                return stored.id

            paired_id = evidence("paired", release, 1)
            dual_id = evidence("dual", release, 2)
            authorityless_id = evidence("authorityless", release, 4)
            conflict_id = evidence("conflict", release, 5)
            wrong_current_id = evidence("wrong-current", release, 6)
            paired_request = db.add_request("Generated", release, "request", release)
            unpaired_release = f"{release}-unpaired"
            unpaired_id = evidence("unpaired", unpaired_release, 3)
            unpaired_request = db.add_request(
                "Generated", unpaired_release, "request", unpaired_release
            )
            authorityless_request_release = f"{release}-authorityless"
            authorityless_request = db.add_request(
                "Generated",
                authorityless_request_release,
                "request",
                authorityless_request_release,
            )
            conflict_bad_request = db.add_request(
                "Generated", wrong_release, "request", wrong_release
            )
            self.assertTrue(db.set_request_current_evidence(paired_request, dual_id))
            self.assertTrue(
                db.set_request_current_evidence(authorityless_request, authorityless_id)
            )
            self.assertTrue(
                db.set_request_current_evidence(conflict_bad_request, wrong_current_id)
            )

            def import_link(
                request_id: int | None, evidence_id: int, ordinal: int
            ) -> None:
                job = db.enqueue_import_job(
                    "force_import",
                    request_id=request_id,
                    payload={
                        "download_log_id": ordinal,
                        "failed_path": f"/tmp/{ordinal}",
                    },
                )
                self.assertTrue(
                    db.set_import_job_candidate_evidence(job.id, evidence_id)
                )

            def download_link(request_id: int, evidence_id: int, ordinal: int) -> None:
                log_id = db.log_download(request_id=request_id, outcome="rejected")
                db.set_download_log_candidate_evidence(log_id, evidence_id)

            operations: list[tuple[str, int | None, int]] = []
            for ordinal in range(multiplicity):
                operations.extend(
                    [
                        ("import", paired_request, paired_id),
                        ("download", paired_request, paired_id),
                        ("download", unpaired_request, unpaired_id),
                        ("import", authorityless_request, authorityless_id),
                        ("import", paired_request, conflict_id),
                        ("download", conflict_bad_request, conflict_id),
                    ]
                )
            operations.append(("import", paired_request, dual_id))
            if reverse_insertion:
                operations.reverse()
            for ordinal, (source, request_id, evidence_id) in enumerate(
                operations, start=1
            ):
                if source == "import":
                    import_link(request_id, evidence_id, ordinal)
                else:
                    assert request_id is not None
                    download_link(request_id, evidence_id, ordinal)
            db._execute(
                "UPDATE album_requests SET mb_release_id = NULL WHERE id = %s",
                (authorityless_request,),
            )
            db._execute(
                "UPDATE album_quality_evidence SET snapshot_fingerprint = %s WHERE id = %s",
                ("f" * 64, authorityless_id),
            )
            db.conn.commit()

            with TemporaryDirectory() as tmp:
                root = Path(tmp)
                paths = [
                    (root / "first.jsonl", root / "first.json", batch_sizes[0]),
                    (root / "second.jsonl", root / "second.json", batch_sizes[1]),
                ]
                first_result = export_decision_corpus(
                    TEST_DSN, paths[0][0], paths[0][1], batch_size=paths[0][2]
                )
                second_result = export_decision_corpus(
                    TEST_DSN, paths[1][0], paths[1][1], batch_size=paths[1][2]
                )
                self.assertFalse(first_result.green)
                self.assertFalse(second_result.green)
                corpus_bytes = paths[0][0].read_bytes()
                coverage_bytes = paths[0][1].read_bytes()
                self.assertEqual(corpus_bytes, paths[1][0].read_bytes())
                self.assertEqual(coverage_bytes, paths[1][1].read_bytes())
                verify_decision_corpus_pair(paths[0][0], paths[0][1])
                corpus = [
                    json.loads(line) for line in corpus_bytes.decode().splitlines()
                ]
                coverage = json.loads(coverage_bytes)

            associations = [
                (paired_id, dual_id, release),
                (dual_id, dual_id, release),
                (unpaired_id, None, unpaired_release),
            ]
            all_evidence_ids = sorted({
                paired_id,
                dual_id,
                authorityless_id,
                conflict_id,
                wrong_current_id,
                unpaired_id,
            })
            assert_export_output_exact(
                corpus,
                associations,
                [dual_id],
                expected_all_evidence_ids=all_evidence_ids,
            )
            self.assertEqual(coverage["total_source_links"], 6 * multiplicity + 1)
            self.assertEqual(
                coverage["source_link_counts"],
                {
                    "import_jobs": 3 * multiplicity + 1,
                    "download_log": 3 * multiplicity,
                },
            )
            self.assertEqual(coverage["total_distinct_candidate_ids"], 5)
            self.assertEqual(
                coverage["identical_associations_collapsed"], 5 * multiplicity - 4
            )
            self.assertEqual(
                coverage["authorityless_candidate_ids"], [authorityless_id]
            )
            self.assertEqual(len(coverage["authorityless_source_links"]), multiplicity)
            self.assertEqual(
                coverage["authorityless_source_links"][0]["reason"],
                "request_missing_release_authority",
            )
            self.assertEqual(coverage["valid_candidates"], {"paired": 2, "unpaired": 1})
            self.assertEqual(
                coverage["referenced_current_ids"],
                [dual_id, authorityless_id, wrong_current_id],
            )
            self.assertIn(
                authorityless_id,
                [item["evidence_id"] for item in coverage["observed_evidence"]],
            )
            self.assertEqual(len(coverage["association_conflicts"]), 1)
            self.assertEqual(
                coverage["association_conflicts"][0]["evidence_id"], conflict_id
            )
            self.assertEqual(len(coverage["candidate_release_mismatches"]), 1)
            self.assertEqual(len(coverage["current_release_mismatches"]), 1)
            self.assertEqual(
                coverage["content_address_mismatches"][0]["evidence_id"],
                authorityless_id,
            )
            output = coverage["outputs"]["corpus"]
            self.assertEqual(
                output["expected_associations"],
                [
                    {
                        "candidate_evidence_id": candidate,
                        "current_evidence_id": current,
                        "request_mb_release_id": request_release,
                    }
                    for candidate, current, request_release in associations
                ],
            )
            self.assertEqual(output["expected_referenced_current_ids"], [dual_id])
            self.assertEqual(
                output["expected_current_only_ids"],
                sorted({authorityless_id, conflict_id, wrong_current_id}),
            )
            self.assertEqual(output["dual_role_ids"], [dual_id])
            self.assertEqual(output["expected_evidence_ids"], all_evidence_ids)
            self.assertEqual(output["sha256"], sha256(corpus_bytes).hexdigest())
        finally:
            db.close()

    def test_exact_checker_trips_on_omission_duplication_and_substitution(self) -> None:
        with self.assertRaises(RenderDifferentialError):
            assert_export_output_exact([], [(1, None, "release")], [])
        with self.assertRaises(RenderDifferentialError):
            assert_export_output_exact(
                [
                    {"id": 1, "is_candidate": True},
                    {"id": 1, "is_candidate": False},
                ],
                [(1, None, "release")],
                [],
            )
        with self.assertRaises(RenderDifferentialError):
            assert_export_output_exact(
                [{"id": 2, "is_candidate": True}], [(1, None, "release")], []
            )
