"""Real-PostgreSQL pins for strict album-quality evidence decoding (#999)."""

from __future__ import annotations

import json
import unittest
from collections.abc import Callable
from copy import deepcopy

import msgspec

from lib.pipeline_db.evidence import (
    EVIDENCE_FILE_PROJECTION_COLUMNS,
    EVIDENCE_PROJECTION_COLUMNS,
    _EvidenceMixin,
    _typed_evidence_rows_from_pg,
)
from lib.quality import (
    AacLatticeCapture,
    AacLatticeTrackScore,
    AlbumQualityEvidenceFile,
    AudioToolDiagnostic,
    AudioValidationReport,
)
from tests.evidence_helpers import make_album_quality_evidence
from tests.test_pipeline_db import make_db, requires_postgres

RawMutator = Callable[[dict[str, object], list[dict[str, object]]], None]


def assert_raw_pg_decoder_rejects(
    row: dict[str, object],
    files: list[dict[str, object]],
) -> None:
    """Invariant checker for a malformed shared raw-PG decoder input."""
    try:
        _EvidenceMixin._album_quality_evidence_from_row(row, files)
    except msgspec.ValidationError:
        return
    raise AssertionError("raw PG evidence decoder accepted malformed input")


@requires_postgres
class TestStrictEvidenceDecoder(unittest.TestCase):
    """Production reads and corpus replay share the same exact row decoder."""

    def setUp(self) -> None:
        self.db = make_db()

    def tearDown(self) -> None:
        self.db.close()

    def _stored(self):
        report = AudioValidationReport(
            outcome="audio_corrupt",
            files_checked=1,
            files_failed=1,
            diagnostics=[
                AudioToolDiagnostic(
                    relative_path="01.m4a",
                    category="decode_error",
                    return_code=69,
                    stderr_excerpt="bad frame",
                    stderr_bytes=9,
                    stderr_sha256="a" * 64,
                    stderr_truncated=False,
                )
            ],
        )
        capture = AacLatticeCapture.from_tracks([
            AacLatticeTrackScore(
                filename="01.m4a",
                offset=512,
                z=3.0,
                proba=0.5,
            )
        ])
        evidence = make_album_quality_evidence(
            mb_release_id="strict-decoder-mbid",
            files=[
                AlbumQualityEvidenceFile(
                    relative_path="01.m4a",
                    size_bytes=123,
                    mtime_ns=456,
                    extension="m4a",
                    container="mp4",
                    codec="aac",
                    decode_ok=False,
                )
            ],
            audio_corrupt=True,
            audio_error="01.m4a: bad frame",
            audio_validation=report,
            aac_lattice=capture,
        )
        self.db.upsert_album_quality_evidence(evidence)
        loaded = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert loaded is not None and loaded.id is not None
        return evidence, loaded

    def _raw_rows(
        self,
        evidence_id: int,
    ) -> tuple[dict[str, object], list[dict[str, object]]]:
        row = self.db._execute(
            "SELECT "
            + ", ".join(EVIDENCE_PROJECTION_COLUMNS)
            + " FROM album_quality_evidence WHERE id = %s",
            (evidence_id,),
        ).fetchone()
        assert row is not None
        files = self.db._execute(
            "SELECT "
            + ", ".join(EVIDENCE_FILE_PROJECTION_COLUMNS)
            + " FROM album_quality_evidence_files WHERE evidence_id = %s",
            (evidence_id,),
        ).fetchall()
        return dict(row), [dict(file) for file in files]

    def test_clean_real_pg_round_trip_is_typed_before_semantic_construction(self):
        """The two live reads and replay boundary preserve one clean row."""
        expected, loaded = self._stored()
        assert loaded.id is not None
        raw, files = self._raw_rows(loaded.id)

        persisted, persisted_files = _typed_evidence_rows_from_pg(raw, files)
        semantic = _EvidenceMixin._album_quality_evidence_from_persisted_rows(
            persisted,
            persisted_files,
        )
        replayed = _EvidenceMixin._album_quality_evidence_from_row(raw, files)
        by_id = self.db.load_album_quality_evidence_by_id(loaded.id)

        self.assertEqual(
            loaded,
            msgspec.structs.replace(
                expected.sorted_for_storage(),
                id=loaded.id,
                measured_at=loaded.measured_at,
            ),
        )
        self.assertEqual(replayed, loaded)
        self.assertEqual(by_id, loaded)
        self.assertIs(semantic.audio_validation, persisted.audio_validation)
        self.assertEqual(
            replayed.aac_lattice.tracks if replayed.aac_lattice else None,
            persisted.aac_lattice_tracks,
        )
        self.assertEqual(
            [file.relative_path for file in persisted_files],
            [file.relative_path for file in replayed.files],
        )

    def test_live_pg_unknown_aac_track_key_is_not_silently_dropped(self):
        """The production ``find`` path rejects the reviewer-reported defect."""
        _expected, loaded = self._stored()
        assert loaded.id is not None
        self.db._execute(
            "UPDATE album_quality_evidence SET aac_lattice_tracks = %s::jsonb "
            "WHERE id = %s",
            (
                json.dumps([
                    {
                        "filename": "01.m4a",
                        "offset": 512,
                        "z": 3.0,
                        "proba": 0.5,
                        "error": None,
                        "reviewer_unknown_key": True,
                    }
                ]),
                loaded.id,
            ),
        )
        self.db.conn.commit()

        with self.assertRaises(msgspec.ValidationError):
            self.db.find_album_quality_evidence(
                mb_release_id=loaded.mb_release_id,
                snapshot_fingerprint=loaded.snapshot_fingerprint,
            )

    def test_raw_pg_boundary_rejects_nested_unknowns_scalars_and_nullability(self):
        """Every nested persisted grammar fails closed before semantic use."""
        _expected, loaded = self._stored()
        assert loaded.id is not None
        raw, files = self._raw_rows(loaded.id)

        def unknown_aac(row: dict[str, object], _files: list[dict[str, object]]) -> None:
            tracks = row["aac_lattice_tracks"]
            assert isinstance(tracks, list) and isinstance(tracks[0], dict)
            tracks[0]["unknown"] = True

        def unknown_audio(row: dict[str, object], _files: list[dict[str, object]]) -> None:
            audio = row["audio_validation"]
            assert isinstance(audio, dict)
            audio["unknown"] = True

        def unknown_diagnostic(row: dict[str, object], _files: list[dict[str, object]]) -> None:
            audio = row["audio_validation"]
            assert isinstance(audio, dict)
            diagnostics = audio["diagnostics"]
            assert isinstance(diagnostics, list) and isinstance(diagnostics[0], dict)
            diagnostics[0]["unknown"] = True

        def unknown_file(_row: dict[str, object], raw_files: list[dict[str, object]]) -> None:
            raw_files[0]["unknown"] = True

        def wrong_bool(row: dict[str, object], _files: list[dict[str, object]]) -> None:
            row["is_cbr"] = "false"

        def null_required(row: dict[str, object], _files: list[dict[str, object]]) -> None:
            row["id"] = None

        cases: tuple[tuple[str, RawMutator], ...] = (
            ("unknown AAC track key", unknown_aac),
            ("unknown audio report key", unknown_audio),
            ("unknown audio diagnostic key", unknown_diagnostic),
            ("unknown evidence file key", unknown_file),
            ("wrong scalar", wrong_bool),
            ("wrong nullability", null_required),
        )
        for label, mutate in cases:
            with self.subTest(label=label):
                bad_row, bad_files = deepcopy(raw), deepcopy(files)
                mutate(bad_row, bad_files)
                with self.assertRaises(msgspec.ValidationError):
                    _EvidenceMixin._album_quality_evidence_from_row(
                        bad_row,
                        bad_files,
                    )
