"""Deterministic pins for the live-corpus DECISION differential harness.

``scripts/decision_differential.py`` is the instrument PR3 owes: it
re-decides real persisted ``album_quality_evidence`` rows through the real
decider so a quality-policy change is MEASURED on the live corpus rather
than reasoned about from the diff (``.claude/rules/test-fidelity.md``
Rule D, one layer down from derived text).

An instrument that can under-report is worse than none, so these pins are
mostly about the ways this one could lie:

* it must decode the corpus with PRODUCTION's decoder;
* it must compare every decision key the decider emits, so a new key
  cannot silently fall out of the watched set;
* it must record a refused row rather than dropping it, so the
  denominator stays honest;
* it must actually SEE the change PR3 makes — a differential that reports
  zero on a world the gate demonstrably moves is the failure mode.
"""

from __future__ import annotations

import io
import json
import os
import sys
import unittest
from contextlib import redirect_stdout
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import msgspec

from lib.quality import (
    VERIFIED_LOSSLESS_CLASSIFIER,
    VERIFIED_LOSSLESS_CLASSIFIER_V3,
    full_pipeline_decision_from_evidence,
)
from scripts.decision_differential import (
    DECISION_ERROR_FIELD,
    DECISION_KEYS,
    PROOF_FIELDS,
    RenderDifferentialError,
    decide_corpus,
    decide_row,
    leg_for_evidence,
    main,
)


def _corpus_row(**overrides: object) -> dict[str, object]:
    """A live-shaped ``album_quality_evidence`` row.

    Column names and types mirror the real read (``lib/pipeline_db/
    evidence.py``): ``measured_at`` is a timestamp, ``files`` carries the
    joined snapshot rows, and every nullable column is present.
    """
    row: dict[str, object] = {
        "id": 1,
        "mb_release_id": "mbid-decision-differential",
        "snapshot_fingerprint": "sha256:decision-differential",
        "source_path": "/Incoming/auto-import/album",
        "measured_at": datetime(2026, 7, 31, tzinfo=UTC).isoformat(),
        "min_bitrate_kbps": 900,
        "avg_bitrate_kbps": 900,
        "median_bitrate_kbps": 900,
        "format": "FLAC",
        "is_cbr": False,
        "spectral_grade": "genuine",
        "spectral_bitrate_kbps": None,
        "spectral_subject": "source",
        "spectral_provenance": "measured",
        "was_converted_from": None,
        "cliff_hz": None,
        "codec_family": "lossless",
        "ultrasonic_deficit_db": None,
        "spectral_measurement_version": 2,
        "codec": "flac",
        "container": "flac",
        "storage_format": "flac",
        "target_format": "opus 128",
        "target_is_cbr": False,
        "lineage_version": 4,
        "v0_min_bitrate_kbps": 245,
        "v0_avg_bitrate_kbps": 245,
        "v0_median_bitrate_kbps": 245,
        "v0_subject": "source",
        "v0_provenance": "measured",
        "on_disk_v0_research_attempted": False,
        "current_enrichment_required": False,
        "verified_lossless": False,
        "verified_lossless_provenance": None,
        "verified_lossless_source": None,
        "verified_lossless_classifier": None,
        "verified_lossless_detail": None,
        "audio_validation": {"outcome": "legacy_unrecorded"},
        "audio_corrupt": False,
        "audio_error": None,
        "folder_layout": "flat",
        "audio_file_count": 1,
        "filetype_band": "flac",
        "matched_bad_audio_hash_id": None,
        "matched_bad_audio_hash_path": None,
        "files": [{
            "relative_path": "01.flac",
            "size_bytes": 1,
            "mtime_ns": 1,
            "extension": "flac",
            "container": "flac",
            "codec": "flac",
            "decode_ok": True,
        }],
    }
    row.update(overrides)
    return row


class TestDecideRow(unittest.TestCase):
    """One corpus row through the real decider."""

    def test_the_decoder_is_productions_own(self):
        """The corpus round-trips into exactly the evidence row the
        importer would read — pinned by deciding the SAME album two ways
        and requiring the identical outcome."""
        from scripts.decision_differential import _evidence_from_corpus_row

        evidence = _evidence_from_corpus_row(_corpus_row())
        direct = full_pipeline_decision_from_evidence(evidence)
        decided = decide_row(_corpus_row())
        for key, value in direct.items():
            if key in ("comparison_basis", "stage2_import_if_stage1_deferred",
                       "comparison_basis_if_stage1_deferred"):
                continue
            self.assertEqual(decided.fields[key], value, key)

    def test_every_decision_key_is_watched(self):
        """The watched set is DERIVED from the decider's own output. A new
        decision key must appear automatically; only the two audit-only
        keys and the nested display payload are excluded, by name."""
        from scripts.decision_differential import _evidence_from_corpus_row

        evidence = _evidence_from_corpus_row(_corpus_row())
        emitted = set(full_pipeline_decision_from_evidence(evidence))
        watched = set(decide_row(_corpus_row()).fields)
        expected_excluded = {
            "comparison_basis",
            "stage2_import_if_stage1_deferred",
            "comparison_basis_if_stage1_deferred",
        }
        self.assertEqual(emitted - watched, expected_excluded)
        self.assertEqual(
            watched - emitted,
            {DECISION_ERROR_FIELD, *PROOF_FIELDS},
        )

    def test_a_refused_row_is_recorded_not_dropped(self):
        """An evidence row the decider refuses raises in production too.
        Recording it keeps the denominator honest; dropping it would
        flatter every differential run."""
        decided = decide_row(_corpus_row(source_path=""))
        self.assertIsInstance(decided.fields[DECISION_ERROR_FIELD], str)
        self.assertIn(
            "source_path", str(decided.fields[DECISION_ERROR_FIELD]),
        )

    def test_a_refused_row_carries_the_same_field_set(self):
        """The diff engine compares rows field by field and fails closed
        on a side whose rows disagree about their fields. A refused row
        that emitted only its error would make every corpus containing
        one uncomparable."""
        refused = decide_row(_corpus_row(source_path=""))
        decided = decide_row(_corpus_row())
        self.assertEqual(set(refused.fields), set(decided.fields))
        for key in DECISION_KEYS:
            self.assertIsNone(refused.fields[key], key)

    def test_a_corpus_row_without_an_id_fails_closed(self):
        with self.assertRaises(RenderDifferentialError):
            decide_row(_corpus_row(id=None))

    def test_a_malformed_row_fails_closed(self):
        with self.assertRaises(RenderDifferentialError):
            decide_row(_corpus_row(files="not-a-list"))

    def test_the_leg_is_the_one_the_decision_saw(self):
        """``leg_for_evidence`` goes through the decider's own context
        adapter, so the reported leg cannot describe a different world
        from the decision beside it."""
        from scripts.decision_differential import _evidence_from_corpus_row

        evidence = _evidence_from_corpus_row(
            _corpus_row(ultrasonic_deficit_db=65.16),
        )
        leg = leg_for_evidence(evidence)
        self.assertEqual(leg.outcome, "denied")
        self.assertEqual(leg.deficit_db, 65.16)


class TestDecideCorpusSeesTheProofGateChange(unittest.TestCase):
    """The instrument's own qualification: it must MOVE on a world the
    gate demonstrably moves, and report the movement by field.

    A differential that reports zero on a change it was built to measure
    is exactly the failure `.claude/rules/test-fidelity.md` Rule D's
    "read the zeros, but only after checking what produced them" warns
    about.
    """

    def _decide_all(self, rows):
        return {
            row["id"]: decide_row(row).fields for row in rows
        }

    def test_the_launder_world_moves_and_the_legacy_world_does_not(self):
        decided = self._decide_all([
            # Legacy: no ultrasonic evidence. This is where most of the
            # library lives and it must not move.
            _corpus_row(
                id=1, ultrasonic_deficit_db=None,
                spectral_measurement_version=None,
            ),
            # A genuine control comfortably under the threshold.
            _corpus_row(id=2, ultrasonic_deficit_db=45.0),
            # A launder deficit, sox-native: the gate's whole purpose.
            _corpus_row(id=3, ultrasonic_deficit_db=65.16),
            # The same deficit through the ffmpeg decode path: a
            # different instrument, so no gate.
            _corpus_row(
                id=4, ultrasonic_deficit_db=65.16,
                codec="alac", container="m4a", storage_format="alac",
                format="ALAC", filetype_band="m4a",
                files=[{
                    "relative_path": "01.m4a", "size_bytes": 1, "mtime_ns": 1,
                    "extension": "m4a", "container": "m4a", "codec": "alac",
                    "decode_ok": True,
                }],
            ),
        ])
        self.assertTrue(decided[1]["verified_lossless"])
        self.assertEqual(
            decided[1]["verified_lossless_classifier"],
            VERIFIED_LOSSLESS_CLASSIFIER,
        )
        self.assertEqual(decided[1]["ultrasonic_leg_reason"],
                         "legacy_measurement")

        self.assertTrue(decided[2]["verified_lossless"])
        self.assertEqual(
            decided[2]["verified_lossless_classifier"],
            VERIFIED_LOSSLESS_CLASSIFIER_V3,
        )

        self.assertFalse(decided[3]["verified_lossless"])
        self.assertIsNone(decided[3]["verified_lossless_classifier"])
        self.assertEqual(decided[3]["ultrasonic_leg_outcome"], "denied")

        self.assertTrue(decided[4]["verified_lossless"])
        self.assertEqual(decided[4]["ultrasonic_leg_reason"],
                         "uncalibrated_decode_path")


class TestCli(unittest.TestCase):
    """The two-tree runbook's two commands, end to end."""

    def _write_corpus(self, path: Path, rows) -> None:
        path.write_text(
            "".join(json.dumps(row) + "\n" for row in rows),
            encoding="utf-8",
        )

    def test_decide_then_diff_reports_the_moved_field(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            base_corpus = root / "base.jsonl"
            current_corpus = root / "current.jsonl"
            # Same album id, one carrying the launder deficit: standing in
            # for the same corpus decided by two trees.
            self._write_corpus(base_corpus, [
                _corpus_row(id=7, ultrasonic_deficit_db=None,
                            spectral_measurement_version=None),
            ])
            self._write_corpus(current_corpus, [
                _corpus_row(id=7, ultrasonic_deficit_db=65.16),
            ])
            base_out = root / "base-decided.jsonl"
            current_out = root / "current-decided.jsonl"
            self.assertEqual(
                main(["decide", "--corpus", str(base_corpus),
                      "--out", str(base_out)]), 0)
            self.assertEqual(
                main(["decide", "--corpus", str(current_corpus),
                      "--out", str(current_out)]), 0)

            buffer = io.StringIO()
            with redirect_stdout(buffer):
                exit_code = main([
                    "diff", "--base", str(base_out),
                    "--current", str(current_out), "--json",
                ])
            self.assertEqual(exit_code, 0)
            report = msgspec.json.decode(buffer.getvalue(), type=dict)
            self.assertEqual(report["total_rows"], 1)
            self.assertEqual(report["changed_rows"], 1)
            changed = {
                name for name, count in report["changed_by_field"].items()
                if count
            }
            self.assertIn("verified_lossless", changed)
            self.assertIn("ultrasonic_leg_reason", changed)
            # Zeros are evidence too: every field is reported.
            self.assertIn("final_status", report["changed_by_field"])

    def test_decide_writes_one_row_per_corpus_row(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            corpus = root / "corpus.jsonl"
            self._write_corpus(corpus, [
                _corpus_row(id=1), _corpus_row(id=2), _corpus_row(id=3),
            ])
            out = root / "decided.jsonl"
            self.assertEqual(decide_corpus(str(corpus), str(out)), 3)
            self.assertEqual(
                len(out.read_text(encoding="utf-8").strip().splitlines()), 3,
            )

    def test_a_bad_corpus_exits_nonzero_without_a_traceback(self):
        with TemporaryDirectory() as tmp:
            corpus = Path(tmp) / "corpus.jsonl"
            corpus.write_text("not json\n", encoding="utf-8")
            self.assertEqual(
                main(["decide", "--corpus", str(corpus)]), 1,
            )


if __name__ == "__main__":
    unittest.main()
