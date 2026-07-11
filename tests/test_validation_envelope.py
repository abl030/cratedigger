"""Tests for the typed view over download_log.validation_result JSONB (#410)."""

from __future__ import annotations

import json
import unittest

import msgspec

from lib.quality import ValidationResult
from lib.validation_envelope import (
    FAILED_PATH_KEY,
    SCENARIO_KEY,
    WRONG_MATCH_TRIAGE_KEY,
    ValidationResultEnvelope,
    WrongMatchTriageAudit,
    decode_validation_envelope,
    derive_validation_log_columns,
)


class TestDecodeValidationEnvelope(unittest.TestCase):

    def test_none_decodes_to_empty_envelope(self) -> None:
        env = decode_validation_envelope(None)
        self.assertIsNone(env.failed_path)
        self.assertEqual(env.source_dirs, [])
        self.assertIsNone(env.wrong_match_triage)

    def test_dict_decodes_declared_keys(self) -> None:
        env = decode_validation_envelope({
            "valid": False,
            "scenario": "wrong_match",
            "distance": 0.42,
            "failed_path": "/mnt/x/failed_imports/Album",
            "source_dirs": ["a", "b"],
            "items": [{"path": "01.mp3"}],
            "candidates": [{"album_id": "mbid-1", "is_target": True}],
        })
        self.assertEqual(env.scenario, "wrong_match")
        self.assertEqual(env.distance, 0.42)
        self.assertEqual(env.failed_path, "/mnt/x/failed_imports/Album")
        self.assertEqual(env.source_dirs, ["a", "b"])
        self.assertEqual(env.items, [{"path": "01.mp3"}])
        self.assertEqual(env.candidates[0]["is_target"], True)

    def test_json_string_decodes(self) -> None:
        env = decode_validation_envelope('{"failed_path": "/p", "distance": 0.1}')
        self.assertEqual(env.failed_path, "/p")
        self.assertEqual(env.distance, 0.1)

    def test_unknown_keys_are_ignored_curator_ban_shape(self) -> None:
        env = decode_validation_envelope({
            "scenario": "curator_ban",
            "hashes_recorded": 12,
            "denylisted_username": "peer",
            "reason": "bad rip",
            "cleanup_errors": [],
        })
        self.assertEqual(env.scenario, "curator_ban")
        self.assertIsNone(env.failed_path)
        self.assertIsNone(env.wrong_match_triage)

    def test_triage_decodes_typed_and_tolerates_legacy_extra_keys(self) -> None:
        env = decode_validation_envelope({
            "failed_path": "/p",
            "wrong_match_triage": {
                "action": "kept_uncertain",
                "outcome": "kept_uncertain",
                "success": True,
                "reason": "no_confident_reject",
                "preview_verdict": "uncertain",
                "preview_decision": "kept",
                "stage_chain": ["preview", "decide"],
                "cleared_rows": 2,
                # legacy writer keys observed in prod rows — must be ignored
                "confident_reject": False,
                "would_import": True,
                "uncertain": True,
                "preview_reason": "x",
                "cleanup": {},
            },
        })
        triage = env.wrong_match_triage
        assert triage is not None
        self.assertEqual(triage.action, "kept_uncertain")
        self.assertEqual(triage.stage_chain, ["preview", "decide"])
        self.assertEqual(triage.cleared_rows, 2)

    def test_wrong_typed_failed_path_raises(self) -> None:
        with self.assertRaises(msgspec.ValidationError):
            decode_validation_envelope({"failed_path": 123})

    def test_wrong_typed_source_dirs_raises(self) -> None:
        with self.assertRaises(msgspec.ValidationError):
            decode_validation_envelope({"source_dirs": "/not/a/list"})

    def test_wrong_typed_triage_raises(self) -> None:
        with self.assertRaises(msgspec.ValidationError):
            decode_validation_envelope({"wrong_match_triage": ["not", "a", "dict"]})

    def test_wrong_typed_stage_chain_raises(self) -> None:
        with self.assertRaises(msgspec.ValidationError):
            decode_validation_envelope(
                {"wrong_match_triage": {"stage_chain": [1, 2]}})

    def test_non_object_json_raises(self) -> None:
        with self.assertRaises(msgspec.ValidationError):
            decode_validation_envelope('["not", "an", "object"]')

    def test_projection_preserves_non_object_validation_errors(self) -> None:
        for raw in ('null', '"not an object"', '[]', b'null'):
            with self.subTest(raw=raw):
                with self.assertRaises(msgspec.ValidationError):
                    derive_validation_log_columns(raw)

    def test_projection_preserves_malformed_json_error_parity(self) -> None:
        for raw in ('{', b'{"distance":', b'\xff'):
            with self.subTest(raw=raw):
                with self.assertRaises(Exception) as decode_error:
                    decode_validation_envelope(raw)
                with self.assertRaises(Exception) as projection_error:
                    derive_validation_log_columns(raw)
                self.assertIs(
                    type(projection_error.exception),
                    type(decode_error.exception),
                )
                self.assertIn(
                    type(decode_error.exception),
                    (json.JSONDecodeError, UnicodeDecodeError),
                )


class TestEnvelopeContract(unittest.TestCase):
    """Drift guards binding the envelope to its producers."""

    def test_envelope_fields_are_validation_result_fields(self) -> None:
        """Every envelope key except the grafted triage key must exist on
        ValidationResult — the writer that produces the blob."""
        envelope_fields = set(ValidationResultEnvelope.__struct_fields__)
        envelope_fields.discard(WRONG_MATCH_TRIAGE_KEY)
        producer_fields = set(ValidationResult.__struct_fields__)
        self.assertLessEqual(envelope_fields, producer_fields)

    def test_sql_key_constants_are_envelope_fields(self) -> None:
        for key in (FAILED_PATH_KEY, SCENARIO_KEY, WRONG_MATCH_TRIAGE_KEY):
            self.assertIn(key, ValidationResultEnvelope.__struct_fields__)

    def test_triage_audit_round_trips_through_json(self) -> None:
        audit = WrongMatchTriageAudit(
            action="deleted_reject",
            outcome="deleted",
            success=True,
            reason="confident_reject",
            preview_verdict="reject",
            preview_decision="rejected_spectral",
            cleanup_eligible=True,
            source_path="/p",
            stage_chain=["preview", "spectral"],
            cleared_rows=1,
            deleted_path="/p",
        )
        wire = msgspec.json.encode(audit)
        env = decode_validation_envelope(
            {"wrong_match_triage": msgspec.json.decode(wire)})
        self.assertEqual(env.wrong_match_triage, audit)

    def test_triage_audit_encoding_omits_defaults(self) -> None:
        """Parity with the old conditional dict building — unset fields stay
        out of the JSONB instead of writing nulls."""
        wire = msgspec.json.decode(
            msgspec.json.encode(WrongMatchTriageAudit(action="deleted_reject")))
        self.assertEqual(wire, {"action": "deleted_reject"})


if __name__ == "__main__":
    unittest.main()
