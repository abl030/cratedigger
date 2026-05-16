#!/usr/bin/env python3
"""Tests for the MeasurementFailure wire-boundary Struct (U4).

These cover the typed payload that preview emits when it cannot produce
evidence — the third leg of the evidence/decision-boundary refactor. The
Struct crosses JSONB (`import_jobs.preview_result`, `download_log.validation_result`)
and the Recents UI grep-classifies on its `reason` tag.

Per .claude/rules/code-quality.md § "Wire-boundary types", this module owes:
  - A round-trip test: encode then decode via msgspec returns an identical
    Struct.
  - A RED test that feeds an invalid taxonomy value to msgspec.convert and
    asserts msgspec.ValidationError. This is the detector that catches
    producer/consumer drift.
"""

import os
import sys
import unittest

import msgspec

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.quality import MeasurementFailure, MeasurementFailureReason  # noqa: E402


class TestMeasurementFailureStruct(unittest.TestCase):
    """MeasurementFailure is a frozen msgspec.Struct with a strict Literal
    taxonomy on .reason and free-text .detail / .source_path."""

    def test_is_msgspec_struct(self) -> None:
        self.assertTrue(issubclass(MeasurementFailure, msgspec.Struct))

    def test_construct_all_fields(self) -> None:
        f = MeasurementFailure(
            reason="source_vanished",
            detail="ffmpeg ENOENT on /tmp/foo/track01.flac",
            source_path="/tmp/foo",
        )
        self.assertEqual(f.reason, "source_vanished")
        self.assertEqual(f.detail, "ffmpeg ENOENT on /tmp/foo/track01.flac")
        self.assertEqual(f.source_path, "/tmp/foo")

    def test_source_path_default(self) -> None:
        """source_path defaults to '' for failures that happen before any
        path was resolved (e.g. request_not_found)."""
        f = MeasurementFailure(reason="request_not_found", detail="gone")
        self.assertEqual(f.source_path, "")

    def test_frozen(self) -> None:
        """The Struct is frozen — mutation raises."""
        f = MeasurementFailure(reason="snapshot_stale", detail="x")
        with self.assertRaises(AttributeError):
            f.detail = "y"  # type: ignore[misc]


class TestMeasurementFailureRoundTrip(unittest.TestCase):
    """msgspec.json.encode → msgspec.convert symmetric round-trip."""

    def test_round_trip_full(self) -> None:
        original = MeasurementFailure(
            reason="snapshot_stale",
            detail="folder changed after retry",
            source_path="/Incoming/auto-import/Album",
        )
        encoded = msgspec.json.encode(original)
        decoded_dict = msgspec.json.decode(encoded)
        restored = msgspec.convert(decoded_dict, type=MeasurementFailure)
        self.assertEqual(restored, original)

    def test_round_trip_default_source_path(self) -> None:
        original = MeasurementFailure(
            reason="request_not_found",
            detail="album_request 42 not found",
        )
        decoded_dict = msgspec.json.decode(msgspec.json.encode(original))
        restored = msgspec.convert(decoded_dict, type=MeasurementFailure)
        self.assertEqual(restored, original)
        self.assertEqual(restored.source_path, "")

    def test_round_trip_to_builtins(self) -> None:
        """msgspec.to_builtins yields a dict suitable for psycopg2 Json."""
        f = MeasurementFailure(
            reason="materialization_error",
            detail="shutil.copytree failed: No space left",
            source_path="/tmp/measure-12",
        )
        payload = msgspec.to_builtins(f)
        self.assertEqual(payload, {
            "reason": "materialization_error",
            "detail": "shutil.copytree failed: No space left",
            "source_path": "/tmp/measure-12",
        })
        restored = msgspec.convert(payload, type=MeasurementFailure)
        self.assertEqual(restored, f)


class TestMeasurementFailureTaxonomyValidation(unittest.TestCase):
    """Wire-boundary RED tests — invalid taxonomy values raise at decode."""

    def test_unknown_reason_rejected(self) -> None:
        """The Literal[...] taxonomy is the contract. Anything outside it
        is a producer/consumer drift bug — msgspec.convert must catch it.

        This is the RED test that justifies the strict-typed Struct: a dict
        with reason='not_a_real_enum_value' would silently round-trip
        through json.loads + dict.get() but blow up here.
        """
        with self.assertRaises(msgspec.ValidationError):
            msgspec.convert(
                {"reason": "not_a_real_enum_value", "detail": "x",
                 "source_path": "/y"},
                type=MeasurementFailure,
            )

    def test_typo_in_reason_rejected(self) -> None:
        """A near-miss typo (real reasons are
        snapshot_stale/source_vanished/etc.) is also rejected."""
        with self.assertRaises(msgspec.ValidationError):
            msgspec.convert(
                {"reason": "snapshot_state", "detail": "x"},  # typo
                type=MeasurementFailure,
            )

    def test_int_reason_rejected(self) -> None:
        """reason must be str, not int — guards against accidental
        enum-int coercion at the wire."""
        with self.assertRaises(msgspec.ValidationError):
            msgspec.convert(
                {"reason": 1, "detail": "x"},
                type=MeasurementFailure,
            )

    def test_int_detail_rejected(self) -> None:
        with self.assertRaises(msgspec.ValidationError):
            msgspec.convert(
                {"reason": "source_vanished", "detail": 12345},
                type=MeasurementFailure,
            )

    def test_missing_required_field_rejected(self) -> None:
        with self.assertRaises(msgspec.ValidationError):
            msgspec.convert(
                {"detail": "no reason supplied"},
                type=MeasurementFailure,
            )

    def test_all_taxonomy_values_decode(self) -> None:
        """Every documented reason in MeasurementFailureReason decodes
        cleanly. If we ever drift the Literal vs the inline list here,
        this test flips RED and the operator must reconcile."""
        documented_reasons = [
            "snapshot_stale",
            "source_vanished",
            "materialization_error",
            "measurement_crashed",
            "evidence_persist_failed",
            "request_not_found",
            "missing_release_id",
            "download_log_not_found",
            "missing_failed_path",
        ]
        for reason in documented_reasons:
            with self.subTest(reason=reason):
                restored = msgspec.convert(
                    {"reason": reason, "detail": "x"},
                    type=MeasurementFailure,
                )
                self.assertEqual(restored.reason, reason)


if __name__ == "__main__":
    unittest.main()
