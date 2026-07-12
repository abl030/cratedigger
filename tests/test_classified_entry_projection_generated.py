"""Pin + generated property for ClassifiedEntry outbound projection.

Invariant: every ``ClassifiedEntry`` field is present in both the pipeline-log
payload and the typed download-history detail row. The HTTP route and detail
builder are the production entry points; ``FakePipelineDB`` supplies state.
"""

from __future__ import annotations

from collections.abc import Mapping
import unittest

import msgspec
from hypothesis import example, given, strategies as st

from lib.quality import ImportResult, ValidationResult
import tests._hypothesis_profiles  # noqa: F401
from tests.helpers import make_request_row
from tests.web._harness import _FakeDbWebServerCase
from web.classify import ClassifiedEntry
from web.download_history_view import build_download_history_row


def _classified_entry_fields() -> set[str]:
    return {field.name for field in msgspec.structs.fields(ClassifiedEntry)}


def assert_classified_fields_forwarded(
    log_payload: Mapping[str, object],
    detail_payload: Mapping[str, object],
) -> None:
    """Assert both outbound surfaces carry the complete classifier contract."""
    expected = _classified_entry_fields()
    missing_log = expected - log_payload.keys()
    missing_detail = expected - detail_payload.keys()
    if missing_log or missing_detail:
        raise AssertionError(
            "ClassifiedEntry projection drift: "
            f"pipeline_log_missing={sorted(missing_log)} "
            f"download_history_missing={sorted(missing_detail)}"
        )


def assert_mixed_source_projection_is_honest(
    payload: Mapping[str, object],
    expected_formats: tuple[str, str],
) -> None:
    """The terminal reject reason and every measured codec stay visible."""
    expected_verdict = "Mixed lossless+lossy source"
    if payload.get("verdict") != expected_verdict:
        raise AssertionError(
            "mixed-source verdict drift: "
            f"expected={expected_verdict!r} actual={payload.get('verdict')!r}"
        )
    label = str(payload.get("downloaded_label") or "")
    missing = [fmt.upper() for fmt in expected_formats if fmt.upper() not in label]
    if missing:
        raise AssertionError(
            "mixed-source codec projection dropped formats: "
            f"missing={missing!r} label={label!r}"
        )


class _ClassifiedEntryProjectionHarness(_FakeDbWebServerCase):
    def outbound_surfaces(
        self,
        *,
        outcome: str,
        filetype: str | None,
        bitrate: int | None,
        username: str | None,
    ) -> tuple[dict[str, object], dict[str, object]]:
        # Hypothesis reuses one unittest fixture across examples. Exercise the
        # fake's real cascade and reseed so every world has exactly one source
        # row; no assertion relies on accumulated insertion order.
        self.db.delete_request(598)
        self.db.seed_request(make_request_row(
            id=598,
            status="imported",
            mb_release_id="classified-entry-projection",
        ))
        self.db.log_download(
            598,
            outcome=outcome,
            filetype=filetype,
            bitrate=bitrate,
            soulseek_username=username,
        )
        raw_row = self.db.get_log(limit=1)[0]
        detail = msgspec.to_builtins(build_download_history_row(raw_row))
        status, response = self._get("/api/pipeline/log?limit=1")
        self.assertEqual(status, 200)
        return response["log"][0], detail

    def mixed_source_payload(
        self,
        *,
        lossless_format: str,
        lossy_format: str,
        validation_scenario: str,
    ) -> dict[str, object]:
        self.db.delete_request(598)
        self.db.seed_request(make_request_row(
            id=598,
            status="wanted",
            mb_release_id="classified-entry-mixed-source",
        ))
        self.db.log_download(
            598,
            outcome="rejected",
            soulseek_username="generated-peer",
            filetype=f"{lossless_format}, {lossy_format}",
            actual_filetype=f"{lossless_format}, {lossy_format}",
            actual_min_bitrate=224,
            import_result=ImportResult(decision="mixed_source").to_json(),
            validation_result=ValidationResult(
                valid=True,
                distance=0.0928,
                scenario=validation_scenario,
            ).to_json(),
        )
        status, response = self._get("/api/pipeline/log?limit=1")
        self.assertEqual(status, 200)
        return response["log"][0]


class TestClassifiedEntryProjectionPin(_ClassifiedEntryProjectionHarness):
    def test_every_classifier_field_reaches_both_outbound_surfaces(self) -> None:
        log_payload, detail_payload = self.outbound_surfaces(
            outcome="success",
            filetype="mp3",
            bitrate=320_000,
            username="archivist",
        )
        assert_classified_fields_forwarded(log_payload, detail_payload)

    def test_classified_entry_is_a_strict_msgspec_wire_type(self) -> None:
        self.assertTrue(issubclass(ClassifiedEntry, msgspec.Struct))
        with self.assertRaises(msgspec.ValidationError):
            msgspec.convert(
                {
                    "badge": 42,
                    "badge_class": "badge-new",
                    "border_color": "#1a4a2a",
                    "verdict": "Imported",
                    "summary": "Imported",
                },
                type=ClassifiedEntry,
            )

    def test_slow_club_mixed_source_projection(self) -> None:
        payload = self.mixed_source_payload(
            lossless_format="flac",
            lossy_format="ogg",
            validation_scenario="strong_match",
        )
        assert_mixed_source_projection_is_honest(payload, ("flac", "ogg"))


class TestGeneratedClassifiedEntryProjection(_ClassifiedEntryProjectionHarness):
    @given(
        outcome=st.sampled_from([
            "success", "rejected", "failed", "timeout", "force_import",
            "curator_ban", "user_offline", "measurement_failed",
        ]),
        filetype=st.one_of(st.none(), st.sampled_from(["mp3", "flac", "m4a"])),
        bitrate=st.one_of(st.none(), st.integers(min_value=1, max_value=2_000_000)),
        username=st.one_of(st.none(), st.text(min_size=1, max_size=20)),
    )
    @example(outcome="success", filetype="mp3", bitrate=320_000, username="peer")
    @example(outcome="timeout", filetype=None, bitrate=None, username=None)
    def test_projection_is_total_across_log_entry_worlds(
        self,
        outcome: str,
        filetype: str | None,
        bitrate: int | None,
        username: str | None,
    ) -> None:
        log_payload, detail_payload = self.outbound_surfaces(
            outcome=outcome,
            filetype=filetype,
            bitrate=bitrate,
            username=username,
        )
        assert_classified_fields_forwarded(log_payload, detail_payload)

    @given(
        lossless_format=st.sampled_from(["flac", "alac", "wav", "aiff", "ape"]),
        lossy_format=st.sampled_from(["mp3", "aac", "m4a", "ogg", "opus", "wma"]),
        validation_scenario=st.sampled_from([
            "strong_match", "medium_match", "validation_passed",
        ]),
    )
    @example(
        lossless_format="flac",
        lossy_format="ogg",
        validation_scenario="strong_match",
    )
    def test_terminal_mixed_source_decision_drives_every_projection_world(
        self,
        lossless_format: str,
        lossy_format: str,
        validation_scenario: str,
    ) -> None:
        payload = self.mixed_source_payload(
            lossless_format=lossless_format,
            lossy_format=lossy_format,
            validation_scenario=validation_scenario,
        )
        assert_mixed_source_projection_is_honest(
            payload,
            (lossless_format, lossy_format),
        )


class TestClassifiedEntryProjectionCheckerTripsOnViolations(unittest.TestCase):
    def test_checker_rejects_a_missing_log_field(self) -> None:
        complete = {name: None for name in _classified_entry_fields()}
        bad_log = dict(complete)
        bad_log.pop("downloaded_label")
        with self.assertRaisesRegex(AssertionError, "downloaded_label"):
            assert_classified_fields_forwarded(bad_log, complete)

    def test_checker_rejects_a_missing_detail_field(self) -> None:
        complete = {name: None for name in _classified_entry_fields()}
        bad_detail = dict(complete)
        bad_detail.pop("summary")
        with self.assertRaisesRegex(AssertionError, "summary"):
            assert_classified_fields_forwarded(complete, bad_detail)

    def test_mixed_source_checker_rejects_validation_scenario_as_verdict(self) -> None:
        with self.assertRaisesRegex(AssertionError, "verdict drift"):
            assert_mixed_source_projection_is_honest(
                {
                    "verdict": "strong_match",
                    "downloaded_label": "FLAC + OGG",
                },
                ("flac", "ogg"),
            )

    def test_mixed_source_checker_rejects_a_dropped_codec(self) -> None:
        with self.assertRaisesRegex(AssertionError, "dropped formats"):
            assert_mixed_source_projection_is_honest(
                {
                    "verdict": "Mixed lossless+lossy source",
                    "downloaded_label": "FLAC",
                },
                ("flac", "ogg"),
            )


if __name__ == "__main__":
    unittest.main()
