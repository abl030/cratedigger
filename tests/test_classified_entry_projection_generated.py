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


if __name__ == "__main__":
    unittest.main()
