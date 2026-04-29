#!/usr/bin/env python3
"""Unit tests for web/classify.py — recents tab classification.

Tests every scenario the pipeline can produce, ensuring each gets
the correct badge, verdict, and summary line.
"""

import os
import sys
import unittest
from dataclasses import replace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from web.classify import (classify_log_entry, quality_label, LogEntry,
                          ClassifiedEntry, _parse_import_result)
from lib.quality import (
    DuplicateRemoveCandidate,
    DuplicateRemoveGuardInfo,
    ImportResult,
    PostflightInfo,
)


# ---------------------------------------------------------------------------
# Helper to build a minimal LogEntry with sensible defaults
# ---------------------------------------------------------------------------

_DEFAULTS = LogEntry(
    id=1,
    request_id=100,
    outcome="success",
    beets_scenario="strong_match",
    beets_distance=0.012,
    soulseek_username="testuser",
    was_converted=False,
    actual_filetype="mp3",
    actual_min_bitrate=320,
    slskd_filetype="mp3",
    spectral_grade=None,
    spectral_bitrate=None,
    existing_min_bitrate=None,
    existing_spectral_bitrate=None,
    request_min_bitrate=320,
    search_filetype_override=None,
    request_status="imported",
    bitrate=320000,
    filetype="mp3",
)


def _entry(**overrides: object) -> LogEntry:
    """Build a LogEntry with sensible defaults, overridden as needed."""
    return replace(_DEFAULTS, **overrides)  # type: ignore[arg-type]


# ============================================================================
# LogEntry
# ============================================================================

class TestLogEntry(unittest.TestCase):

    def test_from_row_basic(self):
        """Construct from a dict (simulating psycopg2 row)."""
        row = {
            "id": 42, "request_id": 100, "outcome": "success",
            "beets_scenario": "strong_match", "beets_distance": 0.012,
            "soulseek_username": "testuser", "album_title": "Test Album",
            "artist_name": "Test Artist",
        }
        entry = LogEntry.from_row(row)
        self.assertEqual(entry.id, 42)
        self.assertEqual(entry.outcome, "success")
        self.assertEqual(entry.album_title, "Test Album")

    def test_from_row_missing_fields(self):
        """Missing fields get defaults, not KeyError."""
        row = {"id": 1, "outcome": "rejected"}
        entry = LogEntry.from_row(row)
        self.assertEqual(entry.id, 1)
        self.assertIsNone(entry.soulseek_username)
        self.assertEqual(entry.was_converted, False)
        self.assertEqual(entry.album_title, "")

    def test_from_row_datetime_serialized(self):
        """Datetime objects get serialized to ISO strings."""
        from datetime import datetime, timezone
        row = {"id": 1, "created_at": datetime(2026, 3, 30, 12, 0, 0, tzinfo=timezone.utc)}
        entry = LogEntry.from_row(row)
        self.assertIsInstance(entry.created_at, str)
        assert entry.created_at is not None
        self.assertIn("2026", entry.created_at)

    def test_to_json_dict(self):
        """to_json_dict returns a plain dict suitable for JSON."""
        entry = _entry(album_title="Test", artist_name="Artist")
        d = entry.to_json_dict()
        self.assertIsInstance(d, dict)
        self.assertEqual(d["album_title"], "Test")
        self.assertEqual(d["outcome"], "success")

    def test_to_json_dict_no_datetime_objects(self):
        """to_json_dict should not contain datetime objects."""
        from datetime import datetime, timezone
        entry = _entry()
        entry.created_at = "2026-03-30T12:00:00+00:00"
        d = entry.to_json_dict()
        for v in d.values():
            self.assertNotIsInstance(v, datetime)


# ============================================================================
# ClassifiedEntry
# ============================================================================

class TestClassifiedEntry(unittest.TestCase):

    def test_has_required_fields(self):
        c = ClassifiedEntry(badge="Imported", badge_class="badge-new",
                            border_color="#1a4a2a", verdict="MP3 320",
                            summary="MP3 320 · testuser")
        self.assertEqual(c.badge, "Imported")
        self.assertEqual(c.summary, "MP3 320 · testuser")

    def test_disambiguation_fields_default_none(self):
        # Clean rows (no post-import beet move, or clean move success) must
        # emit None so the frontend can conditionally render the chip.
        c = ClassifiedEntry(badge="Imported", badge_class="badge-new",
                            border_color="#1a4a2a", verdict="", summary="")
        self.assertIsNone(c.disambiguation_failure)
        self.assertIsNone(c.disambiguation_detail)
        self.assertEqual(c.bad_extensions, [])
        self.assertIsNone(c.wrong_match_triage_action)
        self.assertIsNone(c.wrong_match_triage_summary)
        self.assertEqual(c.wrong_match_triage_stage_chain, [])


# ============================================================================
# classify_log_entry — disambiguation_failure surface (#130)
# ============================================================================

class TestClassifyDisambiguationFailure(unittest.TestCase):
    """The ``PostflightInfo.disambiguation_failure`` field lands in
    ``download_log.import_result`` JSONB but has no UI surface yet.
    Classify must read it through and emit typed reason + detail so the
    Recents tab, ``pipeline-cli show``, and ``/debug-download`` can
    render a warning chip. See issue #130.
    """

    def _entry_with_disambig(self, reason: str, detail: str) -> LogEntry:
        return _entry(
            outcome="success",
            import_result={
                "version": 2,
                "decision": "import",
                "postflight": {
                    "beets_id": 9999,
                    "track_count": 12,
                    "disambiguated": False,
                    "imported_path": "/Beets/Artist/Album",
                    "bad_extensions": [],
                    "disambiguation_failure": {
                        "reason": reason,
                        "detail": detail,
                        "selector": "",
                    },
                    "moved_siblings": [],
                },
                "new_measurement": {
                    "min_bitrate_kbps": 320,
                    "is_cbr": True,
                    "was_converted_from": None,
                    "verified_lossless": False,
                    "spectral_grade": None,
                    "spectral_bitrate_kbps": None,
                    "avg_bitrate_kbps": None,
                },
                "existing_measurement": None,
                "conversion": {
                    "converted": 0,
                    "failed": 0,
                    "target_filetype": None,
                    "final_format": None,
                    "original_filetype": None,
                    "was_converted": False,
                    "is_transcode": False,
                    "post_conversion_min_bitrate": None,
                },
                "spectral": {
                    "suspect_pct": 0.0,
                    "cliff_freq_hz": None,
                    "per_track": [],
                },
            },
        )

    def test_timeout_reason_surfaces(self):
        result = classify_log_entry(
            self._entry_with_disambig("timeout", "timeout after 120s"))
        self.assertEqual(result.disambiguation_failure, "timeout")
        self.assertEqual(result.disambiguation_detail, "timeout after 120s")

    def test_nonzero_rc_reason_surfaces(self):
        result = classify_log_entry(
            self._entry_with_disambig("nonzero_rc", "rc=1: no matching album"))
        self.assertEqual(result.disambiguation_failure, "nonzero_rc")
        self.assertEqual(result.disambiguation_detail,
                         "rc=1: no matching album")

    def test_exception_reason_surfaces(self):
        result = classify_log_entry(
            self._entry_with_disambig("exception",
                                       "FileNotFoundError: beet missing"))
        self.assertEqual(result.disambiguation_failure, "exception")
        self.assertEqual(result.disambiguation_detail,
                         "FileNotFoundError: beet missing")

    def test_clean_import_leaves_fields_none(self):
        # Happy path: ImportResult present, disambiguation succeeded.
        entry = self._entry_with_disambig("timeout", "ignored")
        assert entry.import_result is not None
        entry.import_result["postflight"]["disambiguation_failure"] = None
        entry.import_result["postflight"]["disambiguated"] = True
        result = classify_log_entry(entry)
        self.assertIsNone(result.disambiguation_failure)
        self.assertIsNone(result.disambiguation_detail)

    def test_no_import_result_leaves_fields_none(self):
        # Rejected/timeout rows have no import_result — must not raise.
        result = classify_log_entry(
            _entry(outcome="rejected", import_result=None,
                   beets_scenario="high_distance", beets_distance=0.4))
        self.assertIsNone(result.disambiguation_failure)
        self.assertIsNone(result.disambiguation_detail)


class TestClassifyBadExtensions(unittest.TestCase):
    """Bad postflight extensions should be visible without parsing JSONB."""

    def test_bad_extensions_surface_from_import_result(self):
        result = classify_log_entry(_entry(
            outcome="success",
            import_result={
                "version": 2,
                "decision": "import",
                "postflight": {
                    "beets_id": 123,
                    "track_count": 2,
                    "imported_path": "/Beets/Artist/Album",
                    "bad_extensions": ["01 Track.bak"],
                    "disambiguation_failure": None,
                    "moved_siblings": [],
                },
            },
        ))

        self.assertEqual(result.bad_extensions, ["01 Track.bak"])

    def test_no_import_result_has_no_bad_extensions(self):
        result = classify_log_entry(_entry(import_result=None))
        self.assertEqual(result.bad_extensions, [])


class TestClassifyWrongMatchTriageAudit(unittest.TestCase):
    """Wrong-match triage audit should be visible in Recents without JSONB spelunking."""

    def _rejected_with_triage(self, triage: object) -> LogEntry:
        return _entry(
            outcome="rejected",
            beets_scenario="high_distance",
            beets_distance=0.190,
            soulseek_username="moundsofass",
            album_title="For Screening Purposes Only",
            artist_name="Test Icicles",
            validation_result={
                "scenario": "wrong_match",
                "wrong_match_triage": triage,
            },
        )

    def test_deleted_spectral_reject_surfaces_without_changing_original_verdict(self):
        result = classify_log_entry(self._rejected_with_triage({
            "action": "deleted_reject",
            "reason": "spectral_reject",
            "preview_verdict": "confident_reject",
            "preview_decision": "requeue_upgrade",
            "stage_chain": ["stage1_spectral:reject"],
        }))

        self.assertEqual(result.badge, "Rejected")
        self.assertEqual(result.verdict, "Wrong match (dist 0.190)")
        self.assertEqual(result.summary,
                         "Wrong match (dist 0.190) · moundsofass")
        self.assertEqual(result.wrong_match_triage_action, "deleted_reject")
        self.assertEqual(result.wrong_match_triage_preview_verdict,
                         "confident_reject")
        self.assertEqual(result.wrong_match_triage_preview_decision,
                         "requeue_upgrade")
        self.assertEqual(result.wrong_match_triage_reason, "spectral_reject")
        self.assertEqual(result.wrong_match_triage_stage_chain,
                         ["stage1_spectral:reject"])
        self.assertIn("deleted", result.wrong_match_triage_summary or "")
        self.assertIn("spectral", result.wrong_match_triage_summary or "")

    def test_stage_chain_supplies_spectral_fallback_when_reason_is_generic(self):
        result = classify_log_entry(self._rejected_with_triage({
            "action": "deleted_reject",
            "reason": "requeue_upgrade",
            "preview_verdict": "confident_reject",
            "preview_decision": "requeue_upgrade",
            "stage_chain": [
                "stage0_spectral_gate:would_run",
                "mp3_spectral:reject",
            ],
        }))

        self.assertEqual(result.wrong_match_triage_action, "deleted_reject")
        self.assertIn("deleted", result.wrong_match_triage_summary or "")
        self.assertIn("spectral", result.wrong_match_triage_summary or "")
        self.assertNotIn("requeue upgrade",
                         result.wrong_match_triage_summary or "")

    def test_kept_would_import_surfaces_importable_summary(self):
        result = classify_log_entry(self._rejected_with_triage({
            "action": "kept_would_import",
            "reason": "import",
            "preview_verdict": "would_import",
            "preview_decision": "import",
            "stage_chain": ["stage2_import:import"],
        }))

        self.assertEqual(result.badge, "Rejected")
        self.assertEqual(result.wrong_match_triage_action, "kept_would_import")
        self.assertIn("kept", result.wrong_match_triage_summary or "")
        self.assertIn("import", result.wrong_match_triage_summary or "")

    def test_missing_triage_defaults_empty(self):
        result = classify_log_entry(_entry(
            outcome="rejected",
            beets_scenario="high_distance",
            validation_result={"scenario": "wrong_match"},
        ))

        self.assertIsNone(result.wrong_match_triage_action)
        self.assertIsNone(result.wrong_match_triage_summary)
        self.assertEqual(result.wrong_match_triage_stage_chain, [])

    def test_string_validation_result_decodes_same_as_dict(self):
        result = classify_log_entry(_entry(
            outcome="rejected",
            beets_scenario="high_distance",
            validation_result=(
                '{"wrong_match_triage": {"action": "deleted_reject", '
                '"preview_verdict": "confident_reject", '
                '"preview_decision": "requeue_upgrade", '
                '"stage_chain": ["stage1_spectral:reject"]}}'
            ),
        ))

        self.assertEqual(result.wrong_match_triage_action, "deleted_reject")
        self.assertIn("spectral", result.wrong_match_triage_summary or "")
        self.assertEqual(result.wrong_match_triage_stage_chain,
                         ["stage1_spectral:reject"])

    def test_malformed_validation_result_does_not_raise(self):
        result = classify_log_entry(_entry(
            outcome="rejected",
            beets_scenario="high_distance",
            validation_result="{not-json",
        ))

        self.assertIsNone(result.wrong_match_triage_action)
        self.assertIsNone(result.wrong_match_triage_summary)

    def test_non_object_triage_does_not_raise(self):
        result = classify_log_entry(self._rejected_with_triage("deleted_reject"))

        self.assertIsNone(result.wrong_match_triage_action)
        self.assertIsNone(result.wrong_match_triage_summary)


# ============================================================================
# quality_label
# ============================================================================

class TestQualityLabel(unittest.TestCase):

    def test_flac(self):
        self.assertEqual(quality_label("flac", 0), "FLAC")

    def test_mp3_320(self):
        self.assertEqual(quality_label("mp3", 320), "MP3 320")

    def test_mp3_v0(self):
        self.assertEqual(quality_label("mp3", 243), "MP3 V0")

    def test_mp3_v2(self):
        self.assertEqual(quality_label("mp3", 192), "MP3 V2")

    def test_mp3_low(self):
        self.assertEqual(quality_label("mp3", 128), "MP3 128k")

    def test_no_format(self):
        self.assertEqual(quality_label("", 320), "?")

    def test_no_bitrate(self):
        self.assertEqual(quality_label("mp3", 0), "MP3")

    def test_high_v0_boundary(self):
        self.assertEqual(quality_label("mp3", 220), "MP3 V0")

    def test_just_below_v0(self):
        self.assertEqual(quality_label("mp3", 219), "MP3 V2")

    def test_alac(self):
        self.assertEqual(quality_label("alac", 0), "ALAC")


# ============================================================================
# classify_log_entry — badge classification
# ============================================================================

class TestClassifyBadge(unittest.TestCase):
    """Test that classify_log_entry returns the correct badge for each scenario."""

    def test_new_import(self):
        """First-time import, nothing on disk before."""
        result = classify_log_entry(_entry(outcome="success"))
        self.assertEqual(result.badge, "Imported")
        self.assertEqual(result.badge_class, "badge-new")

    def test_upgrade(self):
        """Successful import that upgraded existing quality."""
        result = classify_log_entry(_entry(
            outcome="success", existing_min_bitrate=192, actual_min_bitrate=320))
        self.assertEqual(result.badge, "Upgraded")
        self.assertEqual(result.badge_class, "badge-upgraded")

    def test_rejected_quality_downgrade(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="quality_downgrade",
            actual_min_bitrate=320, existing_min_bitrate=320))
        self.assertEqual(result.badge, "Rejected")
        self.assertEqual(result.badge_class, "badge-rejected")

    def test_rejected_spectral(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="spectral_reject",
            spectral_bitrate=160, existing_spectral_bitrate=192))
        self.assertEqual(result.badge, "Rejected")
        self.assertEqual(result.badge_class, "badge-rejected")

    def test_rejected_transcode_downgrade(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="transcode_downgrade",
            actual_min_bitrate=197, existing_min_bitrate=320))
        self.assertEqual(result.badge, "Rejected")
        self.assertEqual(result.badge_class, "badge-rejected")

    def test_rejected_suspect_lossless_downgrade(self):
        result = classify_log_entry(_entry(
            outcome="rejected",
            beets_scenario="suspect_lossless_downgrade",
            spectral_grade="suspect",
            spectral_bitrate=160,
            v0_probe_avg_bitrate=175,
            existing_v0_probe_avg_bitrate=171,
        ))
        self.assertEqual(result.badge, "Rejected")
        self.assertEqual(result.badge_class, "badge-rejected")
        self.assertIn("source V0 avg 175kbps", result.verdict)
        self.assertIn("existing source V0 avg 171kbps", result.verdict)
        self.assertIn("not meaningfully better", result.verdict)
        self.assertIn("searching continues", result.verdict)

    def test_rejected_lossless_source_locked(self):
        # Lossy candidate offered against an existing album whose original
        # lossless-source V0 probe is recorded — the lock fires before the
        # comparator runs. Verdict copy must surface (a) the candidate's real
        # bitrate + spectral context, (b) the recorded existing probe, and
        # (c) the structural reason ("only another lossless source can
        # override"). All three are read by users in the recents log to
        # understand WHY the candidate was rejected even though its avg
        # exceeded the on-disk transcode floor.
        result = classify_log_entry(_entry(
            outcome="rejected",
            beets_scenario="lossless_source_locked",
            actual_min_bitrate=176,
            spectral_grade="likely_transcode",
            spectral_bitrate=128,
            existing_v0_probe_avg_bitrate=240,
        ))
        self.assertEqual(result.badge, "Rejected")
        self.assertEqual(result.badge_class, "badge-rejected")
        self.assertIn("Lossless-source locked", result.verdict)
        self.assertIn("240", result.verdict)
        self.assertIn(
            "only another lossless source can override", result.verdict)
        self.assertIn("searching continues", result.verdict)

    def test_rejected_high_distance(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="high_distance", beets_distance=0.45))
        self.assertEqual(result.badge, "Rejected")
        self.assertEqual(result.badge_class, "badge-rejected")

    def test_rejected_audio_corrupt(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="audio_corrupt"))
        self.assertEqual(result.badge, "Rejected")

    def test_rejected_no_candidates(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="no_candidates"))
        self.assertEqual(result.badge, "Rejected")

    def test_rejected_album_name_mismatch(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="album_name_mismatch"))
        self.assertEqual(result.badge, "Rejected")

    def test_transcode_upgrade(self):
        result = classify_log_entry(_entry(
            outcome="success", beets_scenario="transcode_upgrade",
            was_converted=True, actual_min_bitrate=240, existing_min_bitrate=192))
        self.assertEqual(result.badge, "Transcode")
        self.assertEqual(result.badge_class, "badge-transcode")

    def test_transcode_first(self):
        result = classify_log_entry(_entry(
            outcome="success", beets_scenario="transcode_first",
            was_converted=True, actual_min_bitrate=197))
        self.assertEqual(result.badge, "Transcode")
        self.assertEqual(result.badge_class, "badge-transcode")

    def test_provisional_lossless_upgrade(self):
        result = classify_log_entry(_entry(
            outcome="success",
            beets_scenario="provisional_lossless_upgrade",
            spectral_grade="suspect",
            spectral_bitrate=160,
            v0_probe_kind="lossless_source_v0",
            v0_probe_avg_bitrate=228,
            existing_v0_probe_avg_bitrate=171,
            final_format="opus 128",
        ))
        self.assertEqual(result.badge, "Provisional")
        self.assertEqual(result.badge_class, "badge-provisional")
        self.assertIn("source V0 avg 228kbps", result.verdict)
        self.assertIn("existing source V0 avg 171kbps", result.verdict)
        self.assertIn("stored as opus 128", result.verdict)
        self.assertIn("searching continues", result.verdict)

    def test_provisional_lossless_first_probe(self):
        result = classify_log_entry(_entry(
            outcome="success",
            beets_scenario="provisional_lossless_upgrade",
            spectral_grade="likely_transcode",
            v0_probe_avg_bitrate=250,
        ))
        self.assertEqual(result.badge, "Provisional")
        self.assertIn("no comparable source probe", result.verdict)

    def test_force_import(self):
        result = classify_log_entry(_entry(outcome="force_import"))
        self.assertEqual(result.badge, "Force imported")
        self.assertEqual(result.badge_class, "badge-force")

    def test_curator_ban_with_username(self):
        """#188 follow-up: bad-rip click surfaces as a download_log event."""
        result = classify_log_entry(_entry(
            outcome="curator_ban", soulseek_username="H@rco"))
        self.assertEqual(result.badge, "Bad rip")
        self.assertEqual(result.badge_class, "badge-rejected")
        self.assertIn("H@rco", result.verdict)
        self.assertIn("Marked bad rip", result.verdict)

    def test_curator_ban_without_username(self):
        """E1.1 — no uploader resolved → still surfaces, terser verdict."""
        result = classify_log_entry(_entry(
            outcome="curator_ban", soulseek_username=None))
        self.assertEqual(result.badge, "Bad rip")
        self.assertEqual(result.verdict, "Marked bad rip")

    def test_failed(self):
        result = classify_log_entry(_entry(outcome="failed", beets_scenario="exception"))
        self.assertEqual(result.badge, "Failed")
        self.assertEqual(result.badge_class, "badge-failed")

    def test_timeout(self):
        result = classify_log_entry(_entry(outcome="timeout", beets_scenario="timeout"))
        self.assertEqual(result.badge, "Failed")
        self.assertEqual(result.badge_class, "badge-failed")

    def test_search_filetype_override_upgrade(self):
        """search_filetype_override set - replacing garbage CBR with genuine V0."""
        result = classify_log_entry(_entry(
            outcome="success", search_filetype_override="flac",
            existing_min_bitrate=320, actual_min_bitrate=243))
        self.assertEqual(result.badge, "Upgraded")
        self.assertEqual(result.badge_class, "badge-upgraded")


# ============================================================================
# classify_log_entry — border colors
# ============================================================================

class TestClassifyBorderColor(unittest.TestCase):

    def test_success_green_border(self):
        result = classify_log_entry(_entry(outcome="success"))
        self.assertIn(result.border_color, ("#3a6", "#1a4a2a"))

    def test_rejected_red_border(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="quality_downgrade"))
        self.assertEqual(result.border_color, "#a33")

    def test_transcode_amber_border(self):
        result = classify_log_entry(_entry(
            outcome="success", beets_scenario="transcode_upgrade",
            was_converted=True, actual_min_bitrate=240, existing_min_bitrate=192))
        self.assertEqual(result.border_color, "#a93")

    def test_force_import_blue_border(self):
        result = classify_log_entry(_entry(outcome="force_import"))
        self.assertEqual(result.border_color, "#46a")


# ============================================================================
# classify_log_entry — verdicts
# ============================================================================

class TestClassifyVerdict(unittest.TestCase):

    def test_quality_downgrade_verdict(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="quality_downgrade",
            actual_min_bitrate=320, existing_min_bitrate=320))
        self.assertIn("320", result.verdict)
        self.assertIn("not", result.verdict.lower())

    def test_spectral_reject_verdict(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="spectral_reject",
            spectral_bitrate=160, existing_spectral_bitrate=192))
        self.assertIn("160", result.verdict)
        self.assertIn("192", result.verdict)

    def test_spectral_reject_verdict_falls_back_to_min_bitrate(self):
        """When existing_spectral_bitrate is 0/None (genuine files have no cliff),
        the verdict should fall back to existing_min_bitrate."""
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="spectral_reject",
            spectral_bitrate=192, existing_spectral_bitrate=0,
            existing_min_bitrate=226))
        self.assertIn("192", result.verdict)
        self.assertIn("226", result.verdict)
        self.assertNotIn("unknown", result.verdict)

    def test_transcode_downgrade_verdict(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="transcode_downgrade",
            actual_min_bitrate=197, existing_min_bitrate=320))
        self.assertIn("197", result.verdict)
        self.assertIn("transcode", result.verdict.lower())

    def test_high_distance_verdict(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="high_distance", beets_distance=0.45))
        self.assertIn("wrong match", result.verdict.lower())
        self.assertIn("0.45", result.verdict)

    def test_audio_corrupt_verdict(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="audio_corrupt"))
        self.assertIn("corrupt", result.verdict.lower())

    def test_duplicate_remove_guard_verdict(self):
        ir = ImportResult(
            exit_code=7,
            decision="duplicate_remove_guard_failed",
            postflight=PostflightInfo(
                duplicate_remove_guard=DuplicateRemoveGuardInfo(
                    reason="duplicate_count_not_one",
                    target_source="musicbrainz",
                    target_release_id="aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb",
                    duplicate_count=2,
                    message="beets reported 2 duplicate albums; expected exactly 1",
                    candidates=[
                        DuplicateRemoveCandidate(
                            beets_album_id=42,
                            mb_albumid="aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb",
                            album_path="/Beets/Artist/Album",
                            item_count=10,
                        ),
                    ],
                ),
            ),
        )
        result = classify_log_entry(_entry(
            outcome="rejected",
            beets_scenario="duplicate_remove_guard_failed",
            import_result=ir.to_json(),
        ))

        self.assertIn("duplicate remove guard failed", result.verdict.lower())
        self.assertIn("2 duplicates", result.verdict.lower())

    def test_no_candidates_verdict(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="no_candidates"))
        self.assertIn("no", result.verdict.lower())
        self.assertIn("match", result.verdict.lower())

    def test_album_name_mismatch_verdict(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="album_name_mismatch"))
        self.assertIn("name mismatch", result.verdict.lower())

    def test_nested_layout_verdict(self):
        """Gate-rejected force/manual import of a nested folder layout must
        render a friendly label in the Recents tab, not the raw scenario
        string. Otherwise operators see the literal "nested_layout" and have
        to go grepping for what it means.
        """
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="nested_layout"))
        self.assertIn("nested", result.verdict.lower())
        self.assertIn("flatten", result.verdict.lower())
        self.assertNotEqual(
            result.verdict, "nested_layout",
            "verdict must be a friendly label, not the raw scenario string")

    def test_transcode_upgrade_verdict(self):
        result = classify_log_entry(_entry(
            outcome="success", beets_scenario="transcode_upgrade",
            was_converted=True, actual_min_bitrate=240, existing_min_bitrate=192))
        self.assertIn("searching", result.verdict.lower())

    def test_transcode_first_verdict(self):
        result = classify_log_entry(_entry(
            outcome="success", beets_scenario="transcode_first",
            was_converted=True, actual_min_bitrate=197))
        self.assertIn("searching", result.verdict.lower())

    def test_new_import_verdict(self):
        result = classify_log_entry(_entry(outcome="success"))
        v = result.verdict.lower()
        self.assertNotIn("not", v)
        self.assertNotIn("reject", v)

    def test_upgrade_verdict(self):
        result = classify_log_entry(_entry(
            outcome="success", existing_min_bitrate=192, actual_min_bitrate=320))
        v = result.verdict.lower()
        self.assertTrue("upgrade" in v or "192" in v or "320" in v)

    def test_verified_lossless_upgrade_verdict(self):
        result = classify_log_entry(_entry(
            outcome="success", was_converted=True, original_filetype="flac",
            actual_filetype="mp3", actual_min_bitrate=243,
            existing_min_bitrate=192, spectral_grade="genuine"))
        self.assertIn("verified lossless", result.verdict.lower())

    def test_timeout_verdict(self):
        result = classify_log_entry(_entry(outcome="timeout", beets_scenario="timeout"))
        self.assertIn("timed out", result.verdict.lower())

    def test_exception_verdict(self):
        result = classify_log_entry(_entry(outcome="failed", beets_scenario="exception"))
        self.assertIn("error", result.verdict.lower())

    def test_force_import_verdict(self):
        result = classify_log_entry(_entry(outcome="force_import"))
        self.assertIn("force", result.verdict.lower())


# ============================================================================
# classify_log_entry — summary (folded in from build_summary_line)
# ============================================================================

class TestClassifySummary(unittest.TestCase):
    """Test that ClassifiedEntry.summary is concise and contains key info."""

    def test_new_import_summary(self):
        result = classify_log_entry(_entry(
            outcome="success", actual_min_bitrate=320,
            soulseek_username="aguavivi23"))
        self.assertIn("320", result.summary)
        self.assertIn("aguavivi23", result.summary)

    def test_upgrade_summary_includes_username(self):
        result = classify_log_entry(_entry(
            outcome="success", existing_min_bitrate=192, actual_min_bitrate=320,
            soulseek_username="gooduser"))
        self.assertIn("gooduser", result.summary)

    def test_rejected_summary_includes_username(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="quality_downgrade",
            actual_min_bitrate=320, existing_min_bitrate=320,
            soulseek_username="baduser"))
        self.assertIn("baduser", result.summary)

    def test_flac_conversion_summary(self):
        result = classify_log_entry(_entry(
            outcome="success", was_converted=True, original_filetype="flac",
            actual_filetype="mp3", actual_min_bitrate=243,
            soulseek_username="flacuser"))
        self.assertTrue("flac" in result.summary.lower()
                        or "converted" in result.summary.lower()
                        or "V0" in result.summary)

    def test_spectral_reject_summary(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="spectral_reject",
            spectral_bitrate=160, existing_spectral_bitrate=192,
            soulseek_username="fakeflac"))
        self.assertIn("fakeflac", result.summary)
        self.assertIn("160", result.summary)

    def test_summary_no_html(self):
        result = classify_log_entry(_entry(
            outcome="success", existing_min_bitrate=192, actual_min_bitrate=320))
        self.assertNotIn("<", result.summary)
        self.assertNotIn(">", result.summary)

    def test_no_arrow_chains(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="quality_downgrade",
            actual_min_bitrate=320, existing_min_bitrate=320))
        self.assertNotIn("slskd:", result.summary)
        self.assertNotIn("actual:", result.summary)
        self.assertNotIn("\u2192", result.summary)

    def test_missing_username(self):
        result = classify_log_entry(_entry(
            outcome="success", soulseek_username=None))
        self.assertIsInstance(result.summary, str)
        self.assertTrue(len(result.summary) > 0)


# ============================================================================
# Edge cases
# ============================================================================

class TestClassifyEdgeCases(unittest.TestCase):

    def test_missing_scenario(self):
        result = classify_log_entry(_entry(outcome="success", beets_scenario=None))
        self.assertIsInstance(result.badge, str)
        self.assertIsInstance(result.verdict, str)

    def test_zero_bitrate(self):
        result = classify_log_entry(_entry(
            outcome="success", actual_min_bitrate=0, existing_min_bitrate=0))
        self.assertIsInstance(result.badge, str)

    def test_none_bitrate(self):
        result = classify_log_entry(_entry(
            outcome="success", actual_min_bitrate=None, existing_min_bitrate=None))
        self.assertIsInstance(result.badge, str)

    def test_unknown_outcome(self):
        result = classify_log_entry(_entry(outcome="something_new"))
        self.assertIsInstance(result.badge, str)

    def test_all_results_are_classified_entry(self):
        """Every result is a ClassifiedEntry with all fields."""
        entries = [
            _entry(outcome="success"),
            _entry(outcome="rejected", beets_scenario="high_distance"),
            _entry(outcome="force_import"),
            _entry(outcome="timeout"),
            _entry(outcome="failed"),
        ]
        for entry in entries:
            result = classify_log_entry(entry)
            self.assertIsInstance(result, ClassifiedEntry,
                                 f"Expected ClassifiedEntry for outcome={entry.outcome}")
            self.assertTrue(result.badge)
            self.assertTrue(result.verdict)
            self.assertTrue(result.summary)


# ============================================================================
# Exception verdicts with error_message
# ============================================================================

class TestExceptionVerdicts(unittest.TestCase):

    def test_exception_with_error_message(self):
        """Exception verdict should include the error_message when available."""
        result = classify_log_entry(_entry(
            outcome="failed", beets_scenario="exception",
            error_message="FileNotFoundError: /mnt/virtio/music/slskd/foo"))
        self.assertIn("FileNotFoundError", result.verdict)

    def test_exception_without_error_message(self):
        """Exception verdict without error_message should still work."""
        result = classify_log_entry(_entry(
            outcome="failed", beets_scenario="exception",
            error_message=None))
        self.assertIn("error", result.verdict.lower())

    def test_failed_falls_back_to_import_result_downgrade(self):
        """Manual-import failures with only import_result still get a verdict."""
        result = classify_log_entry(_entry(
            outcome="failed",
            beets_scenario=None,
            error_message=None,
            import_result={
                "version": 2,
                "exit_code": 5,
                "decision": "downgrade",
                "new_measurement": {"min_bitrate_kbps": 239},
                "existing_measurement": {"min_bitrate_kbps": 320},
            },
        ))
        self.assertIn("239", result.verdict)
        self.assertIn("320", result.verdict)

    def test_failed_falls_back_to_import_result_error(self):
        """ImportResult error text is surfaced when error_message is blank."""
        result = classify_log_entry(_entry(
            outcome="failed",
            beets_scenario=None,
            error_message=None,
            import_result={
                "version": 2,
                "exit_code": 2,
                "decision": "import_failed",
                "error": "Harness returned rc=2",
            },
        ))
        self.assertIn("Harness returned rc=2", result.verdict)

    def test_timeout_ignores_error_message(self):
        """Timeout verdict is fixed, doesn't use error_message."""
        result = classify_log_entry(_entry(
            outcome="timeout", beets_scenario="timeout",
            error_message="some error"))
        self.assertIn("timed out", result.verdict.lower())


# ============================================================================
# downloaded_label — server-computed download quality label
# ============================================================================

class TestDownloadedLabel(unittest.TestCase):

    def test_mp3_download(self):
        """MP3 320 download gets a label."""
        result = classify_log_entry(_entry(
            outcome="success", actual_filetype="mp3", actual_min_bitrate=320))
        self.assertTrue(hasattr(result, "downloaded_label"))
        self.assertIn("320", result.downloaded_label)

    def test_flac_converted_download(self):
        """FLAC converted to V0 shows conversion."""
        result = classify_log_entry(_entry(
            outcome="success", was_converted=True,
            original_filetype="flac", actual_filetype="mp3",
            actual_min_bitrate=243, bitrate=243000))
        self.assertTrue(hasattr(result, "downloaded_label"))
        self.assertIn("FLAC", result.downloaded_label)
        self.assertIn("V0", result.downloaded_label)

    def test_opus_converted_download(self):
        """FLAC converted to Opus shows correct format, not MP3."""
        result = classify_log_entry(_entry(
            outcome="success", was_converted=True,
            original_filetype="flac", actual_filetype="opus",
            actual_min_bitrate=117, bitrate=117000))
        self.assertIn("FLAC", result.downloaded_label)
        self.assertIn("OPUS", result.downloaded_label)
        self.assertNotIn("MP3", result.downloaded_label)

    def test_no_filetype_download(self):
        """Missing filetype doesn't crash."""
        result = classify_log_entry(_entry(
            outcome="force_import", actual_filetype=None, filetype=None))
        self.assertTrue(hasattr(result, "downloaded_label"))

    def test_bitrate_fallback(self):
        """Falls back to bitrate (bps) when actual_min_bitrate is None."""
        result = classify_log_entry(_entry(
            outcome="success", actual_min_bitrate=None,
            bitrate=155000, actual_filetype="mp3"))
        self.assertTrue(hasattr(result, "downloaded_label"))
        self.assertIn("155", result.downloaded_label)


# ============================================================================
# search_filetype_override - should only trigger with existing files on disk
# ============================================================================

class TestSearchFiletypeOverride(unittest.TestCase):

    def test_search_filetype_override_without_existing_is_new_import(self):
        """search_filetype_override set but nothing on disk = new import, not upgrade."""
        result = classify_log_entry(_entry(
            outcome="success", search_filetype_override="flac",
            existing_min_bitrate=None, actual_min_bitrate=243))
        self.assertEqual(result.badge, "Imported")

    def test_search_filetype_override_with_existing_is_upgrade(self):
        """search_filetype_override set AND existing on disk = upgrade."""
        result = classify_log_entry(_entry(
            outcome="success", search_filetype_override="flac",
            existing_min_bitrate=320, actual_min_bitrate=243))
        self.assertEqual(result.badge, "Upgraded")

    def test_search_filetype_override_opus_shows_opus(self):
        """Opus upgrade should show OPUS in verdict, not MP3."""
        result = classify_log_entry(_entry(
            outcome="success", search_filetype_override="flac",
            existing_min_bitrate=320, actual_filetype="opus",
            was_converted=True, original_filetype="flac"))
        self.assertEqual(result.badge, "Upgraded")
        self.assertIn("OPUS", result.verdict)
        self.assertNotIn("MP3", result.verdict)

    def test_upgrade_opus_shows_opus_in_verdict(self):
        """Opus upgrade verdict should use actual filetype."""
        result = classify_log_entry(_entry(
            outcome="success", existing_min_bitrate=192,
            actual_filetype="opus", actual_min_bitrate=117,
            was_converted=True, original_filetype="flac"))
        self.assertEqual(result.badge, "Upgraded")
        self.assertIn("OPUS", result.verdict)


# ============================================================================
# Spectral fallback bug — verdict must show real bitrate plus spectral estimate
# ============================================================================

class TestVerdictSpectralFallback(unittest.TestCase):
    """Verdicts must show real file bitrate and spectral cliff estimate.

    When actual_min_bitrate is NULL (rejected downloads that were never
    imported), the or-chain in _rejection_verdict falls through to
    spectral_bitrate — a cliff estimate that answers "what was the original
    source quality?" not "what bitrate are these files?".

    These tests reproduce exact live scenarios where the UI either showed only
    spectral numbers or only avg numbers. Both are incomplete: the operator
    needs the real container avg/min and the spectral estimate that drove the
    quality comparison.
    """

    def test_quality_downgrade_shows_real_bitrate_and_spectral(self):
        """The Ataris / Welcome the Night bug: actual_min_bitrate is NULL,
        spectral_bitrate is 96, but the real download is 128kbps min
        (187kbps avg). The import_result JSONB has the correct new_measurement.

        Test intent: verdict must show both the avg bitrate and the spectral
        estimate. Avg explains the container measurement; spectral explains
        why the pipeline still rejected it.
        """
        result = classify_log_entry(_entry(
            outcome="rejected",
            beets_scenario="quality_downgrade",
            actual_min_bitrate=None,
            spectral_bitrate=96,
            existing_min_bitrate=128,
            existing_spectral_bitrate=96,
            bitrate=128000,
            import_result={
                "version": 2,
                "exit_code": 5,
                "decision": "downgrade",
                "new_measurement": {
                    "min_bitrate_kbps": 128,
                    "avg_bitrate_kbps": 187,
                    "median_bitrate_kbps": 192,
                    "spectral_bitrate_kbps": 96,
                    "format": "MP3",
                    "is_cbr": False,
                    "verified_lossless": False,
                },
                "existing_measurement": {
                    "min_bitrate_kbps": 128,
                    "avg_bitrate_kbps": 187,
                    "median_bitrate_kbps": 192,
                    "spectral_bitrate_kbps": 96,
                    "format": "MP3",
                    "is_cbr": False,
                    "verified_lossless": False,
                },
            },
        ))
        # Avg is the container measurement.
        self.assertIn("187", result.verdict)
        self.assertIn("avg", result.verdict)
        # Spectral is the estimated source-quality floor.
        self.assertIn("96", result.verdict)
        self.assertIn("spectral", result.verdict.lower())

    def test_quality_downgrade_without_import_result_uses_container_bitrate(self):
        """When there's no import_result at all, fall back to bitrate field
        (container bitrate in bps), while still showing spectral context."""
        result = classify_log_entry(_entry(
            outcome="rejected",
            beets_scenario="quality_downgrade",
            actual_min_bitrate=None,
            spectral_bitrate=96,
            existing_min_bitrate=128,
            existing_spectral_bitrate=96,
            bitrate=128000,
            import_result=None,
        ))
        self.assertIn("128", result.verdict)
        self.assertIn("96", result.verdict)
        self.assertIn("spectral", result.verdict.lower())

    def test_transcode_downgrade_uses_real_bitrate_not_spectral(self):
        """Same spectral fallback bug in transcode_downgrade scenario."""
        result = classify_log_entry(_entry(
            outcome="rejected",
            beets_scenario="transcode_downgrade",
            actual_min_bitrate=None,
            spectral_bitrate=96,
            existing_min_bitrate=240,
            existing_spectral_bitrate=None,
            bitrate=192000,
            import_result={
                "version": 2,
                "exit_code": 6,
                "decision": "transcode_downgrade",
                "new_measurement": {"min_bitrate_kbps": 192},
                "existing_measurement": {"min_bitrate_kbps": 240},
            },
        ))
        self.assertIn("192", result.verdict)
        self.assertNotIn("96", result.verdict)

    def test_vbr_downgrade_verdict_uses_avg_not_min(self):
        """Unter Null - The Failure Epiphany (req 1749) VBR-downgrade rendering.

        Back-end fix: ``build_existing_measurement`` / ``full_pipeline_decision``
        no longer clobber existing.avg/median when existing is VBR — the
        spectral override only drives min. Under cfg.bitrate_metric=avg
        (production default) the comparison now reads at the avg level:
        new avg 152 is lower than existing avg 225 → downgrade.

        The UI must render the comparison at the same metric the backend
        used. If the verdict prints min (new=152 vs existing=96 after
        clamp), the rejection reads as "152kbps is not better than 96kbps",
        which contradicts itself to any human reader.

        Assert: the verdict mentions the real avg numbers (152 and 225) and
        the spectral estimates for both sides. It must not collapse the
        comparison into a naked "152 is not better than 96" contradiction.
        """
        result = classify_log_entry(_entry(
            outcome="rejected",
            beets_scenario="quality_downgrade",
            actual_min_bitrate=152,
            spectral_bitrate=96,
            existing_min_bitrate=96,          # clamped by override
            existing_spectral_bitrate=96,
            bitrate=152000,
            import_result={
                "version": 2,
                "exit_code": 5,
                "decision": "downgrade",
                "new_measurement": {
                    "min_bitrate_kbps": 152,
                    "avg_bitrate_kbps": 152,
                    "median_bitrate_kbps": 152,
                    "spectral_bitrate_kbps": 96,
                    "spectral_grade": "likely_transcode",
                    "format": "MP3",
                    "is_cbr": True,
                    "verified_lossless": False,
                },
                "existing_measurement": {
                    "min_bitrate_kbps": 96,      # overridden
                    "avg_bitrate_kbps": 225,     # real, preserved by the fix
                    "median_bitrate_kbps": 224,
                    "spectral_bitrate_kbps": 96,
                    "spectral_grade": "likely_transcode",
                    "format": "MP3",
                    "is_cbr": False,
                    "verified_lossless": False,
                },
            },
        ))
        # Verdict must show the real avg numbers that drove the decision.
        self.assertIn("152", result.verdict,
                      "verdict must cite new avg (152)")
        self.assertIn("225", result.verdict,
                      "verdict must cite existing avg (225), "
                      "not the clamped min — otherwise '152 > 96 but "
                      "rejected' reads as a contradiction")
        self.assertIn("spectral", result.verdict.lower())
        self.assertIn("96", result.verdict)
        # And MUST NOT say "152 is not better than existing 96" — the old
        # one-sided min-based comparison.
        self.assertNotRegex(
            result.verdict,
            r"152kbps avg\s+is not better than existing 96",
            "verdict must not emit the misleading min-based comparison")

    def test_quality_downgrade_shows_spectral_on_both_sides(self):
        """Ambient One live shape: raw avg rises, but spectral regresses.

        Recents must explain both sides of the comparison: candidate avg 222
        with spectral 128 is worse than existing avg 192 with spectral 192.
        Showing only "222kbps avg is not better than existing 192kbps avg"
        makes the correct rejection look like arithmetic failed.
        """
        result = classify_log_entry(_entry(
            outcome="rejected",
            beets_scenario="quality_downgrade",
            actual_min_bitrate=None,
            spectral_grade="likely_transcode",
            spectral_bitrate=128,
            existing_min_bitrate=192,
            existing_spectral_bitrate=192,
            bitrate=222000,
            import_result={
                "version": 2,
                "exit_code": 5,
                "decision": "downgrade",
                "new_measurement": {
                    "format": "MP3",
                    "is_cbr": False,
                    "avg_bitrate_kbps": 222,
                    "min_bitrate_kbps": 156,
                    "median_bitrate_kbps": 221,
                    "spectral_grade": "likely_transcode",
                    "spectral_bitrate_kbps": 128,
                    "verified_lossless": False,
                },
                "existing_measurement": {
                    "format": "MP3",
                    "is_cbr": True,
                    "avg_bitrate_kbps": 192,
                    "min_bitrate_kbps": 192,
                    "median_bitrate_kbps": 192,
                    "spectral_grade": "likely_transcode",
                    "spectral_bitrate_kbps": 192,
                    "verified_lossless": False,
                },
            },
        ))

        self.assertIn("222kbps avg", result.verdict)
        self.assertIn("spectral likely_transcode ~128kbps", result.verdict)
        self.assertIn("existing 192kbps avg", result.verdict)
        self.assertIn("spectral likely_transcode ~192kbps", result.verdict)

    def test_transcode_classify_uses_real_bitrate_not_spectral(self):
        """_classify_transcode has the same or-chain bug for success transcodes."""
        result = classify_log_entry(_entry(
            outcome="success",
            beets_scenario="transcode_upgrade",
            actual_min_bitrate=None,
            spectral_bitrate=96,
            existing_min_bitrate=192,
            bitrate=210000,
            was_converted=True,
        ))
        # Should show 210 (bitrate // 1000), not 96 (spectral)
        self.assertIn("210", result.verdict)
        self.assertNotIn("96", result.verdict)

    def test_summary_also_uses_real_bitrate(self):
        """The summary line (collapsed card) inherits from the verdict.
        If the verdict is wrong, the summary is wrong too."""
        result = classify_log_entry(_entry(
            outcome="rejected",
            beets_scenario="quality_downgrade",
            actual_min_bitrate=None,
            spectral_bitrate=96,
            existing_min_bitrate=128,
            existing_spectral_bitrate=96,
            bitrate=128000,
            soulseek_username="nexus15",
            import_result={
                "version": 2,
                "exit_code": 5,
                "decision": "downgrade",
                "new_measurement": {"min_bitrate_kbps": 128},
                "existing_measurement": {"min_bitrate_kbps": 128},
            },
        ))
        self.assertIn("128", result.summary)
        self.assertNotIn("96", result.summary)
        self.assertIn("nexus15", result.summary)


# ============================================================================
# Point-in-time bitrate — per-row verdicts must NOT use current album state
# ============================================================================

class TestPerRowBitrateIsPointInTime(unittest.TestCase):
    """Every per-row display (verdict, summary, downloaded_label) must resolve
    the 'downloaded' bitrate from this row's state at the time of that
    download — never from album_requests.min_bitrate (which the recents query
    JOINs in as request_min_bitrate and which reflects the album's *current*
    state at query time).

    Live reproducer: request 1055 (Velella Velella - Bay of Biscay), two
    successive download_log rows:
      - row 3628 (brandlos, earlier): imported 119kbps → UI should say '128→119'
      - row 3631 (Ceezles, later):    imported 162kbps → UI should say '119→162'
    Both rows have download_log.actual_min_bitrate = NULL, but the JSONB
    carries the correct new_measurement.min_bitrate_kbps. The old code fell
    through to request_min_bitrate (=162, the current album state), so both
    rows painted '→162' as the 'to' value — inventing a fake self-upgrade
    for the older row.
    """

    def _brandlos_row(self):
        """Earlier of two successive upgrades — column NULL, JSONB has 119.
        Must display as '128 → 119', not '128 → 162'."""
        return _entry(
            outcome="success",
            existing_min_bitrate=128,
            actual_min_bitrate=None,          # pre-fix rows have NULL
            bitrate=119000,                   # container bitrate (legacy signal)
            request_min_bitrate=162,          # current album state via JOIN
            import_result={
                "version": 2,
                "decision": "import",
                "new_measurement": {
                    "min_bitrate_kbps": 119,
                    "avg_bitrate_kbps": 179,
                    "median_bitrate_kbps": 181,
                    "format": "MP3",
                    "is_cbr": False,
                    "verified_lossless": False,
                    "spectral_grade": "likely_transcode",
                    "spectral_bitrate_kbps": 160,
                },
                "existing_measurement": {
                    "min_bitrate_kbps": 128,
                    "spectral_grade": "genuine",
                },
            },
        )

    # ---- _upgrade_verdict (line 193) ----

    def test_upgrade_verdict_uses_point_in_time_bitrate(self):
        """The headline bug: two successive upgrades both show current album
        state as the 'to' bitrate. Must use this row's new_measurement."""
        result = classify_log_entry(self._brandlos_row())
        self.assertIn("119", result.verdict,
                      f"verdict must contain 119 (this download), got: {result.verdict!r}")
        self.assertNotIn("162", result.verdict,
                         f"verdict must NOT contain 162 (current album state): {result.verdict!r}")

    def test_upgrade_verdict_reads_from_import_result_when_column_null(self):
        """Historical rows with NULL actual_min_bitrate must render correctly
        from the JSONB — no retroactive reindex needed."""
        result = classify_log_entry(self._brandlos_row())
        self.assertIn("128", result.verdict)  # existing
        self.assertIn("119", result.verdict)  # new, from JSONB
        self.assertTrue(result.verdict.startswith("Upgrade:"),
                        f"expected 'Upgrade:' prefix, got: {result.verdict!r}")

    # ---- _build_summary (line 404) for the Upgraded branch ----

    def test_upgrade_summary_uses_point_in_time_bitrate(self):
        """The collapsed-card summary inherits the verdict for upgrades, so it
        must also be point-in-time (not current album state)."""
        entry = self._brandlos_row()
        entry.soulseek_username = "brandlos"
        result = classify_log_entry(entry)
        self.assertIn("119", result.summary)
        self.assertNotIn("162", result.summary)
        self.assertIn("brandlos", result.summary)

    # ---- _build_summary (line 404) for the Imported branch ----

    def test_imported_summary_uses_point_in_time_bitrate(self):
        """New-import rows (existing_min_bitrate=None) still share the same
        or-chain bug. If a request later gets upgraded, the 'Imported' summary
        for the historical row must not inherit the newer state."""
        entry = _entry(
            outcome="success",
            existing_min_bitrate=None,         # first import
            actual_min_bitrate=None,           # column NULL
            bitrate=128000,
            request_min_bitrate=320,           # album got upgraded later
            import_result={
                "version": 2,
                "decision": "import",
                "new_measurement": {"min_bitrate_kbps": 128},
                "existing_measurement": None,
            },
        )
        result = classify_log_entry(entry)
        self.assertEqual(result.badge, "Imported")
        self.assertIn("128", result.summary)
        self.assertNotIn("320", result.summary)

    # ---- _new_import_verdict (line 312) ----

    def test_new_import_verdict_uses_point_in_time_bitrate(self):
        """Same or-chain in _new_import_verdict."""
        entry = _entry(
            outcome="success",
            existing_min_bitrate=None,
            actual_min_bitrate=None,
            bitrate=128000,
            request_min_bitrate=320,
            import_result={
                "version": 2,
                "decision": "import",
                "new_measurement": {"min_bitrate_kbps": 128},
            },
        )
        result = classify_log_entry(entry)
        self.assertIn("128", result.verdict)
        self.assertNotIn("320", result.verdict)

    # ---- _classify_search_filetype_override (line 300-301) ----

    def test_search_filetype_override_uses_point_in_time_bitrate(self):
        """The 'Replaced unverified CBR with X' label must reflect this row's
        bitrate tier, not the album's current state. Point-in-time 243kbps
        renders as 'V0'; leaked current-state 350kbps would render as '320'."""
        entry = _entry(
            outcome="success",
            search_filetype_override="flac",
            existing_min_bitrate=320,
            actual_min_bitrate=None,
            bitrate=243000,
            request_min_bitrate=350,           # aliases to '320' tier
            actual_filetype="mp3",
            import_result={
                "version": 2,
                "decision": "import",
                "new_measurement": {"min_bitrate_kbps": 243},
            },
        )
        result = classify_log_entry(entry)
        self.assertEqual(result.badge, "Upgraded")
        self.assertIn("V0", result.verdict,
                      f"expected V0 tier (from 243kbps), got: {result.verdict!r}")
        self.assertNotIn("320", result.verdict,
                         f"verdict leaked current-state 320 tier: {result.verdict!r}")

    # ---- _build_downloaded_label (line 432-434) ----

    def test_downloaded_label_uses_point_in_time_bitrate(self):
        """downloaded_label builds from actual_min_bitrate or bitrate//1000;
        it already avoids request_min_bitrate. But for retroactive
        correctness (NULL column rows), the JSONB should be consulted first."""
        entry = _entry(
            outcome="success",
            actual_min_bitrate=None,
            bitrate=119000,
            request_min_bitrate=162,
            actual_filetype="mp3",
            import_result={
                "version": 2,
                "decision": "import",
                "new_measurement": {"min_bitrate_kbps": 119},
            },
        )
        result = classify_log_entry(entry)
        # The label must reflect this row's download, not current state.
        self.assertIn("119", result.downloaded_label)
        self.assertNotIn("162", result.downloaded_label)

    # ---- Parametrized guard: current album state never leaks into a per-row
    # display regardless of which render path runs. ----

    def test_current_album_state_never_leaks_into_per_row_display(self):
        """Invariant: when actual_min_bitrate is NULL and JSONB has a valid
        new_measurement, no per-row string (verdict, summary, downloaded_label)
        contains the request_min_bitrate value. Guards against adding a new
        display call site that copies the old or-chain.

        ALIEN must be in the range quality_label renders literally (<170kbps,
        otherwise it would be aliased to a tier name like 'V0' or '320' and
        hide the leak). Point-in-time values are chosen in a different
        tier range so the bug is unambiguous.
        """
        ALIEN = 157          # <170 → renders as 'MP3 157k', stays literal
        POINT_IN_TIME = 245  # ≥220 → renders as 'MP3 V0', no digit overlap
        scenarios = [
            ("upgrade",
             _entry(outcome="success",
                    existing_min_bitrate=128,
                    actual_min_bitrate=None, bitrate=POINT_IN_TIME * 1000,
                    request_min_bitrate=ALIEN,
                    import_result={
                        "version": 2, "decision": "import",
                        "new_measurement": {"min_bitrate_kbps": POINT_IN_TIME},
                        "existing_measurement": {"min_bitrate_kbps": 128},
                    })),
            ("new_import",
             _entry(outcome="success",
                    existing_min_bitrate=None,
                    actual_min_bitrate=None, bitrate=POINT_IN_TIME * 1000,
                    request_min_bitrate=ALIEN,
                    import_result={
                        "version": 2, "decision": "import",
                        "new_measurement": {"min_bitrate_kbps": POINT_IN_TIME},
                    })),
            ("search_filetype_override",
             _entry(outcome="success",
                    search_filetype_override="flac",
                    existing_min_bitrate=320,
                    actual_min_bitrate=None, bitrate=POINT_IN_TIME * 1000,
                    request_min_bitrate=ALIEN,
                    import_result={
                        "version": 2, "decision": "import",
                        "new_measurement": {"min_bitrate_kbps": POINT_IN_TIME},
                    })),
        ]
        alien = str(ALIEN)
        for desc, entry in scenarios:
            with self.subTest(desc=desc):
                result = classify_log_entry(entry)
                self.assertNotIn(
                    alien, result.verdict,
                    f"{desc}: verdict leaked current album state: {result.verdict!r}")
                self.assertNotIn(
                    alien, result.summary,
                    f"{desc}: summary leaked current album state: {result.summary!r}")
                self.assertNotIn(
                    alien, result.downloaded_label,
                    f"{desc}: downloaded_label leaked current album state: "
                    f"{result.downloaded_label!r}")


class TestParseImportResultTolerantOfMsgspecValidationError(unittest.TestCase):
    """Issue #141 regression guard.

    Post-migration, strict ``msgspec.Struct`` decode raises
    ``msgspec.ValidationError`` on type drift. ``_parse_import_result``
    is called from the Recents tab rendering path for every row in
    ``/api/pipeline/log`` — a single malformed historical row must
    degrade to ``None`` (shown as unclassified) rather than 500 the
    whole route.
    """

    def test_malformed_dict_returns_none(self):
        entry = _entry(import_result={
            "version": 2,
            "exit_code": "not-an-int",  # declared int → ValidationError
            "decision": "import",
        })
        self.assertIsNone(_parse_import_result(entry))

    def test_malformed_json_string_returns_none(self):
        import json as _json
        entry = _entry(import_result=_json.dumps({
            "version": 2,
            "exit_code": "not-an-int",
            "decision": "import",
        }))
        self.assertIsNone(_parse_import_result(entry))


if __name__ == "__main__":
    unittest.main()
