"""Self-tests for the FakePipelineDB download_log cluster.

Split out of ``tests/test_fakes.py`` (#1313) so each cluster of the
fake has its sibling tests beside it.
"""
import unittest
from datetime import timedelta

from lib.quality import (
    AudioQualityMeasurement,
)
from tests.evidence_helpers import make_album_quality_evidence
from tests.fakes import (
    FakePipelineDB,
)
from tests.helpers import (
    make_request_row,
)


class TestFakeRecentSuccessfulUploader(unittest.TestCase):
    """Self-tests for FakePipelineDB.get_recent_successful_uploader (plan U2)."""

    def test_returns_none_on_empty_logs(self):
        db = FakePipelineDB()
        self.assertIsNone(db.get_recent_successful_uploader(42))

    def test_returns_none_when_no_successful_log(self):
        db = FakePipelineDB()
        db.log_download(42, soulseek_username="bob", outcome="rejected")
        db.log_download(42, soulseek_username="alice", outcome="failed")
        self.assertIsNone(db.get_recent_successful_uploader(42))

    def test_returns_most_recent_success(self):
        db = FakePipelineDB()
        db.log_download(42, soulseek_username="alice", outcome="success")
        db.log_download(42, soulseek_username="bob", outcome="success")
        self.assertEqual(db.get_recent_successful_uploader(42), "bob")

    def test_returns_most_recent_force_import(self):
        db = FakePipelineDB()
        db.log_download(42, soulseek_username="alice", outcome="success")
        db.log_download(42, soulseek_username="harco", outcome="force_import")
        self.assertEqual(db.get_recent_successful_uploader(42), "harco")

    def test_ignores_other_request_ids(self):
        db = FakePipelineDB()
        db.log_download(42, soulseek_username="alice", outcome="success")
        db.log_download(99, soulseek_username="bob", outcome="success")
        self.assertEqual(db.get_recent_successful_uploader(42), "alice")
        self.assertEqual(db.get_recent_successful_uploader(99), "bob")

    def test_skips_null_uploader_rows(self):
        db = FakePipelineDB()
        db.log_download(42, soulseek_username="alice", outcome="success")
        db.log_download(42, soulseek_username=None, outcome="success")
        self.assertEqual(db.get_recent_successful_uploader(42), "alice")


class TestFakeDownloadLogEvidenceOverlay(unittest.TestCase):
    """The fake's download_log readers must run the SAME evidence overlay
    production runs (``lib/pipeline_db/download_log.py::
    overlay_evidence_onto_download_log_row``), not a hand-mirror of it.

    Both pins below reproduce a divergence a hand-mirror carried: the
    fake was strictly MORE permissive than production, so a test world
    could earn evidence-derived values production never hands a renderer.
    """

    def _db_with_candidate_evidence(
        self, *, lineage_version: int,
    ) -> tuple[FakePipelineDB, int]:
        """Seed one request + one download_log row pointing at candidate
        evidence carrying a verified-lossless proof and spectral facts."""
        from lib.quality import VERIFIED_LOSSLESS_CLASSIFIER_V4
        from lib.quality.evidence_types import VerifiedLosslessProof

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, mb_release_id="dl-uuid"))
        log_id = db.log_download(1, outcome="success")
        evidence = make_album_quality_evidence(
            mb_release_id="dl-uuid",
            lineage_version=lineage_version,
            codec="flac",
            container="flac",
            storage_format="FLAC",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=900,
                avg_bitrate_kbps=950,
                format="FLAC",
                spectral_grade="genuine",
                spectral_subject="source",
                spectral_provenance="measured",
            ),
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="measured",
                source="flac",
                classifier=VERIFIED_LOSSLESS_CLASSIFIER_V4,
            ),
        )
        db.upsert_album_quality_evidence(evidence)
        stored = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        db.set_download_log_candidate_evidence(log_id, stored.id)
        return db, log_id

    def test_legacy_lineage_never_lends_its_proof_to_any_reader(self):
        """Mirror of ``tests/test_pipeline_db.py``'s real-PG pin: a
        lineage-1 evidence row's proof is NOT this attempt's proof, so
        every evidence-joined reader must null the classifier alias."""
        db, log_id = self._db_with_candidate_evidence(lineage_version=1)
        rows = [
            db.get_download_log_entry(log_id),
            db.get_download_history(1)[0],
            db.get_download_history_batch([1])[1][0],
            next(row for row in db.get_log() if row["id"] == log_id),
            db.get_latest_download_summaries([1])[1]["latest"],
        ]
        for row in rows:
            assert row is not None
            self.assertIsNone(
                row["_evidence_verified_lossless_classifier"],
                "a legacy-lineage evidence row lent its proof to the "
                "renderer",
            )
            self.assertIsNone(row["source_format"])
            # Spectral facts were never target projections, so they
            # still recover from either lineage.
            self.assertEqual(row["spectral_grade"], "genuine")

    def test_source_semantic_lineage_still_lends_its_proof(self):
        """Must-still-work guard: the gate is lineage, not a blanket null."""
        db, log_id = self._db_with_candidate_evidence(lineage_version=5)
        row = db.get_download_log_entry(log_id)
        assert row is not None
        self.assertIsNotNone(row["_evidence_verified_lossless_classifier"])
        self.assertEqual(row["source_format"], "FLAC")

    def test_candidate_role_withholds_conversion_lineage_current_carries_it(self):
        """One evidence row linked as BOTH candidate and current must show
        ``was_converted_from`` only under the current prefix.

        Production spells this in SQL, unconditionally: for the candidate
        prefix ``accusation_evidence_columns`` emits ``NULL::text AS
        _evidence_was_converted_from`` while every other alias reads its
        column, and ``_LOG_QUERY_TEMPLATE`` separately projects
        ``current_evidence.was_converted_from AS
        _current_evidence_was_converted_from``. A canonical evidence row
        may be co-referenced as current, but its CANDIDATE role is always
        source semantics — installed-output conversion lineage is not a
        fact about the downloaded bytes.

        The fake's ``_accusation_alias_projection`` carve-out is the
        mirror of that, and a follow-up mutant proved nothing asserted it:
        deleting the carve-out passed every test in this module.
        """
        from lib.quality import AudioQualityMeasurement

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, mb_release_id="conv-1"))
        log_id = db.log_download(1, outcome="success")
        evidence = make_album_quality_evidence(
            mb_release_id="conv-1",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                format="MP3",
                spectral_grade="genuine",
                spectral_subject="source",
                spectral_provenance="measured",
                was_converted_from="mp3",
            ),
        )
        db.upsert_album_quality_evidence(evidence)
        stored = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        # The SAME row on both sides of the join — the exact shape the
        # carve-out exists for.
        db.set_download_log_candidate_evidence(log_id, stored.id)
        self.assertTrue(db.set_request_current_evidence(1, stored.id))

        row = next(r for r in db.get_log() if r["id"] == log_id)
        self.assertIsNone(
            row["_evidence_was_converted_from"],
            "the candidate role must withhold installed-output conversion "
            "lineage; production hardcodes NULL::text there",
        )
        self.assertEqual(
            row["_current_evidence_was_converted_from"], "mp3",
            "the current prefix is the one that carries the real column",
        )

    def test_linked_import_logs_carry_no_evidence_derived_values(self):
        """``get_linked_import_logs`` has NO evidence join in production
        (``lib/pipeline_db/download_log.py``), so its rows can never carry
        evidence-recovered spectral/V0/source facts however rich the
        linked evidence row is."""
        db, source_log_id = self._db_with_candidate_evidence(
            lineage_version=5)
        successor_id = db.log_download(
            1, outcome="force_import", source_download_log_id=source_log_id)
        db.set_download_log_candidate_evidence(
            successor_id, db.download_logs[0].candidate_evidence_id)

        rows = db.get_linked_import_logs([source_log_id])
        self.assertEqual([row["id"] for row in rows], [successor_id])
        row = rows[0]
        for key in (
            "spectral_grade", "spectral_bitrate", "v0_probe_kind",
            "v0_probe_min_bitrate", "v0_probe_avg_bitrate",
            "v0_probe_median_bitrate",
        ):
            self.assertIsNone(
                row.get(key),
                f"{key} was recovered from evidence production never joins",
            )
        for key in (
            "source_format", "source_min_bitrate", "source_avg_bitrate",
            "source_median_bitrate",
        ):
            self.assertNotIn(
                key, row,
                f"{key} is stamped only by the evidence overlay, which "
                "this reader never runs",
            )


class TestFakeDownloadLogCounts(unittest.TestCase):
    """State-derived mirror of PipelineDB.get_download_log_counts —
    parity with the real SQL is pinned in tests/test_pipeline_db.py."""

    def test_counts_derive_from_logged_state(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        db.log_download(1, outcome="success")
        db.log_download(1, outcome="force_import")
        db.log_download(1, outcome="rejected")
        db.log_search(1, outcome="found")
        db.log_search(1, outcome="found")
        db.log_search(1, outcome="error")
        # Age one found-row out of the 6h window only, one out of both.
        db.search_logs[0].created_at -= timedelta(hours=12)
        db.log_search(1, outcome="found")
        db.search_logs[-1].created_at -= timedelta(days=2)

        counts = db.get_download_log_counts()
        self.assertEqual(counts.total, 3)
        self.assertEqual(counts.imported, 2)
        self.assertEqual(counts.matches_24h, 2)
        self.assertEqual(counts.matches_6h, 1)


class TestFakeDownloadLogIdMint(unittest.TestCase):
    """Minted download_log ids mirror production's sequence-backed PK.

    A test that rewinds ``_next_download_log_id`` below an existing id
    used to mint duplicates silently — the three accessors then disagree
    (oldest vs max-id vs insertion order). The mint guard makes that a
    hard error (#445 item 4; previously a local assert in
    ``test_routes_imports._seed_wrong_match``).
    """

    def test_log_download_rejects_rewound_counter_collision(self):
        import psycopg2.errors

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        db.log_download(1, outcome="rejected")
        db._next_download_log_id = 0
        with self.assertRaises(psycopg2.errors.UniqueViolation):
            db.log_download(1, outcome="rejected")

    def test_log_download_rejects_regressed_id_even_without_collision(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        db._next_download_log_id = 4  # pin → next id is 5
        db.log_download(1, outcome="rejected")
        db._next_download_log_id = 1  # would mint 2 — a sequence never regresses
        with self.assertRaises(AssertionError):
            db.log_download(1, outcome="rejected")

    def test_insert_youtube_running_shares_the_mint_guard(self):
        import psycopg2.errors

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        db.log_download(1, outcome="rejected")
        db._next_download_log_id = 0
        with self.assertRaises(psycopg2.errors.UniqueViolation):
            db.insert_youtube_running(
                request_id=1, browse_id="b", audio_playlist_id=None,
                yt_url="u", expected_track_count=10)

    def test_forward_pinning_still_works(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        db.log_download(1, outcome="rejected")
        db._next_download_log_id = 41  # forward pin — ids stay monotonic
        self.assertEqual(db.log_download(1, outcome="rejected"), 42)


class TestFakeDownloadLogWrites(unittest.TestCase):
    """``log_download`` writes: canonical outcomes, source defaults, the
    transfer detail column, and the derived validation projection.
    """

    def test_log_download_rejects_non_canonical_outcome(self):
        """Mirror of download_log_outcome_check — the fake must reject
        exactly what production rejects (test-fidelity Rule A/B; the
        #146 grace escape shipped outcome='error' past a permissive
        fake and crashed on the real CHECK constraint)."""
        import psycopg2.errors
        db = FakePipelineDB()
        for outcome in ("error", "have_analysis_errors"):
            with self.subTest(outcome=outcome), self.assertRaises(psycopg2.errors.CheckViolation):
                db.log_download(42, outcome=outcome)

    def test_log_download_accepts_have_analysis_error(self):
        db = FakePipelineDB()
        log_id = db.log_download(42, outcome="have_analysis_error")

        self.assertEqual(log_id, 1)
        self.assertEqual(db.download_logs[0].outcome, "have_analysis_error")

    def test_log_download_records_transfer_detail(self):
        """Issue #564 C7: transfer_detail is a first-class field on
        DownloadLogRow, not swallowed into .extra."""
        db = FakePipelineDB()
        detail = [
            {"username": "user1", "filename": "01.flac",
             "last_state": "Completed, Errored",
             "last_exception": "Read error: Connection reset by peer",
             "bytes_transferred": 0, "retry_count": 2},
        ]
        db.log_download(42, outcome="timeout", transfer_detail=detail)

        self.assertEqual(db.download_logs[0].transfer_detail, detail)

    def test_log_download_source_defaults_to_slskd(self):
        """Migration 037's ``source`` discriminator: ``log_download``'s
        default must match the production NOT NULL DEFAULT ``'slskd'`` so
        every existing caller (none of which pass ``source=``) keeps
        writing the same row shape it always has."""
        db = FakePipelineDB()
        db.log_download(42, outcome="success")

        self.assertEqual(db.download_logs[0].source, "slskd")

    def test_log_download_records_explicit_source(self):
        """Issue #1176 PR1: a future ``import-local`` caller passes
        ``source='local'`` — the fake must record exactly what it was
        given, not silently default or drop it. This is the fake-side
        half of the ``source`` parameter; the real-PG round trip lives in
        ``tests/test_pipeline_db.py``."""
        db = FakePipelineDB()
        db.log_download(42, outcome="success", source="local")

        self.assertEqual(db.download_logs[0].source, "local")

    def test_log_download_derives_validation_projection_like_postgres(self):
        from lib.quality import ValidationResult

        db = FakePipelineDB()
        validation_result = ValidationResult(
            distance=0.0,
            scenario="untracked_audio",
        ).to_json()
        db.log_download(
            42,
            outcome="rejected",
            validation_result=validation_result,
        )

        log = db.download_logs[0]
        self.assertEqual(log.beets_distance, 0.0)
        self.assertEqual(log.beets_scenario, "untracked_audio")
        self.assertEqual(log.validation_result, validation_result)

    def test_log_download_derives_custom_envelope_scenario_like_postgres(self):
        db = FakePipelineDB()
        validation_result = {
            "scenario": "curator_ban",
            "hashes_recorded": 2,
        }
        db.log_download(
            42,
            outcome="curator_ban",
            validation_result=validation_result,
        )

        log = db.download_logs[0]
        self.assertEqual(log.beets_scenario, "curator_ban")
        self.assertIsNone(log.beets_distance)
        self.assertIs(log.validation_result, validation_result)

    def test_log_download_explicit_metadata_requires_missing_envelope_key(self):
        import msgspec

        from lib.quality import MeasurementFailure, ValidationResult

        db = FakePipelineDB()
        payload = MeasurementFailure(
            reason="measurement_crashed",
            detail="ffmpeg failed",
        )
        db.log_download(
            42,
            outcome="measurement_failed",
            beets_scenario="measurement_failed",
            validation_result=msgspec.json.encode(payload).decode(),
        )
        self.assertEqual(db.download_logs[-1].beets_scenario,
                         "measurement_failed")

        validation_result = ValidationResult(
            distance=0.1,
            scenario="high_distance",
        ).to_json()
        with self.assertRaisesRegex(ValueError, "beets_distance"):
            db.log_download(
                42,
                outcome="rejected",
                beets_distance=0.2,
                validation_result=validation_result,
            )
        with self.assertRaisesRegex(ValueError, "beets_scenario"):
            db.log_download(
                42,
                outcome="rejected",
                beets_scenario="wrong_value",
                validation_result=validation_result,
            )


class TestFakeDownloadLogReads(unittest.TestCase):
    """The download-log read models: history, filters, auxiliary columns,
    latest summaries (#426), and retained failure paths.
    """

    def test_download_log_history_and_lookup_by_id(self):
        db = FakePipelineDB()
        db.log_download(1, outcome="success")
        db.log_download(1, outcome="failed")
        db.log_download(2, outcome="rejected")

        history_1 = db.get_download_history(1)
        self.assertEqual([r["outcome"] for r in history_1],
                         ["failed", "success"])
        batch = db.get_download_history_batch([1, 2])
        self.assertEqual({k: [r["outcome"] for r in v]
                          for k, v in batch.items()},
                         {1: ["failed", "success"], 2: ["rejected"]})

        first_id = db.download_logs[0].id
        entry = db.get_download_log_entry(first_id)
        assert entry is not None
        self.assertEqual(entry["outcome"], "success")
        self.assertIsNone(db.get_download_log_entry(99999))

    def test_get_log_filters_and_orders_newest_first(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, album_title="Album A"))
        db.log_download(1, outcome="success")
        db.log_download(1, outcome="failed")
        db.log_download(1, outcome="rejected")
        all_rows = db.get_log()
        self.assertEqual([r["outcome"] for r in all_rows],
                         ["rejected", "failed", "success"])
        imported = db.get_log(outcome_filter="imported")
        self.assertEqual([r["outcome"] for r in imported], ["success"])
        rejected = db.get_log(outcome_filter="rejected")
        self.assertEqual([r["outcome"] for r in rejected],
                         ["rejected", "failed"])
        # Joined request columns present.
        self.assertEqual(all_rows[0]["album_title"], "Album A")

    def test_get_log_surfaces_auxiliary_columns(self):
        """Real ``get_log`` returns ``dl.*`` — every ``log_download``
        column must be present, including fields parked in
        ``entry.extra`` (bitrate, spectral_grade, final_format, etc.)
        Codex R2: callers that feed these rows into LogEntry.from_row
        would otherwise classify incomplete data (codex R2)."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        db.log_download(
            1, outcome="success",
            bitrate=256, spectral_grade="genuine",
            final_format="mp3 v0", actual_min_bitrate=245)
        rows = db.get_log()
        self.assertEqual(rows[0]["bitrate"], 256)
        self.assertEqual(rows[0]["spectral_grade"], "genuine")
        self.assertEqual(rows[0]["final_format"], "mp3 v0")
        self.assertEqual(rows[0]["actual_min_bitrate"], 245)

    def test_get_log_keeps_download_source_and_aliases_request_source(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, source="redownload"))
        slskd_id = db.log_download(1, outcome="success")
        yt_id = db.insert_youtube_running(
            request_id=1,
            browse_id="MPREb_fake_get_log",
            audio_playlist_id=None,
            yt_url="https://music.youtube.com/playlist?list=fake",
            expected_track_count=10,
        )
        rows = db.get_log()
        by_id = {row["id"]: row for row in rows}
        self.assertEqual(by_id[slskd_id]["source"], "slskd")
        self.assertEqual(by_id[slskd_id]["request_source"], "redownload")
        self.assertEqual(by_id[yt_id]["source"], "youtube")
        self.assertEqual(by_id[yt_id]["request_source"], "redownload")

    def test_get_latest_download_summaries_mirror(self):
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="T", source="request",
            mb_release_id="f-sum-1", status="wanted")
        db.log_download(rid, "old_user", "flac", "/tmp/1", outcome="rejected")
        db.log_download(rid, "new_user", "flac", "/tmp/2", outcome="success")

        summaries = db.get_latest_download_summaries([rid, 9999])
        self.assertEqual(set(summaries), {rid})
        self.assertEqual(summaries[rid]["count"], 2)
        self.assertEqual(
            summaries[rid]["latest"]["soulseek_username"], "new_user")

    def test_get_retained_failure_paths(self):
        db = FakePipelineDB()
        db.log_download(
            request_id=1,
            outcome="measurement_failed",
            staged_path="/downloads/retained",
        )
        db.log_download(
            request_id=1,
            outcome="measurement_failed",
            staged_path="",
        )
        db.log_download(
            request_id=1,
            outcome="rejected",
            staged_path="/downloads/rejected",
        )

        self.assertEqual(
            db.get_retained_failure_paths(),
            {"/downloads/retained"},
        )


class TestFakeWrongMatchQueue(unittest.TestCase):
    """The Wrong Matches queue is a collapse over download_log rows keyed
    by ``validation_result->>'failed_path'``, plus the clear and triage
    writers that maintain it.
    """

    def test_get_wrong_matches_collapses_per_request_and_path(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, artist_name="A", album_title="B"))
        # Two rejections on the same (request, failed_path) — keep newest.
        db.log_download(1, outcome="rejected",
                        validation_result={"failed_path": "/p1"})
        db.log_download(1, outcome="rejected",
                        validation_result={"failed_path": "/p1"})
        # Different path — separate row.
        db.log_download(1, outcome="rejected",
                        validation_result={"failed_path": "/p2"})
        # Scenario filtered out.
        db.log_download(1, outcome="rejected", validation_result={
            "failed_path": "/p3", "scenario": "audio_corrupt"})
        # Non-rejected — ignored.
        db.log_download(1, outcome="success",
                        validation_result={"failed_path": "/p4"})

        rows = db.get_wrong_matches()
        paths = sorted([
            (r["validation_result"] or {}).get("failed_path")  # type: ignore[union-attr]
            for r in rows])
        self.assertEqual(paths, ["/p1", "/p2"])

    def test_get_wrong_matches_newer_terminal_corrupt_hides_same_path(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, artist_name="A", album_title="B"))
        db.seed_request(make_request_row(id=2, artist_name="C", album_title="D"))
        db.log_download(1, soulseek_username="older-legitimate", outcome="rejected",
                        validation_result={"failed_path": "/same", "scenario": "high_distance"})
        db.log_download(1, soulseek_username="newer-corrupt", outcome="rejected",
                        validation_result={"failed_path": "/same", "scenario": "strong_match"},
                        import_result={"decision": "audio_corrupt"})
        other_id = db.log_download(2, soulseek_username="other-legitimate", outcome="rejected",
                                   validation_result={"failed_path": "/other", "scenario": "high_distance"})
        rows = db.get_wrong_matches()
        self.assertEqual(
            [(row["download_log_id"], row["soulseek_username"]) for row in rows],
            [(other_id, "other-legitimate")],
        )

    def test_get_wrong_matches_newer_legitimate_reuse_surfaces_after_corrupt(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, artist_name="A", album_title="B"))
        db.log_download(1, soulseek_username="older-corrupt", outcome="rejected",
                        validation_result={"failed_path": "/same", "scenario": "strong_match"},
                        import_result={"decision": "audio_corrupt"})
        newest_id = db.log_download(1, soulseek_username="newer-legitimate", outcome="rejected",
                                    validation_result={"failed_path": "/same", "scenario": "high_distance"})
        rows = db.get_wrong_matches()
        self.assertEqual(
            [(row["download_log_id"], row["soulseek_username"]) for row in rows],
            [(newest_id, "newer-legitimate")],
        )

    def test_get_wrong_matches_excludes_every_non_match_rejection_scenario(self):
        from lib.wrong_match_policy import WRONG_MATCH_EXCLUDED_REJECTION_SCENARIOS

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, artist_name="A", album_title="B"))
        db.log_download(1, outcome="rejected", validation_result={
            "failed_path": "/keep", "scenario": "high_distance"})
        for index, scenario in enumerate(
            sorted(WRONG_MATCH_EXCLUDED_REJECTION_SCENARIOS)
        ):
            db.log_download(1, outcome="rejected", validation_result={
                "failed_path": f"/drop-{index}", "scenario": scenario})

        rows = db.get_wrong_matches()
        self.assertEqual(len(rows), 1)
        validation_result = rows[0]["validation_result"]
        assert isinstance(validation_result, dict)
        self.assertEqual(validation_result["failed_path"], "/keep")

    def test_clear_wrong_match_path_strips_key(self):
        db = FakePipelineDB()
        db.log_download(1, outcome="rejected",
                        validation_result={"failed_path": "/p1",
                                           "scenario": "wrong_match"})
        log_id = db.download_logs[0].id
        self.assertTrue(db.clear_wrong_match_path(log_id))
        vr = db.download_logs[0].validation_result
        assert isinstance(vr, dict)
        self.assertNotIn("failed_path", vr)
        self.assertEqual(vr["scenario"], "wrong_match")
        # Second call returns False (already stripped).
        self.assertFalse(db.clear_wrong_match_path(log_id))

    def test_clear_wrong_match_path_handles_json_string(self):
        """Real ``validation_result`` is JSONB — fakes also accept JSON
        strings so tests can pass either shape."""
        import json as _json
        db = FakePipelineDB()
        db.log_download(1, outcome="rejected",
                        validation_result=_json.dumps(
                            {"failed_path": "/p", "x": 1}))
        self.assertTrue(
            db.clear_wrong_match_path(db.download_logs[0].id))
        stored = _json.loads(db.download_logs[0].validation_result)
        self.assertNotIn("failed_path", stored)

    def test_clear_wrong_match_paths_clears_matching_request_and_paths(self):
        db = FakePipelineDB()
        db.log_download(1, outcome="rejected",
                        validation_result={"failed_path": "failed_imports/A",
                                           "x": 1})
        db.log_download(1, outcome="rejected",
                        validation_result={"failed_path": "/abs/A",
                                           "x": 2})
        db.log_download(1, outcome="rejected",
                        validation_result={"failed_path": "/abs/B",
                                           "x": 3})
        db.log_download(2, outcome="rejected",
                        validation_result={"failed_path": "/abs/A",
                                           "x": 4})
        db.log_download(1, outcome="success",
                        validation_result={"failed_path": "/abs/A",
                                           "x": 5})

        cleared = db.clear_wrong_match_paths(
            1, ["failed_imports/A", "/abs/A"])

        self.assertEqual(cleared, 2)
        rows = db.get_wrong_matches()
        remaining = {
            (row["request_id"], row["validation_result"]["failed_path"])  # type: ignore[index]
            for row in rows
        }
        self.assertEqual(remaining, {(1, "/abs/B"), (2, "/abs/A")})

    def test_clear_wrong_match_paths_handles_json_string_payloads(self):
        import json as _json
        db = FakePipelineDB()
        db.log_download(1, outcome="rejected",
                        validation_result=_json.dumps(
                            {"failed_path": "/p", "x": 1}))

        cleared = db.clear_wrong_match_paths(1, ["/p"])

        self.assertEqual(cleared, 1)
        stored = _json.loads(db.download_logs[0].validation_result)
        self.assertNotIn("failed_path", stored)
        self.assertEqual(stored["x"], 1)

    def test_record_wrong_match_triage_merges_typed_audit(self):
        """Mirrors the real jsonb_set writer: typed audit in, omit-defaults
        dict merged onto the existing blob."""
        from lib.validation_envelope import (
            WrongMatchTriageAudit,
            decode_validation_envelope,
        )
        db = FakePipelineDB()
        db.log_download(1, outcome="rejected",
                        validation_result={"failed_path": "/p1",
                                           "scenario": "wrong_match"})
        log_id = db.download_logs[0].id
        audit = WrongMatchTriageAudit(action="deleted_reject", success=True)
        self.assertTrue(db.record_wrong_match_triage(log_id, audit))

        vr = db.download_logs[0].validation_result
        assert isinstance(vr, dict)
        # omit_defaults parity with the real writer — unset fields absent.
        self.assertEqual(vr["wrong_match_triage"],
                         {"action": "deleted_reject", "success": True})
        # Merge, not replace.
        self.assertEqual(vr["failed_path"], "/p1")
        env = decode_validation_envelope(vr)
        self.assertEqual(env.wrong_match_triage, audit)
        # Unknown log id returns False.
        self.assertFalse(db.record_wrong_match_triage(99999, audit))


if __name__ == "__main__":
    unittest.main()
