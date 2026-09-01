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


if __name__ == "__main__":
    unittest.main()
