"""Layer 1 of the test-fidelity hardening (#382): write-payload column contract.

For every typed write payload that maps to a table, assert its persisted
column names are a SUBSET of that table's real columns (queried live from
``information_schema``). This catches the "field present in the payload but
absent from the table" half of the ``album_title`` drift class: adding a
field to ``AddRequestInput`` (or renaming a column under a spectral/V0 update)
without a matching migration fails here, loudly, instead of silently writing
to / reading back ``NULL``.

It pairs with two sibling guards that close the other half (a column the table
has but the write omits):
  * ``add_request`` derives its INSERT column list directly from
    ``AddRequestInput``'s fields (``lib/pipeline_db/requests.py``), so the SQL
    can't omit a payload field; and
  * the Layer-2 round-trip write audit (``test_pipeline_db_write_audit.py``)
    requires a real-PG round-trip per write method.

Follows the ephemeral-PostgreSQL harness used by ``test_pipeline_db.py``
(conftest boots PG + applies migrations; in the dev shell the DSN is always
set, so this never skips — see CLAUDE.md § "Skipped tests are an anti-pattern").
"""
import dataclasses
import json
import os
import sys
import unittest
from datetime import datetime
from typing import cast

import msgspec

sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401 — sets TEST_DB_DSN env var

from lib.pipeline_db import (
    AddRequestInput,
    AlbumRequestRow,
    BadAudioHashInput,
    DownloadLogRow,
    DownloadLogWithEvidenceRow,
    DownloadLogWithOriginRow,
    DownloadLogWithRequestRow,
    PersistedYoutubeRow,
    PipelineDB,
    RequestSpectralStateUpdate,
    RequestV0ProbeStateUpdate,
    TransferLedgerRow,
    WrongMatchCandidateRow,
    album_request_row,
    download_log_row,
    download_log_with_evidence_row,
    download_log_with_origin_row,
    download_log_with_request_row,
    wrong_match_candidate_row,
)
from tests.helpers import (
    make_album_quality_evidence,
    make_import_result,
    make_request_row,
    make_validation_result,
)
from tests.test_pipeline_db import make_db, requires_postgres
from lib.quality import (
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    SpectralMeasurement,
    V0ProbeEvidence,
)

# conftest boots an ephemeral PostgreSQL and exports TEST_DB_DSN for the whole
# suite, so this runs unconditionally — NO skip gate (CLAUDE.md § "Skipped tests
# are an anti-pattern"; the programmatic `unittest.skip()` form used elsewhere is
# also invisible to test_skip_audit.py). If the DSN is genuinely absent,
# setUpClass's connection fails loudly — the intended "a test runs or it doesn't
# exist" behaviour.
TEST_DSN = os.environ.get("TEST_DB_DSN")


def _dataclass_columns(struct_cls) -> set[str]:
    """A flat write dataclass whose field names ARE column names."""
    return {f.name for f in dataclasses.fields(struct_cls)}


def _struct_columns(struct_cls) -> set[str]:
    """A flat write ``msgspec.Struct`` whose field names ARE column names."""
    return {f.name for f in msgspec.structs.fields(struct_cls)}


def _spectral_update_columns() -> set[str]:
    """The album_requests columns a fully-populated spectral update writes."""
    sm = SpectralMeasurement(grade="EXCELLENT", bitrate_kbps=900)
    update = RequestSpectralStateUpdate(last_download=sm, current=sm)
    return set(update.as_update_fields().keys())


def _v0_update_columns() -> set[str]:
    """The album_requests columns a fully-populated V0-probe update writes."""
    v0 = V0ProbeEvidence(
        kind="cbr", min_bitrate_kbps=320,
        avg_bitrate_kbps=320, median_bitrate_kbps=320,
    )
    update = RequestV0ProbeStateUpdate(current_lossless_source=v0)
    return set(update.as_update_fields().keys())


# (payload label, target table, the column names the payload persists)
CONTRACTS: list[tuple[str, str, set[str]]] = [
    ("AddRequestInput", "album_requests", _dataclass_columns(AddRequestInput)),
    ("BadAudioHashInput", "bad_audio_hashes", _dataclass_columns(BadAudioHashInput)),
    ("RequestSpectralStateUpdate", "album_requests", _spectral_update_columns()),
    ("RequestV0ProbeStateUpdate", "album_requests", _v0_update_columns()),
    ("PersistedYoutubeRow", "youtube_album_mappings",
     _struct_columns(PersistedYoutubeRow)),
    ("TransferLedgerRow", "slskd_transfer_ledger",
     _struct_columns(TransferLedgerRow)),
]


class TestWritePayloadColumnContract(unittest.TestCase):
    db: PipelineDB

    @classmethod
    def setUpClass(cls) -> None:
        cls.db = PipelineDB(TEST_DSN)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.db.close()

    def _table_columns(self, table: str) -> set[str]:
        cur = self.db._execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = %s",
            (table,),
        )
        return {r["column_name"] for r in cur.fetchall()}

    def test_payload_columns_are_a_subset_of_table_columns(self) -> None:
        for label, table, cols in CONTRACTS:
            with self.subTest(payload=label):
                table_cols = self._table_columns(table)
                self.assertTrue(
                    table_cols, f"table {table!r} reports no columns")
                missing = cols - table_cols
                self.assertEqual(
                    missing, set(),
                    f"{label} persists column(s) {sorted(missing)} that do "
                    f"not exist on {table!r}. Add a migration for them, or fix "
                    f"the payload field name. (#382 Layer 1 column contract.)",
                )

    def test_contracts_are_non_empty(self) -> None:
        """Guard the guard: a payload that introspected to zero columns
        (e.g. a renamed dataclass) would make the subset check vacuous."""
        for label, _table, cols in CONTRACTS:
            with self.subTest(payload=label):
                self.assertTrue(cols, f"{label} resolved to no columns")

    def test_album_request_row_matches_table_columns_exactly(self) -> None:
        """Read-projection parity (#765 phase 6): ``AlbumRequestRow`` is the
        typed view of ``SELECT * FROM album_requests``, so its keys must
        EQUAL the table's columns — a new migration column fails here until
        the row type learns it in the same PR (and vice versa)."""
        table_cols = self._table_columns("album_requests")
        row_keys = set(AlbumRequestRow.__annotations__)
        self.assertEqual(
            row_keys, table_cols,
            "AlbumRequestRow drifted from album_requests: "
            f"missing={sorted(table_cols - row_keys)} "
            f"stale={sorted(row_keys - table_cols)}",
        )

    def test_make_request_row_builder_matches_row_type(self) -> None:
        """The shared builder must produce every ``AlbumRequestRow`` key —
        production-shape fidelity for every test that seeds request rows
        (this immediately caught the builder lacking ``final_format``)."""
        built = set(make_request_row())
        row_keys = set(AlbumRequestRow.__annotations__)
        self.assertEqual(
            built, row_keys,
            f"builder missing={sorted(row_keys - built)} "
            f"extra={sorted(built - row_keys)}",
        )

    def test_builder_row_survives_the_runtime_validator(self) -> None:
        """``album_request_row`` must accept a production-shaped builder row
        — pins the msgspec-convert boundary against value-type drift."""
        converted = album_request_row(make_request_row())
        self.assertEqual(converted["status"], "wanted")

    def test_bogus_row_is_rejected_by_the_runtime_validator(self) -> None:
        """Known-bad self-test: wrong-typed column value must raise."""
        bad = dict(make_request_row())
        bad["search_attempts"] = "three"
        with self.assertRaises(msgspec.ValidationError):
            album_request_row(bad)

    def test_bogus_struct_field_is_caught_by_the_subset_check(self) -> None:
        """Known-bad self-test: a synthetic ``msgspec.Struct`` with one
        bogus field NOT on ``youtube_album_mappings`` must be reported by
        the subset check as missing — proves the guard has teeth rather
        than vacuously passing every payload."""
        class _BogusYoutubeRow(msgspec.Struct, kw_only=True):
            yt_browse_id: str = ""
            definitely_not_a_real_column: str = ""

        table_cols = self._table_columns("youtube_album_mappings")
        bogus_cols = _struct_columns(_BogusYoutubeRow)
        missing = bogus_cols - table_cols
        self.assertEqual(missing, {"definitely_not_a_real_column"})

    def test_download_log_row_matches_table_columns_exactly(self) -> None:
        """Read-projection parity (#765 phase 6 continuation):
        ``DownloadLogRow`` is the typed view of
        ``SELECT * FROM download_log``, so its keys must EQUAL the table's
        columns — a new migration column fails here until the row type
        learns it in the same PR (and vice versa)."""
        table_cols = self._table_columns("download_log")
        row_keys = set(DownloadLogRow.__annotations__)
        self.assertEqual(
            row_keys, table_cols,
            "DownloadLogRow drifted from download_log: "
            f"missing={sorted(table_cols - row_keys)} "
            f"stale={sorted(row_keys - table_cols)}",
        )


@requires_postgres
class TestDownloadLogRowRuntimeContract(unittest.TestCase):
    """Real-PG round-trip + known-bad tests for ``download_log_row`` —
    Rule A of test-fidelity.md: a fake stores whatever dict shape a test
    hands it, so only a live-PG round-trip proves the write survives the
    ``information_schema`` boundary and the read-side validator accepts
    the actual production shape."""

    def setUp(self) -> None:
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="download-log-row-uuid",
            artist_name="Contract Artist",
            album_title="Contract Album",
            source="request",
        )

    def tearDown(self) -> None:
        self.db.close()

    def _raw_row(self, log_id: int) -> dict[str, object]:
        cur = self.db._execute(
            "SELECT * FROM download_log WHERE id = %s", (log_id,)
        )
        row = cur.fetchone()
        assert row is not None, f"download_log row {log_id} not found"
        return dict(row)

    def test_download_log_row_round_trips_a_production_shaped_row(self) -> None:
        """A row populated through every ``log_download`` kwarg the writer
        exposes must survive ``download_log_row`` — pins the msgspec-convert
        boundary against value-type drift on the real column set (including
        the two JSONB-object columns and the JSONB-ARRAY ``transfer_detail``
        column, verified empirically against ``lib/download.py``'s writer)."""
        validation_result = make_validation_result().to_json()
        import_result = make_import_result().to_json()
        log_id = self.db.log_download(
            request_id=self.req_id,
            soulseek_username="contract-user",
            filetype="flac",
            download_path="/mnt/virtio/Music/Incoming/auto-import/x",
            beets_detail="exact match",
            valid=True,
            outcome="success",
            staged_path="/mnt/virtio/Music/Incoming/auto-import/x",
            bitrate=1000,
            sample_rate=44100,
            bit_depth=16,
            is_vbr=False,
            was_converted=True,
            original_filetype="mp3",
            slskd_filetype="flac",
            actual_filetype="flac",
            actual_min_bitrate=1000,
            spectral_grade="EXCELLENT",
            spectral_bitrate=900,
            existing_min_bitrate=800,
            existing_spectral_bitrate=700,
            import_result=import_result,
            validation_result=validation_result,
            final_format="flac",
            v0_probe_kind="lossless_source_v0",
            v0_probe_min_bitrate=320,
            v0_probe_avg_bitrate=320,
            v0_probe_median_bitrate=320,
            existing_v0_probe_kind="native_lossy_research_v0",
            existing_v0_probe_min_bitrate=320,
            existing_v0_probe_avg_bitrate=320,
            existing_v0_probe_median_bitrate=320,
            transfer_detail=[
                {"filename": "01 track.flac", "reason": "timed_out"},
            ],
        )

        converted = download_log_row(self._raw_row(log_id))

        self.assertEqual(converted["id"], log_id)
        self.assertEqual(converted["request_id"], self.req_id)
        self.assertEqual(converted["soulseek_username"], "contract-user")
        self.assertEqual(converted["outcome"], "success")
        self.assertEqual(converted["source"], "slskd")
        self.assertIsNone(converted["youtube_metadata"])
        self.assertIsNone(converted["candidate_evidence_id"])
        self.assertIsInstance(converted["created_at"], datetime)
        converted_import_result = converted["import_result"]
        assert converted_import_result is not None
        self.assertEqual(converted_import_result["decision"], "import")
        converted_validation_result = converted["validation_result"]
        assert converted_validation_result is not None
        self.assertEqual(
            converted_validation_result["scenario"], "strong_match")
        self.assertEqual(
            converted["transfer_detail"],
            [{"filename": "01 track.flac", "reason": "timed_out"}],
        )

    def test_download_log_row_round_trips_a_youtube_queue_row(self) -> None:
        """A ``source='youtube'`` row carries ``youtube_metadata`` (a JSON
        object) and leaves every slskd-only column NULL — both are valid
        ``DownloadLogRow`` shapes, so the validator must accept this one
        too, not just the slskd-heavy shape above."""
        log_id = self.db.insert_youtube_running(
            request_id=self.req_id,
            browse_id="MPREb_contract",
            audio_playlist_id=None,
            yt_url="https://music.youtube.com/playlist?list=OLAK5uy_contract",
            expected_track_count=10,
        )
        converted = download_log_row(self._raw_row(log_id))
        self.assertEqual(converted["source"], "youtube")
        self.assertEqual(converted["outcome"], "youtube_running")
        converted_youtube_metadata = converted["youtube_metadata"]
        assert converted_youtube_metadata is not None
        self.assertEqual(
            converted_youtube_metadata["browse_id"], "MPREb_contract")
        self.assertIsNone(converted["spectral_grade"])
        self.assertIsNone(converted["beets_distance"])

    def test_bogus_download_log_row_is_rejected_by_the_runtime_validator(
        self,
    ) -> None:
        """Known-bad self-test: wrong-typed column value must raise."""
        log_id = self.db.log_download(
            request_id=self.req_id,
            outcome="rejected",
        )
        bad = self._raw_row(log_id)
        bad["request_id"] = "not-an-int"
        with self.assertRaises(msgspec.ValidationError):
            download_log_row(bad)

    def test_bogus_transfer_detail_shape_is_rejected(self) -> None:
        """Known-bad self-test targeting the one column where the generic
        jsonb -> dict mapping does NOT hold: a bare JSON object (instead of
        the real JSON-array shape) must be rejected, proving the ``list``
        annotation has teeth rather than silently widening to accept
        anything ``jsonb`` can hold."""
        log_id = self.db.log_download(
            request_id=self.req_id,
            outcome="timeout",
        )
        bad = self._raw_row(log_id)
        bad["transfer_detail"] = {"not": "a list"}
        with self.assertRaises(msgspec.ValidationError):
            download_log_row(bad)


@requires_postgres
class TestDownloadLogWithEvidenceRowRuntimeContract(unittest.TestCase):
    """Real-PG round-trip + known-bad tests for
    ``download_log_with_evidence_row`` (issue #784 continuation of #765
    phase 6) — the shared ``dl.* LEFT JOIN album_quality_evidence``
    projection behind ``get_download_log_entry``, ``get_download_history``,
    ``get_download_history_batch``, and ``get_latest_download_summaries``.
    The parity source is a REAL EXECUTED QUERY against the ephemeral PG,
    not a hand-typed key list — a new join column (or a dropped one) must
    be reflected in the row type in the same PR."""

    def setUp(self) -> None:
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="evidence-row-uuid",
            artist_name="Evidence Row Artist",
            album_title="Evidence Row Album",
            source="request",
        )

    def tearDown(self) -> None:
        self.db.close()

    def _seed_full_row(self) -> DownloadLogWithEvidenceRow:
        """One download_log row exercising every extra field this row
        type carries: a source-semantic (lineage v4) candidate evidence
        (source_* + spectral + V0 overlay) and an origin chain
        (source_download_log_id -> original_beets_distance)."""
        source_id = self.db.log_download(
            self.req_id, "source-peer", "flac", "/failed/source",
            outcome="rejected",
            validation_result=json.dumps({
                "scenario": "high_distance", "distance": 0.2328,
            }),
        )
        log_id = self.db.log_download(
            self.req_id, "source-peer", "flac", "/failed/source",
            outcome="force_import",
            source_download_log_id=source_id,
        )
        candidate = make_album_quality_evidence(
            mb_release_id="evidence-row-candidate",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=190, avg_bitrate_kbps=201,
                median_bitrate_kbps=198, format="MP3",
                spectral_grade="genuine",
                spectral_bitrate_kbps=320,
                spectral_subject="installed",
                spectral_provenance="measured",
            ),
            v0_metric=AlbumQualityV0Metric(
                subject="installed", min_bitrate_kbps=300,
                avg_bitrate_kbps=310, median_bitrate_kbps=305,
            ),
        )
        self.db.upsert_album_quality_evidence(candidate)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=candidate.mb_release_id,
            snapshot_fingerprint=candidate.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.db.set_download_log_candidate_evidence(log_id, stored.id)

        row = self.db.get_download_log_entry(log_id)
        assert row is not None
        return row

    def test_get_download_log_entry_matches_download_log_with_evidence_row(
        self,
    ) -> None:
        row = self._seed_full_row()
        self.assertEqual(
            set(row.keys()), set(DownloadLogWithEvidenceRow.__annotations__),
            "get_download_log_entry() drifted from DownloadLogWithEvidenceRow",
        )
        self.assertEqual(row["source_format"], "MP3")
        self.assertEqual(row["source_min_bitrate"], 190)
        self.assertAlmostEqual(cast(float, row["original_beets_distance"]), 0.2328)

    def test_get_download_history_matches_download_log_with_evidence_row(
        self,
    ) -> None:
        self._seed_full_row()
        rows = self.db.get_download_history(self.req_id)
        self.assertTrue(rows, "seeding produced no history rows")
        for row in rows:
            self.assertEqual(
                set(row.keys()),
                set(DownloadLogWithEvidenceRow.__annotations__),
                "get_download_history() drifted from DownloadLogWithEvidenceRow",
            )

    def test_get_download_history_batch_matches_download_log_with_evidence_row(
        self,
    ) -> None:
        self._seed_full_row()
        batch = self.db.get_download_history_batch([self.req_id])
        rows = batch[self.req_id]
        self.assertTrue(rows, "seeding produced no batch rows")
        for row in rows:
            self.assertEqual(
                set(row.keys()),
                set(DownloadLogWithEvidenceRow.__annotations__),
                "get_download_history_batch() drifted from "
                "DownloadLogWithEvidenceRow",
            )

    def test_get_latest_download_summaries_matches_download_log_with_evidence_row(
        self,
    ) -> None:
        self._seed_full_row()
        summaries = self.db.get_latest_download_summaries([self.req_id])
        latest = summaries[self.req_id]["latest"]
        self.assertEqual(
            set(latest.keys()), set(DownloadLogWithEvidenceRow.__annotations__),
            "get_latest_download_summaries() drifted from "
            "DownloadLogWithEvidenceRow",
        )

    def test_source_fields_are_always_present_without_evidence(self) -> None:
        """Known-good self-test for the always-present-but-nullable
        guarantee (#784): ``source_format`` et al. are NOT real
        ``download_log`` columns, so without ANY candidate evidence the
        overlay must still stamp them (as ``None``) rather than leaving
        the key silently absent."""
        log_id = self.db.log_download(
            self.req_id, "no-evidence-peer", "mp3", "/tmp/x",
            outcome="rejected",
        )
        row = self.db.get_download_log_entry(log_id)
        assert row is not None
        self.assertEqual(
            set(row.keys()), set(DownloadLogWithEvidenceRow.__annotations__),
        )
        self.assertIsNone(row["source_format"])
        self.assertIsNone(row["source_min_bitrate"])
        self.assertIsNone(row["source_avg_bitrate"])
        self.assertIsNone(row["source_median_bitrate"])

    def test_bogus_row_is_rejected_by_download_log_with_evidence_row(
        self,
    ) -> None:
        """Known-bad self-test: wrong-typed column value must raise."""
        row = self._seed_full_row()
        bad = dict(row)
        bad["source_min_bitrate"] = "not-an-int"
        with self.assertRaises(msgspec.ValidationError):
            download_log_with_evidence_row(bad)


@requires_postgres
class TestDownloadLogWithRequestRowRuntimeContract(unittest.TestCase):
    """Real-PG round-trip + known-bad tests for
    ``download_log_with_request_row`` — the ``get_log()`` joined
    projection (issue #784 continuation of #765 phase 6)."""

    def setUp(self) -> None:
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="request-row-uuid",
            artist_name="Request Row Artist",
            album_title="Request Row Album",
            source="request",
            year=1999,
            country="US",
        )

    def tearDown(self) -> None:
        self.db.close()

    def _seed_full_row(self) -> DownloadLogWithRequestRow:
        """A ``get_log()`` row exercising every extra field: the
        request's CURRENT evidence (distinct from the per-candidate
        evidence below), a source-semantic candidate evidence, and an
        origin chain."""
        current = make_album_quality_evidence(
            mb_release_id="request-row-current",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=200, avg_bitrate_kbps=256,
                median_bitrate_kbps=250, format="FLAC",
                spectral_grade="genuine",
                spectral_bitrate_kbps=900,
                spectral_subject="installed",
                spectral_provenance="measured",
            ),
            storage_format="FLAC",
            v0_metric=AlbumQualityV0Metric(
                subject="installed", min_bitrate_kbps=300,
                avg_bitrate_kbps=310, median_bitrate_kbps=305,
            ),
        )
        self.db.upsert_album_quality_evidence(current)
        current_stored = self.db.find_album_quality_evidence(
            mb_release_id=current.mb_release_id,
            snapshot_fingerprint=current.snapshot_fingerprint,
        )
        assert current_stored is not None and current_stored.id is not None
        self.assertTrue(self.db.set_request_current_evidence(
            self.req_id, current_stored.id))

        source_id = self.db.log_download(
            self.req_id, "source-peer", "flac", "/failed/source",
            outcome="rejected",
            validation_result=json.dumps({
                "scenario": "high_distance", "distance": 0.2328,
            }),
        )
        log_id = self.db.log_download(
            self.req_id, "source-peer", "flac", "/failed/source",
            outcome="force_import",
            source_download_log_id=source_id,
        )
        candidate = make_album_quality_evidence(
            mb_release_id="request-row-candidate",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=190, avg_bitrate_kbps=201,
                median_bitrate_kbps=198, format="MP3",
            ),
        )
        self.db.upsert_album_quality_evidence(candidate)
        candidate_stored = self.db.find_album_quality_evidence(
            mb_release_id=candidate.mb_release_id,
            snapshot_fingerprint=candidate.snapshot_fingerprint,
        )
        assert candidate_stored is not None and candidate_stored.id is not None
        self.db.set_download_log_candidate_evidence(log_id, candidate_stored.id)

        rows = self.db.get_log(limit=50)
        return next(r for r in rows if r["id"] == log_id)

    def test_get_log_matches_download_log_with_request_row_exactly(
        self,
    ) -> None:
        row = self._seed_full_row()
        self.assertEqual(
            set(row.keys()), set(DownloadLogWithRequestRow.__annotations__),
            "get_log() drifted from DownloadLogWithRequestRow — a new "
            "join column (or a dropped one) must be reflected in the "
            "row type in the same PR.",
        )
        self.assertEqual(row["source_format"], "MP3")
        self.assertEqual(row["_current_evidence_spectral_grade"], "genuine")
        self.assertAlmostEqual(
            cast(float, row["original_beets_distance"]), 0.2328)
        self.assertEqual(row["album_title"], "Request Row Album")
        self.assertEqual(row["request_source"], "request")

    def test_bogus_row_is_rejected_by_download_log_with_request_row(
        self,
    ) -> None:
        """Known-bad self-test: wrong-typed column value must raise."""
        row = self._seed_full_row()
        bad = dict(row)
        bad["request_status"] = 12345
        with self.assertRaises(msgspec.ValidationError):
            download_log_with_request_row(bad)


@requires_postgres
class TestDownloadLogWithOriginRowRuntimeContract(unittest.TestCase):
    """Real-PG round-trip + known-bad tests for
    ``download_log_with_origin_row`` — ``get_linked_import_logs``'s
    narrower, no-evidence-join projection (issue #784)."""

    def setUp(self) -> None:
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="origin-row-uuid",
            artist_name="Origin Row Artist",
            album_title="Origin Row Album",
            source="request",
        )

    def tearDown(self) -> None:
        self.db.close()

    def _seed_linked_row(self) -> DownloadLogWithOriginRow:
        source_id = self.db.log_download(
            self.req_id, "source-peer", "flac", "/failed/source",
            outcome="rejected",
            validation_result=json.dumps({
                "scenario": "high_distance", "distance": 0.2328,
            }),
        )
        linked_id = self.db.log_download(
            self.req_id, "source-peer", "flac", "/failed/source",
            outcome="force_import",
            source_download_log_id=source_id,
        )
        rows = self.db.get_linked_import_logs([source_id])
        return next(r for r in rows if r["id"] == linked_id)

    def test_get_linked_import_logs_matches_download_log_with_origin_row(
        self,
    ) -> None:
        row = self._seed_linked_row()
        self.assertEqual(
            set(row.keys()), set(DownloadLogWithOriginRow.__annotations__),
            "get_linked_import_logs() drifted from DownloadLogWithOriginRow",
        )
        self.assertAlmostEqual(
            cast(float, row["original_beets_distance"]), 0.2328)
        # No evidence join for this reader — the source_* fields
        # DownloadLogWithEvidenceRow carries must NOT appear here.
        self.assertNotIn("source_format", row)

    def test_bogus_row_is_rejected_by_download_log_with_origin_row(
        self,
    ) -> None:
        """Known-bad self-test: wrong-typed column value must raise."""
        row = self._seed_linked_row()
        bad = dict(row)
        bad["original_beets_distance"] = "not-a-float"
        with self.assertRaises(msgspec.ValidationError):
            download_log_with_origin_row(bad)


@requires_postgres
class TestWrongMatchCandidateRowRuntimeContract(unittest.TestCase):
    """Real-PG round-trip + known-bad tests for
    ``wrong_match_candidate_row`` — ``get_wrong_matches``'s standalone
    (non-``dl.*``) projection (issue #784)."""

    def setUp(self) -> None:
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="wrong-match-row-uuid",
            artist_name="Wrong Match Artist",
            album_title="Wrong Match Album",
            source="request",
        )

    def tearDown(self) -> None:
        self.db.close()

    def _seed_candidate_row(self) -> WrongMatchCandidateRow:
        log_id = self.db.log_download(
            self.req_id, "wrong-match-peer", "mp3", "/fi/wrong-match",
            outcome="rejected",
            validation_result=json.dumps({
                "scenario": "high_distance", "distance": 0.4,
                "failed_path": "/fi/wrong-match",
            }),
        )
        candidate = make_album_quality_evidence(
            mb_release_id="wrong-match-candidate",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=128, avg_bitrate_kbps=130,
                median_bitrate_kbps=129, format="MP3",
                spectral_grade="likely_transcode",
                spectral_subject="installed",
                spectral_provenance="measured",
            ),
        )
        self.db.upsert_album_quality_evidence(candidate)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=candidate.mb_release_id,
            snapshot_fingerprint=candidate.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.db.set_download_log_candidate_evidence(log_id, stored.id)

        rows = self.db.get_wrong_matches()
        return next(r for r in rows if r["download_log_id"] == log_id)

    def test_get_wrong_matches_matches_wrong_match_candidate_row(self) -> None:
        row = self._seed_candidate_row()
        self.assertEqual(
            set(row.keys()), set(WrongMatchCandidateRow.__annotations__),
            "get_wrong_matches() drifted from WrongMatchCandidateRow",
        )
        self.assertEqual(row["evidence_source_codec"], "mp3")
        self.assertEqual(row["spectral_grade"], "likely_transcode")
        self.assertEqual(row["album_title"], "Wrong Match Album")

    def test_bogus_row_is_rejected_by_wrong_match_candidate_row(self) -> None:
        """Known-bad self-test: wrong-typed column value must raise."""
        row = self._seed_candidate_row()
        bad = dict(row)
        bad["request_id"] = "not-an-int"
        with self.assertRaises(msgspec.ValidationError):
            wrong_match_candidate_row(bad)


if __name__ == "__main__":
    unittest.main()
