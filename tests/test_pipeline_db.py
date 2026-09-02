"""Tests for lib/pipeline_db.py — Pipeline DB module (PostgreSQL).

Requires a PostgreSQL server. Set TEST_DB_DSN env var to run, e.g.:
    TEST_DB_DSN=postgresql://cratedigger@localhost/cratedigger_test python3 -m unittest tests.test_pipeline_db -v

Tests create/drop tables in the target database — use a dedicated test DB.
"""
import copy
import dataclasses
import json
import os
import sys
import tempfile
import threading
import time
import unittest
from collections.abc import Generator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, ClassVar, Protocol, TypedDict, cast, get_args
from unittest.mock import patch

import msgspec
import psycopg2
import psycopg2.extras

# Bootstrap ephemeral PostgreSQL if available
sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401 — sets TEST_DB_DSN env var

from lib.cycle_counters import COUNTER_NAMES, CycleCounters
from lib.dispatch import DispatchOutcome
from lib.import_execution import (
    CancellationToken,
    ExecutionCancelled,
    ExecutionLeaseSnapshot,
    OwnerSessionIdentity,
)
from lib.import_queue import (
    IMPORT_JOB_FORCE,
    IMPORT_JOB_TYPES,
    IMPORT_PREVIEW_REQUEUE_MAX_EXPONENT,
    AutomationHandoffResult,
    ImportJob,
    force_import_payload,
    import_preview_requeue_delay,
)
from lib.mb_canonical import CanonicalReleaseRedirected
from lib.merge_rekey_service import RESULT_REKEYED, MergeRekeyService
from lib.pipeline_db import (
    JELLYFIN_PIN_STATUSES,
    PLEX_PIN_STATUSES,
    SEARCH_LOG_OUTCOMES,
    DownloadLogOutcome,
    PersistedDistance,
    PersistedTrack,
    PersistedYoutubeRow,
    PipelineDB,
    SupersedeRaceError,
    TransferLedgerRow,
)
from lib.pipeline_db._shared import (
    REQUEST_METADATA_RESERVED_FIELDS,
    SearchPlanItemInput,
)
from lib.pipeline_db.dashboard import (
    SEARCH_ERROR_OUTCOMES,
    UnfindableRunMetricsRow,
    serialize_dashboard_cycle_row,
    serialize_dashboard_heavy_query_row,
    serialize_dashboard_request_row,
    serialize_unfindable_run_row,
    wanted_trend_panel,
)
from lib.pipeline_db.decisions import search_backoff_minutes
from lib.pipeline_db.download_log import (
    LINKED_IMPORT_OUTCOMES,
    LOG_FILTER_IMPORTED_OUTCOMES,
    LOG_FILTER_REJECTED_OUTCOMES,
)
from lib.pipeline_db.requests import (
    CAPTURE_DOWNLOAD_OUTCOMES,
    CAPTURE_IMPORT_JOB_TYPES,
)
from lib.pipeline_db.terminal_outcomes import _TransactionalTransitionsDB
from lib.quality import (
    CURRENT_EVIDENCE_LINEAGE_VERSION,
    ActiveDownloadState,
    AlbumQualityEvidenceFile,
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    AudioToolDiagnostic,
    AudioValidationReport,
    VerifiedLosslessProof,
    legacy_unrecorded_audio_validation_report,
)
from tests.dispatch_helpers import (
    claim_next_import_job,
    claim_next_import_preview_job,
    handoff_automation_owner,
)
from tests.evidence_helpers import make_album_quality_evidence
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import (
    REQUEST_CASCADE_RESET_TABLES,
    delete_all_rows,
    make_request_row,
)

TEST_DSN = os.environ.get("TEST_DB_DSN")

#: The whole canonical ``download_log.outcome`` taxonomy, typed. Derived
#: from the Literal rather than hand-listed, so a migration that widens the
#: CHECK constraint widens every vocabulary round-trip below with it.
_ALL_DOWNLOAD_LOG_OUTCOMES: tuple[DownloadLogOutcome, ...] = get_args(
    DownloadLogOutcome,
)

def requires_postgres(cls):
    """Skip test class if TEST_DB_DSN is not set."""
    if not TEST_DSN:
        return unittest.skip("TEST_DB_DSN not set — skipping PostgreSQL tests")(cls)
    return cls


# Every application table (every table but ``schema_migrations``), verified
# against the live FK graph (issue #1156 item 7) — this is the closure
# ``TRUNCATE <21 explicitly-named tables> CASCADE`` used to reach, now spelled
# out explicitly rather than left to three tables' worth of implicit cascade
# (``album_quality_evidence_files``, ``search_plans``, ``search_plan_items``).
#
# Order matters (see ``tests.helpers.delete_all_rows``'s docstring for the
# full mechanism, including why an explicit transaction is required here at
# all, independent of ordering): ``album_requests`` must precede
# ``import_jobs`` — one of THREE ``ON DELETE RESTRICT`` foreign keys in the
# schema (the other two: ``processing_cleanup_journal`` -> ``import_jobs``,
# and the self-referencing ``album_requests.replaces_request_id`` ->
# ``album_requests``, resolved for free since a single DELETE clears its
# whole table in one statement) — checked immediately rather than deferred
# to commit, regardless of any DEFERRABLE declaration on the constraint.
# ``processing_cleanup_journal`` is kept before ``import_jobs`` too, for the
# simplest correct mental model, though once ``album_requests`` precedes
# ``import_jobs`` its own position relative to ``import_jobs`` is not
# independently load-bearing (proven empirically both ways).
_ALL_TABLES = [
    "album_requests",
    "processing_cleanup_journal",
    "import_jobs",
    "album_quality_evidence_files",
    "album_quality_evidence",
    "peer_observations",
    "cycle_metrics",
    "unfindable_run_metrics",
    "bad_audio_hashes",
    "user_cooldowns",
    "source_denylist",
    "search_plan_items",
    "search_plans",
    "search_log",
    "download_log",
    "album_request_field_resolutions",  # migration 030
    "youtube_album_mappings",  # migration 034
    "youtube_album_empty_resolutions",  # migration 035
    "plex_added_at_pins",  # migration 040
    "jellyfin_date_created_pins",  # migration 046
    "slskd_event_cursor",  # migration 041
    "slskd_search_ledger",  # migration 044
    "slskd_transfer_ledger",  # migration 045
    "album_tracks",
]


def make_db():
    """Create a PipelineDB connected to the test database, with clean tables.

    Schema is migrated once in conftest.py at session start. This helper
    just deletes every row from every table for an isolated test slate —
    see ``tests.helpers.delete_all_rows`` for why DELETE replaced TRUNCATE.
    """
    from lib import pipeline_db
    db = pipeline_db.PipelineDB(TEST_DSN)
    delete_all_rows(db, _ALL_TABLES)
    return db


class _RecordingTestConnection:
    def __init__(self) -> None:
        self.commit_count = 0

    def commit(self) -> None:
        self.commit_count += 1


class _RecordingTestDB:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.conn = _RecordingTestConnection()
        self.atomic_entered = 0

    def _execute(self, sql: str) -> None:
        self.statements.append(sql)

    @contextmanager
    def _atomic(self) -> Generator[_RecordingTestConnection]:
        self.atomic_entered += 1
        yield self.conn


class TestMakeDbIsolation(unittest.TestCase):
    def test_deletes_the_exact_test_slate_once_in_one_transaction(self) -> None:
        db = _RecordingTestDB()
        with patch("lib.pipeline_db.PipelineDB", return_value=db):
            result = make_db()

        self.assertIs(result, db)
        self.assertEqual(db.statements, [f"DELETE FROM {t}" for t in _ALL_TABLES])
        # delete_all_rows runs every DELETE inside one db._atomic() call and
        # commits exactly once inside it (issue #1156 item 7 P3-F4: this
        # delegates to PipelineDB's own transaction/reconnect/rollback
        # machinery instead of duplicating a second, untested copy of it).
        self.assertEqual(db.atomic_entered, 1)
        self.assertEqual(db.conn.commit_count, 1)


def _seed_restrict_and_self_reference_scenario(db: PipelineDB) -> None:
    """Seed both ``ON DELETE RESTRICT`` edges and both NOT DEFERRABLE
    self-referencing edges in the schema at once (issue #1156 item 7):

    - ``album_requests.active_automation_import_job_id -> import_jobs``
      AND ``processing_cleanup_journal -> import_jobs`` (both RESTRICT) —
      a real "processing" owner plus its journal row, live together.
    - ``album_requests.replaces_request_id -> album_requests`` (self, NOT
      DEFERRABLE) — an old row replaced by a new one.
    - ``download_log.source_download_log_id -> download_log`` (self, NOT
      DEFERRABLE) — a force-import row lineage-linked to its source.
    - one independent ``bad_audio_hashes`` row (SET NULL only, no RESTRICT
      predecessor) as the known-bad self-test's drop target below.

    This is exactly the world a wrong table order (or a table silently
    dropped from the reset list) fails on — see
    ``tests.helpers.delete_all_rows``'s docstring for why order matters,
    and ``TestDeleteAllRowsRealPostgresRegression`` below for the pin.
    """
    with db._atomic():
        old_req_id = db._execute(
            "INSERT INTO album_requests (artist_name, album_title, source, "
            "mb_release_id, status) VALUES (%s,%s,%s,%s,%s) RETURNING id",
            ("Old Artist", "Old Album", "request", "mbid-1156-old", "replaced"),
        ).fetchone()["id"]
        new_req_id = db._execute(
            "INSERT INTO album_requests (artist_name, album_title, source, "
            "mb_release_id, status, replaces_request_id) "
            "VALUES (%s,%s,%s,%s,%s,%s) RETURNING id",
            ("New Artist", "New Album", "request", "mbid-1156-new", "wanted",
             old_req_id),
        ).fetchone()["id"]
        proc_req_id = db._execute(
            "INSERT INTO album_requests (artist_name, album_title, source, "
            "mb_release_id, status) VALUES (%s,%s,%s,%s,%s) RETURNING id",
            ("Proc Artist", "Proc Album", "request", "mbid-1156-proc",
             "downloading"),
        ).fetchone()["id"]
        job_id = db._execute(
            "INSERT INTO import_jobs (job_type, status, request_id) "
            "VALUES (%s,%s,%s) RETURNING id",
            ("automation_import", "running", proc_req_id),
        ).fetchone()["id"]
        db._execute(
            "UPDATE album_requests SET status='processing', "
            "active_automation_import_job_id=%s WHERE id=%s",
            (job_id, proc_req_id),
        )
        db._execute(
            "INSERT INTO processing_cleanup_journal (job_id, request_id, "
            "action, source_path, source_manifest, source_manifest_hash) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (job_id, proc_req_id, "move", "/processing/1156", "[]",
             "deadbeef1156"),
        )
        original_dl_id = db._execute(
            "INSERT INTO download_log (request_id, outcome) VALUES (%s,%s) "
            "RETURNING id",
            (new_req_id, "success"),
        ).fetchone()["id"]
        db._execute(
            "INSERT INTO download_log (request_id, outcome, "
            "source_download_log_id) VALUES (%s,%s,%s)",
            (new_req_id, "force_import", original_dl_id),
        )
        db._execute(
            "INSERT INTO bad_audio_hashes (hash_value, audio_format) "
            "VALUES (%s,%s)",
            (bytes.fromhex("ab" * 32), "flac"),
        )
        db.conn.commit()


@requires_postgres
class TestDeleteAllRowsRealPostgresRegression(unittest.TestCase):
    """Real-PG pin (issue #1156 item 7): after ``delete_all_rows``, the
    RESTRICT/self-referencing scenario TRUNCATE ... CASCADE used to clear
    for free is genuinely gone, not merely unreferenced.
    """

    def setUp(self) -> None:
        self.db = make_db()

    def tearDown(self) -> None:
        self.db.close()

    _SEEDED_TABLES = (
        "album_requests", "import_jobs", "processing_cleanup_journal",
        "download_log", "bad_audio_hashes",
    )

    def _assert_seeded_tables_empty(self) -> None:
        for table in self._SEEDED_TABLES:
            with self.subTest(table=table):
                row = self.db._execute(
                    f"SELECT count(*) AS n FROM {table}"
                ).fetchone()
                self.assertEqual(row["n"], 0, f"{table} was not fully cleared")

    def test_request_cascade_reset_tables_leaves_nothing_behind(self) -> None:
        _seed_restrict_and_self_reference_scenario(self.db)
        delete_all_rows(self.db, REQUEST_CASCADE_RESET_TABLES)
        self._assert_seeded_tables_empty()

    def test_all_tables_leaves_nothing_behind(self) -> None:
        _seed_restrict_and_self_reference_scenario(self.db)
        delete_all_rows(self.db, _ALL_TABLES)
        self._assert_seeded_tables_empty()

    def test_known_bad_a_table_dropped_from_the_list_is_not_cleared(
        self,
    ) -> None:
        """Known-bad self-test: proves ``_assert_seeded_tables_empty`` is
        not vacuous. Mutates a LOCAL copy of the reset list (never the
        shipped ``REQUEST_CASCADE_RESET_TABLES`` constant) by dropping
        ``bad_audio_hashes`` — a table with no RESTRICT predecessor, so the
        drop produces a silent leftover row rather than an exception — and
        asserts the row survives. If this assertion ever started passing,
        the "leaves nothing behind" pins above would no longer be
        falsifiable."""
        mutated = tuple(
            t for t in REQUEST_CASCADE_RESET_TABLES if t != "bad_audio_hashes"
        )
        _seed_restrict_and_self_reference_scenario(self.db)
        delete_all_rows(self.db, mutated)
        row = self.db._execute(
            "SELECT count(*) AS n FROM bad_audio_hashes"
        ).fetchone()
        self.assertEqual(
            row["n"], 1,
            "expected the dropped table's row to survive — if it didn't, "
            "the 'leaves nothing behind' pins above are not falsifiable",
        )

    def test_missing_predecessor_raises_and_leaves_the_world_untouched(
        self,
    ) -> None:
        """Failure-path pin (issue #1156 item 7 P3-F5): the precondition
        ``delete_all_rows``'s docstring documents — a table list that
        includes ``album_requests`` on a world with a live processing
        owner must also include ``import_jobs`` and
        ``processing_cleanup_journal`` in the SAME call — is real, and the
        failure it raises leaves nothing partially committed and the
        connection usable for the next test. Exercises ``db._atomic()``'s
        own rollback/autocommit-restore machinery, not a second
        hand-rolled copy of it (P3-F4: the duplicate copy this pin
        replaces silently dropped both dead-connection guards and never
        called ``_ensure_conn()``).
        """
        _seed_restrict_and_self_reference_scenario(self.db)
        before = self.db._execute(
            "SELECT count(*) AS n FROM album_requests"
        ).fetchone()["n"]
        self.assertGreater(before, 0)

        with self.assertRaises(psycopg2.errors.CheckViolation):
            delete_all_rows(self.db, ["album_requests"])

        # The deferred constraint trigger fires at COMMIT and PostgreSQL
        # rolls back the whole transaction on failure — nothing was
        # partially deleted.
        after = self.db._execute(
            "SELECT count(*) AS n FROM album_requests"
        ).fetchone()["n"]
        self.assertEqual(after, before)

        # db._atomic() restores autocommit and the connection stays usable
        # (Critical rule 7: PipelineDB must run autocommit=True).
        self.assertTrue(self.db.conn.autocommit)
        row = self.db._execute("SELECT 1 AS ok").fetchone()
        self.assertEqual(row["ok"], 1)


def _link_projection_evidence(
    db: PipelineDB | FakePipelineDB,
    request_id: int,
    evidence_release_id: str,
    *,
    verified: bool = False,
    provisional: bool = False,
) -> None:
    evidence = make_album_quality_evidence(
        mb_release_id=evidence_release_id,
        source_path=f"/library/{evidence_release_id}",
        measurement=AudioQualityMeasurement(
            min_bitrate_kbps=245,
            avg_bitrate_kbps=256,
            median_bitrate_kbps=252,
            format="MP3",
        ),
        v0_metric=(
            AlbumQualityV0Metric(
                subject="source",
                provenance="carried",
                avg_bitrate_kbps=251,
                min_bitrate_kbps=228,
            )
            if provisional else None
        ),
        verified_lossless_proof=(
            VerifiedLosslessProof(
                provenance="carried",
                source="flac",
                classifier="spectral_verified_lossless",
            )
            if verified else None
        ),
    )
    db.upsert_album_quality_evidence(evidence)
    stored = db.find_album_quality_evidence(
        mb_release_id=evidence_release_id,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
    )
    assert stored is not None and stored.id is not None
    assert db.set_request_current_evidence(request_id, stored.id)


def _seed_foreign_current_evidence_world(
    db: PipelineDB | FakePipelineDB,
    *,
    identity_layout: str,
) -> list[str]:
    """Seed exact evidence plus foreign verified/provisional links."""
    if identity_layout == "musicbrainz":
        request_release_ids = [
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1",
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2",
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa3",
        ]
        evidence_release_ids = [
            request_release_ids[0],
            "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb2",
            "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb3",
        ]
    elif identity_layout == "modern_discogs":
        request_release_ids = ["456701", "456702", "456703"]
        evidence_release_ids = ["456701", "456792", "456793"]
    elif identity_layout == "legacy_discogs":
        request_release_ids = ["456711", "456712", "456713"]
        evidence_release_ids = ["456711", "456782", "456783"]
    else:
        raise ValueError(f"unknown identity layout: {identity_layout}")

    titles = ("Exact evidence", "Foreign evidence", "Foreign provisional")
    for index, (request_release_id, evidence_release_id) in enumerate(
        zip(request_release_ids, evidence_release_ids, strict=True)
    ):
        if identity_layout == "modern_discogs":
            request_id = db.add_request(
                mb_release_id=None,
                discogs_release_id=request_release_id,
                artist_name="Foreign Evidence Artist",
                album_title=titles[index],
                source="request",
                status="imported",
            )
        else:
            request_id = db.add_request(
                mb_release_id=request_release_id,
                artist_name="Foreign Evidence Artist",
                album_title=titles[index],
                source="request",
                status="imported",
            )
        _link_projection_evidence(
            db,
            request_id,
            evidence_release_id,
            verified=index < 2,
            provisional=index == 2,
        )
    return request_release_ids


def _unavailable_execution_lease(
    **_kwargs: object,
) -> ExecutionLeaseSnapshot:
    raise ValueError("test run is outside systemd")


@requires_postgres
class TestAddRequestRoundTrip(unittest.TestCase):
    """Rule A round-trip for add_request (#382 Layer 1). Every column the
    typed AddRequestInput payload persists must read back unchanged — the
    "column written but not read back" half of the album_title drift class.
    Pairs with the AddRequestInput-fields-subset-of-columns check in
    tests/test_pipeline_db_column_contract.py."""

    def test_add_request_round_trip_preserves_every_field(self):
        db = make_db()
        expected = {
            "artist_name": "Round Trip",
            "album_title": "Every Field",
            "source": "request",
            "mb_release_id": "mb-rt-1",
            "mb_release_group_id": "rg-rt-1",
            "mb_artist_id": "art-rt-1",
            "discogs_release_id": "dg-rt-1",
            "year": 1999,
            "release_group_year": 1998,
            "country": "US",
            "format": "CD",
            "source_path": "/incoming/rt",
            "reasoning": "why this pressing",
            "status": "wanted",
            "is_va_compilation": True,
        }
        rid = db.add_request(**expected)
        row = db.get_request(rid)
        self.assertIsNotNone(row)
        assert row is not None
        for col, val in expected.items():
            self.assertEqual(
                row[col], val,
                f"add_request field {col!r} did not round-trip through PG")


@requires_postgres
class TestSupersedeRequestMbidRoundTrip(unittest.TestCase):
    """Rule A round-trip for supersede_request_mbid (U1 — Discogs-pathway
    Replace). Every field the supersede INSERT writes onto the new row must
    read back unchanged through real PG, and the old row must flip to the
    frozen 'replaced' audit state. Before U1 there was NO real-PG round-trip
    for supersede at all, so the new discogs_release_id column had no guard
    against being dropped at the SQL seam — exactly the album_title class
    Rule A targets."""

    def _seed_old(self, db) -> int:
        return db.add_request(
            artist_name="Pendulum",
            album_title="Hold Your Colour (old pressing)",
            source="request",
            mb_release_id="old-mbid",
            mb_release_group_id="rg-old",
            mb_artist_id="art-old",
            year=2005,
            country="AU",
            status="wanted",
        )

    def test_supersede_round_trip_with_discogs_id(self):
        db = make_db()
        old_id = self._seed_old(db)
        new_tracks = [
            {"disc_number": 1, "track_number": 1, "title": "Prelude"},
            {"disc_number": 1, "track_number": 2, "title": "Slam"},
        ]
        new_id = db.supersede_request_mbid(
            old_id,
            new_mb_release_id="new-mbid",
            new_mb_release_group_id="rg-new",
            new_mb_artist_id="art-new",
            new_artist_name="Pendulum",
            new_album_title="Hold Your Colour (target pressing)",
            new_year=2007,
            new_country="JP",
            new_discogs_release_id="12345",
            new_tracks=new_tracks,
        )
        expected = {
            "mb_release_id": "new-mbid",
            "mb_release_group_id": "rg-new",
            "mb_artist_id": "art-new",
            "artist_name": "Pendulum",
            "album_title": "Hold Your Colour (target pressing)",
            "year": 2007,
            "country": "JP",
            "discogs_release_id": "12345",
            "replaces_request_id": old_id,
            "status": "wanted",
            "source": "request",  # inherited from the old row
        }
        new = db.get_request(new_id)
        self.assertIsNotNone(new)
        assert new is not None
        for col, val in expected.items():
            self.assertEqual(
                new[col], val,
                f"supersede field {col!r} did not round-trip through PG")
        # The old row is the frozen 'replaced' audit row.
        old = db.get_request(old_id)
        assert old is not None
        self.assertEqual(old["status"], "replaced")
        # album_tracks for the new row must round-trip through the same
        # getter the rest of the pipeline reads tracks back with.
        tracks = db.get_tracks(new_id)
        self.assertEqual(
            [(t["disc_number"], t["track_number"], t["title"]) for t in tracks],
            [(t["disc_number"], t["track_number"], t["title"]) for t in new_tracks],
        )

    def test_supersede_round_trip_mb_path_discogs_id_null(self):
        # MB Replace passes new_discogs_release_id=None — the column must be
        # NULL, everything else unchanged.
        db = make_db()
        old_id = self._seed_old(db)
        new_id = db.supersede_request_mbid(
            old_id,
            new_mb_release_id="new-mbid-mb",
            new_mb_release_group_id="rg-new",
            new_mb_artist_id="art-new",
            new_artist_name="Pendulum",
            new_album_title="Hold Your Colour",
            new_year=2007,
            new_country="JP",
            new_discogs_release_id=None,
            new_tracks=[],
        )
        new = db.get_request(new_id)
        assert new is not None
        self.assertIsNone(new["discogs_release_id"])
        self.assertEqual(new["mb_release_id"], "new-mbid-mb")
        self.assertEqual(new["status"], "wanted")
        self.assertEqual(new["replaces_request_id"], old_id)
        old = db.get_request(old_id)
        assert old is not None
        self.assertEqual(old["status"], "replaced")


@requires_postgres
class TestPlexAddedAtPinsRoundTrip(unittest.TestCase):
    """Rule A round-trip for the Plex addedAt pin store (migration 040).
    Every field the writer persists must read back unchanged through real PG —
    a FakePipelineDB pass alone can't catch a column dropped at the SQL seam."""

    def test_add_pin_round_trips_every_field(self):
        # Read back via a raw SELECT (not the getter) so the assertion targets
        # exactly what PG preserved — the strongest Rule A form.
        db = make_db()
        pin_id = db.add_plex_added_at_pin(
            imported_path="Muse/2026 - The Wow! Signal",
            original_added_at=1782611948,
            rating_key="458495",
            request_id=8812,
        )
        self.assertIsInstance(pin_id, int)
        cur = db._execute(
            "SELECT imported_path, original_added_at, rating_key, request_id, "
            "status FROM plex_added_at_pins WHERE id = %s", (pin_id,))
        row = cur.fetchone()
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row["imported_path"], "Muse/2026 - The Wow! Signal")
        self.assertEqual(row["original_added_at"], 1782611948)
        self.assertEqual(row["rating_key"], "458495")
        self.assertEqual(row["request_id"], 8812)
        self.assertEqual(row["status"], "pending")

    def test_add_pin_round_trips_nullable_fields(self):
        # rating_key and request_id are nullable — they must round-trip as NULL.
        db = make_db()
        pin_id = db.add_plex_added_at_pin(
            imported_path="A/B", original_added_at=100,
            rating_key=None, request_id=None)
        cur = db._execute(
            "SELECT rating_key, request_id FROM plex_added_at_pins "
            "WHERE id = %s", (pin_id,))
        row = cur.fetchone()
        assert row is not None
        self.assertIsNone(row["rating_key"])
        self.assertIsNone(row["request_id"])

    def test_mark_pin_round_trips_status_and_excludes_from_pending(self):
        from datetime import datetime, timedelta
        db = make_db()
        pin_id = db.add_plex_added_at_pin(
            imported_path="A/B", original_added_at=100,
            rating_key=None, request_id=None)
        now = datetime.now(UTC)
        db.mark_plex_added_at_pin(pin_id, status="done", reconciled_at=now)
        # Round-trip the mutated columns via a raw SELECT.
        cur = db._execute(
            "SELECT status, reconciled_at FROM plex_added_at_pins "
            "WHERE id = %s", (pin_id,))
        row = cur.fetchone()
        assert row is not None
        self.assertEqual(row["status"], "done")
        self.assertIsNotNone(row["reconciled_at"])
        # ...and a 'done' pin drops out of the pending working set.
        rows = db.get_pending_plex_added_at_pins(
            captured_before=now + timedelta(days=1), limit=100)
        self.assertEqual([r for r in rows if r["id"] == pin_id], [],
                         "done pin must not appear in pending")

    def test_status_check_accepts_domain_and_rejects_unknown_value(self):
        db = make_db()
        pin_id = db.add_plex_added_at_pin(
            imported_path="A/B", original_added_at=100,
            rating_key=None, request_id=None)
        for status in PLEX_PIN_STATUSES:
            with self.subTest(status=status):
                db._execute(
                    "UPDATE plex_added_at_pins SET status = %s WHERE id = %s",
                    (status, pin_id),
                )
        db._execute(
            "UPDATE plex_added_at_pins SET status = 'pending' WHERE id = %s",
            (pin_id,),
        )
        with self.assertRaises(psycopg2.errors.CheckViolation):
            db.mark_plex_added_at_pin(
                pin_id,
                status=cast(Any, "stranded"),
                reconciled_at=datetime.now(UTC),
            )
        cur = db._execute(
            "SELECT status, reconciled_at FROM plex_added_at_pins WHERE id = %s",
            (pin_id,),
        )
        row = cur.fetchone()
        assert row is not None
        self.assertEqual((row["status"], row["reconciled_at"]), ("pending", None))

    def test_status_check_constraint_is_named(self):
        db = make_db()
        cur = db._execute("""
            SELECT conname
            FROM pg_constraint
            WHERE conrelid = 'plex_added_at_pins'::regclass
              AND contype = 'c'
        """)
        self.assertIn(
            "plex_added_at_pins_status_check",
            {row["conname"] for row in cur.fetchall()},
        )

    def test_pending_getter_respects_captured_before_cutoff(self):
        from datetime import datetime, timedelta
        db = make_db()
        pin_id = db.add_plex_added_at_pin(
            imported_path="C/D", original_added_at=200,
            rating_key="rk", request_id=1)
        # A cutoff in the past (before the just-now capture) excludes the pin —
        # this is the reconciler's settle-window guard.
        past = datetime.now(UTC) - timedelta(hours=1)
        rows = db.get_pending_plex_added_at_pins(captured_before=past, limit=100)
        self.assertEqual([r for r in rows if r["id"] == pin_id], [])

    def test_prune_terminal_pins_has_strict_status_and_age_boundary(self):
        db = make_db()
        cutoff = datetime(2026, 7, 11, 0, 0, tzinfo=UTC)
        cases = (
            ("old done", "done", cutoff - timedelta(seconds=1), False),
            ("old skipped", "skipped", cutoff - timedelta(days=1), False),
            ("exact boundary", "done", cutoff, True),
            ("inside retention", "skipped", cutoff + timedelta(seconds=1), True),
            ("old pending", "pending", cutoff - timedelta(days=365), True),
        )
        ids: dict[str, int] = {}
        for label, status, reconciled_at, _survives in cases:
            pin_id = db.add_plex_added_at_pin(
                imported_path=f"Artist/{label}", original_added_at=100,
                rating_key=None, request_id=None)
            db._execute(
                "UPDATE plex_added_at_pins SET status = %s, "
                "reconciled_at = %s WHERE id = %s",
                (status, reconciled_at, pin_id),
            )
            ids[label] = pin_id

        removed = db.prune_terminal_plex_added_at_pins(older_than=cutoff)

        self.assertEqual(removed, 2)
        cur = db._execute("SELECT id FROM plex_added_at_pins")
        surviving_ids = {row["id"] for row in cur.fetchall()}
        self.assertEqual(
            surviving_ids,
            {ids[label] for label, _status, _at, survives in cases if survives},
        )


@requires_postgres
class TestJellyfinDateCreatedPinsRoundTrip(unittest.TestCase):
    """Rule A round-trip for the Jellyfin DateCreated pin store (migration
    046). Every field the writer persists must read back unchanged through
    real PG — a FakePipelineDB pass alone can't catch a column dropped at the
    SQL seam, and children_item_ids crosses the JSONB boundary."""

    def test_add_pin_round_trips_every_field(self):
        # Read back via a raw SELECT (not the getter) so the assertion targets
        # exactly what PG preserved — the strongest Rule A form.
        db = make_db()
        pin_id = db.add_jellyfin_date_created_pin(
            imported_path="Muse/2026 - The Wow! Signal",
            original_date_created="2026-04-26T18:31:04.4425337Z",
            album_item_id="d7139f369ef487c32970929c9a4adf01",
            children_item_ids=["tr-1", "tr-2", "tr-3"],
            request_id=8812,
        )
        self.assertIsInstance(pin_id, int)
        cur = db._execute(
            "SELECT imported_path, original_date_created, album_item_id, "
            "children_item_ids, request_id, status "
            "FROM jellyfin_date_created_pins WHERE id = %s", (pin_id,))
        row = cur.fetchone()
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row["imported_path"], "Muse/2026 - The Wow! Signal")
        self.assertEqual(row["original_date_created"],
                         "2026-04-26T18:31:04.4425337Z")
        self.assertEqual(row["album_item_id"], "d7139f369ef487c32970929c9a4adf01")
        self.assertEqual(row["children_item_ids"], ["tr-1", "tr-2", "tr-3"])
        self.assertEqual(row["request_id"], 8812)
        self.assertEqual(row["status"], "pending")

    def test_add_pin_round_trips_nullable_and_empty_fields(self):
        # request_id is nullable and the children snapshot can be empty —
        # both must round-trip exactly.
        db = make_db()
        pin_id = db.add_jellyfin_date_created_pin(
            imported_path="A/B", original_date_created="2026-01-01T00:00:00Z",
            album_item_id="alb", children_item_ids=[], request_id=None)
        cur = db._execute(
            "SELECT children_item_ids, request_id "
            "FROM jellyfin_date_created_pins WHERE id = %s", (pin_id,))
        row = cur.fetchone()
        assert row is not None
        self.assertEqual(row["children_item_ids"], [])
        self.assertIsNone(row["request_id"])

    def test_floor_pin_null_album_item_id_round_trips(self):
        # Migration 053: a floor pin (path-changing upgrade with no findable
        # pre-upgrade item) has no item-id snapshot at all — NULL must
        # round-trip and the pin must surface as pending.
        db = make_db()
        pin_id = db.add_jellyfin_date_created_pin(
            imported_path="Arcade Fire/0000 - B-Sides & Rarities",
            original_date_created="2026-06-04T04:45:50Z",
            album_item_id=None, children_item_ids=[], request_id=8504)
        cur = db._execute(
            "SELECT album_item_id, children_item_ids "
            "FROM jellyfin_date_created_pins WHERE id = %s", (pin_id,))
        row = cur.fetchone()
        assert row is not None
        self.assertIsNone(row["album_item_id"])
        self.assertEqual(row["children_item_ids"], [])
        pending = db.get_pending_jellyfin_date_created_pins(
            captured_before=datetime.now(UTC) + timedelta(days=1))
        match = [r for r in pending if r["id"] == pin_id]
        self.assertEqual(len(match), 1)
        self.assertIsNone(match[0]["album_item_id"])

    def test_mark_pin_round_trips_status_and_excludes_from_pending(self):
        db = make_db()
        pin_id = db.add_jellyfin_date_created_pin(
            imported_path="A/B", original_date_created="2026-01-01T00:00:00Z",
            album_item_id="alb", children_item_ids=["t"], request_id=None)
        now = datetime.now(UTC)
        db.mark_jellyfin_date_created_pin(
            pin_id, status="expired", reconciled_at=now)
        cur = db._execute(
            "SELECT status, reconciled_at FROM jellyfin_date_created_pins "
            "WHERE id = %s", (pin_id,))
        row = cur.fetchone()
        assert row is not None
        self.assertEqual(row["status"], "expired")
        self.assertIsNotNone(row["reconciled_at"])
        rows = db.get_pending_jellyfin_date_created_pins(
            captured_before=now + timedelta(days=1), limit=100)
        self.assertEqual([r for r in rows if r["id"] == pin_id], [],
                         "terminal pin must not appear in pending")

    def test_status_check_accepts_domain_and_rejects_unknown_value(self):
        db = make_db()
        pin_id = db.add_jellyfin_date_created_pin(
            imported_path="A/B", original_date_created="2026-01-01T00:00:00Z",
            album_item_id="alb", children_item_ids=[], request_id=None)
        for status in JELLYFIN_PIN_STATUSES:
            with self.subTest(status=status):
                db._execute(
                    "UPDATE jellyfin_date_created_pins SET status = %s "
                    "WHERE id = %s",
                    (status, pin_id),
                )
        db._execute(
            "UPDATE jellyfin_date_created_pins SET status = 'pending' "
            "WHERE id = %s",
            (pin_id,),
        )
        with self.assertRaises(psycopg2.errors.CheckViolation):
            db.mark_jellyfin_date_created_pin(
                pin_id,
                status=cast(Any, "stranded"),
                reconciled_at=datetime.now(UTC),
            )
        cur = db._execute(
            "SELECT status, reconciled_at FROM jellyfin_date_created_pins "
            "WHERE id = %s",
            (pin_id,),
        )
        row = cur.fetchone()
        assert row is not None
        self.assertEqual((row["status"], row["reconciled_at"]), ("pending", None))

    def test_status_check_constraint_is_named(self):
        db = make_db()
        cur = db._execute("""
            SELECT conname
            FROM pg_constraint
            WHERE conrelid = 'jellyfin_date_created_pins'::regclass
              AND contype = 'c'
        """)
        self.assertIn(
            "jellyfin_date_created_pins_status_check",
            {row["conname"] for row in cur.fetchall()},
        )

    def test_get_oldest_request_chain_created_at_walks_the_chain(self):
        # The Jellyfin floor pin's date source: the recursive walk over
        # replaces_request_id must find the OLDEST created_at, and an
        # unknown id must return None (not crash the capture).
        db = make_db()
        old_id = db.add_request(
            artist_name="Arcade Fire", album_title="B-Sides & Rarities",
            source="request", mb_release_id="mb-chain-old", status="replaced")
        new_id = db.add_request(
            artist_name="Arcade Fire", album_title="B-Sides & Rarities",
            source="request", mb_release_id="mb-chain-new", status="wanted")
        db._execute(
            "UPDATE album_requests SET created_at = %s WHERE id = %s",
            (datetime(2026, 2, 1, tzinfo=UTC), old_id))
        db._execute(
            "UPDATE album_requests "
            "SET created_at = %s, replaces_request_id = %s WHERE id = %s",
            (datetime(2026, 6, 1, tzinfo=UTC), old_id, new_id))
        self.assertEqual(
            db.get_oldest_request_chain_created_at(new_id),
            datetime(2026, 2, 1, tzinfo=UTC))
        self.assertIsNone(db.get_oldest_request_chain_created_at(999999))

    def test_pending_getter_respects_captured_before_cutoff(self):
        db = make_db()
        pin_id = db.add_jellyfin_date_created_pin(
            imported_path="C/D", original_date_created="2026-01-01T00:00:00Z",
            album_item_id="alb", children_item_ids=["t"], request_id=1)
        # A cutoff in the past (before the just-now capture) excludes the pin —
        # this is the reconciler's settle-window guard.
        past = datetime.now(UTC) - timedelta(hours=1)
        rows = db.get_pending_jellyfin_date_created_pins(
            captured_before=past, limit=100)
        self.assertEqual([r for r in rows if r["id"] == pin_id], [])

    def test_prune_terminal_pins_has_strict_status_and_age_boundary(self):
        db = make_db()
        cutoff = datetime(2026, 7, 11, 0, 0, tzinfo=UTC)
        cases = (
            ("old done", "done", cutoff - timedelta(seconds=1), False),
            ("old skipped", "skipped", cutoff - timedelta(days=1), False),
            ("old expired", "expired", cutoff - timedelta(days=90), False),
            ("exact boundary", "expired", cutoff, True),
            ("inside retention", "done", cutoff + timedelta(seconds=1), True),
            ("old pending", "pending", cutoff - timedelta(days=365), True),
        )
        ids: dict[str, int] = {}
        for label, status, reconciled_at, _survives in cases:
            pin_id = db.add_jellyfin_date_created_pin(
                imported_path=f"Artist/{label}",
                original_date_created="2000-01-01T00:00:00Z",
                album_item_id=f"album-{label}", children_item_ids=[],
                request_id=None)
            db._execute(
                "UPDATE jellyfin_date_created_pins SET status = %s, "
                "reconciled_at = %s WHERE id = %s",
                (status, reconciled_at, pin_id),
            )
            ids[label] = pin_id

        removed = db.prune_terminal_jellyfin_date_created_pins(
            older_than=cutoff)

        self.assertEqual(removed, 3)
        cur = db._execute("SELECT id FROM jellyfin_date_created_pins")
        surviving_ids = {row["id"] for row in cur.fetchall()}
        self.assertEqual(
            surviving_ids,
            {ids[label] for label, _status, _at, survives in cases if survives},
        )


@requires_postgres
class TestPinStatusProductionFakeParity(unittest.TestCase):
    """Existing/missing id by valid/invalid status parity for both stores."""

    CASES = (
        ("existing valid", True, False),
        ("existing invalid", True, True),
        ("missing valid", False, False),
        ("missing invalid", False, True),
    )

    def test_plex_mark_matrix(self):
        now = datetime(2026, 7, 11, tzinfo=UTC)
        for scenario, exists, invalid in self.CASES:
            for adapter in ("production", "fake"):
                with self.subTest(scenario=scenario, adapter=adapter):
                    db: Any = (
                        make_db() if adapter == "production"
                        else FakePipelineDB()
                    )
                    pin_id = db.add_plex_added_at_pin(
                        imported_path="A/B", original_added_at=1,
                        rating_key=None, request_id=None)
                    target_id = pin_id if exists else pin_id + 1000
                    status = cast(Any, "stranded" if invalid else "done")
                    error: Exception | None = None
                    try:
                        db.mark_plex_added_at_pin(
                            target_id, status=status, reconciled_at=now)
                    except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
                        error = exc

                    if adapter == "production":
                        cur = db._execute(
                            "SELECT status, reconciled_at "
                            "FROM plex_added_at_pins WHERE id = %s",
                            (pin_id,),
                        )
                        row = cur.fetchone()
                        assert row is not None
                    else:
                        row = db.plex_added_at_pins[0]

                    should_reject = exists and invalid
                    if should_reject:
                        self.assertIsInstance(
                            error, psycopg2.errors.CheckViolation)
                    else:
                        self.assertIsNone(error)
                    self.assertEqual(
                        (row["status"], row["reconciled_at"]),
                        ("done", now) if exists and not invalid
                        else ("pending", None),
                    )

    def test_jellyfin_mark_matrix(self):
        now = datetime(2026, 7, 11, tzinfo=UTC)
        for scenario, exists, invalid in self.CASES:
            for adapter in ("production", "fake"):
                with self.subTest(scenario=scenario, adapter=adapter):
                    db: Any = (
                        make_db() if adapter == "production"
                        else FakePipelineDB()
                    )
                    pin_id = db.add_jellyfin_date_created_pin(
                        imported_path="A/B",
                        original_date_created="2000-01-01T00:00:00Z",
                        album_item_id="album", children_item_ids=[],
                        request_id=None)
                    target_id = pin_id if exists else pin_id + 1000
                    status = cast(
                        Any, "stranded" if invalid else "expired")
                    error: Exception | None = None
                    try:
                        db.mark_jellyfin_date_created_pin(
                            target_id, status=status, reconciled_at=now)
                    except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
                        error = exc

                    if adapter == "production":
                        cur = db._execute(
                            "SELECT status, reconciled_at "
                            "FROM jellyfin_date_created_pins WHERE id = %s",
                            (pin_id,),
                        )
                        row = cur.fetchone()
                        assert row is not None
                    else:
                        row = db.jellyfin_date_created_pins[0]

                    should_reject = exists and invalid
                    if should_reject:
                        self.assertIsInstance(
                            error, psycopg2.errors.CheckViolation)
                    else:
                        self.assertIsNone(error)
                    self.assertEqual(
                        (row["status"], row["reconciled_at"]),
                        ("expired", now) if exists and not invalid
                        else ("pending", None),
                    )


@requires_postgres
class TestSchemaCreation(unittest.TestCase):
    def test_tables_exist(self):
        """All expected tables exist after the migrator has run."""
        db = make_db()
        cur = db._execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        table_names = {r["table_name"] for r in cur.fetchall()}
        self.assertIn("album_requests", table_names)
        self.assertIn("album_tracks", table_names)
        self.assertIn("download_log", table_names)
        self.assertIn("search_log", table_names)
        self.assertIn("source_denylist", table_names)
        self.assertIn("user_cooldowns", table_names)
        self.assertIn("import_jobs", table_names)
        self.assertIn("cycle_metrics", table_names)
        self.assertIn("unfindable_run_metrics", table_names)
        self.assertIn("peer_observations", table_names)
        # Migration 039 dropped the peer/dir combo experiment (#227).
        self.assertNotIn("peer_dir_observations", table_names)
        self.assertNotIn("peer_dir_daily_aggregates", table_names)
        self.assertIn("album_quality_evidence", table_names)
        self.assertIn("album_quality_evidence_files", table_names)
        # The migrator's own tracking table must also exist
        self.assertIn("schema_migrations", table_names)
        db.close()

    def test_import_jobs_schema_constraints_and_indexes(self):
        """Migration 003 creates the durable shared importer queue."""
        db = make_db()
        req_id = db.add_request(
            mb_release_id="queue-schema-mbid",
            artist_name="Queue",
            album_title="Schema",
            source="request",
        )

        cur = db._execute("""
            INSERT INTO import_jobs (
                job_type, status, request_id, dedupe_key, payload
            )
            VALUES (
                'force_import', 'queued', %s, 'force_import:download_log:1',
                '{"failed_path": "/tmp/failed"}'::jsonb
            )
            RETURNING id
        """, (req_id,))
        row = cur.fetchone()
        assert row is not None
        first_id = row["id"]
        self.assertIsInstance(first_id, int)

        with self.assertRaises(psycopg2.errors.UniqueViolation):
            db._execute("""
                INSERT INTO import_jobs (
                    job_type, status, request_id, dedupe_key, payload
                )
                VALUES (
                    'force_import', 'queued', %s, 'force_import:download_log:1',
                    '{"failed_path": "/tmp/other"}'::jsonb
                )
            """, (req_id,))
        db.conn.rollback()

        db._execute(
            "UPDATE import_jobs SET status = 'completed' WHERE id = %s",
            (first_id,),
        )
        db._execute("""
            INSERT INTO import_jobs (
                job_type, status, request_id, dedupe_key, payload
            )
            VALUES (
                'force_import', 'queued', %s, 'force_import:download_log:1',
                '{"failed_path": "/tmp/new"}'::jsonb
            )
        """, (req_id,))

        for column, bad_value in (("status", "bogus"), ("job_type", "bogus")):
            with self.subTest(column=column):
                with self.assertRaises(psycopg2.errors.CheckViolation):
                    db._execute("""
                        INSERT INTO import_jobs (
                            job_type, status, payload
                        )
                        VALUES (
                            %s, %s, '{"failed_path": "/tmp/bad"}'::jsonb
                        )
                    """, (
                        bad_value if column == "job_type" else "force_import",
                        bad_value if column == "status" else "queued",
                    ))
                db.conn.rollback()

        indexes = db._execute("""
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename = 'import_jobs'
        """).fetchall()
        index_names = {row["indexname"] for row in indexes}
        self.assertIn("idx_import_jobs_active_dedupe", index_names)
        self.assertIn("idx_import_jobs_claim", index_names)
        db.close()

    def test_legacy_terminal_preview_jobs_are_normalized(self):
        """Migration 006 keeps old terminal history out of preview backlog."""
        db = make_db()
        req_id = db.add_request(
            mb_release_id="queue-preview-legacy-terminal-mbid",
            artist_name="Queue",
            album_title="Legacy Terminal Preview",
            source="request",
        )
        cur = db._execute("""
            INSERT INTO import_jobs (
                job_type, status, request_id, payload, preview_status,
                preview_attempts, message, completed_at
            )
            VALUES (
                'automation_import', 'completed', %s, '{}'::jsonb, 'waiting',
                0, 'Automation import processing completed', NOW()
            )
            RETURNING id
        """, (req_id,))
        row = cur.fetchone()
        assert row is not None

        migration = (
            Path(__file__).resolve().parents[1]
            / "migrations"
            / "006_normalize_legacy_terminal_preview_jobs.sql"
        )
        db._execute(migration.read_text(encoding="utf-8"))

        cur = db._execute("""
            SELECT preview_status, preview_message, preview_completed_at,
                   importable_at
            FROM import_jobs
            WHERE id = %s
        """, (row["id"],))
        normalized = cur.fetchone()
        assert normalized is not None
        self.assertEqual(normalized["preview_status"], "would_import")
        self.assertEqual(
            normalized["preview_message"],
            "Queued before async preview gate",
        )
        self.assertIsNotNone(normalized["preview_completed_at"])
        self.assertIsNotNone(normalized["importable_at"])
        db.close()

    def test_import_job_preview_schema_constraints_and_indexes(self):
        """Migration 004 adds durable async preview state to import_jobs."""
        db = make_db()
        req_id = db.add_request(
            mb_release_id="queue-preview-schema-mbid",
            artist_name="Queue",
            album_title="Preview Schema",
            source="request",
        )

        cur = db._execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'import_jobs'
        """)
        column_names = {r["column_name"] for r in cur.fetchall()}
        for column in (
            "preview_status",
            "preview_result",
            "preview_message",
            "preview_error",
            "preview_attempts",
            "preview_worker_id",
            "preview_started_at",
            "preview_heartbeat_at",
            "preview_completed_at",
            "importable_at",
        ):
            self.assertIn(column, column_names)

        cur = db._execute("""
            INSERT INTO import_jobs (job_type, request_id, payload)
            VALUES (
                'force_import', %s,
                '{"failed_path": "/tmp/force"}'::jsonb
            )
            RETURNING preview_status, preview_message, preview_attempts,
                      preview_completed_at, importable_at
        """, (req_id,))
        row = cur.fetchone()
        assert row is not None
        self.assertEqual(row["preview_status"], "would_import")
        self.assertEqual(row["preview_message"], "Preview gate disabled")
        self.assertEqual(row["preview_attempts"], 0)
        self.assertIsNotNone(row["preview_completed_at"])
        self.assertIsNotNone(row["importable_at"])

        with self.assertRaises(psycopg2.errors.CheckViolation):
            db._execute("""
                INSERT INTO import_jobs (
                    job_type, request_id, payload, preview_status
                )
                VALUES (
                    'force_import', %s,
                    '{"failed_path": "/tmp/force"}'::jsonb,
                    'not-a-preview-state'
                )
            """, (req_id,))
        db.conn.rollback()

        cur = db._execute("""
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename = 'import_jobs'
        """)
        index_names = {r["indexname"] for r in cur.fetchall()}
        self.assertIn("idx_import_jobs_preview_claim", index_names)
        self.assertIn("idx_import_jobs_importable_claim", index_names)
        db.close()


@requires_postgres
class TestAddAndGetRequest(unittest.TestCase):
    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_add_get_roundtrip(self):
        req_id = self.db.add_request(
            mb_release_id="44438bf9-26d9-4460-9b4f-1a1b015e37a1",
            artist_name="Buke and Gase",
            album_title="Riposte",
            source="redownload",
            year=2014,
            country="US",
        )
        self.assertIsInstance(req_id, int)

        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["mb_release_id"], "44438bf9-26d9-4460-9b4f-1a1b015e37a1")
        self.assertEqual(req["artist_name"], "Buke and Gase")
        self.assertEqual(req["album_title"], "Riposte")
        self.assertEqual(req["source"], "redownload")
        self.assertEqual(req["status"], "wanted")
        self.assertEqual(req["year"], 2014)
        self.assertEqual(req["country"], "US")

    def test_add_minimal_fields(self):
        req_id = self.db.add_request(
            mb_release_id="test-uuid",
            artist_name="Test",
            album_title="Test Album",
            source="request",
        )
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["status"], "wanted")
        self.assertIsNone(req["year"])

    def test_duplicate_mb_release_id_raises(self):
        self.db.add_request(
            mb_release_id="dup-uuid",
            artist_name="A",
            album_title="B",
            source="redownload",
        )
        with self.assertRaises(psycopg2.errors.UniqueViolation):
            self.db.add_request(
                mb_release_id="dup-uuid",
                artist_name="C",
                album_title="D",
                source="request",
            )
        self.db.conn.rollback()

    def test_get_nonexistent_returns_none(self):
        self.assertIsNone(self.db.get_request(9999))

    def test_get_by_mb_release_id(self):
        self.db.add_request(
            mb_release_id="find-me-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        req = self.db.get_request_by_mb_release_id("find-me-uuid")
        assert req is not None
        self.assertEqual(req["artist_name"], "A")

    def test_get_by_mb_release_id_not_found(self):
        self.assertIsNone(self.db.get_request_by_mb_release_id("nope"))

    def test_add_with_discogs_id(self):
        req_id = self.db.add_request(
            artist_name="Test",
            album_title="Test Album",
            source="request",
            discogs_release_id="12345",
        )
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["discogs_release_id"], "12345")
        self.assertIsNone(req["mb_release_id"])

    def test_get_by_discogs_release_id(self):
        self.db.add_request(
            artist_name="A",
            album_title="B",
            source="request",
            discogs_release_id="67890",
        )
        req = self.db.get_request_by_discogs_release_id("67890")
        assert req is not None
        self.assertEqual(req["artist_name"], "A")

    def test_get_by_discogs_release_id_not_found(self):
        self.assertIsNone(self.db.get_request_by_discogs_release_id("nope"))

    def test_delete_request(self):
        req_id = self.db.add_request(
            mb_release_id="del-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        self.db.delete_request(req_id)
        self.assertIsNone(self.db.get_request(req_id))


@requires_postgres
class TestImportJobQueueAPI(unittest.TestCase):
    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="queue-api-mbid",
            artist_name="Queue",
            album_title="API",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_enqueue_dedupes_active_job_and_allows_after_completion(self):
        from lib.import_queue import (
            IMPORT_JOB_FORCE,
            force_import_dedupe_key,
            force_import_payload,
        )

        dedupe = force_import_dedupe_key(17)
        payload = force_import_payload(
            download_log_id=17,
            failed_path="/tmp/failed",
            source_username="alice",
        )
        first = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key=dedupe,
            payload=payload,
        )
        duplicate = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key=dedupe,
            payload=payload,
        )

        self.assertEqual(first.id, duplicate.id)
        self.assertFalse(first.deduped)
        self.assertTrue(duplicate.deduped)

        self.db.mark_import_job_completed(
            first.id,
            result={"success": True},
            message="done",
        )
        later = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key=dedupe,
            payload=payload,
        )
        self.assertNotEqual(first.id, later.id)

    def test_generic_automation_enqueue_is_rejected(self):
        from lib.import_queue import (
            IMPORT_JOB_AUTOMATION,
            automation_import_dedupe_key,
            automation_import_payload,
        )

        with self.assertRaisesRegex(
            ValueError,
            "handoff_automation_import",
        ):
            self.db.enqueue_import_job(
                IMPORT_JOB_AUTOMATION,
                request_id=self.req_id,
                dedupe_key=automation_import_dedupe_key(self.req_id),
                payload=automation_import_payload(),
            )


    def test_execution_lease_columns_round_trip_through_import_job(self):
        from lib.import_queue import (
            IMPORT_JOB_FORCE,
            force_import_dedupe_key,
            force_import_payload,
        )

        job = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key=force_import_dedupe_key(812),
            payload=force_import_payload(
                download_log_id=812,
                failed_path="/tmp/lease-round-trip",
                source_username="alice",
            ),
        )
        self.db._execute("""
            UPDATE import_jobs
            SET execution_invocation_id = 'invocation-round-trip',
                execution_host_boot_id = 'boot-round-trip',
                execution_systemd_unit = 'cratedigger-importer.service',
                execution_worker_pid = 1234,
                execution_worker_start_ticks = 5678,
                execution_beets_pid = 2345,
                execution_beets_start_ticks = 6789
            WHERE id = %s
        """, (job.id,))
        self.db.conn.commit()

        stored = self.db.get_import_job(job.id)
        assert stored is not None
        self.assertEqual(stored.execution_invocation_id, "invocation-round-trip")
        self.assertEqual(stored.execution_host_boot_id, "boot-round-trip")
        self.assertEqual(
            stored.execution_systemd_unit,
            "cratedigger-importer.service",
        )
        self.assertEqual(stored.execution_worker_pid, 1234)
        self.assertEqual(stored.execution_worker_start_ticks, 5678)
        self.assertEqual(stored.execution_beets_pid, 2345)
        self.assertEqual(stored.execution_beets_start_ticks, 6789)

    def test_malformed_force_payload_cannot_insert_or_poison_dedupe(self):
        from lib.import_queue import (
            IMPORT_JOB_FORCE,
            force_import_dedupe_key,
            force_import_payload,
        )

        dedupe = force_import_dedupe_key(37206)
        valid = force_import_payload(
            download_log_id=37206,
            failed_path="/tmp/failed",
        )
        malformed = {**valid, "unexpected": True}

        with self.assertRaises(msgspec.ValidationError):
            self.db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=self.req_id,
                dedupe_key=dedupe,
                payload=malformed,
            )

        self.assertFalse(any(
            job.dedupe_key == dedupe
            for job in self.db.list_import_jobs(limit=100)
        ))
        job = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key=dedupe,
            payload=valid,
        )
        self.assertEqual(job.dedupe_key, dedupe)
        self.assertFalse(job.deduped)

    def test_list_terminal_force_wrong_match_cleanup_jobs_selects_missing_receipts(
        self,
    ) -> None:
        """Issue #1122: the predicate is a positive selection rule, not an
        exclusion enumeration.

        Real-PG proof of the JSONB predicate in
        ``list_terminal_force_wrong_match_cleanup_jobs`` — the ``?``
        key-existence operator, the ``#>>`` nested-success extraction, and
        ``IS DISTINCT FROM`` NULL handling are exactly the class of
        behavior a fake cannot validate. Covers the review-round
        corrections: MAJOR-1 (success-keyed, not presence-keyed — a
        receipt can be present with ``success: false``), MAJOR-2/3 (the
        ``post_commit_wrong_match_scenario`` era-AND-lane marker excludes
        every historical/non-adjudicating shape by construction, not by
        naming each one).
        """
        from lib.import_queue import IMPORT_JOB_FORCE, force_import_payload

        def _force_job(suffix: str):
            return self.db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=self.req_id,
                dedupe_key=f"force-wrong-match-predicate:{suffix}",
                payload=force_import_payload(
                    download_log_id=1,
                    failed_path="/tmp/predicate-source",
                ),
            )

        # -- completed arm --------------------------------------------------

        # Marker present, receipt entirely missing -> included.
        completed_missing = _force_job("completed-missing")
        self.db.mark_import_job_completed(
            completed_missing.id,
            result={
                "success": True, "message": "done", "deferred": False,
                "code": None, "post_commit_wrong_match_scenario": None,
            },
            message="done",
        )

        # Marker present, receipt present but success=false (e.g. entry
        # not found, unsafe path, rmtree failure, EACCES) -> included:
        # MAJOR-1's exact fix. A presence-only check would park this row.
        completed_failed_receipt = _force_job("completed-failed-receipt")
        self.db.mark_import_job_completed(
            completed_failed_receipt.id,
            result={
                "success": True, "message": "done", "deferred": False,
                "code": None, "post_commit_wrong_match_scenario": None,
                "wrong_match_dismissal": {
                    "success": False, "error": "path_unavailable: EACCES",
                },
            },
            message="done",
        )

        # Marker present, receipt proven successful -> excluded.
        completed_successful_receipt = _force_job("completed-successful-receipt")
        self.db.mark_import_job_completed(
            completed_successful_receipt.id,
            result={
                "success": True, "message": "done", "deferred": False,
                "code": None, "post_commit_wrong_match_scenario": None,
                "wrong_match_dismissal": {"success": True},
            },
            message="done",
        )

        # -- failed arm -------------------------------------------------

        # Marker present, receipt entirely missing, ordinary code ->
        # included.
        failed_missing = _force_job("failed-missing")
        self.db.mark_import_job_failed(
            failed_missing.id,
            error="beets rejected: audio_corrupt",
            result={
                "success": False, "message": "rejected", "deferred": False,
                "code": None,
                "post_commit_wrong_match_scenario": "audio_corrupt",
            },
            message="rejected",
        )

        # Marker present, receipt present but success=false -> included
        # (MAJOR-1, failure arm).
        failed_failed_receipt = _force_job("failed-failed-receipt")
        self.db.mark_import_job_failed(
            failed_failed_receipt.id,
            error="beets rejected: audio_corrupt",
            result={
                "success": False, "message": "rejected", "deferred": False,
                "code": None,
                "post_commit_wrong_match_scenario": "audio_corrupt",
                "cleanup": {
                    "success": False, "outcome": "deleted_operator_force_source",
                    "error": "path_unavailable: EACCES",
                },
            },
            message="rejected",
        )

        # Marker present, receipt proven successful -> excluded.
        failed_successful_receipt = _force_job("failed-successful-receipt")
        self.db.mark_import_job_failed(
            failed_successful_receipt.id,
            error="beets rejected",
            result={
                "success": False, "message": "rejected", "deferred": False,
                "code": None,
                "post_commit_wrong_match_scenario": "high_distance",
                "cleanup": {
                    "success": True,
                    "outcome": "preserved_operator_force_source",
                },
            },
            message="rejected",
        )

        # Marker present, receipt missing, but ``requeue_failed`` — the
        # live code never runs the wrong-match decision for this code ->
        # excluded.
        failed_requeue = _force_job("failed-requeue")
        self.db.mark_import_job_failed(
            failed_requeue.id,
            error="requeue failed",
            result={
                "success": False, "message": "requeue UPDATE failed",
                "deferred": False, "code": "requeue_failed",
                "post_commit_wrong_match_scenario": None,
            },
            message="requeue UPDATE failed",
        )

        # The terminal retry-budget bail removes only its private action
        # copy; it must never be replayed as a Wrong Matches decision.
        failed_requeue_exhausted = _force_job("failed-requeue-exhausted")
        self.db.mark_import_job_failed(
            failed_requeue_exhausted.id,
            error="preview/import requeue budget exhausted",
            result={
                "success": False, "message": "budget exhausted",
                "deferred": False, "code": "requeue_exhausted",
                "post_commit_wrong_match_scenario": None,
            },
            message="budget exhausted",
        )

        # Marker present, receipt missing, but ``deferred`` — e.g.
        # release-lock contention. The live cleanup helper IS called but
        # its own first line skips the decision immediately, so no
        # receipt ever lands; replay must not manufacture one -> excluded.
        failed_deferred = _force_job("failed-deferred")
        self.db.mark_import_job_failed(
            failed_deferred.id,
            error="Another import is already in progress",
            result={
                "success": False,
                "message": "Another import is already in progress",
                "deferred": True, "code": None,
                "post_commit_wrong_match_scenario": None,
            },
            message="Another import is already in progress",
        )

        # -- historical / non-adjudicating shapes (MAJOR-2/3) ------------

        # Completed, NO era marker at all (pre-#1122 shape) -> excluded
        # forever by design, even though a bare presence check would have
        # selected it.
        historical_completed = _force_job("historical-completed-no-marker")
        self.db.mark_import_job_completed(
            historical_completed.id,
            result={"success": True},
            message="done",
        )

        # Failed, NO era marker (e.g. the executor-crash literal
        # ``{"success": false}`` written before ``_job_result`` is ever
        # computed, or any other historical/operator shape) -> excluded.
        historical_failed = _force_job("historical-failed-no-marker")
        self.db.mark_import_job_failed(
            historical_failed.id,
            error="RuntimeError: boom",
            result={"success": False},
            message="Executor crashed",
        )

        # Failed with a genuinely NULL ``result`` column -> excluded: no
        # marker can be present in a NULL column, so this stays receiptless
        # forever by the same rule, not a special case.
        historical_null_result = _force_job("historical-null-result")
        self.db._execute(
            "UPDATE import_jobs SET status = 'failed', result = NULL "
            "WHERE id = %s",
            (historical_null_result.id,),
        )
        self.db.conn.commit()

        selected = {
            job.id
            for job in self.db.list_terminal_force_wrong_match_cleanup_jobs()
        }
        self.assertIn(completed_missing.id, selected)
        self.assertIn(completed_failed_receipt.id, selected)
        self.assertNotIn(completed_successful_receipt.id, selected)
        self.assertIn(failed_missing.id, selected)
        self.assertIn(failed_failed_receipt.id, selected)
        self.assertNotIn(failed_successful_receipt.id, selected)
        self.assertNotIn(failed_requeue.id, selected)
        self.assertNotIn(failed_requeue_exhausted.id, selected)
        self.assertNotIn(failed_deferred.id, selected)
        self.assertNotIn(historical_completed.id, selected)
        self.assertNotIn(historical_failed.id, selected)
        self.assertNotIn(historical_null_result.id, selected)

    def test_enqueue_youtube_import_is_allowed_by_pg_constraint(self):
        from lib.import_queue import (
            IMPORT_JOB_YOUTUBE,
            YoutubeImportPayload,
            youtube_import_dedupe_key,
            youtube_import_payload,
        )

        job = self.db.enqueue_import_job(
            IMPORT_JOB_YOUTUBE,
            request_id=self.req_id,
            dedupe_key=youtube_import_dedupe_key(17),
            payload=youtube_import_payload(
                staged_path="/tmp/youtube-staged",
                request_id=self.req_id,
                browse_id="MPREb_pg_constraint",
                download_log_id=17,
            ),
        )

        self.assertEqual(job.job_type, IMPORT_JOB_YOUTUBE)
        self.assertEqual(job.request_id, self.req_id)
        assert isinstance(job.payload, YoutubeImportPayload)
        self.assertEqual(job.payload.browse_id, "MPREb_pg_constraint")

    def test_claim_complete_and_fail_lifecycle(self):
        from lib.import_queue import IMPORT_JOB_FORCE

        job = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key="manual:1",
            payload={"download_log_id": 1, "failed_path": "/tmp/manual"},
        )
        self.db.mark_import_job_preview_importable(
            job.id,
            preview_result={"verdict": "would_import"},
            message="ready",
        )
        claimed = claim_next_import_job(self.db, worker_id="test-worker")
        assert claimed is not None
        self.assertEqual(claimed.status, "running")
        self.assertEqual(claimed.worker_id, "test-worker")
        self.assertEqual(claimed.attempts, 1)
        self.assertIsNone(claim_next_import_job(self.db, worker_id="other"))

        completed = self.db.mark_import_job_completed(
            claimed.id,
            result={"success": True},
            message="imported",
        )
        assert completed is not None
        self.assertEqual(completed.status, "completed")
        self.assertEqual(completed.result, {"success": True})

        missing = self.db.mark_import_job_failed(
            999999,
            error="missing",
            message="missing",
        )
        self.assertIsNone(missing)


    def test_two_sessions_cannot_claim_same_job(self):
        from lib import pipeline_db
        from lib.import_queue import IMPORT_JOB_FORCE

        job = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key="manual:claim-once",
            payload={"download_log_id": 1, "failed_path": "/tmp/manual"},
        )
        self.db.mark_import_job_preview_importable(
            job.id,
            preview_result={"verdict": "would_import"},
            message="ready",
        )
        other = pipeline_db.PipelineDB(TEST_DSN)
        try:
            first = claim_next_import_job(self.db, worker_id="one")
            second = claim_next_import_job(other, worker_id="two")
            self.assertIsNotNone(first)
            self.assertIsNone(second)
        finally:
            other.close()

    def _seed_downloading_force_job(
        self,
        *,
        suffix: str,
        importable: bool,
        source_path: str | None = None,
    ) -> tuple[int, ImportJob, str]:
        request_id = self.db.add_request(
            mb_release_id=f"force-owner-{suffix}",
            artist_name="Force Owner",
            album_title=suffix,
            source="request",
        )
        witness = f"2026-07-29T12:00:{request_id % 60:02d}+00:00"
        exact_source_path = source_path or f"/tmp/force-owner-{suffix}"
        state = ActiveDownloadState(
            filetype="flac",
            enqueued_at=witness,
            current_path=exact_source_path,
            files=[],
        )
        self.assertTrue(self.db.set_downloading(
            request_id,
            state.to_json(),
            expected_status="wanted",
        ))
        source_download_log_id = self.db.log_download(
            request_id,
            outcome="rejected",
            error_message="force owner source",
        )
        job = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=request_id,
            dedupe_key=f"force-owner:{suffix}",
            payload=force_import_payload(
                download_log_id=source_download_log_id,
                failed_path=exact_source_path,
            ),
        )
        if importable:
            ready = self.db.mark_import_job_preview_importable(
                job.id,
                preview_result={"verdict": "evidence_ready"},
            )
            assert ready is not None
            job = ready
        return request_id, job, witness

    def test_force_workers_retain_import_lock_through_effects(self) -> None:
        """Real sessions serialize handoff behind preview and import effects."""
        from lib.pipeline_db import PipelineDB
        from scripts import import_preview_worker, importer

        for lane in ("preview", "import"):
            with self.subTest(lane=lane):
                request_id, _job, witness = (
                    self._seed_downloading_force_job(
                        suffix=f"retained-{lane}",
                        importable=lane == "import",
                    )
                )
                effect_started = threading.Event()
                release_effect = threading.Event()
                errors: list[BaseException] = []

                def preview_effect(
                    _stage_db: object,
                    claimed: ImportJob,
                    _effect_started: threading.Event = effect_started,
                    _release_effect: threading.Event = release_effect,
                    **_kwargs: object,
                ) -> ImportJob:
                    _effect_started.set()
                    self.assertTrue(_release_effect.wait(timeout=5))
                    return claimed

                def import_effect(
                    _stage_db: object,
                    _claimed: ImportJob,
                    *,
                    ctx: object | None = None,
                    cancellation_token: CancellationToken,
                    owner_session_identity: OwnerSessionIdentity,
                    _effect_started: threading.Event = effect_started,
                    _release_effect: threading.Event = release_effect,
                ) -> DispatchOutcome:
                    del ctx, cancellation_token, owner_session_identity
                    _effect_started.set()
                    self.assertTrue(_release_effect.wait(timeout=5))
                    return DispatchOutcome(False, "lock overlap proof")

                def run_force(
                    _lane: str = lane,
                    _errors: list[BaseException] = errors,
                ) -> None:
                    try:
                        if _lane == "preview":
                            import_preview_worker.run_once(
                                self.db,
                                worker_id=f"real-force-{_lane}",
                                execution_lease_factory=(
                                    _unavailable_execution_lease
                                ),
                                process_fn=preview_effect,
                            )
                        else:
                            importer.run_once(
                                self.db,
                                worker_id=f"real-force-{_lane}",
                                execution_lease_factory=(
                                    _unavailable_execution_lease
                                ),
                                execute_fn=import_effect,
                            )
                    except BaseException as exc:  # noqa: BLE001 - thread handoff
                        _errors.append(exc)

                worker = threading.Thread(target=run_force)
                worker.start()
                self.assertTrue(effect_started.wait(timeout=5))
                contender = PipelineDB(TEST_DSN)
                try:
                    blocked = contender.handoff_automation_import(
                        request_id=request_id,
                        expected_enqueued_at=witness,
                        canonical_path=f"/tmp/processing-{lane}",
                        message="must wait for force owner",
                    )
                    self.assertEqual(blocked.outcome, "lock_unavailable")
                    request = contender.get_request(request_id)
                    assert request is not None
                    self.assertEqual(request["status"], "downloading")

                    release_effect.set()
                    worker.join(timeout=5)
                    self.assertFalse(worker.is_alive())
                    self.assertEqual(errors, [])

                    committed = contender.handoff_automation_import(
                        request_id=request_id,
                        expected_enqueued_at=witness,
                        canonical_path=f"/tmp/processing-{lane}",
                        message="force owner exited",
                    )
                    self.assertEqual(committed.outcome, "committed")
                finally:
                    release_effect.set()
                    worker.join(timeout=5)
                    contender.close()

    def test_known_bad_force_owner_released_before_effect_is_detected(
        self,
    ) -> None:
        """Qualify the lock-overlap proof against the historical mutant."""
        from lib.pipeline_db import (
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            PipelineDB,
        )

        for lane in ("preview", "import"):
            with self.subTest(lane=lane):
                request_id, candidate, witness = (
                    self._seed_downloading_force_job(
                        suffix=f"released-{lane}",
                        importable=lane == "import",
                    )
                )
                effect_started = threading.Event()
                release_effect = threading.Event()

                def released_owner_mutant(
                    _lane: str = lane,
                    _request_id: int = request_id,
                    _candidate: ImportJob = candidate,
                    _effect_started: threading.Event = effect_started,
                    _release_effect: threading.Event = release_effect,
                ) -> None:
                    stage = PipelineDB(TEST_DSN)
                    token = CancellationToken()
                    try:
                        with stage._pin_owner_session(
                            token,
                        ), stage.advisory_lock(
                            ADVISORY_LOCK_NAMESPACE_IMPORT,
                            _request_id,
                        ) as acquired:
                            self.assertTrue(acquired)
                            if _lane == "preview":
                                claimed = (
                                    stage
                                    .claim_force_import_preview_job_under_lock(
                                        _candidate.id,
                                        request_id=_request_id,
                                        worker_id="known-bad-preview",
                                    )
                                )
                            else:
                                claimed = (
                                    stage.claim_force_import_job_under_lock(
                                        _candidate.id,
                                        request_id=_request_id,
                                        worker_id="known-bad-import",
                                    )
                                )
                            self.assertIsNotNone(claimed)
                        # Known-bad shape: effect begins after IMPORT exits.
                        _effect_started.set()
                        self.assertTrue(_release_effect.wait(timeout=5))
                    finally:
                        stage.close()

                worker = threading.Thread(target=released_owner_mutant)
                worker.start()
                self.assertTrue(effect_started.wait(timeout=5))
                contender = PipelineDB(TEST_DSN)
                try:
                    committed = contender.handoff_automation_import(
                        request_id=request_id,
                        expected_enqueued_at=witness,
                        canonical_path=f"/tmp/mutant-processing-{lane}",
                        message="known bad overlaps force effect",
                    )
                    self.assertEqual(committed.outcome, "committed")
                    self.assertTrue(worker.is_alive())
                finally:
                    release_effect.set()
                    worker.join(timeout=5)
                    contender.close()

    def test_force_backend_loss_terminates_real_child_group_before_recovery(
        self,
    ) -> None:
        """The full force lane reaps real import_one.py before recovery."""
        from beets import library as beets_library

        from lib.config import CratediggerConfig
        from lib.dispatch import dispatch_import_from_db
        from lib.dispatch.subprocess_runner import run_import_one
        from lib.pipeline_db import PipelineDB
        from lib.quality_evidence import snapshot_audio_files
        from scripts import importer

        errors: list[BaseException] = []
        backend_pid: list[int] = []
        later_effects: list[str] = []
        with tempfile.TemporaryDirectory() as root:
            raw_source = Path(root, "operator-source")
            raw_source.mkdir()
            Path(raw_source, "01.mp3").write_bytes(b"owned source")
            request_id, job, _witness = self._seed_downloading_force_job(
                suffix="backend-kill",
                importable=False,
                source_path=str(raw_source),
            )
            self.db.set_tracks(request_id, [{
                "disc_number": 1,
                "track_number": 1,
                "title": "Controlled child",
                "length_seconds": 60,
                "track_artist": "Force Owner",
            }])

            processing_dir = Path(root, "processing")
            action_path = (
                processing_dir / "albums" / f"force-action-{job.id}"
            )
            action_path.mkdir(parents=True)
            Path(action_path, "01.mp3").write_bytes(b"owned source")
            evidence = make_album_quality_evidence(
                mb_release_id="force-owner-backend-kill",
                source_path=str(action_path),
                files=snapshot_audio_files(str(action_path)),
            )
            self.db.upsert_album_quality_evidence(evidence)
            persisted = self.db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert persisted is not None and persisted.id is not None
            self.db.set_import_job_candidate_evidence(job.id, persisted.id)
            ready = self.db.mark_import_job_preview_importable(
                job.id,
                preview_result={
                    "verdict": "evidence_ready",
                    "action_path": str(action_path),
                },
            )
            assert ready is not None
            job = ready

            library_root = Path(root, "library")
            library_root.mkdir()
            library_db = Path(root, "beets-library.db")
            beets = beets_library.Library(
                str(library_db),
                str(library_root),
            )
            beets._close()
            harness_dir = Path(root, "harness")
            harness_dir.mkdir()
            harness_path = Path(harness_dir, "run_beets_harness.sh")
            harness_path.write_text("# controlled sibling lookup\n")
            leader_file = Path(root, "leader.pid")
            descendant_file = Path(root, "descendant.pid")
            forbidden_effect = Path(root, "must-not-exist")
            child_program = (
                "import os,time,pathlib\n"
                f"pathlib.Path({str(leader_file)!r}).write_text(str(os.getpid()))\n"
                "child=os.fork()\n"
                "if child == 0:\n"
                "    time.sleep(60)\n"
                "    raise SystemExit(0)\n"
                f"pathlib.Path({str(descendant_file)!r}).write_text(str(child))\n"
                "time.sleep(60)\n"
            )
            Path(harness_dir, "import_one.py").write_text(child_program)
            cfg = CratediggerConfig(
                processing_dir=str(processing_dir),
                beets_harness_path=str(harness_path),
                beets_library_db=str(library_db),
                beets_directory=str(library_root),
            )

            def tracked_real_import_one(**kwargs: object):
                result = run_import_one(**kwargs)  # pyright: ignore[reportArgumentType]
                later_effects.append("after-child")
                forbidden_effect.write_text("must not execute")
                return result

            def full_force_dispatch(
                stage_db: PipelineDB,
                **kwargs: object,
            ) -> DispatchOutcome:
                return dispatch_import_from_db(
                    stage_db,
                    run_import_fn=tracked_real_import_one,
                    **kwargs,  # pyright: ignore[reportArgumentType]
                )

            def execute_through_production(
                stage_db: PipelineDB,
                claimed: ImportJob,
                *,
                ctx: object | None = None,
                cancellation_token: CancellationToken,
                owner_session_identity: OwnerSessionIdentity,
            ) -> DispatchOutcome:
                backend_pid.append(owner_session_identity.backend_pid)
                return importer.execute_import_job(
                    stage_db,
                    claimed,
                    ctx=ctx,
                    cancellation_token=cancellation_token,
                    owner_session_identity=owner_session_identity,
                    force_dispatch_fn=full_force_dispatch,
                    force_runtime_config=cfg,
                )

            def run_force() -> None:
                try:
                    importer.run_once(
                        self.db,
                        worker_id="force-backend-kill",
                        execution_lease_factory=_unavailable_execution_lease,
                        execute_fn=execute_through_production,
                    )
                except BaseException as exc:  # noqa: BLE001 - thread handoff
                    errors.append(exc)

            worker = threading.Thread(target=run_force)
            worker.start()
            deadline = time.monotonic() + 10
            while (
                (not leader_file.exists() or not descendant_file.exists())
                and time.monotonic() < deadline
            ):
                threading.Event().wait(0.01)
            self.assertTrue(leader_file.exists())
            self.assertTrue(descendant_file.exists())
            stage_pid_cur = PipelineDB(TEST_DSN)
            try:
                self.assertEqual(len(backend_pid), 1)
                killed = stage_pid_cur._execute(
                    "SELECT pg_terminate_backend(%s) AS killed",
                    (backend_pid[0],),
                ).fetchone()
                self.assertTrue(killed["killed"])
            finally:
                stage_pid_cur.close()

            worker.join(timeout=10)
            self.assertFalse(worker.is_alive())
            self.assertEqual(len(errors), 1)
            self.assertIsInstance(errors[0], ExecutionCancelled)
            self.assertEqual(later_effects, [])
            self.assertFalse(forbidden_effect.exists())
            leader_pid = int(leader_file.read_text())
            with self.assertRaises(ProcessLookupError):
                os.killpg(leader_pid, 0)
            descendant_pid = int(descendant_file.read_text())
            with self.assertRaises(ProcessLookupError):
                os.kill(descendant_pid, 0)

            running = self.db.get_import_job(job.id)
            request = self.db.get_request(request_id)
            assert running is not None and request is not None
            self.assertEqual(running.status, "running")
            self.assertEqual(request["status"], "downloading")
            self.assertEqual(running.worker_id, "force-backend-kill")
            self.assertEqual(running.request_id, request_id)
            self.assertIsNotNone(running.beets_launch_authorized_at)
            self.assertEqual(
                running.beets_launch_release_id,
                "force-owner-backend-kill",
            )
            self.assertEqual(
                running.beets_launch_source_path,
                str(raw_source),
            )

            recovered = self.db.recover_running_import_jobs(
                requeue_message="child group proven absent",
                recovery_message="backend and child group proven absent",
            )
            self.assertEqual([item.id for item in recovered], [job.id])
            self.assertEqual(recovered[0].status, "failed")
            self.assertIsNotNone(recovered[0].completed_at)

    def test_unlaunched_jobs_can_be_requeued_after_worker_restart(self):
        from lib.import_queue import IMPORT_JOB_FORCE

        job = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key="manual:restart-retry",
            payload={"download_log_id": 1, "failed_path": "/tmp/manual"},
        )
        self.db.mark_import_job_preview_importable(
            job.id,
            preview_result={"verdict": "would_import"},
            message="ready",
        )
        claimed = claim_next_import_job(self.db, worker_id="old-worker")
        assert claimed is not None

        recovered = self.db.recover_running_import_jobs(
            requeue_message="worker restarted",
            recovery_message="operator recovery required",
        )
        self.assertEqual([job.id for job in recovered], [claimed.id])
        self.assertEqual(recovered[0].status, "queued")
        self.assertIsNone(recovered[0].worker_id)
        self.assertIsNone(recovered[0].started_at)
        self.assertIsNone(recovered[0].heartbeat_at)
        self.assertEqual(recovered[0].attempts, 1)

        retried = claim_next_import_job(self.db, worker_id="new-worker")
        assert retried is not None
        self.assertEqual(retried.id, claimed.id)
        self.assertEqual(retried.attempts, 2)
        self.assertEqual(retried.worker_id, "new-worker")

    def test_default_force_action_copy_path_matches_the_real_derivation(
        self,
    ) -> None:
        """Issue #1089 review MAJOR-1: ``recover_running_import_jobs``'s
        production default for ``force_action_copy_path_fn`` must derive
        the SAME path ``lib.preview_snapshot.force_action_copy_path`` itself
        computes from the currently configured runtime — not a hand-typed
        approximation the two could silently drift from. Limit (review
        round 3 item 6): ``read_runtime_config`` is patched away here, so
        this proves the two callers agree on how to TURN a config into a
        path, never that ``_default_force_action_copy_path`` resolves the
        real config source correctly on its own."""
        from unittest.mock import patch

        from lib.config import CratediggerConfig
        from lib.pipeline_db.import_jobs import _default_force_action_copy_path
        from lib.preview_snapshot import force_action_copy_path

        cfg = CratediggerConfig(processing_dir="/tmp/cratedigger-processing")
        with patch(
            "lib.config.read_runtime_config", return_value=cfg,
        ):
            self.assertEqual(
                _default_force_action_copy_path(4242),
                force_action_copy_path(cfg, 4242),
            )

    def test_import_claim_requires_preview_importable(self):
        from lib.import_queue import IMPORT_JOB_FORCE

        job = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key="manual:preview-gate",
            payload={"download_log_id": 1, "failed_path": "/tmp/manual"},
        )
        self.assertIsNone(claim_next_import_job(self.db, worker_id="too-early"))

        self.db.mark_import_job_preview_importable(
            job.id,
            preview_result={"verdict": "would_import"},
            message="ready",
        )
        claimed = claim_next_import_job(self.db, worker_id="importer")
        assert claimed is not None
        self.assertEqual(claimed.id, job.id)
        self.assertEqual(claimed.status, "running")

    def test_legacy_would_import_row_is_not_claimable(self):
        """Only neutral evidence readiness may enter the importer lane."""
        from lib.import_queue import IMPORT_JOB_FORCE

        job = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key="manual:legacy-would-import",
            payload={"download_log_id": 1, "failed_path": "/tmp/legacy"},
        )
        self.db._execute(
            "UPDATE import_jobs SET preview_status = 'would_import', "
            "importable_at = NOW() WHERE id = %s",
            (job.id,),
        )

        self.assertIsNone(claim_next_import_job(self.db, worker_id="importer"))

        self.db._execute(
            "UPDATE import_jobs SET preview_status = 'evidence_ready' WHERE id = %s",
            (job.id,),
        )
        claimed = claim_next_import_job(self.db, worker_id="importer")
        assert claimed is not None
        self.assertEqual(claimed.id, job.id)

    def test_import_job_timeline_orders_importable_before_waiting(self):
        from lib.import_queue import IMPORT_JOB_FORCE

        waiting = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key="manual:timeline-waiting",
            payload={"download_log_id": 1, "failed_path": "/tmp/waiting"},
        )
        importable = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key="manual:timeline-importable",
            payload={"download_log_id": 1, "failed_path": "/tmp/importable"},
        )
        self.db.mark_import_job_preview_importable(
            importable.id,
            preview_result={"verdict": "would_import"},
            message="ready",
        )

        timeline = self.db.list_import_job_timeline(limit=10)

        self.assertEqual([job.id for job in timeline[:2]], [importable.id, waiting.id])
        self.assertEqual(timeline[0].preview_status, "evidence_ready")

    def test_import_job_timeline_excludes_terminal_jobs(self):
        from lib.import_queue import IMPORT_JOB_FORCE

        importable = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key="manual:timeline-active",
            payload={"download_log_id": 1, "failed_path": "/tmp/active"},
        )
        self.db.mark_import_job_preview_importable(
            importable.id,
            preview_result={"verdict": "would_import"},
            message="ready",
        )
        older = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key="manual:timeline-old-terminal",
            payload={"download_log_id": 1, "failed_path": "/tmp/old"},
        )
        newer = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key="manual:timeline-new-terminal",
            payload={"download_log_id": 1, "failed_path": "/tmp/new"},
        )
        self.db.mark_import_job_failed(
            older.id,
            error="old",
            message="old",
        )
        self.db.mark_import_job_failed(
            newer.id,
            error="new",
            message="new",
        )

        timeline = self.db.list_import_job_timeline(limit=10)

        self.assertEqual([job.id for job in timeline], [importable.id])

    def test_preview_claim_and_importable_lifecycle(self):
        from lib.import_queue import IMPORT_JOB_FORCE

        queued = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key="manual:preview",
            payload={"download_log_id": 1, "failed_path": "/tmp/manual"},
        )
        self.assertEqual(queued.preview_status, "waiting")
        self.assertEqual(queued.preview_attempts, 0)

        claimed = claim_next_import_preview_job(self.db, worker_id="preview-worker",)
        assert claimed is not None
        self.assertEqual(claimed.id, queued.id)
        self.assertEqual(claimed.status, "queued")
        self.assertEqual(claimed.preview_status, "running")
        self.assertEqual(claimed.preview_attempts, 1)
        self.assertEqual(claimed.preview_worker_id, "preview-worker")
        self.assertIsNone(
            claim_next_import_preview_job(self.db, worker_id="other-worker")
        )

        marked = self.db.mark_import_job_preview_importable(
            claimed.id,
            preview_result={
                "verdict": "would_import",
                "stage_chain": ["stage2_import:import"],
            },
            message="Preview would import",
        )
        assert marked is not None
        assert marked.preview_result is not None
        self.assertEqual(marked.status, "queued")
        self.assertEqual(marked.preview_status, "evidence_ready")
        self.assertEqual(marked.preview_result["verdict"], "would_import")
        self.assertEqual(marked.preview_message, "Preview would import")
        self.assertIsNotNone(marked.preview_completed_at)
        self.assertIsNotNone(marked.importable_at)

    def test_preview_rejection_fails_job_with_audit(self):
        """Post-U5: preview failures use ``preview_status='measurement_failed'``.

        ``'uncertain'`` is no longer in ``IMPORT_JOB_PREVIEW_FAILURE_STATUSES``;
        production code writes ``'measurement_failed'`` via the U4 self-healing
        helper.
        """
        from lib.import_queue import IMPORT_JOB_FORCE

        queued = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key="manual:preview-reject",
            payload={"download_log_id": 1, "failed_path": "/tmp/manual"},
        )

        failed = self.db.mark_import_job_preview_failed(
            queued.id,
            preview_status="measurement_failed",
            error="path_missing",
            preview_result={"verdict": "measurement_failed", "reason": "path_missing"},
            message="Preview failed: path_missing",
        )
        assert failed is not None
        assert failed.preview_result is not None
        self.assertEqual(failed.status, "failed")
        self.assertEqual(failed.preview_status, "measurement_failed")
        self.assertEqual(failed.preview_error, "path_missing")
        self.assertEqual(failed.preview_result["reason"], "path_missing")
        self.assertEqual(failed.message, "Preview failed: path_missing")
        self.assertEqual(failed.error, "path_missing")
        self.assertIsNotNone(failed.preview_completed_at)
        self.assertIsNotNone(failed.completed_at)

    def test_two_sessions_cannot_claim_same_preview_job(self):
        from lib import pipeline_db
        from lib.import_queue import IMPORT_JOB_FORCE

        self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key="manual:preview-claim-once",
            payload={"download_log_id": 1, "failed_path": "/tmp/manual"},
        )
        other = pipeline_db.PipelineDB(TEST_DSN)
        try:
            first = claim_next_import_preview_job(self.db, worker_id="one")
            second = claim_next_import_preview_job(other, worker_id="two")
            self.assertIsNotNone(first)
            self.assertIsNone(second)
        finally:
            other.close()


@requires_postgres
class TestAutomationImportHandoff(unittest.TestCase):
    """Authoritative real-PostgreSQL transcript for the U2 ownership edge."""

    ENQUEUED_A = "2026-07-29T00:00:00+00:00"
    ENQUEUED_B = "2026-07-29T00:01:00+00:00"

    def setUp(self) -> None:
        self.db = make_db()
        self.request_id = self.db.add_request(
            mb_release_id="automation-handoff-real",
            artist_name="Exact",
            album_title="Handoff",
            source="request",
        )
        self._set_downloading(self.db, self.ENQUEUED_A)

    def tearDown(self) -> None:
        self.db.close()

    @staticmethod
    def _state(enqueued_at: str) -> str:
        return json.dumps({
            "filetype": "flac",
            "enqueued_at": enqueued_at,
            "last_progress_at": enqueued_at,
            "files": [{
                "username": "peer",
                "filename": "Exact Handoff/01.flac",
                "file_dir": "Exact Handoff",
                "size": 123,
                "retry_count": 0,
                "bytes_transferred": 123,
                "last_state": "Completed, Succeeded",
            }],
        })

    def _set_downloading(self, db, enqueued_at: str) -> None:
        self.assertTrue(db.set_downloading(
            self.request_id,
            self._state(enqueued_at),
            expected_status="wanted",
        ))

    def _handoff(self, db, enqueued_at: str):
        return db.handoff_automation_import(
            request_id=self.request_id,
            expected_enqueued_at=enqueued_at,
            canonical_path="/processing/albums/exact-handoff",
            message="exact handoff",
        )

    def test_handoff_round_trips_exact_owner_and_processing_state(self):
        result = self.db.handoff_automation_import(
            request_id=self.request_id,
            expected_enqueued_at=self.ENQUEUED_A,
            canonical_path="/processing/albums/exact-handoff",
            message="exact handoff",
        )

        self.assertTrue(result.committed)
        assert result.job is not None
        self.assertEqual(result.job.expected_request_status, "processing")
        request = self.db.get_request(self.request_id)
        assert request is not None
        self.assertEqual(request["status"], "processing")
        self.assertEqual(
            request["active_automation_import_job_id"],
            result.job.id,
        )
        state = request["active_download_state"]
        assert state is not None
        self.assertEqual(state["enqueued_at"], self.ENQUEUED_A)
        self.assertEqual(
            state["current_path"],
            "/processing/albums/exact-handoff",
        )
        self.assertTrue(state["processing_started_at"])
        self.assertEqual(
            [job.id for job in self.db.list_import_jobs(
                request_id=self.request_id,
            )],
            [result.job.id],
        )

        before_rejected_writes = copy.deepcopy(request)
        self.assertFalse(self.db.update_download_state_if_downloading(
            self.request_id,
            self._state(self.ENQUEUED_A),
            expected_enqueued_at=self.ENQUEUED_A,
        ))
        self.assertFalse(self.db.reset_downloading_to_wanted(
            self.request_id,
            expected_status="downloading",
        ))
        self.assertEqual(
            self.db.get_request(self.request_id),
            before_rejected_writes,
        )

        repeated = self._handoff(self.db, self.ENQUEUED_A)
        self.assertEqual(repeated.outcome, "not_downloading")
        self.assertEqual(
            len(self.db.list_import_jobs(request_id=self.request_id)),
            1,
        )

    def test_force_jobs_queued_before_handoff_fail_fresh_claim_cas(self):
        """Real PG rejects stale force preview/import after owner handoff."""
        from lib.import_execution import (
            ExecutionLeaseSnapshot,
            ProcessIdentity,
        )
        from lib.import_queue import (
            IMPORT_JOB_FORCE,
            force_import_payload,
        )
        from lib.pipeline_db import ADVISORY_LOCK_NAMESPACE_IMPORT

        preview_job = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.request_id,
            dedupe_key="force-before-handoff:preview",
            payload=force_import_payload(
                download_log_id=93301,
                failed_path="/tmp/force-before-handoff-preview",
            ),
        )
        import_job = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.request_id,
            dedupe_key="force-before-handoff:import",
            payload=force_import_payload(
                download_log_id=93302,
                failed_path="/tmp/force-before-handoff-import",
            ),
        )
        self.assertEqual(preview_job.expected_request_status, "downloading")
        self.assertEqual(import_job.expected_request_status, "downloading")
        self.assertIsNotNone(self.db.mark_import_job_preview_importable(
            import_job.id,
            preview_result={"verdict": "evidence_ready"},
            message="ready before automation handoff",
        ))
        handoff = self._handoff(self.db, self.ENQUEUED_A)
        self.assertTrue(handoff.committed)

        before_preview = self.db.get_import_job(preview_job.id)
        before_import = self.db.get_import_job(import_job.id)
        self.assertIsNone(claim_next_import_preview_job(self.db, worker_id="stale-force-preview",))
        self.assertIsNone(claim_next_import_job(self.db, worker_id="stale-force-import",))
        assert handoff.job is not None
        owner_preview = claim_next_import_preview_job(self.db, worker_id="automation-preview",
        execution_lease=ExecutionLeaseSnapshot(
            host_boot_id="boot-force-fence",
            invocation_id="invocation-force-fence",
            systemd_unit="cratedigger-import-preview-worker.service",
            worker=ProcessIdentity(pid=933, start_ticks=9330),
        ),)
        self.assertIsNotNone(owner_preview)
        assert owner_preview is not None
        self.assertEqual(owner_preview.id, handoff.job.id)
        with self.db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            self.request_id,
        ) as acquired:
            self.assertTrue(acquired)
            self.assertIsNone(
                self.db.claim_force_import_preview_job_under_lock(
                    preview_job.id,
                    request_id=self.request_id,
                    worker_id="stale-force-preview-stage",
                )
            )
            self.assertIsNone(self.db.claim_force_import_job_under_lock(
                import_job.id,
                request_id=self.request_id,
                worker_id="stale-force-import-stage",
            ))
        self.assertEqual(self.db.get_import_job(preview_job.id), before_preview)
        self.assertEqual(self.db.get_import_job(import_job.id), before_import)

    def test_force_beets_launch_rechecks_absent_automation_owner(self):
        """Final force launch fails if ownership changed after a raw claim."""
        from lib.import_queue import IMPORT_JOB_FORCE, force_import_payload

        source_path = "/tmp/force-before-owner-launch"
        force_job = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.request_id,
            dedupe_key="force-before-handoff:launch",
            payload=force_import_payload(
                download_log_id=93303,
                failed_path=source_path,
            ),
        )
        evidence = make_album_quality_evidence(
            mb_release_id="automation-handoff-real",
            source_path=source_path,
        )
        self.db.upsert_album_quality_evidence(evidence)
        persisted = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        self.assertTrue(self.db.set_import_job_candidate_evidence(
            force_job.id,
            persisted.id,
        ))
        self.assertIsNotNone(self.db.mark_import_job_preview_importable(
            force_job.id,
            preview_result={"verdict": "evidence_ready"},
        ))
        claimed = claim_next_import_job(self.db, worker_id="force-importer")
        self.assertIsNotNone(claimed)

        handoff = self._handoff(self.db, self.ENQUEUED_A)
        self.assertTrue(handoff.committed)
        self.assertIsNone(self.db.authorize_import_job_launch(
            force_job.id,
            request_id=self.request_id,
            release_id="automation-handoff-real",
            source_path=source_path,
        ))

    def test_exact_owner_preview_and_importer_commands_round_trip_leases(self):
        from lib.import_execution import ExecutionLeaseSnapshot, ProcessIdentity

        handoff = self._handoff(self.db, self.ENQUEUED_A)
        assert handoff.job is not None
        job = handoff.job
        preview_lease = ExecutionLeaseSnapshot(
            host_boot_id="boot-a",
            invocation_id="preview-a",
            systemd_unit="cratedigger-import-preview.service",
            worker=ProcessIdentity(pid=101, start_ticks=1001),
        )
        stale_lease = dataclasses.replace(
            preview_lease,
            invocation_id="preview-stale",
        )
        self.assertIsNone(claim_next_import_preview_job(self.db, worker_id="missing-lease",))
        claimed_preview = claim_next_import_preview_job(self.db, worker_id="preview",
        execution_lease=preview_lease,)
        assert claimed_preview is not None
        self.assertEqual(
            claimed_preview.execution_invocation_id,
            preview_lease.invocation_id,
        )
        self.assertEqual(self.db.requeue_stale_import_preview_jobs(
            older_than=timedelta(seconds=-1),
            message="heartbeat age is not automation proof",
        ), [])
        self.assertEqual(self.db.requeue_running_import_preview_jobs(
            message="process restart is not automation proof",
        ), [])
        self.assertFalse(self.db.heartbeat_import_job_preview(
            job.id,
            expected_execution_lease=stale_lease,
        ))
        self.assertTrue(self.db.heartbeat_import_job_preview(
            job.id,
            expected_execution_lease=preview_lease,
        ))

        evidence = make_album_quality_evidence(
            mb_release_id="automation-handoff-real",
            source_path="/processing/albums/exact-handoff",
        )
        self.db.upsert_album_quality_evidence(evidence)
        persisted = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        self.assertFalse(self.db.set_import_job_candidate_evidence(
            job.id,
            persisted.id,
            expected_execution_lease=stale_lease,
        ))
        self.assertTrue(self.db.set_import_job_candidate_evidence(
            job.id,
            persisted.id,
            expected_execution_lease=preview_lease,
        ))
        ready = self.db.mark_import_job_preview_importable(
            job.id,
            preview_result={"verdict": "would_import"},
            expected_execution_lease=preview_lease,
        )
        assert ready is not None
        self.assertIsNone(ready.execution_invocation_id)

        importer_lease = ExecutionLeaseSnapshot(
            host_boot_id="boot-a",
            invocation_id="importer-a",
            systemd_unit="cratedigger-importer.service",
            worker=ProcessIdentity(pid=201, start_ticks=2001),
        )
        claimed_import = claim_next_import_job(self.db, worker_id="importer",
        execution_lease=importer_lease,)
        assert claimed_import is not None
        self.assertEqual(
            claimed_import.execution_worker_pid,
            importer_lease.worker.pid,
        )
        self.assertFalse(self.db.heartbeat_import_job(
            job.id,
            expected_execution_lease=preview_lease,
        ))
        self.assertTrue(self.db.heartbeat_import_job(
            job.id,
            expected_execution_lease=importer_lease,
        ))

    def test_preview_heartbeat_and_terminal_lock_order_do_not_deadlock(self):
        """A heartbeat never queues an owner trigger behind its job lock."""
        from lib import pipeline_db
        from lib.import_execution import ExecutionLeaseSnapshot, ProcessIdentity

        handoff = self._handoff(self.db, self.ENQUEUED_A)
        assert handoff.job is not None
        job = handoff.job
        lease = ExecutionLeaseSnapshot(
            host_boot_id="heartbeat-overlap-boot",
            invocation_id="heartbeat-overlap-preview",
            systemd_unit="cratedigger-import-preview-worker.service",
            worker=ProcessIdentity(pid=501, start_ticks=5001),
        )
        assert claim_next_import_preview_job(self.db, worker_id="preview",
        execution_lease=lease,) is not None

        heartbeat = pipeline_db.PipelineDB(TEST_DSN)
        terminal = pipeline_db.PipelineDB(TEST_DSN)
        request_locked = threading.Event()
        job_locked = threading.Event()
        errors: list[psycopg2.Error] = []
        heartbeat.conn.autocommit = False
        terminal.conn.autocommit = False
        try:
            heartbeat._execute(
                "SET LOCAL statement_timeout = '5s'"
            )
            terminal._execute(
                "SET LOCAL statement_timeout = '5s'"
            )
            heartbeat._execute("""
                UPDATE import_jobs
                SET preview_heartbeat_at = NOW(), updated_at = NOW()
                WHERE id = %s
            """, (job.id,))

            def lock_terminal_scope() -> None:
                try:
                    terminal._execute("""
                        SELECT id
                        FROM album_requests
                        WHERE id = %s
                        FOR UPDATE
                    """, (self.request_id,))
                    request_locked.set()
                    terminal._execute("""
                        SELECT id
                        FROM import_jobs
                        WHERE request_id = %s
                        ORDER BY id
                        FOR UPDATE
                    """, (self.request_id,))
                    job_locked.set()
                    terminal.conn.rollback()
                except psycopg2.Error as exc:
                    errors.append(exc)

            worker = threading.Thread(target=lock_terminal_scope)
            worker.start()
            self.assertTrue(request_locked.wait(timeout=5))
            self.assertFalse(job_locked.is_set())

            # Before the trigger was column-restricted this commit ran the
            # deferred owner validator, requested the already-held request
            # row, and deadlocked against terminal's request -> job order.
            heartbeat.conn.commit()
            worker.join(timeout=5)
            self.assertFalse(worker.is_alive())
            self.assertEqual(errors, [])
            self.assertTrue(job_locked.is_set())
        finally:
            for db in (heartbeat, terminal):
                try:
                    db.conn.rollback()
                except psycopg2.Error:
                    pass
                try:
                    db.conn.autocommit = True
                except psycopg2.Error:
                    pass
                db.close()

    def test_record_import_job_beets_child_round_trip_preserves_exact_execution_columns(
        self,
    ):
        from lib.import_execution import (
            ExecutionLeaseSnapshot,
            ExecutionLivenessDecision,
            ExecutionLivenessEvidence,
            ProcessIdentity,
        )

        handoff = self._handoff(self.db, self.ENQUEUED_A)
        assert handoff.job is not None
        job = handoff.job
        preview_lease = ExecutionLeaseSnapshot(
            host_boot_id="boot-old",
            invocation_id="preview-old",
            systemd_unit="cratedigger-import-preview-worker.service",
            worker=ProcessIdentity(pid=301, start_ticks=3001),
        )
        assert claim_next_import_preview_job(self.db, worker_id="preview",
        execution_lease=preview_lease,) is not None
        evidence = make_album_quality_evidence(
            mb_release_id="automation-handoff-real",
            source_path="/processing/albums/exact-handoff",
        )
        self.db.upsert_album_quality_evidence(evidence)
        persisted = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        self.assertTrue(self.db.set_import_job_candidate_evidence(
            job.id,
            persisted.id,
            expected_execution_lease=preview_lease,
        ))
        assert self.db.mark_import_job_preview_importable(
            job.id,
            expected_execution_lease=preview_lease,
        ) is not None
        importer_lease = ExecutionLeaseSnapshot(
            host_boot_id="boot-old",
            invocation_id="importer-old",
            systemd_unit="cratedigger-importer.service",
            worker=ProcessIdentity(pid=401, start_ticks=4001),
        )
        assert claim_next_import_job(self.db, worker_id="importer",
        execution_lease=importer_lease,) is not None
        self.assertIsNotNone(self.db.authorize_import_job_launch(
            job.id,
            request_id=self.request_id,
            release_id="automation-handoff-real",
            source_path="/processing/albums/exact-handoff",
            expected_execution_lease=importer_lease,
        ))
        authorized = self.db._execute(
            """
            SELECT beets_launch_release_id, beets_launch_source_path,
                   beets_launch_request_status,
                   beets_launch_snapshot_fingerprint
            FROM import_jobs
            WHERE id = %s
            """,
            (job.id,),
        ).fetchone()
        assert authorized is not None
        self.assertEqual(
            (
                authorized["beets_launch_release_id"],
                authorized["beets_launch_source_path"],
                authorized["beets_launch_request_status"],
                authorized["beets_launch_snapshot_fingerprint"],
            ),
            (
                "automation-handoff-real",
                "/processing/albums/exact-handoff",
                "processing",
                evidence.snapshot_fingerprint,
            ),
        )
        child_row = self.db.record_import_job_beets_child(
            job.id,
            expected_execution_lease=importer_lease,
            beets_pid=402,
            beets_start_ticks=4002,
        )
        assert child_row is not None
        reloaded = self.db.get_import_job(job.id)
        assert reloaded is not None
        self.assertEqual(
            (
                reloaded.execution_invocation_id,
                reloaded.execution_host_boot_id,
                reloaded.execution_systemd_unit,
                reloaded.execution_worker_pid,
                reloaded.execution_worker_start_ticks,
                reloaded.execution_beets_pid,
                reloaded.execution_beets_start_ticks,
            ),
            (
                importer_lease.invocation_id,
                importer_lease.host_boot_id,
                importer_lease.systemd_unit,
                importer_lease.worker.pid,
                importer_lease.worker.start_ticks,
                402,
                4002,
            ),
        )
        full_lease = dataclasses.replace(
            importer_lease,
            beets=ProcessIdentity(pid=402, start_ticks=4002),
        )
        dead_evidence = ExecutionLivenessEvidence(
            lease=full_lease,
            current_host_boot_id="boot-new",
            boot_error=None,
            worker=None,
            beets=None,
            invocation=None,
            cgroup=None,
        )
        self.assertIsNone(self.db.recover_automation_import_job(
            job.id,
            expected_execution_lease=full_lease,
            decision=ExecutionLivenessDecision(
                status="unknown",
                reason="probe incomplete",
                evidence=dead_evidence,
            ),
            requeue_message="requeue",
            recovery_message="importer restarted",
        ))
        recovered = self.db.recover_automation_import_job(
            job.id,
            expected_execution_lease=full_lease,
            decision=ExecutionLivenessDecision(
                status="dead",
                reason="old boot ended",
                evidence=dead_evidence,
            ),
            requeue_message="requeue",
            recovery_message="importer restarted",
        )
        # A launched owner whose execution is proven dead is CLOSED, not parked
        # (CLAUDE.md invariant 11): the persisted child identity above stays as
        # audit evidence on the terminal row, and the request goes back into the
        # search pool instead of waiting for an operator command.
        assert recovered is not None
        self.assertEqual(recovered.status, "failed")
        self.assertEqual(recovered.execution_beets_pid, 402)
        released = self.db.get_request(self.request_id)
        assert released is not None
        self.assertEqual(released["status"], "wanted")
        self.assertIsNone(released["active_automation_import_job_id"])
        # The ambiguous operation still never replays automatically.
        self.assertIsNone(claim_next_import_job(self.db, worker_id="must-not-replay",
        execution_lease=importer_lease,))

    def test_stale_a_after_b_replacement_creates_nothing(self):
        other = self.db.__class__(TEST_DSN)
        try:
            self.assertTrue(other.reset_downloading_to_wanted(
                self.request_id,
                expected_status="downloading",
            ))
            self._set_downloading(other, self.ENQUEUED_B)
            before = copy.deepcopy(other.get_request(self.request_id))

            stale = self._handoff(self.db, self.ENQUEUED_A)

            self.assertEqual(stale.outcome, "witness_mismatch")
            self.assertEqual(self.db.get_request(self.request_id), before)
            self.assertEqual(
                self.db.list_import_jobs(request_id=self.request_id),
                [],
            )
        finally:
            other.close()

    def test_overlapping_b_replacement_defeats_blocked_a_handoff(self):
        """A starts while B's replacement transaction owns the row lock."""
        other = self.db.__class__(TEST_DSN)
        started = threading.Event()
        result_box: list[AutomationHandoffResult] = []
        error_box: list[Exception] = []
        with tempfile.TemporaryDirectory() as tmpdir:
            source = os.path.join(tmpdir, "source")
            target = os.path.join(tmpdir, "processing", "albums", "target")
            os.makedirs(source)
            manifest = {
                "01.flac": b"one",
                "02.flac": b"two",
            }
            for name, payload in manifest.items():
                Path(source, name).write_bytes(payload)

            other.conn.autocommit = False
            try:
                with other.conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE album_requests
                        SET active_download_state = %s
                        WHERE id = %s
                        """,
                        (
                            psycopg2.extras.Json(json.loads(
                                self._state(self.ENQUEUED_B),
                            )),
                            self.request_id,
                        ),
                    )

                def run_stale_handoff() -> None:
                    try:
                        with patch.object(
                            self.db,
                            "_automation_handoff_before_request_lock",
                            side_effect=started.set,
                        ):
                            result_box.append(self._handoff(
                                self.db,
                                self.ENQUEUED_A,
                            ))
                    except (
                        AssertionError,
                        RuntimeError,
                        ValueError,
                        psycopg2.Error,
                    ) as exc:
                        error_box.append(exc)

                worker = threading.Thread(target=run_stale_handoff)
                worker.start()
                self.assertTrue(started.wait(timeout=5))
                self.assertTrue(worker.is_alive())
                other.conn.commit()
                worker.join(timeout=5)
                self.assertFalse(worker.is_alive())
            finally:
                if not other.conn.autocommit:
                    other.conn.rollback()
                    other.conn.autocommit = True
                other.close()

            self.assertEqual(error_box, [])
            self.assertEqual(len(result_box), 1)
            self.assertEqual(result_box[0].outcome, "witness_mismatch")
            request = self.db.get_request(self.request_id)
            assert request is not None
            self.assertEqual(request["status"], "downloading")
            self.assertIsNone(request["active_automation_import_job_id"])
            state = request["active_download_state"]
            assert isinstance(state, dict)
            self.assertEqual(state["enqueued_at"], self.ENQUEUED_B)
            self.assertNotIn("current_path", state)
            self.assertEqual(
                self.db.list_import_jobs(request_id=self.request_id),
                [],
            )
            self.assertEqual(self.db.get_log(limit=10), [])
            self.assertFalse(os.path.exists(target))
            self.assertEqual(
                {
                    path.name: path.read_bytes()
                    for path in Path(source).iterdir()
                },
                manifest,
            )

    def test_malformed_state_tags_match_fake(self):
        def real_snapshot(request_id: int) -> Mapping[str, object]:
            row = self.db._execute(
                """
                SELECT status,
                       active_download_state::text AS active_download_state,
                       active_automation_import_job_id,
                       updated_at
                FROM album_requests
                WHERE id = %s
                """,
                (request_id,),
            ).fetchone()
            assert row is not None
            return dict(row)

        cases: tuple[tuple[str, object, str], ...] = (
            ("sql_null", None, "missing_state"),
            ("json_null", None, "missing_state"),
            ("array", [], "missing_state"),
            ("scalar", 7, "missing_state"),
            (
                "missing_witness",
                {"filetype": "flac", "files": []},
                "witness_mismatch",
            ),
            (
                "exact",
                json.loads(self._state(self.ENQUEUED_A)),
                "committed",
            ),
        )
        for index, (name, raw_state, expected) in enumerate(cases, start=1):
            with self.subTest(case=name):
                real_id = self.db.add_request(
                    mb_release_id=f"handoff-shape-real-{index}",
                    artist_name="Shape",
                    album_title=name,
                    source="request",
                )
                fake = FakePipelineDB()
                fake_id = fake.add_request(
                    "Shape",
                    name,
                    "request",
                    mb_release_id=f"handoff-shape-fake-{index}",
                )
                if name == "sql_null":
                    self.db._execute(
                        """
                        UPDATE album_requests
                        SET status = 'downloading',
                            active_download_state = NULL
                        WHERE id = %s
                        """,
                        (real_id,),
                    )
                else:
                    self.db._execute(
                        """
                        UPDATE album_requests
                        SET status = 'downloading',
                            active_download_state = %s
                        WHERE id = %s
                        """,
                        (psycopg2.extras.Json(raw_state), real_id),
                    )
                fake_row = fake.request(fake_id)
                fake_row["status"] = "downloading"
                fake_row["active_download_state"] = copy.deepcopy(raw_state)
                real_before = copy.deepcopy(real_snapshot(real_id))
                fake_before = copy.deepcopy(fake.get_request(fake_id))

                real_result = self.db.handoff_automation_import(
                    request_id=real_id,
                    expected_enqueued_at=self.ENQUEUED_A,
                    canonical_path="/processing/albums/shape",
                    message="shape parity",
                )
                fake_result = fake.handoff_automation_import(
                    request_id=fake_id,
                    expected_enqueued_at=self.ENQUEUED_A,
                    canonical_path="/processing/albums/shape",
                    message="shape parity",
                )

                self.assertEqual(real_result.outcome, expected)
                self.assertEqual(fake_result.outcome, expected)
                if expected != "committed":
                    self.assertEqual(real_snapshot(real_id), real_before)
                    self.assertEqual(fake.get_request(fake_id), fake_before)
                    self.assertEqual(
                        self.db.list_import_jobs(request_id=real_id),
                        [],
                    )
                    self.assertEqual(
                        fake.list_import_jobs(request_id=fake_id),
                        [],
                    )

    def test_real_witness_guard_mutant_is_killed(self):
        other = self.db.__class__(TEST_DSN)
        try:
            self.assertTrue(other.reset_downloading_to_wanted(
                self.request_id,
                expected_status="downloading",
            ))
            self._set_downloading(other, self.ENQUEUED_B)
            before = copy.deepcopy(other.get_request(self.request_id))
            assert before is not None
            with patch.object(
                self.db,
                "_automation_handoff_enforce_witness",
                return_value=False,
            ):
                mutant = self._handoff(self.db, self.ENQUEUED_A)
            after = self.db.get_request(self.request_id)
            assert after is not None
            with self.assertRaisesRegex(AssertionError, "stale handoff"):
                from tests.test_download_incarnation_generated import (
                    assert_handoff_contract,
                )
                assert_handoff_contract(
                    exact=False,
                    before=before,
                    after=after,
                    job_count=len(self.db.list_import_jobs(
                        request_id=self.request_id,
                    )),
                )
            self.assertTrue(mutant.committed)
        finally:
            other.close()

    def test_faults_roll_back_but_sequence_ids_remain_burned(self):
        def fail_at(boundary: int):
            def fail(index: int, _label: str) -> None:
                if index == boundary:
                    raise RuntimeError("fault")
            return fail

        for boundary in (1, 2):
            with self.subTest(boundary=boundary):
                with patch.object(
                    self.db,
                    "_automation_handoff_write_boundary",
                    side_effect=fail_at(boundary),
                ), self.assertRaisesRegex(RuntimeError, "fault"):
                    self._handoff(self.db, self.ENQUEUED_A)

                request = self.db.get_request(self.request_id)
                assert request is not None
                self.assertEqual(request["status"], "downloading")
                self.assertIsNone(
                    request["active_automation_import_job_id"],
                )
                self.assertEqual(
                    self.db.list_import_jobs(request_id=self.request_id),
                    [],
                )

        committed = self._handoff(self.db, self.ENQUEUED_A)
        assert committed.job is not None
        self.assertGreater(committed.job.id, 2)


@requires_postgres
class TestProcessingOwnerGenericWriterGuards(unittest.TestCase):
    """Generic request/job writers fail closed on the private owner edge."""

    def setUp(self) -> None:
        self.db = make_db()

    def tearDown(self) -> None:
        self.db.close()

    def _owned_real(self, suffix: str):
        request_id = self.db.add_request(
            mb_release_id=f"processing-owner-real-{suffix}",
            artist_name="Owned",
            album_title=suffix,
            source="request",
        )
        enqueued_at = "2026-07-29T00:00:00+00:00"
        self.assertTrue(self.db.set_downloading(
            request_id,
            json.dumps({
                "filetype": "flac",
                "enqueued_at": enqueued_at,
                "files": [],
            }),
            expected_status="wanted",
        ))
        result = self.db.handoff_automation_import(
            request_id=request_id,
            expected_enqueued_at=enqueued_at,
            canonical_path="/processing",
            message="owned writer-guard fixture",
        )
        self.assertTrue(result.committed)
        assert result.job is not None
        return request_id, result.job

    @staticmethod
    def _owned_fake(suffix: str):
        db = FakePipelineDB()
        request_id = db.add_request(
            "Owned",
            suffix,
            "request",
            mb_release_id=f"processing-owner-fake-{suffix}",
        )
        enqueued_at = "2026-07-29T00:00:00+00:00"
        assert db.set_downloading(
            request_id,
            json.dumps({
                "filetype": "flac",
                "enqueued_at": enqueued_at,
                "files": [],
            }),
            expected_status="wanted",
        )
        result = db.handoff_automation_import(
            request_id=request_id,
            expected_enqueued_at=enqueued_at,
            canonical_path="/processing",
            message="owned writer-guard fixture",
        )
        assert result.committed and result.job is not None
        return db, request_id, result.job

    def _detach_real(
        self,
        request_id: int,
        job_id: int,
        *,
        job_status: str,
    ) -> None:
        with self.db._atomic():
            self.db._execute(
                "UPDATE import_jobs SET status = %s WHERE id = %s",
                (job_status, job_id),
            )
            self.db._execute("""
                UPDATE album_requests
                SET status = 'wanted',
                    active_automation_import_job_id = NULL
                WHERE id = %s
            """, (request_id,))
            self.db.conn.commit()

    @staticmethod
    def _detach_fake(
        db: FakePipelineDB,
        request_id: int,
        job_id: int,
        *,
        job_status: str,
    ) -> None:
        job = next(row for row in db._import_jobs if row["id"] == job_id)
        job["status"] = job_status
        request = db.request(request_id)
        request["status"] = "wanted"
        request["active_automation_import_job_id"] = None

    def test_request_metadata_status_compare_and_delete_reject_owner(self):
        request_id, _job = self._owned_real("request-guards")
        fake, fake_request_id, _fake_job = self._owned_fake("request-guards")
        real_before = self.db.get_request(request_id)
        fake_before = copy.deepcopy(fake.get_request(fake_request_id))

        self.assertFalse(
            self.db.update_request_fields(request_id, reasoning="late")
        )
        self.assertFalse(
            fake.update_request_fields(fake_request_id, reasoning="late")
        )
        self.assertFalse(
            self.db.update_status(
                request_id,
                "wanted",
                expected_status="processing",
            )
        )
        self.assertFalse(
            fake.update_status(
                fake_request_id,
                "wanted",
                expected_status="processing",
            )
        )
        self.assertFalse(
            self.db.compare_request_status(
                request_id,
                expected_status="processing",
            )
        )
        self.assertFalse(
            fake.compare_request_status(
                fake_request_id,
                expected_status="processing",
            )
        )
        self.assertFalse(self.db.delete_request(request_id))
        self.assertFalse(fake.delete_request(fake_request_id))

        self.assertEqual(self.db.get_request(request_id), real_before)
        self.assertEqual(fake.get_request(fake_request_id), fake_before)

    def test_supersede_rejects_owner_at_its_atomic_row_lock(self):
        request_id, _job = self._owned_real("supersede-guard")
        fake, fake_request_id, _fake_job = self._owned_fake(
            "supersede-guard"
        )
        real_before = self.db.get_request(request_id)
        fake_before = fake.get_request(fake_request_id)
        with self.assertRaises(SupersedeRaceError):
            self.db.supersede_request_mbid(
                request_id,
                new_mb_release_id="processing-owner-real-supersede-new",
                new_mb_release_group_id=None,
                new_mb_artist_id=None,
                new_artist_name="New",
                new_album_title="Pressing",
                new_year=None,
                new_country=None,
                new_tracks=[],
            )
        with self.assertRaises(SupersedeRaceError):
            fake.supersede_request_mbid(
                fake_request_id,
                new_mb_release_id="processing-owner-fake-supersede-new",
                new_mb_release_group_id=None,
                new_mb_artist_id=None,
                new_artist_name="New",
                new_album_title="Pressing",
                new_year=None,
                new_country=None,
                new_tracks=[],
            )

        self.assertEqual(self.db.get_request(request_id), real_before)
        self.assertEqual(fake.get_request(fake_request_id), fake_before)

    def test_rescue_and_reset_reject_owner_with_real_fake_parity(self):
        cases = (
            (
                "rescue",
                lambda db, request_id: db.mark_imported_with_rescue(
                    request_id,
                    expected_status="processing",
                    beets_distance=0.05,
                ),
            ),
            (
                "reset",
                lambda db, request_id: db.reset_to_wanted(
                    request_id,
                    expected_status="processing",
                    min_bitrate=320,
                ),
            ),
        )
        for suffix, writer in cases:
            with self.subTest(writer=suffix):
                request_id, _job = self._owned_real(suffix)
                fake, fake_request_id, _fake_job = self._owned_fake(suffix)
                real_before = self.db.get_request(request_id)
                fake_before = copy.deepcopy(fake.get_request(fake_request_id))

                self.assertFalse(writer(self.db, request_id))
                self.assertFalse(writer(fake, fake_request_id))
                self.assertEqual(self.db.get_request(request_id), real_before)
                self.assertEqual(fake.get_request(fake_request_id), fake_before)

    def test_generic_job_terminal_writers_reject_all_automation_rows(self):
        cases = (
            (
                "completed",
                lambda db, job_id: db.mark_import_job_completed(
                    job_id,
                    result={"unexpected": True},
                ),
            ),
            (
                "failed",
                lambda db, job_id: db.mark_import_job_failed(
                    job_id,
                    error="unexpected",
                ),
            ),
            (
                "preview-failed",
                lambda db, job_id: db.mark_import_job_preview_failed(
                    job_id,
                    preview_status="measurement_failed",
                    error="unexpected",
                ),
            ),
        )
        for attachment in ("attached", "unattached"):
            for suffix, writer in cases:
                case = f"{attachment}-{suffix}"
                with self.subTest(attachment=attachment, writer=suffix):
                    request_id, job = self._owned_real(case)
                    fake, fake_request_id, fake_job = self._owned_fake(case)
                    if attachment == "unattached":
                        self._detach_real(
                            request_id,
                            job.id,
                            job_status="failed",
                        )
                        self._detach_fake(
                            fake,
                            fake_request_id,
                            fake_job.id,
                            job_status="failed",
                        )
                    real_before = self.db.get_import_job(job.id)
                    fake_before = fake.get_import_job(fake_job.id)
                    real_request_before = self.db.get_request(request_id)
                    fake_request_before = copy.deepcopy(
                        fake.get_request(fake_request_id)
                    )

                    self.assertIsNone(writer(self.db, job.id))
                    self.assertIsNone(writer(fake, fake_job.id))

                    self.assertEqual(self.db.get_import_job(job.id), real_before)
                    self.assertEqual(
                        fake.get_import_job(fake_job.id),
                        fake_before,
                    )
                    self.assertEqual(
                        self.db.get_request(request_id),
                        real_request_before,
                    )
                    self.assertEqual(
                        fake.get_request(fake_request_id),
                        fake_request_before,
                    )

    def test_merge_result_rejects_nonterminal_and_attached_automation(self):
        request_id, job = self._owned_real("merge-guard")
        fake, fake_request_id, fake_job = self._owned_fake("merge-guard")

        self.assertIsNone(
            self.db.merge_import_job_result(job.id, {"unexpected": True})
        )
        self.assertIsNone(
            fake.merge_import_job_result(fake_job.id, {"unexpected": True})
        )

        with self.db._atomic():
            self.db._execute(
                "UPDATE import_jobs SET status = 'completed' WHERE id = %s",
                (job.id,),
            )
            real_before = self.db.get_import_job(job.id)
            self.assertIsNone(
                self.db.merge_import_job_result(job.id, {"unexpected": True})
            )
            self.assertEqual(self.db.get_import_job(job.id), real_before)
            self.db.conn.rollback()

        fake_row = next(
            row for row in fake._import_jobs if row["id"] == fake_job.id
        )
        fake_row["status"] = "completed"
        fake_before = fake.get_import_job(fake_job.id)
        self.assertIsNone(
            fake.merge_import_job_result(fake_job.id, {"unexpected": True})
        )
        self.assertEqual(fake.get_import_job(fake_job.id), fake_before)
        fake_request = fake.get_request(fake_request_id)
        real_request = self.db.get_request(request_id)
        self.assertIsNotNone(fake_request)
        self.assertIsNotNone(real_request)
        assert fake_request is not None
        assert real_request is not None
        self.assertEqual(
            fake_request["status"],
            "processing",
        )
        self.assertEqual(real_request["status"], "processing")

    def test_merge_result_allows_unattached_terminal_audit_enrichment(self):
        request_id, job = self._owned_real("merge-audit")
        fake, fake_request_id, fake_job = self._owned_fake("merge-audit")

        self._detach_real(
            request_id,
            job.id,
            job_status="completed",
        )
        self._detach_fake(
            fake,
            fake_request_id,
            fake_job.id,
            job_status="completed",
        )

        real_result = self.db.merge_import_job_result(
            job.id,
            {"scan": "confirmed"},
        )
        fake_result = fake.merge_import_job_result(
            fake_job.id,
            {"scan": "confirmed"},
        )

        self.assertIsNotNone(real_result)
        self.assertIsNotNone(fake_result)
        assert real_result is not None
        assert fake_result is not None
        self.assertEqual(real_result.result, {"scan": "confirmed"})
        self.assertEqual(fake_result.result, {"scan": "confirmed"})


    def test_generic_status_writer_cannot_enter_processing(self):
        request_id = self.db.add_request(
            mb_release_id="processing-owner-enter-real",
            artist_name="Owned",
            album_title="Enter",
            source="request",
        )
        fake = FakePipelineDB()
        fake_request_id = fake.add_request(
            "Owned",
            "Enter",
            "request",
            mb_release_id="processing-owner-enter-fake",
        )

        with self.assertRaises(ValueError):
            self.db.update_status(request_id, "processing")
        with self.assertRaises(ValueError):
            fake.update_status(fake_request_id, "processing")
        real_request = self.db.get_request(request_id)
        fake_request = fake.get_request(fake_request_id)
        self.assertIsNotNone(real_request)
        self.assertIsNotNone(fake_request)
        assert real_request is not None
        assert fake_request is not None
        self.assertEqual(real_request["status"], "wanted")
        self.assertEqual(fake_request["status"], "wanted")


@requires_postgres
class TestRequeueImportJobForPreview(unittest.TestCase):
    """U2: importer can requeue an actively-running job back to preview's lane."""

    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="requeue-preview-mbid",
            artist_name="Requeue",
            album_title="Preview",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def _enqueue_claimed_job(self):
        """Enqueue a manual job, advance it through preview, and have the importer claim it."""
        from lib.import_queue import IMPORT_JOB_FORCE

        job = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key="manual:requeue-for-preview",
            payload={"download_log_id": 1, "failed_path": "/tmp/manual"},
        )
        self.db.mark_import_job_preview_importable(
            job.id,
            preview_result={"verdict": "would_import"},
            message="ready",
        )
        claimed = claim_next_import_job(self.db, worker_id="importer-1")
        assert claimed is not None
        self.assertEqual(claimed.status, "running")
        return claimed

    def test_flips_running_job_back_to_queued_waiting(self):
        claimed = self._enqueue_claimed_job()

        updated = self.db.requeue_import_job_for_preview(
            claimed.id,
            reason="candidate evidence missing",
        )

        assert updated is not None
        self.assertEqual(updated.status, "queued")
        self.assertEqual(updated.preview_status, "waiting")
        self.assertIsNone(updated.worker_id)
        self.assertIsNone(updated.started_at)
        self.assertIsNone(updated.heartbeat_at)
        self.assertIsNone(updated.preview_message)
        self.assertIsNone(updated.preview_error)
        self.assertEqual(updated.message, "candidate evidence missing")

    def test_preserves_attempt_counters(self):
        claimed = self._enqueue_claimed_job()
        prior_attempts = claimed.attempts
        prior_preview_attempts = claimed.preview_attempts
        self.assertEqual(prior_attempts, 1)

        updated = self.db.requeue_import_job_for_preview(
            claimed.id,
            reason="stale snapshot",
        )

        assert updated is not None
        self.assertEqual(updated.attempts, prior_attempts)
        self.assertEqual(updated.preview_attempts, prior_preview_attempts)

    def test_requeued_row_waits_for_growing_preview_backoff(self):
        claimed = self._enqueue_claimed_job()
        self.db.requeue_import_job_for_preview(
            claimed.id,
            reason="incomplete",
        )

        self.assertIsNone(claim_next_import_preview_job(
            self.db, worker_id="preview-too-soon"))
        self.db._execute("""
            UPDATE import_jobs
            SET updated_at = NOW() - INTERVAL '61 seconds'
            WHERE id = %s
        """, (claimed.id,))
        preview = claim_next_import_preview_job(self.db, worker_id="preview-1")
        assert preview is not None
        self.assertEqual(preview.id, claimed.id)
        self.assertEqual(preview.preview_status, "running")
        # Preview's claim clears its own diagnostics.
        self.assertIsNone(preview.preview_message)

    def test_requeued_row_caps_preview_backoff_at_thirty_minutes(self):
        claimed = self._enqueue_claimed_job()
        self.db.requeue_import_job_for_preview(
            claimed.id,
            reason="incident-scale retained attempts",
        )
        self.db._execute("""
            UPDATE import_jobs
            SET attempts = 2454, updated_at = NOW() - INTERVAL '1799 seconds'
            WHERE id = %s
        """, (claimed.id,))

        self.assertIsNone(claim_next_import_preview_job(
            self.db, worker_id="preview-1799"))
        self.db._execute("""
            UPDATE import_jobs
            SET updated_at = NOW() - INTERVAL '1801 seconds'
            WHERE id = %s
        """, (claimed.id,))
        preview = claim_next_import_preview_job(self.db, worker_id="preview-1801")
        assert preview is not None
        self.assertEqual(preview.id, claimed.id)

    def test_idempotent_when_already_requeued(self):
        claimed = self._enqueue_claimed_job()
        first = self.db.requeue_import_job_for_preview(
            claimed.id,
            reason="first requeue",
        )
        # Second call should be a no-op (status no longer running).
        second = self.db.requeue_import_job_for_preview(
            claimed.id,
            reason="second requeue",
        )

        assert first is not None
        self.assertIsNone(second)
        # Message from first requeue stays.
        row = self.db._execute(
            "SELECT message FROM import_jobs WHERE id = %s",
            (claimed.id,),
        ).fetchone()
        assert row is not None
        self.assertEqual(row["message"], "first requeue")

    def test_does_not_touch_unrelated_jobs(self):
        claimed = self._enqueue_claimed_job()
        from lib.import_queue import IMPORT_JOB_FORCE

        other = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key="manual:unrelated",
            payload={"download_log_id": 1, "failed_path": "/tmp/other"},
        )

        self.db.requeue_import_job_for_preview(claimed.id, reason="x")

        other_row = self.db._execute(
            "SELECT status, preview_status FROM import_jobs WHERE id = %s",
            (other.id,),
        ).fetchone()
        assert other_row is not None
        self.assertEqual(other_row["status"], "queued")
        self.assertEqual(other_row["preview_status"], "waiting")


@requires_postgres
class TestImportPreviewScanBackoffParity(unittest.TestCase):
    """The preview scan's SQL backoff IS ``import_preview_requeue_delay``.

    One policy, three implementations: the SQL window inside
    ``peek_import_preview_job_candidates``, the Python function the
    dispatch requeue message and the exhaustion check read, and
    ``FakePipelineDB``'s scan, which calls that same Python function. The
    fake is therefore faithful to production only as far as the SQL and
    the Python agree, and nothing compared them — the two hand-written
    pins next door fix attempts at 1 and at 2454, which is the first
    doubling and the cap, leaving every doubling between them free. Base
    2 could become base 3 in the SQL and nothing in the six-module
    selection for that file noticed — 863 tests today, of which the four
    failures are all this class's own subtests (#1313 residual 1314-3).

    Parity is the whole claim, and it is narrower than the policy: both
    sides read ``IMPORT_PREVIEW_REQUEUE_INITIAL_DELAY`` and its
    neighbours, so a change to a shared constant moves the SQL and the
    expectation together and is invisible here by construction (measured:
    collapsing ``IMPORT_PREVIEW_REQUEUE_MAX_EXPONENT`` to 0 flattens the
    curve to 60s forever and passes). The policy's own shape is held by
    ``tests/test_import_queue.py::TestImportPreviewRequeuePolicy``, which
    kills that one.
    """

    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="preview-backoff-parity-mbid",
            artist_name="Backoff",
            album_title="Parity",
            source="request",
        )
        self.job = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key="manual:preview-backoff-parity",
            payload={"download_log_id": 1, "failed_path": "/tmp/parity"},
        )

    def tearDown(self):
        self.db.close()

    #: Seconds either side of the due moment. The "already due" probe is
    #: safe at any positive margin, since ``NOW()`` only advances; the
    #: "not due yet" probe is the one this number decides, and it is a
    #: two-sided budget. Too small and the test flakes when more than
    #: MARGIN_SECONDS passes between the UPDATE that ages the row and the
    #: SELECT that reads it. Too large and it stops noticing a mutant that
    #: SHORTENS the delay by less than the margin. 5s buys a wide flake
    #: margin for two statements on one connection while still catching a
    #: constant knocked down by a tenth.
    MARGIN_SECONDS = 5

    def _age_row(self, *, attempts: int, seconds_ago: float) -> None:
        """Write the two columns the scan's backoff window reads.

        A real requeue is one producer of this row shape and cannot reach
        most of the curve; the scan sees only ``attempts`` and
        ``updated_at`` on a queued/waiting row, which is what this writes.
        """
        self.db._execute("""
            UPDATE import_jobs
            SET attempts = %s, updated_at = NOW() - make_interval(secs => %s)
            WHERE id = %s
        """, (attempts, seconds_ago, self.job.id))

    def _on_offer(self) -> bool:
        return any(
            candidate.id == self.job.id
            for candidate in self.db.peek_import_preview_job_candidates(limit=10)
        )

    def test_scan_admits_an_attempted_row_exactly_when_the_policy_says_it_is_due(
        self,
    ) -> None:
        # Derived from the policy, not hand-listed: one attempt count per
        # doubling plus two past the cap, so raising the exponent grows
        # this table rather than leaving its new steps unpatrolled.
        attempt_counts = [*range(1, IMPORT_PREVIEW_REQUEUE_MAX_EXPONENT + 3), 2454]
        distinct_delays = {
            import_preview_requeue_delay(attempts) for attempts in attempt_counts
        }
        self.assertEqual(
            len(distinct_delays), IMPORT_PREVIEW_REQUEUE_MAX_EXPONENT + 1,
            "the table must reach every delay the policy can return",
        )

        for attempts in attempt_counts:
            due_after = import_preview_requeue_delay(attempts).total_seconds()
            with self.subTest(attempts=attempts, due_after=due_after):
                self._age_row(
                    attempts=attempts,
                    seconds_ago=due_after - self.MARGIN_SECONDS,
                )
                self.assertFalse(
                    self._on_offer(),
                    f"scan offered a row {self.MARGIN_SECONDS}s before its "
                    f"{due_after}s backoff elapsed",
                )
                self._age_row(
                    attempts=attempts,
                    seconds_ago=due_after + self.MARGIN_SECONDS,
                )
                self.assertTrue(
                    self._on_offer(),
                    f"scan withheld a row {self.MARGIN_SECONDS}s after its "
                    f"{due_after}s backoff elapsed",
                )

    def test_a_never_attempted_row_skips_the_backoff_window_entirely(self) -> None:
        """``attempts = 0`` short-circuits: a fresh row is due immediately."""
        self._age_row(attempts=0, seconds_ago=0)
        self.assertTrue(self._on_offer())


@requires_postgres
class TestRequeueRunningImportPreviewJobs(unittest.TestCase):
    """Startup self-heal for the async preview worker.

    Mirrors the importer's running-job recovery — when the
    preview worker process restarts, it must immediately requeue every
    job in ``preview_status='running'`` regardless of heartbeat age,
    because by definition no preview worker is currently processing
    them (systemd runs a single instance). Before this method existed,
    crash recovery waited on the 15-minute stale-age window in
    ``requeue_stale_import_preview_jobs`` and operators saw preview
    jobs sit stuck for the full window.
    """

    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="requeue-running-preview-mbid",
            artist_name="Requeue",
            album_title="Preview Running",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def _enqueue_running_preview_job(self) -> int:
        from lib.import_queue import IMPORT_JOB_FORCE

        job = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key="manual:requeue-running-preview",
            payload={"download_log_id": 1, "failed_path": "/tmp/manual"},
        )
        claimed = claim_next_import_preview_job(self.db, worker_id="preview-old")
        assert claimed is not None
        self.assertEqual(claimed.preview_status, "running")
        self.assertIsNotNone(claimed.preview_heartbeat_at)
        return job.id

    def test_requeues_fresh_running_job_immediately(self):
        """The bug: a job claimed seconds ago should be requeued on
        startup, not wait 15 minutes for the stale-recovery sweep.
        """
        job_id = self._enqueue_running_preview_job()

        requeued = self.db.requeue_running_import_preview_jobs(
            message="Preview worker restarted while job was running; retry queued",
        )

        self.assertEqual(len(requeued), 1)
        self.assertEqual(requeued[0].id, job_id)
        self.assertEqual(requeued[0].preview_status, "waiting")
        self.assertIsNone(requeued[0].preview_worker_id)
        self.assertIsNone(requeued[0].preview_started_at)
        self.assertIsNone(requeued[0].preview_heartbeat_at)
        self.assertIsNone(requeued[0].preview_error)
        self.assertIn("restarted", (requeued[0].preview_message or ""))

    def test_requeued_job_is_immediately_claimable_by_preview(self):
        job_id = self._enqueue_running_preview_job()
        self.db.requeue_running_import_preview_jobs(
            message="restart",
        )
        reclaim = claim_next_import_preview_job(self.db, worker_id="preview-new")
        assert reclaim is not None
        self.assertEqual(reclaim.id, job_id)
        self.assertEqual(reclaim.preview_status, "running")
        self.assertEqual(reclaim.preview_worker_id, "preview-new")

    def test_does_not_touch_waiting_or_already_imported_jobs(self):
        from lib.import_queue import IMPORT_JOB_FORCE

        # Claim-and-leave-running first so the second enqueue stays waiting.
        running_id = self._enqueue_running_preview_job()
        waiting = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key="manual:waiting",
            payload={"download_log_id": 1, "failed_path": "/tmp/waiting"},
        )

        result = self.db.requeue_running_import_preview_jobs(message="restart")

        self.assertEqual({j.id for j in result}, {running_id})
        waiting_row = self.db._execute(
            "SELECT preview_status FROM import_jobs WHERE id = %s",
            (waiting.id,),
        ).fetchone()
        assert waiting_row is not None
        self.assertEqual(waiting_row["preview_status"], "waiting")


@requires_postgres
class TestUpdateStatus(unittest.TestCase):
    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="status-uuid",
            artist_name="A",
            album_title="B",
            source="redownload",
        )

    def tearDown(self):
        self.db.close()

    def test_status_transitions(self):
        for s in ["wanted", "imported", "unsearchable"]:
            self.db.update_status(self.req_id, s)
            req = self.db.get_request(self.req_id)
            assert req is not None
            self.assertEqual(req["status"], s)

    def test_update_status_with_extra_fields(self):
        self.db.update_status(self.req_id, "imported",
                              beets_distance=0.05)
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["status"], "imported")
        distance = req["beets_distance"]
        assert distance is not None
        self.assertAlmostEqual(distance, 0.05)

    def test_removed_imported_path_field_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "no longer exist"):
            self.db.update_status(
                self.req_id,
                "imported",
                imported_path="/Beets/A/2020 - B",
            )

    def test_update_status_metadata_rejects_lifecycle_and_malformed_fields(self):
        before = self.db.get_request(self.req_id)
        for fields in (
            {"active_download_state": "{}"},
            {"reasoning, status": "smuggled"},
        ):
            with self.subTest(fields=fields):
                with self.assertRaises(ValueError):
                    self.db.update_status(
                        self.req_id,
                        "imported",
                        **fields,
                    )
                self.assertEqual(self.db.get_request(self.req_id), before)


@requires_postgres
class TestGetWanted(unittest.TestCase):
    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_get_wanted_returns_only_wanted(self):
        id1 = self.db.add_request(mb_release_id="w1", artist_name="A", album_title="B", source="request")
        id2 = self.db.add_request(mb_release_id="w2", artist_name="C", album_title="D", source="request")
        id3 = self.db.add_request(mb_release_id="w3", artist_name="E", album_title="F", source="request")
        self.db.update_status(id2, "imported")

        wanted = self.db.get_wanted()
        wanted_ids = [w["id"] for w in wanted]
        self.assertIn(id1, wanted_ids)
        self.assertNotIn(id2, wanted_ids)
        self.assertIn(id3, wanted_ids)

    def test_get_wanted_respects_retry_backoff(self):
        id1 = self.db.add_request(mb_release_id="r1", artist_name="A", album_title="B", source="request")
        future = datetime.now(UTC) + timedelta(hours=1)
        self.db._execute(
            "UPDATE album_requests SET next_retry_after = %s WHERE id = %s",
            (future, id1),
        )
        self.db.conn.commit()

        wanted = self.db.get_wanted()
        self.assertEqual(len(wanted), 0)

    def test_get_wanted_with_limit(self):
        for i in range(5):
            self.db.add_request(mb_release_id=f"lim-{i}", artist_name="A", album_title=f"B{i}", source="request")
        wanted = self.db.get_wanted(limit=3)
        self.assertEqual(len(wanted), 3)


@requires_postgres
class TestGetByStatus(unittest.TestCase):
    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_get_by_status(self):
        id1 = self.db.add_request(mb_release_id="s1", artist_name="A", album_title="B", source="request")
        self.db.add_request(mb_release_id="s2", artist_name="C", album_title="D", source="request")
        self.db.update_status(id1, "imported")

        imported = self.db.get_by_status("imported")
        self.assertEqual(len(imported), 1)
        self.assertEqual(imported[0]["id"], id1)

    def test_count_by_status(self):
        self.db.add_request(mb_release_id="c1", artist_name="A", album_title="B", source="request")
        self.db.add_request(mb_release_id="c2", artist_name="C", album_title="D", source="request")
        id3 = self.db.add_request(mb_release_id="c3", artist_name="E", album_title="F", source="redownload")
        self.db.update_status(id3, "imported")

        counts = self.db.count_by_status()
        self.assertEqual(counts["wanted"], 2)
        self.assertEqual(counts["imported"], 1)


@requires_postgres
class TestTrackManagement(unittest.TestCase):
    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="track-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_empty_request_field_update_is_a_read_only_cas_truth_table(self):
        """Empty/control-only metadata writes still validate lifecycle."""
        from lib import pipeline_db
        from lib.quality import SpectralMeasurement

        replaced_id = self.db.add_request(
            mb_release_id="empty-cas-replaced-old",
            artist_name="A",
            album_title="Old",
            source="request",
        )
        self.db.supersede_request_mbid(
            replaced_id,
            new_mb_release_id="empty-cas-replaced-new",
            new_mb_release_group_id=None,
            new_mb_artist_id=None,
            new_artist_name="A",
            new_album_title="New",
            new_year=None,
            new_country=None,
            new_tracks=[],
        )
        active_before = self.db.get_request(self.req_id)
        replaced_before = self.db.get_request(replaced_id)
        assert active_before is not None
        assert replaced_before is not None

        self.assertTrue(self.db.update_request_fields(self.req_id))
        self.assertTrue(self.db.update_request_fields(
            self.req_id, expected_status="wanted",
        ))
        self.assertFalse(self.db.update_request_fields(
            self.req_id, expected_status="unsearchable",
        ))
        self.assertFalse(self.db.update_request_fields(replaced_id))
        self.assertFalse(self.db.update_request_fields(
            replaced_id, expected_status="replaced",
        ))
        self.assertFalse(self.db.update_request_fields(999_999))
        self.assertFalse(self.db.update_request_fields(
            999_999, expected_status="wanted",
        ))
        self.assertFalse(self.db.update_request_fields(
            replaced_id,
            **pipeline_db.RequestSpectralStateUpdate(
                current=SpectralMeasurement(grade="genuine", bitrate_kbps=320),
            ).as_update_fields(),
        ))

        self.assertEqual(self.db.get_request(self.req_id), active_before)
        self.assertEqual(self.db.get_request(replaced_id), replaced_before)

    def test_metadata_update_rejects_every_reserved_field(self):
        before = self.db.get_request(self.req_id)
        assert before is not None

        for field in sorted(REQUEST_METADATA_RESERVED_FIELDS):
            with self.subTest(field=field):
                with self.assertRaisesRegex(
                    ValueError,
                    "reserved lifecycle/identity fields",
                ):
                    self.db.update_request_fields(
                        self.req_id,
                        **{
                            field: (
                                "replaced" if field == "status" else "smuggled"
                            ),
                        },
                    )
                self.assertEqual(self.db.get_request(self.req_id), before)

        with self.assertRaises(ValueError):
            self.db.update_request_fields(self.req_id, status="unsearchable")
        self.assertEqual(self.db.get_request(self.req_id), before)

    def test_metadata_update_rejects_malformed_identifier(self):
        before = self.db.get_request(self.req_id)
        with self.assertRaisesRegex(ValueError, "lowercase SQL identifiers"):
            self.db.update_request_fields(
                self.req_id,
                **{"reasoning, status": "smuggled"},
            )
        self.assertEqual(self.db.get_request(self.req_id), before)

    def test_set_tracks_round_trip_preserves_every_field(self):
        """Rule A: every track field survives the real PostgreSQL write."""
        tracks = [
            {
                "disc_number": 1,
                "track_number": 1,
                "title": "Intro",
                "length_seconds": 120,
                "track_artist": "Opening Artist",
            },
            {
                "disc_number": 1,
                "track_number": 2,
                "title": "Song",
                "length_seconds": 240,
                "track_artist": None,
            },
            {
                "disc_number": 2,
                "track_number": 1,
                "title": "Outro",
                "length_seconds": 180,
                "track_artist": "Closing Artist",
            },
        ]
        self.db.set_tracks(self.req_id, tracks)

        result = self.db.get_tracks(self.req_id)
        self.assertEqual(len(result), 3)
        for expected, actual in zip(tracks, result, strict=True):
            for field, value in expected.items():
                self.assertEqual(
                    actual[field],
                    value,
                    f"set_tracks field {field!r} was dropped at the PG boundary",
                )

    def test_tied_track_keys_read_back_in_insertion_order(self):
        """Rows sharing a (disc, track) key — the unparseable-position
        (1, 0) sentinel — read back in insertion order via the id
        tiebreak, and update_track_artists maps positionally onto that
        same order (issue #1263 item 1)."""
        tracks = [
            {"disc_number": 1, "track_number": 0, "title": "First Tied",
             "length_seconds": 100, "track_artist": None},
            {"disc_number": 1, "track_number": 0, "title": "Second Tied",
             "length_seconds": 110, "track_artist": None},
            {"disc_number": 1, "track_number": 1, "title": "Numbered",
             "length_seconds": 120, "track_artist": None},
            {"disc_number": 1, "track_number": 0, "title": "Third Tied",
             "length_seconds": 130, "track_artist": None},
        ]
        self.db.set_tracks(self.req_id, tracks)

        titles = [t["title"] for t in self.db.get_tracks(self.req_id)]
        self.assertEqual(
            titles, ["First Tied", "Second Tied", "Third Tied", "Numbered"],
        )

        self.db.update_track_artists(
            self.req_id, ["Artist A", "Artist B", "Artist C", "Artist D"],
        )
        by_title = {
            t["title"]: t["track_artist"]
            for t in self.db.get_tracks(self.req_id)
        }
        self.assertEqual(by_title, {
            "First Tied": "Artist A",
            "Second Tied": "Artist B",
            "Third Tied": "Artist C",
            "Numbered": "Artist D",
        })

    def test_set_tracks_rolls_back_later_constraint_failure(self):
        """A failed replacement leaves the complete prior tracklist intact."""
        old_tracks = [
            {
                "disc_number": 1,
                "track_number": 1,
                "title": "Old One",
                "length_seconds": 111,
                "track_artist": "Old Artist",
            },
            {
                "disc_number": 1,
                "track_number": 2,
                "title": "Old Two",
                "length_seconds": 222,
                "track_artist": None,
            },
        ]
        self.db.set_tracks(self.req_id, old_tracks)

        with self.assertRaises(psycopg2.IntegrityError):
            self.db.set_tracks(self.req_id, [
                {
                    "disc_number": 1,
                    "track_number": 1,
                    "title": "New One",
                    "length_seconds": 333,
                    "track_artist": "New Artist",
                },
                {
                    "disc_number": 1,
                    "track_number": 2,
                    "title": None,
                    "length_seconds": 444,
                    "track_artist": "Broken Later Row",
                },
            ])

        self.assertEqual(self.db.get_tracks(self.req_id), old_tracks)

    def test_set_tracks_replaces_existing(self):
        self.db.set_tracks(self.req_id, [
            {"disc_number": 1, "track_number": 1, "title": "Old", "length_seconds": 100},
        ])
        self.db.set_tracks(self.req_id, [
            {"disc_number": 1, "track_number": 1, "title": "New", "length_seconds": 200},
        ])
        result = self.db.get_tracks(self.req_id)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["title"], "New")

    def test_child_metadata_writers_reject_missing_parent(self):
        self.assertFalse(self.db.update_track_artists(
            999_999,
            ["Orphan Artist"],
        ))
        self.assertFalse(self.db.record_field_resolution(
            999_999,
            "track_artist",
            "resolved",
            None,
        ))

    def test_resolver_tracks_racing_replace_leave_ancestor_frozen(self):
        """Real PG barrier: Replace wins while resolver is in flight."""
        from lib.config import CratediggerConfig
        from lib.pipeline_db import PipelineDB
        from lib.search_plan_service import (
            RESULT_REQUEST_REPLACED,
            SearchPlanService,
        )

        entered = threading.Event()
        release = threading.Event()
        results: list[Any] = []
        errors: list[BaseException] = []

        class BarrierResolver:
            def resolve_tracks(
                self,
                *,
                release_id: str,
                request_id: int,
            ) -> list[dict[str, Any]]:
                entered.set()
                if not release.wait(timeout=10):
                    raise TimeoutError("replace barrier was not released")
                return [{
                    "disc_number": 1,
                    "track_number": 1,
                    "title": "Late resolver result",
                    "length_seconds": 180,
                }]

        def generate() -> None:
            try:
                results.append(SearchPlanService(
                    self.db,
                    CratediggerConfig(),
                    resolver=BarrierResolver(),
                ).generate_for_request(self.req_id, regenerate=False))
            except BaseException as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
                errors.append(exc)

        worker = threading.Thread(target=generate)
        worker.start()
        self.assertTrue(entered.wait(timeout=10))

        replacing_db = PipelineDB(TEST_DSN)
        try:
            replacing_db.supersede_request_mbid(
                self.req_id,
                new_mb_release_id="track-resolver-race-new",
                new_mb_release_group_id=None,
                new_mb_artist_id=None,
                new_artist_name="A",
                new_album_title="B (correct pressing)",
                new_year=None,
                new_country=None,
                new_tracks=[],
            )
            frozen_row = replacing_db.get_request(self.req_id)
            frozen_tracks = replacing_db.get_tracks(self.req_id)
        finally:
            release.set()
            worker.join(timeout=10)
            replacing_db.close()

        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].outcome, RESULT_REQUEST_REPLACED)
        self.assertEqual(self.db.get_request(self.req_id), frozen_row)
        self.assertEqual(self.db.get_tracks(self.req_id), frozen_tracks)

    def test_field_resolver_racing_replace_cannot_rewrite_child_rows(self):
        """Real PG barrier: late scalar/artist resolution loses to Replace."""
        from lib.field_resolver_service import (
            ResolveAllResult,
            apply_resolve_all_result,
        )
        from lib.pipeline_db import PipelineDB

        self.db.set_tracks(self.req_id, [{
            "disc_number": 1,
            "track_number": 1,
            "title": "Track",
            "track_artist": None,
        }])
        entered = threading.Event()
        release = threading.Event()
        applied: list[bool] = []
        errors: list[BaseException] = []

        def resolve_and_apply() -> None:
            try:
                entered.set()
                if not release.wait(timeout=10):
                    raise TimeoutError("field resolver barrier was not released")
                applied.append(apply_resolve_all_result(
                    self.db,
                    self.req_id,
                    ResolveAllResult(
                        release_group_year=1999,
                        track_artists=["Late Artist"],
                        is_va_compilation=False,
                    ),
                    expected_status="wanted",
                ))
            except BaseException as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
                errors.append(exc)

        worker = threading.Thread(target=resolve_and_apply)
        worker.start()
        self.assertTrue(entered.wait(timeout=10))

        replacing_db = PipelineDB(TEST_DSN)
        try:
            replacing_db.supersede_request_mbid(
                self.req_id,
                new_mb_release_id="field-resolver-race-new",
                new_mb_release_group_id=None,
                new_mb_artist_id=None,
                new_artist_name="A",
                new_album_title="B (correct pressing)",
                new_year=None,
                new_country=None,
                new_tracks=[],
            )
            frozen_row = replacing_db.get_request(self.req_id)
            frozen_tracks = replacing_db.get_tracks(self.req_id)
        finally:
            release.set()
            worker.join(timeout=10)
            replacing_db.close()

        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(applied, [False])
        self.assertEqual(self.db.get_request(self.req_id), frozen_row)
        self.assertEqual(self.db.get_tracks(self.req_id), frozen_tracks)
        self.assertIsNone(self.db.get_tracks(self.req_id)[0]["track_artist"])
        self.assertFalse(self.db.record_field_resolution(
            self.req_id,
            "track_artist",
            "resolved",
            None,
        ))
        self.assertIsNone(
            self.db.get_field_resolution(self.req_id, "track_artist"),
        )

    def test_metadata_compare_and_set_loses_to_replace(self):
        """Real PG barrier: a stale set-intent snapshot reports no apply."""
        from lib.pipeline_db import PipelineDB

        ready = threading.Event()
        release = threading.Event()
        applied: list[bool] = []
        errors: list[BaseException] = []

        def write_metadata() -> None:
            try:
                ready.set()
                if not release.wait(timeout=10):
                    raise TimeoutError("metadata barrier was not released")
                applied.append(self.db.update_request_fields(
                    self.req_id,
                    expected_status="wanted",
                    target_format="lossless",
                ))
            except BaseException as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
                errors.append(exc)

        worker = threading.Thread(target=write_metadata)
        worker.start()
        self.assertTrue(ready.wait(timeout=10))

        replacing_db = PipelineDB(TEST_DSN)
        try:
            replacing_db.supersede_request_mbid(
                self.req_id,
                new_mb_release_id="metadata-race-new",
                new_mb_release_group_id=None,
                new_mb_artist_id=None,
                new_artist_name="A",
                new_album_title="B (correct pressing)",
                new_year=None,
                new_country=None,
                new_tracks=[],
            )
            frozen_row = replacing_db.get_request(self.req_id)
        finally:
            release.set()
            worker.join(timeout=10)
            replacing_db.close()

        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(applied, [False])
        self.assertEqual(self.db.get_request(self.req_id), frozen_row)

    def test_field_resolver_snapshot_loses_to_unsearchable_transition(self):
        """Real PG barrier: stale resolver output cannot touch parent/child."""
        from lib.field_resolver_service import (
            ResolveAllResult,
            apply_resolve_all_result,
        )

        self.db.set_tracks(self.req_id, [{
            "disc_number": 1,
            "track_number": 1,
            "title": "Track",
            "track_artist": None,
        }])
        entered = threading.Event()
        release = threading.Event()
        applied: list[bool] = []

        def stale_apply() -> None:
            entered.set()
            if not release.wait(timeout=10):
                raise TimeoutError("field resolver barrier was not released")
            applied.append(apply_resolve_all_result(
                self.db,
                self.req_id,
                ResolveAllResult(
                    release_group_year=1999,
                    is_va_compilation=True,
                    track_artists=["Late Artist"],
                ),
                expected_status="wanted",
            ))

        worker = threading.Thread(target=stale_apply)
        worker.start()
        self.assertTrue(entered.wait(timeout=10))
        self.assertTrue(self.db.update_status(
            self.req_id, "unsearchable", expected_status="wanted",
        ))
        stopped_row = self.db.get_request(self.req_id)
        stopped_tracks = self.db.get_tracks(self.req_id)
        assert stopped_row is not None
        release.set()
        worker.join(timeout=10)

        self.assertFalse(worker.is_alive())
        self.assertEqual(applied, [False])
        self.assertEqual(self.db.get_request(self.req_id), stopped_row)
        self.assertEqual(self.db.get_tracks(self.req_id), stopped_tracks)
        self.assertIsNone(stopped_row["release_group_year"])
        self.assertFalse(stopped_row["is_va_compilation"])
        self.assertIsNone(stopped_tracks[0]["track_artist"])


@requires_postgres
class TestDownloadLog(unittest.TestCase):
    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="dl-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_log_and_get_download(self):
        self.db.log_download(
            request_id=self.req_id,
            soulseek_username="user123",
            filetype="flac",
            download_path="/tmp/dl/files",
            beets_distance=0.08,
            beets_scenario="single-disc",
            outcome="success",
            staged_path="/Incoming/A/B",
        )
        history = self.db.get_download_history(self.req_id)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["soulseek_username"], "user123")
        self.assertAlmostEqual(cast(float, history[0]["beets_distance"]), 0.08)
        self.assertEqual(history[0]["outcome"], "success")

    def test_log_download_preserves_null_beets_distance(self):
        """Rule A (test-fidelity.md): a pre-match reject (#550 defect #4)
        never fabricates a measured distance — ``beets_distance=None``
        must survive the real PG round-trip, not silently coerce to 0 or
        any other default."""
        self.db.log_download(
            request_id=self.req_id,
            soulseek_username="user456",
            filetype="mp3",
            beets_distance=None,
            beets_scenario="untracked_audio",
            outcome="rejected",
        )
        history = self.db.get_download_history(self.req_id)
        self.assertEqual(len(history), 1)
        self.assertIsNone(history[0]["beets_distance"])

    def test_log_download_derives_validation_projection_round_trip(self):
        """Rule A: JSONB evidence is the sole distance/scenario input."""
        from lib.quality import ValidationResult

        validation_result = ValidationResult(
            valid=False,
            distance=0.0,
            scenario="untracked_audio",
            detail="tracked manifest differs",
        ).to_json()
        self.db.log_download(
            request_id=self.req_id,
            soulseek_username="projection-user",
            outcome="rejected",
            validation_result=validation_result,
        )

        history = self.db.get_download_history(self.req_id)
        self.assertEqual(len(history), 1)
        vr = cast(dict, history[0]["validation_result"])
        self.assertEqual(vr["distance"], 0.0)
        self.assertEqual(vr["scenario"], "untracked_audio")
        self.assertEqual(history[0]["beets_distance"], 0.0)
        self.assertEqual(history[0]["beets_scenario"], "untracked_audio")

    def test_log_download_derives_custom_envelope_scenario_round_trip(self):
        """Curator audit blobs share the envelope without being full structs."""
        validation_result = json.dumps({
            "scenario": "curator_ban",
            "hashes_recorded": 3,
            "denylisted_username": "bad-peer",
        })
        self.db.log_download(
            request_id=self.req_id,
            outcome="curator_ban",
            validation_result=validation_result,
        )

        history = self.db.get_download_history(self.req_id)
        self.assertEqual(history[0]["beets_scenario"], "curator_ban")
        self.assertIsNone(history[0]["beets_distance"])
        vr = cast(dict, history[0]["validation_result"])
        self.assertEqual(vr["hashes_recorded"], 3)

    def test_log_download_allows_explicit_metadata_only_when_envelope_omits_it(self):
        """MeasurementFailure has no distance/scenario projection keys."""
        import msgspec

        from lib.quality import MeasurementFailure

        payload = MeasurementFailure(
            reason="measurement_crashed",
            detail="ffmpeg failed",
            source_path="/tmp/source",
        )
        self.db.log_download(
            request_id=self.req_id,
            outcome="measurement_failed",
            beets_scenario="measurement_failed",
            validation_result=msgspec.json.encode(payload).decode(),
        )

        history = self.db.get_download_history(self.req_id)
        self.assertEqual(history[0]["beets_scenario"], "measurement_failed")
        self.assertIsNone(history[0]["beets_distance"])
        vr = cast(dict, history[0]["validation_result"])
        self.assertEqual(vr["reason"], "measurement_crashed")

    def test_measurement_failure_paths_round_trip_as_permanent_retention_set(self):
        retained = "/mnt/music/slskd/retained-measurement-failure"
        self.db.log_download(
            request_id=self.req_id,
            outcome="measurement_failed",
            staged_path=retained,
        )
        self.db.log_download(
            request_id=self.req_id,
            outcome="measurement_failed",
            staged_path=retained,
        )
        self.db.log_download(
            request_id=self.req_id,
            outcome="measurement_failed",
            staged_path="",
        )
        self.db.log_download(
            request_id=self.req_id,
            outcome="rejected",
            staged_path="/mnt/music/slskd/not-retained-by-this-rule",
        )

        self.assertEqual(
            self.db.get_retained_failure_paths(),
            {retained},
        )

    def test_get_retained_failure_paths_protects_historical_quarantine_audit(self):
        """``post_commit_quarantine`` has no current writer.

        Issue #1077, D3: ``audio_corrupt`` is ban + delete, never
        quarantined, so nothing in production writes this key any more.
        Historical rows quarantined before that fix still carry it, and the
        disk reaper must keep honouring it for as long as the audit row
        exists — simulate one directly via SQL rather than the deleted
        writer method.
        """
        log_id = self.db.log_download(
            request_id=self.req_id,
            outcome="rejected",
            validation_result=json.dumps({"scenario": "audio_corrupt"}),
        )
        target = "/mnt/music/slskd/failed_imports/bad_files/Album"
        self.db._execute(
            """
            UPDATE download_log
            SET validation_result = validation_result
                || jsonb_build_object('post_commit_quarantine', %s::jsonb)
                || jsonb_build_object('failed_path', %s::text)
            WHERE id = %s
            """,
            (json.dumps({"moved": True}), target, log_id),
        )
        self.assertEqual(
            self.db.get_retained_failure_paths(),
            {target},
        )

    def test_log_download_rejects_duplicate_validation_projection_inputs(self):
        """There is no precedence rule between evidence and denormalized args."""
        from lib.quality import ValidationResult

        validation_result = ValidationResult(
            distance=0.07,
            scenario="high_distance",
        ).to_json()
        with self.assertRaisesRegex(ValueError, "beets_distance"):
            self.db.log_download(
                request_id=self.req_id,
                outcome="rejected",
                beets_distance=0.99,
                validation_result=validation_result,
            )
        with self.assertRaisesRegex(ValueError, "beets_scenario"):
            self.db.log_download(
                request_id=self.req_id,
                outcome="rejected",
                beets_scenario="wrong_value",
                validation_result=validation_result,
            )
        self.assertEqual(self.db.get_download_history(self.req_id), [])

    def test_multiple_downloads(self):
        self.db.log_download(self.req_id, "user1", "flac", "/tmp/1", outcome="rejected")
        self.db.log_download(self.req_id, "user2", "flac", "/tmp/2", outcome="success",
                             beets_distance=0.05, staged_path="/Incoming/A/B")
        history = self.db.get_download_history(self.req_id)
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]["soulseek_username"], "user2")

    def test_force_import_round_trips_explicit_origin_and_distance(self):
        source_id = self.db.log_download(
            self.req_id,
            "source-peer",
            "flac",
            "/failed/source",
            outcome="rejected",
            validation_result=json.dumps({
                "scenario": "high_distance",
                "distance": 0.2328,
            }),
        )
        force_id = self.db.log_download(
            self.req_id,
            "source-peer",
            "flac",
            "/failed/source",
            outcome="force_import",
            source_download_log_id=source_id,
        )

        row = self.db.get_download_log_entry(force_id)
        assert row is not None
        self.assertEqual(row["source_download_log_id"], source_id)
        self.assertAlmostEqual(
            cast(float, row["original_beets_distance"]), 0.2328)

        recent = {item["id"]: item for item in self.db.get_log(limit=10)}
        self.assertAlmostEqual(
            cast(float, recent[force_id]["original_beets_distance"]), 0.2328)

    def test_linked_import_logs_ignore_recents_outcome_filter(self):
        source_id = self.db.log_download(
            self.req_id,
            "source-peer",
            "flac",
            "/failed/source",
            outcome="rejected",
            validation_result=json.dumps({
                "scenario": "high_distance",
                "distance": 0.2328,
            }),
        )
        linked_id = self.db.log_download(
            self.req_id,
            "source-peer",
            "flac",
            "/failed/source",
            outcome="force_import",
            source_download_log_id=source_id,
        )
        self.db.log_download(
            self.req_id,
            "unrelated-peer",
            "mp3",
            "/Incoming/A/B",
            outcome="success",
        )

        linked = self.db.get_linked_import_logs([source_id])

        self.assertEqual([row["id"] for row in linked], [linked_id])
        self.assertEqual(linked[0]["source_download_log_id"], source_id)
        self.assertAlmostEqual(
            cast(float, linked[0]["original_beets_distance"]), 0.2328
        )

    def test_candidate_evidence_source_overlay_is_consistent_across_reads(self):
        from lib.quality import AudioQualityMeasurement

        log_id = self.db.log_download(
            self.req_id,
            outcome="rejected",
            validation_result=json.dumps({"scenario": "high_distance"}),
        )
        evidence = make_album_quality_evidence(
            mb_release_id="dl-uuid",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=201,
                avg_bitrate_kbps=259,
                median_bitrate_kbps=255,
                format="MP3",
            ),
        )
        self.db.upsert_album_quality_evidence(evidence)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.db.set_download_log_candidate_evidence(log_id, stored.id)

        rows = [
            self.db.get_download_log_entry(log_id),
            self.db.get_download_history(self.req_id)[0],
            self.db.get_download_history_batch([self.req_id])[self.req_id][0],
            next(row for row in self.db.get_log() if row["id"] == log_id),
            self.db.get_latest_download_summaries(
                [self.req_id]
            )[self.req_id]["latest"],
        ]
        for row in rows:
            assert row is not None
            self.assertEqual(row["source_format"], "MP3")
            self.assertEqual(row["source_min_bitrate"], 201)
            self.assertEqual(row["source_avg_bitrate"], 259)
            self.assertEqual(row["source_median_bitrate"], 255)

    def test_every_reader_returns_the_proof_gate_evidence_columns(self):
        """The PR4 aliases survive real PG on every download-log reader.

        Issue #829 Phase 5 PR4 added one shared candidate-evidence column
        block (``_CANDIDATE_EVIDENCE_COLUMNS``) to five queries. Every
        candidate fact round-trips except output-only conversion lineage,
        which must project to NULL even when the canonical row is also linked
        as current evidence.
        """
        from lib.quality import (
            VERIFIED_LOSSLESS_CLASSIFIER_V4,
            AacLatticeCapture,
            AacLatticeTrackScore,
            AudioQualityMeasurement,
            VerifiedLosslessProof,
        )

        log_id = self.db.log_download(self.req_id, outcome="success")
        evidence = make_album_quality_evidence(
            mb_release_id="dl-uuid",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=900,
                avg_bitrate_kbps=950,
                format="FLAC",
                spectral_grade="genuine",
                spectral_bitrate_kbps=None,
                spectral_subject="source",
                spectral_provenance="measured",
                was_converted_from="flac",
                cliff_hz=18250,
                codec_family="lossless",
                ultrasonic_deficit_db=41.5,
                spectral_measurement_version=2,
            ),
            codec="flac",
            container="flac",
            storage_format="FLAC",
            aac_lattice=AacLatticeCapture.from_tracks([
                AacLatticeTrackScore(
                    filename=f"{index:02d}.flac",
                    offset=64 if index < 2 else 100 + index,
                    z=4.25 if index == 0 else 1.5,
                    proba=0.5,
                )
                for index in range(6)
            ]),
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="measured",
                source="flac",
                classifier=VERIFIED_LOSSLESS_CLASSIFIER_V4,
                detail="genuine",
            ),
        )
        self.db.upsert_album_quality_evidence(evidence)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.db.set_download_log_candidate_evidence(log_id, stored.id)
        self.assertTrue(
            self.db.set_request_current_evidence(self.req_id, stored.id)
        )

        expected = {
            "_evidence_format": "FLAC",
            "_evidence_filetype_band": evidence.filetype_band,
            "_evidence_codec_family": "lossless",
            "_evidence_cliff_hz": 18250,
            "_evidence_storage_format": "FLAC",
            "_evidence_spectral_subject": "source",
            "_evidence_was_converted_from": None,
            "_evidence_ultrasonic_deficit_db": 41.5,
            "_evidence_spectral_measurement_version": 2,
            "_evidence_aac_lattice_modal_count": 2,
            "_evidence_aac_lattice_scored_tracks": 6,
            "_evidence_aac_lattice_max_z": 4.25,
            "_evidence_verified_lossless_classifier": (
                VERIFIED_LOSSLESS_CLASSIFIER_V4
            ),
        }
        rows = [
            self.db.get_download_log_entry(log_id),
            self.db.get_download_history(self.req_id)[0],
            self.db.get_download_history_batch([self.req_id])[self.req_id][0],
            next(row for row in self.db.get_log() if row["id"] == log_id),
            self.db.get_latest_download_summaries(
                [self.req_id]
            )[self.req_id]["latest"],
        ]
        for row in rows:
            assert row is not None
            for key, value in expected.items():
                self.assertEqual(
                    row[key], value,
                    f"{key} was dropped at the PG boundary")
            self.assertEqual(
                sorted(row["_evidence_container_extensions"] or []),
                sorted({file.extension for file in evidence.files}),
            )
        reloaded = self.db.load_album_quality_evidence_by_id(stored.id)
        assert reloaded is not None
        self.assertEqual(reloaded.measurement.was_converted_from, "flac")

    def test_shared_candidate_recents_withholds_current_only_lineage(self):
        from web.classify import proof_gate_projection

        log_id = self.db.log_download(self.req_id, outcome="rejected")
        shared = make_album_quality_evidence(
            mb_release_id="dl-uuid",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=128,
                avg_bitrate_kbps=130,
                median_bitrate_kbps=129,
                format="Opus",
                spectral_grade="likely_transcode",
                spectral_bitrate_kbps=128,
                spectral_subject="source",
                spectral_provenance="measured",
                was_converted_from="flac",
            ),
            codec="opus",
            container="opus",
            storage_format="Opus",
        )
        self.db.upsert_album_quality_evidence(shared)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=shared.mb_release_id,
            snapshot_fingerprint=shared.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.assertTrue(
            self.db.set_request_current_evidence(self.req_id, stored.id)
        )
        self.db.set_download_log_candidate_evidence(log_id, stored.id)

        row = self.db.get_download_log_entry(log_id)
        assert row is not None
        projection = proof_gate_projection(row)

        self.assertIsNone(row["_evidence_was_converted_from"])
        self.assertIsNone(projection.verdict_tier)
        self.assertFalse(projection.spectral_accusation_admissible)
        self.assertIsNotNone(projection.spectral_accusation_withheld)

    def test_a_carried_proof_still_reaches_the_renderer(self):
        """``carried`` is this album's OWN proof, propagated to its library row.

        ``lib/quality_evidence.py`` stamps ``EVIDENCE_PROVENANCE_CARRIED``
        when the just-imported candidate's proof is carried onto the current
        evidence for the same album — the documented lossless-source-gated
        propagation, not another album's proof. Gating on it would delete a
        true "proved by" line from 2,241 live requests, so the attribution
        rule is exact release identity plus source-semantic lineage.
        """
        from lib.quality import (
            VERIFIED_LOSSLESS_CLASSIFIER_V4,
            AudioQualityMeasurement,
            VerifiedLosslessProof,
        )

        log_id = self.db.log_download(self.req_id, outcome="success")
        evidence = make_album_quality_evidence(
            mb_release_id="dl-uuid",
            lineage_version=4,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=900,
                avg_bitrate_kbps=950,
                format="FLAC",
                spectral_grade="genuine",
                spectral_subject="source",
                spectral_provenance="measured",
                was_converted_from="flac",
            ),
            codec="flac",
            container="flac",
            storage_format="FLAC",
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="carried",
                source="flac",
                classifier=VERIFIED_LOSSLESS_CLASSIFIER_V4,
            ),
        )
        self.db.upsert_album_quality_evidence(evidence)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.db.set_download_log_candidate_evidence(log_id, stored.id)

        row = self.db.get_download_log_entry(log_id)
        assert row is not None
        self.assertEqual(
            row["_evidence_verified_lossless_classifier"],
            VERIFIED_LOSSLESS_CLASSIFIER_V4,
        )
        self.assertEqual(row["source_format"], "FLAC")

    def test_legacy_lineage_evidence_never_lends_its_proof(self):
        """A cross-walked evidence row's proof must not reach the renderer.

        Migration 021 §6b pointed pre-content-addressing ``download_log``
        rows at whichever content-addressed evidence row their release
        already had, so a legacy-lineage ``candidate_evidence_id`` can name
        a SIBLING attempt's snapshot. The measurement facts are already
        gated on lineage 3/4 for exactly that reason; the minted proof is
        the same kind of fact and is gated in place beside them. Ungated,
        109 live rows wore another attempt's proof — a never-converted MP3
        rendering "MP3 320, verified lossless".

        Spectral facts were never target projections, so they must still
        fold through from the same row: the gate is about which bytes the
        conclusion describes, not a blanket distrust of legacy evidence.
        """
        from lib.quality import (
            VERIFIED_LOSSLESS_CLASSIFIER_V4,
            AudioQualityMeasurement,
            VerifiedLosslessProof,
        )

        log_id = self.db.log_download(self.req_id, outcome="success")
        evidence = make_album_quality_evidence(
            mb_release_id="dl-uuid",
            lineage_version=1,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=900,
                avg_bitrate_kbps=950,
                format="FLAC",
                spectral_grade="genuine",
                spectral_subject="source",
                spectral_provenance="measured",
                was_converted_from="flac",
            ),
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="measured",
                source="flac",
                classifier=VERIFIED_LOSSLESS_CLASSIFIER_V4,
            ),
        )
        self.db.upsert_album_quality_evidence(evidence)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.db.set_download_log_candidate_evidence(log_id, stored.id)

        rows = [
            self.db.get_download_log_entry(log_id),
            self.db.get_download_history(self.req_id)[0],
            self.db.get_download_history_batch([self.req_id])[self.req_id][0],
            next(row for row in self.db.get_log() if row["id"] == log_id),
            self.db.get_latest_download_summaries(
                [self.req_id]
            )[self.req_id]["latest"],
        ]
        for row in rows:
            assert row is not None
            self.assertIsNone(
                row["_evidence_verified_lossless_classifier"],
                "a legacy-lineage evidence row lent its proof to the "
                "renderer",
            )
            self.assertIsNone(row["source_format"])
            self.assertEqual(row["spectral_grade"], "genuine")

    def test_get_log_returns_the_current_evidence_codec_columns(self):
        """The HAVE-side codec facts survive real PG (issue #829 PR4).

        Only ``get_log`` joins the request's CURRENT evidence, and the
        audit-only flag beside the HAVE grade chip is derived from exactly
        these six columns. A dropped one silently returns the historical
        accusing render on the 1,735 live rows the flag exists for.
        """
        from lib.quality import AudioQualityMeasurement

        log_id = self.db.log_download(self.req_id, outcome="rejected")
        installed = make_album_quality_evidence(
            mb_release_id="dl-uuid",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=256,
                avg_bitrate_kbps=256,
                is_cbr=True,
                format="AAC",
                spectral_grade="likely_transcode",
                spectral_bitrate_kbps=128,
                spectral_subject="installed",
                spectral_provenance="measured",
                was_converted_from=None,
                cliff_hz=15000,
                codec_family="aac",
                spectral_measurement_version=2,
            ),
            codec="aac",
            container="m4a",
            storage_format="AAC",
        )
        self.db.upsert_album_quality_evidence(installed)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=installed.mb_release_id,
            snapshot_fingerprint=installed.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.assertTrue(
            self.db.set_request_current_evidence(self.req_id, stored.id))

        row = next(row for row in self.db.get_log() if row["id"] == log_id)
        self.assertEqual(row["_current_evidence_codec_family"], "aac")
        self.assertEqual(row["_current_evidence_cliff_hz"], 15000)
        self.assertEqual(row["_current_evidence_storage_format"], "AAC")
        self.assertEqual(
            row["_current_evidence_filetype_band"], installed.filetype_band)
        self.assertEqual(row["_current_evidence_spectral_subject"], "installed")
        self.assertIsNone(row["_current_evidence_was_converted_from"])

    def test_proof_gate_columns_are_null_without_candidate_evidence(self):
        """No evidence join means no verdict — never a fabricated clearance."""
        log_id = self.db.log_download(self.req_id, outcome="rejected")
        row = next(row for row in self.db.get_log() if row["id"] == log_id)
        self.assertIsNone(row["_evidence_format"])
        self.assertIsNone(row["_evidence_codec_family"])
        self.assertIsNone(row["_evidence_verified_lossless_classifier"])
        self.assertIsNone(row["_evidence_container_extensions"])

    def test_get_log_imported_filter_excludes_rejected_rows(self):
        """Contract guard: only truly-imported rows count as "imported".

        ``get_log(outcome_filter='imported')`` filters on ``outcome IN
        ('success', 'force_import')``. Gate-rejected force-imports
        must NOT write ``outcome='force_import'`` or they'd leak into the UI's
        imported counter and the /api/pipeline/log imported view. Regression
        guard for the audit that caught this: a gate-rejected force import
        belongs in the "rejected" filter, not "imported".
        """
        # A successful auto import.
        self.db.log_download(
            self.req_id, "user-success", "mp3", "/Incoming/A/B",
            outcome="success", beets_distance=0.05)
        # A successful force import.
        self.db.log_download(
            self.req_id, "user-force", "mp3", "/Incoming/A/B",
            outcome="force_import", beets_distance=0.0)
        # A gate-rejected force import (e.g. spectral_reject, audio_corrupt,
        # nested_layout). Per CLAUDE.md the outcome MUST be "rejected".
        self.db.log_download(
            self.req_id, "user-gate", "mp3", "/tmp/reject",
            outcome="rejected", beets_scenario="spectral_reject")
        # A preview measurement failure is also a non-imported failure row.
        self.db.log_download(
            self.req_id,
            outcome="measurement_failed",
            beets_scenario="measurement_failed",
            error_message="ffmpeg decode failed",
        )

        imported = self.db.get_log(outcome_filter="imported")
        outcomes = {row["outcome"] for row in imported}
        self.assertEqual(
            outcomes, {"success", "force_import"},
            f"imported filter must only include success + force_import, "
            f"got {outcomes}")
        self.assertNotIn(
            "rejected", outcomes,
            "gate-rejected rows must not appear under the imported filter")

        rejected = self.db.get_log(outcome_filter="rejected")
        rejected_outcomes = {row["outcome"] for row in rejected}
        self.assertIn("rejected", rejected_outcomes,
                      "gate-rejected rows must surface under the rejected filter")
        self.assertIn(
            "measurement_failed",
            rejected_outcomes,
            "preview failures must surface under the rejected filter",
        )

    def test_get_log_keeps_download_source_and_aliases_request_source(self):
        slskd_id = self.db.log_download(
            self.req_id, "user-success", "mp3", "/Incoming/A/B",
            outcome="success", beets_distance=0.05)
        yt_id = self.db.insert_youtube_running(
            request_id=self.req_id,
            browse_id="MPREb_get_log",
            audio_playlist_id=None,
            yt_url="https://music.youtube.com/playlist?list=get-log",
            expected_track_count=10,
        )

        rows = self.db.get_log(limit=10)
        by_id = {row["id"]: row for row in rows}
        self.assertEqual(by_id[slskd_id]["source"], "slskd")
        self.assertEqual(by_id[slskd_id]["request_source"], "request")
        self.assertEqual(by_id[yt_id]["source"], "youtube")
        self.assertEqual(by_id[yt_id]["request_source"], "request")

    def test_log_download_round_trip_preserves_materialize_failure_evidence(self):
        """Rule A (test-fidelity.md) for issue #868's grace-expiry row.

        ``lib/download.py`` now writes the machine reason alongside the
        operator-facing grace sentence. Both must survive the REAL PG
        round trip: ``FakePipelineDB`` stores whatever dict it is handed,
        so a column omitted from the INSERT list would be invisible in
        every orchestration test and silently ``None`` in production.
        """
        self.db.log_download(
            request_id=self.req_id,
            soulseek_username="user1",
            filetype="flac",
            outcome="failed",
            beets_detail="event_path_never_stamped",
            error_message=(
                "Completed download could not be materialized within 3600s "
                "of processing start; resetting to wanted for re-download"
            ),
        )

        history = self.db.get_download_history(self.req_id)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["beets_detail"], "event_path_never_stamped")
        self.assertEqual(
            history[0]["error_message"],
            "Completed download could not be materialized within 3600s "
            "of processing start; resetting to wanted for re-download",
        )
        # The reason must not be laundered into the distance/scenario
        # projection columns — it is evidence, not a validation verdict.
        self.assertIsNone(history[0]["beets_scenario"])
        self.assertIsNone(history[0]["beets_distance"])

    def test_staged_path_failures_round_trip_to_neutral_history_copy(self):
        """The real writer/read projections feed the real history presenters."""
        from lib.failure_presentation import FailureEvidence, present_failure
        from web.download_history_view import build_recents_download_log_rows

        cases = (
            (
                "staged_path_missing",
                ("The staged download folder could not be accessed before "
                "import (possible filesystem error); requeued"),
            ),
            (
                "staged_path_missing_tracked_files",
                ("Tracked files in the staged download folder could not be "
                "accessed before import (possible filesystem error); requeued"),
            ),
        )
        for reason, expected in cases:
            with self.subTest(reason=reason):
                log_id = self.db.log_download(
                    request_id=self.req_id,
                    soulseek_username=None,
                    filetype="opus",
                    outcome="failed",
                    beets_detail=reason,
                    error_message=reason,
                )

                history_row = next(
                    row for row in self.db.get_download_history(self.req_id)
                    if row["id"] == log_id
                )
                self.assertEqual(
                    (
                        history_row["outcome"],
                        history_row["soulseek_username"],
                        history_row["filetype"],
                        history_row["beets_detail"],
                        history_row["error_message"],
                    ),
                    ("failed", None, "opus", reason, reason),
                )
                evidence = FailureEvidence.from_row(dict(history_row))
                self.assertEqual(
                    present_failure(evidence).verdict,
                    expected,
                )

                recent_row = next(
                    row for row in self.db.get_log(limit=10)
                    if row["id"] == log_id
                )
                classified = build_recents_download_log_rows(
                    [dict(recent_row)])[0]
                self.assertIsNone(classified["soulseek_username"])
                self.assertEqual(classified["outcome"], "failed")
                self.assertEqual(classified["verdict"], expected)
                self.assertEqual(classified["summary"], expected)

    def test_log_download_round_trip_preserves_transfer_detail(self):
        """Rule A (test-fidelity.md): migration 043's transfer_detail
        JSONB column must actually preserve what log_download writes —
        a real-PG round trip, not the fake's verbatim dict storage."""
        from lib.quality import FileFailureDetail
        detail = [
            FileFailureDetail(
                username="user1",
                filename="user1\\Music\\01.flac",
                last_state="Completed, Errored",
                last_exception="Read error: Connection reset by peer",
                bytes_transferred=1234,
                retry_count=2,
            ),
            FileFailureDetail(
                username="user1",
                filename="user1\\Music\\02.flac",
            ),
        ]
        self.db.log_download(
            request_id=self.req_id,
            soulseek_username="user1",
            filetype="flac",
            outcome="timeout",
            error_message="all 2 files errored",
            transfer_detail=msgspec.to_builtins(detail),
        )

        history = self.db.get_download_history(self.req_id)
        self.assertEqual(len(history), 1)
        round_tripped = history[0]["transfer_detail"]
        self.assertEqual(
            round_tripped,
            [msgspec.to_builtins(d) for d in detail],
        )

    def test_log_download_round_trip_preserves_every_field_including_source(
        self,
    ):
        """Rule A (test-fidelity.md): migration 080 (issue #1176 PR1) adds
        the ``source`` parameter to ``log_download`` — a real-PG round trip
        is the only thing that can prove the new column actually landed in
        the INSERT column list rather than being silently dropped
        (``FakePipelineDB`` stores the input dict verbatim and cannot see
        that class of drift; this is the exact ``album_title`` failure mode
        Rule A exists for). Every kwarg is asserted individually, not just
        ``source`` — direct typed kwargs (no dict-unpack) so each assertion
        below reads a literal TypedDict key, matching the house style in
        ``test_log_and_get_download`` rather than a dynamic-key loop."""
        self.db.log_download(
            request_id=self.req_id,
            soulseek_username="local-op",
            filetype="flac",
            download_path="/mnt/virtio/Music/Incoming/auto-import/local",
            beets_distance=0.05,
            beets_scenario="single-disc",
            beets_detail="matched exactly",
            valid=True,
            outcome="success",
            staged_path="/mnt/virtio/Music/Incoming/auto-import/local",
            bitrate=1000,
            sample_rate=44100,
            bit_depth=16,
            is_vbr=False,
            was_converted=False,
            original_filetype="flac",
            slskd_filetype="flac",
            actual_filetype="flac",
            actual_min_bitrate=1000,
            spectral_grade="EXCELLENT",
            spectral_bitrate=1000,
            existing_min_bitrate=900,
            existing_spectral_bitrate=900,
            final_format="flac",
            # The field under test: migration 080 widened
            # download_log_source_check to admit 'local' -- proving the
            # value round-trips through the real INSERT, not just the
            # Python default parameter.
            source="local",
        )

        history = self.db.get_download_history(self.req_id)
        self.assertEqual(len(history), 1)
        row = history[0]
        self.assertEqual(row["soulseek_username"], "local-op")
        self.assertEqual(row["filetype"], "flac")
        self.assertEqual(
            row["download_path"],
            "/mnt/virtio/Music/Incoming/auto-import/local",
        )
        beets_distance = row["beets_distance"]
        assert beets_distance is not None
        self.assertAlmostEqual(beets_distance, 0.05)
        self.assertEqual(row["beets_scenario"], "single-disc")
        self.assertEqual(row["beets_detail"], "matched exactly")
        self.assertEqual(row["valid"], True)
        self.assertEqual(row["outcome"], "success")
        self.assertEqual(
            row["staged_path"],
            "/mnt/virtio/Music/Incoming/auto-import/local",
        )
        self.assertEqual(row["bitrate"], 1000)
        self.assertEqual(row["sample_rate"], 44100)
        self.assertEqual(row["bit_depth"], 16)
        self.assertEqual(row["is_vbr"], False)
        self.assertEqual(row["was_converted"], False)
        self.assertEqual(row["original_filetype"], "flac")
        self.assertEqual(row["slskd_filetype"], "flac")
        self.assertEqual(row["actual_filetype"], "flac")
        self.assertEqual(row["actual_min_bitrate"], 1000)
        self.assertEqual(row["spectral_grade"], "EXCELLENT")
        self.assertEqual(row["spectral_bitrate"], 1000)
        self.assertEqual(row["existing_min_bitrate"], 900)
        self.assertEqual(row["existing_spectral_bitrate"], 900)
        self.assertEqual(row["final_format"], "flac")
        self.assertEqual(row["source"], "local")


@requires_postgres
class TestSearchLog(unittest.TestCase):
    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="search-log-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_log_and_get_search(self):
        self.db.log_search(
            request_id=self.req_id,
            query="*rtist Album",
            result_count=42,
            elapsed_s=3.2,
            outcome="found",
        )
        history = self.db.get_search_history(self.req_id)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["query"], "*rtist Album")
        self.assertEqual(history[0]["result_count"], 42)
        elapsed = history[0]["elapsed_s"]
        assert isinstance(elapsed, (int, float))
        self.assertAlmostEqual(elapsed, 3.2, places=1)
        self.assertEqual(history[0]["outcome"], "found")

    def test_multiple_searches_newest_first(self):
        self.db.log_search(self.req_id, query="q1", outcome="no_results")
        self.db.log_search(self.req_id, query="q2", result_count=5,
                           elapsed_s=2.0, outcome="no_match")
        history = self.db.get_search_history(self.req_id)
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]["outcome"], "no_match")  # most recent first
        self.assertEqual(history[1]["outcome"], "no_results")

    def test_empty_query_outcome(self):
        self.db.log_search(self.req_id, query=None, outcome="empty_query")
        history = self.db.get_search_history(self.req_id)
        self.assertEqual(len(history), 1)
        self.assertIsNone(history[0]["query"])
        self.assertIsNone(history[0]["result_count"])
        self.assertEqual(history[0]["outcome"], "empty_query")

    def test_all_outcomes_valid(self):
        for outcome in ("found", "no_match", "no_results", "timeout", "error", "empty_query"):
            self.db.log_search(self.req_id, query="q", outcome=outcome)
        history = self.db.get_search_history(self.req_id)
        self.assertEqual(len(history), 6)

    def test_exhausted_outcome_now_allowed_post_migration_010(self):
        """Migration 010 widened the CHECK constraint to include 'exhausted'."""
        self.db.log_search(
            self.req_id, query=None, outcome="exhausted",
            variant="exhausted",
        )
        history = self.db.get_search_history(self.req_id)
        self.assertEqual(history[0]["outcome"], "exhausted")
        self.assertEqual(history[0]["variant"], "exhausted")

    def test_log_search_persists_candidates_jsonb_and_round_trips(self):
        """U5 wire-boundary: encode list[CandidateScore] → JSONB → decode."""
        import json

        import msgspec

        from lib.quality import CandidateScore

        candidates = [
            CandidateScore(
                username="u1", dir="A\\Album", filetype="flac",
                matched_tracks=26, total_tracks=26, avg_ratio=0.95,
                missing_titles=[], file_count=26,
            ),
            CandidateScore(
                username="u2", dir="B\\Album", filetype="flac",
                matched_tracks=22, total_tracks=26, avg_ratio=0.0,
                missing_titles=[], file_count=22,
            ),
        ]
        self.db.log_search(
            request_id=self.req_id,
            query="*rtist Album",
            result_count=10,
            elapsed_s=2.5,
            outcome="no_match",
            candidates=candidates,
            variant="default",
            final_state="Completed",
        )

        history = self.db.get_search_history(self.req_id)
        self.assertEqual(len(history), 1)
        row = history[0]
        self.assertEqual(row["variant"], "default")
        self.assertEqual(row["final_state"], "Completed")

        # psycopg2 returns JSONB as already-decoded Python objects, but
        # accept a str fallback in case driver settings differ.
        raw = row["candidates"]
        if isinstance(raw, str):
            raw = json.loads(raw)
        assert isinstance(raw, list)
        self.assertEqual(len(raw), 2)
        decoded = msgspec.convert(raw, type=list[CandidateScore])
        self.assertEqual(decoded[0].username, "u1")
        self.assertEqual(decoded[0].matched_tracks, 26)
        self.assertEqual(decoded[1].file_count, 22)

    def test_log_search_with_null_candidates_writes_sql_null(self):
        """Failure rows (timeout/error) still write a row, candidates NULL."""
        self.db.log_search(
            request_id=self.req_id, query="q", outcome="timeout",
            variant="v1_year", final_state="TimedOut",
            candidates=None,
        )
        history = self.db.get_search_history(self.req_id)
        self.assertIsNone(history[0]["candidates"])
        self.assertEqual(history[0]["variant"], "v1_year")
        self.assertEqual(history[0]["final_state"], "TimedOut")

    def test_log_search_persists_pre_filter_skip_count(self):
        """U2 of search-plan-entropy: ``pre_filter_skip_count`` writes to
        the dedicated column. NOT NULL with default 0; this asserts the
        explicit non-zero path actually round-trips, AND that omitting
        the kwarg defaults to 0 in the persisted row."""
        # Explicit non-zero round-trip.
        self.db.log_search(
            request_id=self.req_id, query="q", outcome="no_match",
            candidates=None, pre_filter_skip_count=42,
        )
        # Default (kwarg omitted) writes 0.
        self.db.log_search(
            request_id=self.req_id, query="q2", outcome="found",
            candidates=None,
        )
        history = self.db.get_search_history(self.req_id)
        # history is newest-first; index 0 == second insert (default).
        self.assertEqual(history[0]["pre_filter_skip_count"], 0)
        self.assertEqual(history[1]["pre_filter_skip_count"], 42)

    def test_log_search_persists_u11_forensics_columns(self):
        """U11 R22-R27: every new forensics column round-trips on log_search.

        Asserts ``rejection_reason``, ``result_count_uncapped``,
        ``query_token_count``, ``query_distinct_token_count``,
        ``expected_track_count``, ``matcher_score_top1``, and
        ``query_template`` survive the INSERT and come back on the
        SELECT * read.
        """
        self.db.log_search(
            request_id=self.req_id,
            query="*rtist Album",
            outcome="no_match",
            candidates=None,
            rejection_reason="avg_ratio_low",
            result_count_uncapped=1234,
            query_token_count=2,
            query_distinct_token_count=2,
            expected_track_count=14,
            matcher_score_top1=2.75,
            query_template="{artist} {title}",
        )
        # Second row: defaults (kwargs omitted) write SQL NULL so we
        # can also assert backwards-compat.
        self.db.log_search(
            request_id=self.req_id, query="q2",
            outcome="no_results", candidates=None,
        )
        history = self.db.get_search_history(self.req_id)
        # newest-first.
        nulls = history[0]
        self.assertIsNone(nulls["rejection_reason"])
        self.assertIsNone(nulls["result_count_uncapped"])
        self.assertIsNone(nulls["query_token_count"])
        self.assertIsNone(nulls["query_distinct_token_count"])
        self.assertIsNone(nulls["expected_track_count"])
        self.assertIsNone(nulls["matcher_score_top1"])
        self.assertIsNone(nulls["query_template"])
        populated = history[1]
        self.assertEqual(populated["rejection_reason"], "avg_ratio_low")
        self.assertEqual(populated["result_count_uncapped"], 1234)
        self.assertEqual(populated["query_token_count"], 2)
        self.assertEqual(populated["query_distinct_token_count"], 2)
        self.assertEqual(populated["expected_track_count"], 14)
        score = populated["matcher_score_top1"]
        assert isinstance(score, float)
        self.assertAlmostEqual(score, 2.75, places=4)
        self.assertEqual(populated["query_template"], "{artist} {title}")

    def test_log_search_candidates_decode_rejects_wrong_type(self):
        """Wire-boundary regression: msgspec.convert raises on type drift.

        At least one RED test that feeds the wrong type at the boundary and
        asserts ``msgspec.ValidationError`` — the strict-typed decoder is
        what catches int-vs-str drift in the JSONB blob downstream.
        """
        import msgspec

        from lib.quality import CandidateScore

        # ``matched_tracks`` is declared int — passing a string at the wire
        # must trip msgspec on read, not silently coerce.
        wrong = [{
            "username": "u1", "dir": "A", "filetype": "flac",
            "matched_tracks": "26",  # WRONG: string for int field
            "total_tracks": 26, "avg_ratio": 0.9,
            "missing_titles": [], "file_count": 26,
        }]
        with self.assertRaises(msgspec.ValidationError):
            msgspec.convert(wrong, type=list[CandidateScore])


@requires_postgres
class TestGetSaturationSummary(unittest.TestCase):
    """U7: ``PipelineDB.get_saturation_summary`` aggregates one request's
    search_log rows in the recent window. Saturation = rows whose
    ``final_state`` matches ``%LimitReached%`` (slskd hit response /
    file ceiling). ``total_pre_filter_skips`` rolls up the U2 column.

    ``saturation_rate`` is computed in Python so the explicit ``0.0``
    fallback survives the empty-window case (NaN would break JSON
    serialisation downstream).
    """

    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="saturation-uuid",
            artist_name="A", album_title="B", source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_empty_request_returns_zeros(self):
        summary = self.db.get_saturation_summary(self.req_id, window_days=14)
        self.assertEqual(summary.total_searches, 0)
        self.assertEqual(summary.saturated_searches, 0)
        # Critical invariant: 0.0, not NaN.
        self.assertEqual(summary.saturation_rate, 0.0)
        self.assertEqual(summary.total_pre_filter_skips, 0)
        self.assertEqual(summary.window_days, 14)

    def test_counts_only_saturated_final_states(self):
        # Only rows whose final_state contains "LimitReached" count
        # as saturated. The slskd state strings are comma-joined so
        # the match must be a substring, not equality.
        for state in (
            "Completed, ResponseLimitReached",
            "Completed, FileLimitReached",
            "Completed",  # not saturated
            "Cancelled",  # not saturated
            None,         # not saturated
        ):
            self.db.log_search(
                request_id=self.req_id, query="q", outcome="found",
                final_state=state,
            )
        summary = self.db.get_saturation_summary(self.req_id, window_days=14)
        self.assertEqual(summary.total_searches, 5)
        self.assertEqual(summary.saturated_searches, 2)
        self.assertAlmostEqual(summary.saturation_rate, 2 / 5)

    def test_sums_pre_filter_skip_count(self):
        for skip in (4, 1, 0, 8):
            self.db.log_search(
                request_id=self.req_id, query="q", outcome="found",
                final_state="Completed", pre_filter_skip_count=skip,
            )
        summary = self.db.get_saturation_summary(self.req_id, window_days=14)
        self.assertEqual(summary.total_searches, 4)
        self.assertEqual(summary.saturated_searches, 0)
        self.assertEqual(summary.total_pre_filter_skips, 13)

    def test_window_days_filters_old_rows(self):
        # Insert two recent rows, then backdate a third via direct SQL
        # so we can test the window cut without sleeping.
        self.db.log_search(
            request_id=self.req_id, query="recent_a",
            outcome="found",
            final_state="Completed, ResponseLimitReached",
            pre_filter_skip_count=2,
        )
        self.db.log_search(
            request_id=self.req_id, query="recent_b",
            outcome="found", final_state="Completed",
            pre_filter_skip_count=1,
        )
        self.db.log_search(
            request_id=self.req_id, query="old",
            outcome="found",
            final_state="Completed, FileLimitReached",
            pre_filter_skip_count=10,
        )
        # Backdate the most recent row 10 days into the past.
        self.db._execute(
            "UPDATE search_log SET created_at = NOW() - INTERVAL '10 days' "
            "WHERE query = %s",
            ("old",),
        )
        # 7-day window: old row out, two recent rows in.
        seven = self.db.get_saturation_summary(self.req_id, window_days=7)
        self.assertEqual(seven.total_searches, 2)
        self.assertEqual(seven.saturated_searches, 1)
        self.assertEqual(seven.total_pre_filter_skips, 3)
        self.assertEqual(seven.window_days, 7)
        # 14-day window: all three rows are in scope.
        fourteen = self.db.get_saturation_summary(
            self.req_id, window_days=14)
        self.assertEqual(fourteen.total_searches, 3)
        self.assertEqual(fourteen.saturated_searches, 2)
        self.assertEqual(fourteen.total_pre_filter_skips, 13)

    def test_isolates_by_request_id(self):
        # Rows for a different request must not bleed into this
        # request's saturation roll-up.
        other = self.db.add_request(
            mb_release_id="other-uuid",
            artist_name="X", album_title="Y", source="request",
        )
        self.db.log_search(
            request_id=other, query="other",
            outcome="found",
            final_state="Completed, ResponseLimitReached",
            pre_filter_skip_count=99,
        )
        self.db.log_search(
            request_id=self.req_id, query="mine",
            outcome="found", final_state="Completed",
            pre_filter_skip_count=2,
        )
        summary = self.db.get_saturation_summary(self.req_id, window_days=14)
        self.assertEqual(summary.total_searches, 1)
        self.assertEqual(summary.saturated_searches, 0)
        self.assertEqual(summary.total_pre_filter_skips, 2)

    def test_window_days_echoes_back(self):
        # The summary echoes window_days so callers don't have to
        # remember what they asked for.
        summary = self.db.get_saturation_summary(self.req_id, window_days=30)
        self.assertEqual(summary.window_days, 30)


@requires_postgres
class TestGetSearchHistoryPage(unittest.TestCase):
    """U1: cursor-style pagination for ``GET /search-plan/history``.

    The DB method ``get_search_history_page`` returns at most ``limit``
    rows for one request_id ordered ``id DESC`` with an opaque
    ``next_before_id`` seed when more rows remain. Mirrors
    ``get_search_history`` shape but bounded.
    """

    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="search-hist-page-uuid",
            artist_name="A", album_title="B", source="request",
        )

    def tearDown(self):
        self.db.close()

    def _seed(self, n: int) -> list[int]:
        for i in range(n):
            self.db.log_search(
                self.req_id, query=f"q{i}", outcome="no_match",
            )
        full = self.db.get_search_history(self.req_id)
        return [int(cast(Any, r["id"])) for r in full]  # newest-first

    def test_first_page_clamps_to_limit_and_seeds_next_before_id(self):
        ids_desc = self._seed(75)
        page = self.db.get_search_history_page(
            self.req_id, limit=50, before_id=None,
        )
        self.assertEqual(len(page.rows), 50)
        # Newest 50 rows in DESC order.
        self.assertEqual(
            [int(cast(Any, r["id"])) for r in page.rows], ids_desc[:50],
        )
        # next_before_id seeds the next page from the 51st row's id.
        self.assertEqual(page.next_before_id, ids_desc[50])

    def test_second_page_via_before_id_returns_strictly_older_rows(self):
        ids_desc = self._seed(75)
        first = self.db.get_search_history_page(
            self.req_id, limit=50, before_id=None,
        )
        second = self.db.get_search_history_page(
            self.req_id, limit=50, before_id=first.next_before_id,
        )
        # First page returns 50 rows; ``next_before_id`` points one row
        # past the boundary (the 51st row), and the second page resumes
        # *at* that row — no row is skipped.
        self.assertEqual(len(second.rows), 25)
        # Second page rows are older-or-equal to the cursor and
        # id-monotonic descending.
        page_ids = [int(cast(Any, r["id"])) for r in second.rows]
        self.assertEqual(page_ids, ids_desc[50:75])
        # No id appears in both pages (no boundary overlap).
        first_ids = {int(cast(Any, r["id"])) for r in first.rows}
        self.assertFalse(first_ids.intersection(page_ids))
        self.assertIsNone(second.next_before_id)

    def test_exhausted_when_fewer_rows_than_limit(self):
        self._seed(30)
        page = self.db.get_search_history_page(
            self.req_id, limit=50, before_id=None,
        )
        self.assertEqual(len(page.rows), 30)
        self.assertIsNone(page.next_before_id)

    def test_empty_when_no_rows_for_request(self):
        page = self.db.get_search_history_page(
            self.req_id, limit=50, before_id=None,
        )
        self.assertEqual(page.rows, [])
        self.assertIsNone(page.next_before_id)

    def test_legacy_only_rows_returned_with_null_plan_columns(self):
        # log_search writes legacy-shaped rows (plan_id IS NULL).
        self.db.log_search(self.req_id, query="legacy", outcome="no_match")
        page = self.db.get_search_history_page(
            self.req_id, limit=10, before_id=None,
        )
        self.assertEqual(len(page.rows), 1)
        row = page.rows[0]
        self.assertEqual(row["query"], "legacy")
        self.assertIsNone(row["plan_id"])
        self.assertIsNone(row["plan_ordinal"])

    def test_other_request_ids_excluded(self):
        other_req = self.db.add_request(
            mb_release_id="search-hist-page-other-uuid",
            artist_name="C", album_title="D", source="request",
        )
        self.db.log_search(other_req, query="other", outcome="no_match")
        self.db.log_search(self.req_id, query="mine", outcome="no_match")
        page = self.db.get_search_history_page(
            self.req_id, limit=10, before_id=None,
        )
        self.assertEqual(len(page.rows), 1)
        self.assertEqual(page.rows[0]["query"], "mine")

    def test_page_returns_dict_rows_with_full_search_log_columns(self):
        self.db.log_search(
            self.req_id, query="q", result_count=5, elapsed_s=1.2,
            outcome="no_match", variant="v0", final_state="Completed",
        )
        page = self.db.get_search_history_page(
            self.req_id, limit=1, before_id=None,
        )
        row = page.rows[0]
        # Spot-check a wider column set than legacy_logs.head provides —
        # this endpoint surfaces full telemetry per row.
        for col in ("id", "request_id", "query", "result_count",
                    "elapsed_s", "outcome", "candidates", "variant",
                    "final_state", "browse_time_s", "match_time_s",
                    "peers_browsed", "peers_browsed_lazy", "fanout_waves",
                    "plan_id", "plan_item_id", "plan_ordinal",
                    "plan_strategy", "plan_canonical_query_key",
                    "plan_repeat_group", "plan_generator_id",
                    "execution_stage", "attempt_consumed",
                    "cursor_update_status", "stale_reason",
                    "plan_cycle_snapshot", "created_at"):
            self.assertIn(col, row, f"missing column {col!r}")


class TestDashboardRowSerializers(unittest.TestCase):
    """Every dashboard row serializer maps each output key from the RIGHT
    input column.

    These four are flat rename-and-coerce functions, which makes them
    blind to exactly one mutation class: a single output key silently
    reading a sibling field. Shape tests (key sets), presence-only route
    contracts (``REQUIRED_FIELDS``), and the fake-vs-real parity tests
    that deliberately compare structure rather than values all survive
    such a swap; so does the one real ``watchdog_kills`` value pin, which
    checks the SQL window aggregate rather than this path. A follow-up
    mutant proved it: pointing ``watchdog_kills`` at
    ``find_download_queued`` in ``serialize_dashboard_cycle_row`` passed
    the entire suite.

    The pattern below is the general kill, not a patch for that one
    mutant: drive each serializer with a row whose every source field
    holds a DISTINCT sentinel, then assert the whole output-to-input
    mapping. Any single-field swap changes a value, and the exact key-set
    assertion stops a new output key from appearing unmapped. This is the
    #1110 / #1241 value-inversion class.

    Sentinel discipline: numeric sentinels start at 2 so none can collide
    with ``True``/``False`` (``True == 1`` in Python) where a serializer
    also carries a boolean, and none is falsy, so an ``or 0`` fallback
    firing wrongly is visible.
    """

    #: ``output key -> source column``. Identity everywhere except the one
    #: deliberate rename, which is the mutant's own target.
    CYCLE_ROW_FIELDS: ClassVar = {
        "id": "id",
        "started_at": "started_at",
        "created_at": "created_at",
        "cycle_total_s": "cycle_total_s",
        "browse_time_s": "browse_time_s",
        "match_time_s": "match_time_s",
        "search_time_s": "search_time_s",
        "watchdog_kills": "cycle_searches_watchdog_killed",
        "find_download_queued": "find_download_queued",
        "find_download_completed": "find_download_completed",
        "find_download_drain_time_s": "find_download_drain_time_s",
        "cache_errors": "cache_errors",
        "cache_write_errors": "cache_write_errors",
        "cache_fuse_tripped": "cache_fuse_tripped",
        "peers_browsed": "peers_browsed",
        "peers_browsed_lazy": "peers_browsed_lazy",
        "fanout_waves": "fanout_waves",
    }

    HEAVY_QUERY_FIELDS: ClassVar = {
        key: key
        for key in (
            "search_log_id", "request_id", "mb_release_id", "artist_name",
            "album_title", "status", "created_at", "query", "variant",
            "outcome", "result_count", "elapsed_s", "browse_time_s",
            "match_time_s", "peers_browsed", "peers_browsed_lazy",
            "peer_dirs", "fanout_waves",
        )
    }

    REQUEST_ROW_FIELDS: ClassVar = {
        key: key
        for key in (
            "request_id", "artist_name", "album_title", "status",
            "last_search_at", "searches_24h", "searches_6h", "found_24h",
            "no_match_24h", "no_results_24h", "reset_24h", "problem_24h",
        )
    }

    UNFINDABLE_RUN_FIELDS: ClassVar = {
        key: key
        for key in (
            "id", "created_at", "cohort_total", "due_backlog_at_start",
            "batch_limit", "candidates_processed", "probes_attempted",
            "categorised_count", "downgraded_count", "no_change_count",
            "probe_failed_count", "not_due_count", "request_not_found_count",
            "breaker_tripped", "duration_seconds",
        )
    }

    def _assert_field_mapping(self, rendered, source, field_map):
        """Each output key equals its own source column, and no other."""
        self.assertEqual(
            set(rendered), set(field_map),
            "serializer output keys drifted from the pinned mapping — add "
            "the new key to the map with the column it must read",
        )
        for out_key, source_key in field_map.items():
            expected = source[source_key]
            if isinstance(expected, datetime):
                expected = expected.isoformat()
            self.assertEqual(
                rendered[out_key], expected,
                f"{out_key!r} must be rendered from {source_key!r}",
            )

    def _distinct_row(self, numeric_keys, *, text_keys=(), datetime_keys=(),
                      bool_keys=()):
        """One row per source column, every value distinct.

        Distinctness is the property the whole pattern rests on — two
        columns sharing a sentinel would make a swap between them
        invisible — so it is asserted here rather than assumed.
        """
        row: dict[str, object] = {}
        for offset, key in enumerate(numeric_keys):
            row[key] = 2 + offset
        for key in text_keys:
            row[key] = f"sentinel-{key}"
        for offset, key in enumerate(datetime_keys):
            row[key] = datetime(2026, 3, 1, tzinfo=UTC) + timedelta(
                minutes=offset + 1)
        for key in bool_keys:
            row[key] = True
        values = list(row.values())
        self.assertEqual(
            len(values), len({repr(value) for value in values}),
            "sentinel collision: a swap between two columns sharing a value "
            "would be invisible to this pin",
        )
        return row

    def test_cycle_row_maps_every_output_from_its_own_column(self):
        source = self._distinct_row(
            [
                "id", "cycle_total_s", "browse_time_s", "match_time_s",
                "search_time_s", "cycle_searches_watchdog_killed",
                "find_download_queued", "find_download_completed",
                "find_download_drain_time_s", "cache_errors",
                "cache_write_errors", "cache_fuse_tripped", "peers_browsed",
                "peers_browsed_lazy", "fanout_waves",
            ],
            datetime_keys=("started_at", "created_at"),
        )
        self._assert_field_mapping(
            serialize_dashboard_cycle_row(source), source,
            self.CYCLE_ROW_FIELDS)

    def test_heavy_query_row_maps_every_output_from_its_own_column(self):
        source = self._distinct_row(
            [
                "search_log_id", "request_id", "result_count", "elapsed_s",
                "browse_time_s", "match_time_s", "peers_browsed",
                "peers_browsed_lazy", "peer_dirs", "fanout_waves",
            ],
            text_keys=(
                "mb_release_id", "artist_name", "album_title", "status",
                "query", "variant", "outcome",
            ),
            datetime_keys=("created_at",),
        )
        self._assert_field_mapping(
            serialize_dashboard_heavy_query_row(source), source,
            self.HEAVY_QUERY_FIELDS)

    def test_request_row_maps_every_output_from_its_own_column(self):
        source = self._distinct_row(
            [
                "request_id", "searches_24h", "searches_6h", "found_24h",
                "no_match_24h", "no_results_24h", "reset_24h", "problem_24h",
            ],
            text_keys=("artist_name", "album_title", "status"),
            datetime_keys=("last_search_at",),
        )
        self._assert_field_mapping(
            serialize_dashboard_request_row(source), source,
            self.REQUEST_ROW_FIELDS)

    def test_unfindable_run_row_maps_every_output_from_its_own_column(self):
        source = self._distinct_row(
            [
                "id", "cohort_total", "due_backlog_at_start", "batch_limit",
                "candidates_processed", "probes_attempted",
                "categorised_count", "downgraded_count", "no_change_count",
                "probe_failed_count", "not_due_count",
                "request_not_found_count", "duration_seconds",
            ],
            datetime_keys=("created_at",),
            bool_keys=("breaker_tripped",),
        )
        # A real typed conversion, not a cast: it also proves the sentinels
        # satisfy the row's declared column types.
        typed = msgspec.convert(source, type=UnfindableRunMetricsRow)
        self._assert_field_mapping(
            serialize_unfindable_run_row(typed), source,
            self.UNFINDABLE_RUN_FIELDS)


class TestWantedTrendPanel(unittest.TestCase):
    """Direct unit coverage for the wanted-backlog trend arithmetic.

    ``wanted_trend_panel`` / ``wanted_trend_window`` are the pure half both
    ``PipelineDB._dashboard_wanted_trend`` and ``FakePipelineDB``'s own
    sample walk delegate to (issue #1278 item 7). The populated and empty
    window shapes are exercised end to end by
    ``TestPipelineDashboardMetrics`` and ``TestFakeDashboardMirror``.

    The zero-elapsed branch needs a sample stamped at or after the
    captured ``now``. Neither fetch excludes one — both select on a lower
    bound only — so what makes it awkward to reach end to end is the
    WRITERS: ``record_cycle_metrics`` stamps ``created_at`` from the real
    completion time, so through either adapter it takes clock skew or a
    caller deliberately passing a future ``completed_at``. Driving the
    function directly is the honest way to pin it.
    """

    NOW = datetime(2026, 8, 31, 12, 0, tzinfo=UTC)

    def test_zero_elapsed_window_reports_unknown_without_dividing(self):
        panel = wanted_trend_panel(
            [(self.NOW, 500)], current_wanted=400, now=self.NOW)
        window = panel["windows"][0]
        self.assertEqual(window["sample_count"], 1)
        self.assertEqual(window["start_wanted"], 500)
        self.assertEqual(window["end_wanted"], 400)
        self.assertEqual(window["delta"], -100)
        self.assertEqual(window["trend"], "unknown")
        for key in ("delta_per_hour", "drain_per_hour", "eta_hours"):
            self.assertIsNone(window[key], key)

    def test_growing_backlog_reports_up_with_no_eta(self):
        panel = wanted_trend_panel(
            [(self.NOW - timedelta(hours=2), 100)],
            current_wanted=140, now=self.NOW)
        window = panel["windows"][0]
        self.assertEqual(window["trend"], "up")
        self.assertEqual(window["delta_per_hour"], 20.0)
        self.assertEqual(window["drain_per_hour"], 0.0)
        self.assertIsNone(window["eta_hours"])

    def test_draining_backlog_reports_eta_from_the_drain_rate(self):
        panel = wanted_trend_panel(
            [(self.NOW - timedelta(hours=4), 200)],
            current_wanted=160, now=self.NOW)
        window = panel["windows"][0]
        self.assertEqual(window["trend"], "down")
        self.assertEqual(window["drain_per_hour"], 10.0)
        self.assertEqual(window["eta_hours"], 16.0)

    def test_series_24h_drops_older_samples_and_appends_the_synthetic_now(self):
        panel = wanted_trend_panel(
            [
                (self.NOW - timedelta(hours=30), 900),
                (self.NOW - timedelta(hours=3), 800),
            ],
            current_wanted=700, now=self.NOW,
        )
        self.assertEqual(
            [(point["wanted_total"], point.get("synthetic"))
             for point in panel["series_24h"]],
            [(800, None), (700, True)],
        )
        # ``latest_sample_at`` names the newest REAL sample, never the
        # synthetic point the series ends with.
        self.assertEqual(
            panel["latest_sample_at"],
            (self.NOW - timedelta(hours=3)).isoformat())
        self.assertEqual(
            [w["label"] for w in panel["windows"]], ["6h", "24h", "7d"])
        # The 7d window still sees the 30h-old sample the series dropped.
        self.assertEqual(panel["windows"][2]["start_wanted"], 900)

    def test_empty_samples_yield_unknown_windows_and_only_the_synthetic_point(self):
        panel = wanted_trend_panel([], current_wanted=42, now=self.NOW)
        self.assertIsNone(panel["latest_sample_at"])
        self.assertEqual(panel["series_24h"], [{
            "sampled_at": self.NOW.isoformat(),
            "wanted_total": 42,
            "synthetic": True,
        }])
        for window in panel["windows"]:
            self.assertEqual(window["sample_count"], 0)
            self.assertEqual(window["trend"], "unknown")
            self.assertIsNone(window["start_wanted"])
            self.assertEqual(window["end_wanted"], 42)


@requires_postgres
class TestPipelineDashboardMetrics(unittest.TestCase):
    def setUp(self):
        self.db = make_db()
        self.req1 = self.db.add_request(
            mb_release_id="dash-1",
            artist_name="Dashboard Artist",
            album_title="Loop Candidate",
            source="request",
        )
        self.req2 = self.db.add_request(
            mb_release_id="dash-2",
            artist_name="Dashboard Artist",
            album_title="Healthy Candidate",
            source="request",
        )
        self.req3 = self.db.add_request(
            mb_release_id="dash-3",
            artist_name="Dashboard Artist",
            album_title="Never Searched",
            source="request",
        )
        self.req4 = self.db.add_request(
            mb_release_id="dash-4",
            artist_name="Dashboard Artist",
            album_title="Active Download",
            source="request",
        )
        self.db.set_downloading(self.req4, json.dumps({"username": "active"}))

    def tearDown(self):
        self.db.close()

    def test_record_cycle_metrics_round_trip_preserves_every_counter(self):
        """Rule A over the whole counters value, against real PostgreSQL.

        Every counter gets a distinct value, so this fails on a counter
        dropped from the INSERT and on one landing in a sibling's column.
        The column list is derived from ``CycleCounters``, which makes a
        drop impossible by construction but a MISNAMED counter a runtime
        error here rather than a silent default -- so the SELECT below
        reads the columns back by name.
        """
        written = CycleCounters(**{
            name: 2 + offset for offset, name in enumerate(COUNTER_NAMES)})
        cycle_id = self.db.record_cycle_metrics(
            cycle_total_s=41.5, counters=written, wanted_total=3)

        cur = self.db._execute(
            "SELECT " + ", ".join(COUNTER_NAMES)
            + ", cycle_total_s, wanted_total FROM cycle_metrics WHERE id = %s",
            (cycle_id,))
        row = cur.fetchone()

        assert row is not None
        for name in COUNTER_NAMES:
            with self.subTest(counter=name):
                self.assertEqual(row[name], getattr(written, name),
                                 f"counter {name} did not survive the "
                                 f"PG boundary in its own column")
        self.assertEqual(row["cycle_total_s"], 41.5)
        self.assertEqual(row["wanted_total"], 3)

    def test_record_cycle_metrics_preserves_an_empty_wanted_backlog(self):
        """A drained backlog is 0, not 1: the writer clamps ``wanted_total``
        at zero to satisfy the migration-016 CHECK, and a clamp that raised
        the floor would quietly invent a request that does not exist."""
        cycle_id = self.db.record_cycle_metrics(
            cycle_total_s=1.0, wanted_total=0)

        cur = self.db._execute(
            "SELECT wanted_total FROM cycle_metrics WHERE id = %s", (cycle_id,))
        row = cur.fetchone()

        assert row is not None
        self.assertEqual(row["wanted_total"], 0)

    def test_record_cycle_metrics_defaults_every_counter_to_zero(self):
        """Omitting ``counters`` writes zeros, as the old per-counter
        keyword defaults did -- not NULL, which the NOT NULL columns
        would reject outright."""
        cycle_id = self.db.record_cycle_metrics(cycle_total_s=1.0)

        cur = self.db._execute(
            "SELECT " + ", ".join(COUNTER_NAMES)
            + " FROM cycle_metrics WHERE id = %s", (cycle_id,))
        row = cur.fetchone()

        assert row is not None
        self.assertEqual([row[name] for name in COUNTER_NAMES],
                         [0] * len(COUNTER_NAMES))

    def test_record_cycle_metrics_and_dashboard_summary(self):
        now = datetime.now(UTC)
        self.db.record_cycle_metrics(
            started_at=now - timedelta(hours=1, seconds=100),
            completed_at=now - timedelta(hours=1),
            cycle_total_s=100.0,
            counters=CycleCounters(
                search_time_s=80.0,
                cycle_searches_watchdog_killed=0,
                find_download_queued=5,
                find_download_completed=5,
            ),
            wanted_total=4,
        )
        self.db.record_cycle_metrics(
            started_at=now - timedelta(hours=2, seconds=300),
            completed_at=now - timedelta(hours=2),
            cycle_total_s=300.0,
            counters=CycleCounters(
                search_time_s=240.0,
                cycle_searches_watchdog_killed=1,
                find_download_queued=3,
                find_download_completed=2,
            ),
            wanted_total=5,
        )
        self.db.record_cycle_metrics(
            started_at=now - timedelta(hours=10, seconds=900),
            completed_at=now - timedelta(hours=10),
            cycle_total_s=900.0,
            counters=CycleCounters(search_time_s=700.0, cache_errors=2),
            wanted_total=6,
        )

        self.db.log_search(
            self.req1, query="loop a", elapsed_s=2.0, outcome="no_results",
            peers_browsed=4, peers_browsed_lazy=1, fanout_waves=1,
            browse_time_s=5.0,
        )
        self.db.log_search(
            self.req1, query="loop b", result_count=500, elapsed_s=4.0,
            outcome="no_match", variant="track_0", peers_browsed=33,
            peers_browsed_lazy=2, fanout_waves=3, browse_time_s=12.5,
            match_time_s=0.5,
        )
        self.db.log_search(
            self.req1, query=None, elapsed_s=1.0, outcome="exhausted"
        )
        self.db.log_search(
            self.req1, query="loop c", elapsed_s=3.0, outcome="timeout"
        )
        self.db.log_search(
            self.req2, query="healthy", elapsed_s=6.0, outcome="found"
        )
        self.db.log_search(
            self.req4, query="active download", elapsed_s=7.0, outcome="found"
        )

        metrics = self.db.get_pipeline_dashboard_metrics()

        searches_24h = metrics["searches"]["windows"][0]
        self.assertEqual(searches_24h["label"], "24h")
        self.assertEqual(searches_24h["searches"], 6)
        self.assertEqual(searches_24h["distinct_requests"], 3)
        self.assertAlmostEqual(searches_24h["searches_per_24h"], 6)
        self.assertEqual(searches_24h["outcomes"]["found"], 2)
        self.assertEqual(searches_24h["outcomes"]["no_match"], 1)
        self.assertEqual(searches_24h["outcomes"]["no_results"], 1)
        self.assertEqual(searches_24h["outcomes"]["exhausted"], 1)
        self.assertEqual(searches_24h["outcomes"]["errors"], 1)

        searches_6h = metrics["searches"]["windows"][1]
        self.assertEqual(searches_6h["label"], "6h")
        self.assertAlmostEqual(searches_6h["searches_per_24h"], 24)

        cycles_6h = metrics["cycles"]["windows"][1]
        self.assertEqual(cycles_6h["label"], "6h")
        self.assertEqual(cycles_6h["cycles"], 2)
        self.assertAlmostEqual(cycles_6h["median_cycle_s"], 200.0)
        self.assertEqual(cycles_6h["max_cycle_s"], 300.0)
        self.assertEqual(cycles_6h["watchdog_kills"], 1)
        self.assertEqual(cycles_6h["find_download_queued"], 8)
        self.assertEqual(cycles_6h["find_download_completed"], 7)

        coverage = metrics["coverage"]
        self.assertEqual(coverage["wanted_total"], 4)
        self.assertEqual(coverage["wanted_searched_24h"], 3)
        self.assertEqual(coverage["wanted_unsearched_24h"], 1)
        self.assertEqual(coverage["wanted_never_searched"], 1)
        self.assertEqual(coverage["active_wanted_searches_24h"], 6)
        self.assertEqual(coverage["matches_24h"], 2)
        self.assertEqual(coverage["matches_6h"], 2)
        self.assertAlmostEqual(coverage["matches_per_hour_24h"], 2 / 24)
        self.assertAlmostEqual(coverage["matches_per_hour_6h"], 2 / 6)
        trend = coverage["wanted_trend"]
        self.assertEqual(trend["current_wanted"], 4)
        self.assertEqual([w["label"] for w in trend["windows"]],
                         ["6h", "24h", "7d"])
        trend_6h = trend["windows"][0]
        self.assertEqual(trend_6h["start_wanted"], 5)
        self.assertEqual(trend_6h["end_wanted"], 4)
        self.assertEqual(trend_6h["delta"], -1)
        self.assertEqual(trend_6h["trend"], "down")
        self.assertGreater(trend_6h["drain_per_hour"], 0)
        self.assertIsNotNone(trend_6h["eta_hours"])
        self.assertGreaterEqual(len(trend["series_24h"]), 4)
        self.assertEqual(
            set(trend["series_24h"][0]),
            {"sampled_at", "wanted_total"},
        )
        self.assertEqual(len(coverage["match_rate_series_24h"]), 24)
        self.assertEqual(
            sum(point["matches"] for point in coverage["match_rate_series_24h"]),
            2,
        )
        self.assertEqual(
            set(coverage["match_rate_series_24h"][0]),
            {"bucket_start", "matches", "matches_per_hour"},
        )
        self.assertEqual(len(coverage["match_rate_series_28d"]), 28)
        self.assertEqual(
            sum(point["matches"] for point in coverage["match_rate_series_28d"]),
            2,
        )
        self.assertEqual(
            set(coverage["match_rate_series_28d"][0]),
            {"bucket_start", "matches", "matches_per_day"},
        )
        self.assertEqual(coverage["top_loop_suspects"][0]["request_id"], self.req1)
        self.assertEqual(coverage["top_loop_suspects"][0]["searches_24h"], 4)
        self.assertEqual(coverage["top_loop_suspects"][0]["reset_24h"], 1)
        self.assertEqual(coverage["top_loop_suspects"][0]["problem_24h"], 1)
        self.assertIn(
            self.req4,
            [row["request_id"] for row in coverage["top_loop_suspects"]],
        )
        self.assertEqual(coverage["stale_wanted"][0]["request_id"], self.req3)
        self.assertIn(
            "downloading",
            [row["status"] for row in coverage["stale_wanted"]],
        )
        heavy = metrics["peers"]["heavy_queries"]
        self.assertEqual(heavy[0]["request_id"], self.req1)
        self.assertEqual(heavy[0]["mb_release_id"], "dash-1")
        self.assertEqual(heavy[0]["query"], "loop b")
        self.assertEqual(heavy[0]["variant"], "track_0")
        self.assertEqual(heavy[0]["result_count"], 500)
        self.assertEqual(heavy[0]["peer_dirs"], 35)
        self.assertEqual(heavy[0]["fanout_waves"], 3)
        self.assertEqual(heavy[0]["browse_time_s"], 12.5)
        self.assertEqual(metrics["cycles"]["outliers"][0]["cycle_total_s"], 900.0)

    def test_dashboard_unfindable_block_empty_then_populated(self):
        """The dashboard's ``unfindable`` block renders sanely with no
        rows (honest empty state, #1112) and surfaces recent runs +
        backlog trend newest-first once runs exist."""
        empty_metrics = self.db.get_pipeline_dashboard_metrics()
        empty_unfindable = empty_metrics["unfindable"]
        self.assertEqual(empty_unfindable["recent_runs"], [])
        self.assertEqual(empty_unfindable["backlog_trend"], {
            "current_backlog": None,
            "latest_sample_at": None,
            "series": [],
        })

        self.db.record_unfindable_run_metrics(
            cohort_total=1301, due_backlog_at_start=900,
            batch_limit=240, candidates_processed=240, probes_attempted=240,
            categorised_count=5, downgraded_count=1, no_change_count=210,
            probe_failed_count=24, breaker_tripped=False,
            duration_seconds=6900.0,
        )
        self.db.record_unfindable_run_metrics(
            cohort_total=1301, due_backlog_at_start=686,
            batch_limit=240, candidates_processed=90, probes_attempted=90,
            probe_failed_count=90, breaker_tripped=True,
            duration_seconds=1800.0,
        )

        metrics = self.db.get_pipeline_dashboard_metrics()
        unfindable = metrics["unfindable"]
        recent = unfindable["recent_runs"]
        self.assertEqual(len(recent), 2)
        # Newest first.
        self.assertEqual(recent[0]["due_backlog_at_start"], 686)
        self.assertTrue(recent[0]["breaker_tripped"])
        self.assertEqual(recent[1]["due_backlog_at_start"], 900)
        self.assertFalse(recent[1]["breaker_tripped"])
        self.assertIsInstance(recent[0]["created_at"], str)

        trend = unfindable["backlog_trend"]
        self.assertEqual(trend["current_backlog"], 686)
        self.assertEqual(trend["latest_sample_at"], recent[0]["created_at"])
        # Chronological (oldest first) — the inverse of recent_runs.
        self.assertEqual(
            [pt["due_backlog_at_start"] for pt in trend["series"]],
            [900, 686],
        )
        self.assertEqual(
            [pt["candidates_processed"] for pt in trend["series"]],
            [240, 90],
        )

    def test_cycle_rows_select_recent_and_24_hour_slowest_cycles(self):
        now = datetime.now(UTC)
        newest_id = self.db.record_cycle_metrics(
            started_at=now - timedelta(hours=1, seconds=25),
            completed_at=now - timedelta(hours=1),
            cycle_total_s=25.0,
        )
        newest_tie_id = self.db.record_cycle_metrics(
            started_at=now - timedelta(hours=1, seconds=10),
            completed_at=now - timedelta(hours=1),
            cycle_total_s=10.0,
        )
        slower_id = self.db.record_cycle_metrics(
            started_at=now - timedelta(hours=2, seconds=100),
            completed_at=now - timedelta(hours=2),
            cycle_total_s=100.0,
        )
        slower_tie_id = self.db.record_cycle_metrics(
            started_at=now - timedelta(hours=3, seconds=100),
            completed_at=now - timedelta(hours=3),
            cycle_total_s=100.0,
        )
        stale_id = self.db.record_cycle_metrics(
            started_at=now - timedelta(hours=25, seconds=500),
            completed_at=now - timedelta(hours=25),
            cycle_total_s=500.0,
        )

        recent = self.db._dashboard_cycle_rows(outliers=False, limit=5)
        outliers = self.db._dashboard_cycle_rows(outliers=True, limit=8)

        self.assertEqual(
            [row["id"] for row in recent],
            [newest_tie_id, newest_id, slower_id, slower_tie_id, stale_id],
        )
        self.assertEqual(
            [row["id"] for row in outliers],
            [slower_tie_id, slower_id, newest_id, newest_tie_id],
        )

    def test_peer_observations_track_distinct_peers(self):
        now = datetime.now(UTC)
        old = now - timedelta(days=2)

        inserted = self.db.record_peer_observations(
            ["user1", "user1", "user2"],
            observed_at=old,
        )
        self.assertEqual(inserted, 2)

        inserted = self.db.record_peer_observations(
            ["user1", "user3"],
            observed_at=now,
        )
        self.assertEqual(inserted, 1)

        peers = self.db.get_peer_metrics(days=14)
        self.assertEqual(peers["totals"]["known_peers"], 3)
        self.assertEqual(peers["totals"]["new_24h"], 1)
        self.assertEqual(
            sum(day["new_peers"] for day in peers["days"]),
            3,
        )


@requires_postgres
class TestPeerObservations(unittest.TestCase):
    """Distinct-peer roster (#227): ``record_peer_observations`` upserts
    one row per hashed username; ``get_peer_metrics`` computes totals and
    the per-day growth curve live (the table is small enough forever)."""

    _TOTALS_KEYS: ClassVar = {"known_peers", "new_24h", "seen_24h", "tracked_since"}
    _DAY_KEYS: ClassVar = {"date", "new_peers", "total_peers"}

    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_record_round_trip_preserves_first_and_last_seen(self):
        """Rule A: every field written must read back through PG. A
        re-observation must advance last_seen_at but never first_seen_at."""
        first = datetime.now(UTC) - timedelta(days=3)
        later = datetime.now(UTC) - timedelta(hours=1)

        self.assertEqual(
            self.db.record_peer_observations(["alice"], observed_at=first), 1)
        self.assertEqual(
            self.db.record_peer_observations(["alice"], observed_at=later), 0)

        row = self.db._execute(
            "SELECT username_hash, first_seen_at, last_seen_at "
            "FROM peer_observations").fetchone()
        assert row is not None
        self.assertEqual(len(row["username_hash"]), 64)
        self.assertEqual(row["first_seen_at"], first)
        self.assertEqual(row["last_seen_at"], later)

    def test_record_ignores_empty_usernames(self):
        self.assertEqual(self.db.record_peer_observations([""]), 0)
        self.assertEqual(self.db.record_peer_observations([]), 0)

    def test_record_stale_observation_never_regresses_last_seen(self):
        now = datetime.now(UTC)
        earlier = now - timedelta(days=1)
        self.db.record_peer_observations(["bob"], observed_at=now)
        self.db.record_peer_observations(["bob"], observed_at=earlier)
        row = self.db._execute(
            "SELECT last_seen_at FROM peer_observations").fetchone()
        assert row is not None
        self.assertEqual(row["last_seen_at"], now)

    def test_metrics_shape_and_cumulative_totals(self):
        """Response shape is pinned; ``total_peers`` is the cumulative
        distinct-peer count at end of each day, carried forward across
        days with no new peers."""
        now = datetime.now(UTC)
        self.db.record_peer_observations(
            ["old1", "old2"], observed_at=now - timedelta(days=5))
        self.db.record_peer_observations(
            ["new1"], observed_at=now)

        resp = self.db.get_peer_metrics(days=14)
        self.assertEqual(set(resp.keys()), {"days", "totals"})
        self.assertEqual(set(resp["totals"].keys()), self._TOTALS_KEYS)
        self.assertEqual(resp["totals"]["known_peers"], 3)
        self.assertEqual(resp["totals"]["new_24h"], 1)
        self.assertEqual(resp["totals"]["seen_24h"], 1)
        self.assertIsInstance(resp["totals"]["tracked_since"], str)

        self.assertEqual(len(resp["days"]), 14)
        for day in resp["days"]:
            self.assertEqual(set(day.keys()), self._DAY_KEYS)
            self.assertIsInstance(day["date"], str)
            self.assertIsInstance(day["new_peers"], int)
            self.assertIsInstance(day["total_peers"], int)
        # Days are ordered DESC (today first); cumulative total today
        # covers all three peers and carries forward over zero-days.
        self.assertEqual(resp["days"][0]["total_peers"], 3)
        self.assertEqual(resp["days"][1]["total_peers"], 2)
        self.assertEqual(resp["days"][0]["new_peers"], 1)
        self.assertEqual(
            sum(day["new_peers"] for day in resp["days"]), 3)

    def test_metrics_cumulative_includes_peers_older_than_window(self):
        """A peer first seen before the day window still counts toward
        every day's running total."""
        now = datetime.now(UTC)
        self.db.record_peer_observations(
            ["ancient"], observed_at=now - timedelta(days=60))
        self.db.record_peer_observations(["fresh"], observed_at=now)

        resp = self.db.get_peer_metrics(days=14)
        self.assertEqual(resp["days"][0]["total_peers"], 2)
        self.assertEqual(resp["days"][-1]["total_peers"], 1)
        self.assertEqual(
            sum(day["new_peers"] for day in resp["days"]), 1)

    def test_metrics_empty_table(self):
        resp = self.db.get_peer_metrics(days=14)
        self.assertEqual(resp["totals"]["known_peers"], 0)
        self.assertIsNone(resp["totals"]["tracked_since"])
        self.assertEqual(len(resp["days"]), 14)
        self.assertTrue(
            all(d["new_peers"] == 0 and d["total_peers"] == 0
                for d in resp["days"]))


@requires_postgres
class TestSearchPlanReadiness(unittest.TestCase):
    """U7: ``get_search_plan_readiness`` aggregates wanted rows into
    plan-readiness buckets that replace exhausted-based reporting.

    The classifier must be exhaustive and exclusive: every wanted row
    belongs to exactly one bucket, the buckets sum to ``wanted_total``,
    and ``wanted_no_plan > 0`` is the operator stop-the-deploy signal.
    """

    def setUp(self):
        from lib.pipeline_db import SearchPlanItemInput
        self.SearchPlanItemInput = SearchPlanItemInput
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def _add_wanted(self, suffix: str) -> int:
        return self.db.add_request(
            mb_release_id=f"plan-readiness-{suffix}",
            artist_name="Readiness", album_title=suffix,
            source="request",
        )

    def _items(self, *queries: str):
        return [
            self.SearchPlanItemInput(
                ordinal=i, strategy=f"slot_{i}", query=q,
                canonical_query_key=q.lower(),
            )
            for i, q in enumerate(queries)
        ]

    def test_empty_db_returns_zeroed_buckets(self):
        readiness = self.db.get_search_plan_readiness("g1")
        self.assertEqual(readiness, {
            "generator_id": "g1",
            "wanted_total": 0,
            "wanted_searchable": 0,
            "wanted_legacy": 0,
            "wanted_failed_deterministic": 0,
            "wanted_failed_transient": 0,
            "wanted_no_plan": 0,
        })

    def test_buckets_partition_wanted_total(self):
        # 1 searchable, 1 legacy, 1 deterministic-failed, 1 transient,
        # 1 no-plan.
        rid_search = self._add_wanted("searchable")
        rid_legacy = self._add_wanted("legacy")
        rid_det = self._add_wanted("det_failed")
        rid_trans = self._add_wanted("trans_failed")
        self._add_wanted("no_plan")
        # Searchable: active plan with current generator.
        self.db.create_successful_search_plan(
            request_id=rid_search, generator_id="g1",
            items=self._items("query A"))
        # Legacy: active plan with old generator id.
        self.db.create_successful_search_plan(
            request_id=rid_legacy, generator_id="g0_old",
            items=self._items("query B"))
        # Deterministic-failed on current generator.
        self.db.create_failed_search_plan(
            request_id=rid_det, generator_id="g1",
            failure_class="no_runnable_query",
            error_message="empty",
            transient=False,
        )
        # Transient-failed on current generator.
        self.db.create_failed_search_plan(
            request_id=rid_trans, generator_id="g1",
            failure_class="resolver_unavailable",
            error_message="resolver down",
            transient=True,
        )
        # rid_noplan has no plans at all.

        readiness = self.db.get_search_plan_readiness("g1")
        self.assertEqual(readiness["wanted_total"], 5)
        self.assertEqual(readiness["wanted_searchable"], 1)
        self.assertEqual(readiness["wanted_legacy"], 1)
        self.assertEqual(readiness["wanted_failed_deterministic"], 1)
        self.assertEqual(readiness["wanted_failed_transient"], 1)
        self.assertEqual(readiness["wanted_no_plan"], 1)
        # Sum invariant.
        self.assertEqual(
            readiness["wanted_total"],
            sum(readiness[k] for k in (
                "wanted_searchable", "wanted_legacy",
                "wanted_failed_deterministic", "wanted_failed_transient",
                "wanted_no_plan")))

    def test_old_generator_failed_plan_falls_to_no_plan(self):
        """A failed plan on a *different* generator id does not satisfy
        the readiness check for the current id -- treat it the same as
        having no plan at all (startup reconciliation will retry)."""
        rid = self._add_wanted("old_generator_failure")
        self.db.create_failed_search_plan(
            request_id=rid, generator_id="g0_old",
            failure_class="no_runnable_query",
            error_message="historical",
            transient=False,
        )
        readiness = self.db.get_search_plan_readiness("g1")
        self.assertEqual(readiness["wanted_total"], 1)
        self.assertEqual(readiness["wanted_no_plan"], 1)
        self.assertEqual(readiness["wanted_failed_deterministic"], 0)


class _ReadinessSeedDB(Protocol):
    """Exactly the three writes ``_seed_readiness_worlds`` performs.

    Both ``PipelineDB`` and ``FakePipelineDB`` satisfy it structurally,
    which is what lets one seeding function build the same world in each.
    """

    def add_request(
        self,
        *,
        artist_name: str,
        album_title: str,
        source: str,
        mb_release_id: str | None = None,
    ) -> int: ...

    def create_successful_search_plan(
        self,
        *,
        request_id: int,
        generator_id: str,
        items: list[SearchPlanItemInput],
    ) -> int: ...

    def create_failed_search_plan(
        self,
        *,
        request_id: int,
        generator_id: str,
        failure_class: str,
        error_message: str | None = None,
        transient: bool,
    ) -> int: ...


def _seed_readiness_worlds(
    db: _ReadinessSeedDB, *, current: str, old: str,
) -> None:
    """Build one wanted request per readiness bucket in ``db``.

    ``db`` is either a real ``PipelineDB`` or a ``FakePipelineDB`` — the
    calls are identical, which is the point: the two must agree on how
    they bucket the same world.
    """

    def _items(query: str) -> list[SearchPlanItemInput]:
        return [SearchPlanItemInput(
            ordinal=0, strategy="slot_0", query=query,
            canonical_query_key=query.lower(),
        )]

    def _wanted(suffix: str) -> int:
        return db.add_request(
            mb_release_id=f"readiness-parity-{suffix}",
            artist_name="Parity", album_title=suffix, source="request",
        )

    db.create_successful_search_plan(
        request_id=_wanted("searchable"), generator_id=current,
        items=_items("query A"),
    )
    # Two searchable rows, so a bucket that silently collapsed to a count
    # of one per bucket could not pass.
    db.create_successful_search_plan(
        request_id=_wanted("searchable-2"), generator_id=current,
        items=_items("query B"),
    )
    db.create_successful_search_plan(
        request_id=_wanted("legacy"), generator_id=old,
        items=_items("query C"),
    )
    db.create_failed_search_plan(
        request_id=_wanted("deterministic"), generator_id=current,
        failure_class="no_runnable_query", error_message="empty",
        transient=False,
    )
    db.create_failed_search_plan(
        request_id=_wanted("transient"), generator_id=current,
        failure_class="resolver_unavailable", error_message="down",
        transient=True,
    )
    # Deterministic must outrank transient on the same request.
    both = _wanted("both-failures")
    db.create_failed_search_plan(
        request_id=both, generator_id=current,
        failure_class="resolver_unavailable", error_message="down",
        transient=True,
    )
    db.create_failed_search_plan(
        request_id=both, generator_id=current,
        failure_class="no_runnable_query", error_message="empty",
        transient=False,
    )
    # A failure recorded under an older generator id proves nothing about
    # the current one.
    db.create_failed_search_plan(
        request_id=_wanted("old-generator-failure"), generator_id=old,
        failure_class="no_runnable_query", error_message="historical",
        transient=False,
    )
    _wanted("no-plan")


@requires_postgres
class TestPlanReadinessParity(unittest.TestCase):
    """Real PostgreSQL and the in-memory twin bucket the same world alike.

    Issue #1278 item 7: the twin used to restate the five-bucket
    precedence in Python beside production's SQL CASE ladder. It now calls
    ``decisions.classify_plan_readiness_bucket``; this pins that the CASE
    ladder still agrees with it over a world covering every bucket.
    """

    def setUp(self):
        self.db = make_db()
        self.fake = FakePipelineDB()

    def tearDown(self):
        self.db.close()

    def test_real_and_fake_readiness_agree_across_every_bucket(self):
        for db in (self.db, self.fake):
            _seed_readiness_worlds(db, current="gen-current", old="gen-old")
        live = self.db.get_search_plan_readiness("gen-current")
        twin = self.fake.get_search_plan_readiness("gen-current")
        self.assertEqual(live, twin)
        # The world is not degenerate: every bucket is populated, so an
        # all-zeros agreement cannot pass this test.
        self.assertEqual(live, {
            "generator_id": "gen-current",
            "wanted_total": 8,
            "wanted_searchable": 2,
            "wanted_legacy": 1,
            "wanted_failed_deterministic": 2,
            "wanted_failed_transient": 1,
            "wanted_no_plan": 2,
        })


@requires_postgres
class TestSearchBackoffSqlParity(unittest.TestCase):
    """Both SQL retry-pacing writers agree with ``search_backoff_minutes``.

    The Python callers now share one formula; these two adapters keep
    their own ``LEAST(base * POWER(2, LEAST(counter, cap)), max)``
    expression inside the atomic UPDATE that increments the counter. The
    domain deliberately crosses 1024: PostgreSQL resolves ``POWER`` to
    ``double precision``, and before issue #1278 item 7 clamped the
    exponent, ``POWER(2, 1024)`` raised ``value out of range: overflow``
    instead of being capped. The upper bound stops at 4096 because that is
    already an order of magnitude past the live worst counter (407,
    measured 2026-08-31) — nothing production can reach is excluded.
    """

    #: Prior-attempt counts the pin drives end to end.
    PRIORS = (0, 1, 2, 3, 4, 10, 407, 1022, 1023, 1024, 1025, 4096)

    def setUp(self):
        self.db = make_db()
        self.request_id = self.db.add_request(
            mb_release_id="backoff-sql-parity",
            artist_name="Backoff", album_title="Parity", source="request",
        )

    def tearDown(self):
        self.db.close()

    def _seed_counter(self, column: str, prior: int) -> None:
        self.db._execute(
            f"UPDATE album_requests SET {column} = %s WHERE id = %s",
            (prior, self.request_id),
        )

    def _observed_backoff_minutes(self) -> int:
        cur = self.db._execute(
            "SELECT last_attempt_at, next_retry_after "
            "FROM album_requests WHERE id = %s",
            (self.request_id,),
        )
        row = cur.fetchone()
        assert row is not None
        delta = row["next_retry_after"] - row["last_attempt_at"]
        return round(delta.total_seconds() / 60)

    def test_request_record_attempt_matches_the_shared_formula(self):
        for attempt_type in ("search", "download", "validation"):
            for prior in self.PRIORS:
                with self.subTest(attempt_type=attempt_type, prior=prior):
                    self._seed_counter(f"{attempt_type}_attempts", prior)
                    self.assertTrue(self.db.record_attempt(
                        self.request_id, attempt_type,
                        expected_status="wanted",
                    ))
                    self.assertEqual(
                        self._observed_backoff_minutes(),
                        search_backoff_minutes(prior),
                    )

    def test_terminal_record_attempt_matches_the_shared_formula(self):
        boundaries: list[str] = []
        transitions_db = _TransactionalTransitionsDB(
            self.db, boundaries.append,
        )
        for prior in self.PRIORS:
            with self.subTest(prior=prior):
                self._seed_counter("validation_attempts", prior)
                self.assertTrue(transitions_db.record_attempt(
                    self.request_id, "validation",
                    expected_status="wanted",
                ))
                self.assertEqual(
                    self._observed_backoff_minutes(),
                    search_backoff_minutes(prior),
                )
        self.assertEqual(
            set(boundaries), {"request.attempt.validation"},
        )


@requires_postgres
class TestSharedOutcomeVocabularies(unittest.TestCase):
    """Every exported outcome vocabulary matches the SQL it describes.

    Issue #1278 item 7 exported six vocabulary tuples so the in-memory twin
    stops hand-copying them. The SQL keeps its own literals, so each tuple
    is bound to its query by round-tripping a vocabulary through real
    PostgreSQL and asserting the query admits exactly the members — a drift
    in either direction fails here.

    The four ``download_log.outcome`` tuples are driven against the WHOLE
    canonical taxonomy (``get_args(DownloadLogOutcome)``, itself pinned to
    the CHECK constraint by ``tests/test_migrator.py``), the job-type
    tuple against the whole ``IMPORT_JOB_TYPES`` frozenset, and the
    ``search_log.outcome`` case against the whole ``SEARCH_LOG_OUTCOMES``
    frozenset (pinned to migration 010's CHECK by the same migrator
    test module — the #1278 item-7 residual sweep replaced the
    hand-written probe list this test used to carry).
    """

    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def _request(self, suffix: str) -> int:
        return self.db.add_request(
            mb_release_id=f"vocabulary-{suffix}",
            artist_name="Vocabulary", album_title=suffix, source="request",
        )

    def _captured(self, suffix: str) -> bool:
        overlay = self.db.get_pipeline_overlay([f"vocabulary-{suffix}"])
        return bool(overlay[f"vocabulary-{suffix}"]["has_captured_history"])

    def test_log_filter_outcomes_match_get_log(self):
        by_outcome: dict[str, int] = {}
        for outcome in sorted(_ALL_DOWNLOAD_LOG_OUTCOMES):
            request_id = self._request(f"log-{outcome}")
            by_outcome[outcome] = self.db.log_download(
                request_id=request_id, outcome=outcome,
            )
        for name, vocabulary in (
            ("imported", LOG_FILTER_IMPORTED_OUTCOMES),
            ("rejected", LOG_FILTER_REJECTED_OUTCOMES),
        ):
            with self.subTest(filter=name):
                rows = self.db.get_log(limit=200, outcome_filter=name)
                self.assertEqual(
                    {str(row["outcome"]) for row in rows}, set(vocabulary),
                )

    def test_linked_import_outcomes_match_the_successor_query(self):
        origin_request = self._request("linked-origin")
        origin_id = self.db.log_download(
            request_id=origin_request, outcome="rejected",
        )
        for outcome in sorted(_ALL_DOWNLOAD_LOG_OUTCOMES):
            self.db.log_download(
                request_id=self._request(f"linked-{outcome}"),
                outcome=outcome,
                source_download_log_id=origin_id,
            )
        rows = self.db.get_linked_import_logs([origin_id])
        self.assertEqual(
            {str(row["outcome"]) for row in rows},
            set(LINKED_IMPORT_OUTCOMES),
        )

    def test_capture_download_outcomes_match_has_captured_history(self):
        admitted: set[str] = set()
        for outcome in sorted(_ALL_DOWNLOAD_LOG_OUTCOMES):
            suffix = f"capture-{outcome}"
            self.db.log_download(
                request_id=self._request(suffix),
                outcome=outcome,
            )
            if self._captured(suffix):
                admitted.add(outcome)
        self.assertEqual(admitted, set(CAPTURE_DOWNLOAD_OUTCOMES))

    def test_capture_job_types_match_has_captured_history(self):
        admitted: set[str] = set()
        for job_type in sorted(IMPORT_JOB_TYPES):
            suffix = f"capture-job-{job_type}"
            self.db._execute(
                "INSERT INTO import_jobs "
                "(request_id, job_type, status, payload) "
                "VALUES (%s, %s, 'completed', '{}'::jsonb)",
                (self._request(suffix), job_type),
            )
            if self._captured(suffix):
                admitted.add(job_type)
        self.assertEqual(admitted, set(CAPTURE_IMPORT_JOB_TYPES))

    def test_search_error_outcomes_match_both_dashboard_panels(self):
        """Both panels count exactly ``SEARCH_ERROR_OUTCOMES`` as errors.

        The probe list is the WHOLE canonical taxonomy —
        ``SEARCH_LOG_OUTCOMES``, itself pinned to migration 010's
        ``search_log_outcome_check`` by ``tests/test_migrator.py`` — so a
        migration widening the vocabulary flows into this probe
        automatically and a new outcome the panels miscount fails here.
        (Before the #1278 item-7 residual sweep this list was hand-written
        and only as complete as the hand that wrote it.)
        """
        self.assertLessEqual(
            set(SEARCH_ERROR_OUTCOMES), SEARCH_LOG_OUTCOMES,
            "the error subset must stay inside the canonical taxonomy")
        request_id = self._request("search-errors")
        for outcome in sorted(SEARCH_LOG_OUTCOMES):
            self.db.log_search(request_id, query="q", outcome=outcome)
        window = self.db._dashboard_search_window("24h", 24)
        self.assertEqual(
            window["outcomes"]["errors"], len(SEARCH_ERROR_OUTCOMES),
        )
        problem = {
            int(row["request_id"]): int(row["problem_24h"])
            for row in self.db._dashboard_loop_suspects()
        }
        self.assertEqual(
            problem.get(request_id), len(SEARCH_ERROR_OUTCOMES),
        )


@requires_postgres
class TestDashboardWrapMetrics(unittest.TestCase):
    """U7: ``cursor_update_status='wrapped'`` rows replace ``outcome=
    'exhausted'`` as the cycle-wrap signal in dashboard search windows.
    Historical exhausted rows still appear in the existing
    ``outcomes.exhausted`` bucket so legacy reporting does not lie about
    pre-cutover history.
    """

    def setUp(self):
        from lib.pipeline_db import (
            ConsumedAttemptInput,
            SearchPlanItemInput,
        )
        self.SearchPlanItemInput = SearchPlanItemInput
        self.ConsumedAttemptInput = ConsumedAttemptInput
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="wrap-mbid",
            artist_name="Wrap", album_title="Test",
            source="request",
        )
        items = [
            SearchPlanItemInput(
                ordinal=i, strategy=f"slot_{i}", query=f"q{i}",
                canonical_query_key=f"k{i}",
            )
            for i in range(2)
        ]
        self.plan_id = self.db.create_successful_search_plan(
            request_id=self.req_id, generator_id="g1",
            items=items, set_active=True,
        )
        plan_items = self.db._execute(
            "SELECT id, ordinal FROM search_plan_items "
            "WHERE plan_id = %s ORDER BY ordinal", (self.plan_id,)
        ).fetchall()
        self.plan_items = [dict(r) for r in plan_items]

    def tearDown(self):
        self.db.close()

    def _consume(self, ordinal: int, outcome: str) -> None:
        item = self.plan_items[ordinal]
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.db.record_consumed_search_attempt(self.ConsumedAttemptInput(
            request_id=self.req_id,
            plan_id=self.plan_id,
            plan_item_id=item["id"],
            plan_ordinal=ordinal,
            plan_strategy=f"slot_{ordinal}",
            plan_canonical_query_key=f"k{ordinal}",
            plan_repeat_group=None,
            plan_generator_id="g1",
            query=f"q{ordinal}",
            outcome=outcome,
            plan_item_count=len(self.plan_items),
            cycle_count_snapshot=int(req["plan_cycle_count"]),
            elapsed_s=1.0, result_count=0,
            apply_scheduler_attempt=True,
            scheduler_success=False,
        ))

    def test_wrap_count_increments_per_cycle_wrap(self):
        # Walk through the plan twice -- two wraps expected.
        for _ in range(2):
            for ordinal in range(len(self.plan_items)):
                self._consume(ordinal, "no_results")

        metrics = self.db.get_pipeline_dashboard_metrics(plan_generator_id="g1")
        windows = metrics["searches"]["windows"]
        self.assertTrue(windows)
        window_24h = next(w for w in windows if w["label"] == "24h")
        self.assertEqual(window_24h["cursor_wraps"], 2)
        # Sanity: no new exhausted rows after the cutover.
        self.assertEqual(window_24h["outcomes"]["exhausted"], 0)
        # Cache attribution stays cycle-only -- ``search_log`` has no
        # per-search cache columns.
        self.assertEqual(window_24h["cache_attribution_level"], "cycle_only")

    def test_historical_exhausted_rows_still_counted(self):
        """Pre-cutover ``outcome='exhausted'`` rows must remain visible in
        the existing search-window bucket. The dashboard does not strip
        them out; it only stops emitting new ones."""
        self.db.log_search(
            self.req_id, query="historical-exhausted",
            elapsed_s=0.0, outcome="exhausted",
        )
        metrics = self.db.get_pipeline_dashboard_metrics(plan_generator_id="g1")
        window_24h = next(
            w for w in metrics["searches"]["windows"] if w["label"] == "24h"
        )
        self.assertEqual(window_24h["outcomes"]["exhausted"], 1)
        # No wraps yet -- historical exhausted is not a wrap.
        self.assertEqual(window_24h["cursor_wraps"], 0)


@requires_postgres
class TestDenylist(unittest.TestCase):
    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="deny-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_add_and_get_denylist(self):
        self.db.add_denylist(self.req_id, "bad_user", "low bitrate")
        denied = self.db.get_denylisted_users(self.req_id)
        self.assertEqual(len(denied), 1)
        self.assertEqual(denied[0]["username"], "bad_user")
        self.assertEqual(denied[0]["reason"], "low bitrate")

    def test_multiple_denied_users(self):
        self.db.add_denylist(self.req_id, "user1", "bad quality")
        self.db.add_denylist(self.req_id, "user2", "incomplete")
        denied = self.db.get_denylisted_users(self.req_id)
        usernames = {d["username"] for d in denied}
        self.assertEqual(usernames, {"user1", "user2"})

    def test_duplicate_denylist_ignored(self):
        self.db.add_denylist(self.req_id, "user1", "reason1")
        self.db.add_denylist(self.req_id, "user1", "reason2")
        denied = self.db.get_denylisted_users(self.req_id)
        self.assertEqual(len(denied), 1)

    def test_world_audit_read_lists_rows_across_requests(self):
        second_id = self.db.add_request(
            mb_release_id="deny-uuid-2",
            artist_name="B",
            album_title="B",
            source="request",
        )
        self.db.add_denylist(self.req_id, "user-a", "reason-a")
        self.db.add_denylist(second_id, "user-b", "reason-b")

        rows = self.db.list_denylist_rows()

        self.assertEqual(
            [(row["request_id"], row["username"]) for row in rows],
            [(self.req_id, "user-a"), (second_id, "user-b")],
        )


@requires_postgres
class TestRetryLogic(unittest.TestCase):
    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="retry-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_record_attempt_increments_counters(self):
        self.db.record_attempt(self.req_id, "search", expected_status="wanted")
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["search_attempts"], 1)

        self.db.record_attempt(self.req_id, "search", expected_status="wanted")
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["search_attempts"], 2)

    def test_record_attempt_accepts_exact_counter_types(self):
        for attempt_type, counter in (
            ("search", "search_attempts"),
            ("download", "download_attempts"),
            ("validation", "validation_attempts"),
        ):
            with self.subTest(attempt_type=attempt_type):
                self.assertTrue(self.db.record_attempt(
                    self.req_id, attempt_type, expected_status="wanted"))
                req = self.db.get_request(self.req_id)
                assert req is not None
                self.assertEqual(req[counter], 1)

    def test_record_attempt_sets_backoff(self):
        self.db.record_attempt(self.req_id, "download", expected_status="wanted")
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["download_attempts"], 1)
        self.assertIsNotNone(req["last_attempt_at"])
        next_retry = req["next_retry_after"]
        assert next_retry is not None
        self.assertGreater(next_retry, datetime.now(UTC))

    def test_record_attempt_rejects_unknown_type_before_updating_request(self):
        with self.assertRaises(ValueError):
            self.db.record_attempt(
                self.req_id,
                "search; UPDATE album_requests SET search_attempts = 99",
                expected_status="wanted",
            )

        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["search_attempts"], 0)
        self.assertEqual(req["download_attempts"], 0)
        self.assertEqual(req["validation_attempts"], 0)

    def test_record_attempt_rejects_processing_owner_even_when_status_matches(self):
        handoff_automation_owner(self.db, self.req_id)
        before = self.db.get_request(self.req_id)
        assert before is not None

        self.assertFalse(self.db.record_attempt(
            self.req_id,
            "download",
            expected_status="processing",
        ))

        self.assertEqual(self.db.get_request(self.req_id), before)

    def test_exponential_backoff(self):
        self.db.record_attempt(self.req_id, "search", expected_status="wanted")
        req1 = self.db.get_request(self.req_id)
        assert req1 is not None
        retry1 = req1["next_retry_after"]
        assert retry1 is not None

        self.db.record_attempt(self.req_id, "search", expected_status="wanted")
        req2 = self.db.get_request(self.req_id)
        assert req2 is not None
        retry2 = req2["next_retry_after"]
        assert retry2 is not None

        now = datetime.now(UTC)
        delta1 = (retry1 - now).total_seconds()
        delta2 = (retry2 - now).total_seconds()
        self.assertGreater(delta2, delta1)

    def test_backoff_caps_at_four_hours(self):
        # BACKOFF_MAX_MINUTES = 60 * 4 per lib/pipeline_db.py (was 6h
        # until commit 1d84037 lowered it to raise steady-state search
        # frequency from ~4 to ~6 searches/release/day).
        for _ in range(6):
            self.db.record_attempt(self.req_id, "search", expected_status="wanted")

        req = self.db.get_request(self.req_id)
        assert req is not None
        retry_at = req["next_retry_after"]
        assert retry_at is not None

        delta = (retry_at - datetime.now(UTC)).total_seconds()
        self.assertLessEqual(delta, 4 * 60 * 60 + 5)
        self.assertGreater(delta, 3 * 60 * 60)


@requires_postgres
class TestSourcePreservation(unittest.TestCase):
    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_request_source_preserved(self):
        req_id = self.db.add_request(
            mb_release_id="req-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        self.db.update_status(req_id, "imported")
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["source"], "request")

    def test_redownload_source_preserved(self):
        req_id = self.db.add_request(
            mb_release_id="rd-uuid",
            artist_name="A",
            album_title="B",
            source="redownload",
        )
        self.db.update_status(req_id, "imported")
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["source"], "redownload")


@requires_postgres
class TestResetToWanted(unittest.TestCase):
    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def _make_request(self, suffix: str = "") -> int:
        req_id = self.db.add_request(
            mb_release_id=f"reset-{suffix}-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        self.db.update_status(req_id, "imported")
        return req_id

    def test_reset_to_wanted(self):
        req_id = self._make_request("basic")
        self.db.reset_to_wanted(req_id)
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["status"], "wanted")
        self.assertIsNone(req["next_retry_after"])
        self.assertEqual(req["search_attempts"], 0)
        self.assertEqual(req["download_attempts"], 0)
        self.assertEqual(req["validation_attempts"], 0)

    def test_reset_to_wanted_can_preserve_retry_counters(self):
        req_id = self._make_request("preserve-counters")
        self.db.record_attempt(req_id, "search", expected_status="imported")
        self.db.record_attempt(req_id, "download", expected_status="imported")
        self.db.record_attempt(req_id, "validation", expected_status="imported")
        before = self.db.get_request(req_id)
        assert before is not None
        before_retry = before["next_retry_after"]

        self.db.reset_to_wanted(req_id, clear_retry_counters=False)

        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["status"], "wanted")
        self.assertEqual(req["search_attempts"], 1)
        self.assertEqual(req["download_attempts"], 1)
        self.assertEqual(req["validation_attempts"], 1)
        self.assertEqual(req["next_retry_after"], before_retry)

    def test_reset_to_wanted_round_trips_priority_started_at(self):
        req_id = self._make_request("priority-window")
        priority_started_at = datetime(
            2026, 7, 20, 4, 0, tzinfo=UTC)

        applied = self.db.reset_to_wanted(
            req_id,
            expected_status="imported",
            priority_started_at=priority_started_at,
        )

        self.assertTrue(applied)
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["priority_started_at"], priority_started_at)

    def test_get_wanted_returns_attempted_and_untried_rows(self):
        attempted_id = self.db.add_request(
            mb_release_id="diagnostic-attempted",
            artist_name="Attempted",
            album_title="Attempted",
            source="request",
        )
        untried_id = self.db.add_request(
            mb_release_id="diagnostic-untried",
            artist_name="Untried",
            album_title="Untried",
            source="request",
        )
        self.db._execute(
            "UPDATE album_requests SET search_attempts = 5 WHERE id = %s",
            (attempted_id,),
        )
        self.db.conn.commit()

        wanted = self.db.get_wanted()

        self.assertEqual(
            {int(row["id"]) for row in wanted},
            {attempted_id, untried_id},
        )

    def test_preserves_search_filetype_override_when_omitted(self):
        req_id = self._make_request("preserve-qo")
        self.db.update_request_fields(req_id, search_filetype_override="flac,mp3 v0")
        self.db.reset_to_wanted(req_id)
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["search_filetype_override"], "flac,mp3 v0")

    def test_sets_search_filetype_override_when_passed(self):
        req_id = self._make_request("set-qo")
        self.db.update_request_fields(req_id, search_filetype_override="flac,mp3 v0,mp3 320")
        self.db.reset_to_wanted(req_id, search_filetype_override="flac,mp3 v0")
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["search_filetype_override"], "flac,mp3 v0")

    def test_clears_search_filetype_override_when_none(self):
        req_id = self._make_request("clear-qo")
        self.db.update_request_fields(req_id, search_filetype_override="flac")
        self.db.reset_to_wanted(req_id, search_filetype_override=None)
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertIsNone(req["search_filetype_override"])

    def test_preserves_min_bitrate_when_omitted(self):
        req_id = self._make_request("preserve-br")
        self.db.update_request_fields(req_id, min_bitrate=320)
        self.db.reset_to_wanted(req_id)
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["min_bitrate"], 320)

    def test_sets_min_bitrate_when_passed(self):
        req_id = self._make_request("set-br")
        self.db.update_request_fields(req_id, min_bitrate=192)
        self.db.reset_to_wanted(req_id, min_bitrate=320)
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["min_bitrate"], 320)
        self.assertEqual(req["prev_min_bitrate"], 192)

    def test_explicit_previous_bitrate_wins_over_derived_history(self):
        req_id = self._make_request("explicit-prev-br")
        self.db.update_request_fields(
            req_id,
            min_bitrate=192,
            prev_min_bitrate=128,
        )

        applied = self.db.reset_to_wanted(
            req_id,
            expected_status="imported",
            min_bitrate=320,
            prev_min_bitrate=256,
        )

        self.assertTrue(applied)
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["status"], "wanted")
        self.assertEqual(req["min_bitrate"], 320)
        self.assertEqual(req["prev_min_bitrate"], 256)

@requires_postgres
class TestClearOnDiskQualityFields(unittest.TestCase):
    """``clear_on_disk_quality_fields`` is the write-side half of the
    "beets is the source of truth" invariant: once an album leaves beets
    (ban-source, manual ``beet rm``), every ``album_requests`` field that
    describes on-disk state must be cleared. Preserves ``min_bitrate`` as
    a conservative baseline for the next quality-gate comparison, and
    leaves ``last_download_spectral_*`` alone (that's a download-attempt
    audit field, not on-disk state).
    """

    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def _make_request(self, suffix: str = "") -> int:
        req_id = self.db.add_request(
            mb_release_id=f"clear-od-{suffix}-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        self.db.update_status(req_id, "imported")
        return req_id

    def test_clears_spectral_and_verified_lossless(self):
        req_id = self._make_request("basic")
        self.db.update_request_fields(
            req_id,
            verified_lossless=True,
            current_spectral_grade="likely_transcode",
            current_spectral_bitrate=160,
            current_lossless_source_v0_probe_min_bitrate=165,
            current_lossless_source_v0_probe_avg_bitrate=171,
            current_lossless_source_v0_probe_median_bitrate=169,
        )

        self.db.clear_on_disk_quality_fields(req_id)

        req = self.db.get_request(req_id)
        assert req is not None
        self.assertFalse(req["verified_lossless"])
        self.assertIsNone(req["current_spectral_grade"])
        self.assertIsNone(req["current_spectral_bitrate"])
        self.assertIsNone(req["current_lossless_source_v0_probe_min_bitrate"])
        self.assertIsNone(req["current_lossless_source_v0_probe_avg_bitrate"])
        self.assertIsNone(req["current_lossless_source_v0_probe_median_bitrate"])

    def test_clears_current_evidence_link_but_preserves_audit_row(self):
        req_id = self._make_request("evidence")
        evidence = make_album_quality_evidence(
            mb_release_id="clear-od-evidence-uuid",
        )
        self.db.upsert_album_quality_evidence(evidence)
        persisted = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        self.db.set_request_current_evidence(req_id, persisted.id)

        self.db.clear_on_disk_quality_fields(req_id)

        self.assertIsNone(self.db.get_request_current_evidence_id(req_id))
        self.assertIsNotNone(
            self.db.load_album_quality_evidence_by_id(persisted.id),
            "unlinked content evidence remains immutable audit data",
        )

    def test_preserves_min_bitrate(self):
        """min_bitrate is a baseline for the NEXT gate, not on-disk state."""
        req_id = self._make_request("preserve-min")
        self.db.update_request_fields(req_id, min_bitrate=320)

        self.db.clear_on_disk_quality_fields(req_id)

        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["min_bitrate"], 320)

    def test_preserves_last_download_spectral(self):
        """last_download_* tracks the latest download attempt, not on-disk state."""
        req_id = self._make_request("preserve-ld")
        self.db.update_request_fields(
            req_id,
            last_download_spectral_grade="suspect",
            last_download_spectral_bitrate=192,
        )

        self.db.clear_on_disk_quality_fields(req_id)

        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["last_download_spectral_grade"], "suspect")
        self.assertEqual(req["last_download_spectral_bitrate"], 192)

    def test_idempotent_when_fields_already_clear(self):
        req_id = self._make_request("idempotent")

        self.db.clear_on_disk_quality_fields(req_id)
        self.db.clear_on_disk_quality_fields(req_id)

        req = self.db.get_request(req_id)
        assert req is not None
        self.assertFalse(req["verified_lossless"])
        self.assertIsNone(req["current_spectral_grade"])
        self.assertIsNone(req["current_spectral_bitrate"])

    def test_processing_owner_rejects_on_disk_quality_clear(self):
        req_id = self.db.add_request(
            mb_release_id="clear-od-processing-owner-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        self.db.update_request_fields(
            req_id,
            verified_lossless=True,
            current_spectral_grade="genuine",
            current_spectral_bitrate=245,
        )
        handoff_automation_owner(self.db, req_id)
        before = self.db.get_request(req_id)
        assert before is not None

        self.db.clear_on_disk_quality_fields(req_id)

        self.assertEqual(self.db.get_request(req_id), before)


@requires_postgres
class TestApplyTransitionDB(unittest.TestCase):
    """DB-backed contract tests for apply_transition preserve semantics."""

    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def _make_request(self, suffix: str = "", **extra: object) -> int:
        from lib.transitions import apply_transition
        req_id = self.db.add_request(
            mb_release_id=f"transition-{suffix}-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        if extra:
            self.db.update_request_fields(req_id, **extra)
        # Move to imported so we can transition to wanted
        apply_transition(self.db, req_id, "imported", from_status="wanted")
        return req_id

    def test_transition_to_wanted_preserves_override(self):
        from lib.transitions import apply_transition
        req_id = self._make_request("preserve", search_filetype_override="flac,mp3 v0")
        apply_transition(self.db, req_id, "wanted", from_status="imported")
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["search_filetype_override"], "flac,mp3 v0")

    def test_transition_to_wanted_with_narrowed_override(self):
        from lib.transitions import apply_transition
        req_id = self._make_request("narrow", search_filetype_override="flac,mp3 v0,mp3 320")
        apply_transition(self.db, req_id, "wanted", from_status="imported",
                         search_filetype_override="flac,mp3 v0")
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["search_filetype_override"], "flac,mp3 v0")

    def test_transition_to_imported_clears_override(self):
        from lib.transitions import apply_transition
        req_id = self._make_request("clear", search_filetype_override="flac")
        apply_transition(self.db, req_id, "wanted", from_status="imported")
        apply_transition(self.db, req_id, "imported", from_status="wanted",
                         search_filetype_override=None)
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertIsNone(req["search_filetype_override"])

    def test_two_session_cas_race_exactly_one_transition_wins(self):
        from lib.pipeline_db import PipelineDB
        from lib.transitions import (
            TransitionApplied,
            TransitionConflict,
            apply_transition,
        )

        req_id = self.db.add_request(
            mb_release_id="transition-cas-race",
            artist_name="A",
            album_title="B",
            source="request",
        )
        db_two = PipelineDB(TEST_DSN)
        barrier = threading.Barrier(2)

        def race(db, target: str):
            barrier.wait(timeout=5)
            return apply_transition(
                db, req_id, target, from_status="wanted")

        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(
                    lambda pair: race(*pair),
                    ((self.db, "unsearchable"), (db_two, "imported")),
                ))
        finally:
            db_two.close()

        self.assertEqual(
            sum(isinstance(result, TransitionApplied) for result in results), 1)
        self.assertEqual(
            sum(isinstance(result, TransitionConflict) for result in results), 1)
        row = self.db.get_request(req_id)
        assert row is not None
        self.assertIn(row["status"], {"unsearchable", "imported"})

    def test_replaced_row_is_frozen_across_every_status_writer(self):
        from lib.transitions import TransitionConflict, apply_transition

        old_id = self.db.add_request(
            mb_release_id="transition-frozen-old",
            artist_name="A",
            album_title="B",
            source="request",
        )
        self.db.supersede_request_mbid(
            old_id,
            new_mb_release_id="transition-frozen-new",
            new_mb_release_group_id=None,
            new_mb_artist_id=None,
            new_artist_name="A",
            new_album_title="B2",
            new_year=None,
            new_country=None,
            new_tracks=[],
        )
        before = self.db.get_request(old_id)
        assert before is not None

        result = apply_transition(
            self.db, old_id, "wanted", from_status="replaced")
        self.assertIsInstance(result, TransitionConflict)
        self.assertFalse(self.db.update_status(
            old_id, "wanted", expected_status="replaced"))
        self.assertFalse(self.db.reset_to_wanted(
            old_id, expected_status="replaced"))
        self.assertFalse(self.db.mark_imported_with_rescue(
            old_id, expected_status="replaced"))
        self.assertFalse(self.db.set_downloading(
            old_id, "{}", expected_status="wanted"))
        self.assertEqual(self.db.get_request(old_id), before)

    def test_replace_wins_between_requeue_and_late_retry_write(self):
        """Real-PG barrier pin for the former two-write thaw race."""
        from lib.pipeline_db import PipelineDB

        request_id = self.db.add_request(
            mb_release_id="transition-late-retry-old",
            artist_name="A",
            album_title="B",
            source="request",
        )
        self.assertTrue(self.db.set_downloading(
            request_id,
            '{"files": [], "filetype": "flac"}',
            expected_status="wanted",
        ))
        worker_db = PipelineDB(TEST_DSN)
        replace_db = PipelineDB(TEST_DSN)
        reset_done = threading.Barrier(2)
        replace_done = threading.Barrier(2)

        def late_worker() -> bool:
            self.assertTrue(worker_db.reset_downloading_to_wanted(
                request_id,
                expected_status="downloading",
            ))
            reset_done.wait(timeout=5)
            replace_done.wait(timeout=5)
            return worker_db.record_attempt(
                request_id,
                "download",
                expected_status="wanted",
            )

        def replace() -> int:
            reset_done.wait(timeout=5)
            new_id = replace_db.supersede_request_mbid(
                request_id,
                new_mb_release_id="transition-late-retry-new",
                new_mb_release_group_id=None,
                new_mb_artist_id=None,
                new_artist_name="A",
                new_album_title="B2",
                new_year=None,
                new_country=None,
                new_tracks=[],
            )
            replace_done.wait(timeout=5)
            return new_id

        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                worker_future = pool.submit(late_worker)
                replace_future = pool.submit(replace)
                self.assertGreater(replace_future.result(timeout=10), request_id)
                self.assertFalse(worker_future.result(timeout=10))
        finally:
            worker_db.close()
            replace_db.close()

        row = self.db.get_request(request_id)
        assert row is not None
        self.assertEqual(row["status"], "replaced")
        self.assertEqual(row["download_attempts"], 0)
        self.assertIsNone(row["next_retry_after"])

    def test_replace_from_downloading_freezes_metadata_writers(self):
        request_id = self.db.add_request(
            mb_release_id="transition-metadata-old",
            artist_name="A",
            album_title="B",
            source="request",
        )
        self.assertTrue(self.db.set_downloading(
            request_id,
            '{"files": [], "filetype": "flac", "current_path": "/old"}',
            expected_status="wanted",
        ))
        self.db.supersede_request_mbid(
            request_id,
            new_mb_release_id="transition-metadata-new",
            new_mb_release_group_id=None,
            new_mb_artist_id=None,
            new_artist_name="A",
            new_album_title="B2",
            new_year=None,
            new_country=None,
            new_tracks=[],
        )
        frozen = self.db.get_request(request_id)
        assert frozen is not None

        self.assertFalse(self.db.update_download_state_if_downloading(
            request_id,
            '{"files": [], "filetype": "mp3", '
            '"enqueued_at": "2026-07-13T15:00:00+00:00"}',
            expected_enqueued_at="2026-07-13T15:00:00+00:00",
        ))
        self.assertFalse(self.db.set_request_current_evidence(
            request_id,
            999,
            expected_status="downloading",
        ))
        self.assertFalse(self.db.record_attempt(
            request_id,
            "download",
            expected_status="wanted",
        ))
        self.assertEqual(self.db.get_request(request_id), frozen)


@requires_postgres
class TestAlbumQualityEvidenceStorage(unittest.TestCase):
    """Content-addressed album-quality evidence storage (post migration 021).

    The pre-021 ``TestAlbumQualityEvidenceStorage`` exercised the
    ``AlbumQualityEvidenceOwner``-keyed surface — owner round trips,
    ``validate_album_quality_evidence_owner``, ``load_album_quality_evidence(owner)``,
    legacy-scalars fallback, owner-typed delete-cascade. All those production
    methods were removed in U2/U3 (commit 5bd1bbb). The cases that were
    purely about the old key shape have been deleted; the cases that
    exercise behaviour still meaningful on the new ``(mb_release_id,
    snapshot_fingerprint)`` key have been migrated below.

    Equivalence proofs for deleted tests:
        - ``test_upsert_load_request_current_round_trips_typed_evidence``,
          ``test_upsert_load_download_log_candidate_uses_neutral_v0_shape``,
          ``test_import_job_candidate_owner_round_trips`` — covered by
          content-addressed round-trip below + dispatch slice/orchestration
          tests in ``tests/test_dispatch_core.py`` and
          ``tests/test_import_evidence.py``.
        - ``test_legacy_scalars_are_not_loaded_as_active_evidence`` — the
          legacy-scalars fallback path was removed alongside owner-keyed
          load.
        - ``test_validation_rejects_bad_owner_and_bad_snapshot`` — the
          owner-validation method no longer exists. Snapshot-shape
          validation is still covered below.
    """

    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="evidence-uuid",
            artist_name="Evidence Artist",
            album_title="Evidence Album",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def _seed(self, **kwargs):
        """Build evidence with the canonical content-addressed shape."""
        return make_album_quality_evidence(
            mb_release_id=kwargs.pop("mb_release_id", "mbid-fixture"),
            **kwargs,
        )

    def test_rejects_claimed_fingerprint_that_disagrees_with_its_files(self):
        """The canonical writer rejects a malformed content-address receipt."""
        evidence = self._seed(
            files=[
                AlbumQualityEvidenceFile(
                    relative_path="01.mp3",
                    size_bytes=123,
                    mtime_ns=456,
                    extension="mp3",
                    container="mp3",
                    codec="mp3",
                )
            ],
        )
        wrong_fingerprint = (
            "0" * 64
            if evidence.snapshot_fingerprint != "0" * 64
            else "1" * 64
        )
        malformed = msgspec.structs.replace(
            evidence,
            snapshot_fingerprint=wrong_fingerprint,
        )

        with self.assertRaisesRegex(ValueError, "snapshot_fingerprint"):
            self.db.upsert_album_quality_evidence(malformed)

        self.assertIsNone(
            self.db.find_album_quality_evidence(
                mb_release_id=malformed.mb_release_id,
                snapshot_fingerprint=wrong_fingerprint,
            )
        )

    def test_back_to_mono_corrupt_attempt_survives_stale_spectral_collision(self):
        """Issue #1030: persistence completion is not cache eligibility.

        Back to Mono repeatedly produced a fresh, detailed corrupt-FLAC
        attempt while its content-addressed row retained an older candidate
        spectral tuple with no analyzer generation.  The attempt was durably
        linked, but the action loader applied the cache-generation gate and
        demoted the concrete corruption fact to ``measurement_failed`` before
        the unified decider could reject it.

        This is deliberately a composed real-PostgreSQL regression: it drives
        the measurement-only writer, same-address merge, FK reload, action
        admission, and real policy decider.
        """
        from lib.import_evidence import ensure_candidate_evidence_for_action
        from lib.measurement import PreimportMeasurement
        from lib.quality import (
            SpectralAnalysisDetail,
            SpectralDetail,
            full_pipeline_decision_from_evidence,
        )
        from lib.quality_evidence import (
            persist_candidate_evidence_from_measurement,
            snapshot_audio_files,
        )
        from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION
        from tests.evidence_helpers import make_audio_corrupt_validation_report

        release_id = "a3e94b84-d6e8-4897-938f-250b9e3a6abb"
        with tempfile.TemporaryDirectory() as source_path:
            track_path = Path(source_path, "01 - Back to Mono.flac")
            track_path.write_bytes(b"not really flac")
            files = snapshot_audio_files(source_path)
            stale = self._seed(
                mb_release_id=release_id,
                source_path=source_path,
                files=files,
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=900,
                    avg_bitrate_kbps=920,
                    median_bitrate_kbps=910,
                    format="FLAC",
                    spectral_grade="suspect",
                    spectral_bitrate_kbps=96,
                    spectral_subject="source",
                    spectral_provenance="measured",
                    spectral_measurement_version=None,
                ),
                preserve_spectral_measurement_version=True,
                codec="flac",
                container="flac",
                storage_format="FLAC",
            )
            self.db.upsert_album_quality_evidence(stale)
            download_log_id = self.db.log_download(
                request_id=self.req_id,
                outcome="rejected",
            )
            corrupt_report = make_audio_corrupt_validation_report(
                track_path.name,
                detail="invalid sync code",
            )
            measurement = PreimportMeasurement(
                corrupt_files=[track_path.name],
                audio_validation=corrupt_report,
                audio_corrupt=True,
                audio_error="invalid sync code",
                folder_layout="flat",
                audio_file_count=1,
                filetype_band="flac",
                lossless_candidate=True,
                min_bitrate_kbps=900,
                spectral_audit=SpectralDetail(
                    candidate=SpectralAnalysisDetail(
                        attempted=True,
                        grade="genuine",
                        bitrate_kbps=96,
                        spectral_measurement_version=(
                            SPECTRAL_MEASUREMENT_VERSION
                        ),
                    ),
                    existing=SpectralAnalysisDetail(attempted=False),
                ),
            )

            persisted = persist_candidate_evidence_from_measurement(
                self.db,
                mb_release_id=release_id,
                source_path=source_path,
                measurement=measurement,
                download_log_id=download_log_id,
                files=files,
            )
            self.assertEqual(persisted.status, "ready")

            admitted = ensure_candidate_evidence_for_action(
                self.db,
                source_path=source_path,
                download_log_id=download_log_id,
            )

            self.assertTrue(admitted.available, admitted.provenance)
            assert admitted.evidence is not None
            decision = full_pipeline_decision_from_evidence(admitted.evidence)
            self.assertEqual(decision["preimport_audio"], "reject_corrupt")
            self.assertFalse(decision["imported"])

    def test_candidate_attempt_cannot_overwrite_dual_role_source_spectral(self):
        """#1030: candidate projection must not rewrite current audit truth."""
        from lib.quality_evidence import (
            CandidateEvidencePersistenceReceipt,
            candidate_evidence_from_persistence_receipt,
        )
        from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION

        release_id = "dual-role-preserved-source"
        files = [AlbumQualityEvidenceFile(
            relative_path="01.opus",
            size_bytes=128,
            mtime_ns=1_700_000_000_000_000_000,
            extension="opus",
            container="opus",
            codec="opus",
        )]
        current = self._seed(
            mb_release_id=release_id,
            files=files,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=128,
                avg_bitrate_kbps=130,
                median_bitrate_kbps=129,
                format="Opus",
                spectral_grade="likely_transcode",
                spectral_bitrate_kbps=96,
                spectral_subject="source",
                spectral_provenance="carried",
                spectral_measurement_version=None,
                was_converted_from="flac",
            ),
            preserve_spectral_measurement_version=True,
            codec="opus",
            container="opus",
            storage_format="Opus",
        )
        self.db.upsert_album_quality_evidence(current)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=release_id,
            snapshot_fingerprint=current.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.assertTrue(self.db.set_request_current_evidence(
            self.req_id,
            stored.id,
        ))

        fresh_candidate = msgspec.structs.replace(
            current,
            measurement=msgspec.structs.replace(
                current.measurement,
                spectral_grade="genuine",
                spectral_bitrate_kbps=192,
                spectral_subject="source",
                spectral_provenance="measured",
                spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
                was_converted_from=None,
            ),
        )
        self.db.upsert_album_quality_evidence(
            fresh_candidate,
            spectral_write_intent="replace",
        )

        canonical = self.db.load_album_quality_evidence_by_id(stored.id)
        assert canonical is not None
        self.assertEqual(
            (
                canonical.measurement.spectral_grade,
                canonical.measurement.spectral_bitrate_kbps,
                canonical.measurement.spectral_subject,
                canonical.measurement.spectral_provenance,
                canonical.measurement.spectral_measurement_version,
                canonical.measurement.was_converted_from,
            ),
            ("likely_transcode", 96, "source", "carried", None, "flac"),
        )

        projected = candidate_evidence_from_persistence_receipt(
            canonical,
            CandidateEvidencePersistenceReceipt(
                evidence_id=stored.id,
                snapshot_fingerprint=stored.snapshot_fingerprint,
                spectral_write_intent="replace",
                spectral_outcome="measured",
                spectral_grade="genuine",
                spectral_bitrate_kbps=192,
                spectral_subject="source",
                spectral_provenance="measured",
                spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
            ),
        )
        self.assertEqual(projected.measurement.spectral_grade, "genuine")
        self.assertEqual(projected.measurement.spectral_bitrate_kbps, 192)
        self.assertIsNone(projected.measurement.was_converted_from)

    def test_candidate_attempt_refreshes_current_owned_native_source_spectral(self):
        """#1030: ordinary source measurements remain remeasurable."""
        from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION

        release_id = "current-native-source-measured"
        files = [AlbumQualityEvidenceFile(
            relative_path="01.mp3",
            size_bytes=128,
            mtime_ns=1_700_000_000_000_000_001,
            extension="mp3",
            container="mp3",
            codec="mp3",
        )]
        current = self._seed(
            mb_release_id=release_id,
            files=files,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=128,
                avg_bitrate_kbps=130,
                median_bitrate_kbps=129,
                format="MP3",
                spectral_grade="suspect",
                spectral_bitrate_kbps=96,
                spectral_subject="source",
                spectral_provenance="measured",
                spectral_measurement_version=None,
            ),
            preserve_spectral_measurement_version=True,
            codec="mp3",
            container="mp3",
            storage_format="MP3",
        )
        self.db.upsert_album_quality_evidence(current)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=release_id,
            snapshot_fingerprint=current.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.assertTrue(self.db.set_request_current_evidence(
            self.req_id,
            stored.id,
        ))

        fresh = msgspec.structs.replace(
            current,
            measurement=msgspec.structs.replace(
                current.measurement,
                spectral_grade="genuine",
                spectral_bitrate_kbps=192,
                spectral_subject="source",
                spectral_provenance="measured",
                spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
            ),
        )
        self.db.upsert_album_quality_evidence(
            fresh,
            spectral_write_intent="replace",
        )

        canonical = self.db.load_album_quality_evidence_by_id(stored.id)
        assert canonical is not None
        self.assertEqual(
            (
                canonical.measurement.spectral_grade,
                canonical.measurement.spectral_bitrate_kbps,
                canonical.measurement.spectral_subject,
                canonical.measurement.spectral_provenance,
                canonical.measurement.spectral_measurement_version,
                canonical.measurement.was_converted_from,
            ),
            (
                "genuine",
                192,
                "source",
                "measured",
                SPECTRAL_MEASUREMENT_VERSION,
                None,
            ),
        )

    def test_upsert_then_find_by_content_address_round_trips(self):
        from lib.quality import (
            AlbumQualityEvidenceFile,
            AlbumQualityV0Metric,
            AudioQualityMeasurement,
            VerifiedLosslessProof,
        )

        evidence = self._seed(
            mb_release_id="mbid-round-trip",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=860,
                avg_bitrate_kbps=912,
                median_bitrate_kbps=899,
                format="flac",
                spectral_grade="genuine",
                spectral_subject="source",
                spectral_provenance="measured",
            ),
            files=[
                AlbumQualityEvidenceFile(
                    relative_path="02 - Beta.flac",
                    size_bytes=2000,
                    mtime_ns=20,
                    extension="flac",
                    container="flac",
                    codec="flac",
                ),
                AlbumQualityEvidenceFile(
                    relative_path="01 - Alpha.flac",
                    size_bytes=1000,
                    mtime_ns=10,
                    extension="flac",
                    container="flac",
                    codec="flac",
                ),
            ],
            codec="flac",
            container="flac",
            storage_format="flac",
            target_format="lossless",
            on_disk_v0_research_attempted=True,
            current_enrichment_required=True,
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=165,
                avg_bitrate_kbps=228,
                median_bitrate_kbps=225,
                subject="source",
                provenance="measured",
            ),
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="measured",
                source="lossless candidate",
                classifier="spectral+v0",
                detail="genuine spectral result",
            ),
        )

        self.db.upsert_album_quality_evidence(evidence)
        loaded = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )

        assert loaded is not None
        self.assertEqual(
            loaded,
            msgspec.structs.replace(
                evidence.sorted_for_storage(),
                id=loaded.id,
            ),
        )
        self.assertEqual(loaded.measurement.format, "flac")
        self.assertTrue(loaded.on_disk_v0_research_attempted)
        self.assertTrue(loaded.current_enrichment_required)
        self.assertIsNotNone(loaded.verified_lossless_proof)
        self.assertEqual(loaded.target_format, "lossless")
        self.assertFalse(loaded.target_is_cbr)
        self.assertEqual(loaded.lineage_version, CURRENT_EVIDENCE_LINEAGE_VERSION)
        self.assertIsNotNone(loaded.verified_lossless_proof)
        # Files round-trip sorted-for-storage.
        self.assertEqual(
            [file.relative_path for file in loaded.files],
            ["01 - Alpha.flac", "02 - Beta.flac"],
        )
        assert loaded.v0_metric is not None
        self.assertEqual(loaded.v0_metric.avg_bitrate_kbps, 228)

    def test_cd_rip_positive_evidence_round_trips_and_survives_weak_writer(self):
        """Migration 070 JSONB uses the production decoder and is monotonic."""
        from lib.quality import (
            AccurateRipBitMatch,
            CdRipBitVerification,
            CdTocIdentity,
            CtdbWholeDiscMatch,
        )

        cd_rip = CdRipBitVerification(
            source_format="flac",
            toc=CdTocIdentity(
                track_offsets_sectors=[0, 12345],
                leadout_sector=24567,
                accuraterip_id="00009012-0000c0de-0a123402",
                musicbrainz_disc_id="disc_identity",
            ),
            accuraterip=AccurateRipBitMatch(
                provider="accuraterip",
                url="https://www.accuraterip.com/example.bin",
                checksum_version="arv2",
                read_offset_samples=-222,
                track_confidences=[31, 47],
                track_checksums=[0x12345678, 0x90ABCDEF],
                response_sha256="a" * 64,
            ),
            ctdb=CtdbWholeDiscMatch(
                provider="ctdb",
                url="https://db.cue.tools/example",
                entry_id="ctdb-123",
                confidence=8026,
                crc32=0xA1B2C3D4,
                stride_samples=5880,
                response_toc_sectors=[32, 12377, 24599],
                response_toc_shift_sectors=32,
                response_sha256="b" * 64,
            ),
        )
        evidence = self._seed(
            mb_release_id="mbid-cd-rip-round-trip",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=800,
                avg_bitrate_kbps=850,
                median_bitrate_kbps=840,
                format="FLAC",
            ),
            files=[AlbumQualityEvidenceFile(
                relative_path="01 - Track.flac",
                size_bytes=123456,
                mtime_ns=1_700_000_000_000_000_000,
                extension="flac",
                container="flac",
                codec="flac",
            )],
            codec="flac",
            container="flac",
            storage_format="FLAC",
            verified_lossless_proof=cd_rip.verified_lossless_proof(),
            cd_rip_verification=cd_rip,
        )
        self.db.upsert_album_quality_evidence(evidence)
        self.db.upsert_album_quality_evidence(msgspec.structs.replace(
            evidence,
            verified_lossless_proof=None,
            cd_rip_verification=None,
        ))

        loaded = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )

        assert loaded is not None
        self.assertEqual(loaded.cd_rip_verification, cd_rip)
        self.assertEqual(
            loaded.verified_lossless_proof,
            cd_rip.verified_lossless_proof(),
        )
        valid = msgspec.json.decode(
            msgspec.json.encode(cd_rip),
            type=dict[str, object],
        )
        valid_ar = msgspec.json.decode(
            msgspec.json.encode(cd_rip.accuraterip),
            type=dict[str, object],
        )
        valid_ctdb = msgspec.json.decode(
            msgspec.json.encode(cd_rip.ctdb),
            type=dict[str, object],
        )
        valid_toc = msgspec.json.decode(
            msgspec.json.encode(cd_rip.toc),
            type=dict[str, object],
        )
        cases: dict[str, object] = {
            "empty object": {},
            "no provider": {
                **valid,
                "accuraterip": None,
                "ctdb": None,
            },
            "missing nested field": {
                **valid,
                "accuraterip": {
                    **valid_ar,
                    "response_sha256": None,
                },
            },
            "bad provider URL": {
                **valid,
                "accuraterip": {
                    **valid_ar,
                    "url": "http://www.accuraterip.com/plaintext.bin",
                },
            },
            "non-positive confidence": {
                **valid,
                "accuraterip": {
                    **valid_ar,
                    "track_confidences": [31, 0],
                },
            },
            "wrong track count": {
                **valid,
                "accuraterip": {
                    **valid_ar,
                    "track_checksums": [0x12345678],
                },
            },
            "fractional TOC sector": {
                **valid,
                "toc": {
                    **valid_toc,
                    "track_offsets_sectors": [0, 12345.5],
                },
                "ctdb": {
                    **valid_ctdb,
                    "response_toc_sectors": [32, 12377.5, 24599],
                },
            },
            "non-increasing TOC": {
                **valid,
                "toc": {
                    **valid_toc,
                    "track_offsets_sectors": [0, 0],
                },
                "ctdb": {
                    **valid_ctdb,
                    "response_toc_sectors": [32, 32, 24599],
                },
            },
            "leadout before last track": {
                **valid,
                "toc": {
                    **valid_toc,
                    "leadout_sector": 12345,
                },
                "ctdb": {
                    **valid_ctdb,
                    "response_toc_sectors": [32, 12377, 12377],
                },
            },
            "fractional confidence": {
                **valid,
                "accuraterip": {
                    **valid_ar,
                    "track_confidences": [31, 47.5],
                },
            },
            "fractional checksum": {
                **valid,
                "accuraterip": {
                    **valid_ar,
                    "track_checksums": [0x12345678, 47.5],
                },
            },
            "invalid offset": {
                **valid,
                "accuraterip": {
                    **valid_ar,
                    "read_offset_samples": 5001,
                },
            },
            "CTDB mismatched response TOC": {
                **valid,
                "ctdb": {
                    **valid_ctdb,
                    "response_toc_sectors": [32, 12378, 24599],
                },
            },
            "CTDB incorrect response shift": {
                **valid,
                "ctdb": {
                    **valid_ctdb,
                    "response_toc_shift_sectors": 31,
                },
            },
            "CTDB non-positive confidence": {
                **valid,
                "ctdb": {
                    **valid_ctdb,
                    "confidence": 0,
                },
            },
        }
        for label, malformed in cases.items():
            with self.subTest(label=label), self.assertRaises(
                psycopg2.errors.CheckViolation
            ):
                self.db._execute(
                    """
                    UPDATE album_quality_evidence
                    SET cd_rip_verification = %s::jsonb
                    WHERE id = %s
                    """,
                    (json.dumps(malformed), loaded.id),
                )

        self.db._execute(
            """
            UPDATE album_quality_evidence
            SET verified_lossless_detail = 'tampered scalar projection'
            WHERE id = %s
            """,
            (loaded.id,),
        )
        with self.assertRaisesRegex(ValueError, "exact scalar proof"):
            self.db.load_album_quality_evidence_by_id(loaded.id)

    def test_upsert_then_find_round_trips_every_measurement_field(self):
        """Rule A (``.claude/rules/test-fidelity.md`` — "EVERY input key",
        review round 2 should-fix 8): every ``AudioQualityMeasurement``
        field, not just the four issue #829 Phase 5 PR1 capture fields,
        must read back through real PG unchanged — ``FakePipelineDB``
        alone would hide SQL column-list drift."""
        from lib.quality import AudioQualityMeasurement

        measurement_in = AudioQualityMeasurement(
            min_bitrate_kbps=192,
            avg_bitrate_kbps=200,
            median_bitrate_kbps=196,
            format="MP3",
            is_cbr=False,
            spectral_grade="suspect",
            spectral_bitrate_kbps=192,
            spectral_subject="source",
            spectral_provenance="measured",
            was_converted_from=None,
            cliff_hz=16500,
            codec_family="mp3",
            ultrasonic_deficit_db=42.5,
            spectral_measurement_version=2,
        )
        evidence = self._seed(
            mb_release_id="mbid-spectral-capture",
            measurement=measurement_in,
        )

        self.db.upsert_album_quality_evidence(evidence)
        loaded = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )

        assert loaded is not None
        for field in msgspec.structs.fields(AudioQualityMeasurement):
            with self.subTest(field=field.name):
                self.assertEqual(
                    getattr(loaded.measurement, field.name),
                    getattr(measurement_in, field.name),
                    f"measurement.{field.name} was dropped at the PG boundary",
                )

    def test_upsert_new_spectral_capture_fields_null_by_default(self):
        """Legacy/absent capture stays NULL — no fabricated defaults
        (issue #829 Phase 5 PR1, forward-only, no backfill)."""
        evidence = self._seed(
            mb_release_id="mbid-spectral-capture-null",
            preserve_spectral_measurement_version=True,
        )

        self.db.upsert_album_quality_evidence(evidence)
        loaded = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )

        assert loaded is not None
        self.assertIsNone(loaded.measurement.cliff_hz)
        self.assertIsNone(loaded.measurement.codec_family)
        self.assertIsNone(loaded.measurement.ultrasonic_deficit_db)
        self.assertIsNone(loaded.measurement.spectral_measurement_version)

    def test_upsert_stale_writer_preserves_new_capture_fields(self):
        """The new capture fields are one atomic fact alongside
        spectral_grade (issue #829 Phase 5 PR1): a stale writer without a
        grade cannot erase them, mirroring the existing spectral-pair
        preservation guard."""
        from lib.quality import AudioQualityMeasurement

        evidence = self._seed(
            mb_release_id="mbid-spectral-capture-preserve",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=192,
                avg_bitrate_kbps=192,
                median_bitrate_kbps=192,
                format="MP3",
                spectral_grade="genuine",
                spectral_subject="source",
                spectral_provenance="measured",
                cliff_hz=17000,
                codec_family="mp3",
                ultrasonic_deficit_db=55.0,
                spectral_measurement_version=2,
            ),
        )
        self.db.upsert_album_quality_evidence(evidence)

        stale_writer = msgspec.structs.replace(
            evidence,
            measurement=msgspec.structs.replace(
                evidence.measurement,
                spectral_grade=None,
                spectral_bitrate_kbps=None,
                spectral_subject=None,
                spectral_provenance=None,
                cliff_hz=None,
                codec_family=None,
                ultrasonic_deficit_db=None,
                spectral_measurement_version=None,
            ),
        )
        self.db.upsert_album_quality_evidence(stale_writer)

        loaded = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert loaded is not None
        self.assertEqual(loaded.measurement.cliff_hz, 17000)
        self.assertEqual(loaded.measurement.codec_family, "mp3")
        self.assertEqual(loaded.measurement.ultrasonic_deficit_db, 55.0)
        self.assertEqual(loaded.measurement.spectral_measurement_version, 2)

    def test_upsert_round_trips_every_aac_lattice_field(self):
        """Rule A for the issue #829 AAC-lattice capture (invariant A-I3):
        EVERY field of ``AacLatticeCapture`` and of every per-track row must
        read back through real PostgreSQL unchanged. ``FakePipelineDB``
        stores the Struct verbatim, so only this catches a missing INSERT
        column, a lossy numeric type, or a JSONB row that decodes short."""
        from lib.quality import AacLatticeCapture, AacLatticeTrackScore

        tracks_in = [
            AacLatticeTrackScore(
                filename="01 - Alpha.flac",
                offset=960, z=28.531250014901161, proba=0.1198,
            ),
            AacLatticeTrackScore(
                filename="02 - Beta.flac",
                offset=960, z=31.134000000000001, proba=0.2207,
            ),
            AacLatticeTrackScore(
                filename="03 - Gamma.flac",
                error=(
                    "AacLatticeUnsupportedRateError: "
                    "unsupported sample rate 96 kHz"
                ),
            ),
        ]
        capture_in = AacLatticeCapture.from_tracks(tracks_in)
        evidence = self._seed(
            mb_release_id="mbid-aac-lattice-round-trip",
            aac_lattice=capture_in,
        )

        self.db.upsert_album_quality_evidence(evidence)
        loaded = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )

        assert loaded is not None
        capture_out = loaded.aac_lattice
        assert capture_out is not None
        for field in msgspec.structs.fields(AacLatticeCapture):
            with self.subTest(field=field.name):
                self.assertEqual(
                    getattr(capture_out, field.name),
                    getattr(capture_in, field.name),
                    f"aac_lattice.{field.name} was dropped at the PG boundary",
                )
        for index, track_in in enumerate(tracks_in):
            for field in msgspec.structs.fields(AacLatticeTrackScore):
                with self.subTest(track=index, field=field.name):
                    self.assertEqual(
                        getattr(capture_out.tracks[index], field.name),
                        getattr(track_in, field.name),
                        f"aac_lattice_tracks[{index}].{field.name} was "
                        "dropped at the PG boundary",
                    )
        # The scalars are the SQL-queryable projection of the same fact and
        # must agree with the array they were derived from.
        self.assertEqual(capture_out.modal_offset, 960)
        self.assertEqual(capture_out.modal_count, 2)
        self.assertEqual(capture_out.scored_tracks, 2)
        self.assertEqual(capture_out.max_z, 31.134000000000001)

    def test_upsert_aac_lattice_is_null_when_never_measured(self):
        """NULL across all five columns means never measured — the cohort
        gate skips most albums, and a fabricated empty capture would lie
        about that (forward-only, no backfill)."""
        evidence = self._seed(mb_release_id="mbid-aac-lattice-null")

        self.db.upsert_album_quality_evidence(evidence)
        loaded = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )

        assert loaded is not None
        self.assertIsNone(loaded.aac_lattice)
        cur = self.db._execute(
            "SELECT aac_lattice_tracks, aac_lattice_modal_offset, "
            "aac_lattice_modal_count, aac_lattice_scored_tracks, "
            "aac_lattice_max_z FROM album_quality_evidence WHERE id = %s",
            (loaded.id,),
        )
        row = cur.fetchone()
        assert row is not None
        for column, value in dict(row).items():
            with self.subTest(column=column):
                self.assertIsNone(value)

    def test_upsert_measured_but_unscored_lattice_is_not_null(self):
        """"Measured, nothing scored" is a distinct, storable fact from
        "never measured" — a 96 kHz album has no lattice and that is
        evidence, not silence."""
        from lib.quality import AacLatticeCapture, AacLatticeTrackScore

        capture = AacLatticeCapture.from_tracks([
            AacLatticeTrackScore(
                filename="01.flac",
                error=(
                    "AacLatticeUnsupportedRateError: "
                    "unsupported sample rate 96 kHz"
                ),
            ),
        ])
        evidence = self._seed(
            mb_release_id="mbid-aac-lattice-unscored",
            aac_lattice=capture,
        )

        self.db.upsert_album_quality_evidence(evidence)
        loaded = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )

        assert loaded is not None
        assert loaded.aac_lattice is not None
        self.assertEqual(loaded.aac_lattice.scored_tracks, 0)
        self.assertEqual(len(loaded.aac_lattice.tracks), 1)
        self.assertIsNone(loaded.aac_lattice.modal_offset)

    def test_upsert_without_a_lattice_preserves_the_stored_capture(self):
        """The lattice follows the V0 tuple's guard, not the spectral one: a
        same-address writer that never ran the cohort gate must not erase a
        tens-of-seconds-per-track measurement of the exact same bytes."""
        from lib.quality import AacLatticeCapture, AacLatticeTrackScore

        capture = AacLatticeCapture.from_tracks([
            AacLatticeTrackScore(
                filename="01.flac", offset=960, z=28.0, proba=0.13,
            ),
        ])
        evidence = self._seed(
            mb_release_id="mbid-aac-lattice-preserve",
            aac_lattice=capture,
        )
        self.db.upsert_album_quality_evidence(evidence)
        self.db.upsert_album_quality_evidence(
            msgspec.structs.replace(evidence, aac_lattice=None)
        )

        loaded = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )

        assert loaded is not None
        assert loaded.aac_lattice is not None
        self.assertEqual(loaded.aac_lattice.modal_offset, 960)
        self.assertEqual(loaded.aac_lattice.max_z, 28.0)
        self.assertEqual(len(loaded.aac_lattice.tracks), 1)

    def test_upsert_with_a_fresh_lattice_replaces_the_stored_one(self):
        from lib.quality import AacLatticeCapture, AacLatticeTrackScore

        first = AacLatticeCapture.from_tracks([
            AacLatticeTrackScore(
                filename="01.flac", offset=960, z=28.0, proba=0.13,
            ),
        ])
        second = AacLatticeCapture.from_tracks([
            AacLatticeTrackScore(filename="01.flac", error="boom"),
        ])
        evidence = self._seed(
            mb_release_id="mbid-aac-lattice-replace",
            aac_lattice=first,
        )
        self.db.upsert_album_quality_evidence(evidence)
        self.db.upsert_album_quality_evidence(
            msgspec.structs.replace(evidence, aac_lattice=second)
        )

        loaded = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )

        assert loaded is not None
        self.assertEqual(loaded.aac_lattice, second)

    def test_aac_lattice_shape_constraint_rejects_stranded_scalars(self):
        """Migration 069's shape CHECK is the last line: it must reject an
        album statistic with no per-track array behind it, whatever bypassed
        the Python validation."""
        import psycopg2.errors

        evidence = self._seed(mb_release_id="mbid-aac-lattice-check")
        self.db.upsert_album_quality_evidence(evidence)
        loaded = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert loaded is not None
        with self.assertRaises(psycopg2.errors.CheckViolation):
            self.db._execute(
                "UPDATE album_quality_evidence SET aac_lattice_modal_offset "
                "= 960 WHERE id = %s",
                (loaded.id,),
            )

    def test_same_address_upsert_cannot_clear_current_enrichment_gate(self):
        evidence = self._seed(
            mb_release_id="mbid-enrichment-gate",
            current_enrichment_required=True,
        )
        self.db.upsert_album_quality_evidence(evidence)
        self.db.upsert_album_quality_evidence(msgspec.structs.replace(
            evidence,
            current_enrichment_required=False,
        ))

        loaded = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )

        assert loaded is not None
        self.assertTrue(loaded.current_enrichment_required)

    def test_fresh_candidate_clears_legacy_conversion_lineage(self):
        """Real PostgreSQL treats a candidate NULL as deliberate source truth."""
        evidence = self._seed(
            mb_release_id="candidate-lineage-clear",
            files=[AlbumQualityEvidenceFile(
                relative_path="01.flac",
                size_bytes=1,
                mtime_ns=1,
                extension="flac",
                container="flac",
                codec="flac",
            )],
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=800,
                avg_bitrate_kbps=900,
                median_bitrate_kbps=850,
                format="FLAC",
                was_converted_from="flac",
            ),
            codec="flac",
            container="flac",
            storage_format="FLAC",
            target_format="opus 128",
        )
        self.db.upsert_album_quality_evidence(evidence)
        self.db.upsert_album_quality_evidence(msgspec.structs.replace(
            evidence,
            measurement=msgspec.structs.replace(
                evidence.measurement,
                was_converted_from=None,
            ),
        ))

        loaded = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )

        assert loaded is not None
        self.assertIsNone(loaded.measurement.was_converted_from)

    def test_candidate_refresh_preserves_current_linked_conversion_lineage(self):
        """A shared content row must retain its current-library history."""
        mbid = "candidate-current-shared-lineage"
        request_id = self.db.add_request(
            mb_release_id=mbid,
            artist_name="Evidence Artist",
            album_title="Shared content row",
            source="request",
        )
        current = self._seed(
            mb_release_id=mbid,
            files=[AlbumQualityEvidenceFile(
                relative_path="01.opus",
                size_bytes=1,
                mtime_ns=1,
                extension="opus",
                container="opus",
                codec="opus",
            )],
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=128,
                avg_bitrate_kbps=130,
                median_bitrate_kbps=129,
                format="Opus",
                was_converted_from="flac",
            ),
            codec="opus",
            container="opus",
            storage_format="Opus",
        )
        self.db.upsert_album_quality_evidence(current)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=mbid,
            snapshot_fingerprint=current.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.assertTrue(
            self.db.set_request_current_evidence(request_id, stored.id)
        )

        candidate = msgspec.structs.replace(
            current,
            measurement=msgspec.structs.replace(
                current.measurement,
                was_converted_from=None,
            ),
        )
        self.db.upsert_album_quality_evidence(candidate)
        loaded = self.db.find_album_quality_evidence(
            mb_release_id=mbid,
            snapshot_fingerprint=current.snapshot_fingerprint,
        )

        assert loaded is not None
        self.assertEqual(loaded.id, stored.id)
        self.assertEqual(loaded.measurement.was_converted_from, "flac")

    def test_v0_research_claim_is_atomic_across_connections(self):
        from lib.pipeline_db import PipelineDB

        evidence = self._seed(
            mb_release_id="evidence-uuid",
            v0_metric=None,
            on_disk_v0_research_attempted=False,
        )
        self.db.upsert_album_quality_evidence(evidence)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.assertTrue(self.db.set_request_current_evidence(
            self.req_id, stored.id))

        db_two = PipelineDB(TEST_DSN)
        barrier = threading.Barrier(2)

        def claim(db) -> bool:
            barrier.wait(timeout=5)
            return db.claim_current_v0_research_attempt(
                request_id=self.req_id,
                expected_evidence_id=stored.id,
                expected_snapshot_fingerprint=stored.snapshot_fingerprint,
            )

        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(claim, (self.db, db_two)))
        finally:
            db_two.close()

        self.assertEqual(results.count(True), 1)
        self.assertEqual(results.count(False), 1)
        claimed = self.db.load_album_quality_evidence_by_id(stored.id)
        assert claimed is not None
        self.assertTrue(claimed.on_disk_v0_research_attempted)
        self.assertIsNone(claimed.v0_metric)

    def test_current_spectral_write_overwrites_with_fresh_measured_audit(self):
        """Issue #815 fresh-audit-wins (real-PG round-trip). A fresh measured
        installed-subject audit of the exact snapshot re-persists over ANY
        disagreeing persisted grade; only the identity guards (request FK,
        evidence id, snapshot fingerprint) still gate the write."""
        evidence = self._seed(
            mb_release_id="evidence-uuid",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                format="MP3",
                spectral_grade=None,
                spectral_bitrate_kbps=None,
            ),
        )
        self.db.upsert_album_quality_evidence(evidence)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.assertTrue(self.db.set_request_current_evidence(
            self.req_id, stored.id))

        # A wrong fingerprint is still refused.
        self.assertFalse(self.db.persist_current_spectral_measurement(
            request_id=self.req_id,
            expected_evidence_id=stored.id,
            expected_snapshot_fingerprint="wrong",
            grade="genuine",
            bitrate_kbps=96,
        ))
        # First exact write fills the empty slot.
        self.assertTrue(self.db.persist_current_spectral_measurement(
            request_id=self.req_id,
            expected_evidence_id=stored.id,
            expected_snapshot_fingerprint=stored.snapshot_fingerprint,
            grade="genuine",
            bitrate_kbps=96,
        ))
        # A disagreeing fresh measured audit of the SAME snapshot now WINS
        # (pre-#815 this was fill-only-if-NULL and returned False).
        self.assertTrue(self.db.persist_current_spectral_measurement(
            request_id=self.req_id,
            expected_evidence_id=stored.id,
            expected_snapshot_fingerprint=stored.snapshot_fingerprint,
            grade="likely_transcode",
            bitrate_kbps=160,
        ))

        loaded = self.db.load_album_quality_evidence_by_id(stored.id)
        assert loaded is not None
        self.assertEqual(loaded.measurement.spectral_grade, "likely_transcode")
        self.assertEqual(loaded.measurement.spectral_bitrate_kbps, 160)
        self.assertEqual(loaded.measurement.spectral_subject, "installed")
        self.assertEqual(loaded.measurement.spectral_provenance, "measured")

    def test_current_spectral_write_carries_capture_facts_real_pg(self):
        """Issue #829 Phase 5 finding A (round 3 review): every writer of
        ``spectral_grade`` must carry the four measured capture facts as one
        atomic fact — this writer bypassed that contract entirely. Real-PG
        round-trip (Rule A): the stored row, re-read from the actual
        ``album_quality_evidence`` columns, must carry the fresh capture
        facts alongside the fresh grade, and a later re-audit that changes
        the grade must also replace the capture facts (not strand the old
        ones behind a new grade)."""
        evidence = self._seed(
            mb_release_id="evidence-capture-facts",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                format="MP3",
                spectral_grade=None,
                spectral_bitrate_kbps=None,
            ),
        )
        self.db.upsert_album_quality_evidence(evidence)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.assertTrue(self.db.set_request_current_evidence(
            self.req_id, stored.id))

        self.assertTrue(self.db.persist_current_spectral_measurement(
            request_id=self.req_id,
            expected_evidence_id=stored.id,
            expected_snapshot_fingerprint=stored.snapshot_fingerprint,
            grade="genuine",
            bitrate_kbps=96,
            cliff_hz=17500,
            codec_family="mp3",
            ultrasonic_deficit_db=12.5,
            spectral_measurement_version=2,
        ))
        loaded = self.db.load_album_quality_evidence_by_id(stored.id)
        assert loaded is not None
        self.assertEqual(loaded.measurement.spectral_grade, "genuine")
        self.assertEqual(loaded.measurement.cliff_hz, 17500)
        self.assertEqual(loaded.measurement.codec_family, "mp3")
        self.assertEqual(loaded.measurement.ultrasonic_deficit_db, 12.5)
        self.assertEqual(loaded.measurement.spectral_measurement_version, 2)

        # A later fresh re-audit with a DIFFERENT grade replaces the capture
        # facts too — a stale cliff_hz/codec_family must never survive behind
        # a fresh grade.
        self.assertTrue(self.db.persist_current_spectral_measurement(
            request_id=self.req_id,
            expected_evidence_id=stored.id,
            expected_snapshot_fingerprint=stored.snapshot_fingerprint,
            grade="likely_transcode",
            bitrate_kbps=160,
            cliff_hz=13000,
            codec_family="aac",
            ultrasonic_deficit_db=30.0,
            spectral_measurement_version=2,
        ))
        reloaded = self.db.load_album_quality_evidence_by_id(stored.id)
        assert reloaded is not None
        self.assertEqual(reloaded.measurement.spectral_grade, "likely_transcode")
        self.assertEqual(reloaded.measurement.cliff_hz, 13000)
        self.assertEqual(reloaded.measurement.codec_family, "aac")
        self.assertEqual(reloaded.measurement.ultrasonic_deficit_db, 30.0)
        self.assertEqual(reloaded.measurement.spectral_measurement_version, 2)

    def test_current_spectral_write_accepts_source_v0_provenance(self):
        from lib.quality import AlbumQualityV0Metric

        evidence = self._seed(
            mb_release_id="evidence-uuid",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=128,
                avg_bitrate_kbps=128,
                median_bitrate_kbps=128,
                format="Opus",
            ),
            v0_metric=AlbumQualityV0Metric(
                avg_bitrate_kbps=225,
                subject="source",
                provenance="carried",
            ),
            codec="opus",
            container="opus",
            storage_format="Opus",
        )
        self.db.upsert_album_quality_evidence(evidence)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.assertTrue(self.db.set_request_current_evidence(
            self.req_id, stored.id,
        ))

        self.assertTrue(self.db.persist_current_spectral_measurement(
            request_id=self.req_id,
            expected_evidence_id=stored.id,
            expected_snapshot_fingerprint=stored.snapshot_fingerprint,
            grade="genuine",
            bitrate_kbps=None,
        ))

    def test_fake_current_spectral_write_accepts_source_v0_provenance(self):
        from lib.quality import AlbumQualityV0Metric

        db = FakePipelineDB()
        request_id = db.add_request(
            mb_release_id="fake-spectral-writer",
            artist_name="A",
            album_title="B",
            source="request",
        )
        evidence = self._seed(
            mb_release_id="fake-spectral-writer",
            measurement=AudioQualityMeasurement(format="Opus"),
            v0_metric=AlbumQualityV0Metric(
                avg_bitrate_kbps=225,
                subject="source",
                provenance="carried",
            ),
            codec="opus",
            container="opus",
            storage_format="Opus",
        )
        db.upsert_album_quality_evidence(evidence)
        stored = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.assertTrue(db.set_request_current_evidence(request_id, stored.id))

        self.assertTrue(db.persist_current_spectral_measurement(
            request_id=request_id,
            expected_evidence_id=stored.id,
            expected_snapshot_fingerprint=stored.snapshot_fingerprint,
            grade="genuine",
            bitrate_kbps=None,
        ))

    def test_current_source_v0_write_matches_fake(self):
        from lib.quality import AlbumQualityV0Metric

        evidence = self._seed(
            mb_release_id="source-v0-writer",
            measurement=AudioQualityMeasurement(
                format="Opus",
                spectral_grade="genuine",
                spectral_subject="installed",
                spectral_provenance="measured",
            ),
            on_disk_v0_research_attempted=True,
            codec="opus",
            container="opus",
            storage_format="Opus",
        )
        adapters = (("fake", FakePipelineDB()), ("postgres", self.db))
        for name, db in adapters:
            with self.subTest(adapter=name):
                request_id = db.add_request(
                    mb_release_id=evidence.mb_release_id,
                    artist_name="A",
                    album_title="B",
                    source="request",
                )
                db.upsert_album_quality_evidence(evidence)
                stored = db.find_album_quality_evidence(
                    mb_release_id=evidence.mb_release_id,
                    snapshot_fingerprint=evidence.snapshot_fingerprint,
                )
                assert stored is not None and stored.id is not None
                self.assertTrue(db.set_request_current_evidence(
                    request_id, stored.id,
                ))

                self.assertTrue(db.persist_current_v0_research_metric(
                    request_id=request_id,
                    expected_evidence_id=stored.id,
                    expected_snapshot_fingerprint=stored.snapshot_fingerprint,
                    metric=AlbumQualityV0Metric(
                        avg_bitrate_kbps=225,
                        subject="source",
                        provenance="measured",
                    ),
                ))

    def test_stale_provenance_upsert_preserves_fresh_installed_spectral(self):
        from lib.quality import AlbumQualityV0Metric, VerifiedLosslessProof

        for anchor in ("source_v0", "proof"):
            for order in (("fresh", "stale"), ("stale", "fresh")):
                with self.subTest(anchor=anchor, order=order):
                    mbid = f"merge-lossless-{anchor}-{order[0]}"
                    existing = self._seed(
                        mb_release_id=mbid,
                        measurement=AudioQualityMeasurement(
                            min_bitrate_kbps=128,
                            avg_bitrate_kbps=128,
                            median_bitrate_kbps=128,
                            format="Opus",
                            spectral_grade="genuine",
                            spectral_subject="installed",
                            spectral_provenance="measured",
                        ),
                        codec="opus",
                        container="opus",
                        storage_format="Opus",
                    )
                    stale = msgspec.structs.replace(
                        existing,
                        measurement=msgspec.structs.replace(
                            existing.measurement,
                            spectral_grade=None,
                            spectral_bitrate_kbps=None,
                            spectral_subject=None,
                            spectral_provenance=None,
                            spectral_measurement_version=None,
                            was_converted_from=None,
                        ),
                        v0_metric=(
                            AlbumQualityV0Metric(
                                avg_bitrate_kbps=225,
                                subject="source",
                                provenance="carried",
                            )
                            if anchor == "source_v0"
                            else None
                        ),
                        verified_lossless_proof=(
                            VerifiedLosslessProof(
                                provenance="carried",
                                source="flac",
                                classifier="spectral_verified_lossless",
                            )
                            if anchor == "proof"
                            else None
                        ),
                    )

                    for writer in order:
                        self.db.upsert_album_quality_evidence(
                            existing if writer == "fresh" else stale,
                        )

                    loaded = self.db.find_album_quality_evidence(
                        mb_release_id=mbid,
                        snapshot_fingerprint=existing.snapshot_fingerprint,
                    )
                    assert loaded is not None
                    self.assertEqual(
                        loaded.measurement.spectral_grade, "genuine"
                    )
                    self.assertEqual(
                        loaded.measurement.spectral_subject, "installed"
                    )
                    self.assertEqual(
                        loaded.measurement.spectral_provenance, "measured"
                    )

    def test_v0_research_attempt_marker_is_monotonic_on_upsert(self):
        evidence = self._seed(
            mb_release_id="monotonic-attempt",
            v0_metric=None,
            on_disk_v0_research_attempted=True,
        )
        self.db.upsert_album_quality_evidence(evidence)
        stale_writer = msgspec.structs.replace(
            evidence,
            on_disk_v0_research_attempted=False,
            storage_format="mp3",
        )

        self.db.upsert_album_quality_evidence(stale_writer)

        stored = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None
        self.assertTrue(stored.on_disk_v0_research_attempted)
        self.assertEqual(stored.storage_format, "mp3")

    def test_upsert_without_v0_preserves_complete_stored_v0_tuple(self):
        from lib.quality import AlbumQualityV0Metric

        metric = AlbumQualityV0Metric(
            min_bitrate_kbps=201,
            avg_bitrate_kbps=259,
            median_bitrate_kbps=255,
            subject="installed",
            provenance="measured",
        )
        evidence = self._seed(
            mb_release_id="preserve-v0-tuple",
            v0_metric=metric,
            on_disk_v0_research_attempted=True,
        )
        self.db.upsert_album_quality_evidence(evidence)

        with self.assertRaisesRegex(
            ValueError, "v0_metric must include at least one bitrate metric"
        ):
            self.db.upsert_album_quality_evidence(msgspec.structs.replace(
                evidence,
                v0_metric=AlbumQualityV0Metric(
                    subject="installed",
                    provenance="measured",
                ),
            ))

        stale_writer = msgspec.structs.replace(
            evidence,
            v0_metric=None,
            on_disk_v0_research_attempted=False,
            storage_format="mp3",
        )
        self.db.upsert_album_quality_evidence(stale_writer)

        stored = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None
        self.assertEqual(stored.v0_metric, metric)
        self.assertTrue(stored.on_disk_v0_research_attempted)
        self.assertEqual(stored.storage_format, "mp3")

        replacement = AlbumQualityV0Metric(
            provenance="measured",
            avg_bitrate_kbps=261,
            subject="installed",
        )
        self.db.upsert_album_quality_evidence(msgspec.structs.replace(
            evidence,
            v0_metric=replacement,
            on_disk_v0_research_attempted=False,
        ))
        replaced = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert replaced is not None
        self.assertEqual(replaced.v0_metric, replacement)
        self.assertTrue(replaced.on_disk_v0_research_attempted)

    def test_v3_to_v4_conflict_clears_omitted_legacy_facts(self):
        evidence = self._seed(
            mb_release_id="v3-v4-clear-legacy",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=245,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=252,
                format="MP3",
            ),
        )
        self.db._execute(
            """
            INSERT INTO album_quality_evidence (
                mb_release_id, snapshot_fingerprint, source_path,
                measured_at, format, lineage_version,
                spectral_grade, spectral_bitrate_kbps,
                spectral_subject, spectral_provenance,
                v0_avg_bitrate_kbps, v0_subject, v0_provenance,
                audio_validation
            )
            VALUES (
                %s, %s, %s, NOW(), 'MP3', 3,
                'genuine', 192,
                'unknown-live-subject', 'unknown-live-provenance',
                245, 'unknown-live-subject', 'unknown-live-provenance',
                %s::jsonb
            )
            """,
            (
                evidence.mb_release_id,
                evidence.snapshot_fingerprint,
                evidence.source_path,
                msgspec.json.encode(
                    legacy_unrecorded_audio_validation_report()
                ).decode(),
            ),
        )

        self.db.upsert_album_quality_evidence(evidence)

        cur = self.db._execute(
            """
            SELECT lineage_version,
                   spectral_grade, spectral_bitrate_kbps,
                   spectral_subject, spectral_provenance,
                   v0_min_bitrate_kbps, v0_avg_bitrate_kbps,
                   v0_median_bitrate_kbps, v0_subject, v0_provenance
            FROM album_quality_evidence
            WHERE mb_release_id = %s AND snapshot_fingerprint = %s
            """,
            (evidence.mb_release_id, evidence.snapshot_fingerprint),
        )
        row = cur.fetchone()
        assert row is not None
        self.assertEqual(
            row["lineage_version"], CURRENT_EVIDENCE_LINEAGE_VERSION)
        self.assertTrue(all(
            row[column] is None
            for column in (
                "spectral_grade",
                "spectral_bitrate_kbps",
                "spectral_subject",
                "spectral_provenance",
                "v0_min_bitrate_kbps",
                "v0_avg_bitrate_kbps",
                "v0_median_bitrate_kbps",
                "v0_subject",
                "v0_provenance",
            )
        ))

        loaded = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert loaded is not None
        self.assertIsNone(loaded.measurement.spectral_grade)
        self.assertIsNone(loaded.v0_metric)

    def test_duplicate_content_address_upsert_replaces_snapshot_rows(self):
        from lib.quality import AlbumQualityEvidenceFile

        # Two distinct file sets → two distinct snapshot fingerprints; the
        # second upsert with the same mb_release_id but different files
        # creates a new row, not a replacement (content-addressed). Reusing
        # the first fingerprint via msgspec.replace lets us assert
        # "same content address replaces".
        files_v1 = [
            AlbumQualityEvidenceFile(
                relative_path="01.mp3",
                size_bytes=1,
                mtime_ns=1,
                extension="mp3",
                container="mp3",
            ),
        ]
        first = msgspec.structs.replace(
            self._seed(mb_release_id="mbid-replace", files=files_v1),
            lineage_version=1,
        )
        self.db.upsert_album_quality_evidence(first)
        original = self.db.find_album_quality_evidence(
            mb_release_id=first.mb_release_id,
            snapshot_fingerprint=first.snapshot_fingerprint,
        )
        assert original is not None and original.id is not None

        # Same content address, but mutate non-keyed fields. Failure-time
        # current-evidence repair relies on this exact v1 -> v3 in-place path.
        replaced = msgspec.structs.replace(
            first,
            source_path="/different/current/location",
            storage_format="mp3",
            lineage_version=3,
        )
        self.db.upsert_album_quality_evidence(replaced)

        loaded = self.db.find_album_quality_evidence(
            mb_release_id=first.mb_release_id,
            snapshot_fingerprint=first.snapshot_fingerprint,
        )
        assert loaded is not None
        self.assertEqual(loaded.id, original.id)
        self.assertEqual(loaded.source_path, original.source_path)
        self.assertEqual(loaded.storage_format, "mp3")
        self.assertEqual(loaded.lineage_version, 3)

        # Only one row exists for this content address.
        cur = self.db._execute(
            "SELECT count(*) AS n FROM album_quality_evidence "
            "WHERE mb_release_id = %s AND snapshot_fingerprint = %s",
            (first.mb_release_id, first.snapshot_fingerprint),
        )
        row = cur.fetchone()
        assert row is not None
        self.assertEqual(row["n"], 1)

    def test_fk_chain_resolves_request_current_evidence(self):
        evidence = self._seed(mb_release_id="mbid-fk-current")
        self.db.upsert_album_quality_evidence(evidence)
        persisted = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        self.db.set_request_current_evidence(self.req_id, persisted.id)

        evidence_id = self.db.get_request_current_evidence_id(self.req_id)
        self.assertEqual(evidence_id, persisted.id)
        loaded = self.db.load_album_quality_evidence_by_id(evidence_id)
        assert loaded is not None
        self.assertEqual(loaded.mb_release_id, "mbid-fk-current")

    def test_fk_chain_resolves_download_log_candidate_evidence(self):
        log_id = self.db.log_download(
            request_id=self.req_id, outcome="rejected"
        )
        evidence = self._seed(mb_release_id="mbid-fk-dl")
        self.db.upsert_album_quality_evidence(evidence)
        persisted = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        self.db.set_download_log_candidate_evidence(log_id, persisted.id)

        evidence_id = self.db.get_download_log_candidate_evidence_id(log_id)
        self.assertEqual(evidence_id, persisted.id)

    def test_get_latest_download_log_candidate_evidence_id(self):
        """Issue #813 tooling tier: pipeline-cli quality's live-candidate
        replay walks attempt history for the newest candidate evidence."""
        self.assertIsNone(
            self.db.get_latest_download_log_candidate_evidence_id(
                self.req_id))

        older_log_id = self.db.log_download(
            request_id=self.req_id, outcome="rejected")
        older = self._seed(mb_release_id="mbid-latest-older")
        self.db.upsert_album_quality_evidence(older)
        older_persisted = self.db.find_album_quality_evidence(
            mb_release_id=older.mb_release_id,
            snapshot_fingerprint=older.snapshot_fingerprint,
        )
        assert older_persisted is not None and older_persisted.id is not None
        self.db.set_download_log_candidate_evidence(
            older_log_id, older_persisted.id)

        self.assertEqual(
            self.db.get_latest_download_log_candidate_evidence_id(
                self.req_id),
            older_persisted.id,
        )

        # A later attempt with no candidate evidence must not shadow the
        # older evidence-bearing row.
        self.db.log_download(request_id=self.req_id, outcome="failed")
        self.assertEqual(
            self.db.get_latest_download_log_candidate_evidence_id(
                self.req_id),
            older_persisted.id,
        )

        newer_log_id = self.db.log_download(
            request_id=self.req_id, outcome="rejected")
        newer = self._seed(mb_release_id="mbid-latest-newer")
        self.db.upsert_album_quality_evidence(newer)
        newer_persisted = self.db.find_album_quality_evidence(
            mb_release_id=newer.mb_release_id,
            snapshot_fingerprint=newer.snapshot_fingerprint,
        )
        assert newer_persisted is not None and newer_persisted.id is not None
        self.db.set_download_log_candidate_evidence(
            newer_log_id, newer_persisted.id)

        self.assertEqual(
            self.db.get_latest_download_log_candidate_evidence_id(
                self.req_id),
            newer_persisted.id,
        )

    def test_fk_chain_resolves_import_job_candidate_evidence(self):
        job = self.db.enqueue_import_job(
            "force_import",
            request_id=self.req_id,
            payload={"download_log_id": 1, "failed_path": "/tmp/candidate"},
        )
        evidence = self._seed(mb_release_id="mbid-fk-job")
        self.db.upsert_album_quality_evidence(evidence)
        persisted = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        self.db.set_import_job_candidate_evidence(job.id, persisted.id)

        evidence_id = self.db.get_import_job_candidate_evidence_id(job.id)
        self.assertEqual(evidence_id, persisted.id)

    def test_round_trip_preview_evidence_facts_with_every_new_field(self):
        """U1: new preview-evidence fields round-trip through upsert/find."""
        import msgspec

        from lib.quality import AlbumQualityEvidenceFile

        # Seed a bad_audio_hashes row to reference; matched_bad_audio_hash_id
        # is an optional FK.
        cur = self.db._execute(
            """
            INSERT INTO bad_audio_hashes (hash_value, audio_format, request_id)
            VALUES (decode('abcd1234', 'hex'), 'mp3', %s)
            RETURNING id
            """,
            (self.req_id,),
        )
        row = cur.fetchone()
        assert row is not None
        bad_id = int(row["id"])

        evidence = self._seed(
            mb_release_id="mbid-preview-facts",
            audio_corrupt=True,
            audio_error=(
                "01 - Track.mp3: Invalid data found when processing input"
            ),
            files=[
                AlbumQualityEvidenceFile(
                    relative_path="01 - Track.mp3",
                    size_bytes=12345,
                    mtime_ns=10,
                    extension="mp3",
                    container="mp3",
                    codec="mp3",
                    decode_ok=False,
                ),
            ],
        )
        evidence = msgspec.structs.replace(
            evidence,
            folder_layout="nested",
            audio_file_count=1,
            filetype_band="mp3",
            matched_bad_audio_hash_id=bad_id,
            matched_bad_audio_hash_path="01 - Track.mp3",
        )

        self.db.upsert_album_quality_evidence(evidence)
        loaded = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )

        assert loaded is not None
        self.assertTrue(loaded.audio_corrupt)
        self.assertEqual(
            loaded.audio_error,
            "01 - Track.mp3: Invalid data found when processing input",
        )
        self.assertEqual(loaded.folder_layout, "nested")
        self.assertEqual(loaded.audio_file_count, 1)
        self.assertEqual(loaded.filetype_band, "mp3")
        self.assertEqual(loaded.matched_bad_audio_hash_id, bad_id)
        self.assertEqual(loaded.matched_bad_audio_hash_path, "01 - Track.mp3")
        self.assertEqual(len(loaded.files), 1)
        self.assertFalse(loaded.files[0].decode_ok)

    def test_audio_validation_round_trip_and_weak_writer_preservation(self):
        """Every diagnostic field survives PG and stale writers lose."""
        files = [
            AlbumQualityEvidenceFile(
                relative_path="disc-1/01.flac",
                size_bytes=123,
                mtime_ns=456,
                extension="flac",
                container="flac",
                codec="flac",
                decode_ok=False,
            ),
        ]
        report = AudioValidationReport(
            tool_version="8.1.1",
            outcome="audio_corrupt",
            files_checked=1,
            files_failed=1,
            diagnostics=[
                AudioToolDiagnostic(
                    relative_path="disc-1/01.flac",
                    category="decode_error",
                    return_code=69,
                    stderr_excerpt="Invalid data",
                    stderr_bytes=4096,
                    stderr_sha256="b" * 64,
                    stderr_truncated=True,
                ),
            ],
        )
        evidence = self._seed(
            mb_release_id="mbid-audio-validation-round-trip",
            files=files,
            audio_corrupt=True,
            audio_error="disc-1/01.flac: Invalid data",
            audio_validation=report,
        )
        self.db.upsert_album_quality_evidence(evidence)

        loaded = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert loaded is not None
        self.assertEqual(loaded.audio_validation, report)
        self.assertFalse(loaded.files[0].decode_ok)

        self.db.upsert_album_quality_evidence(msgspec.structs.replace(
            evidence,
            audio_validation=legacy_unrecorded_audio_validation_report(),
            audio_corrupt=False,
            audio_error=None,
            files=[msgspec.structs.replace(files[0], decode_ok=True)],
        ))
        preserved = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert preserved is not None
        self.assertEqual(preserved.audio_validation, report)
        self.assertTrue(preserved.audio_corrupt)
        self.assertEqual(preserved.audio_error, evidence.audio_error)
        self.assertFalse(preserved.files[0].decode_ok)

    def test_audio_validation_database_constraint_rejects_bad_shape(self):
        """Migration 064 enforces the complete typed bounded audit contract."""
        evidence = self._seed(mb_release_id="mbid-audio-validation-check")
        self.db.upsert_album_quality_evidence(evidence)

        diagnostic_struct = AudioToolDiagnostic(
            relative_path="01.flac",
            category="decode_error",
            return_code=69,
            stderr_excerpt="Invalid data",
            stderr_bytes=12,
            stderr_sha256="b" * 64,
            stderr_truncated=False,
        )
        diagnostic: dict[str, object] = msgspec.to_builtins(
            diagnostic_struct
        )
        valid: dict[str, object] = msgspec.to_builtins(AudioValidationReport(
            outcome="audio_corrupt",
            files_checked=1,
            files_failed=1,
            diagnostics=[diagnostic_struct],
        ))

        malformed: list[tuple[str, dict[str, object], bool]] = [
            ("missing required fields", {"outcome": "passed"}, False),
            (
                "negative count",
                {**valid, "files_checked": -1},
                True,
            ),
            (
                "decimal count",
                {**valid, "files_checked": 1.5},
                True,
            ),
            (
                "failure count mismatch",
                {**valid, "files_failed": 2},
                True,
            ),
            (
                "wrong diagnostic category",
                {
                    **valid,
                    "diagnostics": [{
                        **diagnostic,
                        "category": "read_error",
                    }],
                },
                True,
            ),
            (
                "diagnostic cap exceeded",
                {
                    **valid,
                    "files_checked": 17,
                    "files_failed": 17,
                    "diagnostics": [diagnostic] * 17,
                },
                True,
            ),
            (
                "missing diagnostic field",
                {
                    **valid,
                    "diagnostics": [{
                        key: value
                        for key, value in diagnostic.items()
                        if key != "stderr_truncated"
                    }],
                },
                True,
            ),
            (
                "noninteger return code",
                {
                    **valid,
                    "diagnostics": [{
                        **diagnostic,
                        "return_code": 69.5,
                    }],
                },
                True,
            ),
            (
                "negative stderr size",
                {
                    **valid,
                    "diagnostics": [{
                        **diagnostic,
                        "stderr_bytes": -1,
                    }],
                },
                True,
            ),
            (
                "oversize stderr excerpt",
                {
                    **valid,
                    "diagnostics": [{
                        **diagnostic,
                        "stderr_excerpt": "é" * 1025,
                    }],
                },
                True,
            ),
            (
                "scalar disagreement",
                {**valid, "outcome": "passed", "files_failed": 0,
                 "diagnostics": []},
                True,
            ),
        ]
        for label, payload, scalar_corrupt in malformed:
            with self.subTest(label=label):
                with self.assertRaises(
                    psycopg2.errors.CheckViolation
                ) as raised:
                    self.db._execute(
                        """
                        UPDATE album_quality_evidence
                        SET audio_validation = %s::jsonb,
                            audio_corrupt = %s
                        WHERE mb_release_id = %s
                        """,
                        (
                            json.dumps(payload),
                            scalar_corrupt,
                            evidence.mb_release_id,
                        ),
                    )
                self.assertEqual(
                    raised.exception.diag.constraint_name,
                    "album_quality_evidence_audio_validation_shape_check",
                )
                self.db.conn.rollback()

    def test_empty_fileset_is_storable_when_audio_file_count_is_zero(self):
        """U1 AE4: audio_file_count=0 + files=[] round-trips without error."""
        import msgspec

        evidence = self._seed(
            mb_release_id="mbid-empty-fileset",
            files=[],
        )
        evidence = msgspec.structs.replace(evidence, audio_file_count=0)
        self.db.upsert_album_quality_evidence(evidence)
        loaded = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert loaded is not None
        self.assertEqual(loaded.audio_file_count, 0)
        self.assertEqual(loaded.files, [])

    def test_non_empty_files_with_audio_file_count_zero_inconsistency_still_storable(
        self,
    ):
        """U1: files=[] with audio_file_count!=0 raises (consistency guard)."""
        import msgspec

        evidence = self._seed(
            mb_release_id="mbid-inconsistent",
            files=[],
        )
        evidence = msgspec.structs.replace(evidence, audio_file_count=2)
        with self.assertRaisesRegex(ValueError, "at least one snapshot file is required"):
            self.db.upsert_album_quality_evidence(evidence)

    def test_matched_bad_audio_hash_id_and_path_must_pair(self):
        """U1: hash FK and paired path must be set together or both NULL."""
        import msgspec

        evidence = self._seed(mb_release_id="mbid-bad-hash-pair")
        bad_pair = msgspec.structs.replace(
            evidence, matched_bad_audio_hash_id=1, matched_bad_audio_hash_path=None,
        )
        with self.assertRaisesRegex(ValueError, "must be set together or both NULL"):
            self.db.upsert_album_quality_evidence(bad_pair)
        bad_pair2 = msgspec.structs.replace(
            evidence,
            matched_bad_audio_hash_id=None,
            matched_bad_audio_hash_path="a.mp3",
        )
        with self.assertRaisesRegex(ValueError, "must be set together or both NULL"):
            self.db.upsert_album_quality_evidence(bad_pair2)

    def test_extension_validation_still_enforced_on_evidence_files(self):
        from lib.quality import AlbumQualityEvidenceFile

        with self.assertRaisesRegex(ValueError, "extension is required"):
            self.db.upsert_album_quality_evidence(self._seed(
                mb_release_id="mbid-bad-ext",
                files=[
                    AlbumQualityEvidenceFile(
                        relative_path="bad",
                        size_bytes=1,
                        mtime_ns=1,
                        extension="",
                        container="mp3",
                    ),
                ],
            ))


@requires_postgres
class TestSpectralColumns(unittest.TestCase):
    """Test spectral quality columns on download_log and album_requests."""

    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="spectral-uuid",
            artist_name="Test Artist",
            album_title="Test Album",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_log_download_with_spectral_fields(self):
        self.db.log_download(
            request_id=self.req_id,
            soulseek_username="testuser",
            filetype="mp3",
            outcome="success",
            spectral_grade="suspect",
            spectral_bitrate=128,
            slskd_filetype="mp3",
            actual_filetype="mp3",
            actual_min_bitrate=320000,
            existing_min_bitrate=92,
            existing_spectral_bitrate=64,
        )
        history = self.db.get_download_history(self.req_id)
        self.assertEqual(len(history), 1)
        h = history[0]
        self.assertEqual(h["spectral_grade"], "suspect")
        self.assertEqual(h["spectral_bitrate"], 128)
        self.assertEqual(h["slskd_filetype"], "mp3")
        self.assertEqual(h["actual_filetype"], "mp3")
        self.assertEqual(h["actual_min_bitrate"], 320000)
        self.assertEqual(h["existing_min_bitrate"], 92)
        self.assertEqual(h["existing_spectral_bitrate"], 64)

    def test_spectral_fields_null_by_default(self):
        self.db.log_download(
            request_id=self.req_id,
            soulseek_username="testuser",
            outcome="success",
        )
        history = self.db.get_download_history(self.req_id)
        h = history[0]
        self.assertIsNone(h.get("spectral_grade"))
        self.assertIsNone(h.get("spectral_bitrate"))
        self.assertIsNone(h.get("slskd_filetype"))

    def test_log_download_with_v0_probe_fields(self):
        self.db.log_download(
            request_id=self.req_id,
            soulseek_username="testuser",
            filetype="flac",
            outcome="success",
            v0_probe_kind="lossless_source_v0",
            v0_probe_min_bitrate=165,
            v0_probe_avg_bitrate=228,
            v0_probe_median_bitrate=225,
            existing_v0_probe_kind="lossless_source_v0",
            existing_v0_probe_min_bitrate=128,
            existing_v0_probe_avg_bitrate=171,
            existing_v0_probe_median_bitrate=169,
        )

        history = self.db.get_download_history(self.req_id)
        self.assertEqual(len(history), 1)
        h = history[0]
        self.assertEqual(h["v0_probe_kind"], "lossless_source_v0")
        self.assertEqual(h["v0_probe_min_bitrate"], 165)
        self.assertEqual(h["v0_probe_avg_bitrate"], 228)
        self.assertEqual(h["v0_probe_median_bitrate"], 225)
        self.assertEqual(h["existing_v0_probe_kind"], "lossless_source_v0")
        self.assertEqual(h["existing_v0_probe_min_bitrate"], 128)
        self.assertEqual(h["existing_v0_probe_avg_bitrate"], 171)
        self.assertEqual(h["existing_v0_probe_median_bitrate"], 169)

    def test_album_request_spectral_columns(self):
        self.db.update_status(self.req_id, "imported",
                              last_download_spectral_bitrate=128,
                              last_download_spectral_grade="suspect")
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["last_download_spectral_bitrate"], 128)
        self.assertEqual(req["last_download_spectral_grade"], "suspect")

    def test_on_disk_spectral_columns(self):
        """current_spectral_grade/bitrate describe files currently in beets."""
        self.db.update_status(self.req_id, "imported",
                              current_spectral_grade="suspect",
                              current_spectral_bitrate=160)
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["current_spectral_grade"], "suspect")
        self.assertEqual(req["current_spectral_bitrate"], 160)

    def test_on_disk_spectral_null_by_default(self):
        """current_spectral columns are NULL for pre-existing albums."""
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertIsNone(req["current_spectral_grade"])
        self.assertIsNone(req["current_spectral_bitrate"])

    def test_spectral_state_update_fields_write_both_pairs(self):
        """``RequestSpectralStateUpdate.as_update_fields`` through the live
        production shape (dispatch merges it into ``update_request_fields``)
        round-trips both spectral pairs on real PG."""
        from lib import pipeline_db
        from lib.quality import SpectralMeasurement

        self.db.update_request_fields(
            self.req_id,
            **pipeline_db.RequestSpectralStateUpdate(
                last_download=SpectralMeasurement(
                    grade="suspect", bitrate_kbps=128),
                current=SpectralMeasurement(
                    grade="genuine", bitrate_kbps=245),
            ).as_update_fields(),
        )

        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["last_download_spectral_grade"], "suspect")
        self.assertEqual(req["last_download_spectral_bitrate"], 128)
        self.assertEqual(req["current_spectral_grade"], "genuine")
        self.assertEqual(req["current_spectral_bitrate"], 245)

    def test_spectral_state_update_on_disk_only_clears_nulls(self):
        from lib import pipeline_db
        from lib.quality import SpectralMeasurement

        self.db.update_status(
            self.req_id,
            "imported",
            last_download_spectral_grade="likely_transcode",
            last_download_spectral_bitrate=192,
            current_spectral_grade="likely_transcode",
            current_spectral_bitrate=192,
        )

        self.db.update_request_fields(
            self.req_id,
            **pipeline_db.RequestSpectralStateUpdate(
                current=SpectralMeasurement(
                    grade="genuine", bitrate_kbps=None),
            ).as_update_fields(),
        )

        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["last_download_spectral_grade"], "likely_transcode")
        self.assertEqual(req["last_download_spectral_bitrate"], 192)
        self.assertEqual(req["current_spectral_grade"], "genuine")
        self.assertIsNone(req["current_spectral_bitrate"])

    def test_v0_probe_state_update_fields_set_current_source_probe(self):
        """``RequestV0ProbeStateUpdate.as_update_fields()`` is the live wire
        between the importer (``lib/dispatch/``) and the request
        row. Production funnels the fields through ``finalize_request`` →
        ``mark_imported_with_rescue`` / ``update_status``; this test drives
        the same column names through ``update_request_fields`` (both
        writers interpolate the dict keys into an ``UPDATE album_requests``
        SET list, and the column contract is pinned by
        ``test_pipeline_db_column_contract.py``)."""
        from lib import pipeline_db
        from lib.quality import V0ProbeEvidence

        update = pipeline_db.RequestV0ProbeStateUpdate(
            current_lossless_source=V0ProbeEvidence(
                kind="lossless_source_v0",
                min_bitrate_kbps=165,
                avg_bitrate_kbps=228,
                median_bitrate_kbps=225,
            ),
        )
        self.db.update_request_fields(self.req_id, **update.as_update_fields())

        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["current_lossless_source_v0_probe_min_bitrate"], 165)
        self.assertEqual(req["current_lossless_source_v0_probe_avg_bitrate"], 228)
        self.assertEqual(req["current_lossless_source_v0_probe_median_bitrate"], 225)

    def test_v0_probe_state_update_fields_can_clear_current_source_probe(self):
        from lib import pipeline_db
        from lib.quality import V0ProbeEvidence

        set_update = pipeline_db.RequestV0ProbeStateUpdate(
            current_lossless_source=V0ProbeEvidence(
                kind="lossless_source_v0",
                min_bitrate_kbps=165,
                avg_bitrate_kbps=228,
                median_bitrate_kbps=225,
            ),
        )
        self.db.update_request_fields(
            self.req_id, **set_update.as_update_fields())
        clear_update = pipeline_db.RequestV0ProbeStateUpdate(
            clear_current_lossless_source=True,
        )
        self.db.update_request_fields(
            self.req_id, **clear_update.as_update_fields())

        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertIsNone(req["current_lossless_source_v0_probe_min_bitrate"])
        self.assertIsNone(req["current_lossless_source_v0_probe_avg_bitrate"])
        self.assertIsNone(req["current_lossless_source_v0_probe_median_bitrate"])

    def test_v0_probe_fields_null_by_default(self):
        self.db.log_download(
            request_id=self.req_id,
            soulseek_username="testuser",
            outcome="success",
        )

        history = self.db.get_download_history(self.req_id)
        self.assertIsNone(history[0].get("v0_probe_kind"))
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertIsNone(req["current_lossless_source_v0_probe_min_bitrate"])
        self.assertIsNone(req["current_lossless_source_v0_probe_avg_bitrate"])
        self.assertIsNone(req["current_lossless_source_v0_probe_median_bitrate"])


@requires_postgres
class TestBatchHistory(unittest.TestCase):
    """Test get_download_history_batch — batch download history lookup."""

    def setUp(self):
        self.db = make_db()
        self.req1 = self.db.add_request(
            mb_release_id="batch-1", artist_name="A", album_title="B", source="request")
        self.req2 = self.db.add_request(
            mb_release_id="batch-2", artist_name="C", album_title="D", source="request")
        self.req3 = self.db.add_request(
            mb_release_id="batch-3", artist_name="E", album_title="F", source="request")
        # Add history for req1 and req2, but not req3
        self.db.log_download(self.req1, soulseek_username="user1", outcome="success")
        self.db.log_download(self.req1, soulseek_username="user2", outcome="rejected")
        self.db.log_download(self.req2, soulseek_username="user3", outcome="success")

    def tearDown(self):
        self.db.close()

    def test_returns_grouped_by_request_id(self):
        result = self.db.get_download_history_batch([self.req1, self.req2, self.req3])
        self.assertIn(self.req1, result)
        self.assertIn(self.req2, result)
        self.assertNotIn(self.req3, result)  # no history
        self.assertEqual(len(result[self.req1]), 2)
        self.assertEqual(len(result[self.req2]), 1)

    def test_empty_list(self):
        result = self.db.get_download_history_batch([])
        self.assertEqual(result, {})

    def test_order_is_desc_by_id(self):
        result = self.db.get_download_history_batch([self.req1])
        history = result[self.req1]
        # Most recent first (rejected was logged after success)
        self.assertEqual(history[0]["outcome"], "rejected")
        self.assertEqual(history[1]["outcome"], "success")


@requires_postgres
class TestTrackCounts(unittest.TestCase):
    """Test get_track_counts — batch track count lookup."""

    def setUp(self):
        self.db = make_db()
        self.req1 = self.db.add_request(
            mb_release_id="tc-1", artist_name="A", album_title="B", source="request")
        self.req2 = self.db.add_request(
            mb_release_id="tc-2", artist_name="C", album_title="D", source="request")
        self.req3 = self.db.add_request(
            mb_release_id="tc-3", artist_name="E", album_title="F", source="request")
        self.db.set_tracks(self.req1, [
            {"disc_number": 1, "track_number": 1, "title": "T1", "length_seconds": 100},
            {"disc_number": 1, "track_number": 2, "title": "T2", "length_seconds": 200},
        ])
        self.db.set_tracks(self.req2, [
            {"disc_number": 1, "track_number": 1, "title": "T1", "length_seconds": 100},
        ])
        # req3 has no tracks

    def tearDown(self):
        self.db.close()

    def test_returns_counts(self):
        result = self.db.get_track_counts([self.req1, self.req2, self.req3])
        self.assertEqual(result[self.req1], 2)
        self.assertEqual(result[self.req2], 1)
        self.assertNotIn(self.req3, result)  # no tracks

    def test_empty_list(self):
        result = self.db.get_track_counts([])
        self.assertEqual(result, {})


@requires_postgres
class TestDownloadingStatus(unittest.TestCase):
    """Test the 'downloading' status and active_download_state JSONB column."""

    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_downloading_status_allowed(self):
        """Insert row, update to 'downloading', verify roundtrip."""
        req_id = self.db.add_request(
            mb_release_id="dl-status-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        self.db.update_status(req_id, "downloading")
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["status"], "downloading")

    def test_active_download_state_jsonb_roundtrip(self):
        """Write JSONB to active_download_state column, read back, verify structure."""
        req_id = self.db.add_request(
            mb_release_id="ads-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        state = {
            "filetype": "flac",
            "enqueued_at": "2026-04-03T12:00:00+00:00",
            "files": [
                {"username": "user1", "filename": "user1\\Music\\01.flac",
                 "file_dir": "user1\\Music", "size": 30000000}
            ],
        }
        self.db._execute(
            "UPDATE album_requests SET active_download_state = %s::jsonb WHERE id = %s",
            (json.dumps(state), req_id),
        )
        req = self.db.get_request(req_id)
        assert req is not None
        ads: Any = req["active_download_state"]
        self.assertIsInstance(ads, dict)
        self.assertEqual(ads["filetype"], "flac")
        self.assertEqual(len(ads["files"]), 1)
        self.assertEqual(ads["files"][0]["username"], "user1")

    def test_get_downloading(self):
        """get_downloading() returns only status='downloading' rows."""
        id1 = self.db.add_request(mb_release_id="gd-1", artist_name="A",
                                  album_title="B", source="request")
        id2 = self.db.add_request(mb_release_id="gd-2", artist_name="C",
                                  album_title="D", source="request")
        id3 = self.db.add_request(mb_release_id="gd-3", artist_name="E",
                                  album_title="F", source="request")
        self.db.update_status(id1, "downloading")
        self.db.update_status(id2, "downloading")
        # id3 stays wanted

        downloading = self.db.get_downloading()
        dl_ids = [r["id"] for r in downloading]
        self.assertIn(id1, dl_ids)
        self.assertIn(id2, dl_ids)
        self.assertNotIn(id3, dl_ids)

    def test_set_downloading(self):
        """set_downloading() sets status + writes JSONB atomically."""
        req_id = self.db.add_request(
            mb_release_id="sd-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        state_json = json.dumps({
            "filetype": "mp3 v0",
            "enqueued_at": "2026-04-03T12:00:00+00:00",
            "files": [],
        })
        self.db.set_downloading(req_id, state_json)
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["status"], "downloading")
        self.assertIsNotNone(req["active_download_state"])
        ads: Any = req["active_download_state"]
        self.assertEqual(ads["filetype"], "mp3 v0")
        # Starting a download should not consume a backoff attempt.
        self.assertEqual(req["download_attempts"], 0)

    def test_set_downloading_returns_true_from_wanted(self):
        """set_downloading() returns True when album is wanted."""
        req_id = self.db.add_request(
            mb_release_id="guard-ok", artist_name="A", album_title="B",
            source="request")
        state_json = json.dumps({"filetype": "flac", "enqueued_at": "t", "files": []})
        result = self.db.set_downloading(req_id, state_json)
        self.assertTrue(result)
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["status"], "downloading")

    def test_set_downloading_noop_from_imported(self):
        """set_downloading() returns False and doesn't overwrite imported status."""
        req_id = self.db.add_request(
            mb_release_id="guard-imp", artist_name="A", album_title="B",
            source="request")
        self.db.update_status(req_id, "imported")
        state_json = json.dumps({"filetype": "flac", "enqueued_at": "t", "files": []})
        result = self.db.set_downloading(req_id, state_json)
        self.assertFalse(result)
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["status"], "imported")

    def test_set_downloading_noop_from_downloading(self):
        """set_downloading() returns False when already downloading (no state overwrite)."""
        req_id = self.db.add_request(
            mb_release_id="guard-dl", artist_name="A", album_title="B",
            source="request")
        original_state = json.dumps({"filetype": "flac", "enqueued_at": "t", "files": []})
        self.db.set_downloading(req_id, original_state)
        new_state = json.dumps({"filetype": "mp3 v0", "enqueued_at": "t2", "files": []})
        result = self.db.set_downloading(req_id, new_state)
        self.assertFalse(result)
        # Original state preserved
        req = self.db.get_request(req_id)
        assert req is not None
        ads: Any = req["active_download_state"]
        self.assertEqual(ads["filetype"], "flac")

    def test_set_downloading_noop_from_unsearchable(self):
        """set_downloading() returns False when status is unsearchable."""
        req_id = self.db.add_request(
            mb_release_id="guard-man", artist_name="A", album_title="B",
            source="request")
        self.db.update_status(req_id, "unsearchable")
        state_json = json.dumps({"filetype": "flac", "enqueued_at": "t", "files": []})
        result = self.db.set_downloading(req_id, state_json)
        self.assertFalse(result)
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["status"], "unsearchable")

    def test_update_download_state_if_downloading_success_and_guard(self):
        req_id = self.db.add_request(
            mb_release_id="udsifd-ok",
            artist_name="A",
            album_title="B",
            source="request",
        )
        blocked_id = self.db.add_request(
            mb_release_id="udsifd-blocked",
            artist_name="C",
            album_title="D",
            source="request",
        )
        original_state = json.dumps({
            "filetype": "flac",
            "enqueued_at": "2026-04-03T12:00:00+00:00",
            "files": [],
        })
        self.db.set_downloading(req_id, original_state)
        self.db.set_downloading(blocked_id, original_state)
        self.db.update_status(blocked_id, "imported")

        updated = self.db.update_download_state_if_downloading(
            req_id,
            json.dumps({
                "filetype": "mp3 v0",
                "enqueued_at": "2026-04-03T12:00:00+00:00",
                "files": [],
            }),
            expected_enqueued_at="2026-04-03T12:00:00+00:00",
        )
        blocked = self.db.update_download_state_if_downloading(
            blocked_id,
            json.dumps({
                "filetype": "mp3 320",
                "enqueued_at": "2026-04-03T12:00:00+00:00",
                "files": [],
            }),
            expected_enqueued_at="2026-04-03T12:00:00+00:00",
        )

        self.assertTrue(updated)
        self.assertFalse(blocked)
        req = self.db.get_request(req_id)
        blocked_req = self.db.get_request(blocked_id)
        assert req is not None
        assert blocked_req is not None
        ads: Any = req["active_download_state"]
        self.assertEqual(ads["filetype"], "mp3 v0")
        self.assertIsNone(blocked_req["active_download_state"])

    def test_update_download_state_fence_real_and_fake_case_parity(self):
        witness = "2026-04-03T12:00:00+00:00"
        other_witness = "2026-04-03T12:01:00+00:00"

        def state(
            enqueued_at: str | None,
            *,
            filetype: str = "flac",
        ) -> dict[str, object]:
            result: dict[str, object] = {
                "filetype": filetype,
                "files": [],
            }
            if enqueued_at is not None:
                result["enqueued_at"] = enqueued_at
            return result

        cases = (
            (
                "matching witness",
                "downloading",
                state(witness),
                json.dumps(state(witness, filetype="mp3 v0")),
                True,
                None,
            ),
            (
                "non-downloading status",
                "wanted",
                state(witness),
                json.dumps(state(witness, filetype="mp3 v0")),
                False,
                None,
            ),
            (
                "changed stored witness",
                "downloading",
                state(other_witness),
                json.dumps(state(witness, filetype="mp3 v0")),
                False,
                None,
            ),
            (
                "equivalent timestamp with different text",
                "downloading",
                state("2026-04-03T12:00:00Z"),
                json.dumps(state(witness, filetype="mp3 v0")),
                False,
                None,
            ),
            (
                "changed outgoing witness",
                "downloading",
                state(witness),
                json.dumps(state(other_witness, filetype="mp3 v0")),
                False,
                None,
            ),
            (
                "missing stored witness",
                "downloading",
                state(None),
                json.dumps(state(witness, filetype="mp3 v0")),
                False,
                None,
            ),
            (
                "missing outgoing witness",
                "downloading",
                state(witness),
                json.dumps(state(None, filetype="mp3 v0")),
                False,
                None,
            ),
            (
                "missing stored state",
                "downloading",
                None,
                json.dumps(state(witness, filetype="mp3 v0")),
                False,
                None,
            ),
            (
                "valid non-object outgoing state",
                "downloading",
                state(witness),
                "[]",
                False,
                None,
            ),
            (
                "malformed outgoing JSON",
                "downloading",
                state(witness),
                '{"enqueued_at":',
                None,
                psycopg2.errors.InvalidTextRepresentation,
            ),
            (
                "nonstandard JSON constant",
                "downloading",
                state(witness),
                (
                    '{"filetype":"flac",'
                    f'"enqueued_at":"{witness}",'
                    '"files":[],"x":NaN}'
                ),
                None,
                psycopg2.errors.InvalidTextRepresentation,
            ),
        )

        for ordinal, (
            description,
            status,
            stored_state,
            outgoing_json,
            expected_result,
            expected_error,
        ) in enumerate(cases, start=1):
            with self.subTest(description=description):
                req_id = self.db.add_request(
                    mb_release_id=f"udsifd-parity-{ordinal}",
                    artist_name="A",
                    album_title="B",
                    source="request",
                )
                self.assertTrue(self.db.set_downloading(
                    req_id,
                    json.dumps(state(witness)),
                ))
                self.db._execute(
                    """
                    UPDATE album_requests
                    SET status = %s,
                        active_download_state = %s::jsonb
                    WHERE id = %s
                    """,
                    (
                        status,
                        (
                            json.dumps(stored_state)
                            if stored_state is not None
                            else None
                        ),
                        req_id,
                    ),
                )
                self.db.conn.commit()

                fake = FakePipelineDB()
                fake.seed_request(make_request_row(
                    id=req_id,
                    mb_release_id=f"udsifd-parity-{ordinal}",
                    status=status,
                    active_download_state=stored_state,
                ))
                real_before = self.db.get_request(req_id)
                fake_before = copy.deepcopy(fake.request(req_id))
                assert real_before is not None

                real_error: type[psycopg2.Error] | None = None
                fake_error: type[psycopg2.Error] | None = None
                real_result: bool | None = None
                fake_result: bool | None = None
                try:
                    real_result = (
                        self.db.update_download_state_if_downloading(
                            req_id,
                            outgoing_json,
                            expected_enqueued_at=witness,
                        )
                    )
                except psycopg2.Error as exc:
                    real_error = type(exc)
                try:
                    fake_result = fake.update_download_state_if_downloading(
                        req_id,
                        outgoing_json,
                        expected_enqueued_at=witness,
                    )
                except psycopg2.Error as exc:
                    fake_error = type(exc)

                self.assertIs(real_error, expected_error)
                self.assertIs(fake_error, expected_error)
                self.assertEqual(real_result, expected_result)
                self.assertEqual(fake_result, expected_result)

                real_after = self.db.get_request(req_id)
                fake_after = fake.request(req_id)
                assert real_after is not None
                if expected_result:
                    expected_state = json.loads(outgoing_json)
                    self.assertEqual(
                        real_after["active_download_state"],
                        expected_state,
                    )
                    self.assertEqual(
                        fake_after["active_download_state"],
                        expected_state,
                    )
                else:
                    self.assertEqual(real_after, real_before)
                    self.assertEqual(fake_after, fake_before)

    def test_stale_two_handle_write_preserves_new_incarnation_and_metadata(self):
        from lib.pipeline_db import PipelineDB

        witness_a = "2026-04-03T12:00:00+00:00"
        witness_b = "2026-04-03T12:01:00+00:00"
        b_updated_at = datetime(2026, 4, 3, 12, 1, tzinfo=UTC)
        shared_file = {
            "username": "user",
            "filename": "Album\\01.flac",
            "file_dir": "Album",
            "size": 123,
        }

        def state(enqueued_at: str, bytes_transferred: int) -> str:
            return json.dumps({
                "filetype": "flac",
                "enqueued_at": enqueued_at,
                "current_path": "/same/path",
                "files": [{
                    **shared_file,
                    "bytes_transferred": bytes_transferred,
                }],
            })

        req_id = self.db.add_request(
            mb_release_id="udsifd-two-handle",
            artist_name="A",
            album_title="B",
            source="request",
        )
        self.assertTrue(self.db.set_downloading(req_id, state(witness_a, 0)))
        stale_handle = PipelineDB(TEST_DSN)
        blocker_handle = PipelineDB(TEST_DSN)
        stale_thread: threading.Thread | None = None
        stale_results: list[bool] = []
        stale_errors: list[psycopg2.Error] = []

        def backend_pid(db: PipelineDB) -> int:
            row = db._execute(
                "SELECT pg_backend_pid() AS pid"
            ).fetchone()
            assert row is not None
            return int(row["pid"])

        def write_stale_state() -> None:
            try:
                stale_results.append(
                    stale_handle.update_download_state_if_downloading(
                        req_id,
                        state(witness_a, 20),
                        expected_enqueued_at=witness_a,
                    )
                )
            except psycopg2.Error as exc:
                stale_errors.append(exc)

        try:
            stale_row = stale_handle.get_request(req_id)
            assert stale_row is not None
            stale_state = stale_row["active_download_state"]
            assert stale_state is not None
            self.assertEqual(
                stale_state["enqueued_at"],
                witness_a,
            )

            expected_b_row = copy.deepcopy(self.db.get_request(req_id))
            assert expected_b_row is not None
            expected_b_row["active_download_state"] = json.loads(
                state(witness_b, 10)
            )
            expected_b_row["updated_at"] = b_updated_at

            blocker_pid = backend_pid(blocker_handle)
            stale_pid = backend_pid(stale_handle)
            with blocker_handle._atomic():
                blocker_handle._execute(
                    """
                    UPDATE album_requests
                    SET active_download_state = %s::jsonb,
                        updated_at = %s
                    WHERE id = %s
                    """,
                    (
                        state(witness_b, 10),
                        b_updated_at,
                        req_id,
                    ),
                )
                stale_thread = threading.Thread(
                    target=write_stale_state,
                    daemon=True,
                )
                stale_thread.start()

                deadline = time.monotonic() + 5
                observed_blocker = False
                while time.monotonic() < deadline:
                    row = self.db._execute(
                        """
                        SELECT pg_blocking_pids(%s) AS blockers
                        """,
                        (stale_pid,),
                    ).fetchone()
                    assert row is not None
                    blockers = row["blockers"]
                    if blocker_pid in blockers:
                        observed_blocker = True
                        break
                    time.sleep(0.01)
                self.assertTrue(
                    observed_blocker,
                    "stale writer never blocked behind the B row lock",
                )
                blocker_handle.conn.commit()

            stale_thread.join(timeout=5)
            self.assertFalse(
                stale_thread.is_alive(),
                "stale writer did not finish after the B commit",
            )
            self.assertEqual(stale_errors, [])
            self.assertEqual(stale_results, [False])
            self.assertEqual(self.db.get_request(req_id), expected_b_row)
        finally:
            blocker_handle.close()
            if stale_thread is not None:
                stale_thread.join(timeout=5)
                if stale_thread.is_alive():
                    stale_handle.conn.cancel()
                    stale_thread.join(timeout=5)
                self.assertFalse(
                    stale_thread.is_alive(),
                    "stale writer survived connection cancellation",
                )
            stale_handle.close()

    def test_reset_downloading_to_wanted_success_and_guard(self):
        req_id = self.db.add_request(
            mb_release_id="rdtw-ok",
            artist_name="A",
            album_title="B",
            source="request",
        )
        blocked_id = self.db.add_request(
            mb_release_id="rdtw-blocked",
            artist_name="C",
            album_title="D",
            source="request",
        )
        state_json = json.dumps({
            "filetype": "flac",
            "enqueued_at": "2026-04-03T12:00:00+00:00",
            "files": [],
        })
        self.db.set_downloading(req_id, state_json)
        self.db.record_attempt(req_id, "download", expected_status="downloading")

        reset = self.db.reset_downloading_to_wanted(req_id)
        blocked = self.db.reset_downloading_to_wanted(blocked_id)

        self.assertTrue(reset)
        self.assertFalse(blocked)
        req = self.db.get_request(req_id)
        blocked_req = self.db.get_request(blocked_id)
        assert req is not None
        assert blocked_req is not None
        self.assertEqual(req["status"], "wanted")
        self.assertIsNone(req["active_download_state"])
        self.assertEqual(req["download_attempts"], 1)
        self.assertEqual(blocked_req["status"], "wanted")

    def test_reset_downloading_accepts_explicit_previous_bitrate(self):
        req_id = self.db.add_request(
            mb_release_id="rdtw-prev-br",
            artist_name="A",
            album_title="B",
            source="request",
        )
        self.db.update_request_fields(
            req_id,
            min_bitrate=245,
            prev_min_bitrate=128,
        )
        self.assertTrue(self.db.set_downloading(req_id, json.dumps({
            "filetype": "flac",
            "enqueued_at": "2026-04-03T12:00:00+00:00",
            "files": [],
        })))

        applied = self.db.reset_downloading_to_wanted(
            req_id,
            min_bitrate=192,
            prev_min_bitrate=None,
        )

        self.assertTrue(applied)
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["status"], "wanted")
        self.assertEqual(req["min_bitrate"], 192)
        self.assertIsNone(req["prev_min_bitrate"])

@requires_postgres
class TestUserCooldowns(unittest.TestCase):
    """Tests for global user cooldown system (issue #39)."""

    def setUp(self):
        self.db = make_db()
        # Create two requests for cross-request cooldown testing
        self.req1 = self.db.add_request(
            mb_release_id="cool-1", artist_name="A", album_title="B", source="request")
        self.req2 = self.db.add_request(
            mb_release_id="cool-2", artist_name="C", album_title="D", source="request")

    def tearDown(self):
        self.db.close()

    def test_user_cooldowns_table_exists(self):
        cur = self.db._execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'user_cooldowns'
        """)
        row = cur.fetchone()
        assert row is not None
        self.assertEqual(row["table_name"], "user_cooldowns")

    def test_add_and_get_cooldown(self):
        from datetime import datetime, timedelta
        until = datetime.now(UTC) + timedelta(days=3)
        self.db.add_cooldown("deaduser", until, "5 consecutive timeouts")
        cooled = self.db.get_cooled_down_users()
        self.assertIn("deaduser", cooled)

    def test_expired_cooldown_not_returned(self):
        from datetime import datetime, timedelta
        past = datetime.now(UTC) - timedelta(hours=1)
        self.db.add_cooldown("expireduser", past, "old timeout")
        cooled = self.db.get_cooled_down_users()
        self.assertNotIn("expireduser", cooled)

    def test_upsert_extends_cooldown(self):
        from datetime import datetime, timedelta
        until1 = datetime.now(UTC) + timedelta(days=1)
        until2 = datetime.now(UTC) + timedelta(days=5)
        self.db.add_cooldown("user1", until1, "first")
        self.db.add_cooldown("user1", until2, "extended")
        cur = self.db._execute(
            "SELECT cooldown_until FROM user_cooldowns WHERE username = %s",
            ("user1",),
        )
        rows = cur.fetchall()
        self.assertEqual(len(rows), 1)
        # Should have the later date
        self.assertGreater(rows[0]["cooldown_until"], until1)

    def test_check_and_apply_cooldown_triggers(self):
        """5 timeouts across different requests → cooldown applied."""
        for i in range(5):
            req = self.req1 if i < 3 else self.req2
            self.db.log_download(request_id=req, soulseek_username="baduser",
                                 outcome="timeout")
        result = self.db.check_and_apply_cooldown("baduser")
        self.assertTrue(result)
        cooled = self.db.get_cooled_down_users()
        self.assertIn("baduser", cooled)

    def test_check_and_apply_cooldown_mixed_no_trigger(self):
        """3 timeouts + 2 successes → no cooldown."""
        outcomes: list[DownloadLogOutcome] = [
            "timeout", "timeout", "success", "timeout", "success"]
        for outcome in outcomes:
            self.db.log_download(request_id=self.req1, soulseek_username="mixeduser",
                                 outcome=outcome)
        result = self.db.check_and_apply_cooldown("mixeduser")
        self.assertFalse(result)
        cooled = self.db.get_cooled_down_users()
        self.assertNotIn("mixeduser", cooled)

    def test_check_and_apply_cooldown_counts_multi_user_rows(self):
        """Comma-joined usernames in download_log should count for each user."""
        for i in range(5):
            req = self.req1 if i < 3 else self.req2
            self.db.log_download(
                request_id=req,
                soulseek_username="disc1user, disc2user",
                outcome="timeout",
            )
        self.assertTrue(self.db.check_and_apply_cooldown("disc1user"))
        cooled = self.db.get_cooled_down_users()
        self.assertIn("disc1user", cooled)

    def test_check_and_apply_cooldown_below_threshold(self):
        """Only 2 outcomes → not enough data → no cooldown."""
        self.db.log_download(request_id=self.req1, soulseek_username="newuser",
                             outcome="timeout")
        self.db.log_download(request_id=self.req1, soulseek_username="newuser",
                             outcome="timeout")
        result = self.db.check_and_apply_cooldown("newuser")
        self.assertFalse(result)

    def test_check_and_apply_cooldown_ignores_abandoned_auto_import_audit(self):
        """Interrupted local imports should not count as source failures."""
        for _ in range(4):
            self.db.log_download(
                request_id=self.req1,
                soulseek_username="retryuser",
                outcome="timeout",
            )
        self.db.log_download(
            request_id=self.req1,
            soulseek_username="retryuser",
            outcome="failed",
            beets_scenario="abandoned_auto_import",
        )

        result = self.db.check_and_apply_cooldown("retryuser")

        self.assertFalse(result)
        self.assertNotIn("retryuser", self.db.get_cooled_down_users())


class TestReleaseIdToLockKey(unittest.TestCase):
    """Issue #133 / #132 P1: ``release_id_to_lock_key`` must be stable
    across processes and fit int32 so it can drive the two-arg
    ``pg_advisory_lock``.

    Pure function — no PG dependency, runs without ``@requires_postgres``.
    """

    def test_same_mbid_maps_to_same_key(self) -> None:
        from lib.pipeline_db import release_id_to_lock_key
        mbid = "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"
        self.assertEqual(
            release_id_to_lock_key(mbid),
            release_id_to_lock_key(mbid))

    def test_different_mbids_produce_different_keys(self) -> None:
        """Collision is statistically possible but unlikely with the
        handful of MBIDs in this test — a regression that makes the
        hash degenerate (e.g. returning a constant) would show up here.
        """
        from lib.pipeline_db import release_id_to_lock_key
        keys = {
            release_id_to_lock_key(s)
            for s in [
                "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb",
                "cccccccc-4444-5555-6666-dddddddddddd",
                "eeeeeeee-7777-8888-9999-ffffffffffff",
                "12856590",   # Discogs numeric id
                "1073741824",  # Discogs numeric
                "",            # edge case: empty string → hash(b"") = 0
            ]
        }
        self.assertEqual(len(keys), 6)

    def test_key_fits_non_negative_int32(self) -> None:
        """``pg_advisory_lock(int4, int4)`` takes signed int32; we mask
        to 31 bits so the value is always in [0, 2^31-1]. Negative keys
        work too in PG but keeping them non-negative makes ``pg_locks``
        rows readable during debugging."""
        from lib.pipeline_db import release_id_to_lock_key
        for s in [
                "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb",
                "cccccccc-4444-5555-6666-dddddddddddd",
                "12856590",
                "xxxxxxxx-yyyy-zzzz-wwww-vvvvvvvvvvvv",
                "",
        ]:
            k = release_id_to_lock_key(s)
            self.assertGreaterEqual(k, 0)
            self.assertLess(k, 1 << 31)

    def test_key_is_stable_across_imports(self) -> None:
        """Sanity: the function does NOT use ``hash()`` (which is salted
        per-interpreter and would break cross-process locking). Re-import
        the module and verify the same input still maps to the same key.
        """
        import importlib

        from lib import pipeline_db
        mbid = "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"
        k1 = pipeline_db.release_id_to_lock_key(mbid)
        importlib.reload(pipeline_db)
        k2 = pipeline_db.release_id_to_lock_key(mbid)
        self.assertEqual(k1, k2)

    def test_whitespace_is_stripped_before_hashing(self) -> None:
        """Legacy DB rows sometimes carry stray leading/trailing
        whitespace on ``mb_release_id``. Two processes that normalise
        differently would otherwise hash to different keys and the
        advisory lock would silently fail to serialise them. ``.strip()``
        before hashing closes that gap."""
        from lib.pipeline_db import release_id_to_lock_key
        mbid = "12856590"
        self.assertEqual(
            release_id_to_lock_key(mbid),
            release_id_to_lock_key(f" {mbid}"))
        self.assertEqual(
            release_id_to_lock_key(mbid),
            release_id_to_lock_key(f"{mbid}\t"))
        self.assertEqual(
            release_id_to_lock_key(mbid),
            release_id_to_lock_key(f"  {mbid}\n"))


@requires_postgres
class TestAdvisoryLock(unittest.TestCase):
    """Issue #92: ``PipelineDB.advisory_lock`` must cross-session-serialize.

    A second session trying the same ``(namespace, key)`` must see ``False``
    while the first session holds the lock, and must succeed once the first
    session releases. This guards the force-import concurrency fix
    in ``dispatch_import_from_db``.
    """

    NS = 0x46494D50  # ADVISORY_LOCK_NAMESPACE_IMPORT

    def setUp(self):
        self.db1 = make_db()  # truncates + gives us a fresh connection
        from lib import pipeline_db
        self.db2 = pipeline_db.PipelineDB(TEST_DSN)

    def tearDown(self):
        self.db1.close()
        self.db2.close()

    def test_lock_acquired_when_free(self):
        with self.db1.advisory_lock(self.NS, 12345) as acquired:
            self.assertTrue(acquired)

    def test_second_session_blocked_then_unblocked(self):
        with self.db1.advisory_lock(self.NS, 12345) as acquired1:
            self.assertTrue(acquired1)
            with self.db2.advisory_lock(self.NS, 12345) as acquired2:
                self.assertFalse(acquired2)
        # After the first session releases, the second can acquire.
        with self.db2.advisory_lock(self.NS, 12345) as acquired3:
            self.assertTrue(acquired3)

    def test_different_keys_do_not_collide(self):
        with self.db1.advisory_lock(self.NS, 12345) as a1:
            self.assertTrue(a1)
            with self.db2.advisory_lock(self.NS, 67890) as a2:
                self.assertTrue(a2)

    def test_lock_released_on_exception(self):
        """Exception raised inside the with-block must still release the lock."""
        with self.assertRaises(RuntimeError), self.db1.advisory_lock(self.NS, 12345) as acquired:
            self.assertTrue(acquired)
            raise RuntimeError("boom")
        # Lock must be free now — a different session can acquire it.
        with self.db2.advisory_lock(self.NS, 12345) as a2:
            self.assertTrue(a2)

    def test_release_namespace_isolated_from_import_namespace(self):
        """Issue #133 / #132 P1: the RELEASE lock namespace must not
        collide with the IMPORT lock namespace. Holding one in session A
        must not prevent session B from acquiring the other at the same
        integer key — they are logically unrelated resources.
        """
        from lib.pipeline_db import (
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            ADVISORY_LOCK_NAMESPACE_RELEASE,
        )
        with self.db1.advisory_lock(
                ADVISORY_LOCK_NAMESPACE_IMPORT, 12345) as a1:
            self.assertTrue(a1)
            with self.db2.advisory_lock(
                    ADVISORY_LOCK_NAMESPACE_RELEASE, 12345) as a2:
                self.assertTrue(a2)

    def test_reentrant_within_same_session(self):
        """``docs/advisory-locks.md`` depends on within-session
        reentrancy: the auto path's outer ``_handle_valid_result``
        acquire and ``dispatch_import_core``'s inner acquire on the
        same key both succeed because they share a session.

        Two acquires from the same session both return True; the inner
        release must be a no-op (the outer context still prevents a
        second session from acquiring). Only after the outer context
        releases does a second session succeed.
        """
        with self.db1.advisory_lock(self.NS, 12345) as outer:
            self.assertTrue(outer)
            with self.db1.advisory_lock(self.NS, 12345) as inner:
                self.assertTrue(inner)
                # Inner release happens on __exit__; outer still holds.
                # Second session must still be locked out.
                with self.db2.advisory_lock(self.NS, 12345) as other:
                    self.assertFalse(other)
            # Back in the outer context after the inner release — the
            # second session must STILL be blocked (outer still holds).
            with self.db2.advisory_lock(self.NS, 12345) as other:
                self.assertFalse(other)
        # After the outer context releases, the second session can
        # finally acquire.
        with self.db2.advisory_lock(self.NS, 12345) as other:
            self.assertTrue(other)

    def test_wrong_match_cleanup_namespace_isolated(self):
        from lib.pipeline_db import (
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            ADVISORY_LOCK_NAMESPACE_WRONG_MATCH_CLEANUP,
            wrong_match_cleanup_lock_key,
        )

        key = wrong_match_cleanup_lock_key(42, 77, "/failed/Artist - Album")
        with self.db1.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            key,
        ) as import_lock:
            self.assertTrue(import_lock)
            with self.db2.advisory_lock(
                ADVISORY_LOCK_NAMESPACE_WRONG_MATCH_CLEANUP,
                key,
            ) as cleanup_lock:
                self.assertTrue(cleanup_lock)


@requires_postgres
class TestGetWrongMatches(unittest.TestCase):
    """Issue #113: every rejected row with a failed_path must be reachable.

    The previous ``DISTINCT ON (request_id)`` collapsed every rejection for a
    request to the newest row, hiding older failed_imports dirs on disk.
    ``get_wrong_matches`` now returns one row per eligible ``download_log``
    entry so the web UI can group and expand them for per-candidate actions.
    """

    def setUp(self):
        self.db = make_db()
        self.req1 = self.db.add_request(
            mb_release_id="wm-uuid-1", artist_name="Artist 1",
            album_title="Album 1", source="request")
        self.req2 = self.db.add_request(
            mb_release_id="wm-uuid-2", artist_name="Artist 2",
            album_title="Album 2", source="request")

    def tearDown(self):
        self.db.close()

    def _log_rejected(self, request_id: int, username: str,
                      failed_path: str | None,
                      scenario: str | None = "high_distance") -> None:
        vr: dict[str, object] = {"scenario": scenario, "distance": 0.25}
        if failed_path is not None:
            vr["failed_path"] = failed_path
        self.db.log_download(
            request_id=request_id,
            soulseek_username=username,
            outcome="rejected",
            validation_result=json.dumps(vr),
        )

    def test_wrong_match_rows_carry_both_accusation_column_blocks(self):
        """Issue #829 PR4/N3: real PG must return BOTH evidence joins.

        The per-entry chip reads the candidate's codec facts and the group
        badge reads the installed copy's, so an alias pointing at the
        wrong join, or missing entirely, silently reverts a surface to
        accusing an audit-only codec. Asserted through the production
        adapter, not by eyeballing column names.
        """
        from lib.pipeline_db._shared import (
            CANDIDATE_EVIDENCE_PREFIX,
            CURRENT_EVIDENCE_PREFIX,
        )
        from lib.quality import (
            AudioQualityMeasurement,
            CodecFamily,
            EvidenceSubject,
        )
        from web.classify import (
            AccusationFlags,
            evidence_column_accusation_flags,
        )

        def _link(
            subject: EvidenceSubject, fmt: str, family: CodecFamily,
            path: str,
        ) -> int:
            # Distinct files, so the content-addressed fingerprint differs
            # and the two upserts are two rows rather than one overwrite.
            evidence = make_album_quality_evidence(
                mb_release_id="wm-uuid-1",
                source_path=path,
                files=[AlbumQualityEvidenceFile(
                    relative_path=f"01 - {subject}.{fmt.lower()}",
                    size_bytes=123456,
                    mtime_ns=1_700_000_000_000_000_000,
                    extension=fmt.lower(),
                    container=fmt.lower(),
                    codec=family,
                )],
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=256, avg_bitrate_kbps=256, is_cbr=True,
                    format=fmt, spectral_grade="likely_transcode",
                    spectral_bitrate_kbps=128, spectral_subject=subject,
                    spectral_provenance="measured", cliff_hz=15000,
                    codec_family=family, spectral_measurement_version=2,
                ),
                codec=family, container=fmt.lower(), storage_format=fmt,
            )
            self.db.upsert_album_quality_evidence(evidence)
            persisted = self.db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert persisted is not None and persisted.id is not None
            return persisted.id

        self._log_rejected(self.req1, "peer", "/failed/Pressing")
        log_id = self.db.get_wrong_matches()[0]["download_log_id"]
        # The candidate is an audit-only AAC; the installed copy is an MP3
        # whose cliff IS admissible, so a swapped join is visible.
        self.db.set_download_log_candidate_evidence(
            log_id, _link("source", "AAC", "aac", "/slskd/candidate"))
        self.assertTrue(self.db.set_request_current_evidence(
            self.req1, _link("installed", "MP3", "mp3", "/Beets/installed")))

        row = self.db.get_wrong_matches()[0]

        self.assertEqual(
            evidence_column_accusation_flags(
                row, prefix=CANDIDATE_EVIDENCE_PREFIX),
            AccusationFlags(admissible=False, withheld="audit_only_codec"),
        )
        self.assertEqual(
            evidence_column_accusation_flags(
                row, prefix=CURRENT_EVIDENCE_PREFIX),
            AccusationFlags(admissible=True),
        )

    def test_shared_wrong_match_projects_candidate_source_lineage(self):
        """A current-linked canonical row cannot lend lineage to a candidate."""
        from lib.pipeline_db._shared import (
            CANDIDATE_EVIDENCE_PREFIX,
            CURRENT_EVIDENCE_PREFIX,
        )
        from web.classify import (
            AccusationFlags,
            evidence_column_accusation_flags,
        )

        self._log_rejected(self.req1, "peer", "/failed/Shared")
        log_id = self.db.get_wrong_matches()[0]["download_log_id"]
        shared = make_album_quality_evidence(
            mb_release_id="wm-uuid-1",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=128,
                avg_bitrate_kbps=130,
                median_bitrate_kbps=129,
                format="Opus",
                spectral_grade="likely_transcode",
                spectral_bitrate_kbps=128,
                spectral_subject="source",
                spectral_provenance="measured",
                was_converted_from="flac",
            ),
            codec="opus",
            container="opus",
            storage_format="Opus",
        )
        self.db.upsert_album_quality_evidence(shared)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=shared.mb_release_id,
            snapshot_fingerprint=shared.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.db.set_download_log_candidate_evidence(log_id, stored.id)
        self.assertTrue(
            self.db.set_request_current_evidence(self.req1, stored.id)
        )

        row = self.db.get_wrong_matches()[0]

        self.assertIsNone(row["_evidence_was_converted_from"])
        self.assertEqual(
            evidence_column_accusation_flags(
                row, prefix=CANDIDATE_EVIDENCE_PREFIX,
            ),
            AccusationFlags(admissible=False, withheld="audit_only_codec"),
        )
        self.assertEqual(
            row["_current_evidence_was_converted_from"], "flac",
        )
        self.assertEqual(
            evidence_column_accusation_flags(
                row, prefix=CURRENT_EVIDENCE_PREFIX,
            ),
            AccusationFlags(admissible=True),
        )
        reloaded = self.db.load_album_quality_evidence_by_id(stored.id)
        assert reloaded is not None
        self.assertEqual(reloaded.measurement.was_converted_from, "flac")

    def test_terminal_audio_corrupt_retained_auto_import_is_not_wrong_match(self):
        """#867: terminal evidence outranks an earlier strong match envelope.

        The historical ``post_commit_quarantine`` audit key has no current
        writer (issue #1077, D3: ``audio_corrupt`` is ban + delete, never
        quarantined) — this row's exclusion from Wrong Matches rests
        entirely on ``import_result->>'decision'``, exactly as it does for a
        freshly ban+deleted row with no quarantine folder at all.
        """
        corrupt_log_id = self.db.log_download(
            request_id=self.req1, soulseek_username="corrupt-peer", outcome="rejected",
            validation_result=json.dumps({"scenario": "strong_match", "distance": 0.1247,
                                          "failed_path": "/Incoming/auto-import/Corrupt"}),
            import_result=json.dumps({"decision": "audio_corrupt"}),
        )
        evidence = make_album_quality_evidence(
            mb_release_id="wm-uuid-1", audio_corrupt=True,
        )
        self.db.upsert_album_quality_evidence(evidence)
        persisted = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        self.db.set_download_log_candidate_evidence(corrupt_log_id, persisted.id)
        self._log_rejected(self.req2, "legitimate-peer", "/failed/Pressing", "high_distance")

        rows = self.db.get_wrong_matches()

        self.assertEqual([row["soulseek_username"] for row in rows], ["legitimate-peer"])

    def test_newer_terminal_corrupt_hides_same_path_older_legitimate_row(self):
        self._log_rejected(self.req1, "older-legitimate", "/same/path", "high_distance")
        self.db.log_download(
            request_id=self.req1, soulseek_username="newer-corrupt", outcome="rejected",
            validation_result=json.dumps({"scenario": "strong_match", "distance": 0.1247,
                                          "failed_path": "/same/path"}),
            import_result=json.dumps({"decision": "audio_corrupt"}),
        )
        self._log_rejected(self.req2, "other-legitimate", "/other/path", "high_distance")
        self.assertEqual(
            [row["soulseek_username"] for row in self.db.get_wrong_matches()],
            ["other-legitimate"],
        )

    def test_newer_legitimate_reuse_surfaces_after_older_terminal_corrupt(self):
        self.db.log_download(
            request_id=self.req1, soulseek_username="older-corrupt", outcome="rejected",
            validation_result=json.dumps({"scenario": "strong_match", "distance": 0.1247,
                                          "failed_path": "/same/path"}),
            import_result=json.dumps({"decision": "audio_corrupt"}),
        )
        self._log_rejected(self.req1, "newer-legitimate", "/same/path", "high_distance")
        rows = self.db.get_wrong_matches()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["soulseek_username"], "newer-legitimate")

    def test_returns_every_rejected_row_for_same_request(self):
        """RED for issue #113: three rejected rows with failed_path → three returned."""
        self._log_rejected(self.req1, "alice", "/fi/path_0")
        self._log_rejected(self.req1, "bob",   "/fi/path_1")
        self._log_rejected(self.req1, "carol", "/fi/path_2")

        rows = self.db.get_wrong_matches()
        self.assertEqual(
            len(rows), 3,
            f"Expected all 3 rejections for request {self.req1}, got {len(rows)}. "
            f"DISTINCT ON is collapsing them.")
        self.assertEqual({r["request_id"] for r in rows}, {self.req1})
        self.assertEqual(
            {r["soulseek_username"] for r in rows},
            {"alice", "bob", "carol"})

    def test_rows_ordered_newest_first_per_request(self):
        """Within a request, rows must be ordered by download_log id DESC."""
        self._log_rejected(self.req1, "oldest",  "/fi/a")
        self._log_rejected(self.req1, "middle",  "/fi/b")
        self._log_rejected(self.req1, "newest",  "/fi/c")

        rows = self.db.get_wrong_matches()
        usernames = [r["soulseek_username"] for r in rows]
        self.assertEqual(usernames, ["newest", "middle", "oldest"])

    def test_rows_across_multiple_requests(self):
        """Every eligible row across multiple requests is returned."""
        self._log_rejected(self.req1, "r1-a", "/fi/1a")
        self._log_rejected(self.req1, "r1-b", "/fi/1b")
        self._log_rejected(self.req2, "r2-a", "/fi/2a")
        self._log_rejected(self.req2, "r2-b", "/fi/2b")

        rows = self.db.get_wrong_matches()
        self.assertEqual(len(rows), 4)
        by_req: dict[int, list[str]] = {}
        for r in rows:
            rid = r["request_id"]
            assert isinstance(rid, int)
            user = r["soulseek_username"]
            assert isinstance(user, str)
            by_req.setdefault(rid, []).append(user)
        self.assertEqual(sorted(by_req[self.req1]), ["r1-a", "r1-b"])
        self.assertEqual(sorted(by_req[self.req2]), ["r2-a", "r2-b"])

    def test_excludes_rows_with_null_failed_path(self):
        self._log_rejected(self.req1, "has-path",  "/fi/ok")
        self._log_rejected(self.req1, "no-path",   None)

        rows = self.db.get_wrong_matches()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["soulseek_username"], "has-path")

    def test_null_scenario_remains_visible(self):
        """SQL NULL must survive the excluded-scenario array predicate."""
        self._log_rejected(self.req1, "null-scenario", "/fi/null", scenario=None)

        rows = self.db.get_wrong_matches()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["soulseek_username"], "null-scenario")

    def test_excludes_every_non_match_rejection_scenario(self):
        from lib.wrong_match_policy import WRONG_MATCH_EXCLUDED_REJECTION_SCENARIOS

        self._log_rejected(self.req1, "ok", "/fi/keep", scenario="high_distance")
        for index, scenario in enumerate(
            sorted(WRONG_MATCH_EXCLUDED_REJECTION_SCENARIOS)
        ):
            self._log_rejected(
                self.req1,
                scenario,
                f"/fi/drop-{index}",
                scenario=scenario,
            )

        rows = self.db.get_wrong_matches()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["soulseek_username"], "ok")

    def test_deduplicates_same_failed_path_per_request(self):
        """Codex round 2: when the same folder is retried and rejected repeatedly,
        `download_log` accumulates duplicate rows for the same `failed_path`.
        The UI must show one row per actionable folder, not one per log entry.
        Keeps the newest row per `(request_id, failed_path)` pair.
        """
        # Live pattern: slskd reuses the `_9` suffix after the folder is
        # deleted, so the same failed_path can appear on two distinct rejected
        # download_log rows (older one is stale, newer is actionable).
        self._log_rejected(self.req1, "alice-old", "/fi/path_9")
        self._log_rejected(self.req1, "alice-new", "/fi/path_9")  # same path, newer
        self._log_rejected(self.req1, "bob",       "/fi/path_8")

        rows = self.db.get_wrong_matches()
        self.assertEqual(
            len(rows), 2,
            f"Expected 2 distinct folders (_9, _8), got {len(rows)}. "
            f"Same failed_path should collapse to newest row.")
        by_path = {
            r["soulseek_username"]: r for r in rows
        }
        # The surviving row for path_9 must be the newest ("alice-new"),
        # not the stale "alice-old".
        self.assertIn("alice-new", by_path)
        self.assertNotIn("alice-old", by_path)
        self.assertIn("bob", by_path)

    def test_clear_wrong_match_paths_clears_matching_request_and_paths(self):
        """Force-import cleanup clears every observed representation of one source."""
        self._log_rejected(self.req1, "raw", "failed_imports/Album")
        self._log_rejected(self.req1, "absolute", "/abs/Album")
        self._log_rejected(self.req1, "other-path", "/abs/Other")
        self._log_rejected(self.req2, "other-request", "/abs/Album")
        self.db.log_download(
            request_id=self.req1,
            soulseek_username="successful",
            outcome="success",
            validation_result=json.dumps({"failed_path": "/abs/Album"}),
        )

        cleared = self.db.clear_wrong_match_paths(
            self.req1, ["failed_imports/Album", "/abs/Album"])

        self.assertEqual(cleared, 2)
        rows = self.db.get_wrong_matches()
        remaining = {
            (r["request_id"], r["soulseek_username"])
            for r in rows
        }
        self.assertEqual(remaining, {
            (self.req1, "other-path"),
            (self.req2, "other-request"),
        })

    def test_excludes_non_rejected_outcomes(self):
        """success / force_import / timeout must never surface in wrong-matches."""
        self._log_rejected(self.req1, "reject-me", "/fi/keep")
        self.db.log_download(
            request_id=self.req1, soulseek_username="success-u",
            outcome="success",
            validation_result=json.dumps({"failed_path": "/fi/no-1"}))
        self.db.log_download(
            request_id=self.req1, soulseek_username="force-u",
            outcome="force_import",
            validation_result=json.dumps({"failed_path": "/fi/no-2"}))
        self.db.log_download(
            request_id=self.req1, soulseek_username="timeout-u",
            outcome="timeout",
            validation_result=json.dumps({"failed_path": "/fi/no-3"}))

        rows = self.db.get_wrong_matches()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["soulseek_username"], "reject-me")

    def test_result_shape_has_required_fields(self):
        """Each row must carry the fields the route layer reads."""
        self._log_rejected(self.req1, "alice", "/fi/a")

        rows = self.db.get_wrong_matches()
        self.assertEqual(len(rows), 1)
        row = rows[0]
        for field in ("download_log_id", "request_id", "artist_name",
                      "album_title", "mb_release_id", "soulseek_username",
                      "validation_result"):
            self.assertIn(field, row)

    def test_result_exposes_per_attempt_spectral_and_v0_probe_columns(self):
        """Per-candidate evidence (download_log columns) is projected.

        The Wrong Matches tab needs per-attempt spectral grade/floor and
        lossless-source V0 probe average to let the operator eyeball
        candidates by audio quality before destructive actions. These
        columns already exist on ``download_log`` (migrations 001/007);
        ``get_wrong_matches`` must surface them.
        """
        self.db.log_download(
            request_id=self.req1,
            soulseek_username="alice",
            outcome="rejected",
            spectral_grade="suspect",
            spectral_bitrate=320,
            v0_probe_kind="lossless_source_v0",
            v0_probe_avg_bitrate=265,
            validation_result=json.dumps({
                "scenario": "high_distance",
                "distance": 0.25,
                "failed_path": "/fi/path_a",
            }),
        )

        rows = self.db.get_wrong_matches()
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["spectral_grade"], "suspect")
        self.assertEqual(row["spectral_bitrate"], 320)
        self.assertEqual(row["v0_probe_kind"], "lossless_source_v0")
        self.assertEqual(row["v0_probe_avg_bitrate"], 265)

    def test_result_per_attempt_evidence_keys_present_when_null(self):
        """Legacy rows (pre-migration-007 / pre-spectral) come back with
        the four keys present and ``None`` — never missing."""
        self._log_rejected(self.req1, "alice", "/fi/legacy")

        rows = self.db.get_wrong_matches()
        self.assertEqual(len(rows), 1)
        row = rows[0]
        for field in ("spectral_grade", "spectral_bitrate",
                      "v0_probe_kind", "v0_probe_avg_bitrate"):
            self.assertIn(field, row)
            self.assertIsNone(row[field])

    def test_result_dedup_keeps_newer_evidence(self):
        """When the same failed_path is retried, the newer attempt wins —
        including its per-attempt spectral/V0 evidence."""
        self.db.log_download(
            request_id=self.req1,
            soulseek_username="alice-old",
            outcome="rejected",
            spectral_grade="genuine",
            spectral_bitrate=900,
            validation_result=json.dumps({
                "scenario": "high_distance",
                "failed_path": "/fi/path_dup",
            }),
        )
        self.db.log_download(
            request_id=self.req1,
            soulseek_username="alice-new",
            outcome="rejected",
            spectral_grade="suspect",
            spectral_bitrate=280,
            v0_probe_kind="lossless_source_v0",
            v0_probe_avg_bitrate=255,
            validation_result=json.dumps({
                "scenario": "high_distance",
                "failed_path": "/fi/path_dup",
            }),
        )

        rows = self.db.get_wrong_matches()
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["soulseek_username"], "alice-new")
        self.assertEqual(row["spectral_grade"], "suspect")
        self.assertEqual(row["spectral_bitrate"], 280)
        self.assertEqual(row["v0_probe_avg_bitrate"], 255)

    def test_result_surfaces_evidence_when_denorm_columns_are_null(self):
        """RED guard: ``get_wrong_matches`` must join album_quality_evidence.

        Live rejections write the canonical measurement to
        ``album_quality_evidence`` and link it via
        ``download_log.candidate_evidence_id``; the legacy denorm columns
        on ``download_log`` (``spectral_grade`` etc.) stay NULL because
        the wrong-match path rejects before the denorm-writing dispatch
        runs. The SQL must LEFT JOIN the evidence row and prefer it over
        the denorm columns; otherwise every wrong-match candidate
        silently surfaces as ``spectral=None / format=None`` in the UI.

        Reproduces the regression that motivated this slice — the route
        was reading ``dl.spectral_grade`` directly and showing dashes for
        every actual rejection on prod (every candidate evidence row was
        populated; nothing surfaced).
        """
        from lib.quality import (
            AlbumQualityEvidenceFile,
            AlbumQualityV0Metric,
            AudioQualityMeasurement,
            VerifiedLosslessProof,
        )

        # log_download intentionally writes NO denorm spectral / V0
        # values — this mirrors the live wrong-match-reject row shape.
        log_id = self.db.log_download(
            request_id=self.req1,
            soulseek_username="alice",
            outcome="rejected",
            validation_result=json.dumps({
                "scenario": "high_distance",
                "failed_path": "/fi/evidence-only",
            }),
        )

        # Seed canonical evidence and link it to the download_log row.
        evidence = make_album_quality_evidence(
            mb_release_id="wm-uuid-1",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=0,
                avg_bitrate_kbps=920,
                median_bitrate_kbps=900,
                format="FLAC",
                spectral_grade="genuine",
                spectral_bitrate_kbps=21,
                spectral_subject="source",
                spectral_provenance="measured",
            ),
            files=[
                AlbumQualityEvidenceFile(
                    relative_path="01.flac",
                    size_bytes=1000,
                    mtime_ns=10,
                    extension="flac",
                    container="flac",
                    codec="flac",
                ),
            ],
            codec="flac",
            container="flac",
            storage_format="FLAC",
            target_format="opus 128",
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=220,
                avg_bitrate_kbps=265,
                median_bitrate_kbps=260,
                subject="source",
                provenance="measured",
            ),
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="measured",
                source="real wire shape",
                classifier="spectral+v0",
                detail=None,
            ),
        )
        self.db.upsert_album_quality_evidence(evidence)
        persisted = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        self.db.set_download_log_candidate_evidence(log_id, persisted.id)

        rows = self.db.get_wrong_matches()
        self.assertEqual(len(rows), 1)
        row = rows[0]

        # Evidence-derived spectral and V0 fields (COALESCEd against the
        # NULL denorm columns) reach the row payload.
        self.assertEqual(row["spectral_grade"], "genuine")
        self.assertEqual(row["spectral_bitrate"], 21)
        self.assertEqual(row["v0_probe_kind"], "lossless_source_v0")
        self.assertEqual(row["v0_probe_avg_bitrate"], 265)

        # New evidence-only fields surfaced for the entry quality badge.
        self.assertEqual(row["evidence_source_codec"], "flac")
        self.assertEqual(row["evidence_source_container"], "flac")
        self.assertEqual(row["evidence_storage_format"], "FLAC")
        self.assertEqual(row["evidence_target_format"], "opus 128")
        self.assertFalse(row["evidence_target_is_cbr"])
        self.assertEqual(row["evidence_min_bitrate"], 0)
        self.assertTrue(row["evidence_verified_lossless"])

    def test_download_history_seams_overlay_evidence_onto_legacy_columns(self):
        """RED guard: every download_log read seam overlays evidence.

        The denorm spectral / V0 columns on download_log are NULL whenever
        a candidate was rejected before the dispatch backfill ran. The
        per-request download-history view (pipeline detail tab) reads
        rows through ``get_download_history`` /
        ``get_download_history_batch`` / ``get_download_log_entry`` and
        feeds them to ``LogEntry.from_row`` which extracts
        ``spectral_grade`` / ``v0_probe_kind`` directly. Without an
        evidence overlay every wrong-match row in the audit trail
        silently shows blank spectral / V0 evidence — same regression
        class as ``get_wrong_matches`` itself.
        """
        from lib.quality import (
            AlbumQualityEvidenceFile,
            AlbumQualityV0Metric,
            AudioQualityMeasurement,
        )

        # NO denorm values on the row — only evidence.
        log_id = self.db.log_download(
            request_id=self.req1,
            soulseek_username="alice",
            outcome="rejected",
            validation_result=json.dumps({
                "scenario": "high_distance",
                "failed_path": "/fi/history-evidence-only",
            }),
        )
        evidence = make_album_quality_evidence(
            mb_release_id="wm-uuid-1",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=0,
                avg_bitrate_kbps=900,
                median_bitrate_kbps=880,
                format="FLAC",
                spectral_grade="suspect",
                spectral_bitrate_kbps=18,
                spectral_subject="source",
                spectral_provenance="measured",
            ),
            files=[
                AlbumQualityEvidenceFile(
                    relative_path="01.flac",
                    size_bytes=1000,
                    mtime_ns=10,
                    extension="flac",
                    container="flac",
                    codec="flac",
                ),
            ],
            codec="flac",
            container="flac",
            storage_format="FLAC",
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=200,
                avg_bitrate_kbps=245,
                median_bitrate_kbps=240,
                subject="source",
                provenance="measured",
            ),
        )
        self.db.upsert_album_quality_evidence(evidence)
        persisted = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        self.db.set_download_log_candidate_evidence(log_id, persisted.id)

        # All three reader seams must overlay evidence onto the row.
        # The overlay translates the evidence lineage label
        # (``lossless_source``) into the download_log probe-kind wire shape
        # (``lossless_source_v0``) so the frontend renderer's kind-aware
        # branches match.
        entry = self.db.get_download_log_entry(log_id)
        assert entry is not None
        self.assertEqual(entry["spectral_grade"], "suspect")
        self.assertEqual(entry["spectral_bitrate"], 18)
        self.assertEqual(entry["v0_probe_kind"], "lossless_source_v0")
        self.assertEqual(entry["v0_probe_min_bitrate"], 200)
        self.assertEqual(entry["v0_probe_avg_bitrate"], 245)
        self.assertEqual(entry["v0_probe_median_bitrate"], 240)

        history = self.db.get_download_history(self.req1)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["spectral_grade"], "suspect")
        self.assertEqual(history[0]["v0_probe_avg_bitrate"], 245)

        batch = self.db.get_download_history_batch([self.req1])
        self.assertEqual(batch[self.req1][0]["spectral_grade"], "suspect")
        self.assertEqual(batch[self.req1][0]["v0_probe_kind"], "lossless_source_v0")

        recent = {row["id"]: row for row in self.db.get_log(limit=50)}
        self.assertEqual(recent[log_id]["spectral_grade"], "suspect")
        self.assertEqual(recent[log_id]["v0_probe_min_bitrate"], 200)
        self.assertEqual(recent[log_id]["v0_probe_avg_bitrate"], 245)
        self.assertEqual(recent[log_id]["v0_probe_median_bitrate"], 240)

    def test_v1_download_history_overlay_fails_closed_for_source_projection(self):
        """Historical target-shaped measurements never become source facts."""
        from lib.quality import (
            AlbumQualityV0Metric,
            AudioQualityMeasurement,
        )

        log_id = self.db.log_download(
            request_id=self.req1,
            soulseek_username="historical-v1",
            outcome="rejected",
        )
        evidence = make_album_quality_evidence(
            mb_release_id="wm-uuid-1",
            lineage_version=1,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=121,
                avg_bitrate_kbps=128,
                median_bitrate_kbps=127,
                format="OPUS 128",
                spectral_grade="likely_transcode",
                spectral_bitrate_kbps=96,
            ),
            storage_format="OPUS 128",
            v0_metric=AlbumQualityV0Metric(
                provenance="measured",
                min_bitrate_kbps=201,
                avg_bitrate_kbps=259,
                median_bitrate_kbps=255,
                subject="installed",
            ),
        )
        self.db.upsert_album_quality_evidence(evidence)
        persisted = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        self.db.set_download_log_candidate_evidence(log_id, persisted.id)

        entry = self.db.get_download_log_entry(log_id)
        assert entry is not None
        self.assertIsNone(entry.get("source_format"))
        self.assertIsNone(entry.get("source_min_bitrate"))
        self.assertIsNone(entry.get("source_avg_bitrate"))
        self.assertIsNone(entry.get("source_median_bitrate"))
        self.assertEqual(entry["spectral_grade"], "likely_transcode")
        self.assertEqual(entry["spectral_bitrate"], 96)
        self.assertEqual(entry["v0_probe_kind"], "native_lossy_research_v0")
        self.assertEqual(entry["v0_probe_avg_bitrate"], 259)

    def test_download_history_keeps_explicit_denorm_when_evidence_missing(self):
        """Legacy rows without an evidence FK fall back to denorm columns.

        Historical download_log rows (pre-evidence) populated
        spectral_grade directly; the overlay must leave those alone so
        the audit trail doesn't lose data when evidence is absent.
        """
        log_id = self.db.log_download(
            request_id=self.req1,
            soulseek_username="historical",
            outcome="rejected",
            beets_scenario="high_distance",
            spectral_grade="genuine",
            spectral_bitrate=920,
            validation_result=json.dumps({
                "failed_path": "/fi/historical",
            }),
        )

        entry = self.db.get_download_log_entry(log_id)
        assert entry is not None
        self.assertEqual(entry["spectral_grade"], "genuine")
        self.assertEqual(entry["spectral_bitrate"], 920)
        self.assertIsNone(entry["v0_probe_kind"])

    def test_result_carries_current_request_quality_fields(self):
        """Row must expose the request's on-disk quality state.

        The wrong-matches tab needs to show the current album's quality at
        the group level so the user can judge whether force-importing is
        worthwhile. That data lives on ``album_requests`` (status,
        min_bitrate, verified_lossless, spectral pair) and is pulled in via
        the existing JOIN.
        """
        # Seed the request with imported-quality state.
        self.db._execute(
            "UPDATE album_requests SET status = %s, min_bitrate = %s, "
            "verified_lossless = %s, current_spectral_grade = %s, "
            "current_spectral_bitrate = %s "
            "WHERE id = %s",
            ("imported", 207, True, "genuine", None, self.req1),
        )
        self._log_rejected(self.req1, "alice", "/fi/a")

        rows = self.db.get_wrong_matches()
        row = rows[0]
        self.assertEqual(row["request_status"], "imported")
        self.assertEqual(row["request_min_bitrate"], 207)
        self.assertTrue(row["request_verified_lossless"])
        self.assertEqual(row["request_current_spectral_grade"], "genuine")
        self.assertIsNone(row["request_current_spectral_bitrate"])

    def test_get_wrong_matches_keyset_parity(self):
        """#523 -- fake<->production parity for the widest read projection.

        Seeds the SAME sequence ``_log_rejected`` runs on ``self.db``
        (real PG) onto a fresh ``FakePipelineDB``, then compares the full
        21-column wrong-match projection. Reuses
        ``TestReadProjectionParity._assert_keyset_parity`` -- it is a
        staticmethod whose first param is the ``TestCase`` instance, so
        cross-class reuse is safe.
        """
        from tests.fakes import FakePipelineDB

        fake = FakePipelineDB()
        fake_req1 = fake.add_request(
            mb_release_id="wm-uuid-1", artist_name="Artist 1",
            album_title="Album 1", source="request")

        self._log_rejected(self.req1, "alice", "/fi/parity-a")
        fake.log_download(
            request_id=fake_req1,
            soulseek_username="alice",
            outcome="rejected",
            validation_result=json.dumps({
                "scenario": "high_distance", "distance": 0.25,
                "failed_path": "/fi/parity-a",
            }),
        )

        real_rows = self.db.get_wrong_matches()
        fake_rows = fake.get_wrong_matches()
        self.assertTrue(
            real_rows, "seeding produced no rows on real PG — "
            "get_wrong_matches parity would pass vacuously")
        self.assertTrue(
            fake_rows, "seeding produced no rows on FakePipelineDB — "
            "get_wrong_matches parity would pass vacuously")
        TestReadProjectionParity._assert_keyset_parity(
            self, real_rows, fake_rows, "get_wrong_matches")


@requires_postgres
class TestBadAudioHashes(unittest.TestCase):
    """Real-DB coverage for the bad_audio_hashes helpers (plan U2)."""

    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="bad-hash-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def _hash(self, n: int) -> bytes:
        return bytes([n]) * 32

    def test_add_returns_count_for_fresh_inserts(self):
        from lib.pipeline_db import BadAudioHashInput
        inputs = [
            BadAudioHashInput(hash_value=self._hash(1), audio_format="flac"),
            BadAudioHashInput(hash_value=self._hash(2), audio_format="mp3"),
            BadAudioHashInput(hash_value=self._hash(3), audio_format="m4a"),
        ]
        n = self.db.add_bad_audio_hashes(self.req_id, "H@rco", "bad rip", inputs)
        self.assertEqual(n, 3)

    def test_add_empty_list_returns_zero(self):
        n = self.db.add_bad_audio_hashes(self.req_id, "u", "r", [])
        self.assertEqual(n, 0)

    def test_add_full_duplicate_returns_zero(self):
        from lib.pipeline_db import BadAudioHashInput
        inputs = [
            BadAudioHashInput(hash_value=self._hash(1), audio_format="flac"),
            BadAudioHashInput(hash_value=self._hash(2), audio_format="mp3"),
        ]
        first = self.db.add_bad_audio_hashes(self.req_id, "H@rco", "x", inputs)
        second = self.db.add_bad_audio_hashes(
            self.req_id, "OtherUser", "y", inputs)
        self.assertEqual(first, 2)
        self.assertEqual(second, 0)

    def test_add_partial_overlap_returns_partial_count(self):
        from lib.pipeline_db import BadAudioHashInput
        first_batch = [
            BadAudioHashInput(hash_value=self._hash(1), audio_format="flac"),
            BadAudioHashInput(hash_value=self._hash(2), audio_format="flac"),
        ]
        self.db.add_bad_audio_hashes(self.req_id, "H@rco", "x", first_batch)
        second_batch = [
            BadAudioHashInput(hash_value=self._hash(2), audio_format="flac"),
            BadAudioHashInput(hash_value=self._hash(3), audio_format="flac"),
        ]
        n = self.db.add_bad_audio_hashes(
            self.req_id, "Other", "y", second_batch)
        self.assertEqual(n, 1)

    def test_add_same_hash_different_format_both_inserted(self):
        from lib.pipeline_db import BadAudioHashInput
        inputs = [
            BadAudioHashInput(hash_value=self._hash(1), audio_format="flac"),
            BadAudioHashInput(hash_value=self._hash(1), audio_format="mp3"),
        ]
        n = self.db.add_bad_audio_hashes(self.req_id, "u", "r", inputs)
        self.assertEqual(n, 2)

    def test_lookup_hits_when_present(self):
        from lib.pipeline_db import BadAudioHashInput
        self.db.add_bad_audio_hashes(
            self.req_id, "H@rco", "x",
            [BadAudioHashInput(hash_value=self._hash(7), audio_format="flac")],
        )
        row = self.db.lookup_bad_audio_hash(self._hash(7), "flac")
        assert row is not None
        self.assertEqual(row.hash_value, self._hash(7))
        self.assertEqual(row.audio_format, "flac")
        self.assertEqual(row.request_id, self.req_id)
        self.assertEqual(row.reported_username, "H@rco")
        self.assertEqual(row.reason, "x")
        self.assertIsNotNone(row.reported_at)

    def test_lookup_miss_returns_none(self):
        self.assertIsNone(
            self.db.lookup_bad_audio_hash(self._hash(99), "flac"))

    def test_lookup_format_must_match(self):
        from lib.pipeline_db import BadAudioHashInput
        self.db.add_bad_audio_hashes(
            self.req_id, "u", "r",
            [BadAudioHashInput(hash_value=self._hash(7), audio_format="flac")],
        )
        # Same hash, different format → miss
        self.assertIsNone(
            self.db.lookup_bad_audio_hash(self._hash(7), "mp3"))
        # Same format, different hash → miss
        self.assertIsNone(
            self.db.lookup_bad_audio_hash(self._hash(8), "flac"))

    def test_has_any_false_on_fresh_table(self):
        self.assertFalse(self.db.has_any_bad_audio_hashes())

    def test_has_any_true_after_one_insert(self):
        from lib.pipeline_db import BadAudioHashInput
        self.db.add_bad_audio_hashes(
            self.req_id, None, None,
            [BadAudioHashInput(hash_value=self._hash(1), audio_format="flac")],
        )
        self.assertTrue(self.db.has_any_bad_audio_hashes())


@requires_postgres
class TestRecentSuccessfulUploader(unittest.TestCase):
    """Real-DB coverage for get_recent_successful_uploader (plan U2)."""

    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="rsu-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_returns_none_when_no_logs(self):
        self.assertIsNone(
            self.db.get_recent_successful_uploader(self.req_id))

    def test_returns_none_when_only_rejected_logs(self):
        self.db.log_download(
            self.req_id, soulseek_username="bob", outcome="rejected")
        self.db.log_download(
            self.req_id, soulseek_username="alice", outcome="failed")
        self.assertIsNone(
            self.db.get_recent_successful_uploader(self.req_id))

    def test_returns_most_recent_success_when_multiple_present(self):
        self.db.log_download(
            self.req_id, soulseek_username="alice", outcome="success")
        self.db.log_download(
            self.req_id, soulseek_username="bob", outcome="success")
        self.assertEqual(
            self.db.get_recent_successful_uploader(self.req_id), "bob")

    def test_returns_force_import_uploader(self):
        self.db.log_download(
            self.req_id, soulseek_username="alice", outcome="success")
        self.db.log_download(
            self.req_id, soulseek_username="harco", outcome="force_import")
        self.assertEqual(
            self.db.get_recent_successful_uploader(self.req_id), "harco")

    def test_isolated_per_request(self):
        other = self.db.add_request(
            mb_release_id="rsu-other",
            artist_name="A",
            album_title="C",
            source="request",
        )
        self.db.log_download(
            self.req_id, soulseek_username="alice", outcome="success")
        self.db.log_download(
            other, soulseek_username="bob", outcome="success")
        self.assertEqual(
            self.db.get_recent_successful_uploader(self.req_id), "alice")
        self.assertEqual(
            self.db.get_recent_successful_uploader(other), "bob")


@requires_postgres
class TestActiveImportJobsForWrongMatch(unittest.TestCase):
    """Real-DB coverage for Wrong Matches active-job race checks."""

    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="wm-active-uuid",
            artist_name="Wrong",
            album_title="Match",
            source="request",
        )
        self.other_req_id = self.db.add_request(
            mb_release_id="wm-active-other",
            artist_name="Other",
            album_title="Match",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_matches_active_jobs_by_row_request_path_and_source_dirs(self):
        from lib.import_queue import (
            IMPORT_JOB_FORCE,
            force_import_payload,
        )

        path = "/tmp/failed/Artist - Album"
        source_dir = "user1\\Music\\Artist\\Album"

        by_download_log = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.other_req_id,
            dedupe_key="wm:download-log",
            payload=force_import_payload(download_log_id=77, failed_path="/tmp/other"),
        )
        self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key="wm:request",
            payload={"download_log_id": 1, "failed_path": "/tmp/unrelated"},
        )
        by_path = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.other_req_id,
            dedupe_key="wm:path",
            payload={"download_log_id": 1, "failed_path": path},
        )
        by_source_dir = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.other_req_id,
            dedupe_key="wm:source-dir",
            payload=force_import_payload(
                download_log_id=88,
                failed_path="/tmp/source-dir-other",
                source_dirs=[source_dir],
            ),
        )
        self.db._execute(
            "UPDATE import_jobs SET status = 'running' WHERE id = %s",
            (by_source_dir.id,),
        )
        ignored = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key="wm:ignored",
            payload=force_import_payload(download_log_id=77, failed_path=path),
        )
        completed = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key="wm:completed",
            payload=force_import_payload(download_log_id=77, failed_path=path),
        )
        self.db.mark_import_job_completed(completed.id, result={"ok": True})

        jobs = self.db.list_active_import_jobs_for_wrong_match(
            download_log_id=77,
            request_id=self.req_id,
            failed_paths=[path],
            source_dirs=[source_dir],
            ignore_import_job_id=ignored.id,
        )

        self.assertEqual(
            {job.id for job in jobs},
            {by_download_log.id, by_path.id, by_source_dir.id},
        )


# ---------------------------------------------------------------------------
# Persisted search plans (U1)
# ---------------------------------------------------------------------------


@requires_postgres
class TestPersistedSearchPlanCRUD(unittest.TestCase):
    def setUp(self):
        from lib.pipeline_db import SearchPlanItemInput
        self.SearchPlanItemInput = SearchPlanItemInput
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="plan-crud-mbid",
            artist_name="Plan",
            album_title="CRUD",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def _items(self, *queries: str) -> list:
        return [
            self.SearchPlanItemInput(
                ordinal=i,
                strategy=f"slot_{i}",
                query=q,
                canonical_query_key=q.lower(),
                repeat_group="default" if i == 0 else None,
            )
            for i, q in enumerate(queries)
        ]

    def test_successful_plan_sets_active_and_resets_cursor(self):
        plan_id = self.db.create_successful_search_plan(
            request_id=self.req_id,
            generator_id="g1",
            items=self._items("Artist Album", "Artist Track1"),
            metadata_snapshot={"year": 2024},
            provenance={"dropped_low_entropy_tokens": ["the"]},
        )
        active = self.db.get_active_search_plan(self.req_id)
        assert active is not None
        self.assertEqual(active.plan.id, plan_id)
        self.assertEqual(active.plan.status, "active")
        self.assertEqual(active.plan.generator_id, "g1")
        self.assertEqual(active.next_ordinal, 0)
        self.assertEqual(active.cycle_count, 0)
        self.assertEqual(len(active.items), 2)
        self.assertEqual(active.items[0].ordinal, 0)
        self.assertEqual(active.items[0].query, "Artist Album")
        from lib.pipeline_db import SearchPlanMetadataSnapshot, SearchPlanProvenance
        self.assertIsInstance(
            active.plan.metadata_snapshot, SearchPlanMetadataSnapshot)
        self.assertIsInstance(active.plan.provenance, SearchPlanProvenance)
        assert active.plan.metadata_snapshot is not None
        assert active.plan.provenance is not None
        self.assertEqual(active.plan.metadata_snapshot.year, 2024)
        self.assertEqual(
            active.plan.provenance.values["dropped_low_entropy_tokens"],
            ["the"])

    def test_successful_plan_without_set_active_leaves_request_planless(self):
        plan_id = self.db.create_successful_search_plan(
            request_id=self.req_id,
            generator_id="g1",
            items=self._items("Q1"),
            set_active=False,
        )
        self.assertIsNone(self.db.get_active_search_plan(self.req_id))
        # The plan still exists.
        cur = self.db._execute(
            "SELECT status FROM search_plans WHERE id = %s", (plan_id,))
        row = cur.fetchone()
        assert row is not None
        self.assertEqual(row["status"], "active")

    def test_successful_plan_requires_items(self):
        with self.assertRaises(ValueError):
            self.db.create_successful_search_plan(
                request_id=self.req_id,
                generator_id="g1",
                items=[],
            )

    def test_deterministic_failed_plan_leaves_request_unsearchable(self):
        plan_id = self.db.create_failed_search_plan(
            request_id=self.req_id,
            generator_id="g1",
            failure_class="no_runnable_query",
            error_message="no usable artist/title query",
            transient=False,
        )
        # Request stays wanted, but no active plan -> not searchable.
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["status"], "wanted")
        self.assertIsNone(req["active_plan_id"])
        self.assertIsNone(self.db.get_active_search_plan(self.req_id))
        cur = self.db._execute(
            "SELECT status, failure_class, error_message FROM search_plans "
            "WHERE id = %s", (plan_id,))
        row = cur.fetchone()
        assert row is not None
        self.assertEqual(row["status"], "failed_deterministic")
        self.assertEqual(row["failure_class"], "no_runnable_query")
        self.assertEqual(row["error_message"], "no usable artist/title query")

    def test_transient_failed_plan_is_not_sticky(self):
        plan_id = self.db.create_failed_search_plan(
            request_id=self.req_id,
            generator_id="g1",
            failure_class="resolver_unavailable",
            transient=True,
        )
        cur = self.db._execute(
            "SELECT status FROM search_plans WHERE id = %s", (plan_id,))
        row = cur.fetchone()
        assert row is not None
        self.assertEqual(row["status"], "failed_transient")

    def test_supersede_replaces_active_plan_and_resets_cursor(self):
        first = self.db.create_successful_search_plan(
            request_id=self.req_id,
            generator_id="g1",
            items=self._items("Q1", "Q2"),
        )
        # Move cursor / cycle to non-zero so we can prove they reset.
        self.db._execute(
            "UPDATE album_requests "
            "SET next_plan_ordinal = 1, plan_cycle_count = 3 WHERE id = %s",
            (self.req_id,),
        )
        new_id = self.db.supersede_search_plan_with_replacement(
            request_id=self.req_id,
            generator_id="g2",
            items=self._items("Q3"),
        )
        # Cursor/cycle reset.
        active = self.db.get_active_search_plan(self.req_id)
        assert active is not None
        self.assertEqual(active.plan.id, new_id)
        self.assertEqual(active.next_ordinal, 0)
        self.assertEqual(active.cycle_count, 0)
        # Old plan flipped to superseded with link to replacement.
        cur = self.db._execute(
            "SELECT status, superseded_at, superseded_by_plan_id "
            "FROM search_plans WHERE id = %s", (first,))
        row = cur.fetchone()
        assert row is not None
        self.assertEqual(row["status"], "superseded")
        self.assertIsNotNone(row["superseded_at"])
        self.assertEqual(row["superseded_by_plan_id"], new_id)

    def test_get_active_search_plan_returns_items_in_ordinal_order(self):
        """Single-query plan fetch returns items ordered by ordinal with
        every column hydrated (provenance, canonical_query_key,
        repeat_group). Guards the JSONB-aggregation rewrite of
        ``get_active_search_plan`` against drift from the prior two-query
        shape.
        """
        items = [
            self.SearchPlanItemInput(
                ordinal=0, strategy="primary", query="Artist Album",
                canonical_query_key="artist album",
                repeat_group="default",
                provenance={"repeat_index": 1},
            ),
            self.SearchPlanItemInput(
                ordinal=1, strategy="track1", query="Artist Track 1",
                canonical_query_key="artist track 1",
                provenance={"source_track_index": 0, "track_slot_index": 1},
            ),
            self.SearchPlanItemInput(
                ordinal=2, strategy="track2", query="Artist Track 2",
                canonical_query_key="artist track 2",
                provenance=None,
            ),
        ]
        # Insert plan items with ordinals reversed to prove ORDER BY works.
        self.db.create_successful_search_plan(
            request_id=self.req_id, generator_id="g1",
            items=list(reversed(items)),
        )
        active = self.db.get_active_search_plan(self.req_id)
        assert active is not None
        self.assertEqual(len(active.items), 3)
        ordinals = [it.ordinal for it in active.items]
        self.assertEqual(ordinals, [0, 1, 2])
        self.assertEqual(active.items[0].query, "Artist Album")
        self.assertEqual(active.items[0].canonical_query_key, "artist album")
        self.assertEqual(active.items[0].repeat_group, "default")
        from lib.pipeline_db import SearchPlanItemProvenance
        self.assertIsInstance(
            active.items[0].provenance, SearchPlanItemProvenance)
        assert active.items[0].provenance is not None
        assert active.items[1].provenance is not None
        self.assertEqual(active.items[0].provenance.values["repeat_index"], 1)
        self.assertEqual(active.items[1].strategy, "track1")
        self.assertEqual(
            active.items[1].provenance.values["source_track_index"], 0)
        self.assertEqual(
            active.items[1].provenance.values["track_slot_index"], 1)
        self.assertIsNone(active.items[2].provenance)
        self.assertIsNone(active.items[2].repeat_group)
        # IDs must be present + ints (matched the prior two-query contract).
        for it in active.items:
            self.assertIsInstance(it.id, int)
            self.assertGreater(it.id, 0)
            self.assertEqual(it.plan_id, active.plan.id)

    def test_get_active_search_plan_handles_zero_items(self):
        """LEFT JOIN + jsonb_agg can mis-handle the zero-item case (NULL
        or ``[null]``). Confirm an active plan whose items got deleted
        out-of-band returns ``items=[]`` rather than crashing or
        constructing a row from NULLs.
        """
        plan_id = self.db.create_successful_search_plan(
            request_id=self.req_id, generator_id="g1",
            items=self._items("only"),
        )
        # Out-of-band deletion (production never does this; the migrator
        # might, or a future cleanup tool).
        self.db._execute(
            "DELETE FROM search_plan_items WHERE plan_id = %s", (plan_id,))
        active = self.db.get_active_search_plan(self.req_id)
        assert active is not None
        self.assertEqual(active.plan.id, plan_id)
        self.assertEqual(active.items, [])

    def test_replaced_request_rejects_every_plan_mutation(self):
        from lib.pipeline_db import (
            ConsumedAttemptInput,
            NonConsumingAttemptInput,
            ReplacedRequestMutationError,
        )

        plan_id = self.db.create_successful_search_plan(
            request_id=self.req_id,
            generator_id="g1",
            items=self._items("Q1", "Q2"),
        )
        active = self.db.get_active_search_plan(self.req_id)
        assert active is not None
        first = active.items[0]
        self.db.supersede_request_mbid(
            self.req_id,
            new_mb_release_id="plan-crud-replacement",
            new_mb_release_group_id=None,
            new_mb_artist_id=None,
            new_artist_name="Plan",
            new_album_title="Replacement",
            new_year=None,
            new_country=None,
            new_tracks=[],
        )
        frozen = self.db.get_request(self.req_id)
        assert frozen is not None

        with self.assertRaises(ReplacedRequestMutationError):
            self.db.create_failed_search_plan(
                request_id=self.req_id,
                generator_id="g2",
                failure_class="dependency_failure",
                transient=True,
            )
        with self.assertRaises(ReplacedRequestMutationError):
            self.db.create_successful_search_plan(
                request_id=self.req_id,
                generator_id="g2",
                items=self._items("Q3"),
            )
        with self.assertRaises(ReplacedRequestMutationError):
            self.db.supersede_search_plan_with_replacement(
                request_id=self.req_id,
                generator_id="g2",
                items=self._items("Q3"),
            )
        with self.assertRaises(ReplacedRequestMutationError):
            self.db.advance_search_plan_cursor(
                self.req_id,
                target_ordinal=1,
                plan_item_count=2,
            )

        consumed = self.db.record_consumed_search_attempt(
            ConsumedAttemptInput(
                request_id=self.req_id,
                plan_id=plan_id,
                plan_item_id=first.id,
                plan_ordinal=first.ordinal,
                plan_strategy=first.strategy,
                plan_canonical_query_key=first.canonical_query_key,
                plan_repeat_group=first.repeat_group,
                plan_generator_id="g1",
                query=first.query,
                outcome="no_results",
                plan_item_count=2,
                cycle_count_snapshot=0,
                elapsed_s=0.1,
                result_count=0,
                apply_scheduler_attempt=True,
                scheduler_success=False,
            )
        )
        self.assertTrue(consumed.is_stale)
        stale_log = self.db._execute(
            "SELECT stale_reason FROM search_log WHERE id = %s",
            (consumed.search_log_id,),
        ).fetchone()
        assert stale_log is not None
        self.assertEqual(stale_log["stale_reason"], "request_replaced")

        self.db.record_non_consuming_search_attempt(
            NonConsumingAttemptInput(
                request_id=self.req_id,
                outcome="error",
                apply_scheduler_attempt=True,
            )
        )
        self.assertEqual(self.db.get_request(self.req_id), frozen)


@requires_postgres
class TestListSearchPlanClassificationForRequests(unittest.TestCase):
    """Batch-fetch dry-run classification.

    ``list_search_plan_classification_for_requests`` collapses the
    per-row 5-query inspection call into a single query. Verify the
    per-request data returned matches what the previous per-row
    ``get_search_plan_inspection`` path would have surfaced for the
    same rows.
    """

    def setUp(self):
        from lib.pipeline_db import SearchPlanItemInput
        self.SearchPlanItemInput = SearchPlanItemInput
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def _add(self, mbid: str) -> int:
        return self.db.add_request(
            mb_release_id=mbid, artist_name="Artist",
            album_title=mbid, source="request",
        )

    def test_empty_input_returns_empty_dict(self):
        self.assertEqual(
            self.db.list_search_plan_classification_for_requests([]), {})

    def test_returns_none_generator_ids_when_no_failed_plans(self):
        rid = self._add("nofail")
        result = self.db.list_search_plan_classification_for_requests([rid])
        self.assertIn(rid, result)
        self.assertIsNone(
            result[rid].latest_failed_deterministic_generator_id)
        self.assertIsNone(
            result[rid].latest_failed_transient_generator_id)

    def test_returns_latest_failed_generator_ids_per_request(self):
        rid_a = self._add("rid-a")
        rid_b = self._add("rid-b")
        rid_c = self._add("rid-c")
        # rid_a: deterministic failure on g-old, then deterministic
        # failure on g-new -- the new one should win.
        self.db.create_failed_search_plan(
            request_id=rid_a, generator_id="g-old",
            failure_class="no_runnable_query", transient=False,
        )
        self.db.create_failed_search_plan(
            request_id=rid_a, generator_id="g-new",
            failure_class="metadata_incomplete", transient=False,
        )
        # rid_b: only transient failure on g-new.
        self.db.create_failed_search_plan(
            request_id=rid_b, generator_id="g-new",
            failure_class="resolver_unavailable", transient=True,
        )
        # rid_c: both -- one deterministic g-det, one transient g-trans.
        self.db.create_failed_search_plan(
            request_id=rid_c, generator_id="g-det",
            failure_class="no_runnable_query", transient=False,
        )
        self.db.create_failed_search_plan(
            request_id=rid_c, generator_id="g-trans",
            failure_class="dependency_failure", transient=True,
        )

        result = self.db.list_search_plan_classification_for_requests(
            [rid_a, rid_b, rid_c])

        self.assertEqual(
            result[rid_a].latest_failed_deterministic_generator_id, "g-new")
        self.assertIsNone(
            result[rid_a].latest_failed_transient_generator_id)

        self.assertIsNone(
            result[rid_b].latest_failed_deterministic_generator_id)
        self.assertEqual(
            result[rid_b].latest_failed_transient_generator_id, "g-new")
        self.assertIsNotNone(
            result[rid_b].latest_failed_transient_created_at)

        self.assertEqual(
            result[rid_c].latest_failed_deterministic_generator_id, "g-det")
        self.assertEqual(
            result[rid_c].latest_failed_transient_generator_id, "g-trans")

    def test_matches_per_row_inspection_for_same_data(self):
        """Equivalence guard: the batch result for each request must
        agree with what ``get_search_plan_inspection`` would say about
        the same rows. This is the contract the dry-run classifier
        relies on after the rewrite.
        """
        rid = self._add("equiv")
        # Mixed plan history: deterministic failure, then a successful
        # plan (irrelevant to the classifier), then a transient
        # failure.
        self.db.create_failed_search_plan(
            request_id=rid, generator_id="g1",
            failure_class="no_runnable_query", transient=False,
        )
        self.db.create_successful_search_plan(
            request_id=rid, generator_id="g1",
            items=[self.SearchPlanItemInput(
                ordinal=0, strategy="default", query="q")],
            set_active=False,
        )
        self.db.create_failed_search_plan(
            request_id=rid, generator_id="g2",
            failure_class="resolver_unavailable", transient=True,
        )

        inspection = self.db.get_search_plan_inspection(rid)
        det = inspection.latest_failed_deterministic
        trans = inspection.latest_failed_transient

        batch = self.db.list_search_plan_classification_for_requests([rid])
        entry = batch[rid]
        self.assertEqual(
            entry.latest_failed_deterministic_generator_id,
            det.generator_id if det is not None else None,
        )
        self.assertEqual(
            entry.latest_failed_transient_generator_id,
            trans.generator_id if trans is not None else None,
        )
        self.assertEqual(
            entry.latest_failed_transient_created_at,
            trans.created_at if trans is not None else None,
        )


@requires_postgres
class TestGetWantedSearchable(unittest.TestCase):
    """``get_wanted_searchable`` filters Phase 2 execution candidates.

    Only rows whose active plan exists, status='active', and
    generator_id matches the passed-in id are returned. Rows without a
    current-generator active plan must be excluded so Phase 2 cannot
    accidentally fall back to recomputing variants from search_attempts.
    """

    def setUp(self):
        from lib.pipeline_db import SearchPlanItemInput
        self.SearchPlanItemInput = SearchPlanItemInput
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def _add_wanted(self, mbid: str) -> int:
        return self.db.add_request(
            mb_release_id=mbid, artist_name="A",
            album_title=mbid, source="request",
        )

    def _make_active(self, request_id: int, generator_id: str) -> int:
        return self.db.create_successful_search_plan(
            request_id=request_id,
            generator_id=generator_id,
            items=[self.SearchPlanItemInput(
                ordinal=0, strategy="default", query=f"q-{request_id}")],
        )

    def _add_searchable(
        self,
        mbid: str,
        *,
        created_at: datetime,
        attempts: int = 0,
        priority_started_at: datetime | None = None,
    ) -> int:
        request_id = self._add_wanted(mbid)
        self._make_active(request_id, "g1")
        self.db._execute(
            """
            UPDATE album_requests
            SET created_at = %s,
                priority_started_at = %s,
                search_attempts = %s,
                download_attempts = %s,
                validation_attempts = %s
            WHERE id = %s
            """,
            (
                created_at,
                priority_started_at,
                attempts,
                attempts,
                attempts,
                request_id,
            ),
        )
        self.db.conn.commit()
        return request_id

    def test_low_volume_new_requests_all_run_with_established_floor(self):
        now = datetime(2026, 7, 20, 4, 0, tzinfo=UTC)
        new_ids = {
            self._add_searchable(
                f"new-{index}", created_at=now - timedelta(hours=1),
                attempts=1,
            )
            for index in range(4)
        }
        established_ids = {
            self._add_searchable(
                f"old-{index}", created_at=now - timedelta(days=2),
                attempts=1,
            )
            for index in range(20)
        }

        rows = self.db.get_wanted_searchable("g1", limit=16, now=now)
        selected = {int(row["id"]) for row in rows}

        self.assertEqual(new_ids & selected, new_ids)
        self.assertEqual(len(established_ids & selected), 12)
        self.assertEqual(len(selected), 16)

    def test_seventeen_new_requests_use_four_slots_not_the_whole_page(self):
        now = datetime(2026, 7, 20, 4, 0, tzinfo=UTC)
        new_ids = {
            self._add_searchable(
                f"burst-{index}", created_at=now - timedelta(hours=1),
                attempts=1,
            )
            for index in range(17)
        }
        established_ids = {
            self._add_searchable(
                f"established-{index}",
                created_at=now - timedelta(days=2),
                attempts=1,
            )
            for index in range(20)
        }

        selected = {
            int(row["id"])
            for row in self.db.get_wanted_searchable("g1", limit=16, now=now)
        }

        self.assertEqual(len(new_ids & selected), 4)
        self.assertEqual(len(established_ids & selected), 12)

    def test_unused_new_capacity_is_borrowed_by_established_requests(self):
        now = datetime(2026, 7, 20, 4, 0, tzinfo=UTC)
        new_ids = {
            self._add_searchable(
                f"few-new-{index}", created_at=now - timedelta(hours=1),
                attempts=1,
            )
            for index in range(2)
        }
        established_ids = {
            self._add_searchable(
                f"many-old-{index}", created_at=now - timedelta(days=2),
                attempts=1,
            )
            for index in range(20)
        }

        selected = {
            int(row["id"])
            for row in self.db.get_wanted_searchable("g1", limit=16, now=now)
        }

        self.assertEqual(new_ids & selected, new_ids)
        self.assertEqual(len(established_ids & selected), 14)

    def test_unused_established_capacity_is_borrowed_by_new_requests(self):
        now = datetime(2026, 7, 20, 4, 0, tzinfo=UTC)
        new_ids = {
            self._add_searchable(
                f"many-new-{index}", created_at=now - timedelta(hours=1),
                attempts=1,
            )
            for index in range(20)
        }
        established_ids = {
            self._add_searchable(
                f"few-old-{index}", created_at=now - timedelta(days=2),
                attempts=1,
            )
            for index in range(2)
        }

        selected = {
            int(row["id"])
            for row in self.db.get_wanted_searchable("g1", limit=16, now=now)
        }

        self.assertEqual(len(new_ids & selected), 14)
        self.assertEqual(established_ids & selected, established_ids)

    def test_small_page_keeps_proportional_established_floor(self):
        now = datetime(2026, 7, 20, 4, 0, tzinfo=UTC)
        new_ids = {
            self._add_searchable(
                f"small-new-{index}", created_at=now - timedelta(hours=1),
                attempts=1,
            )
            for index in range(2)
        }
        established_ids = {
            self._add_searchable(
                f"small-old-{index}", created_at=now - timedelta(days=2),
                attempts=1,
            )
            for index in range(5)
        }

        selected = {
            int(row["id"])
            for row in self.db.get_wanted_searchable("g1", limit=5, now=now)
        }

        self.assertEqual(len(new_ids & selected), 1)
        self.assertEqual(len(established_ids & selected), 4)

    def test_page_size_must_leave_capacity_for_both_cohorts(self):
        for page_size in (-1, 0, 1):
            with self.subTest(page_size=page_size), self.assertRaisesRegex(ValueError, "at least 2"):
                self.db.get_wanted_searchable(
                    "g1", limit=page_size,
                    now=datetime(2026, 7, 20, 4, 0, tzinfo=UTC),
                )

    def test_exact_24_hour_boundary_is_established(self):
        now = datetime(2026, 7, 20, 4, 0, tzinfo=UTC)
        new_ids = {
            self._add_searchable(
                f"inside-{index}",
                created_at=now - timedelta(hours=23, minutes=59),
                attempts=1,
            )
            for index in range(5)
        }
        boundary_id = self._add_searchable(
            "exact-boundary", created_at=now - timedelta(hours=24),
            attempts=1,
        )
        priority_boundary_id = self._add_searchable(
            "priority-exact-boundary",
            created_at=now - timedelta(days=10),
            priority_started_at=now - timedelta(hours=24),
            attempts=1,
        )
        established_ids = {boundary_id, priority_boundary_id} | {
            self._add_searchable(
                f"beyond-{index}", created_at=now - timedelta(days=2),
                attempts=1,
            )
            for index in range(10)
        }

        for _ in range(8):
            selected = {
                int(row["id"])
                for row in self.db.get_wanted_searchable(
                    "g1", limit=16, now=now)
            }
            self.assertEqual(len(new_ids & selected), 4)
            self.assertEqual(established_ids & selected, established_ids)

    def test_backoff_and_downloading_are_excluded_before_allocation(self):
        now = datetime(2026, 7, 20, 4, 0, tzinfo=UTC)
        due_id = self._add_searchable(
            "new-due", created_at=now - timedelta(hours=1), attempts=1)
        backed_off_id = self._add_searchable(
            "new-backed-off", created_at=now - timedelta(hours=1), attempts=1)
        downloading_id = self._add_searchable(
            "new-downloading", created_at=now - timedelta(hours=1), attempts=1)
        self.db._execute(
            "UPDATE album_requests SET next_retry_after = %s WHERE id = %s",
            (now + timedelta(minutes=1), backed_off_id),
        )
        self.db._execute(
            "UPDATE album_requests SET status = 'downloading' WHERE id = %s",
            (downloading_id,),
        )
        established_ids = {
            self._add_searchable(
                f"gate-old-{index}", created_at=now - timedelta(days=2),
                attempts=1,
            )
            for index in range(15)
        }
        self.db.conn.commit()

        selected = {
            int(row["id"])
            for row in self.db.get_wanted_searchable("g1", limit=16, now=now)
        }

        self.assertIn(due_id, selected)
        self.assertNotIn(backed_off_id, selected)
        self.assertNotIn(downloading_id, selected)
        self.assertEqual(established_ids & selected, established_ids)

    def test_aged_untried_request_uses_same_established_lottery(self):
        now = datetime(2026, 7, 20, 4, 0, tzinfo=UTC)
        new_ids = {
            self._add_searchable(
                f"fresh-untried-{index}",
                created_at=now - timedelta(hours=1),
                attempts=1,
            )
            for index in range(4)
        }
        aged_untried_id = self._add_searchable(
            "aged-untried", created_at=now - timedelta(days=2),
            attempts=0,
        )
        established_ids = {aged_untried_id} | {
            self._add_searchable(
                f"aged-tried-{index}", created_at=now - timedelta(days=2),
                attempts=2,
            )
            for index in range(23)
        }

        untried_selected: list[bool] = []
        for _ in range(48):
            selected = {
                int(row["id"])
                for row in self.db.get_wanted_searchable(
                    "g1", limit=16, now=now)
            }
            self.assertEqual(new_ids & selected, new_ids)
            self.assertEqual(len(established_ids & selected), 12)
            untried_selected.append(aged_untried_id in selected)

        self.assertIn(True, untried_selected)
        self.assertIn(False, untried_selected)

    def test_manual_requeue_does_not_reset_priority_age(self):
        now = datetime(2026, 7, 20, 4, 0, tzinfo=UTC)
        requeued_id = self._add_searchable(
            "old-manual-requeue",
            created_at=now - timedelta(days=2),
            attempts=3,
        )
        before = self.db.get_request(requeued_id)
        assert before is not None
        self.assertTrue(self.db.update_status(
            requeued_id, "imported", expected_status="wanted"))
        self.assertTrue(self.db.reset_to_wanted(
            requeued_id, expected_status="imported"))
        after = self.db.get_request(requeued_id)
        assert after is not None
        self.assertEqual(after["created_at"], before["created_at"])
        self.assertEqual(after["search_attempts"], 0)

        new_ids = {
            self._add_searchable(
                f"requeue-new-{index}",
                created_at=now - timedelta(hours=1),
                attempts=1,
            )
            for index in range(5)
        }
        established_ids = {requeued_id} | {
            self._add_searchable(
                f"requeue-old-{index}",
                created_at=now - timedelta(days=2),
                attempts=1,
            )
            for index in range(11)
        }

        selected = {
            int(row["id"])
            for row in self.db.get_wanted_searchable("g1", limit=16, now=now)
        }

        self.assertEqual(len(new_ids & selected), 4)
        self.assertEqual(established_ids & selected, established_ids)

    def test_recent_bad_rip_priority_puts_aged_request_in_new_cohort(self):
        now = datetime(2026, 7, 20, 4, 0, tzinfo=UTC)
        bad_rip_id = self._add_searchable(
            "aged-bad-rip",
            created_at=now - timedelta(days=10),
            priority_started_at=now - timedelta(hours=1),
            attempts=3,
        )
        new_ids = {bad_rip_id} | {
            self._add_searchable(
                f"ordinary-new-{index}",
                created_at=now - timedelta(hours=1),
                attempts=1,
            )
            for index in range(3)
        }
        established_ids = {
            self._add_searchable(
                f"bad-rip-old-{index}",
                created_at=now - timedelta(days=2),
                attempts=1,
            )
            for index in range(20)
        }

        selected = {
            int(row["id"])
            for row in self.db.get_wanted_searchable("g1", limit=16, now=now)
        }

        self.assertEqual(new_ids & selected, new_ids)
        self.assertEqual(len(established_ids & selected), 12)

    def test_title_blacklist_is_applied_before_capacity(self):
        now = datetime(2026, 7, 20, 4, 0, tzinfo=UTC)
        blocked_ids = {
            self._add_searchable(
                f"Blocked release {index}",
                created_at=now - timedelta(hours=1),
                attempts=1,
            )
            for index in range(4)
        }
        allowed_new_id = self._add_searchable(
            "Allowed release", created_at=now - timedelta(hours=1), attempts=1)
        established_ids = {
            self._add_searchable(
                f"allowed-old-{index}", created_at=now - timedelta(days=2),
                attempts=1,
            )
            for index in range(20)
        }

        selected = {
            int(row["id"])
            for row in self.db.get_wanted_searchable(
                "g1",
                limit=16,
                title_blacklist=("blocked",),
                now=now,
            )
        }

        self.assertFalse(blocked_ids & selected)
        self.assertIn(allowed_new_id, selected)
        self.assertEqual(len(established_ids & selected), 15)
        self.assertEqual(len(selected), 16)

    def test_randomizes_within_both_cohorts(self):
        now = datetime(2026, 7, 20, 4, 0, tzinfo=UTC)
        new_ids = {
            self._add_searchable(
                f"random-new-{index}", created_at=now - timedelta(hours=1),
                attempts=1,
            )
            for index in range(8)
        }
        established_ids = {
            self._add_searchable(
                f"random-old-{index}", created_at=now - timedelta(days=2),
                attempts=1,
            )
            for index in range(20)
        }
        samples = [
            {
                int(row["id"])
                for row in self.db.get_wanted_searchable(
                    "g1", limit=16, now=now)
            }
            for _ in range(8)
        ]

        self.assertGreater(
            len({frozenset(sample & new_ids) for sample in samples}), 1)
        self.assertGreater(
            len({frozenset(sample & established_ids) for sample in samples}), 1)

    def test_returns_only_rows_with_current_generator_active_plan(self):
        rid_match = self._add_wanted("match")
        self._make_active(rid_match, "g1")

        self._add_wanted("no-plan")  # never planned
        rid_old_gen = self._add_wanted("old-gen")
        self._make_active(rid_old_gen, "g0")  # different gen

        rid_imported = self._add_wanted("imp")
        self._make_active(rid_imported, "g1")
        self.db.update_status(rid_imported, "imported")

        rows = self.db.get_wanted_searchable("g1")
        rids = {r["id"] for r in rows}
        self.assertEqual(rids, {rid_match})

    def test_respects_retry_backoff(self):
        rid = self._add_wanted("backoff")
        self._make_active(rid, "g1")
        future = datetime.now(UTC) + timedelta(hours=1)
        self.db._execute(
            "UPDATE album_requests SET next_retry_after = %s WHERE id = %s",
            (future, rid),
        )
        self.db.conn.commit()
        self.assertEqual(self.db.get_wanted_searchable("g1"), [])

    def test_excludes_request_after_supersede_to_new_generator(self):
        # Old-generator active plan -> supersede to new -> the new id
        # must be searchable; the old id is not.
        rid = self._add_wanted("supersede")
        self._make_active(rid, "g0")
        self.assertEqual(
            [r["id"] for r in self.db.get_wanted_searchable("g0")], [rid])
        self.db.supersede_search_plan_with_replacement(
            request_id=rid,
            generator_id="g1",
            items=[self.SearchPlanItemInput(
                ordinal=0, strategy="default", query="q-new")],
        )
        self.assertEqual(self.db.get_wanted_searchable("g0"), [])
        rids = [r["id"] for r in self.db.get_wanted_searchable("g1")]
        self.assertEqual(rids, [rid])

    def test_failed_deterministic_only_excluded(self):
        rid = self._add_wanted("det-fail")
        self.db.create_failed_search_plan(
            request_id=rid, generator_id="g1",
            failure_class="no_runnable_query", transient=False,
        )
        self.assertEqual(self.db.get_wanted_searchable("g1"), [])

    def test_failed_transient_only_excluded(self):
        rid = self._add_wanted("trans-fail")
        self.db.create_failed_search_plan(
            request_id=rid, generator_id="g1",
            failure_class="resolver_unavailable", transient=True,
        )
        self.assertEqual(self.db.get_wanted_searchable("g1"), [])

    def test_active_youtube_rescue_excluded(self):
        from lib.import_queue import (
            IMPORT_JOB_YOUTUBE,
            youtube_import_dedupe_key,
            youtube_import_payload,
        )

        rid_running = self._add_wanted("yt-running")
        self._make_active(rid_running, "g1")
        self.db.insert_youtube_running(
            request_id=rid_running,
            browse_id="MPREb_running",
            audio_playlist_id=None,
            yt_url="https://music.youtube.com/playlist?list=running",
            expected_track_count=10,
        )

        rid_import = self._add_wanted("yt-import")
        self._make_active(rid_import, "g1")
        self.db.enqueue_import_job(
            IMPORT_JOB_YOUTUBE,
            request_id=rid_import,
            dedupe_key=youtube_import_dedupe_key(123),
            payload=youtube_import_payload(
                staged_path="/tmp/yt-import",
                request_id=rid_import,
                browse_id="MPREb_import",
                download_log_id=1,
            ),
        )

        rid_clear = self._add_wanted("clear")
        self._make_active(rid_clear, "g1")

        rows = self.db.get_wanted_searchable("g1")
        self.assertEqual({r["id"] for r in rows}, {rid_clear})

    def test_limit_applied(self):
        ids = []
        for i in range(5):
            rid = self._add_wanted(f"lim-{i}")
            self._make_active(rid, "g1")
            ids.append(rid)
        rows = self.db.get_wanted_searchable("g1", limit=3)
        self.assertEqual(len(rows), 3)
        self.assertTrue({r["id"] for r in rows}.issubset(set(ids)))


@requires_postgres
class TestPersistedSearchPlanReconciliation(unittest.TestCase):
    def setUp(self):
        from lib.pipeline_db import SearchPlanItemInput
        self.SearchPlanItemInput = SearchPlanItemInput
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def _add(self, mbid: str, status: str = "wanted") -> int:
        rid = self.db.add_request(
            mb_release_id=mbid,
            artist_name="A",
            album_title=mbid,
            source="request",
        )
        if status != "wanted":
            self.db.update_status(rid, status)
        return rid

    def test_lists_all_wanted_ignoring_retry_eligibility_and_pagination(self):
        far_future = datetime.now(UTC) + timedelta(hours=24)
        rid_due = self._add("recon-due")
        rid_backoff = self._add("recon-backoff")
        rid_imported = self._add("recon-imported", status="imported")
        # Set far-future retry on the second wanted row -- get_wanted would
        # skip it; reconciliation MUST include it.
        self.db._execute(
            "UPDATE album_requests SET next_retry_after = %s WHERE id = %s",
            (far_future, rid_backoff),
        )
        # And one wanted row already has an active plan.
        self.db.create_successful_search_plan(
            request_id=rid_due,
            generator_id="g1",
            items=[self.SearchPlanItemInput(
                ordinal=0, strategy="default", query="q")],
        )

        rows = self.db.list_wanted_for_plan_reconciliation()
        rids = {r.request_id for r in rows}
        self.assertIn(rid_due, rids)
        self.assertIn(rid_backoff, rids)
        self.assertNotIn(rid_imported, rids)

        by_id = {r.request_id: r for r in rows}
        self.assertIsNotNone(by_id[rid_due].active_plan_id)
        self.assertEqual(by_id[rid_due].active_plan_generator_id, "g1")
        self.assertIsNone(by_id[rid_backoff].active_plan_id)
        self.assertIsNone(by_id[rid_backoff].active_plan_generator_id)

    def test_reconciliation_candidate_ignores_non_active_plan_pointer(self):
        rid = self._add("recon-malformed")
        failed_id = self.db.create_failed_search_plan(
            request_id=rid,
            generator_id="g1",
            failure_class="no_runnable_query",
            error_message="failed",
            transient=False,
        )
        self.db._execute(
            "UPDATE album_requests SET active_plan_id = %s WHERE id = %s",
            (failed_id, rid),
        )

        rows = self.db.list_wanted_for_plan_reconciliation()
        by_id = {r.request_id: r for r in rows}
        self.assertIn(rid, by_id)
        self.assertIsNone(by_id[rid].active_plan_id)
        self.assertIsNone(by_id[rid].active_plan_generator_id)


@requires_postgres
class TestPersistedSearchPlanInspection(unittest.TestCase):
    def setUp(self):
        from lib.pipeline_db import SearchPlanItemInput
        self.SearchPlanItemInput = SearchPlanItemInput
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="inspect-mbid",
            artist_name="A", album_title="B", source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_inspection_returns_active_failed_superseded_and_legacy_counts(self):
        # Legacy log row (no plan context).
        self.db.log_search(
            self.req_id, query="legacy", outcome="error",
        )
        # Deterministic + transient failed attempts.
        self.db.create_failed_search_plan(
            request_id=self.req_id, generator_id="g1",
            failure_class="no_runnable_query", transient=False,
        )
        self.db.create_failed_search_plan(
            request_id=self.req_id, generator_id="g1",
            failure_class="resolver_unavailable", transient=True,
        )
        # First successful, then supersede with second.
        self.db.create_successful_search_plan(
            request_id=self.req_id, generator_id="g1",
            items=[self.SearchPlanItemInput(
                ordinal=0, strategy="default", query="q1")],
        )
        new_id = self.db.supersede_search_plan_with_replacement(
            request_id=self.req_id, generator_id="g2",
            items=[self.SearchPlanItemInput(
                ordinal=0, strategy="default", query="q2")],
        )

        info = self.db.get_search_plan_inspection(self.req_id)
        assert info.active is not None
        self.assertEqual(info.active.plan.id, new_id)
        assert info.latest_failed_deterministic is not None
        self.assertEqual(
            info.latest_failed_deterministic.failure_class,
            "no_runnable_query")
        assert info.latest_failed_transient is not None
        self.assertEqual(
            info.latest_failed_transient.failure_class,
            "resolver_unavailable")
        self.assertEqual(info.superseded_count, 1)
        self.assertEqual(info.legacy_search_log_count, 1)


@requires_postgres
class TestRecordConsumedSearchAttempt(unittest.TestCase):
    def setUp(self):
        from lib.pipeline_db import ConsumedAttemptInput, SearchPlanItemInput
        self.ConsumedAttemptInput = ConsumedAttemptInput
        self.SearchPlanItemInput = SearchPlanItemInput
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="consumed-mbid",
            artist_name="A", album_title="B", source="request",
        )
        self.plan_id = self.db.create_successful_search_plan(
            request_id=self.req_id,
            generator_id="g1",
            items=[
                self.SearchPlanItemInput(
                    ordinal=0, strategy="default", query="Q0",
                    canonical_query_key="q0", repeat_group="rg"),
                self.SearchPlanItemInput(
                    ordinal=1, strategy="track_0", query="Q1",
                    canonical_query_key="q1"),
            ],
        )
        active = self.db.get_active_search_plan(self.req_id)
        assert active is not None
        self.active = active
        self.item_ids = [it.id for it in active.items]

    def tearDown(self):
        self.db.close()

    def _attempt(self, ordinal: int, **overrides: object):
        # ``dataclasses.replace`` (not a merged-dict-then-``**kwargs``
        # unpack) because ``ConsumedAttemptInput`` has heterogeneous
        # per-field types — a ``dict[str, <single value type>]`` can never
        # type-check against a dataclass constructor with mixed
        # int/str/float/bool/None fields once the constructor's own
        # argument types are precisely known (issue #784: fixing
        # lib.pipeline_db.search_plan's return types made
        # ``self.active.items[...]``'s attributes precisely typed for the
        # first time, which is what surfaces this).
        base = self.ConsumedAttemptInput(
            request_id=self.req_id,
            plan_id=self.plan_id,
            plan_item_id=self.item_ids[ordinal],
            plan_ordinal=ordinal,
            plan_strategy=self.active.items[ordinal].strategy,
            plan_canonical_query_key=(
                self.active.items[ordinal].canonical_query_key),
            plan_repeat_group=self.active.items[ordinal].repeat_group,
            plan_generator_id="g1",
            query=self.active.items[ordinal].query,
            outcome="no_match",
            plan_item_count=len(self.active.items),
            apply_scheduler_attempt=True,
            scheduler_success=False,
        )
        return dataclasses.replace(base, **overrides)

    def test_advance_ordinal_writes_log_and_updates_cursor(self):
        result = self.db.record_consumed_search_attempt(
            self._attempt(0, outcome="no_match"))
        self.assertEqual(result.cursor_update_status, "advanced")
        self.assertEqual(result.new_next_ordinal, 1)
        self.assertEqual(result.new_cycle_count, 0)
        self.assertFalse(result.is_stale)
        # Log row written with plan context + cycle snapshot.
        rows = self.db.get_search_history(self.req_id)
        self.assertEqual(len(rows), 1)
        log = rows[0]
        self.assertEqual(log["plan_id"], self.plan_id)
        self.assertEqual(log["plan_ordinal"], 0)
        self.assertEqual(log["execution_stage"], "accepted")
        self.assertTrue(log["attempt_consumed"])
        self.assertEqual(log["cursor_update_status"], "advanced")
        self.assertEqual(log["plan_cycle_snapshot"], 0)
        # Request cursor advanced.
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["next_plan_ordinal"], 1)
        self.assertEqual(req["plan_cycle_count"], 0)
        # Scheduler/backoff updated.
        self.assertEqual(req["search_attempts"], 1)
        self.assertIsNotNone(req["next_retry_after"])

    def test_final_ordinal_wraps_and_increments_cycle(self):
        # Move cursor to final ordinal first.
        self.db._execute(
            "UPDATE album_requests SET next_plan_ordinal = 1 WHERE id = %s",
            (self.req_id,),
        )
        result = self.db.record_consumed_search_attempt(self._attempt(1))
        self.assertEqual(result.cursor_update_status, "wrapped")
        self.assertEqual(result.new_next_ordinal, 0)
        self.assertEqual(result.new_cycle_count, 1)
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["next_plan_ordinal"], 0)
        self.assertEqual(req["plan_cycle_count"], 1)
        # Log row reflects pre-write cycle snapshot.
        rows = self.db.get_search_history(self.req_id)
        self.assertEqual(rows[0]["cursor_update_status"], "wrapped")
        self.assertEqual(rows[0]["plan_cycle_snapshot"], 0)

    def test_stale_completion_logs_but_does_not_advance(self):
        # Advance the cursor manually so the executor's plan_ordinal=0
        # no longer matches.
        self.db._execute(
            "UPDATE album_requests SET next_plan_ordinal = 1 WHERE id = %s",
            (self.req_id,),
        )
        result = self.db.record_consumed_search_attempt(self._attempt(0))
        self.assertTrue(result.is_stale)
        self.assertEqual(result.cursor_update_status, "stale")
        # Cursor NOT advanced.
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["next_plan_ordinal"], 1)
        # Log row still inserted, flagged stale.
        rows = self.db.get_search_history(self.req_id)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["execution_stage"], "stale_completion")
        self.assertFalse(rows[0]["attempt_consumed"])
        self.assertEqual(rows[0]["cursor_update_status"], "stale")
        self.assertEqual(rows[0]["stale_reason"], "regenerated")
        # No scheduler/backoff bump on stale.
        self.assertEqual(req["search_attempts"], 0)

    def test_stale_when_cycle_count_does_not_match(self):
        self.db._execute(
            "UPDATE album_requests SET plan_cycle_count = 1 WHERE id = %s",
            (self.req_id,),
        )
        result = self.db.record_consumed_search_attempt(self._attempt(0))
        self.assertTrue(result.is_stale)
        self.assertEqual(result.cursor_update_status, "stale")
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["next_plan_ordinal"], 0)
        self.assertEqual(req["plan_cycle_count"], 1)
        rows = self.db.get_search_history(self.req_id)
        self.assertEqual(rows[0]["execution_stage"], "stale_completion")
        self.assertFalse(rows[0]["attempt_consumed"])
        self.assertEqual(rows[0]["plan_cycle_snapshot"], 0)

    def test_stale_when_plan_id_does_not_match(self):
        # Regenerate -- new plan, cursor reset.
        new_plan = self.db.supersede_search_plan_with_replacement(
            request_id=self.req_id,
            generator_id="g2",
            items=[self.SearchPlanItemInput(
                ordinal=0, strategy="default", query="Qnew")],
        )
        # Old executor completes against the old plan id.
        result = self.db.record_consumed_search_attempt(self._attempt(0))
        self.assertTrue(result.is_stale)
        # Active plan is still the new one, cursor at 0.
        active = self.db.get_active_search_plan(self.req_id)
        assert active is not None
        self.assertEqual(active.plan.id, new_plan)
        self.assertEqual(active.next_ordinal, 0)

    def test_rejects_plan_item_from_another_request(self):
        other_req_id = self.db.add_request(
            mb_release_id="consumed-other-mbid",
            artist_name="C", album_title="D", source="request",
        )
        other_plan = self.db.create_successful_search_plan(
            request_id=other_req_id,
            generator_id="g1",
            items=[self.SearchPlanItemInput(
                ordinal=0, strategy="default", query="Q-other")],
        )
        other_active = self.db.get_active_search_plan(other_req_id)
        assert other_active is not None
        with self.assertRaises(ValueError):
            self.db.record_consumed_search_attempt(
                self._attempt(
                    0,
                    plan_id=self.plan_id,
                    plan_item_id=other_active.items[0].id,
                )
            )
        self.assertIsNotNone(other_plan)
        rows = self.db.get_search_history(self.req_id)
        self.assertEqual(rows, [])

    def test_consumed_attempt_persists_u11_forensics_columns(self):
        """U11: consumed-attempt rows surface R22-R27 from the input."""
        self.db.record_consumed_search_attempt(self._attempt(
            0,
            outcome="no_match",
            rejection_reason="strict_count_mismatch",
            result_count_uncapped=873,
            query_token_count=4,
            query_distinct_token_count=3,
            expected_track_count=10,
            matcher_score_top1=1.5,
            query_template="{artist} {title} FLAC",
        ))
        rows = self.db.get_search_history(self.req_id)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["rejection_reason"], "strict_count_mismatch")
        self.assertEqual(row["result_count_uncapped"], 873)
        self.assertEqual(row["query_token_count"], 4)
        self.assertEqual(row["query_distinct_token_count"], 3)
        self.assertEqual(row["expected_track_count"], 10)
        score = row["matcher_score_top1"]
        assert isinstance(score, float)
        self.assertAlmostEqual(score, 1.5, places=4)
        self.assertEqual(row["query_template"], "{artist} {title} FLAC")

    def test_consumed_attempt_persists_cross_request_conflict_marker(self):
        """#1196 item 2: a non-empty
        ``cross_request_conflict_request_ids`` input round-trips through
        the real ``search_log`` column (migration 079), and an omitted
        one persists as SQL NULL rather than an empty array -- the
        marker's mere presence is the "a guard skip happened" fact."""
        self.db.record_consumed_search_attempt(self._attempt(
            0, outcome="no_match",
            cross_request_conflict_request_ids=(8781, 8846),
        ))
        self.db._execute(
            "UPDATE album_requests SET next_plan_ordinal = 0 WHERE id = %s",
            (self.req_id,),
        )
        self.db.record_consumed_search_attempt(self._attempt(
            0, outcome="no_match",
        ))
        rows = self.db.get_search_history(self.req_id)
        self.assertEqual(len(rows), 2)
        markers = [r["cross_request_conflict_request_ids"] for r in rows]
        with_marker = [m for m in markers if m is not None]
        without_marker = [m for m in markers if m is None]
        self.assertEqual(len(with_marker), 1)
        self.assertEqual(len(without_marker), 1)
        marker = with_marker[0]
        assert isinstance(marker, list)
        self.assertEqual(sorted(marker), [8781, 8846])

    def test_u12_wrap_writes_failure_class_b_cands_never_match(self):
        """U12: wrap classifies all-no_match cycle as B."""
        # Cycle 0: both items return no_match (matcher rejected candidates).
        self.db.record_consumed_search_attempt(self._attempt(
            0, outcome="no_match", rejection_reason="strict_count_mismatch",
        ))
        # Final ordinal → wrap.
        result = self.db.record_consumed_search_attempt(self._attempt(
            1, outcome="no_match", rejection_reason="avg_ratio_low",
        ))
        self.assertEqual(result.cursor_update_status, "wrapped")
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["failure_class"], "B_cands_never_match")

    def test_u12_wrap_writes_failure_class_a_zero_results_dominant(self):
        """U12: wrap classifies dominant-no_results cycle as A."""
        self.db.record_consumed_search_attempt(self._attempt(
            0, outcome="no_results",
        ))
        result = self.db.record_consumed_search_attempt(self._attempt(
            1, outcome="no_results",
        ))
        self.assertEqual(result.cursor_update_status, "wrapped")
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["failure_class"], "A_zero_results_dominant")

    def test_u12_non_wrap_advance_does_not_write_failure_class(self):
        """U12: classification only fires on wrap, not on plain advance."""
        result = self.db.record_consumed_search_attempt(self._attempt(
            0, outcome="no_match",
        ))
        self.assertEqual(result.cursor_update_status, "advanced")
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertIsNone(req["failure_class"])

    def test_u12_wrap_with_status_imported_classifies_resolved(self):
        """U12: status moved past 'wanted' overrides search-pattern verdict."""
        # Mid-cycle, the importer marked the request 'imported'.
        self.db._execute(
            "UPDATE album_requests SET status = 'imported' WHERE id = %s",
            (self.req_id,),
        )
        self.db.record_consumed_search_attempt(self._attempt(
            0, outcome="no_match",
        ))
        result = self.db.record_consumed_search_attempt(self._attempt(
            1, outcome="no_match",
        ))
        self.assertEqual(result.cursor_update_status, "wrapped")
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["failure_class"], "resolved")

    def test_u12_wrap_preserves_prior_failure_class_on_degenerate_cycle(self):
        """U12: empty cycle (all stale) leaves prior failure_class intact.

        Seed a prior failure_class, then trigger a wrap whose only
        consumed attempt is the wrap itself. Verify the classifier sees
        one consumed attempt; for richer "zero consumed" coverage see
        the FakePipelineDB self-test where we can drive the
        no-consumed-attempts case directly.
        """
        # Seed a prior verdict so we can distinguish "unchanged" from
        # "overwritten".
        self.db._execute(
            "UPDATE album_requests SET failure_class = 'E_mixed' "
            "WHERE id = %s",
            (self.req_id,),
        )
        # Single attempt + wrap. Branch ordering: found dominates →
        # D_found_but_no_import overwrites the prior E_mixed.
        self.db._execute(
            "UPDATE album_requests SET next_plan_ordinal = 1 WHERE id = %s",
            (self.req_id,),
        )
        result = self.db.record_consumed_search_attempt(self._attempt(
            1, outcome="found",
        ))
        self.assertEqual(result.cursor_update_status, "wrapped")
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["failure_class"], "D_found_but_no_import")

    def test_u12_wrap_d_found_but_no_import(self):
        """U12: one found + status still wanted → D_found_but_no_import."""
        self.db.record_consumed_search_attempt(self._attempt(
            0, outcome="found",
        ))
        result = self.db.record_consumed_search_attempt(self._attempt(
            1, outcome="no_match",
        ))
        self.assertEqual(result.cursor_update_status, "wrapped")
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["failure_class"], "D_found_but_no_import")

    def test_u12_failure_class_check_constraint_enforced(self):
        """U12: every classifier verdict must satisfy the CHECK constraint.

        Walk a wrap for each of A/B/D/resolved/E (constants from the
        classifier module) and assert that PostgreSQL accepts the
        write. If the classifier ever returns a value the schema
        rejects, this surfaces as a constraint violation at write time
        — not as silent corruption.
        """
        from lib.search_classification import ALL_FAILURE_CLASSES
        for fc in ALL_FAILURE_CLASSES:
            with self.subTest(failure_class=fc):
                self.db._execute(
                    "UPDATE album_requests SET failure_class = %s "
                    "WHERE id = %s",
                    (fc, self.req_id),
                )
                req = self.db.get_request(self.req_id)
                assert req is not None
                self.assertEqual(req["failure_class"], fc)


@requires_postgres
class TestRecordNonConsumingSearchAttempt(unittest.TestCase):
    def setUp(self):
        from lib.pipeline_db import NonConsumingAttemptInput, SearchPlanItemInput
        self.NonConsumingAttemptInput = NonConsumingAttemptInput
        self.SearchPlanItemInput = SearchPlanItemInput
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="nonconsuming-mbid",
            artist_name="A", album_title="B", source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_writes_visible_log_and_applies_backoff_without_advancing(self):
        log_id = self.db.record_non_consuming_search_attempt(
            self.NonConsumingAttemptInput(
                request_id=self.req_id,
                outcome="error",
                error_message="slskd 503",
                apply_scheduler_attempt=True,
            )
        )
        self.assertGreater(log_id, 0)
        rows = self.db.get_search_history(self.req_id)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["outcome"], "error")
        self.assertEqual(rows[0]["execution_stage"], "pre_attempt")
        self.assertFalse(rows[0]["attempt_consumed"])
        self.assertEqual(rows[0]["cursor_update_status"], "unchanged")
        self.assertEqual(rows[0]["plan_cycle_snapshot"], 0)
        # Cursor + cycle untouched, scheduler/backoff applied.
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["next_plan_ordinal"], 0)
        self.assertEqual(req["plan_cycle_count"], 0)
        self.assertEqual(req["search_attempts"], 1)
        self.assertIsNotNone(req["next_retry_after"])

    def test_can_skip_scheduler_attempt(self):
        self.db.record_non_consuming_search_attempt(
            self.NonConsumingAttemptInput(
                request_id=self.req_id,
                outcome="error",
                apply_scheduler_attempt=False,
            )
        )
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["search_attempts"], 0)
        self.assertIsNone(req["next_retry_after"])

    def test_non_consuming_attempt_persists_u11_forensics_columns(self):
        """U11: pre-attempt rows surface R22-R27 from the input."""
        self.db.record_non_consuming_search_attempt(
            self.NonConsumingAttemptInput(
                request_id=self.req_id,
                outcome="error",
                error_message="slskd 503",
                apply_scheduler_attempt=True,
                rejection_reason=None,
                result_count_uncapped=None,
                query_token_count=3,
                query_distinct_token_count=3,
                expected_track_count=12,
                matcher_score_top1=None,
                query_template="{artist} {title}",
            )
        )
        rows = self.db.get_search_history(self.req_id)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        # Pre-attempt: matcher never ran → score/reason/uncapped NULL.
        self.assertIsNone(row["rejection_reason"])
        self.assertIsNone(row["matcher_score_top1"])
        self.assertIsNone(row["result_count_uncapped"])
        # Token-counts + template + expected-track-count come from
        # plan-context state that's known before slskd dispatch.
        self.assertEqual(row["query_token_count"], 3)
        self.assertEqual(row["query_distinct_token_count"], 3)
        self.assertEqual(row["expected_track_count"], 12)
        self.assertEqual(row["query_template"], "{artist} {title}")


@requires_postgres
class TestRequestSearchSummaryViewU11RoundTrip(unittest.TestCase):
    """U11 R29: writing search_log rows with populated forensics columns
    must surface through ``request_search_summary`` for the dominant
    rejection-reason rollup.

    Migration 031 defines ``request_search_summary`` with
    ``MODE() WITHIN GROUP (ORDER BY rejection_reason)`` — the mode is
    the most-frequent non-NULL reason. This test pins that contract:
    five known rows with mixed reasons must roll up to the operator's
    expected ``dominant_rejection_reason``.
    """

    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="u11-summary-mbid",
            artist_name="A", album_title="B", source="request",
        )

    def tearDown(self):
        self.db.close()

    def _log(self, reason: str | None, outcome: str = "no_match") -> None:
        self.db.log_search(
            request_id=self.req_id,
            query="q",
            outcome=outcome,
            candidates=[],
            rejection_reason=reason,
        )

    def test_dominant_rejection_reason_rolls_up_from_recent_rows(self):
        # 5 rows: 3 avg_ratio_low, 1 strict_count_mismatch, 1 NULL
        # (e.g. found). Mode over non-NULL = avg_ratio_low.
        self._log("avg_ratio_low")
        self._log("avg_ratio_low")
        self._log("strict_count_mismatch")
        self._log("avg_ratio_low")
        self._log(None, outcome="found")

        cur = self.db._execute(
            "SELECT total_searches, dominant_rejection_reason "
            "FROM request_search_summary WHERE request_id = %s",
            (self.req_id,),
        )
        row = cur.fetchone()
        assert row is not None
        self.assertEqual(row["total_searches"], 5)
        self.assertEqual(row["dominant_rejection_reason"], "avg_ratio_low")

    def test_all_null_reasons_produce_null_dominant(self):
        # Every row's reason is NULL (e.g. all found / no_results).
        self._log(None, outcome="found")
        self._log(None, outcome="no_results")
        cur = self.db._execute(
            "SELECT dominant_rejection_reason "
            "FROM request_search_summary WHERE request_id = %s",
            (self.req_id,),
        )
        row = cur.fetchone()
        assert row is not None
        self.assertIsNone(row["dominant_rejection_reason"])


@requires_postgres
class TestPersistedSearchPlanLifecycleEdgeCases(unittest.TestCase):
    def setUp(self):
        from lib.pipeline_db import ConsumedAttemptInput, SearchPlanItemInput
        self.ConsumedAttemptInput = ConsumedAttemptInput
        self.SearchPlanItemInput = SearchPlanItemInput
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="lifecycle-mbid",
            artist_name="A", album_title="B", source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_historical_logs_with_null_plan_context_still_returned(self):
        self.db.log_search(self.req_id, query="legacy", outcome="error")
        rows = self.db.get_search_history(self.req_id)
        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0].get("plan_id"))
        self.assertIsNone(rows[0].get("execution_stage"))

    def test_request_delete_cascades_plans_keeps_inspection_at_zero(self):
        self.db.create_successful_search_plan(
            request_id=self.req_id,
            generator_id="g1",
            items=[self.SearchPlanItemInput(
                ordinal=0, strategy="default", query="q")],
        )
        self.db.delete_request(self.req_id)
        # After deletion the cascade should clear plans / items / logs (because
        # search_log already CASCADEs on request from migration 001). The
        # inspection method just returns zeros for a missing request.
        info = self.db.get_search_plan_inspection(self.req_id)
        self.assertIsNone(info.active)
        self.assertIsNone(info.latest_failed_deterministic)
        self.assertIsNone(info.latest_failed_transient)
        self.assertEqual(info.superseded_count, 0)
        self.assertEqual(info.legacy_search_log_count, 0)

    def test_consumed_attempt_rolls_back_on_failure_no_partial_state(self):
        plan_id = self.db.create_successful_search_plan(
            request_id=self.req_id,
            generator_id="g1",
            items=[self.SearchPlanItemInput(
                ordinal=0, strategy="default", query="q")],
        )
        # Build an attempt referencing a plan_item_id that doesn't exist; the
        # service rejects it inside the transaction and rolls the cursor write
        # back too.
        attempt = self.ConsumedAttemptInput(
            request_id=self.req_id,
            plan_id=plan_id,
            plan_item_id=999999,
            plan_ordinal=0,
            plan_strategy="default",
            plan_canonical_query_key=None,
            plan_repeat_group=None,
            plan_generator_id="g1",
            query="q",
            outcome="no_match",
            plan_item_count=1,
        )
        with self.assertRaisesRegex(
            ValueError, "plan_item_id=999999 does not belong",
        ):
            self.db.record_consumed_search_attempt(attempt)
        # No log row, cursor untouched.
        rows = self.db.get_search_history(self.req_id)
        self.assertEqual(rows, [])
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["next_plan_ordinal"], 0)
        self.assertEqual(req["plan_cycle_count"], 0)


@requires_postgres
class TestSearchPlanStats(unittest.TestCase):
    """U8: ``get_search_plan_stats`` aggregates plan-aware search_log
    rows into per-slot and per-query-group usefulness stats. Cache
    attribution is ``cycle_only`` because there are no per-search
    cache columns on ``search_log`` today.
    """

    def setUp(self):
        from lib.pipeline_db import (
            ConsumedAttemptInput,
            NonConsumingAttemptInput,
            SearchPlanItemInput,
        )
        self.SearchPlanItemInput = SearchPlanItemInput
        self.ConsumedAttemptInput = ConsumedAttemptInput
        self.NonConsumingAttemptInput = NonConsumingAttemptInput
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="stats-mbid",
            artist_name="Stats", album_title="Test",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def _make_plan(self, *, ordinals: int = 2, generator_id: str = "g1"):
        items = [
            self.SearchPlanItemInput(
                ordinal=i, strategy="default" if i == 0 else f"strategy_{i}",
                query=f"q{i}", canonical_query_key=f"k{i}",
                repeat_group="default-3" if i == 0 else None,
            )
            for i in range(ordinals)
        ]
        return self.db.create_successful_search_plan(
            request_id=self.req_id, generator_id=generator_id,
            items=items, set_active=True,
        )

    def _consume(self, plan_id, plan_item_id, ordinal, strategy, query,
                 *, outcome, plan_item_count, **kw):
        req = self.db.get_request(self.req_id)
        assert req is not None
        attempt = self.ConsumedAttemptInput(
            request_id=self.req_id,
            plan_id=plan_id, plan_item_id=plan_item_id,
            plan_ordinal=ordinal, plan_strategy=strategy,
            plan_canonical_query_key=kw.pop(
                "canonical_query_key", f"k{ordinal}"),
            plan_repeat_group=kw.pop("repeat_group", None),
            plan_generator_id="g1",
            query=query, outcome=outcome,
            plan_item_count=plan_item_count,
            cycle_count_snapshot=kw.pop(
                "cycle_count_snapshot", int(req["plan_cycle_count"])),
            elapsed_s=kw.pop("elapsed_s", 1.0),
            result_count=kw.pop("result_count", 0),
            browse_time_s=kw.pop("browse_time_s", 0.5),
            match_time_s=kw.pop("match_time_s", 0.25),
            peers_browsed=kw.pop("peers_browsed", 4),
            peers_browsed_lazy=kw.pop("peers_browsed_lazy", 1),
            fanout_waves=kw.pop("fanout_waves", 1),
            apply_scheduler_attempt=kw.pop("apply_scheduler_attempt", True),
            scheduler_success=kw.pop("scheduler_success", False),
        )
        return self.db.record_consumed_search_attempt(attempt)

    def _items_for(self, plan_id):
        cur = self.db._execute(
            "SELECT id, ordinal, strategy, canonical_query_key, "
            "repeat_group FROM search_plan_items WHERE plan_id = %s "
            "ORDER BY ordinal",
            (plan_id,),
        )
        return [dict(r) for r in cur.fetchall()]

    def test_stats_groups_by_slot_and_query_group(self):
        plan_id = self._make_plan(ordinals=2)
        items = self._items_for(plan_id)
        # Run two attempts on slot 0, one on slot 1.
        self._consume(plan_id, items[0]["id"], 0, "default", "q0",
                      outcome="no_match", plan_item_count=2,
                      repeat_group="default-3")
        self._consume(plan_id, items[1]["id"], 1, "strategy_1", "q1",
                      outcome="found", plan_item_count=2,
                      result_count=5, elapsed_s=2.0)
        # After wrap, slot 0 again.
        self._consume(plan_id, items[0]["id"], 0, "default", "q0",
                      outcome="no_results", plan_item_count=2,
                      repeat_group="default-3")

        stats = self.db.get_search_plan_stats(self.req_id)
        slots = stats.current.slots
        self.assertEqual(len(slots), 2)
        # Slots are ordered by ordinal.
        self.assertEqual(slots[0].identity["ordinal"], 0)
        self.assertEqual(slots[0].attempts, 2)
        self.assertEqual(slots[0].consumed_attempts, 2)
        self.assertEqual(
            slots[0].outcome_counts,
            {"no_match": 1, "no_results": 1})
        self.assertEqual(slots[1].identity["ordinal"], 1)
        self.assertEqual(slots[1].attempts, 1)
        self.assertEqual(slots[1].outcome_counts, {"found": 1})
        # Cache attribution is honest about cycle-only counters.
        self.assertEqual(stats.current.cache_attribution_level, "cycle_only")
        self.assertFalse(stats.current.cache_per_search_available)
        # Query groups exist with stable (repeat_group, key) order.
        # ordinal-1 has no repeat_group (sorts first as ""),
        # ordinal-0 carries "default-3".
        order = [
            (g.identity["repeat_group"] or "",
             g.identity["canonical_query_key"] or "")
            for g in stats.current.query_groups
        ]
        self.assertEqual(order, sorted(order))

    def test_stats_includes_legacy_bucket_when_current_only_false(self):
        # One legacy log without plan context.
        self.db.log_search(
            request_id=self.req_id, query="legacy",
            outcome="no_match", variant="v1",
        )
        plan_id = self._make_plan(ordinals=1)
        items = self._items_for(plan_id)
        self._consume(plan_id, items[0]["id"], 0, "default", "q0",
                      outcome="found", plan_item_count=1,
                      repeat_group="default-3")

        # Default current_only=True: no legacy in current cohort,
        # legacy bucket only appears in superseded_and_legacy when
        # current_only=False.
        stats_current = self.db.get_search_plan_stats(self.req_id)
        self.assertIsNone(stats_current.current.legacy_bucket)
        self.assertEqual(
            stats_current.superseded_and_legacy.slots, [])
        self.assertIsNone(
            stats_current.superseded_and_legacy.legacy_bucket)

        stats_full = self.db.get_search_plan_stats(
            self.req_id, current_only=False)
        self.assertIsNotNone(stats_full.superseded_and_legacy.legacy_bucket)
        legacy = stats_full.superseded_and_legacy.legacy_bucket
        assert legacy is not None
        self.assertEqual(legacy.attempts, 1)
        self.assertEqual(legacy.identity, {"kind": "legacy"})

    def test_stats_counts_non_consuming_pre_attempt_rows(self):
        plan_id = self._make_plan(ordinals=1)
        items = self._items_for(plan_id)
        # Pre-attempt failure: non-consuming.
        self.db.record_non_consuming_search_attempt(
            self.NonConsumingAttemptInput(
                request_id=self.req_id, outcome="empty_query",
                plan_id=plan_id, plan_item_id=items[0]["id"],
                plan_ordinal=0, plan_strategy="default",
                plan_canonical_query_key="k0", plan_repeat_group=None,
                plan_generator_id="g1", query="",
            ))
        # Consumed attempt that yields a found.
        self._consume(plan_id, items[0]["id"], 0, "default", "q0",
                      outcome="found", plan_item_count=1)

        stats = self.db.get_search_plan_stats(self.req_id)
        # Both rows live on slot 0; non-consuming counted separately.
        self.assertEqual(len(stats.current.slots), 1)
        slot0 = stats.current.slots[0]
        self.assertEqual(slot0.attempts, 2)
        self.assertEqual(slot0.consumed_attempts, 1)
        self.assertEqual(slot0.non_consuming_attempts, 1)
        self.assertEqual(slot0.stale_completion_attempts, 0)


class _FakeCursor:
    def __init__(self, conn, raise_on_execute=None, mark_conn_closed_on_error=False):
        self._conn = conn
        self._raise_on_execute = raise_on_execute
        self._mark_conn_closed_on_error = mark_conn_closed_on_error
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        exc = self._raise_on_execute
        if exc is not None:
            if self._mark_conn_closed_on_error:
                self._conn.closed = 2
            raise exc

    def fetchone(self):
        return {"ok": 1}


class _FakeConn:
    def __init__(self, raise_on_execute=None, mark_conn_closed_on_error=False):
        self.closed = 0
        self.autocommit = False
        self._raise_on_execute = raise_on_execute
        self._mark_conn_closed_on_error = mark_conn_closed_on_error
        self.cursors = []

    def cursor(self, *args, **kwargs):
        cur = _FakeCursor(
            self,
            raise_on_execute=self._raise_on_execute,
            mark_conn_closed_on_error=self._mark_conn_closed_on_error,
        )
        self.cursors.append(cur)
        return cur

    def close(self):
        self.closed = 1


class TestPipelineDBReconnectOnDeadConn(unittest.TestCase):
    """``PipelineDB._execute`` must transparently reconnect when the
    server has closed the socket between statements.

    Reproduces the live failure mode from the import-preview worker:
    the connection sits idle between jobs long enough that PostgreSQL
    (or an intermediary) tears it down, libpq doesn't notice until the
    next send, and the next ``cur.execute`` raises ``OperationalError``
    with ``conn.closed != 0``. ``_execute`` must reconnect once and
    retry the statement instead of letting the exception escape and
    crash the worker thread.
    """

    def test_reconnects_and_retries_on_operational_error_with_dead_conn(self):
        import psycopg2 as real_psycopg2

        dead_conn = _FakeConn(
            raise_on_execute=real_psycopg2.OperationalError(
                "server closed the connection unexpectedly"
            ),
            mark_conn_closed_on_error=True,
        )
        live_conn = _FakeConn()
        conn_iter = iter([dead_conn, live_conn])

        with patch("psycopg2.connect", side_effect=lambda *a, **kw: next(conn_iter)):
            from lib import pipeline_db
            db = pipeline_db.PipelineDB(dsn="postgresql://fake")
            cur = db._execute("SELECT 1")

        # We consumed both fake conns: initial + reconnect-on-retry.
        self.assertEqual(db.conn, live_conn)
        # The retry happened on the live conn.
        self.assertEqual(len(live_conn.cursors), 1)
        self.assertIs(cur, live_conn.cursors[0])
        self.assertEqual(cur.executed, [("SELECT 1", None)])

    def test_does_not_retry_when_conn_still_open_after_error(self):
        """Statement-level OperationalError (e.g. statement_timeout) keeps
        the connection open. We must NOT silently retry — that would
        mask real query failures and could double-execute side effects.
        Re-raise so the caller sees the error.
        """
        import psycopg2 as real_psycopg2

        live_but_failing_conn = _FakeConn(
            raise_on_execute=real_psycopg2.OperationalError(
                "canceling statement due to statement timeout"
            ),
            mark_conn_closed_on_error=False,
        )
        conn_iter = iter([live_but_failing_conn])

        with patch("psycopg2.connect", side_effect=lambda *a, **kw: next(conn_iter)):
            from lib import pipeline_db
            db = pipeline_db.PipelineDB(dsn="postgresql://fake")
            with self.assertRaises(real_psycopg2.OperationalError):
                db._execute("SELECT 1")

        # Only the original conn was used; no reconnect.
        self.assertEqual(db.conn, live_but_failing_conn)
        self.assertEqual(len(live_but_failing_conn.cursors), 1)


@requires_postgres
class TestFieldResolutionRecording(unittest.TestCase):
    """``record_field_resolution`` UPSERT contract against real PG.

    Side table: ``album_request_field_resolutions`` (migration 030).
    Tests pin the UPSERT semantics: fresh row carries ``attempts=1``;
    conflict increments ``attempts`` and updates status/reason/timestamp.
    """

    def _seed_request(self, db):
        req_id = db.add_request(
            mb_release_id="rec-mbid-0001",
            mb_release_group_id=None,
            mb_artist_id=None,
            discogs_release_id=None,
            artist_name="Test Artist",
            album_title="Test Album",
            year=2026,
            country="US",
            source="request",
        )
        return req_id

    def test_record_field_resolution_round_trip_preserves_request_and_field(self):
        db = make_db()
        req_id = self._seed_request(db)

        db.record_field_resolution(
            request_id=req_id,
            field_name="release_group_year",
            status="resolved",
            reason_code=None,
        )

        row = db.get_field_resolution(req_id, "release_group_year")
        assert row is not None
        self.assertEqual(row["request_id"], req_id)
        self.assertEqual(row["field_name"], "release_group_year")
        self.assertEqual(row["status"], "resolved")
        self.assertIsNone(row["reason_code"])
        self.assertEqual(row["attempts"], 1)
        self.assertIsNotNone(row["resolved_at"])

    def test_conflict_increments_attempts_and_updates_fields(self):
        db = make_db()
        req_id = self._seed_request(db)

        db.record_field_resolution(
            req_id, "release_group_year",
            "unresolved_mirror_unavailable", "URLError",
        )
        # Capture the first row's resolved_at to assert it advances.
        first = db.get_field_resolution(req_id, "release_group_year")
        assert first is not None
        first_resolved_at = first["resolved_at"]

        # Sleep enough to ensure NOW() advances (microsecond resolution
        # may be the same on very fast machines; use a small delay).
        time.sleep(0.05)

        db.record_field_resolution(
            req_id, "release_group_year",
            "resolved", None,
        )

        row = db.get_field_resolution(req_id, "release_group_year")
        assert row is not None
        self.assertEqual(row["status"], "resolved")
        self.assertIsNone(row["reason_code"])
        self.assertEqual(row["attempts"], 2)
        self.assertGreater(row["resolved_at"], first_resolved_at)

    def test_unique_constraint_one_row_per_field(self):
        """UNIQUE(request_id, field_name) — distinct field_name gives a 2nd row."""
        db = make_db()
        req_id = self._seed_request(db)

        db.record_field_resolution(
            req_id, "release_group_year", "resolved", None,
        )
        db.record_field_resolution(
            req_id, "catalog_number", "unresolved_404", "http_404",
        )

        cur = db._execute(
            "SELECT COUNT(*)::int AS n FROM album_request_field_resolutions "
            "WHERE request_id = %s",
            (req_id,),
        )
        row = cur.fetchone() or {}
        self.assertEqual(row.get("n"), 2)

    def test_fk_cascade_on_request_delete(self):
        db = make_db()
        req_id = self._seed_request(db)
        db.record_field_resolution(
            req_id, "release_group_year", "resolved", None,
        )
        # Sanity check.
        self.assertIsNotNone(
            db.get_field_resolution(req_id, "release_group_year"),
        )
        # Delete the parent.
        db._execute("DELETE FROM album_requests WHERE id = %s", (req_id,))
        # Migration 030's FK is ON DELETE CASCADE.
        self.assertIsNone(
            db.get_field_resolution(req_id, "release_group_year"),
        )

    def test_get_field_resolution_returns_none_when_absent(self):
        db = make_db()
        req_id = self._seed_request(db)
        self.assertIsNone(db.get_field_resolution(req_id, "track_artist"))


@requires_postgres
class TestMarkImportedWithRescue(unittest.TestCase):
    """U14: long-tail-rescue event capture against real PG.

    Pins the atomic four-write contract:
      1. ``status`` → ``'imported'``
      2. ``rescued_at`` → ``NOW()`` (when prior unfindable category set)
      3. ``prior_unfindable_category`` → the cleared category value
      4. ``unfindable_category`` → ``NULL`` (the rescue IS the resolution)

    All four mutations commit together OR none of them apply. The
    method follows the ``replace_request_with_new_mbid`` autocommit-flip
    pattern: ``conn.autocommit=False`` + explicit ``commit()`` /
    ``rollback()`` in try/finally.
    """

    UNFINDABLE_CATEGORIES = (
        "artist_absent",
        "album_absent_artist_present",
        "one_track_structural",
        "wrong_pressing_available",
    )

    def _seed_wanted(self, db, *, category=None, rescued_at=None,
                     prior_category=None):
        rid = db.add_request(
            mb_release_id=f"rescue-{category or 'none'}",
            artist_name="Rescue Artist",
            album_title="Rescue Album",
            source="request",
        )
        # Set the unfindable category WHILE the row is still wanted —
        # ``set_unfindable_category`` is guarded by ``status='wanted'`` in
        # production (lost-update protection against concurrent rescue),
        # so a seed helper that flipped to downloading first would silently
        # no-op the category write.
        if category is not None:
            ts = datetime(2026, 5, 20, tzinfo=UTC)
            db.set_unfindable_category(
                rid, category=category, categorised_at=ts,
            )
        # Move to downloading so the imported transition is the canonical one.
        db._execute(
            "UPDATE album_requests SET status = 'downloading' WHERE id = %s",
            (rid,),
        )
        if rescued_at is not None or prior_category is not None:
            db._execute(
                "UPDATE album_requests "
                "SET rescued_at = %s, prior_unfindable_category = %s "
                "WHERE id = %s",
                (rescued_at, prior_category, rid),
            )
        return rid

    def test_rescue_writes_three_columns_on_first_import_from_unfindable(self):
        """Happy path: row with unfindable_category gets rescue stamp."""
        for category in self.UNFINDABLE_CATEGORIES:
            with self.subTest(category=category):
                db = make_db()
                rid = self._seed_wanted(db, category=category)

                db.mark_imported_with_rescue(rid, beets_distance=0.05)

                row = db.get_request(rid)
                assert row is not None
                self.assertEqual(row["status"], "imported")
                self.assertIsNone(row["unfindable_category"])
                self.assertEqual(
                    row["prior_unfindable_category"], category)
                self.assertIsNotNone(row["rescued_at"])
                # Sanity: the imported extras also landed.
                rescued_distance = row["beets_distance"]
                assert rescued_distance is not None
                self.assertEqual(float(rescued_distance), 0.05)

    def test_no_rescue_stamp_when_unfindable_was_null(self):
        """No prior category → ``rescued_at`` stays NULL."""
        db = make_db()
        rid = self._seed_wanted(db, category=None)

        db.mark_imported_with_rescue(rid, beets_distance=0.05)

        row = db.get_request(rid)
        assert row is not None
        self.assertEqual(row["status"], "imported")
        self.assertIsNone(row["rescued_at"])
        self.assertIsNone(row["prior_unfindable_category"])
        self.assertIsNone(row["unfindable_category"])

    def test_first_rescue_wins_re_import_does_not_overwrite(self):
        """One-shot capture: a row already rescued is not re-stamped.

        Simulates: rescued → Replace → new request → re-categorised →
        imports again. The second import must NOT bump ``rescued_at``
        nor change ``prior_unfindable_category``. Original rescue
        instant is the canonical audit record.
        """
        db = make_db()
        original_rescue_at = datetime(2026, 1, 15, tzinfo=UTC)
        rid = self._seed_wanted(
            db,
            category="album_absent_artist_present",
            rescued_at=original_rescue_at,
            prior_category="artist_absent",
        )

        db.mark_imported_with_rescue(rid, beets_distance=0.05)

        row = db.get_request(rid)
        assert row is not None
        self.assertEqual(row["status"], "imported")
        # rescued_at is immutable once set.
        self.assertEqual(row["rescued_at"], original_rescue_at)
        # prior_unfindable_category is immutable too — original rescue wins.
        self.assertEqual(row["prior_unfindable_category"], "artist_absent")
        # Current unfindable_category is still cleared (the rescue IS
        # the resolution, regardless of one-shot-stamp semantics).
        self.assertIsNone(row["unfindable_category"])

    def test_atomic_rollback_on_mid_transaction_failure(self):
        """A forced failure inside the transaction leaves the row untouched.

        Forces an exception inside the autocommit-disabled block by
        passing an ``extra`` kwarg that references a non-existent
        column. The dynamic ``UPDATE`` raises ``UndefinedColumn``
        AFTER the row lock + read have been taken but BEFORE the
        commit fires — exactly the mid-flow scenario the autocommit-
        flip pattern exists to protect against.

        Without ``autocommit=False`` + try/finally, three separate
        UPDATEs in autocommit mode would leave a half-rescued row in
        the audit trail. With the pattern, the row is rolled back to
        its pre-call state and autocommit is restored for subsequent
        calls.
        """
        db = make_db()
        rid = self._seed_wanted(db, category="artist_absent")

        before = db.get_request(rid)
        assert before is not None
        self.assertEqual(before["status"], "downloading")
        self.assertEqual(before["unfindable_category"], "artist_absent")

        with self.assertRaises(psycopg2.errors.UndefinedColumn):
            # ``column_that_does_not_exist`` rides through the
            # dynamic ``sets`` builder into the UPDATE statement,
            # raising ``UndefinedColumn`` inside the transaction.
            db.mark_imported_with_rescue(
                rid, column_that_does_not_exist=1,
            )

        # All writes rolled back together — the row is untouched.
        after = db.get_request(rid)
        assert after is not None
        self.assertEqual(after["status"], "downloading")
        self.assertEqual(after["unfindable_category"], "artist_absent")
        self.assertIsNone(after["rescued_at"])
        self.assertIsNone(after["prior_unfindable_category"])
        # Autocommit restored after the failure so subsequent calls work.
        self.assertTrue(db.conn.autocommit)
        # Sanity: the next call still works (proves rollback cleared
        # the failed transaction state).
        db.mark_imported_with_rescue(rid, beets_distance=0.07)
        retried = db.get_request(rid)
        assert retried is not None
        self.assertEqual(retried["status"], "imported")
        self.assertEqual(retried["prior_unfindable_category"], "artist_absent")


class _RecordUnfindableRunMetricsKwargs(TypedDict):
    """Exact kwarg shape of ``PipelineDB.record_unfindable_run_metrics``
    (#1112) -- lets the Rule A round-trip test build ONE typed dict, pass
    it as ``**kwargs`` to the writer, then loop over the SAME dict for
    every assertion (F6, review round 1) with full pyright coverage on
    both the call and the loop."""

    cohort_total: int
    due_backlog_at_start: int
    batch_limit: int
    candidates_processed: int
    probes_attempted: int
    categorised_count: int
    downgraded_count: int
    no_change_count: int
    probe_failed_count: int
    not_due_count: int
    request_not_found_count: int
    breaker_tripped: bool
    duration_seconds: float


@requires_postgres
class TestUnfindableDetectionPipelineDB(unittest.TestCase):
    """U13: real-PG round-trip coverage for the 4 detection writers.

    The FakePipelineDB mirrors give us shape coverage; this class pins
    the production SQL against the real fixture so the CHECK
    constraints (migration 028's 4-category vocabulary) and the
    ``AND status='wanted'`` lost-update guards behave exactly like
    operators will see them on doc2.

    Mirrors the ``TestMarkImportedWithRescue`` style next door.
    """

    UNFINDABLE_CATEGORIES = (
        "artist_absent",
        "album_absent_artist_present",
        "one_track_structural",
        "wrong_pressing_available",
    )

    def _seed_wanted(self, db, *, artist_name="A", album_title="B",
                     mbid=None):
        return db.add_request(
            mb_release_id=mbid or f"unf-{artist_name}-{album_title}",
            artist_name=artist_name,
            album_title=album_title,
            source="request",
        )

    # ---- list_unfindable_probe_candidates ----

    def test_list_candidates_orders_oldest_first_and_filters_by_cadence(self):
        """NULL probes sort first; rows fresher than window are excluded."""
        db = make_db()
        now = datetime.now(UTC)
        # Three wanted rows.
        rid_null = self._seed_wanted(db, artist_name="Null", mbid="unf-null")
        rid_old = self._seed_wanted(db, artist_name="Old", mbid="unf-old")
        rid_fresh = self._seed_wanted(
            db, artist_name="Fresh", mbid="unf-fresh")
        # Old probe = 10 days ago (older than 7d window → eligible).
        db.record_artist_probe(
            rid_old, match_count=0,
            observed_at=now - timedelta(days=10),
        )
        # Fresh probe = 1 day ago (inside window → ineligible).
        db.record_artist_probe(
            rid_fresh, match_count=0,
            observed_at=now - timedelta(days=1),
        )

        cands = db.list_unfindable_probe_candidates(
            limit=10, probe_interval_days=7,
        )
        ids = [c["id"] for c in cands]
        # NULL probe sorts first.
        self.assertEqual(ids[0], rid_null)
        # Old probe included; fresh probe excluded.
        self.assertIn(rid_old, ids)
        self.assertNotIn(rid_fresh, ids)

    def test_list_candidates_excludes_non_wanted(self):
        """A row in any non-wanted status is excluded from the cohort."""
        db = make_db()
        rid = self._seed_wanted(db, mbid="unf-imp")
        db._execute(
            "UPDATE album_requests SET status = 'imported' WHERE id = %s",
            (rid,),
        )
        cands = db.list_unfindable_probe_candidates(
            limit=10, probe_interval_days=7,
        )
        self.assertNotIn(rid, [c["id"] for c in cands])

    # ---- record_artist_probe ----

    def test_record_artist_probe_round_trips_count_and_timestamp(self):
        """Probe column updates land and round-trip through SELECT."""
        db = make_db()
        rid = self._seed_wanted(db, mbid="unf-rec-1")
        ts = datetime(2026, 5, 26, 12, 0, tzinfo=UTC)

        db.record_artist_probe(rid, match_count=42, observed_at=ts)

        row = db.get_request(rid)
        assert row is not None
        self.assertEqual(row["last_artist_probe_match_count"], 42)
        self.assertEqual(row["last_artist_probe_at"], ts)

    def test_record_artist_probe_silent_noop_when_status_not_wanted(self):
        """The lost-update guard makes late writes invisible — no error."""
        db = make_db()
        rid = self._seed_wanted(db, mbid="unf-rec-2")
        # Capture the pre-existing probe state (NULL by default).
        before = db.get_request(rid)
        assert before is not None
        self.assertIsNone(before["last_artist_probe_at"])
        # Concurrent rescue flips status mid-probe.
        db._execute(
            "UPDATE album_requests SET status = 'imported' WHERE id = %s",
            (rid,),
        )
        # Detection's late write — must be a silent no-op.
        ts = datetime(2026, 5, 26, 12, 0, tzinfo=UTC)
        db.record_artist_probe(rid, match_count=99, observed_at=ts)
        after = db.get_request(rid)
        assert after is not None
        # Probe columns untouched.
        self.assertIsNone(after["last_artist_probe_at"])
        self.assertIsNone(after["last_artist_probe_match_count"])

    # ---- set_unfindable_category ----

    def test_set_unfindable_category_round_trips_all_four_categories(self):
        """Every valid category round-trips; CHECK constraint passes."""
        ts = datetime(2026, 5, 26, tzinfo=UTC)
        for category in self.UNFINDABLE_CATEGORIES:
            with self.subTest(category=category):
                db = make_db()
                rid = self._seed_wanted(db, mbid=f"unf-set-{category}")
                db.set_unfindable_category(
                    rid, category=category, categorised_at=ts,
                )
                row = db.get_request(rid)
                assert row is not None
                self.assertEqual(row["unfindable_category"], category)
                self.assertEqual(row["unfindable_categorised_at"], ts)

    def test_set_unfindable_category_rejects_off_vocabulary_value(self):
        """An unknown category trips the CHECK constraint → IntegrityError."""
        from psycopg2.errors import CheckViolation

        db = make_db()
        rid = self._seed_wanted(db, mbid="unf-set-bad")
        ts = datetime(2026, 5, 26, tzinfo=UTC)
        with self.assertRaises(CheckViolation):
            db.set_unfindable_category(
                rid, category="garbage_value", categorised_at=ts,
            )

    def test_set_unfindable_category_silent_noop_when_status_not_wanted(self):
        """Late verdict write does not clobber a row already past wanted."""
        db = make_db()
        rid = self._seed_wanted(db, mbid="unf-set-imp")
        db._execute(
            "UPDATE album_requests SET status = 'imported' WHERE id = %s",
            (rid,),
        )
        ts = datetime(2026, 5, 26, tzinfo=UTC)
        db.set_unfindable_category(
            rid, category="artist_absent", categorised_at=ts,
        )
        row = db.get_request(rid)
        assert row is not None
        # Category never landed; row remains in imported shape.
        self.assertIsNone(row["unfindable_category"])
        self.assertEqual(row["status"], "imported")

    # ---- get_unfindable_search_log_signal ----

    def test_search_log_signal_aggregates_zero_find_and_wrong_pressing(self):
        """Hand-computed aggregates match the production SQL."""
        from lib.pipeline_db import (
            ConsumedAttemptInput,
            SearchPlanItemInput,
        )

        db = make_db()
        rid = self._seed_wanted(db, mbid="unf-sig")
        # Seed a plan + advance the cursor 4 times so we have
        # 4 distinct ``plan_cycle_snapshot`` values in the log.
        # Cycles 0..3 from four ordinal consumptions.
        plan_id = db.create_successful_search_plan(
            request_id=rid,
            generator_id="unf-gen",
            items=[
                SearchPlanItemInput(
                    ordinal=0, strategy="default",
                    query="q0", canonical_query_key="q0"),
            ],
        )
        active = db.get_active_search_plan(rid)
        assert active is not None
        item_id = active.items[0].id

        def _attempt(cycle_idx: int, *, outcome: str,
                     rejection_reason: str | None = None,
                     matcher_score_top1: float | None = None):
            return ConsumedAttemptInput(
                request_id=rid,
                plan_id=plan_id,
                plan_item_id=item_id,
                plan_ordinal=0,
                plan_strategy="default",
                plan_canonical_query_key="q0",
                plan_repeat_group=None,
                plan_generator_id="unf-gen",
                query="q0",
                outcome=outcome,
                plan_item_count=1,
                cycle_count_snapshot=cycle_idx,
                apply_scheduler_attempt=True,
                scheduler_success=(outcome == "found"),
                rejection_reason=rejection_reason,
                matcher_score_top1=matcher_score_top1,
            )

        # Cycle 0: no_match w/ wrong-pressing signature (high score) → hit.
        db.record_consumed_search_attempt(_attempt(
            0, outcome="no_match",
            rejection_reason="strict_count_mismatch",
            matcher_score_top1=0.9,
        ))
        # Cycle 1: one found → cycle NOT zero-find.
        db.record_consumed_search_attempt(_attempt(1, outcome="found"))
        # Cycle 2: no_match w/ low score → not a wrong-pressing hit;
        # AND no found → counts as a zero-find cycle.
        db.record_consumed_search_attempt(_attempt(
            2, outcome="no_match",
            rejection_reason="strict_count_mismatch",
            matcher_score_top1=0.5,
        ))
        # Cycle 3: no_results → zero-find cycle.
        db.record_consumed_search_attempt(_attempt(3, outcome="no_results"))

        sig = db.get_unfindable_search_log_signal(
            rid, window_days=30, matcher_score_threshold=0.85,
        )
        # Cycles 0, 2, 3 are zero-find (cycle 1 had the found row).
        self.assertEqual(sig.zero_find_cycles, 3)
        # One wrong-pressing hit (cycle 0).
        self.assertEqual(sig.wrong_pressing_hits, 1)

    def test_search_log_signal_byte_identical_with_cross_request_conflict_marker(
        self,
    ):
        """#1196 item 2: the cross-request enqueue-guard skip marker must
        NEVER change unfindable-classification inputs. Exact repro of
        ``test_search_log_signal_aggregates_zero_find_and_wrong_pressing``
        above, with ``cross_request_conflict_request_ids`` populated on
        EVERY row (found, wrong-pressing-hit, and plain no_match/
        no_results) -- ``get_unfindable_search_log_signal``'s SQL never
        references the new column, so both aggregates must come back
        byte-identical to the marker-free world."""
        from lib.pipeline_db import (
            ConsumedAttemptInput,
            SearchPlanItemInput,
        )

        db = make_db()
        rid = self._seed_wanted(db, mbid="unf-sig-conflict")
        plan_id = db.create_successful_search_plan(
            request_id=rid,
            generator_id="unf-gen",
            items=[
                SearchPlanItemInput(
                    ordinal=0, strategy="default",
                    query="q0", canonical_query_key="q0"),
            ],
        )
        active = db.get_active_search_plan(rid)
        assert active is not None
        item_id = active.items[0].id

        def _attempt(cycle_idx: int, *, outcome: str,
                     rejection_reason: str | None = None,
                     matcher_score_top1: float | None = None):
            return ConsumedAttemptInput(
                request_id=rid,
                plan_id=plan_id,
                plan_item_id=item_id,
                plan_ordinal=0,
                plan_strategy="default",
                plan_canonical_query_key="q0",
                plan_repeat_group=None,
                plan_generator_id="unf-gen",
                query="q0",
                outcome=outcome,
                plan_item_count=1,
                cycle_count_snapshot=cycle_idx,
                apply_scheduler_attempt=True,
                scheduler_success=(outcome == "found"),
                rejection_reason=rejection_reason,
                matcher_score_top1=matcher_score_top1,
                # The ONLY difference from the marker-free scenario
                # above -- every row also carries the guard-skip marker.
                cross_request_conflict_request_ids=(4242,),
            )

        db.record_consumed_search_attempt(_attempt(
            0, outcome="no_match",
            rejection_reason="strict_count_mismatch",
            matcher_score_top1=0.9,
        ))
        db.record_consumed_search_attempt(_attempt(1, outcome="found"))
        db.record_consumed_search_attempt(_attempt(
            2, outcome="no_match",
            rejection_reason="strict_count_mismatch",
            matcher_score_top1=0.5,
        ))
        db.record_consumed_search_attempt(_attempt(3, outcome="no_results"))

        sig = db.get_unfindable_search_log_signal(
            rid, window_days=30, matcher_score_threshold=0.85,
        )
        self.assertEqual(sig.zero_find_cycles, 3)
        self.assertEqual(sig.wrong_pressing_hits, 1)
        # Also prove the marker itself landed (this test isn't
        # accidentally exercising the marker-free code path).
        rows = db.get_search_history(rid)
        self.assertTrue(all(
            row["cross_request_conflict_request_ids"] == [4242]
            for row in rows
        ))

    # ---- record_unfindable_run_metrics (#1112) ----

    def test_record_unfindable_run_metrics_round_trip_preserves_every_field(
        self,
    ) -> None:
        """Every kwarg passed to the writer reads back via the getter
        (Rule A, canonical loop form -- a 15th column can't silently
        skip assertion)."""
        db = make_db()

        # candidates_processed (243) = categorised(11) + downgraded(2) +
        # no_change(190) + probe_failed(34) + not_due(1) +
        # request_not_found(5); probes_attempted (237) = candidates_processed
        # - not_due(1) - request_not_found(5). Migration 077's two CHECK
        # constraints (#1112 review round 2, R5) enforce both arithmetically
        # -- a fixture that violates either now fails INSERT, not just the
        # partition-invariant prose three lines away.
        kwargs: _RecordUnfindableRunMetricsKwargs = {
            "cohort_total": 1301,
            "due_backlog_at_start": 686,
            "batch_limit": 240,
            "candidates_processed": 243,
            "probes_attempted": 237,
            "categorised_count": 11,
            "downgraded_count": 2,
            "no_change_count": 190,
            "probe_failed_count": 34,
            "not_due_count": 1,
            "request_not_found_count": 5,
            "breaker_tripped": False,
            "duration_seconds": 6961.5,
        }
        new_id = db.record_unfindable_run_metrics(**kwargs)

        rows = db.get_unfindable_run_metrics(limit=5)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["id"], new_id)
        self.assertIsInstance(row["created_at"], datetime)
        for key, value in kwargs.items():
            self.assertEqual(
                row[key], value, f"field {key} was dropped at the PG boundary")

    def test_record_unfindable_run_metrics_defaults_outcome_counts_to_zero(
        self,
    ) -> None:
        """A breaker-tripped run before any outcome is recorded still
        writes a valid row -- the six outcome counts default to 0."""
        db = make_db()

        db.record_unfindable_run_metrics(
            cohort_total=50,
            due_backlog_at_start=50,
            batch_limit=240,
            candidates_processed=0,
            probes_attempted=0,
            breaker_tripped=True,
            duration_seconds=3.2,
        )

        row = db.get_unfindable_run_metrics(limit=1)[0]
        self.assertEqual(row["candidates_processed"], 0)
        self.assertEqual(row["probes_attempted"], 0)
        self.assertTrue(row["breaker_tripped"])
        for key in (
            "categorised_count", "downgraded_count", "no_change_count",
            "probe_failed_count", "not_due_count",
            "request_not_found_count",
        ):
            self.assertEqual(row[key], 0, key)

    def test_get_unfindable_run_metrics_orders_newest_first(self) -> None:
        db = make_db()
        for probes in (10, 20, 30):
            db.record_unfindable_run_metrics(
                cohort_total=100, due_backlog_at_start=100,
                batch_limit=240, candidates_processed=probes,
                probes_attempted=probes,
                # no_change_count=probes satisfies migration 077's
                # partition CHECK (the six outcome counts must sum to
                # candidates_processed) with every other count at its
                # zero default.
                no_change_count=probes,
                breaker_tripped=False, duration_seconds=1.0,
            )

        rows = db.get_unfindable_run_metrics(limit=10)
        self.assertEqual([r["probes_attempted"] for r in rows], [30, 20, 10])

    def test_record_unfindable_run_metrics_rejects_non_partitioning_counts(
        self,
    ) -> None:
        """unfindable_run_metrics_partition_check (migration 077, #1112
        review round 2 R5): the six RESULT_* outcome counts must sum to
        candidates_processed exactly, DB-enforced."""
        from psycopg2.errors import CheckViolation

        db = make_db()
        with self.assertRaises(CheckViolation):
            db.record_unfindable_run_metrics(
                cohort_total=10, due_backlog_at_start=5,
                batch_limit=5, candidates_processed=5, probes_attempted=5,
                breaker_tripped=False, duration_seconds=1.0,
                categorised_count=1, no_change_count=1,  # sums to 2, not 5
            )

    def test_record_unfindable_run_metrics_rejects_wrong_probes_attempted(
        self,
    ) -> None:
        """unfindable_run_metrics_probes_attempted_check (migration 077,
        #1112 review round 2 R5): probes_attempted must equal
        candidates_processed minus not_due_count minus
        request_not_found_count, DB-enforced."""
        from psycopg2.errors import CheckViolation

        db = make_db()
        with self.assertRaises(CheckViolation):
            db.record_unfindable_run_metrics(
                cohort_total=10, due_backlog_at_start=5,
                batch_limit=5, candidates_processed=5,
                probes_attempted=5,  # should be 5 - 0 - 2 = 3
                breaker_tripped=False, duration_seconds=1.0,
                no_change_count=3, request_not_found_count=2,
            )


@requires_postgres
class TestYoutubeAlbumMappings(unittest.TestCase):
    """Integration tests for PipelineDB youtube_album_mappings CRUD (U4).

    Exercises the real PostgreSQL CRUD against migration 034. The atomic
    replace test verifies that mid-replace state is never visible — a
    concurrent reader sees either the old matrix or the new, never an
    interleaved subset.
    """

    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def _row(self, **overrides: Any) -> PersistedYoutubeRow:
        fields: dict[str, Any] = {
            "yt_browse_id": "MPREb_abc",
            "yt_audio_playlist_id": "OLAK5uy_abc",
            "yt_url": "https://music.youtube.com/playlist?list=OLAK5uy_abc",
            "yt_year": 2020,
            "yt_track_count": 10,
            # Album-level facts the service writes alongside the row
            # (migration 036). Round 2 P0-1 + maintainability-5.
            "album_title": "Test Album",
            "album_artist": "Test Album Artist",
            "yt_tracks": [
                PersistedTrack(
                    title="Track 1", video_id="v1", length_seconds=200,
                    track_number=1, disc_number=1,
                    artists=[{"name": "Artist"}],
                ),
            ],
            "distances": [
                PersistedDistance(mbid="mb-1", distance=0.05),
            ],
        }
        fields.update(overrides)
        return PersistedYoutubeRow(**fields)

    def test_get_returns_none_when_pair_never_resolved(self):
        # Distinction matters: ``None`` = "never resolved" (cache MISS),
        # ``[]`` = "resolved to empty matrix" (cache HIT). See
        # ce-code-review finding #3.
        self.assertIsNone(
            self.db.get_youtube_album_mapping("rg-1", "mb"),
        )

    def test_get_returns_empty_list_after_upsert_of_empty_rows(self):
        # Upserting an empty matrix stamps the empty-resolution marker
        # so the next read returns ``[]`` (cache HIT) instead of
        # ``None`` (cache MISS). Without this, the resolver re-polls
        # YT on every cycle for empty-search release groups (R14).
        self.db.upsert_youtube_album_mapping("rg-empty", "mb", [])
        self.assertEqual(
            self.db.get_youtube_album_mapping("rg-empty", "mb"),
            [],
        )

    def test_empty_marker_cleared_on_non_empty_upsert(self):
        # An empty resolve followed by a non-empty resolve must clear the
        # empty marker — subsequent reads return the matrix, not [].
        self.db.upsert_youtube_album_mapping("rg-flip", "mb", [])
        self.assertEqual(
            self.db.get_youtube_album_mapping("rg-flip", "mb"), [])
        self.db.upsert_youtube_album_mapping("rg-flip", "mb", [
            self._row(yt_browse_id="MPREb_real"),
        ])
        got = self.db.get_youtube_album_mapping("rg-flip", "mb")
        self.assertIsNotNone(got)
        assert got is not None
        self.assertEqual([r["yt_browse_id"] for r in got], ["MPREb_real"])

    def test_upsert_inserts_new_rows_and_get_returns_them(self):
        rows = [
            self._row(yt_browse_id="MPREb_a"),
            self._row(yt_browse_id="MPREb_b"),
        ]

        self.db.upsert_youtube_album_mapping("rg-1", "mb", rows)

        got = self.db.get_youtube_album_mapping("rg-1", "mb")
        assert got is not None
        self.assertEqual(len(got), 2)
        self.assertEqual(
            [r["yt_browse_id"] for r in got],
            ["MPREb_a", "MPREb_b"],
        )
        # JSONB columns deserialize back into native Python lists/dicts.
        self.assertEqual(
            got[0]["yt_tracks"][0]["title"], "Track 1")
        # Per ce-code-review finding #25 the field is ``mbid``, not
        # ``mb_release_id`` — aligns with the service-side
        # ``ResolvedDistance.mbid`` wire contract.
        self.assertEqual(
            got[0]["distances"][0]["mbid"], "mb-1")

    def test_upsert_round_trip_preserves_every_field(self):
        """Rule A (``.claude/rules/test-fidelity.md``): every field of the
        typed ``PersistedYoutubeRow`` payload must round-trip through real
        PostgreSQL.

        Round 2 P0-1: ``album_title`` (and now ``album_artist``) were
        silently dropped because the INSERT column list didn't include
        them and ``psycopg2.extras.execute_values`` ignores extra dict
        keys. The Fake-based test stored the dict verbatim and never
        flagged the divergence. #546 W3 made the column list itself
        DERIVE from ``msgspec.structs.fields(PersistedYoutubeRow)`` so
        this class of bug can no longer be expressed — this test iterates
        the SAME derived field list and fails naming the offending field
        if a future drift somehow reappears.
        """
        row_in = self._row(
            yt_browse_id="MPREb_roundtrip",
            yt_audio_playlist_id="OLAK5uy_roundtrip",
            yt_url="https://music.youtube.com/playlist?list=OLAK5uy_roundtrip",
            yt_year=1996,
            yt_track_count=12,
            album_title="The Roundtrip Sessions",
            album_artist="Various Artists",
        )
        self.db.upsert_youtube_album_mapping("rg-rt", "mb", [row_in])
        rows_out = self.db.get_youtube_album_mapping("rg-rt", "mb")
        assert rows_out is not None
        self.assertEqual(len(rows_out), 1)
        for f in msgspec.structs.fields(PersistedYoutubeRow):
            expected = getattr(row_in, f.name)
            if f.name in ("yt_tracks", "distances"):
                expected = msgspec.to_builtins(expected)
            self.assertEqual(
                rows_out[0].get(f.name), expected,
                msg=f"field {f.name} was dropped at the PG boundary",
            )

    def test_get_orders_rows_by_yt_browse_id(self):
        self.db.upsert_youtube_album_mapping("rg-1", "mb", [
            self._row(yt_browse_id="MPREb_z"),
            self._row(yt_browse_id="MPREb_a"),
            self._row(yt_browse_id="MPREb_m"),
        ])

        got = self.db.get_youtube_album_mapping("rg-1", "mb")
        assert got is not None
        self.assertEqual(
            [r["yt_browse_id"] for r in got],
            ["MPREb_a", "MPREb_m", "MPREb_z"],
        )

    def test_upsert_atomically_replaces_existing_rows(self):
        """DELETE + INSERTs in one transaction; reader never sees partial state."""
        self.db.upsert_youtube_album_mapping("rg-1", "mb", [
            self._row(yt_browse_id="MPREb_old1"),
            self._row(yt_browse_id="MPREb_old2"),
            self._row(yt_browse_id="MPREb_old3"),
        ])

        self.db.upsert_youtube_album_mapping("rg-1", "mb", [
            self._row(yt_browse_id="MPREb_new1"),
            self._row(yt_browse_id="MPREb_new2"),
        ])

        got = self.db.get_youtube_album_mapping("rg-1", "mb")
        assert got is not None
        self.assertEqual(
            [r["yt_browse_id"] for r in got],
            ["MPREb_new1", "MPREb_new2"],
        )

    def test_upsert_does_not_affect_other_release_group_or_source(self):
        self.db.upsert_youtube_album_mapping("rg-1", "mb", [
            self._row(yt_browse_id="MPREb_a")])
        self.db.upsert_youtube_album_mapping("rg-2", "mb", [
            self._row(yt_browse_id="MPREb_b")])
        self.db.upsert_youtube_album_mapping("rg-1", "discogs", [
            self._row(yt_browse_id="MPREb_c")])

        # Replace only rg-1/mb.
        self.db.upsert_youtube_album_mapping("rg-1", "mb", [
            self._row(yt_browse_id="MPREb_a_v2")])

        rg1_mb = self.db.get_youtube_album_mapping("rg-1", "mb")
        rg2_mb = self.db.get_youtube_album_mapping("rg-2", "mb")
        rg1_discogs = self.db.get_youtube_album_mapping("rg-1", "discogs")
        assert rg1_mb is not None
        assert rg2_mb is not None
        assert rg1_discogs is not None
        self.assertEqual(
            [r["yt_browse_id"] for r in rg1_mb],
            ["MPREb_a_v2"],
        )
        self.assertEqual(
            [r["yt_browse_id"] for r in rg2_mb],
            ["MPREb_b"],
        )
        self.assertEqual(
            [r["yt_browse_id"] for r in rg1_discogs],
            ["MPREb_c"],
        )

    def test_upsert_preserves_nullable_fields(self):
        """yt_audio_playlist_id + yt_year are NULLable per migration 034."""
        self.db.upsert_youtube_album_mapping("rg-1", "mb", [
            self._row(
                yt_browse_id="MPREb_nulls",
                yt_audio_playlist_id=None,
                yt_year=None,
            ),
        ])

        got = self.db.get_youtube_album_mapping("rg-1", "mb")
        assert got is not None
        self.assertEqual(len(got), 1)
        self.assertIsNone(got[0]["yt_audio_playlist_id"])
        self.assertIsNone(got[0]["yt_year"])

    def test_upsert_with_empty_rows_clears_the_pair(self):
        """Passing an empty list deletes the pair's existing matrix."""
        self.db.upsert_youtube_album_mapping("rg-1", "mb", [
            self._row(yt_browse_id="MPREb_a"),
            self._row(yt_browse_id="MPREb_b"),
        ])

        self.db.upsert_youtube_album_mapping("rg-1", "mb", [])

        self.assertEqual(
            self.db.get_youtube_album_mapping("rg-1", "mb"), [])

    def test_find_mapping_for_release_matches_exact_distance(self):
        self.db.upsert_youtube_album_mapping("discogs-master-1", "discogs", [
            self._row(
                yt_browse_id="MPREb_discogs",
                distances=[
                    PersistedDistance(mbid="12345", distance=0.05),
                    PersistedDistance(mbid="67890", distance=0.25),
                ],
            )
        ])

        got = self.db.find_youtube_album_mapping_for_release(
            source="discogs",
            release_id="12345",
            browse_id="MPREb_discogs",
        )

        self.assertIsNotNone(got)
        assert got is not None
        self.assertEqual(got["release_group_identifier"], "discogs-master-1")
        self.assertEqual(got["source"], "discogs")
        self.assertIsNone(self.db.find_youtube_album_mapping_for_release(
            source="mb", release_id="12345", browse_id="MPREb_discogs"))
        self.assertIsNone(self.db.find_youtube_album_mapping_for_release(
            source="discogs", release_id="99999", browse_id="MPREb_discogs"))
        self.assertIsNone(self.db.find_youtube_album_mapping_for_release(
            source="discogs", release_id="12345", browse_id="MPREb_other"))

    def test_upsert_rolls_back_on_insert_failure(self):
        """If a row insert violates a constraint, the prior matrix survives."""
        self.db.upsert_youtube_album_mapping("rg-1", "mb", [
            self._row(yt_browse_id="MPREb_pre1"),
            self._row(yt_browse_id="MPREb_pre2"),
        ])

        # CHECK constraint forbids source != ('mb', 'discogs'). We can't
        # break source on the second call (the method parameter would have
        # to flow into INSERT), so trigger failure via duplicate
        # yt_browse_id within the same upsert payload — the UNIQUE
        # (release_group_identifier, source, yt_browse_id) constraint
        # rejects it.
        with self.assertRaises(psycopg2.errors.UniqueViolation):
            self.db.upsert_youtube_album_mapping("rg-1", "mb", [
                self._row(yt_browse_id="MPREb_dup"),
                self._row(yt_browse_id="MPREb_dup"),
            ])

        # Prior matrix must survive — rollback preserved it.
        got = self.db.get_youtube_album_mapping("rg-1", "mb")
        assert got is not None
        self.assertEqual(
            [r["yt_browse_id"] for r in got],
            ["MPREb_pre1", "MPREb_pre2"],
        )


@requires_postgres
class TestYoutubeIngestDownloadLog(unittest.TestCase):
    """Integration tests for YT-rescue ingest methods on download_log (U2).

    Exercises the real PostgreSQL CRUD against migration 037: source
    discriminator, ``youtube_metadata`` JSONB, partial unique index, and
    the widened ``download_log_outcome_check`` constraint. The Rule A
    round-trip test (``test_insert_youtube_running_round_trip_preserves_every_field``)
    is the load-bearing guard against a future field drifting between
    the Python payload and the INSERT column list.
    """

    def setUp(self) -> None:
        self.db = make_db()
        self.request_id = self.db.add_request(
            mb_release_id="yt-rescue-mbid-1",
            artist_name="Test Artist",
            album_title="Test Album",
            source="request",
        )

    def tearDown(self) -> None:
        self.db.close()

    def _yt_payload(self, **overrides: Any) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": self.request_id,
            "browse_id": "MPREb_default",
            "audio_playlist_id": "OLAK5uy_default",
            "yt_url": "https://music.youtube.com/playlist?list=OLAK5uy_default",
            "expected_track_count": 10,
        }
        payload.update(overrides)
        return payload

    def test_insert_youtube_running_round_trip_preserves_every_field(self):
        """Rule A: every key in the input dict round-trips through PG.

        If a future schema/method change drops a field from the INSERT
        column list (or from the JSONB blob the helper writes), the
        for-loop below names the offending key. This is the load-bearing
        guard per ``.claude/rules/test-fidelity.md`` § "Rule A".
        """
        payload = self._yt_payload(
            browse_id="MPREb_roundtrip",
            audio_playlist_id="OLAK5uy_roundtrip",
            yt_url="https://music.youtube.com/playlist?list=OLAK5uy_roundtrip",
            expected_track_count=12,
        )
        log_id = self.db.insert_youtube_running(**payload)
        entry = self.db.get_download_log_entry(log_id)
        assert entry is not None

        # Top-level row columns set by INSERT.
        self.assertEqual(entry["request_id"], payload["request_id"])
        self.assertEqual(entry["source"], "youtube")
        self.assertEqual(entry["outcome"], "youtube_running")

        # JSONB metadata: every supplied field round-trips through psycopg2.
        meta = cast(dict, entry["youtube_metadata"])
        self.assertIsInstance(meta, dict)
        for key, expected in {
            "yt_url": payload["yt_url"],
            "browse_id": payload["browse_id"],
            "audio_playlist_id": payload["audio_playlist_id"],
            "expected_track_count": payload["expected_track_count"],
        }.items():
            self.assertEqual(
                meta.get(key), expected,
                msg=f"field {key} was dropped at the PG boundary",
            )

    def test_insert_youtube_running_persists_resolver_audit_fields(self):
        log_id = self.db.insert_youtube_running(
            **self._yt_payload(),
            resolver_mapping_id=44,
            per_track_video_ids=["v1", "v2"],
        )

        entry = self.db.get_download_log_entry(log_id)
        assert entry is not None
        meta = cast(dict, entry["youtube_metadata"])
        self.assertEqual(meta["resolver_mapping_id"], 44)
        self.assertEqual(meta["per_track_video_ids"], ["v1", "v2"])

    def test_insert_youtube_running_raises_on_idempotency_violation(self):
        """Partial unique index serialises submissions per R4."""
        first_id = self.db.insert_youtube_running(**self._yt_payload())
        from lib.pipeline_db import YoutubeInFlightError
        with self.assertRaises(YoutubeInFlightError) as ctx:
            self.db.insert_youtube_running(**self._yt_payload(
                browse_id="MPREb_collide",
            ))
        # Existing id surfaced via the exception so the service can put
        # it in SubmitResult.detail.
        self.assertEqual(ctx.exception.existing_download_log_id, first_id)
        self.assertEqual(ctx.exception.request_id, self.request_id)

    def test_insert_after_terminal_succeeds(self):
        """Once a row goes terminal, the partial index admits the next.

        Confirms the WHERE clause on the partial index is keyed to
        ``outcome='youtube_running'`` — otherwise terminal rows would
        permanently block re-submission.
        """
        first_id = self.db.insert_youtube_running(**self._yt_payload())
        self.db.update_youtube_terminal(
            first_id, "youtube_failed", {"reason": "test_release"},
        )
        # Second submit MUST now succeed.
        second_id = self.db.insert_youtube_running(**self._yt_payload(
            browse_id="MPREb_after_terminal",
        ))
        self.assertNotEqual(first_id, second_id)

    def test_update_youtube_terminal_to_success_round_trip_preserves_metadata(self):
        log_id = self.db.insert_youtube_running(**self._yt_payload())
        terminal_meta = {
            "per_track_video_ids": ["v1", "v2", "v3"],
            "observed_track_count": 10,
            "expected_track_count": 10,
        }
        self.db.update_youtube_terminal(log_id, "youtube_success", terminal_meta)

        entry = self.db.get_download_log_entry(log_id)
        assert entry is not None
        self.assertEqual(entry["outcome"], "youtube_success")
        meta = cast(dict, entry["youtube_metadata"])
        self.assertIsInstance(meta, dict)
        # Merge: submission-time fields survive.
        self.assertEqual(meta["browse_id"], "MPREb_default")
        # Terminal-time fields are layered on top.
        self.assertEqual(meta["per_track_video_ids"], ["v1", "v2", "v3"])
        self.assertEqual(meta["observed_track_count"], 10)

    def test_update_youtube_terminal_to_failed_writes_metadata(self):
        log_id = self.db.insert_youtube_running(**self._yt_payload())
        terminal_meta = {
            "reason": "track_count_mismatch",
            "observed_track_count": 7,
            "expected_track_count": 10,
            "stderr_excerpt": "[ytdl] short play\n",
        }
        self.db.update_youtube_terminal(log_id, "youtube_failed", terminal_meta)

        entry = self.db.get_download_log_entry(log_id)
        assert entry is not None
        self.assertEqual(entry["outcome"], "youtube_failed")
        meta = cast(dict, entry["youtube_metadata"])
        self.assertIsInstance(meta, dict)
        self.assertEqual(meta["reason"], "track_count_mismatch")
        self.assertEqual(meta["observed_track_count"], 7)
        self.assertEqual(meta["stderr_excerpt"], "[ytdl] short play\n")

    def test_update_youtube_terminal_rejects_non_terminal_outcomes(self):
        log_id = self.db.insert_youtube_running(**self._yt_payload())
        for bogus in ("youtube_running", "success", "rejected", ""):
            with self.subTest(outcome=bogus), self.assertRaises(ValueError):
                self.db.update_youtube_terminal(log_id, bogus, {})

    def test_claim_next_youtube_pending_excludes_slskd_rows(self):
        """Source discriminator must filter slskd rows out of the worker queue."""
        # An slskd-side row for the same request.
        self.db.log_download(
            self.request_id, soulseek_username="alice", outcome="success",
        )
        yt_id = self.db.insert_youtube_running(**self._yt_payload())
        rows = self.db.claim_next_youtube_pending(worker_id="w", limit=10)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["id"], yt_id)
        self.assertEqual(rows[0]["source"], "youtube")
        self.assertEqual(rows[0]["outcome"], "youtube_running")

    def test_claim_next_youtube_pending_excludes_terminal_rows(self):
        log_id = self.db.insert_youtube_running(**self._yt_payload())
        # A terminal (never-claimed) row is not drainable.
        self.db.update_youtube_terminal(log_id, "youtube_success", {})
        self.assertEqual(
            self.db.claim_next_youtube_pending(worker_id="w", limit=10), [])

    def test_claim_next_youtube_pending_orders_by_created_at(self):
        """FIFO contract per R16: earliest created_at first."""
        # Distinct requests so the partial unique index permits multiple
        # in-flight rows.
        rid_b = self.db.add_request(
            mb_release_id="yt-rescue-mbid-b",
            artist_name="B Artist",
            album_title="B Album",
            source="request",
        )
        rid_c = self.db.add_request(
            mb_release_id="yt-rescue-mbid-c",
            artist_name="C Artist",
            album_title="C Album",
            source="request",
        )
        first = self.db.insert_youtube_running(**self._yt_payload(
            request_id=rid_b, browse_id="MPREb_b",
        ))
        second = self.db.insert_youtube_running(**self._yt_payload(
            request_id=rid_c, browse_id="MPREb_c",
        ))
        rows = self.db.claim_next_youtube_pending(worker_id="w", limit=10)
        self.assertEqual([r["id"] for r in rows], [first, second])

    def test_claim_next_youtube_pending_marks_worker_metadata(self):
        rid_b = self.db.add_request(
            mb_release_id="yt-rescue-mbid-b",
            artist_name="B Artist",
            album_title="B Album",
            source="request",
        )
        first = self.db.insert_youtube_running(**self._yt_payload())
        second = self.db.insert_youtube_running(**self._yt_payload(
            request_id=rid_b, browse_id="MPREb_b",
        ))
        claimed = self.db.claim_next_youtube_pending(
            worker_id="worker-1", limit=1)
        self.assertEqual([r["id"] for r in claimed], [first])
        # The unclaimed sibling is still drainable by the next claim.
        self.assertEqual(
            [r["id"] for r in self.db.claim_next_youtube_pending(
                worker_id="worker-2", limit=10)],
            [second],
        )
        meta = claimed[0]["youtube_metadata"]
        self.assertEqual(meta["worker_id"], "worker-1")
        self.assertIsNotNone(meta["worker_claimed_at"])

    def test_find_orphan_youtube_running_returns_claimed_ids(self):
        rid_b = self.db.add_request(
            mb_release_id="yt-rescue-mbid-b-claimed",
            artist_name="B Artist",
            album_title="B Album",
            source="request",
        )
        first = self.db.insert_youtube_running(**self._yt_payload())
        second = self.db.insert_youtube_running(**self._yt_payload(
            request_id=rid_b, browse_id="MPREb_b",
        ))
        self.assertEqual(self.db.find_orphan_youtube_running(), [])
        self.db.claim_next_youtube_pending(worker_id="worker-1", limit=1)
        orphans = self.db.find_orphan_youtube_running()
        self.assertEqual(orphans, [first])

        # Worker's startup sweep marks each failed; the orphan set
        # resolves to empty.
        for log_id in orphans:
            self.db.update_youtube_terminal(
                log_id, "youtube_failed", {"reason": "worker_interrupted"},
            )
        self.assertEqual(self.db.find_orphan_youtube_running(), [])
        # The surviving sibling is still drainable after the orphan sweep.
        self.assertEqual(
            [r["id"] for r in self.db.claim_next_youtube_pending(
                worker_id="worker-2", limit=10)],
            [second],
        )

    def test_active_youtube_import_guard_is_request_scoped(self):
        from lib.import_queue import (
            IMPORT_JOB_YOUTUBE,
            youtube_import_dedupe_key,
            youtube_import_payload,
        )

        job = self.db.enqueue_import_job(
            IMPORT_JOB_YOUTUBE,
            request_id=self.request_id,
            dedupe_key=youtube_import_dedupe_key(901),
            payload=youtube_import_payload(
                staged_path="/tmp/yt-a",
                request_id=self.request_id,
                browse_id="MPREb_a",
                download_log_id=901,
            ),
        )

        active = self.db.find_active_youtube_import_job(
            request_id=self.request_id,
            browse_id="MPREb_b",
        )
        assert active is not None
        self.assertEqual(active.id, job.id)

        with self.assertRaises(psycopg2.errors.UniqueViolation):
            self.db.enqueue_import_job(
                IMPORT_JOB_YOUTUBE,
                request_id=self.request_id,
                dedupe_key=youtube_import_dedupe_key(902),
                payload=youtube_import_payload(
                    staged_path="/tmp/yt-b",
                    request_id=self.request_id,
                    browse_id="MPREb_b",
                    download_log_id=902,
                ),
            )

    def test_atomic_youtube_import_enqueue_marks_download_log_success(self):
        from lib.import_queue import (
            youtube_import_dedupe_key,
            youtube_import_payload,
        )

        log_id = self.db.insert_youtube_running(**self._yt_payload())
        payload = youtube_import_payload(
            staged_path="/tmp/yt-staged",
            request_id=self.request_id,
            browse_id="MPREb_default",
            download_log_id=log_id,
        )

        job = self.db.enqueue_youtube_import_and_mark_success(
            download_log_id=log_id,
            request_id=self.request_id,
            dedupe_key=youtube_import_dedupe_key(log_id),
            payload=payload,
            message="yt handoff",
            terminal_metadata={"observed_track_count": 10},
        )

        self.assertEqual(job.request_id, self.request_id)
        entry = self.db.get_download_log_entry(log_id)
        assert entry is not None
        self.assertEqual(entry["outcome"], "youtube_success")
        self.assertEqual(
            cast(dict, entry["youtube_metadata"])["observed_track_count"], 10)

    def test_malformed_youtube_handoff_cannot_mutate_or_poison_dedupe(self):
        from lib.import_queue import (
            youtube_import_dedupe_key,
            youtube_import_payload,
        )

        log_id = self.db.insert_youtube_running(**self._yt_payload())
        dedupe = youtube_import_dedupe_key(log_id)
        valid = youtube_import_payload(
            staged_path="/tmp/yt-staged",
            request_id=self.request_id,
            browse_id="MPREb_default",
            download_log_id=log_id,
        )
        malformed = {**valid, "unexpected": True}

        with self.assertRaises(msgspec.ValidationError):
            self.db.enqueue_youtube_import_and_mark_success(
                download_log_id=log_id,
                request_id=self.request_id,
                dedupe_key=dedupe,
                payload=malformed,
                message="invalid yt handoff",
                terminal_metadata={"observed_track_count": 10},
            )

        self.assertFalse(any(
            job.dedupe_key == dedupe
            for job in self.db.list_import_jobs(limit=100)
        ))
        entry = self.db.get_download_log_entry(log_id)
        assert entry is not None
        self.assertEqual(entry["outcome"], "youtube_running")

        job = self.db.enqueue_youtube_import_and_mark_success(
            download_log_id=log_id,
            request_id=self.request_id,
            dedupe_key=dedupe,
            payload=valid,
            message="valid yt handoff",
            terminal_metadata={"observed_track_count": 10},
        )
        self.assertEqual(job.dedupe_key, dedupe)
        self.assertFalse(job.deduped)
        entry = self.db.get_download_log_entry(log_id)
        assert entry is not None
        self.assertEqual(entry["outcome"], "youtube_success")

    def test_read_seam_includes_source_and_youtube_metadata(self):
        """Every download_log read seam surfaces the new columns."""
        # An slskd row (source defaults to 'slskd', youtube_metadata=NULL).
        slskd_id = self.db.log_download(
            self.request_id, soulseek_username="alice", outcome="success",
        )
        yt_id = self.db.insert_youtube_running(**self._yt_payload())
        self.db.update_youtube_terminal(
            yt_id, "youtube_success",
            {"observed_track_count": 10, "expected_track_count": 10},
        )

        # get_download_log_entry
        slskd_entry = self.db.get_download_log_entry(slskd_id)
        assert slskd_entry is not None
        self.assertEqual(slskd_entry["source"], "slskd")
        self.assertIsNone(slskd_entry["youtube_metadata"])

        yt_entry = self.db.get_download_log_entry(yt_id)
        assert yt_entry is not None
        self.assertEqual(yt_entry["source"], "youtube")
        self.assertEqual(yt_entry["outcome"], "youtube_success")
        yt_meta = cast(dict, yt_entry["youtube_metadata"])
        self.assertIsInstance(yt_meta, dict)
        self.assertEqual(yt_meta["observed_track_count"], 10)

        # get_download_history
        history = self.db.get_download_history(self.request_id)
        self.assertEqual(len(history), 2)
        by_source = {r["source"]: r for r in history}
        self.assertEqual(set(by_source.keys()), {"slskd", "youtube"})
        self.assertIsNone(by_source["slskd"]["youtube_metadata"])
        self.assertIsInstance(by_source["youtube"]["youtube_metadata"], dict)

        # get_download_history_batch
        batch = self.db.get_download_history_batch([self.request_id])
        rows = batch[self.request_id]
        self.assertEqual({r["source"] for r in rows}, {"slskd", "youtube"})


def _terminate_backend(dsn, pid):
    """Kill a PostgreSQL backend from a *second* session so the next statement
    on the original connection dies mid-flight (``conn.closed`` flips truthy).

    This is the real "server closed the socket unexpectedly" failure mode the
    ``_execute`` reconnect branch and the ``_atomic`` rollback handler must
    survive — reproduced deterministically instead of via a fake socket.
    """
    killer = psycopg2.connect(dsn)
    killer.autocommit = True
    try:
        with killer.cursor() as cur:
            cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
            cur.fetchone()
    finally:
        killer.close()


@requires_postgres
class TestAtomicAndExecuteHardening(unittest.TestCase):
    """Issue #395 — error-path hardening for the shared transaction
    primitives in ``lib/pipeline_db/_core.py``, exercised against real PG.

    Item 1: ``_execute`` must NOT silently reconnect onto a fresh
    ``autocommit=True`` connection when it dies mid-statement *inside* a
    transaction (``autocommit=False``) — that would drop the in-flight
    transaction's partial writes. It must re-raise so ``_atomic`` rolls back.
    Outside a transaction the reconnect-and-retry heal must still fire.

    Item 2: when the connection is dead, both ``rollback()`` and the
    autocommit-restore in ``_atomic``'s ``finally`` raise a *secondary*
    ``InterfaceError``. Neither may mask the ORIGINAL exception the caller
    should see.
    """

    @staticmethod
    def _backend_pid(db):
        with db.conn.cursor() as cur:
            cur.execute("SELECT pg_backend_pid()")
            return cur.fetchone()[0]

    def test_execute_inside_transaction_reraises_instead_of_reconnecting(self):
        """Item 1 guard: a mid-statement socket death while ``autocommit=False``
        re-raises and leaves the connection object untouched — no silent swap
        to a fresh autocommit=True connection that would lose the transaction.
        """
        db = make_db()
        self.addCleanup(db.close)
        original_conn = db.conn
        # Simulate being inside `with self._atomic():` — explicit transaction.
        db.conn.autocommit = False
        pid = self._backend_pid(db)  # also opens the transaction
        _terminate_backend(db.dsn, pid)
        with self.assertRaises((psycopg2.OperationalError, psycopg2.InterfaceError)):
            db._execute("SELECT 1")
        # The guard held: _execute did NOT reconnect.
        self.assertIs(db.conn, original_conn)

    def test_execute_reconnects_outside_transaction(self):
        """Item 1 scope check: outside a transaction (``autocommit=True``) a
        dead socket must still heal via reconnect-and-retry. The guard is
        scoped to ``autocommit=False`` only and must not regress this — the
        live failure mode the reconnect branch exists for.
        """
        db = make_db()
        self.addCleanup(db.close)
        original_conn = db.conn
        self.assertTrue(db.conn.autocommit)
        pid = self._backend_pid(db)
        _terminate_backend(db.dsn, pid)
        cur = db._execute("SELECT 1 AS one")
        self.assertEqual(cur.fetchone()["one"], 1)
        self.assertIsNot(db.conn, original_conn)  # reconnected
        self.assertTrue(db.conn.autocommit)

    def test_atomic_rollback_failure_preserves_original_exception(self):
        """Item 2: a dead connection makes both ``rollback()`` and the
        autocommit-restore raise ``InterfaceError``. The ORIGINAL exception
        from the block body must still propagate.
        """
        db = make_db()
        self.addCleanup(db.close)

        class _Boom(Exception):
            pass

        with self.assertRaises(_Boom), db._atomic():
            # Kill the connection so BOTH rollback() (except handler) and
            # autocommit-restore (finally) raise a secondary InterfaceError.
            db.conn.close()
            raise _Boom("the real failure the operator must see")

    def test_atomic_commit_failure_propagates_commit_error(self):
        """Item 2: when the caller's ``commit()`` raises ``OperationalError``
        because the backend died, that commit error propagates — not a
        secondary ``InterfaceError`` from the guarded rollback / restore.
        (``InterfaceError`` is a sibling of ``OperationalError``, so a leak
        would fail this assertion.)
        """
        db = make_db()
        self.addCleanup(db.close)
        with self.assertRaises(psycopg2.OperationalError), db._atomic():
            pid = self._backend_pid(db)
            _terminate_backend(db.dsn, pid)
            db.conn.commit()  # backend gone -> raises OperationalError

    def test_atomic_happy_path_commits_and_restores_autocommit(self):
        """No behaviour change on the happy path: flip to ``autocommit=False``
        for the block, caller commits, autocommit is restored, write persists.
        """
        db = make_db()
        self.addCleanup(db.close)
        self.assertTrue(db.conn.autocommit)
        rid = db.add_request(
            artist_name="Atomic", album_title="Happy Path", source="request")
        with db._atomic():
            self.assertFalse(db.conn.autocommit)  # flipped for the block
            with db.conn.cursor() as cur:
                cur.execute(
                    "UPDATE album_requests SET reasoning = %s WHERE id = %s",
                    ("atomic-write", rid),
                )
            db.conn.commit()
        self.assertTrue(db.conn.autocommit)  # restored
        row = db.get_request(rid)
        assert row is not None
        self.assertEqual(row["reasoning"], "atomic-write")


@requires_postgres
class TestWrongMatchTriageRoundTrip(unittest.TestCase):
    """Real-PG round-trip for the typed triage write path (#410).

    Test-fidelity Rule A: the typed payload must survive the jsonb_set
    write and decode back identical through the one envelope decode site.
    """

    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="triage-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        self.log_id = self.db.log_download(
            request_id=self.req_id,
            soulseek_username="peer",
            outcome="rejected",
            validation_result=json.dumps({
                "failed_path": "/mnt/x/failed_imports/B",
                "scenario": "wrong_match",
            }),
        )

    def tearDown(self):
        self.db.close()

    def test_triage_audit_round_trips_every_field(self):
        from lib.validation_envelope import (
            WrongMatchTriageAudit,
            decode_validation_envelope,
        )

        audit = WrongMatchTriageAudit(
            action="deleted_reject",
            outcome="deleted",
            success=True,
            reason="confident_reject",
            preview_verdict="reject",
            preview_decision="rejected_spectral",
            cleanup_eligible=True,
            source_path="/mnt/x/failed_imports/B",
            stage_chain=["stage1_spectral", "stage2_import"],
            cleared_rows=2,
            deleted_path="/mnt/x/failed_imports/B",
            path_missing=False,
            error=None,
        )
        self.assertTrue(
            self.db.record_wrong_match_triage(self.log_id, audit))

        cur = self.db._execute(
            "SELECT validation_result FROM download_log WHERE id = %s",
            (self.log_id,))
        row = cur.fetchone()
        assert row is not None
        env = decode_validation_envelope(row["validation_result"])
        self.assertEqual(env.wrong_match_triage, audit)
        # jsonb_set must merge, not replace — the pre-existing keys survive.
        self.assertEqual(env.failed_path, "/mnt/x/failed_imports/B")
        self.assertEqual(env.scenario, "wrong_match")

    def test_clear_wrong_match_path_removes_only_the_failed_path_key(self):
        from lib.validation_envelope import decode_validation_envelope

        self.assertTrue(self.db.clear_wrong_match_path(self.log_id))
        entry = self.db.get_download_log_entry(self.log_id)
        assert entry is not None
        env = decode_validation_envelope(entry["validation_result"])
        self.assertIsNone(env.failed_path)
        self.assertEqual(env.scenario, "wrong_match")


@requires_postgres
class TestLatestDownloadSummaries(unittest.TestCase):
    """#426: ``get_latest_download_summaries`` returns only the newest
    download_log row + a history count per request, instead of dragging
    the full per-request history (with fat JSONB) over the wire."""

    def setUp(self):
        self.db = make_db()
        self.r1 = self.db.add_request(
            artist_name="A", album_title="One", source="request",
            mb_release_id="sum-1")
        self.r2 = self.db.add_request(
            artist_name="B", album_title="Two", source="request",
            mb_release_id="sum-2")
        self.r3 = self.db.add_request(
            artist_name="C", album_title="NoHistory", source="request",
            mb_release_id="sum-3")

    def tearDown(self):
        self.db.close()

    def test_latest_row_and_count_per_request(self):
        self.db.log_download(self.r1, "user_old", "flac", "/tmp/1",
                             outcome="rejected")
        self.db.log_download(self.r1, "user_mid", "flac", "/tmp/2",
                             outcome="rejected")
        self.db.log_download(self.r1, "user_new", "flac", "/tmp/3",
                             outcome="success",
                             validation_result=json.dumps({"valid": True}))
        self.db.log_download(self.r2, "solo", "mp3", "/tmp/4",
                             outcome="rejected")

        summaries = self.db.get_latest_download_summaries(
            [self.r1, self.r2, self.r3])

        self.assertEqual(set(summaries), {self.r1, self.r2})
        s1 = summaries[self.r1]
        self.assertEqual(s1["count"], 3)
        self.assertEqual(s1["latest"]["soulseek_username"], "user_new")
        self.assertEqual(s1["latest"]["outcome"], "success")
        # The latest row must carry everything the history classifier
        # consumes (JSONB included) — it feeds build_download_history_row.
        self.assertIn("validation_result", s1["latest"])
        self.assertIn("import_result", s1["latest"])
        self.assertEqual(summaries[self.r2]["count"], 1)

    def test_empty_input_returns_empty(self):
        self.assertEqual(self.db.get_latest_download_summaries([]), {})

    def test_latest_row_overlays_candidate_evidence(self):
        """The evidence overlay that get_download_history_batch applied
        must survive on the summary's latest row."""
        from lib.quality import AudioQualityMeasurement
        log_id = self.db.log_download(self.r1, "u", "flac", "/tmp/x",
                                      outcome="rejected")
        evidence = make_album_quality_evidence(
            mb_release_id="sum-1",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=900,
                avg_bitrate_kbps=950,
                median_bitrate_kbps=940,
                format="flac",
                spectral_grade="genuine",
                spectral_bitrate_kbps=998,
            ),
            storage_format="FLAC",
        )
        self.db.upsert_album_quality_evidence(evidence)
        stored = self.db.find_album_quality_evidence(
            mb_release_id="sum-1",
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.db.set_download_log_candidate_evidence(log_id, stored.id)

        summaries = self.db.get_latest_download_summaries([self.r1])
        latest = summaries[self.r1]["latest"]
        self.assertEqual(latest["spectral_grade"], "genuine")
        self.assertEqual(latest["spectral_bitrate"], 998)


@requires_postgres
class TestSearchRequests(unittest.TestCase):
    """#426: operator search over artist/album across all statuses."""

    def setUp(self):
        self.db = make_db()
        self.db.add_request(
            artist_name="The Mountain Goats", album_title="Tallahassee",
            source="request", mb_release_id="sr-1", status="imported")
        self.db.add_request(
            artist_name="Goat", album_title="World Music",
            source="request", mb_release_id="sr-2", status="wanted")
        self.db.add_request(
            artist_name="100% Wool", album_title="Felt",
            source="request", mb_release_id="sr-3", status="unsearchable")

    def tearDown(self):
        self.db.close()

    def test_matches_artist_case_insensitive(self):
        rows = self.db.search_requests("mountain")
        self.assertEqual([r["mb_release_id"] for r in rows], ["sr-1"])

    def test_matches_album_title(self):
        rows = self.db.search_requests("world mus")
        self.assertEqual([r["mb_release_id"] for r in rows], ["sr-2"])

    def test_matches_across_statuses(self):
        rows = self.db.search_requests("goat")
        self.assertEqual(
            {r["mb_release_id"] for r in rows}, {"sr-1", "sr-2"})

    def test_like_wildcards_are_escaped(self):
        rows = self.db.search_requests("100%")
        self.assertEqual([r["mb_release_id"] for r in rows], ["sr-3"])

    def test_status_narrowing_happens_in_sql(self):
        rows = self.db.search_requests("goat", status="wanted")
        self.assertEqual([r["mb_release_id"] for r in rows], ["sr-2"])

    def test_limit_and_blank_query(self):
        self.assertEqual(self.db.search_requests("  "), [])
        rows = self.db.search_requests("o", limit=2)
        self.assertEqual(len(rows), 2)


@requires_postgres
class TestGetByStatusRecentWindow(unittest.TestCase):
    """#426: the imported list is served newest-first with a cap."""

    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_newest_first_with_limit(self):
        ids = []
        for i in range(3):
            ids.append(self.db.add_request(
                artist_name=f"A{i}", album_title=f"T{i}",
                source="request", mb_release_id=f"recent-{i}",
                status="imported"))
        # Touch the oldest row so updated_at ordering (not insert order)
        # decides recency.
        self.db.update_request_fields(ids[0], reasoning="touched")

        rows = self.db.get_by_status("imported", limit=2, newest_first=True)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["id"], ids[0])

    def test_default_shape_unchanged(self):
        self.db.add_request(
            artist_name="A", album_title="T", source="request",
            mb_release_id="legacy-order", status="wanted")
        rows = self.db.get_by_status("wanted")
        self.assertEqual(len(rows), 1)


@requires_postgres
class TestDashboardFakeParity(unittest.TestCase):
    """Structural parity gate: FakePipelineDB's dashboard mirror vs the
    real PostgreSQL read-model on identically-seeded telemetry.

    The fake's get_pipeline_dashboard_metrics is a ~300-line Python
    mirror of ~700 lines of SQL; this test makes drift mechanical
    instead of review-archaeological. Both sides are seeded through the
    SAME writer calls, then the payloads are compared as SHAPES —
    recursive dict key sets, list lengths, first-element shapes, and
    leaf type categories (null / bool / num / str). Values are not
    compared (timestamps and rates are time-anchored); a key rename, a
    dropped panel, a sparse-vs-dense series, or a None-vs-0 leaf all
    fail loudly.
    """

    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    @staticmethod
    def _seed(db: Any) -> None:
        rid = db.add_request(
            "Parity Artist", "Parity Album", "request",
            mb_release_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        )
        db.log_search(
            rid, query="found q", outcome="found", result_count=5,
            elapsed_s=2.0, variant="v1", final_state="Completed",
            browse_time_s=42.0, match_time_s=1.0, peers_browsed=110,
            peers_browsed_lazy=5, fanout_waves=6,
        )
        db.log_search(rid, query="loop", outcome="no_match", elapsed_s=1.0)
        db.log_search(rid, query="old style", outcome="exhausted")
        db.record_cycle_metrics(
            cycle_total_s=300.0,
            counters=CycleCounters(
                browse_time_s=20.0, match_time_s=10.0, search_time_s=240.0,
                peers_browsed=8, fanout_waves=2,
                find_download_queued=4, find_download_completed=4,
            ),
            wanted_total=10,
        )
        db.record_peer_observations(["peer-a", "peer-b"])
        db.record_unfindable_run_metrics(
            cohort_total=10, due_backlog_at_start=5,
            batch_limit=5, candidates_processed=5, probes_attempted=5,
            breaker_tripped=False, duration_seconds=12.5,
            categorised_count=1, no_change_count=4,
        )

    @classmethod
    def _shape(cls, value: Any) -> Any:
        if isinstance(value, dict):
            return {k: cls._shape(v) for k, v in sorted(value.items())}
        if isinstance(value, list):
            head = cls._shape(value[0]) if value else None
            return ("list", len(value), head)
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "bool"
        if isinstance(value, (int, float)):
            return "num"
        if isinstance(value, str):
            return "str"
        return type(value).__name__

    def test_fake_dashboard_shape_matches_real_pg(self):
        from tests.fakes import FakePipelineDB
        self._seed(self.db)
        fake = FakePipelineDB()
        self._seed(fake)

        real_shape = self._shape(self.db.get_pipeline_dashboard_metrics())
        fake_shape = self._shape(fake.get_pipeline_dashboard_metrics())

        self.assertEqual(
            real_shape, fake_shape,
            "FakePipelineDB's dashboard mirror drifted from the real "
            "PostgreSQL read-model — fix the fake (tests/fakes.py), "
            "never the production SQL, unless the SQL change is the "
            "point of your PR.",
        )

    #: (completed_at offset from the anchor, cycle_total_s). Row 0 and row
    #: 1 tie on completed_at; rows 2 and 3 tie on cycle_total_s. Both
    #: production ORDER BYs break their tie on ``id DESC``, so each pair
    #: has a decided winner neither insertion order nor luck can supply.
    _CYCLE_TIE_SPECS: ClassVar[list[tuple[int, float]]] = [
        (1, 25.0),
        (1, 10.0),
        (2, 100.0),
        (3, 100.0),
    ]

    @classmethod
    def _seed_cycle_ties(cls, db, anchor: datetime) -> list[int]:
        """Seed the tied cycle world on whichever backend is handed in."""
        return [
            db.record_cycle_metrics(
                completed_at=anchor - timedelta(hours=hours_ago),
                cycle_total_s=total_s,
            )
            for hours_ago, total_s in cls._CYCLE_TIE_SPECS
        ]

    @staticmethod
    def _seed_order(panel_rows, seed_ids: list[int]) -> list[int]:
        """Panel order expressed as seeding positions, not raw ids.

        The two backends mint different ids (PG sequences never reset
        between tests; the fake counts from 1), so the ids themselves are
        incomparable while their ORDER is exactly the contract.
        """
        return [seed_ids.index(int(row["id"])) for row in panel_rows]

    def test_dashboard_cycle_ties_follow_production_order(self):
        """Tie-breaking on the cycle panels, measured against real PG.

        This used to construct a ``FakePipelineDB`` alone and assert a
        hardcoded "production order" it never measured — a fake-only test
        inside a parity class, which could only ever confirm the fake
        agreed with itself. It now seeds the SAME tied worlds into both
        backends and requires the two orderings to agree, plus the
        absolute order production's ``ORDER BY`` implies, so agreement by
        construction cannot hollow it out.
        """
        from tests.fakes import FakePipelineDB

        anchor = datetime.now(UTC)
        fake = FakePipelineDB()
        real_ids = self._seed_cycle_ties(self.db, anchor)
        fake_ids = self._seed_cycle_ties(fake, anchor)

        real_cycles = self.db.get_pipeline_dashboard_metrics()["cycles"]
        fake_cycles = fake.get_pipeline_dashboard_metrics()["cycles"]

        real_recent = self._seed_order(real_cycles["recent"], real_ids)
        fake_recent = self._seed_order(fake_cycles["recent"], fake_ids)
        real_outliers = self._seed_order(real_cycles["outliers"], real_ids)
        fake_outliers = self._seed_order(fake_cycles["outliers"], fake_ids)

        self.assertEqual(
            real_recent, fake_recent,
            "recent-cycles order drifted between real PG and the fake")
        self.assertEqual(
            real_outliers, fake_outliers,
            "outlier-cycles order drifted between real PG and the fake")

        # Absolute: recent is (created_at DESC, id DESC) — the later of the
        # two 1h-old rows first; outliers is (cycle_total_s DESC, id DESC)
        # — the later of the two 100s rows first.
        self.assertEqual(real_recent, [1, 0, 2, 3])
        self.assertEqual(real_outliers, [3, 2, 0, 1])


@requires_postgres
class TestGetDownloadLogCounts(unittest.TestCase):
    """#445 item 2 — the /api/pipeline/log counts aggregate, promoted
    from inline route SQL to a named PipelineDB method."""

    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_empty_tables_yield_zero_counts(self):
        counts = self.db.get_download_log_counts()
        self.assertEqual(
            (counts.total, counts.imported,
             counts.matches_24h, counts.matches_6h),
            (0, 0, 0, 0))

    def test_counts_aggregate_downloads_and_found_searches(self):
        rid = self.db.add_request(
            mb_release_id="counts-mbid-1", artist_name="A",
            album_title="B", source="request")
        self.db.log_download(rid, outcome="success")
        self.db.log_download(rid, outcome="force_import")
        self.db.log_download(rid, outcome="rejected")
        self.db.log_search(rid, outcome="found")
        self.db.log_search(rid, outcome="found")
        self.db.log_search(rid, outcome="error")
        # Age one found-row out of the 6h window but not the 24h one.
        self.db._execute(
            "UPDATE search_log SET created_at = NOW() - INTERVAL '12 hours' "
            "WHERE id = (SELECT MIN(id) FROM search_log "
            "            WHERE outcome = 'found')")
        # And one found-row out of BOTH windows.
        self.db.log_search(rid, outcome="found")
        self.db._execute(
            "UPDATE search_log SET created_at = NOW() - INTERVAL '2 days' "
            "WHERE id = (SELECT MAX(id) FROM search_log)")

        counts = self.db.get_download_log_counts()
        self.assertEqual(counts.total, 3)
        self.assertEqual(counts.imported, 2)
        self.assertEqual(counts.matches_24h, 2)
        self.assertEqual(counts.matches_6h, 1)

    def test_fake_parity_on_identical_state(self):
        from tests.fakes import FakePipelineDB

        fake = FakePipelineDB()
        for db in (self.db, fake):
            rid = db.add_request(
                mb_release_id="parity-mbid-1", artist_name="A",
                album_title="B", source="request")
            db.log_download(rid, outcome="success")
            db.log_download(rid, outcome="timeout")
            db.log_search(rid, outcome="found")
            db.log_search(rid, outcome="no_results")
        real = self.db.get_download_log_counts()
        mirrored = fake.get_download_log_counts()
        self.assertEqual(
            (real.total, real.imported, real.matches_24h, real.matches_6h),
            (mirrored.total, mirrored.imported,
             mirrored.matches_24h, mirrored.matches_6h),
            "FakePipelineDB's counts mirror drifted from the real SQL — "
            "fix the fake (tests/fakes.py), never the production SQL, "
            "unless the SQL change is the point of your PR.")


@requires_postgres
class TestGetPipelineOverlay(unittest.TestCase):
    """#445 item 2 — web/overlay.py::check_pipeline's inline SQL,
    promoted to a named PipelineDB method."""

    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_empty_mbids_short_circuits(self):
        self.assertEqual(self.db.get_pipeline_overlay([]), {})

    def test_maps_known_mbids_with_overlay_fields(self):
        rid = self.db.add_request(
            mb_release_id="overlay-mbid-1", artist_name="A",
            album_title="B", source="request")
        self.db.update_request_fields(
            rid, min_bitrate=900, search_filetype_override="lossless")

        info = self.db.get_pipeline_overlay(
            ["overlay-mbid-1", "overlay-mbid-unknown"])

        self.assertEqual(set(info), {"overlay-mbid-1"})
        row = info["overlay-mbid-1"]
        self.assertEqual(row["id"], rid)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], "lossless")
        self.assertIsNone(row["target_format"])
        self.assertEqual(row["min_bitrate"], 900)

    def test_matches_and_keys_exact_mb_and_discogs_release_identities(self):
        fake = FakePipelineDB()
        mbid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        discogs_id = "456789"
        request_ids: dict[int, tuple[int, int]] = {}
        for db in (self.db, fake):
            mb_request_id = db.add_request(
                mb_release_id=mbid,
                artist_name="Identity Artist",
                album_title="MusicBrainz pressing",
                source="request",
            )
            discogs_request_id = db.add_request(
                mb_release_id=None,
                discogs_release_id=discogs_id,
                artist_name="Identity Artist",
                album_title="Discogs pressing",
                source="request",
            )
            request_ids[id(db)] = (mb_request_id, discogs_request_id)

        requested = [mbid, discogs_id, discogs_id]
        real = self.db.get_pipeline_overlay(requested)
        mirrored = fake.get_pipeline_overlay(requested)

        self.assertEqual(set(real), {mbid, discogs_id})
        self.assertEqual(set(mirrored), {mbid, discogs_id})
        self.assertEqual(real[mbid]["id"], request_ids[id(self.db)][0])
        self.assertEqual(real[discogs_id]["id"], request_ids[id(self.db)][1])
        self.assertEqual(mirrored[mbid]["id"], request_ids[id(fake)][0])
        self.assertEqual(mirrored[discogs_id]["id"], request_ids[id(fake)][1])

        strip_id = lambda rows: {
            release_id: {
                key: value for key, value in row.items() if key != "id"
            }
            for release_id, row in rows.items()
        }
        self.assertEqual(strip_id(real), strip_id(mirrored))

    def test_numeric_overlay_supports_legacy_layout_and_prefers_dedicated_column(self):
        fake = FakePipelineDB()
        ids_by_backend: dict[int, tuple[int, int, int]] = {}
        for db in (self.db, fake):
            legacy_only = db.add_request(
                mb_release_id="456781",
                artist_name="Legacy Discogs Artist",
                album_title="Legacy only",
                source="request",
            )
            legacy_collision = db.add_request(
                mb_release_id="456782",
                artist_name="Legacy Discogs Artist",
                album_title="Legacy collision",
                source="request",
            )
            dedicated = db.add_request(
                mb_release_id=None,
                discogs_release_id="456782",
                artist_name="Modern Discogs Artist",
                album_title="Dedicated column",
                source="request",
            )
            ids_by_backend[id(db)] = (
                legacy_only,
                legacy_collision,
                dedicated,
            )

        for db in (self.db, fake):
            with self.subTest(backend=type(db).__name__):
                rows = db.get_pipeline_overlay(["456781", "456782"])
                expected = ids_by_backend[id(db)]
                self.assertEqual(rows["456781"]["id"], expected[0])
                self.assertNotEqual(rows["456782"]["id"], expected[1])
                self.assertEqual(rows["456782"]["id"], expected[2])

    @staticmethod
    def _seed_import_job_history(
        db: PipelineDB | FakePipelineDB,
        *,
        request_id: int,
        job_type: str,
        status: str,
    ) -> None:
        if isinstance(db, FakePipelineDB):
            job = db._append_import_job(
                "automation_import",
                request_id=request_id,
                dedupe_key=None,
                payload={},
                message=None,
            )
            row = next(
                candidate
                for candidate in db._import_jobs
                if candidate["id"] == job.id
            )
            row["job_type"] = job_type
            row["status"] = status
            return
        db._execute(
            """
            INSERT INTO import_jobs (job_type, status, request_id, payload)
            VALUES (%s, %s, %s, '{}'::jsonb)
            """,
            (job_type, status, request_id),
        )

    def _seed_capture_history_world(
        self, db: PipelineDB | FakePipelineDB,
    ) -> list[str]:
        cases = [
            ("legacy-imported", "imported", None, None, True),
            ("download-success", "wanted", "success", None, True),
            ("download-force", "wanted", "force_import", None, True),
            # download_log.outcome='manual_import' stays live (7 real audit
            # rows) — unaffected by migration 080, which only retires the
            # *job_type* value 'manual_import' (a different taxonomy, a
            # different column). There is no "job-manual" case any more:
            # import_jobs.job_type='manual_import' is no longer a value the
            # CHECK constraint (or _CAPTURE_AND_EVIDENCE_SELECT) admits.
            ("download-manual", "wanted", "manual_import", None, True),
            ("job-automation", "wanted", None, ("automation_import", "completed"), True),
            ("job-force", "wanted", None, ("force_import", "completed"), True),
            ("job-youtube", "wanted", None, ("youtube_import", "completed"), True),
            # Issue #1176 PR1 round 2 (product decision, not deferred to
            # PR3): a successful local import genuinely is a capture — the
            # album was acquired and installed — so it confers
            # has_captured_history exactly as force_import/youtube_import do.
            ("job-local", "wanted", None, ("local_import", "completed"), True),
            ("no-witness", "wanted", None, None, False),
            ("download-rejected", "wanted", "rejected", None, False),
            ("download-youtube", "wanted", "youtube_success", None, False),
            ("job-failed", "wanted", None, ("force_import", "failed"), False),
            ("job-queued", "wanted", None, ("youtube_import", "queued"), False),
        ]
        mbids: list[str] = []
        for suffix, status, download_outcome, job, _expected in cases:
            mbid = f"capture-{suffix}"
            request_id = db.add_request(
                mb_release_id=mbid,
                artist_name="Capture Matrix Artist",
                album_title=suffix,
                source="request",
                status=status,
            )
            if download_outcome is not None:
                db.log_download(request_id, outcome=download_outcome)
            if job is not None:
                self._seed_import_job_history(
                    db,
                    request_id=request_id,
                    job_type=job[0],
                    status=job[1],
                )
            mbids.append(mbid)
        return mbids

    def test_capture_history_witness_truth_table(self):
        mbids = self._seed_capture_history_world(self.db)

        overlay = self.db.get_pipeline_overlay(mbids)

        expected = {
            "capture-legacy-imported": True,
            "capture-download-success": True,
            "capture-download-force": True,
            "capture-download-manual": True,
            "capture-job-automation": True,
            "capture-job-force": True,
            "capture-job-youtube": True,
            "capture-job-local": True,
            "capture-no-witness": False,
            "capture-download-rejected": False,
            "capture-download-youtube": False,
            "capture-job-failed": False,
            "capture-job-queued": False,
        }
        self.assertEqual(
            {mbid: row["has_captured_history"] for mbid, row in overlay.items()},
            expected,
        )

    def test_status_only_fallback_expires_on_reopen_but_witness_survives(self):
        fallback_id = self.db.add_request(
            mb_release_id="capture-reopen-fallback",
            artist_name="Capture Reopen Artist",
            album_title="Status only",
            source="request",
            status="imported",
        )
        witnessed_id = self.db.add_request(
            mb_release_id="capture-reopen-witnessed",
            artist_name="Capture Reopen Artist",
            album_title="Witnessed",
            source="request",
            status="imported",
        )
        self.db.log_download(witnessed_id, outcome="success")

        before = self.db.get_pipeline_overlay([
            "capture-reopen-fallback", "capture-reopen-witnessed",
        ])
        self.assertTrue(before["capture-reopen-fallback"]["has_captured_history"])
        self.assertTrue(before["capture-reopen-witnessed"]["has_captured_history"])

        self.assertTrue(self.db.update_status(
            fallback_id, "wanted", expected_status="imported"))
        self.assertTrue(self.db.update_status(
            witnessed_id, "wanted", expected_status="imported"))
        after = self.db.get_pipeline_overlay([
            "capture-reopen-fallback", "capture-reopen-witnessed",
        ])

        self.assertFalse(after["capture-reopen-fallback"]["has_captured_history"])
        self.assertTrue(after["capture-reopen-witnessed"]["has_captured_history"])

    def test_capture_and_evidence_fake_parity_on_identical_state(self):
        fake = FakePipelineDB()
        real_mbids = self._seed_capture_history_world(self.db)
        fake_mbids = self._seed_capture_history_world(fake)
        self.assertEqual(real_mbids, fake_mbids)

        def facts(
            rows: Mapping[str, Mapping[str, object]],
        ) -> dict[str, tuple[object, object, object]]:
            return {
                mbid: (
                    row["has_captured_history"],
                    row["verified_lossless"],
                    row["provisional_lossless"],
                )
                for mbid, row in rows.items()
            }

        self.assertEqual(
            facts(self.db.get_pipeline_overlay(real_mbids)),
            facts(fake.get_pipeline_overlay(fake_mbids)),
        )

    def _seed_identity_state(self, db) -> None:
        """Seed one verified, one provisional, one plain request."""
        from lib.quality import AlbumQualityV0Metric, VerifiedLosslessProof
        from tests.evidence_helpers import make_album_quality_evidence

        def link_evidence(mbid: str, proof) -> None:
            rid = db.add_request(
                mb_release_id=mbid, artist_name="A",
                album_title="B", source="request")
            evidence = make_album_quality_evidence(
                mb_release_id=mbid,
                source_path=f"/library/{mbid}",
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=245,
                    avg_bitrate_kbps=256,
                    median_bitrate_kbps=252,
                    format="MP3",
                ),
                v0_metric=AlbumQualityV0Metric(
                    subject="source", provenance="carried",
                    avg_bitrate_kbps=251, min_bitrate_kbps=228,
                ),
                verified_lossless_proof=proof,
            )
            db.upsert_album_quality_evidence(evidence)
            stored = db.find_album_quality_evidence(
                mb_release_id=mbid,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert stored is not None and stored.id is not None
            db.set_request_current_evidence(rid, stored.id)

        link_evidence(
            "overlay-idn-verified",
            VerifiedLosslessProof(
                provenance="carried", source="flac",
                classifier="spectral_verified_lossless",
            ),
        )
        link_evidence("overlay-idn-provisional", None)
        db.add_request(
            mb_release_id="overlay-idn-plain", artist_name="E",
            album_title="F", source="request")

    def test_overlay_carries_quality_identity(self):
        """The badge overlay derives verified/provisional from the linked
        current evidence — the persistent UI identity the provisional
        engineering lost (2026-07-18)."""
        self._seed_identity_state(self.db)

        info = self.db.get_pipeline_overlay([
            "overlay-idn-verified", "overlay-idn-provisional",
            "overlay-idn-plain",
        ])

        self.assertTrue(info["overlay-idn-verified"]["verified_lossless"])
        self.assertFalse(info["overlay-idn-verified"]["provisional_lossless"])
        self.assertFalse(info["overlay-idn-provisional"]["verified_lossless"])
        self.assertTrue(info["overlay-idn-provisional"]["provisional_lossless"])
        self.assertFalse(info["overlay-idn-plain"]["verified_lossless"])
        self.assertFalse(info["overlay-idn-plain"]["provisional_lossless"])

    def test_identity_fake_parity(self):
        from tests.fakes import FakePipelineDB

        fake = FakePipelineDB()
        self._seed_identity_state(self.db)
        self._seed_identity_state(fake)
        mbids = [
            "overlay-idn-verified", "overlay-idn-provisional",
            "overlay-idn-plain",
        ]
        strip = lambda o: {m: {k: v for k, v in row.items() if k != "id"}
                           for m, row in o.items()}
        self.assertEqual(
            strip(self.db.get_pipeline_overlay(mbids)),
            strip(fake.get_pipeline_overlay(mbids)),
            "FakePipelineDB's overlay identity mirror drifted from the "
            "real SQL — fix the fake, never the production SQL, unless "
            "the SQL change is the point of your PR.")

    def test_overlay_rejects_foreign_modern_discogs_current_evidence(self):
        fake = FakePipelineDB()
        release_ids = _seed_foreign_current_evidence_world(
            self.db,
            identity_layout="modern_discogs",
        )
        self.assertEqual(
            _seed_foreign_current_evidence_world(
                fake,
                identity_layout="modern_discogs",
            ),
            release_ids,
        )

        real = self.db.get_pipeline_overlay(release_ids)
        mirrored = fake.get_pipeline_overlay(release_ids)

        self.assertTrue(real[release_ids[0]]["verified_lossless"])
        self.assertFalse(real[release_ids[0]]["provisional_lossless"])
        self.assertFalse(real[release_ids[1]]["verified_lossless"])
        self.assertFalse(real[release_ids[1]]["provisional_lossless"])
        self.assertFalse(real[release_ids[2]]["verified_lossless"])
        self.assertFalse(real[release_ids[2]]["provisional_lossless"])
        strip_ids = lambda rows: {
            key: {field: value for field, value in row.items() if field != "id"}
            for key, row in rows.items()
        }
        self.assertEqual(strip_ids(real), strip_ids(mirrored))

    def test_fake_parity_on_identical_state(self):
        from tests.fakes import FakePipelineDB

        fake = FakePipelineDB()
        rids: dict[int, int] = {}
        for db in (self.db, fake):
            rid = db.add_request(
                mb_release_id="overlay-parity-1", artist_name="A",
                album_title="B", source="request")
            db.update_request_fields(rid, min_bitrate=320)
            db.add_request(
                mb_release_id="overlay-parity-2", artist_name="C",
                album_title="D", source="request", status="unsearchable")
            rids[id(db)] = rid
        mbids = ["overlay-parity-1", "overlay-parity-2", "nope"]
        real = self.db.get_pipeline_overlay(mbids)
        mirrored = fake.get_pipeline_overlay(mbids)
        # The PG sequence isn't reset between tests, so ids differ by
        # backend — pin each backend's id mapping, compare the rest.
        self.assertEqual(real["overlay-parity-1"]["id"], rids[id(self.db)])
        self.assertEqual(mirrored["overlay-parity-1"]["id"], rids[id(fake)])
        strip = lambda o: {m: {k: v for k, v in row.items() if k != "id"}
                           for m, row in o.items()}
        self.assertEqual(
            strip(real), strip(mirrored),
            "FakePipelineDB's overlay mirror drifted from the real SQL — "
            "fix the fake (tests/fakes.py), never the production SQL, "
            "unless the SQL change is the point of your PR.")


@requires_postgres
class TestListLibraryRequestCandidates(unittest.TestCase):
    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def _seed_cardinality_world(self, db: PipelineDB | FakePipelineDB) -> None:
        db.add_request(
            discogs_release_id="456789",
            artist_name="Candidate Artist",
            album_title="Modern one",
            source="request",
        )
        db.add_request(
            discogs_release_id="456789",
            artist_name="Candidate Artist",
            album_title="Modern two",
            source="request",
        )
        db.add_request(
            mb_release_id="456789",
            artist_name="Candidate Artist",
            album_title="Legacy",
            source="request",
        )
        db.add_request(
            mb_release_id="not-a-release-id",
            discogs_release_id="456789",
            artist_name="Candidate Artist",
            album_title="Conflicting",
            source="request",
        )

    def test_returns_every_strict_modern_and_legacy_discogs_candidate(self):
        self._seed_cardinality_world(self.db)

        rows = self.db.list_library_request_candidates(["456789"])

        self.assertEqual(
            [row["album_title"] for row in rows],
            ["Modern one", "Modern two", "Legacy"],
        )
        self.assertEqual(len({row["id"] for row in rows}), 3)

    def test_fake_parity_preserves_candidate_cardinality_and_projection(self):
        fake = FakePipelineDB()
        self._seed_cardinality_world(self.db)
        self._seed_cardinality_world(fake)

        def facts(
            rows: Sequence[Mapping[str, object]],
        ) -> list[tuple[object, ...]]:
            return [
                (
                    row["album_title"],
                    row["mb_release_id"],
                    row["discogs_release_id"],
                    row["has_captured_history"],
                    row["verified_lossless"],
                    row["provisional_lossless"],
                )
                for row in rows
            ]

        self.assertEqual(
            facts(self.db.list_library_request_candidates(["456789"])),
            facts(fake.list_library_request_candidates(["456789"])),
        )

    def test_candidates_reject_foreign_legacy_discogs_current_evidence(self):
        fake = FakePipelineDB()
        release_ids = _seed_foreign_current_evidence_world(
            self.db,
            identity_layout="legacy_discogs",
        )
        self.assertEqual(
            _seed_foreign_current_evidence_world(
                fake,
                identity_layout="legacy_discogs",
            ),
            release_ids,
        )

        def facts(
            db: PipelineDB | FakePipelineDB,
        ) -> dict[str, tuple[bool | None, bool]]:
            return {
                str(row["album_title"]): (
                    row["verified_lossless"],
                    row["provisional_lossless"],
                )
                for row in db.list_library_request_candidates(release_ids)
            }

        real = facts(self.db)
        mirrored = facts(fake)
        self.assertTrue(real["Exact evidence"][0])
        self.assertEqual(real["Foreign evidence"], (False, False))
        self.assertEqual(real["Foreign provisional"], (False, False))
        self.assertEqual(real, mirrored)


@requires_postgres
class TestListRequestsByArtistProjection(unittest.TestCase):
    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def _link_evidence(
        self,
        db: PipelineDB | FakePipelineDB,
        request_id: int,
        mbid: str,
        *,
        verified: bool,
    ) -> None:
        from lib.quality import AlbumQualityV0Metric, VerifiedLosslessProof

        evidence = make_album_quality_evidence(
            mb_release_id=mbid,
            source_path=f"/library/{mbid}",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=245,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=252,
                format="MP3",
            ),
            v0_metric=(
                None if verified else AlbumQualityV0Metric(
                    subject="source",
                    provenance="carried",
                    avg_bitrate_kbps=251,
                    min_bitrate_kbps=228,
                )
            ),
            verified_lossless_proof=(
                VerifiedLosslessProof(
                    provenance="carried",
                    source="flac",
                    classifier="spectral_verified_lossless",
                )
                if verified else None
            ),
        )
        db.upsert_album_quality_evidence(evidence)
        stored = db.find_album_quality_evidence(
            mb_release_id=mbid,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        db.set_request_current_evidence(request_id, stored.id)

    def _seed_artist_projection_world(
        self, db: PipelineDB | FakePipelineDB,
    ) -> None:
        verified_id = db.add_request(
            mb_release_id="artist-projection-verified",
            artist_name="Projection Artist",
            album_title="Verified",
            source="request",
            status="wanted",
        )
        db.log_download(verified_id, outcome="success")
        self._link_evidence(
            db, verified_id, "artist-projection-verified", verified=True)

        provisional_id = db.add_request(
            discogs_release_id="456789",
            artist_name="Projection Artist",
            album_title="Provisional",
            source="request",
            status="imported",
        )
        self._link_evidence(
            db, provisional_id, "456789", verified=False)

        evidence_only_id = db.add_request(
            mb_release_id="artist-projection-evidence-only",
            artist_name="Projection Artist",
            album_title="Evidence only",
            source="request",
            status="wanted",
        )
        self._link_evidence(
            db,
            evidence_only_id,
            "artist-projection-evidence-only",
            verified=True,
        )

    def test_projects_capture_and_linked_evidence_without_duplicate_requests(self):
        self._seed_artist_projection_world(self.db)

        rows = self.db.list_requests_by_artist("Projection Artist")
        by_title = {row["album_title"]: row for row in rows}

        self.assertEqual(len(rows), 3)
        self.assertEqual(len({row["id"] for row in rows}), 3)
        self.assertTrue(by_title["Verified"]["has_captured_history"])
        self.assertTrue(by_title["Verified"]["verified_lossless"])
        self.assertFalse(by_title["Verified"]["provisional_lossless"])
        self.assertEqual(by_title["Provisional"]["discogs_release_id"], "456789")
        self.assertTrue(by_title["Provisional"]["has_captured_history"])
        self.assertFalse(by_title["Provisional"]["verified_lossless"])
        self.assertTrue(by_title["Provisional"]["provisional_lossless"])
        self.assertFalse(by_title["Evidence only"]["has_captured_history"])
        self.assertTrue(by_title["Evidence only"]["verified_lossless"])
        self.assertFalse(by_title["Evidence only"]["provisional_lossless"])

    def test_artist_projection_fake_parity(self):
        fake = FakePipelineDB()
        self._seed_artist_projection_world(self.db)
        self._seed_artist_projection_world(fake)

        def facts(rows: Sequence[Mapping[str, object]]) -> dict[str, tuple[object, ...]]:
            return {
                str(row["album_title"]): (
                    row["mb_release_id"],
                    row["discogs_release_id"],
                    row["has_captured_history"],
                    row["verified_lossless"],
                    row["provisional_lossless"],
                )
                for row in rows
            }

        self.assertEqual(
            facts(self.db.list_requests_by_artist("Projection Artist")),
            facts(fake.list_requests_by_artist("Projection Artist")),
        )

    def test_artist_projection_rejects_foreign_mb_current_evidence(self) -> None:
        fake = FakePipelineDB()
        release_ids = _seed_foreign_current_evidence_world(
            self.db,
            identity_layout="musicbrainz",
        )
        self.assertEqual(
            _seed_foreign_current_evidence_world(
                fake,
                identity_layout="musicbrainz",
            ),
            release_ids,
        )

        def facts(
            db: PipelineDB | FakePipelineDB,
        ) -> dict[str, tuple[bool | None, bool]]:
            return {
                str(row["album_title"]): (
                    row["verified_lossless"],
                    row["provisional_lossless"],
                )
                for row in db.list_requests_by_artist("Foreign Evidence Artist")
            }

        real = facts(self.db)
        mirrored = facts(fake)
        self.assertTrue(real["Exact evidence"][0])
        self.assertEqual(real["Foreign evidence"], (False, False))
        self.assertEqual(real["Foreign provisional"], (False, False))
        self.assertEqual(real, mirrored)


@requires_postgres
class TestSlskdEventCursorRoundTrip(unittest.TestCase):
    """Rule A round-trip for upsert_slskd_event_cursor (issue #146)."""

    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_get_returns_none_before_first_upsert(self):
        self.assertIsNone(self.db.get_slskd_event_cursor())

    def test_upsert_round_trip_preserves_every_field(self):
        self.db.upsert_slskd_event_cursor(
            "11da6649-4ffc-4d72-afc0-b4238afcc4ec",
            "2026-07-01T23:00:10.7447018Z",
        )

        cursor = self.db.get_slskd_event_cursor()

        assert cursor is not None
        self.assertEqual(
            cursor["last_event_id"], "11da6649-4ffc-4d72-afc0-b4238afcc4ec")
        # Stored verbatim — 7-digit fractional seconds must survive.
        self.assertEqual(
            cursor["last_event_timestamp"], "2026-07-01T23:00:10.7447018Z")
        self.assertIsNotNone(cursor["updated_at"])

    def test_upsert_is_single_row_replace(self):
        self.db.upsert_slskd_event_cursor("ev-1", "2026-07-01T00:00:00.0000000Z")
        self.db.upsert_slskd_event_cursor("ev-2", "2026-07-02T00:00:00.0000000Z")

        cursor = self.db.get_slskd_event_cursor()

        assert cursor is not None
        self.assertEqual(cursor["last_event_id"], "ev-2")
        cur = self.db._execute("SELECT COUNT(*) AS n FROM slskd_event_cursor")
        self.assertEqual(cur.fetchone()["n"], 1)

    def test_fake_parity_on_identical_state(self):
        from tests.fakes import FakePipelineDB

        fake = FakePipelineDB()
        for db in (self.db, fake):
            db.upsert_slskd_event_cursor("ev-1", "2026-07-01T00:00:00.0000000Z")
        real = self.db.get_slskd_event_cursor()
        mirrored = fake.get_slskd_event_cursor()
        assert real is not None and mirrored is not None
        strip = lambda c: {k: v for k, v in c.items() if k != "updated_at"}
        self.assertEqual(strip(real), strip(mirrored))


@requires_postgres
class TestSearchLedgerRoundTrip(unittest.TestCase):
    """Rule A round-trip for the slskd search-id write-ahead ledger
    (migration 044, issue #576)."""

    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_get_unswept_search_ids_empty_before_any_record(self):
        self.assertEqual(
            self.db.get_unswept_search_ids(
                older_than=datetime.now(UTC) + timedelta(seconds=1)),
            [])

    def test_record_round_trip_preserves_every_field(self):
        self.db.record_search_id("sid-rt-1", "plan_search", 4321)

        rows = self.db.get_unswept_search_ids(
            older_than=datetime.now(UTC) + timedelta(seconds=1))

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["search_id"], "sid-rt-1")
        self.assertEqual(row["purpose"], "plan_search")
        self.assertEqual(row["request_id"], 4321)
        self.assertIsNotNone(row["created_at"])

    def test_record_search_id_with_null_request_id_round_trips(self):
        # artist_probe callers may have no request context.
        self.db.record_search_id("sid-rt-null", "artist_probe", None)

        rows = self.db.get_unswept_search_ids(
            older_than=datetime.now(UTC) + timedelta(seconds=1))

        self.assertEqual(rows[0]["request_id"], None)

    def test_get_unswept_search_ids_excludes_rows_inside_grace_window(self):
        self.db.record_search_id("sid-fresh", "plan_search", 1)

        rows = self.db.get_unswept_search_ids(
            older_than=datetime.now(UTC) - timedelta(hours=1))

        self.assertEqual(rows, [])

    def test_record_search_id_conflict_does_not_raise(self):
        # ON CONFLICT DO NOTHING — ids are unique by construction, but a
        # second insert for the same id must be a harmless no-op.
        self.db.record_search_id("sid-dup", "plan_search", 1)
        self.db.record_search_id("sid-dup", "plan_search", 1)  # must not raise
        cur = self.db._execute(
            "SELECT COUNT(*) AS n FROM slskd_search_ledger WHERE search_id = %s",
            ("sid-dup",))
        self.assertEqual(cur.fetchone()["n"], 1)

    def test_mark_search_ids_deleted_removes_from_unswept(self):
        self.db.record_search_id("sid-a", "plan_search", 1)
        self.db.record_search_id("sid-b", "plan_search", 2)

        self.db.mark_search_ids_deleted(["sid-a"])

        rows = self.db.get_unswept_search_ids(
            older_than=datetime.now(UTC) + timedelta(seconds=1))
        self.assertEqual([r["search_id"] for r in rows], ["sid-b"])
        cur = self.db._execute(
            "SELECT deleted_at FROM slskd_search_ledger WHERE search_id = %s",
            ("sid-a",))
        self.assertIsNotNone(cur.fetchone()["deleted_at"])

    def test_mark_search_ids_deleted_empty_list_is_a_noop(self):
        self.db.mark_search_ids_deleted([])  # must not raise / not query

    def test_prune_search_ledger_removes_only_old_deleted_rows(self):
        self.db.record_search_id("sid-old", "plan_search", 1)
        self.db.record_search_id("sid-undeleted", "plan_search", 2)
        self.db.mark_search_ids_deleted(["sid-old"])
        self.db._execute(
            "UPDATE slskd_search_ledger SET deleted_at = %s WHERE search_id = %s",
            (datetime.now(UTC) - timedelta(days=10), "sid-old"))

        removed = self.db.prune_search_ledger(
            deleted_before=datetime.now(UTC) - timedelta(days=7))

        self.assertEqual(removed, 1)
        cur = self.db._execute(
            "SELECT search_id FROM slskd_search_ledger ORDER BY search_id")
        self.assertEqual(
            [r["search_id"] for r in cur.fetchall()], ["sid-undeleted"])

    def test_fake_parity_on_identical_state(self):
        from tests.fakes import FakePipelineDB

        fake = FakePipelineDB()
        for db in (self.db, fake):
            db.record_search_id("sid-parity", "plan_search", 99)
        cutoff = datetime.now(UTC) + timedelta(seconds=1)
        real_rows = self.db.get_unswept_search_ids(older_than=cutoff)
        fake_rows = fake.get_unswept_search_ids(older_than=cutoff)
        strip = lambda rows: [
            {k: v for k, v in r.items() if k != "created_at"} for r in rows
        ]
        self.assertEqual(strip(real_rows), strip(fake_rows))


@requires_postgres
class TestTransferLedgerRoundTrip(unittest.TestCase):
    """Round-trip the durable slskd queue ownership ledger."""

    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def _seed_request(self, status: str = "wanted") -> int:
        return self.db.add_request(
            artist_name="Artist",
            album_title="Album",
            source="request",
            status=status,
        )

    def _ledger_rows(self, request_id: int) -> list[dict[str, Any]]:
        cur = self.db._execute(
            """
            SELECT id, request_id, username, filename, attempt_fingerprint,
                   enqueued_at, accepted_at, local_path
            FROM slskd_transfer_ledger
            WHERE request_id = %s
            ORDER BY enqueued_at, id
            """,
            (request_id,),
        )
        return [dict(row) for row in cur.fetchall()]

    def _backdate(self, request_id: int, *, days: int) -> None:
        self.db._execute(
            """
            UPDATE slskd_transfer_ledger
            SET enqueued_at = NOW() - (%s * INTERVAL '1 day')
            WHERE request_id = %s
            """,
            (days, request_id),
        )

    def test_record_transfer_enqueue_round_trip_preserves_every_field(self):
        rid = self._seed_request()

        self.db.record_transfer_enqueue([
            TransferLedgerRow(
                request_id=rid,
                username="p0",
                filename="Music\\a.flac",
                attempt_fingerprint="fp1",
            ),
            TransferLedgerRow(
                request_id=rid,
                username="p0",
                filename="Music\\b.flac",
            ),
        ])

        rows = self._ledger_rows(rid)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["username"], "p0")
        self.assertEqual(rows[0]["filename"], "Music\\a.flac")
        self.assertEqual(rows[0]["attempt_fingerprint"], "fp1")
        self.assertIsNone(rows[0]["accepted_at"])
        self.assertIsNone(rows[0]["local_path"])

    def test_confirm_transfer_enqueue_owns_only_newest_pending_row(self):
        rid = self._seed_request()
        row = TransferLedgerRow(
            request_id=rid, username="p0", filename="a.flac")
        self.db.record_transfer_enqueue([row])
        first_id = self._ledger_rows(rid)[0]["id"]
        self.db.record_transfer_enqueue([row])

        self.assertEqual(
            self.db.confirm_transfer_enqueue("p0", "a.flac", request_id=rid), 1)

        rows = self._ledger_rows(rid)
        accepted = [item for item in rows if item["accepted_at"] is not None]
        self.assertEqual(len(accepted), 1)
        self.assertNotEqual(accepted[0]["id"], first_id)
        self.assertEqual(
            self.db.get_owned_transfer_keys(), {("p0", "a.flac")})

    def test_confirm_transfer_enqueue_never_promotes_a_sibling_request(self):
        """#1278 item 2: acceptance is scoped to the request that asked.

        Two requests can hold pending intent on the SAME queue key -- the
        same peer serving the same remote path for two pressings, or a
        Replace-lineage retry. Unscoped, this UPDATE took the newest
        pending row for the key ACROSS THE WHOLE TABLE, so one request's
        accepted POST could promote another's rejected or still-in-flight
        intent into destructive ownership it never earned.
        """
        mine = self._seed_request()
        sibling = self.db.add_request(
            artist_name="Artist", album_title="Other Album",
            source="request", status="wanted")
        self.db.record_transfer_enqueue([TransferLedgerRow(
            request_id=mine, username="p0", filename="a.flac")])
        # The sibling's intent is NEWER, so an unscoped "newest pending
        # row for this key" confirm would land on it instead.
        self.db.record_transfer_enqueue([TransferLedgerRow(
            request_id=sibling, username="p0", filename="a.flac")])

        confirmed = self.db.confirm_transfer_enqueue(
            "p0", "a.flac", request_id=mine)

        self.assertEqual(confirmed, 1)
        self.assertIsNotNone(self._ledger_rows(mine)[0]["accepted_at"])
        self.assertIsNone(self._ledger_rows(sibling)[0]["accepted_at"])

    def test_confirm_transfer_enqueue_is_zero_for_a_request_with_no_intent(self):
        """A request that never ledgered this key confirms nothing, even
        when another request's pending row is sitting there."""
        mine = self._seed_request()
        sibling = self.db.add_request(
            artist_name="Artist", album_title="Other Album",
            source="request", status="wanted")
        self.db.record_transfer_enqueue([TransferLedgerRow(
            request_id=sibling, username="p0", filename="a.flac")])

        self.assertEqual(
            self.db.confirm_transfer_enqueue(
                "p0", "a.flac", request_id=mine),
            0)
        self.assertIsNone(self._ledger_rows(sibling)[0]["accepted_at"])
        self.assertEqual(self.db.get_owned_transfer_keys(), set())

    def test_completion_event_stamps_newest_open_queue_row(self):
        rid = self._seed_request()
        row = TransferLedgerRow(
            request_id=rid, username="p0", filename="a.flac")
        self.db.record_transfer_enqueue([row])
        first_id = self._ledger_rows(rid)[0]["id"]
        self.db.record_transfer_enqueue([row])
        self.db.confirm_transfer_enqueue("p0", "a.flac", request_id=rid)

        stamped = self.db.stamp_transfer_completion(
            "p0", "a.flac", "/downloads/a.flac")

        self.assertEqual(stamped, 1)
        stamped_rows = [
            item for item in self._ledger_rows(rid)
            if item["local_path"] is not None
        ]
        self.assertEqual(len(stamped_rows), 1)
        self.assertNotEqual(stamped_rows[0]["id"], first_id)
        self.assertEqual(stamped_rows[0]["local_path"], "/downloads/a.flac")

    def test_completion_event_miss_and_multirow_replay_are_noops(self):
        rid = self._seed_request()
        row = TransferLedgerRow(
            request_id=rid, username="p0", filename="a.flac")
        self.db.record_transfer_enqueue([row, row])
        self.db.confirm_transfer_enqueue("p0", "a.flac", request_id=rid)

        self.assertEqual(
            self.db.stamp_transfer_completion(
                "foreign", "a.flac", "/downloads/foreign.flac"),
            0,
        )
        self.assertEqual(
            self.db.stamp_transfer_completion(
                "p0", "a.flac", "/downloads/a.flac"),
            1,
        )
        self.assertEqual(
            self.db.stamp_transfer_completion(
                "p0", "a.flac", "/downloads/a.flac"),
            0,
        )
        stamped = [
            item for item in self._ledger_rows(rid)
            if item["local_path"] is not None
        ]
        self.assertEqual(len(stamped), 1)

    def test_owned_transfer_keys_cover_confirmed_retries_and_unstamped_rows(self):
        rid = self._seed_request()
        self.db.record_transfer_enqueue([
            TransferLedgerRow(
                request_id=rid, username="p0", filename="a.flac"),
            TransferLedgerRow(
                request_id=rid, username="p0", filename="a.flac"),
            TransferLedgerRow(
                request_id=rid, username="p1", filename="b.flac"),
        ])
        self.db.confirm_transfer_enqueue("p0", "a.flac", request_id=rid)
        self.db.confirm_transfer_enqueue("p1", "b.flac", request_id=rid)
        self.db.stamp_transfer_completion(
            "p0", "a.flac", "/downloads/a.flac")

        self.assertEqual(
            self.db.get_owned_transfer_keys(),
            {("p0", "a.flac"), ("p1", "b.flac")},
        )
        self.assertEqual(
            self.db.get_owned_local_paths(), {"/downloads/a.flac"})

    def test_owned_transfer_keys_for_answers_the_same_question_as_the_fake(self):
        """The keyed ownership read (#1278) is the whole-ledger read
        restricted to the asked keys, and both adapters agree.

        Driven through real PG and the fake over identical state: the
        destructive paths in lib/slskd_transfers.py consult this on
        worker threads, and a fake that answered more generously than
        PG would make every ownership-gating test a fiction.
        """
        from tests.fakes import FakePipelineDB

        fake = FakePipelineDB()
        rid = self._seed_request()
        fake.seed_request({
            "id": rid,
            "status": "wanted",
            "artist_name": "Artist",
            "album_title": "Album",
        })
        rows = [
            TransferLedgerRow(
                request_id=rid, username="p0", filename="a.flac"),
            TransferLedgerRow(
                request_id=rid, username="p1", filename="b.flac"),
        ]
        for db in (self.db, fake):
            db.record_transfer_enqueue(rows)
            db.confirm_transfer_enqueue("p0", "a.flac", request_id=rid)
            # p1 stays pending intent: asked, never accepted.

        asked = [
            ("p0", "a.flac"),
            ("p1", "b.flac"),
            ("stranger", "a.flac"),
        ]
        for db in (self.db, fake):
            with self.subTest(db=type(db).__name__):
                self.assertEqual(
                    db.get_owned_transfer_keys_for(asked),
                    {("p0", "a.flac")},
                )
                self.assertEqual(db.get_owned_transfer_keys_for([]), set())
                self.assertEqual(
                    db.get_owned_transfer_keys_for(asked),
                    db.get_owned_transfer_keys() & set(asked),
                )

    def test_confirm_transfer_enqueue_scoping_matches_the_fake(self):
        """The request-scoped confirm (#1278 item 2) answers the same in
        PG and in the fake, over identical two-request state.

        Every ownership-gated test in the tree runs against the fake; a
        fake that promoted a sibling's row where PG refuses would make
        all of them fictions about a database that behaves differently.
        """
        from tests.fakes import FakePipelineDB

        fake = FakePipelineDB()
        mine = self._seed_request()
        sibling = self.db.add_request(
            artist_name="Artist", album_title="Other Album",
            source="request", status="wanted")
        for request_id, title in ((mine, "Album"), (sibling, "Other Album")):
            fake.seed_request({
                "id": request_id,
                "status": "wanted",
                "artist_name": "Artist",
                "album_title": title,
            })
        for db in (self.db, fake):
            db.record_transfer_enqueue([TransferLedgerRow(
                request_id=mine, username="p0", filename="a.flac")])
            db.record_transfer_enqueue([TransferLedgerRow(
                request_id=sibling, username="p0", filename="a.flac")])

        for db in (self.db, fake):
            with self.subTest(db=type(db).__name__):
                self.assertEqual(
                    db.confirm_transfer_enqueue(
                        "p0", "a.flac", request_id=mine),
                    1)
                # The sibling's intent stayed pending, so a second
                # confirm for IT still has a row to promote.
                self.assertEqual(
                    db.confirm_transfer_enqueue(
                        "p0", "a.flac", request_id=sibling),
                    1)
                # Both consumed: a third confirm finds nothing.
                self.assertEqual(
                    db.confirm_transfer_enqueue(
                        "p0", "a.flac", request_id=mine),
                    0)

    def test_owned_transfer_keys_for_answers_membership_not_row_count(self):
        """One queue key retried across attempts holds many accepted
        rows; the answer is a membership set, not one entry per row.

        Pins the set-shaped contract callers rely on (they test `in`),
        not the SQL's `DISTINCT` -- which is a wire-size optimisation the
        set comprehension would make redundant either way.
        """
        rid = self._seed_request()
        row = TransferLedgerRow(
            request_id=rid, username="p0", filename="a.flac")
        for _ in range(3):
            self.db.record_transfer_enqueue([row])
            self.db.confirm_transfer_enqueue(
                "p0", "a.flac", request_id=rid)

        self.assertEqual(
            self.db.get_owned_transfer_keys_for([("p0", "a.flac")]),
            {("p0", "a.flac")},
        )

    def test_path_evidence_without_acceptance_is_rejected_by_the_schema(self):
        """Migration 083: ``local_path IS NOT NULL`` IMPLIES accepted.

        ``get_owned_local_paths`` is documented as the sole positive disk
        ownership signal yet selects on ``local_path IS NOT NULL`` alone.
        Before this constraint that query was safe only because of an
        argument about a different method's UPDATE; the constraint makes
        it safe against every writer, so the omission is provably
        equivalent rather than prose-equivalent.
        """
        import psycopg2.errors

        rid = self._seed_request()

        with self.assertRaises(psycopg2.errors.CheckViolation):
            self.db._execute(
                "INSERT INTO slskd_transfer_ledger "
                "(request_id, username, filename, local_path) "
                "VALUES (%s, %s, %s, %s)",
                (rid, "p0", "a.flac", "/downloads/a.flac"),
            )

        # The two legal shapes still write: pending intent with no path,
        # and accepted ownership carrying one.
        self.db._execute(
            "INSERT INTO slskd_transfer_ledger "
            "(request_id, username, filename) VALUES (%s, %s, %s)",
            (rid, "p0", "pending.flac"),
        )
        self.db._execute(
            "INSERT INTO slskd_transfer_ledger "
            "(request_id, username, filename, accepted_at, local_path) "
            "VALUES (%s, %s, %s, NOW(), %s)",
            (rid, "p0", "accepted.flac", "/downloads/accepted.flac"),
        )
        self.assertEqual(
            self.db.get_owned_local_paths(), {"/downloads/accepted.flac"})

    def test_stamping_cannot_be_undone_into_a_pathful_pending_row(self):
        """The constraint also holds against an UPDATE that would strip
        acceptance off a row already carrying path evidence -- the
        direction a repair one-shot or operator fix would take."""
        import psycopg2.errors

        rid = self._seed_request()
        self.db.record_transfer_enqueue([
            TransferLedgerRow(
                request_id=rid, username="p0", filename="a.flac"),
        ])
        self.db.confirm_transfer_enqueue("p0", "a.flac", request_id=rid)
        self.db.stamp_transfer_completion("p0", "a.flac", "/downloads/a.flac")

        with self.assertRaises(psycopg2.errors.CheckViolation):
            self.db._execute(
                "UPDATE slskd_transfer_ledger SET accepted_at = NULL "
                "WHERE request_id = %s",
                (rid,),
            )

    def test_transfer_reads_match_fake(self):
        from tests.fakes import FakePipelineDB

        fake = FakePipelineDB()
        rid = self._seed_request()
        fake.seed_request({
            "id": rid,
            "status": "wanted",
            "artist_name": "Artist",
            "album_title": "Album",
            "year": None,
        })
        rows = [
            TransferLedgerRow(
                request_id=rid,
                username="p0",
                filename="a.flac",
                attempt_fingerprint="fp1",
            ),
            TransferLedgerRow(
                request_id=rid, username="p1", filename="b.flac"),
        ]
        for db in (self.db, fake):
            db.record_transfer_enqueue(rows)
            db.confirm_transfer_enqueue("p0", "a.flac", request_id=rid)
            db.stamp_transfer_completion(
                "p0", "a.flac", "/downloads/a.flac")

        self.assertEqual(
            self.db.get_owned_transfer_keys(), fake.get_owned_transfer_keys())
        self.assertEqual(
            self.db.get_owned_local_paths(), fake.get_owned_local_paths())
        # The seeded request is ``wanted`` with no active_download_state,
        # i.e. the abandoned shape — both sides must agree it is reapable.
        self.assertEqual(
            self.db.get_abandoned_owned_local_paths(),
            fake.get_abandoned_owned_local_paths())
        self.assertEqual(
            self.db.get_abandoned_owned_local_paths(), {"/downloads/a.flac"})

    def test_abandoned_owned_local_paths_selects_only_wanted_without_state(self):
        """Real-PG contract for the reaper's age-exemption predicate: a
        request parked at ``wanted`` holding no ``active_download_state``
        references nothing, so its stamped files are reap-eligible now."""
        cases = [
            ("wanted, no state", "wanted", False, True),
            ("wanted, holding state", "wanted", True, False),
            ("imported", "imported", False, False),
            ("downloading", "downloading", False, False),
        ]
        for index, (desc, status, with_state, expected) in enumerate(cases):
            with self.subTest(desc=desc):
                rid = self._seed_request(status)
                path = f"/downloads/{index}.flac"
                self.db.record_transfer_enqueue([
                    TransferLedgerRow(
                        request_id=rid, username=f"u{index}",
                        filename=f"{index}.flac"),
                ])
                self.db.confirm_transfer_enqueue(
                    f"u{index}", f"{index}.flac", request_id=rid)
                self.db.stamp_transfer_completion(
                    f"u{index}", f"{index}.flac", path)
                if with_state:
                    self.db._execute(
                        "UPDATE album_requests SET active_download_state = "
                        "%s::jsonb WHERE id = %s",
                        ('{"files": []}', rid))

                paths = self.db.get_abandoned_owned_local_paths()

                self.assertEqual(path in paths, expected, desc)

    def test_prune_removes_old_inactive_accepted_rows(self):
        active = self._seed_request("downloading")
        inactive = self._seed_request("imported")
        recent = self._seed_request("imported")
        missing = 999999
        for request_id in (active, inactive, recent, missing):
            self.db.record_transfer_enqueue([
                TransferLedgerRow(
                    request_id=request_id,
                    username=f"p{request_id}",
                    filename=f"{request_id}.flac",
                ),
            ])
            self.db.confirm_transfer_enqueue(
                f"p{request_id}", f"{request_id}.flac",
                request_id=request_id)
        for request_id in (active, inactive, missing):
            self._backdate(request_id, days=200)

        removed = self.db.prune_transfer_ledger(
            older_than=datetime.now(UTC) - timedelta(days=90))

        self.assertEqual(removed, 2)
        remaining = self.db.get_owned_transfer_keys()
        self.assertIn((f"p{active}", f"{active}.flac"), remaining)
        self.assertIn((f"p{recent}", f"{recent}.flac"), remaining)
        self.assertNotIn((f"p{inactive}", f"{inactive}.flac"), remaining)
        self.assertNotIn((f"p{missing}", f"{missing}.flac"), remaining)

    def test_prune_old_pending_rows_ignores_active_request_status(self):
        pending_wanted = self._seed_request("wanted")
        pending_downloading = self._seed_request("downloading")
        accepted_wanted = self._seed_request("wanted")
        accepted_downloading = self._seed_request("downloading")
        for request_id in (
            pending_wanted,
            pending_downloading,
            accepted_wanted,
            accepted_downloading,
        ):
            self.db.record_transfer_enqueue([
                TransferLedgerRow(
                    request_id=request_id,
                    username=f"p{request_id}",
                    filename=f"{request_id}.flac",
                ),
            ])
            self._backdate(request_id, days=200)
        for request_id in (accepted_wanted, accepted_downloading):
            self.db.confirm_transfer_enqueue(
                f"p{request_id}", f"{request_id}.flac",
                request_id=request_id)

        removed = self.db.prune_transfer_ledger(
            older_than=datetime.now(UTC) - timedelta(days=90))

        self.assertEqual(removed, 2)
        self.assertEqual(self._ledger_rows(pending_wanted), [])
        self.assertEqual(self._ledger_rows(pending_downloading), [])
        self.assertEqual(len(self._ledger_rows(accepted_wanted)), 1)
        self.assertEqual(len(self._ledger_rows(accepted_downloading)), 1)

    def test_prune_exact_boundary_row_survives(self):
        rid = self._seed_request("imported")
        self.db.record_transfer_enqueue([
            TransferLedgerRow(
                request_id=rid, username="p0", filename="a.flac"),
        ])
        boundary = datetime.now(UTC) - timedelta(days=90)
        self.db._execute(
            "UPDATE slskd_transfer_ledger SET enqueued_at = %s "
            "WHERE request_id = %s",
            (boundary, rid),
        )

        self.assertEqual(self.db.prune_transfer_ledger(boundary), 0)
        self.assertEqual(len(self._ledger_rows(rid)), 1)

    def _seed_processing_request(self) -> int:
        """Seed a request in status='processing' with a valid automation
        import job owner: migration 066's CHECK
        (``album_requests_processing_owner_equivalent``) plus its deferred
        ``enforce_complete_processing_owner`` trigger both require
        ``active_automation_import_job_id`` to name a real, still-active
        (queued/running/recovery_required) ``automation_import`` job. The
        trigger is deferred to COMMIT, not to the end of each statement --
        under this connection's normal ``autocommit=True`` each ``_execute``
        call IS its own committed transaction, so the INSERT and the UPDATE
        must share one explicit transaction or the INSERT alone commits
        with the request still 'wanted' and the trigger raises."""
        rid = self._seed_request("wanted")
        self.db.conn.autocommit = False
        try:
            cur = self.db._execute(
                "INSERT INTO import_jobs (job_type, request_id) "
                "VALUES ('automation_import', %s) RETURNING id",
                (rid,),
            )
            row = cur.fetchone()
            assert row is not None
            job_id = row["id"]
            self.db._execute(
                "UPDATE album_requests SET status = 'processing', "
                "active_automation_import_job_id = %s WHERE id = %s",
                (job_id, rid),
            )
            self.db.conn.commit()
        finally:
            self.db.conn.autocommit = True
        return rid

    def _seed_accepted_row(
        self, *, status: str, username: str, filename: str,
    ) -> int:
        rid = (
            self._seed_processing_request() if status == "processing"
            else self._seed_request(status)
        )
        self.db.record_transfer_enqueue([
            TransferLedgerRow(request_id=rid, username=username, filename=filename),
        ])
        self.db.confirm_transfer_enqueue(
            username, filename, request_id=rid)
        return rid

    def test_get_conflicting_transfer_request_ids_downloading_owner_conflicts(self):
        """#1178 PR2 cross-cycle guard: an accepted row owned by a request
        CURRENTLY downloading the same key is a live conflict."""
        owner = self._seed_accepted_row(
            status="downloading", username="TheBun", filename="01.flac")
        candidate = self._seed_request("wanted")

        conflicting = self.db.get_conflicting_transfer_request_ids(
            [("TheBun", "01.flac")], exclude_request_id=candidate)

        self.assertEqual(conflicting, {owner})

    def test_get_conflicting_transfer_request_ids_missing_fingerprint_key_blocks(
        self,
    ):
        """#1199 item 2: an owner's ``active_download_state`` that exists
        but LACKS the ``attempt_fingerprint`` key fails CLOSED
        unconditionally -- every accepted row for that ``'downloading'``
        owner counts as in-scope (blocks), with no attempt-boundary
        rescue by age. Proven with BOTH a 30-day-old row and a
        just-written row so age plays no part in either direction; a
        prior version of this query fell back to a clock comparison here
        (deleted -- the measured deploy-window cohort was empty) that
        would have excluded the old row.

        Equivalence note: this test replaces
        ``test_get_conflicting_transfer_request_ids_scopes_to_current_
        attempt``, which asserted the OLD key did NOT block under the
        now-deleted time-predicate fallback. That differentiation no
        longer exists in production; attempt-scoping now lives ONLY in
        the fingerprint-equality arm, covered by
        ``test_get_conflicting_transfer_request_ids_fingerprint_match_
        blocks`` and ``test_get_conflicting_transfer_request_ids_
        different_fingerprint_beats_newer_time``.
        """
        owner = self._seed_request("downloading")
        old_username, old_filename = "OLD", "old.flac"
        current_username, current_filename = "NEW", "new.flac"
        candidate = self._seed_request("wanted")

        self.db.record_transfer_enqueue([
            TransferLedgerRow(
                request_id=owner, username=old_username, filename=old_filename),
        ])
        self.db.confirm_transfer_enqueue(
            old_username, old_filename, request_id=owner)
        self.db._execute(
            "UPDATE slskd_transfer_ledger SET enqueued_at = "
            "NOW() - INTERVAL '30 days' "
            "WHERE request_id = %s AND username = %s",
            (owner, old_username),
        )

        # active_download_state exists (proving this is NOT the NULL-state
        # pin below) but carries no "attempt_fingerprint" key.
        current_state = {
            "filetype": "flac", "enqueued_at": datetime.now(UTC).isoformat(),
            "files": [],
        }
        self.db._execute(
            "UPDATE album_requests SET active_download_state = %s::jsonb "
            "WHERE id = %s",
            (json.dumps(current_state), owner),
        )
        self.db.record_transfer_enqueue([
            TransferLedgerRow(
                request_id=owner, username=current_username,
                filename=current_filename),
        ])
        self.db.confirm_transfer_enqueue(
            current_username, current_filename, request_id=owner)

        self.assertEqual(
            self.db.get_conflicting_transfer_request_ids(
                [(old_username, old_filename)], exclude_request_id=candidate),
            {owner},
            "a missing fingerprint key fails closed regardless of the "
            "ledger row's age",
        )
        self.assertEqual(
            self.db.get_conflicting_transfer_request_ids(
                [(current_username, current_filename)],
                exclude_request_id=candidate),
            {owner},
            "a missing fingerprint key still blocks the current key too",
        )

    def test_get_conflicting_transfer_request_ids_status_filter(self):
        """Replace-lineage attempt sharing (8781/8846) and an owner that
        already moved on must NEVER block -- only 'downloading' does. The
        filter is deliberately the single value 'downloading', never
        'processing' (CLAUDE.md critical invariant 10) -- 'processing' is
        included here specifically to kill the
        ``'downloading' -> IN ('downloading', 'processing')`` mutant
        (#1178 PR2 review F1); every other status in this table already
        happened to make that mutant unreachable."""
        for status in ("replaced", "wanted", "imported", "processing"):
            with self.subTest(status=status):
                owner = self._seed_accepted_row(
                    status=status, username=f"peer-{status}",
                    filename=f"{status}.flac")
                candidate = self._seed_request("wanted")

                conflicting = self.db.get_conflicting_transfer_request_ids(
                    [(f"peer-{status}", f"{status}.flac")],
                    exclude_request_id=candidate)

                self.assertEqual(conflicting, set(), (status, owner))

    def test_get_conflicting_transfer_request_ids_excludes_self(self):
        rid = self._seed_accepted_row(
            status="downloading", username="p0", filename="a.flac")

        conflicting = self.db.get_conflicting_transfer_request_ids(
            [("p0", "a.flac")], exclude_request_id=rid)

        self.assertEqual(conflicting, set())

    def test_get_conflicting_transfer_request_ids_ignores_pending_intent(self):
        owner = self._seed_request("downloading")
        self.db.record_transfer_enqueue([
            TransferLedgerRow(request_id=owner, username="p0", filename="a.flac"),
        ])  # never confirmed -- accepted_at stays NULL
        candidate = self._seed_request("wanted")

        conflicting = self.db.get_conflicting_transfer_request_ids(
            [("p0", "a.flac")], exclude_request_id=candidate)

        self.assertEqual(conflicting, set())

    def test_get_conflicting_transfer_request_ids_ignores_unrelated_keys(self):
        self._seed_accepted_row(
            status="downloading", username="p0", filename="a.flac")
        candidate = self._seed_request("wanted")

        conflicting = self.db.get_conflicting_transfer_request_ids(
            [("p0", "b.flac")], exclude_request_id=candidate)

        self.assertEqual(conflicting, set())

    def test_get_conflicting_transfer_request_ids_empty_keys_is_a_noop(self):
        candidate = self._seed_request("wanted")

        self.assertEqual(
            self.db.get_conflicting_transfer_request_ids(
                [], exclude_request_id=candidate),
            set(),
        )

    def test_get_conflicting_transfer_request_ids_matches_fake(self):
        """The fake must mirror the real join's semantics exactly
        (test-fidelity.md Rule A pattern applied to a read method) --
        including the missing-fingerprint fail-closed arm (review F6,
        updated for #1199 item 2): without a scoped owner carrying BOTH
        an old and a current accepted row under a fingerprint-less
        state, every case above takes the unconditional NULL-state ELSE
        arm through a DIFFERENT code path (no active_download_state row
        at all) and the "state exists but lacks the key" arm is never
        exercised on either side."""
        from tests.fakes import FakePipelineDB

        fake = FakePipelineDB()
        cases = [
            ("owner-downloading", "downloading"),
            ("owner-wanted", "wanted"),
            ("owner-imported", "imported"),
            ("owner-replaced", "replaced"),
        ]
        candidate = self._seed_request("wanted")
        fake.seed_request({
            "id": candidate, "status": "wanted",
            "artist_name": "Artist", "album_title": "Album", "year": None,
        })
        keys: list[tuple[str, str]] = []
        for username, status in cases:
            rid = self._seed_request(status)
            fake.seed_request({
                "id": rid, "status": status,
                "artist_name": "Artist", "album_title": "Album", "year": None,
            })
            row = TransferLedgerRow(
                request_id=rid, username=username, filename="a.flac")
            for db in (self.db, fake):
                db.record_transfer_enqueue([row])
                db.confirm_transfer_enqueue(
                    username, "a.flac", request_id=rid)
            keys.append((username, "a.flac"))

        # F6: a scoped owner carrying BOTH an old and a current accepted
        # row, with a state that EXISTS but lacks "attempt_fingerprint",
        # seeded identically on both the real and fake sides so parity is
        # proven at that specific arm -- not just at the simpler
        # no-state-at-all case every "cases" entry above exercises.
        scoped_owner = self._seed_request("downloading")
        fake.seed_request({
            "id": scoped_owner, "status": "downloading",
            "artist_name": "Artist", "album_title": "Album", "year": None,
        })
        old_username, old_filename = "owner-old-attempt", "old.flac"
        current_username, current_filename = (
            "owner-current-attempt", "current.flac")
        for db in (self.db, fake):
            db.record_transfer_enqueue([
                TransferLedgerRow(
                    request_id=scoped_owner, username=old_username,
                    filename=old_filename),
            ])
            db.confirm_transfer_enqueue(
                old_username, old_filename, request_id=scoped_owner)
        self.db._execute(
            "UPDATE slskd_transfer_ledger SET enqueued_at = "
            "NOW() - INTERVAL '30 days' "
            "WHERE request_id = %s AND username = %s",
            (scoped_owner, old_username),
        )
        old_fake_id = next(
            fid for fid, r in fake._transfer_ledger.items()
            if r.request_id == scoped_owner and r.username == old_username)
        fake._transfer_ledger[old_fake_id].enqueued_at = (
            datetime.now(UTC) - timedelta(days=30))

        current_state = {
            "filetype": "flac", "enqueued_at": datetime.now(UTC).isoformat(),
            "files": [],
        }
        self.db._execute(
            "UPDATE album_requests SET active_download_state = %s::jsonb "
            "WHERE id = %s",
            (json.dumps(current_state), scoped_owner),
        )
        fake.request(scoped_owner)["active_download_state"] = current_state
        for db in (self.db, fake):
            db.record_transfer_enqueue([
                TransferLedgerRow(
                    request_id=scoped_owner, username=current_username,
                    filename=current_filename),
            ])
            db.confirm_transfer_enqueue(
                current_username, current_filename,
                request_id=scoped_owner)
        keys.extend([
            (old_username, old_filename),
            (current_username, current_filename),
        ])

        # #1196 item 1: a SECOND scoped owner whose state carries
        # ``attempt_fingerprint`` -- seeded identically on both sides so
        # parity is proven at the fingerprint-equality branch too, not
        # just the missing-fingerprint fail-closed arm ``scoped_owner``
        # above exercises. Its old row's ``enqueued_at`` is pushed AFTER
        # the witness -- deliberately a shape a clock comparison would
        # get wrong -- so this only stays excluded on both sides because
        # the fingerprint (not any clock) decides it.
        fp_owner = self._seed_request("downloading")
        fake.seed_request({
            "id": fp_owner, "status": "downloading",
            "artist_name": "Artist", "album_title": "Album", "year": None,
        })
        fp_old_username, fp_old_filename = "fp-owner-old", "old-fp.flac"
        fp_cur_username, fp_cur_filename = "fp-owner-current", "cur-fp.flac"
        fp_old, fp_current = "aaaaaaaa", "bbbbbbbb"
        fp_witness = datetime.now(UTC)
        fp_state = {
            "filetype": "flac", "enqueued_at": fp_witness.isoformat(),
            "files": [], "attempt_fingerprint": fp_current,
        }
        self.db._execute(
            "UPDATE album_requests SET active_download_state = %s::jsonb "
            "WHERE id = %s",
            (json.dumps(fp_state), fp_owner),
        )
        fake.request(fp_owner)["active_download_state"] = fp_state
        for db in (self.db, fake):
            db.record_transfer_enqueue([
                TransferLedgerRow(
                    request_id=fp_owner, username=fp_old_username,
                    filename=fp_old_filename, attempt_fingerprint=fp_old),
                TransferLedgerRow(
                    request_id=fp_owner, username=fp_cur_username,
                    filename=fp_cur_filename,
                    attempt_fingerprint=fp_current),
            ])
            db.confirm_transfer_enqueue(
                fp_old_username, fp_old_filename, request_id=fp_owner)
            db.confirm_transfer_enqueue(
                fp_cur_username, fp_cur_filename, request_id=fp_owner)
        newer_than_witness = fp_witness + timedelta(seconds=5)
        self.db._execute(
            "UPDATE slskd_transfer_ledger SET enqueued_at = %s "
            "WHERE request_id = %s AND username = %s",
            (newer_than_witness, fp_owner, fp_old_username),
        )
        fp_old_fake_id = next(
            fid for fid, r in fake._transfer_ledger.items()
            if r.request_id == fp_owner and r.username == fp_old_username)
        fake._transfer_ledger[fp_old_fake_id].enqueued_at = newer_than_witness
        keys.extend([
            (fp_old_username, fp_old_filename),
            (fp_cur_username, fp_cur_filename),
        ])

        self.assertEqual(
            self.db.get_conflicting_transfer_request_ids(
                keys, exclude_request_id=candidate),
            fake.get_conflicting_transfer_request_ids(
                keys, exclude_request_id=candidate),
        )
        # The missing-fingerprint arm must actually be earning its keep:
        # BOTH the old and the current key block, regardless of age --
        # there is no attempt-boundary rescue when the state lacks the
        # fingerprint key.
        self.assertEqual(
            self.db.get_conflicting_transfer_request_ids(
                [(old_username, old_filename)], exclude_request_id=candidate),
            {scoped_owner},
        )
        self.assertEqual(
            self.db.get_conflicting_transfer_request_ids(
                [(current_username, current_filename)],
                exclude_request_id=candidate),
            {scoped_owner},
        )
        # Fingerprint arm on BOTH sides: a newer-but-wrong-fingerprint
        # row is excluded, a matching-fingerprint row still blocks.
        self.assertEqual(
            self.db.get_conflicting_transfer_request_ids(
                [(fp_old_username, fp_old_filename)],
                exclude_request_id=candidate),
            set(),
        )
        self.assertEqual(
            fake.get_conflicting_transfer_request_ids(
                [(fp_old_username, fp_old_filename)],
                exclude_request_id=candidate),
            set(),
        )
        self.assertEqual(
            self.db.get_conflicting_transfer_request_ids(
                [(fp_cur_username, fp_cur_filename)],
                exclude_request_id=candidate),
            {fp_owner},
        )
        self.assertEqual(
            fake.get_conflicting_transfer_request_ids(
                [(fp_cur_username, fp_cur_filename)],
                exclude_request_id=candidate),
            {fp_owner},
        )

    def test_get_conflicting_transfer_request_ids_fingerprint_match_blocks(self):
        """#1196 item 1 world (a): the owner's state carries
        attempt_fingerprint; an accepted ledger row with the SAME
        fingerprint blocks by exact identity -- proven with a state
        timestamp LATER than the ledger row's enqueued_at, a shape the
        OLD time-only predicate would NOT have blocked, so this test
        only passes because the fingerprint arm actually ran."""
        owner = self._seed_request("downloading")
        username, filename = "peer", "a.flac"
        fp = "deadbeef"
        self.db.record_transfer_enqueue([
            TransferLedgerRow(
                request_id=owner, username=username, filename=filename,
                attempt_fingerprint=fp),
        ])
        self.db.confirm_transfer_enqueue(
            username, filename, request_id=owner)
        ledger_enqueued_at = datetime.now(UTC)
        self.db._execute(
            "UPDATE slskd_transfer_ledger SET enqueued_at = %s "
            "WHERE request_id = %s AND username = %s",
            (ledger_enqueued_at, owner, username),
        )
        state = {
            "filetype": "flac",
            "enqueued_at": (
                ledger_enqueued_at + timedelta(hours=1)).isoformat(),
            "files": [], "attempt_fingerprint": fp,
        }
        self.db._execute(
            "UPDATE album_requests SET active_download_state = %s::jsonb "
            "WHERE id = %s",
            (json.dumps(state), owner),
        )
        candidate = self._seed_request("wanted")

        conflicting = self.db.get_conflicting_transfer_request_ids(
            [(username, filename)], exclude_request_id=candidate)

        self.assertEqual(
            conflicting, {owner},
            "matching fingerprint must block even though the ledger "
            "row's enqueued_at is BEFORE the state witness -- the old "
            "time predicate alone would have excluded it",
        )

    def test_get_conflicting_transfer_request_ids_different_fingerprint_beats_newer_time(self):
        """#1196 item 1 world (b): a different-fingerprint row must NOT
        block even when its enqueued_at is NEWER than the current-attempt
        witness -- the exactness win over the time predicate. Reverting
        to the pre-#1196 SQL (dropping the fingerprint CASE arm entirely)
        makes this test RED, because the old time-only predicate DOES
        block on this exact shape."""
        owner = self._seed_request("downloading")
        old_username, old_filename = "OLD", "old.flac"
        fp_old, fp_current = "aaaaaaaa", "bbbbbbbb"
        witness = datetime.now(UTC)
        state = {
            "filetype": "flac", "enqueued_at": witness.isoformat(),
            "files": [], "attempt_fingerprint": fp_current,
        }
        self.db._execute(
            "UPDATE album_requests SET active_download_state = %s::jsonb "
            "WHERE id = %s",
            (json.dumps(state), owner),
        )
        self.db.record_transfer_enqueue([
            TransferLedgerRow(
                request_id=owner, username=old_username,
                filename=old_filename, attempt_fingerprint=fp_old),
        ])
        self.db.confirm_transfer_enqueue(
            old_username, old_filename, request_id=owner)
        # NEWER than the witness -- the OLD time predicate
        # (enqueued_at >= witness) would have counted this row as the
        # current attempt and blocked.
        self.db._execute(
            "UPDATE slskd_transfer_ledger SET enqueued_at = %s "
            "WHERE request_id = %s AND username = %s",
            (witness + timedelta(seconds=5), owner, old_username),
        )
        candidate = self._seed_request("wanted")

        conflicting = self.db.get_conflicting_transfer_request_ids(
            [(old_username, old_filename)], exclude_request_id=candidate)

        self.assertEqual(
            conflicting, set(),
            "a different-fingerprint row must not block even though "
            "its enqueued_at is newer than the current-attempt witness",
        )

    def test_get_conflicting_transfer_request_ids_null_state_blocks(self):
        """#1196 item 1 world (d), updated for #1199 item 2: a NULL
        active_download_state still fails CLOSED unconditionally --
        every accepted row for that 'downloading' owner counts as
        in-scope, regardless of the ledger row's own fingerprint. (Review
        F3: this is NOT a dedicated ``IS NULL`` SQL arm -- that arm was
        proven redundant on real PG and removed. The fail-closed
        guarantee this pin protects lives in the ELSE arm, which #1199
        item 2 simplified from a clock ``COALESCE`` to unconditional
        ``TRUE``: ``->>`` on a NULL jsonb state returns NULL for the
        ``attempt_fingerprint`` key regardless, so the ``CASE`` always
        reaches ``ELSE TRUE``. This pin kills a mutant that inverts or
        removes that ``TRUE``.)"""
        owner = self._seed_accepted_row(
            status="downloading", username="p0", filename="a.flac")
        row = self.db.get_request(owner)
        assert row is not None
        self.assertIsNone(row["active_download_state"])
        candidate = self._seed_request("wanted")

        conflicting = self.db.get_conflicting_transfer_request_ids(
            [("p0", "a.flac")], exclude_request_id=candidate)

        self.assertEqual(conflicting, {owner})

    def test_get_conflicting_transfer_request_ids_explicit_json_null_fingerprint_blocks(
        self,
    ):
        """Hostile-shape pin (issue #1199 review F8): an
        ``active_download_state`` that carries an EXPLICIT JSON ``null``
        for ``attempt_fingerprint`` (``{"attempt_fingerprint": null, ...}``
        -- distinct from the key being ABSENT, and never producible by
        ``build_active_download_state``'s ``omit_defaults=True``, but
        constructible by hostile/manual data) must still fail CLOSED
        (block). This is the world that distinguishes ``->>`` from ``->``
        in the WHEN test: ``->>`` extracts a JSON null as SQL NULL (so the
        CASE reaches ``ELSE TRUE`` and blocks); ``->`` would instead
        extract a non-NULL jsonb 'null' scalar, taking the WHEN branch and
        comparing it against the ledger's own (non-null) text
        ``attempt_fingerprint`` -- which never matches, so a ``->``
        regression would fail OPEN (not block) on exactly this shape."""
        owner = self._seed_request("downloading")
        username, filename = "p0", "a.flac"
        self.db.record_transfer_enqueue([
            TransferLedgerRow(
                request_id=owner, username=username, filename=filename,
                attempt_fingerprint="deadbeef"),
        ])
        self.db.confirm_transfer_enqueue(
            username, filename, request_id=owner)
        state = {
            "filetype": "flac", "enqueued_at": datetime.now(UTC).isoformat(),
            "files": [], "attempt_fingerprint": None,
        }
        self.db._execute(
            "UPDATE album_requests SET active_download_state = %s::jsonb "
            "WHERE id = %s",
            (json.dumps(state), owner),
        )
        candidate = self._seed_request("wanted")

        conflicting = self.db.get_conflicting_transfer_request_ids(
            [(username, filename)], exclude_request_id=candidate)

        self.assertEqual(
            conflicting, {owner},
            "an explicit JSON null attempt_fingerprint must fail closed "
            "(block), exactly like a missing key or a NULL top-level "
            "state -- a -> regression instead of ->> would fail open here",
        )

@requires_postgres
class TestReadProjectionParity(unittest.TestCase):
    """#481 item 2 — fake<->production READ-projection parity gate.

    ``FakePipelineDB`` hand-mirrors production SELECT projections as
    literal key tuples (``_long_tail_projection`` in
    ``tests/fakes/pipeline_db/requests.py``, the ``list_triage_page``
    projection in ``tests/fakes/pipeline_db/misc.py``)
    across dozens of ``get_*`` methods. Nothing failed if the two
    drifted — PR #480 had to update the SQL projection and the fake's
    key tuple in lockstep by hand. This is the read-side mirror of
    ``.claude/rules/test-fidelity.md`` Rule A (write round-trips):
    seed an IDENTICAL row through the real ``PipelineDB`` and
    ``FakePipelineDB``, call the same ``get_*`` method on both, and
    assert KEY-SET EQUALITY of the returned rows (not value equality —
    ids and timestamps are backend-assigned/time-anchored and
    deliberately not compared). A key-set drift means the fake returns
    a column production doesn't (or vice versa) — exactly the seam
    that keeps fake-driven contract tests green while the live route
    500s or renders nulls.

    **The audit table.** Each ``test_*`` method below is one entry;
    growing coverage means adding another method here, seeding both
    backends identically, and calling ``_assert_keyset_parity``. Not
    yet covered — see the PR body / final report Suggestions for the
    rest of the ~51 ``get_*`` methods FakePipelineDB mirrors.
    """

    def setUp(self):
        self.db = make_db()
        from tests.fakes import FakePipelineDB
        self.fake = FakePipelineDB()

    def tearDown(self):
        self.db.close()

    @staticmethod
    def _assert_keyset_parity(
        test: unittest.TestCase,
        real_rows: "Sequence[Mapping[str, Any]]",
        fake_rows: "Sequence[Mapping[str, Any]]",
        label: str,
    ) -> None:
        """Assert real PG and FakePipelineDB return identically-keyed rows.

        Compares row count, then per-row key sets — NOT values (some
        columns are backend-assigned like ``id`` or time-anchored).
        On drift, the failure names the exact column(s) that differ,
        matching the DX of the Rule A round-trip tests.
        """
        test.assertEqual(
            len(real_rows), len(fake_rows),
            f"{label}: real PG returned {len(real_rows)} row(s), "
            f"FakePipelineDB returned {len(fake_rows)} row(s) — seeding "
            f"drifted between the two backends",
        )
        for i, (real_row, fake_row) in enumerate(
            zip(real_rows, fake_rows, strict=True)
        ):
            real_keys = set(real_row.keys())
            fake_keys = set(fake_row.keys())
            if real_keys == fake_keys:
                continue
            only_real = sorted(real_keys - fake_keys)
            only_fake = sorted(fake_keys - real_keys)
            test.fail(
                f"{label} row {i}: projection key-set drifted between "
                f"real PG and FakePipelineDB — columns only in real PG: "
                f"{only_real}; columns only in FakePipelineDB: {only_fake}. "
                f"Fix the fake's projection to mirror production (or the "
                f"reverse if the SQL change is the point of the PR)."
            )

    # --- get_long_tail_cohort / get_long_tail_request ----------------------

    def _seed_long_tail_request(
        self, db: Any, *, mb_release_id: str, with_tracks: bool,
        with_rescue: bool,
    ) -> int:
        rid = db.add_request(
            "Long Tail Artist", "Long Tail Album", "request",
            mb_release_id=mb_release_id,
        )
        if with_tracks:
            db.set_tracks(rid, [
                {"disc_number": 1, "track_number": 1, "title": "One",
                 "length_seconds": 100},
                {"disc_number": 1, "track_number": 2, "title": "Two",
                 "length_seconds": 200},
            ])
        if with_rescue:
            db.insert_youtube_running(
                request_id=rid, browse_id="MPREb_parity",
                audio_playlist_id=None,
                yt_url="https://example.invalid/parity",
                expected_track_count=2,
            )
        return rid

    def test_get_long_tail_cohort_keyset_parity(self):
        for db in (self.db, self.fake):
            self._seed_long_tail_request(
                db, mb_release_id="lt-parity-plain", with_tracks=False,
                with_rescue=False)
            self._seed_long_tail_request(
                db, mb_release_id="lt-parity-full", with_tracks=True,
                with_rescue=True)

        real_rows = self.db.get_long_tail_cohort()
        fake_rows = self.fake.get_long_tail_cohort()
        self._assert_keyset_parity(
            self, real_rows, fake_rows, "get_long_tail_cohort")

    def test_long_tail_cohort_carries_the_current_accusation_columns(self):
        """Issue #829 PR4/N3: the worklist chip's codec facts come from a
        real join, so real PG must actually return them."""
        from lib.pipeline_db._shared import CURRENT_EVIDENCE_PREFIX
        from lib.quality import AudioQualityMeasurement
        from web.classify import (
            AccusationFlags,
            evidence_column_accusation_flags,
        )

        rid = self._seed_long_tail_request(
            self.db, mb_release_id="lt-audit-only", with_tracks=False,
            with_rescue=False)
        evidence = make_album_quality_evidence(
            mb_release_id="lt-audit-only",
            source_path="/Beets/installed",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=256, avg_bitrate_kbps=256, is_cbr=True,
                format="AAC", spectral_grade="likely_transcode",
                spectral_bitrate_kbps=128, spectral_subject="installed",
                spectral_provenance="measured", cliff_hz=15000,
                codec_family="aac", spectral_measurement_version=2,
            ),
            codec="aac", container="m4a", storage_format="AAC",
        )
        self.db.upsert_album_quality_evidence(evidence)
        persisted = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        self.assertTrue(
            self.db.set_request_current_evidence(rid, persisted.id))

        row = next(
            r for r in self.db.get_long_tail_cohort() if r["id"] == rid)

        self.assertEqual(
            evidence_column_accusation_flags(
                row, prefix=CURRENT_EVIDENCE_PREFIX),
            AccusationFlags(admissible=False, withheld="audit_only_codec"),
        )

    def test_get_long_tail_request_keyset_parity(self):
        real_id = self._seed_long_tail_request(
            self.db, mb_release_id="lt-single-parity", with_tracks=True,
            with_rescue=True)
        fake_id = self._seed_long_tail_request(
            self.fake, mb_release_id="lt-single-parity", with_tracks=True,
            with_rescue=True)

        real_row = self.db.get_long_tail_request(real_id)
        fake_row = self.fake.get_long_tail_request(fake_id)
        assert real_row is not None and fake_row is not None
        self._assert_keyset_parity(
            self, [real_row], [fake_row], "get_long_tail_request (hit)")

    def test_get_long_tail_request_none_branch_parity(self):
        # Non-existent id — both sides must agree on None. There's
        # nothing to key-compare when both sides are None; the
        # assertion IS the parity check here.
        self.assertIsNone(self.db.get_long_tail_request(999_999_999))
        self.assertIsNone(self.fake.get_long_tail_request(999_999_999))

    # --- list_triage_page ----------------------------------------------------

    def _seed_triage_request(self, db: Any, *, mb_release_id: str) -> int:
        return db.add_request(
            "Triage Artist", "Triage Album", "request",
            mb_release_id=mb_release_id,
        )

    def test_list_triage_page_all_keyset_parity(self):
        from lib.triage_service import ParsedTriageFilter

        for db in (self.db, self.fake):
            self._seed_triage_request(db, mb_release_id="triage-all-1")

        filter_spec = ParsedTriageFilter(kind="all", raw="all")
        real_rows = self.db.list_triage_page(
            filter_spec=filter_spec, page_size=50, after_request_id=None)
        fake_rows = self.fake.list_triage_page(
            filter_spec=filter_spec, page_size=50, after_request_id=None)
        self._assert_keyset_parity(
            self, real_rows, fake_rows, "list_triage_page(all)")

    def test_list_triage_page_unfindable_keyset_parity(self):
        from lib.triage_service import ParsedTriageFilter
        from lib.unfindable_detection_service import CATEGORY_ARTIST_ABSENT

        now = datetime.now(UTC)
        for db in (self.db, self.fake):
            rid = self._seed_triage_request(
                db, mb_release_id="triage-unfindable-1")
            db.set_unfindable_category(
                rid, category=CATEGORY_ARTIST_ABSENT, categorised_at=now)

        filter_spec = ParsedTriageFilter(
            kind="unfindable", unfindable_category=CATEGORY_ARTIST_ABSENT,
            raw=f"unfindable:{CATEGORY_ARTIST_ABSENT}")
        real_rows = self.db.list_triage_page(
            filter_spec=filter_spec, page_size=50, after_request_id=None)
        fake_rows = self.fake.list_triage_page(
            filter_spec=filter_spec, page_size=50, after_request_id=None)
        self._assert_keyset_parity(
            self, real_rows, fake_rows, "list_triage_page(unfindable)")

    def test_list_triage_page_data_quality_keyset_parity(self):
        from lib.triage_service import ParsedTriageFilter

        for db in (self.db, self.fake):
            rid = self._seed_triage_request(
                db, mb_release_id="triage-dataq-1")
            db.record_field_resolution(
                rid, "release_group_year", "unresolved_mirror_unavailable",
                "URLError")

        filter_spec = ParsedTriageFilter(
            kind="data_quality", raw="data_quality")
        real_rows = self.db.list_triage_page(
            filter_spec=filter_spec, page_size=50, after_request_id=None)
        fake_rows = self.fake.list_triage_page(
            filter_spec=filter_spec, page_size=50, after_request_id=None)
        self._assert_keyset_parity(
            self, real_rows, fake_rows, "list_triage_page(data_quality)")

    def test_list_triage_page_search_not_converting_keyset_parity(self):
        from lib.triage_service import ParsedTriageFilter

        for db in (self.db, self.fake):
            rid = self._seed_triage_request(
                db, mb_release_id="triage-search-1")
            db.log_search(rid, query="q", outcome="no_match", elapsed_s=1.0)

        filter_spec = ParsedTriageFilter(
            kind="search_not_converting", raw="search_not_converting")
        real_rows = self.db.list_triage_page(
            filter_spec=filter_spec, page_size=50, after_request_id=None)
        fake_rows = self.fake.list_triage_page(
            filter_spec=filter_spec, page_size=50, after_request_id=None)
        self._assert_keyset_parity(
            self, real_rows, fake_rows,
            "list_triage_page(search_not_converting)")

    # --- get_field_resolutions_for_requests ----------------------------------

    def test_get_field_resolutions_for_requests_keyset_parity(self):
        ids: dict[int, int] = {}
        for db in (self.db, self.fake):
            rid = self._seed_triage_request(
                db, mb_release_id="fieldres-parity-1")
            db.record_field_resolution(
                rid, "catalog_number", "unresolved_404", "http_404")
            ids[id(db)] = rid

        real_map = self.db.get_field_resolutions_for_requests(
            [ids[id(self.db)]])
        fake_map = self.fake.get_field_resolutions_for_requests(
            [ids[id(self.fake)]])
        real_rows = real_map.get(ids[id(self.db)], [])
        fake_rows = fake_map.get(ids[id(self.fake)], [])
        self._assert_keyset_parity(
            self, real_rows, fake_rows,
            "get_field_resolutions_for_requests")

    # --- get_wanted_searchable (#523) ----------------------------------------

    def _seed_wanted_searchable_request(
        self, db: Any, *, mb_release_id: str, generator_id: str,
    ) -> int:
        from lib.pipeline_db import SearchPlanItemInput

        rid = db.add_request(
            "WS Artist", "WS Album", "request", mb_release_id=mb_release_id)
        db.create_successful_search_plan(
            request_id=rid, generator_id=generator_id,
            items=[SearchPlanItemInput(
                ordinal=0, strategy="default", query="Q")])
        return rid

    def test_get_wanted_searchable_keyset_parity(self):
        for db in (self.db, self.fake):
            self._seed_wanted_searchable_request(
                db, mb_release_id="ws-parity", generator_id="g-parity")

        real_rows = self.db.get_wanted_searchable("g-parity")
        fake_rows = self.fake.get_wanted_searchable("g-parity")
        self.assertTrue(
            real_rows, "seeding produced no rows on real PG — "
            "get_wanted_searchable parity would pass vacuously")
        self.assertTrue(
            fake_rows, "seeding produced no rows on FakePipelineDB — "
            "get_wanted_searchable parity would pass vacuously")
        self._assert_keyset_parity(
            self, real_rows, fake_rows, "get_wanted_searchable")

    def test_get_wanted_searchable_no_active_plan_empty_branch(self):
        # A wanted request with no active plan is not execution-eligible
        # -- both backends must agree on the empty-list contract. This
        # is the explicit contract being asserted, not the keyset check,
        # so an empty result here is the expected (non-vacuous) outcome.
        for db in (self.db, self.fake):
            db.add_request(
                "WS Artist", "WS Album (no plan)", "request",
                mb_release_id="ws-no-plan")

        self.assertEqual(self.db.get_wanted_searchable("g-parity"), [])
        self.assertEqual(self.fake.get_wanted_searchable("g-parity"), [])

    def test_get_wanted_searchable_blacklist_row_shape_parity(self):
        for db in (self.db, self.fake):
            allowed_id = db.add_request(
                "WS Artist", "Allowed Album", "request",
                mb_release_id="ws-allowed")
            blocked_id = db.add_request(
                "WS Artist", "Blocked Album", "request",
                mb_release_id="ws-blocked")
            from lib.pipeline_db import SearchPlanItemInput
            for request_id in (allowed_id, blocked_id):
                db.create_successful_search_plan(
                    request_id=request_id,
                    generator_id="g-parity",
                    items=[SearchPlanItemInput(
                        ordinal=0, strategy="default", query="Q")],
                )

        real_rows = self.db.get_wanted_searchable(
            "g-parity", title_blacklist=("blocked",))
        fake_rows = self.fake.get_wanted_searchable(
            "g-parity", title_blacklist=("blocked",))

        self.assertEqual(len(real_rows), 1)
        self.assertEqual(len(fake_rows), 1)
        self._assert_keyset_parity(
            self, real_rows, fake_rows,
            "get_wanted_searchable(blacklist)")

    # --- get_search_summaries_for_requests (#523) ----------------------------

    def _seed_search_summary_request(
        self, db: Any, *, mb_release_id: str,
    ) -> int:
        rid = db.add_request(
            "Summary Artist", "Summary Album", "request",
            mb_release_id=mb_release_id)
        db.log_search(
            rid, query="q1", outcome="found", result_count=5, elapsed_s=1.0)
        return rid

    def test_get_search_summaries_for_requests_keyset_parity(self):
        ids: dict[int, int] = {}
        for db in (self.db, self.fake):
            ids[id(db)] = self._seed_search_summary_request(
                db, mb_release_id="summary-parity-1")

        real_map = self.db.get_search_summaries_for_requests(
            [ids[id(self.db)]])
        fake_map = self.fake.get_search_summaries_for_requests(
            [ids[id(self.fake)]])
        real_rows = list(real_map.values())
        fake_rows = list(fake_map.values())
        self.assertTrue(
            real_rows, "seeding produced no rows on real PG — "
            "get_search_summaries_for_requests parity would pass vacuously")
        self.assertTrue(
            fake_rows, "seeding produced no rows on FakePipelineDB — "
            "get_search_summaries_for_requests parity would pass vacuously")
        self._assert_keyset_parity(
            self, real_rows, fake_rows, "get_search_summaries_for_requests")

    def test_get_search_summaries_for_requests_empty_input_contract(self):
        # Contract: an empty id list short-circuits to {} without a query
        # on both backends -- not a keyset check, the {} equality IS the
        # assertion.
        self.assertEqual(self.db.get_search_summaries_for_requests([]), {})
        self.assertEqual(self.fake.get_search_summaries_for_requests([]), {})

    # --- get_recent_search_log_for_requests (#523) ---------------------------

    def test_get_recent_search_log_for_requests_keyset_parity(self):
        ids: dict[int, int] = {}
        for db in (self.db, self.fake):
            rid = db.add_request(
                "RecentLog Artist", "RecentLog Album", "request",
                mb_release_id="recentlog-parity-1")
            db.log_search(
                rid, query="q1", outcome="found", result_count=5,
                elapsed_s=1.0)
            ids[id(db)] = rid

        real_map = self.db.get_recent_search_log_for_requests(
            [ids[id(self.db)]], per_request_limit=5)
        fake_map = self.fake.get_recent_search_log_for_requests(
            [ids[id(self.fake)]], per_request_limit=5)
        real_rows = [row for rows in real_map.values() for row in rows]
        fake_rows = [row for rows in fake_map.values() for row in rows]
        self.assertTrue(
            real_rows, "seeding produced no rows on real PG — "
            "get_recent_search_log_for_requests parity would pass vacuously")
        self.assertTrue(
            fake_rows, "seeding produced no rows on FakePipelineDB — "
            "get_recent_search_log_for_requests parity would pass vacuously")
        self._assert_keyset_parity(
            self, real_rows, fake_rows, "get_recent_search_log_for_requests")

    # --- get_youtube_album_mapping (#523, tri-state) -------------------------

    @staticmethod
    def _youtube_mapping_row(**overrides: Any) -> PersistedYoutubeRow:
        fields: dict[str, Any] = {
            "yt_browse_id": "MPREb_parity",
            "yt_audio_playlist_id": "OLAK5uy_parity",
            "yt_url": "https://music.youtube.com/playlist?list=OLAK5uy_parity",
            "yt_year": 2020,
            "yt_track_count": 10,
            "album_title": "Parity Album",
            "album_artist": "Parity Artist",
            "yt_tracks": [
                PersistedTrack(
                    title="Track 1", video_id="v1", length_seconds=200,
                    track_number=1, disc_number=1,
                    artists=[{"name": "Artist"}],
                ),
            ],
            "distances": [
                PersistedDistance(mbid="mb-1", distance=0.05),
            ],
        }
        fields.update(overrides)
        return PersistedYoutubeRow(**fields)

    def test_get_youtube_album_mapping_keyset_parity(self):
        for db in (self.db, self.fake):
            db.upsert_youtube_album_mapping(
                "rg-parity", "mb", [self._youtube_mapping_row()])

        real_rows = self.db.get_youtube_album_mapping("rg-parity", "mb")
        fake_rows = self.fake.get_youtube_album_mapping("rg-parity", "mb")
        self.assertTrue(
            real_rows, "seeding produced no rows on real PG — "
            "get_youtube_album_mapping parity would pass vacuously")
        self.assertTrue(
            fake_rows, "seeding produced no rows on FakePipelineDB — "
            "get_youtube_album_mapping parity would pass vacuously")
        assert real_rows is not None and fake_rows is not None
        self._assert_keyset_parity(
            self, real_rows, fake_rows, "get_youtube_album_mapping")

    def test_get_youtube_album_mapping_resolved_empty_branch_parity(self):
        # Contract: upserting an empty matrix stamps the empty-resolution
        # marker -- both backends return [] (cache HIT), never None.
        for db in (self.db, self.fake):
            db.upsert_youtube_album_mapping("rg-parity-empty", "mb", [])

        self.assertEqual(
            self.db.get_youtube_album_mapping("rg-parity-empty", "mb"), [])
        self.assertEqual(
            self.fake.get_youtube_album_mapping("rg-parity-empty", "mb"), [])

    def test_get_youtube_album_mapping_never_resolved_branch_parity(self):
        # Contract: an unknown (rg, source) pair is a cache MISS -- None
        # on both backends, distinct from the resolved-empty [] above.
        self.assertIsNone(
            self.db.get_youtube_album_mapping("rg-never-resolved", "mb"))
        self.assertIsNone(
            self.fake.get_youtube_album_mapping("rg-never-resolved", "mb"))


@requires_postgres
class TestReadProjectionRegistryParity(unittest.TestCase):
    """#546 W1 — registry-driven read-projection parity gate.

    ``TestReadProjectionParity`` (above) is the hand-written half: one
    ``test_*`` method per covered projection. This class is the
    self-enforcing half — it iterates ``PARITY_REGISTRY`` from
    ``tests/read_projection_registry.py`` and runs every registered
    seeder against a fresh real ``PipelineDB`` and a fresh
    ``FakePipelineDB``, asserting key-set parity for each. Adding a
    seeder to the registry is all it takes to gate a new mirror; the
    companion audit (``tests/test_read_projection_audit.py``) forces
    every ``FakePipelineDB`` read method into the registry, the
    hand-written coverage, or the allowlist.

    Reuses ``TestReadProjectionParity._assert_keyset_parity`` — a
    staticmethod whose first argument is the ``TestCase`` instance, so
    cross-class reuse is safe.
    """

    def test_every_registered_mirror_has_keyset_parity(self):
        from tests.fakes import FakePipelineDB
        from tests.read_projection_registry import PARITY_REGISTRY

        for method_name, seeder in sorted(PARITY_REGISTRY.items()):
            with self.subTest(method=method_name):
                real_db = make_db()
                try:
                    fake_db = FakePipelineDB()
                    real_rows = seeder(real_db)
                    fake_rows = seeder(fake_db)
                    self.assertTrue(
                        real_rows,
                        f"{method_name}: seeder produced no rows on real "
                        f"PG — parity would pass vacuously; fix the seeder "
                        f"in tests/read_projection_registry.py")
                    self.assertTrue(
                        fake_rows,
                        f"{method_name}: seeder produced no rows on "
                        f"FakePipelineDB — parity would pass vacuously; fix "
                        f"the seeder in tests/read_projection_registry.py")
                    TestReadProjectionParity._assert_keyset_parity(
                        self, real_rows, fake_rows, method_name)
                finally:
                    real_db.close()


@requires_postgres
class TestEphemeralPostgresClockFrame(unittest.TestCase):
    """The disposable cluster's session timezone is pinned to UTC.

    ``lib/ephemeral_postgres.py`` passes ``-c timezone=UTC`` because
    initdb inherits the HOST timezone, under which
    ``date_trunc('day', NOW())`` buckets in local time while the fakes'
    dense-bucket mirrors truncate in UTC — the value-parity gate below
    then fails only inside two one-hour UTC wall-clock windows per day
    (reproduced on pristine main 2026-09-01 00:04 UTC). Without this pin,
    removing that option goes green everywhere except inside those
    windows.
    """

    def test_session_timezone_is_utc(self):
        db = make_db()
        try:
            cur = db._execute("SELECT current_setting('TimeZone') AS tz")
            self.assertEqual(cur.fetchone()["tz"], "UTC")
        finally:
            db.close()


class TestReadProjectionValueParity(unittest.TestCase):
    """#1278 item 7 — registry-driven read-projection VALUE parity gate.

    The key gate above compares KEY SETS. The mirrors registered here are
    the ones whose VALUES are decided by SQL the fake reimplements —
    whether that key gate EXCUSES them via ``ALLOWLIST`` (a percentile, a
    computed metric dict) or merely HAND-COVERS their key set with a test
    in this file (a rollup view's aggregate, a ``DISTINCT ON`` collapse, a
    view join that decides membership, a table's column DEFAULTs). In
    every case the key set was not the risk; what the SQL computes is.
    Extracting that SQL into shared Python would be the wrong fix (the
    database is the authority on its own aggregation), so this gate takes
    the other axis instead: run the same seeder on both backends and
    compare every non-excluded field by VALUE.

    Each held-out field carries its own rationale in the registry, so a
    field can only leave the comparison with a written reason attached —
    and every declared exclusion must actually be REACHED, so one that
    quietly stops excluding anything fails instead of rotting.
    """

    def test_every_value_registered_mirror_agrees_on_values(self):
        from lib import pipeline_db
        from tests.fakes import FakePipelineDB
        from tests.read_projection_registry import (
            VALUE_PARITY_REGISTRY,
            compare_projection_values,
        )

        for method_name, entry in sorted(VALUE_PARITY_REGISTRY.items()):
            with self.subTest(method=method_name):
                real_db = make_db()
                try:
                    fake_db = FakePipelineDB()
                    # Both sides must genuinely be the two DIFFERENT
                    # backends. Swapping make_db() for a second
                    # FakePipelineDB passed this whole gate silently
                    # (#1278 item 7 runner survivor S1) — the fake agreeing
                    # with itself is not parity.
                    self.assertIsInstance(real_db, pipeline_db.PipelineDB)
                    self.assertNotIsInstance(real_db, FakePipelineDB)
                    self.assertIsInstance(fake_db, FakePipelineDB)
                    real_rows = entry.seeder(real_db)
                    fake_rows = entry.seeder(fake_db)
                    self.assertTrue(
                        real_rows,
                        f"{method_name}: seeder produced no rows on real PG "
                        f"— value parity would pass vacuously; fix the "
                        f"seeder in tests/read_projection_registry.py")
                    self.assertTrue(
                        fake_rows,
                        f"{method_name}: seeder produced no rows on "
                        f"FakePipelineDB — value parity would pass "
                        f"vacuously; fix the seeder in "
                        f"tests/read_projection_registry.py")
                    result = compare_projection_values(
                        real_rows, fake_rows,
                        excluded=entry.excluded_paths)
                    self.assertGreaterEqual(
                        result.substantive_leaves, 1,
                        f"{method_name}: compared "
                        f"{result.compared_leaves} field(s) but not one of "
                        f"them held a non-null, non-empty, non-zero value "
                        f"— a payload of nulls and zeros agrees for free. "
                        f"Seed values that DISTINGUISH.")
                    dead = sorted(
                        entry.excluded_paths - result.excluded_hits)
                    self.assertEqual(
                        dead, [],
                        f"{method_name}: these declared exclusions were "
                        f"never reached, so they excuse nothing — the field "
                        f"was renamed, or the seeder stopped producing it. "
                        f"Delete them or fix the path: {dead}")
                    self.assertEqual(
                        result.mismatches, (),
                        f"{method_name}: real PG and FakePipelineDB "
                        f"computed different values from identically "
                        f"seeded state. Fix the fake (production SQL is "
                        f"the authority on its own aggregation), or — if "
                        f"the two are allowed to differ — add a "
                        f"ValueExclusion with its rationale in "
                        f"tests/read_projection_registry.py:\n  - "
                        + "\n  - ".join(result.mismatches))
                finally:
                    real_db.close()


@requires_postgres
class TestMergeRekeyWrite(unittest.TestCase):
    """Real-PostgreSQL transcript for the MusicBrainz merge rekey (#1059).

    Rule A: the identity this write moves must be readable back — through
    ``get_request`` AND through a raw SELECT on the column itself, because the
    whole point of the write is that ``album_requests.mb_release_id`` really
    changed in PostgreSQL, not just in a Python dict.

    The write moves TWO tables, so the round trip covers both. Evidence is
    content-addressed by ``(mb_release_id, snapshot_fingerprint)``, and every
    consumer gates on the request's identity matching the evidence row's:
    ``backfill_current_evidence_from_album_info`` drops a mismatched HAVE row's
    verified-lossless proof, ``_refresh_current_evidence_after_import`` returns
    ``identity_mismatch``, and the Recents projection nulls every
    ``_evidence_*`` / ``_current_evidence_*`` field. Rows left behind at the
    merged-away id are stranded, so a partial move is not a partial success —
    it is the bug.
    """

    MERGED = "6b209cc5-62b0-4ef7-9336-c2dbd876301a"
    SURVIVOR = "9b59f78b-3ca6-41e1-8025-6ed4bcfad4e4"
    ENQUEUED = "2026-08-11T00:00:00+00:00"

    def setUp(self) -> None:
        self.db = make_db()
        self.addCleanup(self.db.close)
        self.request_id = self.db.add_request(
            mb_release_id=self.MERGED,
            artist_name="DICE",
            album_title="Midnight Zoo",
            source="request",
        )
        self.assertTrue(self.db.set_downloading(
            self.request_id,
            json.dumps({
                "filetype": "flac",
                "enqueued_at": self.ENQUEUED,
                "last_progress_at": self.ENQUEUED,
                "files": [],
            }),
            expected_status="wanted",
        ))
        handoff = self.db.handoff_automation_import(
            request_id=self.request_id,
            expected_enqueued_at=self.ENQUEUED,
            canonical_path="/processing/albums/dice-midnight-zoo",
            message="merge rekey fixture",
        )
        self.assertTrue(handoff.committed)
        assert handoff.job is not None
        self.job_id = handoff.job.id

    def _stored_release_id(self) -> str | None:
        cur = self.db._execute(
            "SELECT mb_release_id FROM album_requests WHERE id = %s",
            (self.request_id,),
        )
        row = cur.fetchone()
        assert row is not None
        value = row["mb_release_id"]
        return None if value is None else str(value)

    def _seed_evidence(
        self,
        release_id: str,
        *,
        relative_path: str,
        verified: bool = False,
        link_as_current: bool = False,
    ) -> tuple[int, str]:
        """Persist one evidence row; return ``(id, snapshot_fingerprint)``."""
        evidence = make_album_quality_evidence(
            mb_release_id=release_id,
            source_path=f"/library/{release_id}",
            files=[AlbumQualityEvidenceFile(
                relative_path=relative_path,
                size_bytes=4242,
                mtime_ns=1_700_000_000_000_000_000,
                extension="flac",
                container="flac",
                codec="flac",
            )],
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=900,
                avg_bitrate_kbps=950,
                median_bitrate_kbps=940,
                format="FLAC",
            ),
            codec="flac",
            container="flac",
            storage_format="FLAC",
            verified_lossless_proof=(
                VerifiedLosslessProof(
                    provenance="measured",
                    source="flac",
                    classifier="spectral_verified_lossless",
                    detail="genuine",
                ) if verified else None
            ),
        )
        self.db.upsert_album_quality_evidence(evidence)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        if link_as_current:
            self.assertTrue(self.db.set_request_current_evidence(
                self.request_id, stored.id,
            ))
        return stored.id, evidence.snapshot_fingerprint

    def _evidence_release_id(self, evidence_id: int) -> str | None:
        cur = self.db._execute(
            "SELECT mb_release_id FROM album_quality_evidence WHERE id = %s",
            (evidence_id,),
        )
        row = cur.fetchone()
        return None if row is None else str(row["mb_release_id"])

    def _evidence_ids_at(self, release_id: str) -> set[int]:
        cur = self.db._execute(
            "SELECT id FROM album_quality_evidence WHERE mb_release_id = %s",
            (release_id,),
        )
        return {int(row["id"]) for row in cur.fetchall()}

    def test_rekey_round_trip_preserves_the_survivor_identity(self):
        applied = self.db.update_request_release_for_merge(
            self.request_id,
            old_release_id=self.MERGED,
            new_release_id=self.SURVIVOR,
            expected_import_job_id=self.job_id,
        )

        self.assertTrue(applied)
        cur = self.db._execute(
            "SELECT mb_release_id FROM album_requests WHERE id = %s",
            (self.request_id,),
        )
        stored = cur.fetchone()
        assert stored is not None
        self.assertEqual(stored["mb_release_id"], self.SURVIVOR)
        row = self.db.get_request(self.request_id)
        assert row is not None
        self.assertEqual(row["mb_release_id"], self.SURVIVOR)
        # The rekey moves identity and nothing else: the owner fence, the
        # processing state and the immutable download witness all stand.
        self.assertEqual(row["status"], "processing")
        self.assertEqual(row["active_automation_import_job_id"], self.job_id)
        self.assertIsNotNone(row["active_download_state"])

    def test_every_evidence_row_moves_with_the_request_identity(self):
        """The HAVE row and every candidate row land at the survivor."""
        current_id, current_fingerprint = self._seed_evidence(
            self.MERGED, relative_path="01 Installed.flac",
            verified=True, link_as_current=True,
        )
        candidate_id, candidate_fingerprint = self._seed_evidence(
            self.MERGED, relative_path="01 Candidate.flac",
        )
        # A neighbouring pressing's evidence is untouched by any of this.
        foreign_id, _ = self._seed_evidence(
            "3333cccc-4444-4444-4444-555555555555",
            relative_path="01 Neighbour.flac",
        )

        self.assertTrue(self.db.update_request_release_for_merge(
            self.request_id,
            old_release_id=self.MERGED,
            new_release_id=self.SURVIVOR,
            expected_import_job_id=self.job_id,
        ))

        self.assertEqual(self._evidence_ids_at(self.MERGED), set())
        self.assertEqual(
            self._evidence_ids_at(self.SURVIVOR), {current_id, candidate_id},
        )
        self.assertEqual(
            self._evidence_release_id(foreign_id),
            "3333cccc-4444-4444-4444-555555555555",
        )
        # Content addressing is preserved: same surrogate id, same fingerprint,
        # new release. The request's FK therefore still resolves, and now
        # resolves to a row whose identity MATCHES — which is the exact
        # predicate every evidence consumer gates on.
        row = self.db.get_request(self.request_id)
        assert row is not None
        self.assertEqual(row["current_evidence_id"], current_id)
        for evidence_id, fingerprint in (
            (current_id, current_fingerprint),
            (candidate_id, candidate_fingerprint),
        ):
            moved = self.db.load_album_quality_evidence_by_id(evidence_id)
            assert moved is not None
            self.assertEqual(moved.mb_release_id, self.SURVIVOR)
            self.assertEqual(moved.snapshot_fingerprint, fingerprint)
            self.assertIsNotNone(self.db.find_album_quality_evidence(
                mb_release_id=self.SURVIVOR,
                snapshot_fingerprint=fingerprint,
            ))
        # And the proof itself survived the move, byte for byte.
        installed = self.db.load_album_quality_evidence_by_id(current_id)
        assert installed is not None and installed.verified_lossless_proof
        self.assertEqual(
            installed.verified_lossless_proof.classifier,
            "spectral_verified_lossless",
        )

    def test_a_fingerprint_collision_at_the_survivor_writes_nothing(self):
        """UNIQUE(mb_release_id, snapshot_fingerprint) fails the WHOLE rekey.

        Two rows describing the same bytes at two ids is two independent
        measurements; choosing a winner is an unowned quality decision, so
        the request row must not move either.
        """
        moving_id, fingerprint = self._seed_evidence(
            self.MERGED, relative_path="01 Installed.flac",
            verified=True, link_as_current=True,
        )
        # The survivor already carries the same snapshot — the collision.
        colliding_id, colliding_fingerprint = self._seed_evidence(
            self.SURVIVOR, relative_path="01 Installed.flac",
        )
        self.assertEqual(colliding_fingerprint, fingerprint)

        self.assertFalse(self.db.update_request_release_for_merge(
            self.request_id,
            old_release_id=self.MERGED,
            new_release_id=self.SURVIVOR,
            expected_import_job_id=self.job_id,
        ))

        # Nothing moved: not the row, not the evidence, not the FK.
        self.assertEqual(self._stored_release_id(), self.MERGED)
        self.assertEqual(self._evidence_release_id(moving_id), self.MERGED)
        self.assertEqual(self._evidence_release_id(colliding_id), self.SURVIVOR)
        row = self.db.get_request(self.request_id)
        assert row is not None
        self.assertEqual(row["current_evidence_id"], moving_id)
        self.assertEqual(row["status"], "processing")
        self.assertEqual(row["active_automation_import_job_id"], self.job_id)
        # The session is usable afterwards — a rolled-back UniqueViolation
        # must not leave the connection in an aborted transaction, and the
        # refusal must be idempotent rather than a one-shot poison.
        later_id, _ = self._seed_evidence(
            "4444dddd-5555-4555-8555-666666666666",
            relative_path="01 Later.flac",
        )
        self.assertEqual(
            self._evidence_release_id(later_id),
            "4444dddd-5555-4555-8555-666666666666",
        )
        self.assertFalse(self.db.update_request_release_for_merge(
            self.request_id,
            old_release_id=self.MERGED,
            new_release_id=self.SURVIVOR,
            expected_import_job_id=self.job_id,
        ))
        self.assertEqual(self._stored_release_id(), self.MERGED)

    def test_a_refused_request_cas_never_touches_evidence(self):
        """The request CAS is checked first; a lost fence writes nothing."""
        evidence_id, _ = self._seed_evidence(
            self.MERGED, relative_path="01 Installed.flac",
            link_as_current=True,
        )

        self.assertFalse(self.db.update_request_release_for_merge(
            self.request_id,
            old_release_id=self.MERGED,
            new_release_id=self.SURVIVOR,
            expected_import_job_id=self.job_id + 1000,
        ))

        self.assertEqual(self._stored_release_id(), self.MERGED)
        self.assertEqual(self._evidence_release_id(evidence_id), self.MERGED)
        self.assertEqual(self._evidence_ids_at(self.SURVIVOR), set())

    def test_a_stale_identity_or_foreign_owner_writes_nothing(self):
        cases = (
            ("stale old id", "0" * 8 + "-0000-0000-0000-" + "0" * 12, self.job_id),
            ("foreign owner", self.MERGED, self.job_id + 1000),
        )
        for label, old_release_id, job_id in cases:
            with self.subTest(case=label):
                self.assertFalse(self.db.update_request_release_for_merge(
                    self.request_id,
                    old_release_id=old_release_id,
                    new_release_id=self.SURVIVOR,
                    expected_import_job_id=job_id,
                ))
                self.assertEqual(self._stored_release_id(), self.MERGED)

    def test_a_survivor_another_request_holds_fails_closed(self):
        """UNIQUE(mb_release_id) is reported, never raised or merged."""
        other = self.db.add_request(
            mb_release_id=self.SURVIVOR,
            artist_name="DICE",
            album_title="Midnight Zoo (other pressing request)",
            source="request",
        )

        self.assertFalse(self.db.update_request_release_for_merge(
            self.request_id,
            old_release_id=self.MERGED,
            new_release_id=self.SURVIVOR,
            expected_import_job_id=self.job_id,
        ))
        self.assertEqual(self._stored_release_id(), self.MERGED)
        survivor_row = self.db.get_request(other)
        assert survivor_row is not None
        self.assertEqual(survivor_row["mb_release_id"], self.SURVIVOR)

    def test_a_request_without_its_processing_owner_is_refused(self):
        """A row with no attached automation owner is never rekeyed."""
        idle = self.db.add_request(
            mb_release_id="1f1f1f1f-2222-3333-4444-555555555555",
            artist_name="Idle",
            album_title="Wanted",
            source="request",
        )

        self.assertFalse(self.db.update_request_release_for_merge(
            idle,
            old_release_id="1f1f1f1f-2222-3333-4444-555555555555",
            new_release_id=self.SURVIVOR,
            expected_import_job_id=self.job_id,
        ))
        row = self.db.get_request(idle)
        assert row is not None
        self.assertEqual(
            row["mb_release_id"], "1f1f1f1f-2222-3333-4444-555555555555",
        )

    def test_degenerate_release_ids_are_rejected_before_any_sql(self):
        for label, old_id, new_id in (
            ("same id", self.MERGED, self.MERGED),
            ("blank old", "", self.SURVIVOR),
            ("blank new", self.MERGED, ""),
        ):
            with self.subTest(case=label), self.assertRaises(ValueError):
                self.db.update_request_release_for_merge(
                    self.request_id,
                    old_release_id=old_id,
                    new_release_id=new_id,
                    expected_import_job_id=self.job_id,
                )
        self.assertEqual(self._stored_release_id(), self.MERGED)

    def _collision(self):
        return self.db.merge_rekey_collision(
            self.request_id,
            old_release_id=self.MERGED,
            new_release_id=self.SURVIVOR,
        )

    def test_a_clear_survivor_reports_no_collision_and_the_write_lands(self):
        """The pre-check and the write must agree, in real PostgreSQL (#1080).

        The seam reads this BEFORE it retags the shared Beets library, so a
        pre-check that says "clear" while the write refuses would be worse
        than no pre-check at all: it would authorize the library mutation that
        creates the split identity.
        """
        self._seed_evidence(self.MERGED, relative_path="01 Installed.flac")

        collision = self._collision()

        self.assertFalse(collision.blocked)
        self.assertIsNone(collision.rival_request_id)
        self.assertEqual(collision.colliding_fingerprints, ())
        self.assertTrue(self.db.update_request_release_for_merge(
            self.request_id,
            old_release_id=self.MERGED,
            new_release_id=self.SURVIVOR,
            expected_import_job_id=self.job_id,
        ))
        self.assertEqual(self._stored_release_id(), self.SURVIVOR)

    def test_a_rival_request_at_the_survivor_is_reported_before_anything_moves(
        self,
    ):
        """UNIQUE(mb_release_id), knowable by a read."""
        other = self.db.add_request(
            mb_release_id=self.SURVIVOR,
            artist_name="DICE",
            album_title="Midnight Zoo (other pressing request)",
            source="request",
        )

        collision = self._collision()

        self.assertTrue(collision.blocked)
        self.assertEqual(collision.rival_request_id, other)
        self.assertIn(str(other), collision.detail())
        # And the write really does refuse that world.
        self.assertFalse(self.db.update_request_release_for_merge(
            self.request_id,
            old_release_id=self.MERGED,
            new_release_id=self.SURVIVOR,
            expected_import_job_id=self.job_id,
        ))
        self.assertEqual(self._stored_release_id(), self.MERGED)

    def test_a_frozen_replaced_rival_still_blocks_the_rekey(self):
        """``album_requests.mb_release_id`` is UNIQUE with no status filter.

        A frozen audit ancestor occupies the survivor exactly as a live
        request does, so the pre-check must not filter by status — filtering
        would report "clear" for a world the write refuses, which is the one
        answer that authorizes a library mutation it should not.
        """
        rival = self.db.add_request(
            mb_release_id=self.SURVIVOR,
            artist_name="DICE",
            album_title="Midnight Zoo (ancestor)",
            source="request",
        )
        self.db.supersede_request_mbid(
            rival,
            new_mb_release_id="7777aaaa-8888-4888-8888-999999999999",
            new_mb_release_group_id=None,
            new_mb_artist_id=None,
            new_artist_name="DICE",
            new_album_title="Midnight Zoo",
            new_year=None,
            new_country=None,
            new_tracks=[],
        )
        frozen = self.db.get_request(rival)
        assert frozen is not None
        self.assertEqual(frozen["status"], "replaced")
        self.assertEqual(frozen["mb_release_id"], self.SURVIVOR)

        collision = self._collision()

        self.assertTrue(collision.blocked)
        self.assertEqual(collision.rival_request_id, rival)
        self.assertFalse(self.db.update_request_release_for_merge(
            self.request_id,
            old_release_id=self.MERGED,
            new_release_id=self.SURVIVOR,
            expected_import_job_id=self.job_id,
        ))
        self.assertEqual(self._stored_release_id(), self.MERGED)

    def test_a_fingerprint_already_at_the_survivor_is_reported(self):
        """UNIQUE(mb_release_id, snapshot_fingerprint), knowable by a read."""
        _, fingerprint = self._seed_evidence(
            self.MERGED, relative_path="01 Installed.flac",
        )
        _, colliding = self._seed_evidence(
            self.SURVIVOR, relative_path="01 Installed.flac",
        )
        self.assertEqual(colliding, fingerprint)
        # A fingerprint that exists only on ONE side is not a collision.
        self._seed_evidence(self.MERGED, relative_path="01 Candidate.flac")
        self._seed_evidence(self.SURVIVOR, relative_path="01 Neighbour.flac")

        collision = self._collision()

        self.assertTrue(collision.blocked)
        self.assertIsNone(collision.rival_request_id)
        self.assertEqual(collision.colliding_fingerprints, (fingerprint,))
        self.assertFalse(self.db.update_request_release_for_merge(
            self.request_id,
            old_release_id=self.MERGED,
            new_release_id=self.SURVIVOR,
            expected_import_job_id=self.job_id,
        ))
        self.assertEqual(self._stored_release_id(), self.MERGED)


@requires_postgres
class TestMergeRekeyUnderAForceClaim(unittest.TestCase):
    """The force lane's arm of the merge-rekey fence, in real PostgreSQL (#1080).

    Force import is the same path as any other import with the Beets distance
    overridden, so it follows a MusicBrainz merge through the same seam. It
    cannot hold the ``processing`` owner pointer — migration 066 reserves that
    for one active ``automation_import`` job — so the identity write's fence
    admits the OTHER production claim instead:
    ``claim_force_import_job_under_lock``'s exact request predicate plus a
    ``running`` force job on this request.

    Rule A: every case here asserts the persisted column, not a return value.
    """

    MERGED = "6b209cc5-62b0-4ef7-9336-c2dbd876301a"
    SURVIVOR = "9b59f78b-3ca6-41e1-8025-6ed4bcfad4e4"

    def setUp(self) -> None:
        self.db = make_db()
        self.addCleanup(self.db.close)
        self.request_id = self.db.add_request(
            mb_release_id=self.MERGED,
            artist_name="DICE",
            album_title="Midnight Zoo",
            source="request",
        )
        self.job_id = self._queue_force_job()

    def _queue_force_job(self) -> int:
        from lib.import_queue import force_import_dedupe_key

        job = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.request_id,
            dedupe_key=force_import_dedupe_key(self.request_id),
            payload=force_import_payload(
                download_log_id=1,
                failed_path="/quarantine/dice",
            ),
        )
        self.assertIsNotNone(self.db.mark_import_job_preview_importable(
            job.id, preview_result={}, message="force preview ready",
        ))
        return job.id

    def _claim(self) -> None:
        claimed = self.db.claim_force_import_job_under_lock(
            self.job_id, request_id=self.request_id, worker_id="pg-fence-test",
        )
        assert claimed is not None and claimed.status == "running"

    def _stored_release_id(self) -> str | None:
        row = self.db.get_request(self.request_id)
        assert row is not None
        value = row["mb_release_id"]
        return None if value is None else str(value)

    def test_a_running_force_claim_rekeys_the_wanted_request(self) -> None:
        self._claim()

        self.assertTrue(self.db.update_request_release_for_merge(
            self.request_id,
            old_release_id=self.MERGED,
            new_release_id=self.SURVIVOR,
            expected_import_job_id=self.job_id,
        ))

        self.assertEqual(self._stored_release_id(), self.SURVIVOR)
        row = self.db.get_request(self.request_id)
        assert row is not None
        # The lifecycle is untouched: a force import does not own the request,
        # it only borrows the identity long enough to correct it.
        self.assertEqual(row["status"], "wanted")
        self.assertIsNone(row["active_automation_import_job_id"])

    def test_an_unclaimed_force_job_writes_nothing(self) -> None:
        """``queued`` is not a claim. Only the worker that took it may rekey."""
        self.assertFalse(self.db.update_request_release_for_merge(
            self.request_id,
            old_release_id=self.MERGED,
            new_release_id=self.SURVIVOR,
            expected_import_job_id=self.job_id,
        ))
        self.assertEqual(self._stored_release_id(), self.MERGED)

    def test_a_force_job_naming_another_request_writes_nothing(self) -> None:
        other = self.db.add_request(
            mb_release_id="1f1f1f1f-2222-3333-4444-555555555555",
            artist_name="Other",
            album_title="Other",
            source="request",
        )
        self._claim()

        self.assertFalse(self.db.update_request_release_for_merge(
            other,
            old_release_id="1f1f1f1f-2222-3333-4444-555555555555",
            new_release_id=self.SURVIVOR,
            expected_import_job_id=self.job_id,
        ))

        row = self.db.get_request(other)
        assert row is not None
        self.assertEqual(
            row["mb_release_id"], "1f1f1f1f-2222-3333-4444-555555555555",
        )

    def test_a_running_non_force_job_never_authorizes_the_rekey(self) -> None:
        """The force arm's ``job_type`` term, on the write that actually writes.

        ``merge_rekey_claim_holds`` refuses a YouTube rescue in Python before
        the write is attempted — but its own docstring says that is the
        PRE-check, not the authority, and this SQL "is what actually writes".
        Drop ``j.job_type = 'force_import'`` from the EXISTS and the arm reads
        "any import job with this id, running, on this request", so a running
        YouTube rescue — which holds neither production claim — would
        authorize an ``mb_release_id`` rewrite.

        Rule A: asserted on the persisted column, from a job claimed through
        the same ``claim_import_job_candidate`` the importer uses for
        non-request-scoped work.
        """
        from lib.import_queue import (
            IMPORT_JOB_YOUTUBE,
            youtube_import_dedupe_key,
            youtube_import_payload,
        )

        rescue = self.db.enqueue_import_job(
            IMPORT_JOB_YOUTUBE,
            request_id=self.request_id,
            dedupe_key=youtube_import_dedupe_key(901),
            payload=youtube_import_payload(
                staged_path="/Incoming/auto-import/dice",
                request_id=self.request_id,
                browse_id="MPREb_dice",
                download_log_id=901,
            ),
        )
        self.assertIsNotNone(self.db.mark_import_job_preview_importable(
            rescue.id, preview_result={}, message="rescue preview ready",
        ))
        running = self.db.claim_import_job_candidate(
            rescue.id, worker_id="pg-fence-test",
        )
        assert running is not None
        # Everything the widened arm would match on: same id, same request,
        # running. Only the job type is wrong.
        self.assertEqual(running.status, "running")
        self.assertEqual(running.request_id, self.request_id)
        self.assertNotEqual(running.job_type, IMPORT_JOB_FORCE)
        row = self.db.get_request(self.request_id)
        assert row is not None
        self.assertEqual(row["status"], "wanted")
        self.assertIsNone(row["active_automation_import_job_id"])

        self.assertFalse(self.db.update_request_release_for_merge(
            self.request_id,
            old_release_id=self.MERGED,
            new_release_id=self.SURVIVOR,
            expected_import_job_id=rescue.id,
        ))

        self.assertEqual(self._stored_release_id(), self.MERGED)

    def test_a_replaced_row_is_never_rekeyed_by_a_force_claim(self) -> None:
        """``replaced`` rows are frozen audit ancestors (invariant: pipeline-db).

        The automation arm excluded them by construction — a frozen row is
        never ``processing``. The force arm has to say so explicitly, because
        every other non-``processing`` status IS a legal force target.
        """
        self._claim()
        self.db.supersede_request_mbid(
            self.request_id,
            new_mb_release_id=self.SURVIVOR,
            new_mb_release_group_id=None,
            new_mb_artist_id=None,
            new_artist_name="DICE",
            new_album_title="Midnight Zoo",
            new_year=None,
            new_country=None,
            new_tracks=[],
        )
        frozen = self.db.get_request(self.request_id)
        assert frozen is not None
        self.assertEqual(frozen["status"], "replaced")
        frozen_release_id = str(frozen["mb_release_id"])

        self.assertFalse(self.db.update_request_release_for_merge(
            self.request_id,
            old_release_id=frozen_release_id,
            new_release_id="2a2a2a2a-3333-4444-8555-666666666666",
            expected_import_job_id=self.job_id,
        ))

        still = self.db.get_request(self.request_id)
        assert still is not None
        self.assertEqual(still["mb_release_id"], frozen_release_id)
        self.assertEqual(still["status"], "replaced")


@requires_postgres
class TestMergeRekeyUnderOperatorClaim(unittest.TestCase):
    """The operator arm of the merge-rekey fence, in real PostgreSQL (#1089).

    The operator button never holds an import claim — it acts directly on an
    ``imported`` row nothing else currently owns. Every refusal world here
    writes NOTHING to either the request row or its evidence (Rule A: both
    are asserted, not just the boolean return).

    Authority: "really we need to re-key mbid and beets don't we so they go
    away. we could surface these here and have a button which re-keys with
    the current machinery we've built couldn't we?" —
    https://github.com/abl030/cratedigger/issues/1089#issuecomment-5274933957
    """

    MERGED = "6b209cc5-62b0-4ef7-9336-c2dbd876301a"
    SURVIVOR = "9b59f78b-3ca6-41e1-8025-6ed4bcfad4e4"

    def setUp(self) -> None:
        self.db = make_db()
        self.addCleanup(self.db.close)
        self.request_id = self.db.add_request(
            mb_release_id=self.MERGED,
            artist_name="DICE",
            album_title="Midnight Zoo",
            source="request",
            status="imported",
        )

    def _stored_release_id(self) -> str | None:
        row = self.db.get_request(self.request_id)
        assert row is not None
        value = row["mb_release_id"]
        return None if value is None else str(value)

    def _rekey(self, *, expected_import_job_id: int | None = None) -> bool:
        return self.db.update_request_release_for_merge(
            self.request_id,
            old_release_id=self.MERGED,
            new_release_id=self.SURVIVOR,
            expected_import_job_id=expected_import_job_id,
        )

    def _evidence_release_id(self, evidence_id: int) -> str | None:
        cur = self.db._execute(
            "SELECT mb_release_id FROM album_quality_evidence WHERE id = %s",
            (evidence_id,),
        )
        row = cur.fetchone()
        return None if row is None else str(row["mb_release_id"])

    def _seed_evidence(self, release_id: str) -> int:
        evidence = make_album_quality_evidence(
            mb_release_id=release_id,
            source_path=f"/library/{release_id}",
            files=[AlbumQualityEvidenceFile(
                relative_path="01 Installed.flac",
                size_bytes=4242,
                mtime_ns=1_700_000_000_000_000_000,
                extension="flac",
                container="flac",
                codec="flac",
            )],
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=900,
                avg_bitrate_kbps=950,
                median_bitrate_kbps=940,
                format="FLAC",
            ),
            codec="flac",
            container="flac",
            storage_format="FLAC",
        )
        self.db.upsert_album_quality_evidence(evidence)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        return stored.id

    def _queue_force_job(self) -> int:
        job = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.request_id,
            dedupe_key=f"force-{self.request_id}",
            payload=force_import_payload(
                download_log_id=1, failed_path="/quarantine/dice",
            ),
        )
        return job.id

    def test_the_operator_arm_rekeys_an_imported_unowned_unclaimed_request(
        self,
    ) -> None:
        self.assertTrue(self._rekey())

        row = self.db.get_request(self.request_id)
        assert row is not None
        self.assertEqual(row["mb_release_id"], self.SURVIVOR)
        self.assertEqual(row["status"], "imported")
        self.assertIsNone(row["active_automation_import_job_id"])
        cur = self.db._execute(
            "SELECT mb_release_id FROM album_requests WHERE id = %s",
            (self.request_id,),
        )
        stored = cur.fetchone()
        assert stored is not None
        self.assertEqual(stored["mb_release_id"], self.SURVIVOR)

    def test_the_operator_arm_moves_evidence_with_the_request(self) -> None:
        moving_id = self._seed_evidence(self.MERGED)

        self.assertTrue(self._rekey())

        self.assertEqual(self._evidence_release_id(moving_id), self.SURVIVOR)

    def test_the_full_service_witness_pass_composes_with_the_real_move(
        self,
    ) -> None:
        """#1089 MINOR-F (review round 3): the deterministic/generated
        ``tests.test_merge_rekey_service`` suites cover this composition
        against ``FakePipelineDB``; this is the REAL-PostgreSQL leg —
        driving the actual ``MergeRekeyService.rekey_request`` (mandatory
        witness, #1089 MAJOR-C, included) against this class's real ``db``,
        with a real on-disk survivor album whose bytes match the request's
        linked current evidence exactly. Proves the witness-pass path and
        the row+evidence move compose against real PostgreSQL, not only the
        fake.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            real_path = os.path.join(tmp_dir, "01 Track.flac")
            with open(real_path, "wb") as handle:
                handle.write(b"\x00" * 4242)
            evidence = make_album_quality_evidence(
                mb_release_id=self.MERGED,
                source_path=tmp_dir,
                files=[AlbumQualityEvidenceFile(
                    relative_path="01 Track.flac",
                    size_bytes=4242,
                    mtime_ns=1_700_000_000_000_000_000,
                    extension="flac",
                    container="flac",
                    codec="flac",
                )],
            )
            self.db.upsert_album_quality_evidence(evidence)
            stored = self.db.find_album_quality_evidence(
                mb_release_id=self.MERGED,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert stored is not None and stored.id is not None
            self.assertTrue(
                self.db.set_request_current_evidence(
                    self.request_id, stored.id,
                ),
            )

            beets = FakeBeetsDB()
            beets.set_album_ids_for_release(self.SURVIVOR, [19345])
            beets.set_item_paths(self.SURVIVOR, [(19345, real_path)])
            service = MergeRekeyService(
                self.db,
                beets,
                canonical_release_fn=(
                    lambda _release_id: CanonicalReleaseRedirected(
                        self.SURVIVOR,
                    )
                ),
            )

            result = service.rekey_request(self.request_id)

            self.assertEqual(result.outcome, RESULT_REKEYED)
            self.assertEqual(self._stored_release_id(), self.SURVIVOR)
            self.assertEqual(
                self._evidence_release_id(stored.id), self.SURVIVOR,
            )

    def test_a_non_imported_status_refuses_and_writes_nothing(self) -> None:
        for status in ("wanted", "downloading", "unsearchable"):
            with self.subTest(status=status):
                db = make_db()
                self.addCleanup(db.close)
                request_id = db.add_request(
                    mb_release_id=self.MERGED,
                    artist_name="DICE",
                    album_title="Midnight Zoo",
                    source="request",
                    status=status,
                )
                evidence_id = self._seed_evidence_for(db, self.MERGED)

                self.assertFalse(db.update_request_release_for_merge(
                    request_id,
                    old_release_id=self.MERGED,
                    new_release_id=self.SURVIVOR,
                    expected_import_job_id=None,
                ))

                row = db.get_request(request_id)
                assert row is not None
                self.assertEqual(row["mb_release_id"], self.MERGED)
                self.assertEqual(
                    self._evidence_release_id_for(db, evidence_id),
                    self.MERGED,
                )

    def test_a_processing_status_refuses_and_writes_nothing(self) -> None:
        """#1089 MINOR-7: the contract names ``processing`` explicitly as a
        status the operator arm must refuse. A real owning automation job is
        required — migration 066's owner-equivalence CHECK forbids a
        ``processing`` row with no attached job, so a fake status string
        alone would not be reachable in real PostgreSQL."""
        db = make_db()
        self.addCleanup(db.close)
        request_id = db.add_request(
            mb_release_id=self.MERGED,
            artist_name="DICE",
            album_title="Midnight Zoo (processing)",
            source="request",
        )
        enqueued = "2026-08-13T00:00:00+00:00"
        self.assertTrue(db.set_downloading(
            request_id,
            json.dumps({
                "filetype": "flac",
                "enqueued_at": enqueued,
                "last_progress_at": enqueued,
                "files": [],
            }),
            expected_status="wanted",
        ))
        handoff = db.handoff_automation_import(
            request_id=request_id,
            expected_enqueued_at=enqueued,
            canonical_path="/processing/albums/dice-midnight-zoo",
            message="MINOR-7 processing fixture",
        )
        self.assertTrue(handoff.committed)
        evidence_id = self._seed_evidence_for(db, self.MERGED)

        self.assertFalse(db.update_request_release_for_merge(
            request_id,
            old_release_id=self.MERGED,
            new_release_id=self.SURVIVOR,
            expected_import_job_id=None,
        ))

        row = db.get_request(request_id)
        assert row is not None
        self.assertEqual(row["status"], "processing")
        self.assertEqual(row["mb_release_id"], self.MERGED)
        self.assertEqual(
            self._evidence_release_id_for(db, evidence_id), self.MERGED,
        )

    def test_a_replaced_status_refuses_and_writes_nothing(self) -> None:
        """#1089 MINOR-7: the contract names ``replaced`` explicitly — a
        frozen audit ancestor is never rekeyed, even though its
        ``mb_release_id`` is otherwise untouched by supersession."""
        db = make_db()
        self.addCleanup(db.close)
        request_id = db.add_request(
            mb_release_id=self.MERGED,
            artist_name="DICE",
            album_title="Midnight Zoo (replaced)",
            source="request",
        )
        evidence_id = self._seed_evidence_for(db, self.MERGED)
        db.supersede_request_mbid(
            request_id,
            new_mb_release_id="d0000000-0000-0000-0000-000000000001",
            new_mb_release_group_id=None,
            new_mb_artist_id=None,
            new_artist_name="DICE",
            new_album_title="Midnight Zoo (successor pressing)",
            new_year=None,
            new_country=None,
            new_tracks=[],
        )

        self.assertFalse(db.update_request_release_for_merge(
            request_id,
            old_release_id=self.MERGED,
            new_release_id=self.SURVIVOR,
            expected_import_job_id=None,
        ))

        row = db.get_request(request_id)
        assert row is not None
        self.assertEqual(row["status"], "replaced")
        self.assertEqual(row["mb_release_id"], self.MERGED)
        self.assertEqual(
            self._evidence_release_id_for(db, evidence_id), self.MERGED,
        )

    def _seed_evidence_for(self, db: "PipelineDB", release_id: str) -> int:
        evidence = make_album_quality_evidence(
            mb_release_id=release_id, source_path=f"/library/{release_id}",
        )
        db.upsert_album_quality_evidence(evidence)
        stored = db.find_album_quality_evidence(
            mb_release_id=release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        return stored.id

    def _evidence_release_id_for(self, db: "PipelineDB", evidence_id: int) -> str | None:
        cur = db._execute(
            "SELECT mb_release_id FROM album_quality_evidence WHERE id = %s",
            (evidence_id,),
        )
        row = cur.fetchone()
        return None if row is None else str(row["mb_release_id"])

    def test_a_queued_job_of_any_type_blocks_the_operator_arm(self) -> None:
        from lib.import_queue import IMPORT_JOB_YOUTUBE

        for job_type in (IMPORT_JOB_FORCE, IMPORT_JOB_YOUTUBE):
            with self.subTest(job_type=job_type):
                db = make_db()
                self.addCleanup(db.close)
                request_id = db.add_request(
                    mb_release_id=self.MERGED,
                    artist_name="DICE",
                    album_title=f"Midnight Zoo ({job_type})",
                    source="request",
                    status="imported",
                )
                if job_type == IMPORT_JOB_FORCE:
                    db.enqueue_import_job(
                        IMPORT_JOB_FORCE,
                        request_id=request_id,
                        dedupe_key=f"force-{request_id}",
                        payload=force_import_payload(
                            download_log_id=1, failed_path="/quarantine/dice",
                        ),
                    )
                else:
                    from lib.import_queue import youtube_import_payload
                    db.enqueue_import_job(
                        IMPORT_JOB_YOUTUBE,
                        request_id=request_id,
                        payload=youtube_import_payload(
                            staged_path="/Incoming/auto-import/dice",
                            request_id=request_id,
                            browse_id="MPREb_dice",
                            download_log_id=9,
                        ),
                    )

                self.assertFalse(db.update_request_release_for_merge(
                    request_id,
                    old_release_id=self.MERGED,
                    new_release_id=self.SURVIVOR,
                    expected_import_job_id=None,
                ))

                row = db.get_request(request_id)
                assert row is not None
                self.assertEqual(row["mb_release_id"], self.MERGED)

    def test_a_running_force_job_blocks_the_operator_arm(self) -> None:
        job_id = self._queue_force_job()
        self.assertIsNotNone(self.db.mark_import_job_preview_importable(
            job_id, preview_result={}, message="ready",
        ))
        claimed = self.db.claim_force_import_job_under_lock(
            job_id, request_id=self.request_id, worker_id="pg-operator-fence-test",
        )
        assert claimed is not None and claimed.status == "running"

        self.assertFalse(self._rekey())

        self.assertEqual(self._stored_release_id(), self.MERGED)

    def test_a_terminal_job_on_this_request_never_blocks_the_operator_arm(
        self,
    ) -> None:
        """Must-still-work: a completed job on this request is inert."""
        job_id = self._queue_force_job()
        self.assertIsNotNone(self.db.mark_import_job_completed(
            job_id, result={}, message="done",
        ))

        self.assertTrue(self._rekey())

        self.assertEqual(self._stored_release_id(), self.SURVIVOR)

    def test_a_real_job_id_never_satisfies_the_operator_arm(self) -> None:
        """``expected_import_job_id IS NULL`` guards the operator arm.

        A completed job on this request — otherwise no different from the
        must-still-work world above — must still refuse when the caller
        supplies its id instead of ``None``: the force arm refuses it (not
        ``running``) and the operator arm refuses it (not ``NULL``), so
        NEITHER arm may admit the write.
        """
        job_id = self._queue_force_job()
        self.assertIsNotNone(self.db.mark_import_job_completed(
            job_id, result={}, message="done",
        ))

        self.assertFalse(self._rekey(expected_import_job_id=job_id))

        self.assertEqual(self._stored_release_id(), self.MERGED)

    def test_a_stale_identity_refuses_and_writes_nothing(self) -> None:
        self.assertFalse(self.db.update_request_release_for_merge(
            self.request_id,
            old_release_id="00000000-0000-4000-8000-000000000000",
            new_release_id=self.SURVIVOR,
            expected_import_job_id=None,
        ))

        self.assertEqual(self._stored_release_id(), self.MERGED)

    def test_a_survivor_another_request_holds_refuses_and_writes_nothing(
        self,
    ) -> None:
        other = self.db.add_request(
            mb_release_id=self.SURVIVOR,
            artist_name="DICE",
            album_title="Midnight Zoo (other pressing)",
            source="request",
        )
        moving_id = self._seed_evidence(self.MERGED)

        self.assertFalse(self._rekey())

        self.assertEqual(self._stored_release_id(), self.MERGED)
        self.assertEqual(self._evidence_release_id(moving_id), self.MERGED)
        other_row = self.db.get_request(other)
        assert other_row is not None
        self.assertEqual(other_row["mb_release_id"], self.SURVIVOR)

    def test_an_evidence_fingerprint_collision_refuses_and_writes_nothing(
        self,
    ) -> None:
        moving_id = self._seed_evidence(self.MERGED)
        colliding_id = self._seed_evidence(self.SURVIVOR)

        self.assertFalse(self._rekey())

        self.assertEqual(self._stored_release_id(), self.MERGED)
        self.assertEqual(self._evidence_release_id(moving_id), self.MERGED)
        self.assertEqual(self._evidence_release_id(colliding_id), self.SURVIVOR)


@requires_postgres
class TestSetMarkedIncompleteRoundTrip(unittest.TestCase):
    """Rule A round-trip for set_marked_incomplete (issue #1241).

    Migration 082's ``album_requests.marked_incomplete_at`` must survive the
    real PG seam in both directions: a mark reads back as a timestamp
    through ``get_request``, a clear reads back as NULL, and the frozen
    ``replaced`` audit state refuses the write entirely.
    """

    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def _seed(self, mb_release_id: str = "mark-mbid") -> int:
        return self.db.add_request(
            artist_name="Dirt Dress",
            album_title="Theme Songs",
            source="request",
            mb_release_id=mb_release_id,
            status="imported",
        )

    def test_mark_round_trip_and_idempotence(self):
        request_id = self._seed()
        row = self.db.get_request(request_id)
        assert row is not None
        self.assertIsNone(row["marked_incomplete_at"])

        self.assertFalse(self.db.request_marked_incomplete(request_id))
        before = self.db.get_request(request_id)
        assert before is not None
        self.assertEqual(
            self.db.set_marked_incomplete(request_id, marked=True), "marked"
        )
        row = self.db.get_request(request_id)
        assert row is not None
        first_stamp = row["marked_incomplete_at"]
        self.assertIsNotNone(first_stamp)
        # The write restamps updated_at like every other request mutation.
        self.assertGreater(row["updated_at"], before["updated_at"])
        # The dispatch path's narrow scalar read agrees with the row.
        self.assertTrue(self.db.request_marked_incomplete(request_id))
        self.assertFalse(self.db.request_marked_incomplete(987654))

        # Re-marking is a distinct no-op outcome and never re-stamps.
        self.assertEqual(
            self.db.set_marked_incomplete(request_id, marked=True),
            "already_marked",
        )
        row = self.db.get_request(request_id)
        assert row is not None
        self.assertEqual(row["marked_incomplete_at"], first_stamp)

    def test_clear_round_trip_and_idempotence(self):
        request_id = self._seed("mark-mbid-clear")
        self.db.set_marked_incomplete(request_id, marked=True)
        self.assertEqual(
            self.db.set_marked_incomplete(request_id, marked=False), "cleared"
        )
        row = self.db.get_request(request_id)
        assert row is not None
        self.assertIsNone(row["marked_incomplete_at"])
        self.assertEqual(
            self.db.set_marked_incomplete(request_id, marked=False),
            "already_clear",
        )

    def test_not_found(self):
        self.assertEqual(
            self.db.set_marked_incomplete(987654, marked=True), "not_found"
        )

    def test_replaced_row_refuses_the_write(self):
        old_id = self._seed("mark-mbid-old")
        self.db.supersede_request_mbid(
            old_id,
            new_mb_release_id="mark-mbid-new",
            new_mb_release_group_id="rg-mark",
            new_mb_artist_id="art-mark",
            new_artist_name="Dirt Dress",
            new_album_title="Theme Songs",
            new_year=2007,
            new_country="US",
            new_tracks=[
                {"disc_number": 1, "track_number": 1, "title": "Theme"},
            ],
        )
        self.assertEqual(
            self.db.set_marked_incomplete(old_id, marked=True), "replaced"
        )
        row = self.db.get_request(old_id)
        assert row is not None
        self.assertIsNone(row["marked_incomplete_at"])


if __name__ == "__main__":
    unittest.main()
