"""Every bound value is PostgreSQL-storable (``lib/pipeline_db/pg_safe.py``).

INVARIANT: a ``PipelineDB`` write never fails because a string holds U+0000
or a lone surrogate, or because a JSON payload holds NaN/±Infinity. Clean
values round-trip byte-identically; trailing NULs vanish; every interior
unstorable character is stored as U+FFFD; a non-finite float is JSON null.

The generated twin is ``tests/test_pg_safe_generated.py``.
"""

from __future__ import annotations

import io
import json
import math
import sys
import unittest
from dataclasses import replace
from types import SimpleNamespace
from typing import ClassVar

import msgspec
import psycopg2.extras

from lib.pipeline_db.pg_safe import (
    pg_json_dumps,
    pg_jsonb_text,
    pg_safe_json,
    pg_safe_text,
)
from lib.quality import ChooseMatchMessage
from tests.evidence_helpers import make_album_quality_evidence
from tests.test_harness_wire_contract_audit import (
    _album_match,
    _AlbumMatch,
    _Distance,
    _item_stub,
    _Stub,
    _track_stub,
    beets_harness,
)
from tests.test_pipeline_db import make_db, requires_postgres
from tests.test_terminal_outcomes import (
    _prepare_automation_terminal_command,
    _seed_running_import,
)

LIVE_TRACK_ID = "77411cb8-f7ec-4dae-842e-c50abc019a2c"


class TestPgSafeText(unittest.TestCase):
    CASES: ClassVar[list[tuple[str, str, str]]] = [
        ("clean ascii", "Smelly Beaver", "Smelly Beaver"),
        ("clean unicode", "Björk 🎵 ￾", "Björk 🎵 ￾"),
        ("existing U+FFFD kept", "a�b", "a�b"),
        ("trailing NUL padding", LIVE_TRACK_ID + "\x00", LIVE_TRACK_ID),
        ("several trailing NULs", "Opener\x00\x00\x00", "Opener"),
        ("interior NUL", "Artist\x00Guest", "Artist�Guest"),
        ("leading NUL", "\x00x", "�x"),
        ("only NULs", "\x00\x00", ""),
        ("lone low surrogate", "caf\udce9", "caf�"),
        ("lone high surrogate", "\ud83cx", "�x"),
        ("surrogate then NUL", "a\udce9\x00", "a�"),
        ("NUL then surrogate", "a\x00\udce9", "a��"),
        ("only NULs are stripped from the tail", "SAX", "SAX"),
        ("a dirty string keeps a non-NUL tail", "a\x00X", "a�X"),
    ]

    def test_cases(self) -> None:
        for desc, value, expected in self.CASES:
            with self.subTest(desc=desc):
                self.assertEqual(pg_safe_text(value), expected)

    def test_clean_value_is_returned_unchanged(self) -> None:
        value = "untouched"
        self.assertIs(pg_safe_text(value), value)


class TestPgJsonDumps(unittest.TestCase):

    def test_non_finite_floats_become_null(self) -> None:
        encoded = pg_json_dumps(
            {"nan": math.nan, "inf": math.inf, "ninf": -math.inf, "ok": 1.5},
        )
        self.assertEqual(
            msgspec.json.decode(encoded),
            {"nan": None, "inf": None, "ninf": None, "ok": 1.5},
        )

    def test_keys_and_nested_values_are_cleaned(self) -> None:
        encoded = pg_json_dumps(
            {"k\x00": [{"id": LIVE_TRACK_ID + "\x00"}, ("caf\udce9", None)]},
        )
        self.assertEqual(
            msgspec.json.decode(encoded),
            {"k": [{"id": LIVE_TRACK_ID}, ["caf�", None]]},
        )

    def test_struct_input_is_encoded_through_to_builtins(self) -> None:
        class _Row(msgspec.Struct):
            name: str
            score: float

        self.assertEqual(
            msgspec.json.decode(pg_json_dumps(_Row(name="a\x00", score=math.nan))),
            {"name": "a", "score": None},
        )

    def test_clean_payload_matches_plain_msgspec(self) -> None:
        payload = {"a": [1, 2.5, True, None, "x"], "b": {"c": "é"}}
        self.assertEqual(
            pg_json_dumps(payload), msgspec.json.encode(payload).decode(),
        )

    def test_pg_safe_json_nulls_non_finite_floats_itself(self) -> None:
        # msgspec would also encode these as null; the cleaner must not rely
        # on that, because the fake and other direct callers skip msgspec.
        self.assertEqual(
            pg_safe_json([math.nan, math.inf, -math.inf, 2.5]),
            [None, None, None, 2.5],
        )

    def test_non_string_keys_are_left_alone(self) -> None:
        self.assertEqual(pg_safe_json({1: "a\x00", 2.5: "b"}), {1: "a", 2.5: "b"})
        nan_key = pg_safe_json({math.nan: 1})
        assert isinstance(nan_key, dict)
        self.assertTrue(all(isinstance(k, float) for k in nan_key))

    def test_pg_safe_json_leaves_scalars_alone(self) -> None:
        for value in (0, -7, True, None, 2**70, 0.0):
            with self.subTest(value=value):
                self.assertEqual(pg_safe_json(value), value)


@requires_postgres
class TestEveryParameterPathIsStorable(unittest.TestCase):
    """The real adapters over the real writer into real PostgreSQL."""

    def setUp(self) -> None:
        self.db = make_db()
        self.addCleanup(self.db.close)
        self.request_id = self.db.add_request(
            artist_name="Limp Bizkit\x00", album_title="Smelly Beaver",
            source="request",
            mb_release_id="5bb2c0f8-0000-0000-0000-000000000000",
        )

    def _row(self, log_id: int) -> dict[str, object]:
        cur = self.db._execute(
            "SELECT error_message, soulseek_username,"
            " candidate_contributor_usernames, transfer_detail"
            " FROM download_log WHERE id = %s",
            (log_id,),
        )
        row = cur.fetchone()
        assert row is not None
        return dict(row)

    def test_text_textarray_and_json_paths(self) -> None:
        log_id = self.db.log_download(
            self.request_id,
            soulseek_username="peer\udce9",
            contributor_usernames=["peer\udce9", "other\x00"],
            outcome="failed",
            error_message="Transfer rejected: Banned\x00",
            transfer_detail=[{"file": "01 caf\udce9.mp3", "ratio": math.nan}],
        )
        self.assertEqual(self._row(log_id), {
            "error_message": "Transfer rejected: Banned",
            "soulseek_username": "peer�",
            "candidate_contributor_usernames": ["other", "peer�"],
            "transfer_detail": [{"file": "01 caf�.mp3", "ratio": None}],
        })

    def test_request_text_columns_are_cleaned(self) -> None:
        row = self.db.get_request(self.request_id)
        assert row is not None
        self.assertEqual(row["artist_name"], "Limp Bizkit")

    def test_a_lookup_parameter_is_cleaned_like_the_stored_value(self) -> None:
        cur = self.db._execute(
            "SELECT id FROM album_requests WHERE artist_name = %s",
            ("Limp Bizkit\x00",),
        )
        self.assertEqual([r["id"] for r in cur.fetchall()], [self.request_id])

    def test_execute_values_path(self) -> None:
        cur = self.db.conn.cursor()
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO download_log (request_id, outcome, error_message,"
            " transfer_detail) VALUES %s RETURNING id",
            [(self.request_id, "failed", "x\x00",
              psycopg2.extras.Json({"v": "y\udce9"}))],
        )
        row = cur.fetchone()
        assert row is not None
        stored = self._row(int(row[0]))
        self.assertEqual(stored["error_message"], "x")
        self.assertEqual(stored["transfer_detail"], {"v": "y�"})


@requires_postgres
class TestEvidenceAudioValidationIsStorable(unittest.TestCase):
    """ffmpeg stderr reaches ``album_quality_evidence.audio_validation``.

    ``bounded_audio_tool_diagnostic`` collapses whitespace but keeps NUL, and
    this column used to be bound as msgspec-encoded text. (A non-UTF-8
    filename never reaches this write: ``snapshot_fingerprint`` UTF-8-encodes
    every path first and raises — issue #1422.)
    """

    def test_nul_in_stderr_excerpt_is_stored(self) -> None:
        db = make_db()
        self.addCleanup(db.close)
        evidence = make_album_quality_evidence(
            audio_corrupt=True,
            audio_error="Invalid data found\x00\x00",
        )
        db.upsert_album_quality_evidence(evidence)
        cur = db._execute("SELECT audio_validation FROM album_quality_evidence")
        rows = cur.fetchall()
        self.assertEqual(len(rows), 1)
        diagnostics = rows[0]["audio_validation"]["diagnostics"]
        self.assertEqual(diagnostics[0]["stderr_excerpt"], "Invalid data found")


_DIRTY_JSON = json.dumps({"id": LIVE_TRACK_ID + "\x00", "ratio": math.nan})
_CLEAN_STORED = {"id": LIVE_TRACK_ID, "ratio": None}


@requires_postgres
class TestEveryJsonbBindIsStorable(unittest.TestCase):
    """One real write per JSONB bind the adapter change rewrote.

    Each feeds a NUL-padded string and a NaN, the two values that reach
    these binds from external text, and reads the column back.
    """

    def setUp(self) -> None:
        self.db = make_db()
        self.addCleanup(self.db.close)
        self.request_id = self.db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="00000000-0000-0000-0000-0000000000aa",
        )

    def _one(self, sql: str, params: tuple[object, ...]) -> object:
        row = self.db._execute(sql, params).fetchone()
        assert row is not None
        return next(iter(dict(row).values()))

    def test_none_stays_sql_null(self) -> None:
        self.assertIsNone(pg_jsonb_text(None))
        log_id = self.db.log_download(self.request_id, outcome="failed")
        self.assertEqual(
            self._one(
                "SELECT import_result IS NULL AND validation_result IS NULL"
                " FROM download_log WHERE id = %s", (log_id,),
            ),
            True,
        )

    def test_log_download_json_text_params(self) -> None:
        log_id = self.db.log_download(
            self.request_id, outcome="failed",
            import_result=_DIRTY_JSON, validation_result=_DIRTY_JSON,
        )
        row = self.db._execute(
            "SELECT import_result, validation_result FROM download_log"
            " WHERE id = %s", (log_id,),
        ).fetchone()
        assert row is not None
        self.assertEqual(row["import_result"], _CLEAN_STORED)
        self.assertEqual(row["validation_result"], _CLEAN_STORED)

    def test_job_less_rejection_audit(self) -> None:
        from lib import transitions
        from lib.terminal_outcomes import (
            RequestRejectionOutcome,
            TerminalDownloadAudit,
        )

        result = self.db.persist_request_rejection_outcome(
            RequestRejectionOutcome(
                request_id=self.request_id,
                audit=TerminalDownloadAudit(
                    outcome="rejected",
                    import_result=_DIRTY_JSON,
                    validation_result=_DIRTY_JSON,
                ),
                transition=transitions.RequestTransition.to_wanted_fields(
                    attempt_type="validation", fields={},
                ),
            ),
        )
        row = self.db._execute(
            "SELECT import_result, validation_result FROM download_log"
            " WHERE id = %s", (result.download_log_id,),
        ).fetchone()
        assert row is not None
        self.assertEqual(row["import_result"], _CLEAN_STORED)
        self.assertEqual(row["validation_result"], _CLEAN_STORED)

    def _state(self) -> object:
        return self._one(
            "SELECT active_download_state FROM album_requests WHERE id = %s",
            (self.request_id,),
        )

    def test_set_downloading(self) -> None:
        self.assertTrue(self.db.set_downloading(self.request_id, _DIRTY_JSON))
        self.assertEqual(self._state(), _CLEAN_STORED)

    def _plan(self) -> tuple[int, list[dict[str, object]]]:
        from lib.pipeline_db import SearchPlanItemInput

        plan_id = self.db.create_successful_search_plan(
            request_id=self.request_id, generator_id="g1",
            items=[SearchPlanItemInput(
                ordinal=0, strategy="slot_0", query="q0",
                canonical_query_key="k0",
            )],
            set_active=True,
        )
        items = self.db._execute(
            "SELECT id, ordinal FROM search_plan_items WHERE plan_id = %s",
            (plan_id,),
        ).fetchall()
        return plan_id, [dict(r) for r in items]

    def test_set_downloading_if_plan_current(self) -> None:
        plan_id, _items = self._plan()
        request = self.db.get_request(self.request_id)
        assert request is not None
        self.assertTrue(self.db.set_downloading_if_plan_current(
            self.request_id, _DIRTY_JSON,
            plan_id=plan_id,
            plan_ordinal=int(request["next_plan_ordinal"]),
            cycle_count_snapshot=int(request["plan_cycle_count"]),
        ))
        self.assertEqual(self._state(), _CLEAN_STORED)

    def test_consumed_search_attempt_candidates(self) -> None:
        from lib.pipeline_db import ConsumedAttemptInput

        plan_id, items = self._plan()
        request = self.db.get_request(self.request_id)
        assert request is not None
        self.db.record_consumed_search_attempt(ConsumedAttemptInput(
            request_id=self.request_id,
            plan_id=plan_id,
            plan_item_id=int(str(items[0]["id"])),
            plan_ordinal=0,
            plan_strategy="slot_0",
            plan_canonical_query_key="k0",
            plan_repeat_group=None,
            plan_generator_id="g1",
            query="q0",
            outcome="no_results",
            plan_item_count=1,
            cycle_count_snapshot=int(request["plan_cycle_count"]),
            elapsed_s=1.0, result_count=0,
            apply_scheduler_attempt=True,
            scheduler_success=False,
            candidates_json=json.dumps([{"user": "peer\x00", "score": math.nan}]),
        ))
        self.assertEqual(
            self._one(
                "SELECT candidates FROM search_log WHERE request_id = %s",
                (self.request_id,),
            ),
            [{"user": "peer", "score": None}],
        )

    def test_wrong_match_triage(self) -> None:
        from lib.validation_envelope import WrongMatchTriageAudit

        log_id = self.db.log_download(self.request_id, outcome="rejected")
        self.assertTrue(self.db.record_wrong_match_triage(
            log_id,
            WrongMatchTriageAudit(action="deleted_reject", error="boom\x00"),
        ))
        triage = self._one(
            "SELECT validation_result -> 'wrong_match_triage'"
            " FROM download_log WHERE id = %s", (log_id,),
        )
        assert isinstance(triage, dict)
        self.assertEqual(triage["error"], "boom")

    def test_jellyfin_pin_children(self) -> None:
        pin_id = self.db.add_jellyfin_date_created_pin(
            imported_path="/x",
            original_date_created="2026-04-26T18:31:04.4425337Z",
            album_item_id="alb-1",
            children_item_ids=["tr-1\x00", "tr-2"],
            request_id=None,
        )
        self.assertEqual(
            self._one(
                "SELECT children_item_ids FROM jellyfin_date_created_pins"
                " WHERE id = %s", (pin_id,),
            ),
            ["tr-1", "tr-2"],
        )


def _nul_tagged_item() -> _Stub:
    return _Stub(**{
        **vars(_item_stub()),
        "mb_trackid": LIVE_TRACK_ID + "\x00",
        "title": "Opener\x00\x00",
        "artist": "The Artist\x00Guest",
    })


def capture_nul_tagged_choose_match() -> str:
    """One real harness ``choose_match`` line whose file tags carry NULs."""
    session = beets_harness.HarnessImportSession.__new__(
        beets_harness.HarnessImportSession,
    )
    session._task_counter = 0
    session._pretend = True
    item = _nul_tagged_item()
    task = SimpleNamespace(
        candidates=[_AlbumMatch(
            distance=_Distance(),
            info=_album_match().info,  # pyright: ignore[reportAttributeAccessIssue]
            mapping={item: _track_stub()},
            extra_items=[_nul_tagged_item()],
            extra_tracks=[_track_stub()],
        )],
        paths=[b"/downloads/x"],
        items=[item],
        rec=SimpleNamespace(name="strong"),
        cur_artist="The Artist",
        cur_album="The Album",
    )
    module_globals = vars(beets_harness)
    prior_stdout = module_globals["_protocol_stdout"]
    prior_stdin = sys.stdin
    buffer = io.StringIO()
    module_globals["_protocol_stdout"] = buffer
    sys.stdin = io.StringIO("")
    hit_eof = False
    try:
        try:
            session.choose_match(task)
        except EOFError:
            hit_eof = True
    finally:
        module_globals["_protocol_stdout"] = prior_stdout
        sys.stdin = prior_stdin
    assert hit_eof, "choose_match returned without blocking on the controller"
    lines = [line for line in buffer.getvalue().splitlines() if line.strip()]
    assert len(lines) == 1, lines
    return lines[0]


@requires_postgres
class TestLiveSmellyBeaverWorld(unittest.TestCase):
    """Request 6343: real harness output → real automation terminal bundle.

    The writer that failed live is ``_insert_terminal_download_audit``,
    reached through ``persist_import_terminal_outcome`` with a
    ``validation_result`` JSON string encoded from the harness message.
    """

    def test_automation_terminal_audit_stores_the_nul_padded_track_id(self) -> None:
        line = capture_nul_tagged_choose_match()
        self.assertIn(LIVE_TRACK_ID + "\\u0000", line)
        decoded = msgspec.convert(json.loads(line), type=ChooseMatchMessage)
        db, request_id, job_id = _seed_running_import(automation_state=True)
        self.addCleanup(db.close)
        command = _prepare_automation_terminal_command(db, request_id, job_id)
        command = replace(command, audit=replace(
            command.audit,
            validation_result=msgspec.json.encode(decoded).decode(),
            import_result=json.dumps({"ratio": math.nan, "error": "x\udce9"}),
        ))

        db.persist_import_terminal_outcome(command)

        row = db.get_download_history(request_id)[0]
        validation = row["validation_result"]
        assert isinstance(validation, dict)
        items = validation["items"]
        assert isinstance(items, list)
        first = items[0]
        assert isinstance(first, dict)
        self.assertEqual(first["mb_trackid"], LIVE_TRACK_ID)
        self.assertEqual(first["title"], "Opener")
        self.assertEqual(first["artist"], "The Artist�Guest")
        self.assertEqual(row["import_result"], {"ratio": None, "error": "x�"})


if __name__ == "__main__":
    unittest.main()
