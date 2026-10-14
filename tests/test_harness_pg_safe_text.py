"""Harness wire strings must be storable in PostgreSQL (U+0000 never reaches it).

INVARIANT: every string the harness emits on its protocol stdout — keys and
values, at any depth — is free of U+0000, so the consumer can persist the
message into ``download_log`` JSONB. Trailing NULs (ID3 terminator padding)
vanish; an interior NUL becomes U+FFFD; NUL-free strings are untouched.

Live incident (2026-10-12, request 6343, Limp Bizkit "Smelly Beaver"): one
file's ``mb_trackid`` tag read as ``<uuid>\\x00``. The harness emitted it
verbatim, the terminal audit INSERT raised ``UntranslatableCharacter``, and
the import became a world failure that returned the request to ``wanted``
on every grab of those files.

The generated twin is ``tests/test_harness_pg_safe_text_generated.py``.
"""

from __future__ import annotations

import io
import json
import sys
import unittest
from types import SimpleNamespace

import msgspec

from lib.quality import ChooseMatchMessage
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

LIVE_TRACK_ID = "77411cb8-f7ec-4dae-842e-c50abc019a2c"


def nul_violations(value: object, path: str = "$") -> list[str]:
    """Every string (key or value) under ``value`` that still holds U+0000."""
    violations: list[str] = []
    if isinstance(value, str):
        if "\x00" in value:
            violations.append(f"{path}: string holds U+0000: {value!r}")
    elif isinstance(value, dict):
        for key, item in value.items():
            violations.extend(nul_violations(key, f"{path}.<key {key!r}>"))
            violations.extend(nul_violations(item, f"{path}.{key}"))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            violations.extend(nul_violations(item, f"{path}[{index}]"))
    return violations


def send_captured(message: dict[str, object]) -> str:
    """Run the real ``_send`` against a buffer; return the emitted line."""
    module_globals = vars(beets_harness)
    prior = module_globals["_protocol_stdout"]
    buffer = io.StringIO()
    module_globals["_protocol_stdout"] = buffer
    try:
        beets_harness._send(message)
    finally:
        module_globals["_protocol_stdout"] = prior
    return buffer.getvalue()


def _nul_tagged_item() -> _Stub:
    return _Stub(**{
        **vars(_item_stub()),
        "mb_trackid": LIVE_TRACK_ID + "\x00",
        "title": "Opener\x00\x00",
        "artist": "The Artist\x00Guest",
    })


def capture_nul_tagged_choose_match() -> str:
    """One real ``choose_match`` line for a task whose file tags carry NULs."""
    session = beets_harness.HarnessImportSession.__new__(
        beets_harness.HarnessImportSession,
    )
    session._task_counter = 0
    session._pretend = True
    item = _nul_tagged_item()
    candidate = _AlbumMatch(
        distance=_Distance(),
        info=_album_match().info,  # pyright: ignore[reportAttributeAccessIssue]
        mapping={item: _track_stub()},
        extra_items=[_nul_tagged_item()],
        extra_tracks=[_track_stub()],
    )
    task = SimpleNamespace(
        candidates=[candidate],
        paths=[b"/downloads/x"],
        items=[item],
        rec=SimpleNamespace(name="strong"),
        cur_artist="The Artist\x00",
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


class TestNulTaggedChooseMatchIsStorable(unittest.TestCase):
    """The live world, through the real harness emitter."""

    def test_trailing_nul_is_dropped_and_interior_nul_is_marked(self) -> None:
        message = json.loads(capture_nul_tagged_choose_match())
        self.assertEqual(nul_violations(message), [])
        items = message["items"]
        self.assertEqual(items[0]["mb_trackid"], LIVE_TRACK_ID)
        self.assertEqual(items[0]["title"], "Opener")
        self.assertEqual(items[0]["artist"], "The Artist�Guest")
        mapped = message["candidates"][0]["mapping"][0]["item"]
        self.assertEqual(mapped["mb_trackid"], LIVE_TRACK_ID)

    def test_nul_free_message_is_unchanged(self) -> None:
        message: dict[str, object] = {
            "type": "x", "nested": [{"k": "v", "n": 1, "f": 0.5, "b": True}],
            "none": None, "tuple": ("a", "b"),
        }
        self.assertEqual(
            send_captured(message), json.dumps(message) + "\n",
        )


@requires_postgres
class TestNulTaggedChooseMatchReachesPostgres(unittest.TestCase):
    """Composition: real harness line → real decoder → real JSONB INSERT."""

    def setUp(self) -> None:
        self.db = make_db()
        self.addCleanup(self.db.close)
        self.request_id = self.db.add_request(
            artist_name="Limp Bizkit", album_title="Smelly Beaver",
            source="request", mb_release_id="5bb2c0f8-0000-0000-0000-000000000000",
        )

    def test_download_log_insert_round_trips_the_live_track_id(self) -> None:
        decoded = msgspec.convert(
            json.loads(capture_nul_tagged_choose_match()),
            type=ChooseMatchMessage,
        )
        log_id = self.db.log_download(
            self.request_id, outcome="failed",
            validation_result=msgspec.json.encode(decoded).decode(),
        )
        cur = self.db._execute(
            "SELECT validation_result FROM download_log WHERE id = %s",
            (log_id,),
        )
        row = cur.fetchone()
        assert row is not None
        stored = row["validation_result"]
        self.assertEqual(stored["items"][0]["mb_trackid"], LIVE_TRACK_ID)


class TestNulCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests, one per clause of ``nul_violations``."""

    def test_value_string_is_named(self) -> None:
        self.assertRegex(nul_violations({"a": "x\x00"})[0], r"\$\.a: string holds")

    def test_key_string_is_named(self) -> None:
        self.assertRegex(nul_violations({"a\x00": 1})[0], r"<key 'a\\x00'>")

    def test_list_element_is_named(self) -> None:
        self.assertRegex(nul_violations(["ok", "x\x00"])[0], r"\$\[1\]")

    def test_clean_world_is_quiet(self) -> None:
        self.assertEqual(nul_violations({"a": ["�", 1, None]}), [])


if __name__ == "__main__":
    unittest.main()
