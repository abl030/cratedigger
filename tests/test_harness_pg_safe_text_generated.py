"""Generated property: every harness wire message is PostgreSQL-storable.

INVARIANT (see ``tests/test_harness_pg_safe_text.py`` for the live pin):
the real ``_send`` emits no U+0000 in any key or value at any depth; a
NUL-free message is emitted exactly as ``json.dumps`` would; trailing NUL
padding on a string is invisible; and the emitted line survives a real
``download_log`` JSONB INSERT unchanged.
"""

from __future__ import annotations

import json
import unittest

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from tests.test_harness_pg_safe_text import (
    LIVE_TRACK_ID,
    nul_violations,
    send_captured,
)
from tests.test_pipeline_db import make_db, requires_postgres

_TEXT = st.text(alphabet=st.sampled_from("aX\x00 é�\\\"u0"), max_size=8)
_SCALARS = st.none() | st.booleans() | st.integers() | _TEXT
_VALUES = st.recursive(
    _SCALARS,
    lambda children: (
        st.lists(children, max_size=4)
        | st.tuples(children, children)
        | st.dictionaries(_TEXT, children, max_size=4)
    ),
    max_leaves=12,
)
_MESSAGES = st.dictionaries(_TEXT, _VALUES, max_size=5)


def _holds_nul(value: object) -> bool:
    return bool(nul_violations(json.loads(json.dumps(value))))


def send_violations(message: dict[str, object], emitted_line: str) -> list[str]:
    """Every clause of the invariant the emitted line breaks."""
    violations: list[str] = []
    if not emitted_line.endswith("\n") or "\n" in emitted_line[:-1]:
        violations.append("emitted output is not exactly one protocol line")
    decoded: object = json.loads(emitted_line.split("\n", 1)[0])
    violations.extend(nul_violations(decoded))
    if not _holds_nul(message) and emitted_line != json.dumps(message) + "\n":
        violations.append("a NUL-free message was altered on the wire")
    return violations


class TestEveryWireMessageIsStorable(unittest.TestCase):

    @given(message=_MESSAGES)
    @example(message={"mb_trackid": LIVE_TRACK_ID + "\x00"})
    @example(message={"k\x00": ["\x00", ("a\x00b", None)]})
    @example(message={"plain": "no nul", "esc": "\\u0000"})
    def test_send_emits_no_nul_and_keeps_clean_messages(
        self, message: dict[str, object],
    ) -> None:
        self.assertEqual(send_violations(message, send_captured(message)), [])

    @given(text=_TEXT.filter(lambda s: "\x00" not in s), pad=st.integers(1, 4))
    @example(text=LIVE_TRACK_ID, pad=1)
    def test_trailing_nul_padding_is_invisible(self, text: str, pad: int) -> None:
        self.assertEqual(
            send_captured({"v": text + "\x00" * pad}),
            send_captured({"v": text}),
        )


@requires_postgres
class TestEveryWireMessageSurvivesJsonbInsert(unittest.TestCase):
    """Composition with the real consumer: emitted line → download_log JSONB."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.db = make_db()
        cls.request_id = cls.db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="00000000-0000-0000-0000-000000000001",
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.db.close()

    @given(message=_MESSAGES)
    @example(message={"items": [{"mb_trackid": LIVE_TRACK_ID + "\x00"}]})
    def test_emitted_line_round_trips_through_postgres(
        self, message: dict[str, object],
    ) -> None:
        line = send_captured(message)
        log_id = self.db.log_download(
            self.request_id, outcome="failed", validation_result=line,
        )
        cur = self.db._execute(
            "SELECT validation_result FROM download_log WHERE id = %s",
            (log_id,),
        )
        row = cur.fetchone()
        assert row is not None
        self.assertEqual(row["validation_result"], json.loads(line))


class TestSendCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests, one per clause of ``send_violations``."""

    def test_multi_line_output_is_named(self) -> None:
        self.assertIn(
            "emitted output is not exactly one protocol line",
            send_violations({}, "{}\n{}\n"),
        )

    def test_surviving_nul_is_named(self) -> None:
        violations = send_violations({"v": "x\x00"}, '{"v": "x\\u0000"}\n')
        self.assertRegex(violations[0], r"\$\.v: string holds U\+0000")

    def test_altered_clean_message_is_named(self) -> None:
        self.assertIn(
            "a NUL-free message was altered on the wire",
            send_violations({"v": "x"}, '{"v": "y"}\n'),
        )

    def test_scrubbed_nul_message_is_quiet(self) -> None:
        self.assertEqual(send_violations({"v": "x\x00"}, '{"v": "x"}\n'), [])


if __name__ == "__main__":
    unittest.main()
