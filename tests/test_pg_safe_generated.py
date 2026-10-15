"""Generated property: every value a PipelineDB writer binds is storable.

INVARIANT (pins in ``tests/test_pg_safe.py``): over arbitrary strings and
JSON-shaped payloads, a real ``log_download`` write never fails; what reads
back holds no U+0000, lone surrogate, or non-finite float; a clean value
reads back unchanged; and every interior unstorable character is marked by
exactly one U+FFFD.
"""

from __future__ import annotations

import json
import math
import unittest

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.pipeline_db.pg_safe import pg_safe_text
from tests.test_pg_safe import LIVE_TRACK_ID
from tests.test_pipeline_db import make_db, requires_postgres

_BAD = frozenset({"\x00"}) | frozenset(chr(c) for c in range(0xD800, 0xE000))
_TEXT = st.text(
    alphabet=st.sampled_from(
        ["a", "X", " ", "\x00", "é", "�", "\ud83c", "\udce9", "🎵", "\\", '"', "u"],
    ),
    max_size=10,
)
_FLOATS = st.floats(allow_nan=True, allow_infinity=True)
_JSON = st.recursive(
    st.none() | st.booleans() | st.integers() | _FLOATS | _TEXT,
    lambda children: (
        st.lists(children, max_size=4)
        | st.tuples(children, children)
        | st.dictionaries(_TEXT, children, max_size=4)
    ),
    max_leaves=12,
)


def _bad_count(value: str) -> int:
    return sum(1 for ch in value if ch in _BAD)


def text_violations(original: str, stored: str) -> list[str]:
    """Every clause of the text contract ``stored`` breaks."""
    violations: list[str] = []
    if _bad_count(stored):
        violations.append("stored text holds an unstorable character")
    if not _bad_count(original) and stored != original:
        violations.append("clean text was altered")
    marked = original.rstrip("\x00")
    if stored.count("�") != marked.count("�") + _bad_count(marked):
        violations.append("interior unstorable characters are not each marked once")
    return violations


def _is_clean(value: object) -> bool:
    if isinstance(value, str):
        return not _bad_count(value)
    if isinstance(value, float):
        return math.isfinite(value)
    if isinstance(value, dict):
        return all(_is_clean(k) and _is_clean(v) for k, v in value.items())
    if isinstance(value, (list, tuple)):
        return all(_is_clean(v) for v in value)
    return True


def _as_json(value: object) -> object:
    """``value`` with tuples as lists, the shape JSON reads back."""
    if isinstance(value, dict):
        return {k: _as_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_as_json(v) for v in value]
    return value


def _json_equal(expected: object, stored: object) -> bool:
    """JSON equality, with numbers compared the way JSONB keeps them.

    JSONB stores numbers as exact decimals and prints some floats in integer
    form (``4.934925614736811e16`` reads back as ``49349256147368110``), so a
    float only has to read back as the same number.
    """
    if isinstance(expected, float) and not isinstance(stored, bool):
        return isinstance(stored, (int, float)) and math.isclose(
            float(stored), expected, rel_tol=1e-15,
        )
    if isinstance(expected, dict) and isinstance(stored, dict):
        return expected.keys() == stored.keys() and all(
            _json_equal(v, stored[k]) for k, v in expected.items()
        )
    if isinstance(expected, list) and isinstance(stored, list):
        return len(expected) == len(stored) and all(
            _json_equal(e, s) for e, s in zip(expected, stored, strict=True)
        )
    return type(expected) is type(stored) and expected == stored


def json_violations(original: object, stored: object) -> list[str]:
    """Every clause of the JSON contract ``stored`` breaks."""
    violations: list[str] = []
    if not _is_clean(stored):
        violations.append("stored JSON holds an unstorable string or float")
    if _is_clean(original) and not _json_equal(_as_json(original), stored):
        violations.append("clean JSON was altered")
    return violations


class TestTextContract(unittest.TestCase):

    @given(text=_TEXT)
    @example(text=LIVE_TRACK_ID + "\x00")
    @example(text="a\x00\udce9")
    def test_cleaning_meets_the_text_contract(self, text: str) -> None:
        self.assertEqual(text_violations(text, pg_safe_text(text)), [])

    @given(text=_TEXT, pad=st.integers(1, 4))
    def test_trailing_nul_padding_is_invisible(self, text: str, pad: int) -> None:
        self.assertEqual(pg_safe_text(text + "\x00" * pad), pg_safe_text(text))


@requires_postgres
class TestRealWriterStoresEveryValue(unittest.TestCase):
    """Real ``log_download`` → real adapters → real PostgreSQL → read back."""

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

    def _stored(self, log_id: int) -> dict[str, object]:
        cur = self.db._execute(
            "SELECT error_message, candidate_contributor_usernames,"
            " transfer_detail FROM download_log WHERE id = %s",
            (log_id,),
        )
        row = cur.fetchone()
        assert row is not None
        return dict(row)

    @given(text=_TEXT, names=st.lists(_TEXT.filter(bool), max_size=3))
    @example(text="Transfer rejected: Banned\x00", names=["peer\udce9"])
    def test_text_and_text_array_parameters(
        self, text: str, names: list[str],
    ) -> None:
        log_id = self.db.log_download(
            self.request_id, outcome="failed", error_message=text,
            contributor_usernames=names,
        )
        stored = self._stored(log_id)
        error_message = stored["error_message"]
        assert isinstance(error_message, str)
        self.assertEqual(text_violations(text, error_message), [])
        stored_names = stored["candidate_contributor_usernames"] or []
        assert isinstance(stored_names, list)
        for name in stored_names:
            assert isinstance(name, str)
            self.assertEqual(_bad_count(name), 0)

    @given(payload=_JSON)
    @example(payload={"id": LIVE_TRACK_ID + "\x00", "ratio": math.nan})
    def test_json_text_parameters(self, payload: object) -> None:
        """Caller-encoded JSON text (stdlib, NaN tokens and escapes kept)."""
        log_id = self.db.log_download(
            self.request_id, outcome="failed", import_result=json.dumps(payload),
        )
        row = self.db._execute(
            "SELECT import_result FROM download_log WHERE id = %s", (log_id,),
        ).fetchone()
        assert row is not None
        stored = row["import_result"]
        if payload is None:
            self.assertIsNone(stored)
            return
        self.assertEqual(json_violations(payload, stored), [])

    @given(payload=_JSON)
    @example(payload=[{"items": [{"mb_trackid": LIVE_TRACK_ID + "\x00"}]}])
    @example(payload={"ratio": math.nan, "pair": ("caf\udce9", math.inf)})
    @example(payload=4.934925614736811e16)
    def test_json_parameters(self, payload: object) -> None:
        log_id = self.db.log_download(
            self.request_id, outcome="failed", transfer_detail=payload,
        )
        stored = self._stored(log_id)["transfer_detail"]
        if payload is None:
            self.assertIsNone(stored)
            return
        self.assertEqual(json_violations(payload, stored), [])


class TestCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests, one per clause, plus a quiet world per checker."""

    def test_text_unstorable_survivor(self) -> None:
        self.assertIn(
            "stored text holds an unstorable character",
            text_violations("a\x00", "a\x00"),
        )

    def test_text_clean_altered(self) -> None:
        self.assertIn("clean text was altered", text_violations("ab", "a"))

    def test_text_interior_not_marked(self) -> None:
        self.assertIn(
            "interior unstorable characters are not each marked once",
            text_violations("a\x00b", "ab"),
        )

    def test_text_quiet_when_padding_dropped(self) -> None:
        self.assertEqual(text_violations("a\x00b\x00", "a�b"), [])

    def test_json_unstorable_survivor(self) -> None:
        self.assertIn(
            "stored JSON holds an unstorable string or float",
            json_violations({"v": "x"}, {"v": math.nan}),
        )

    def test_json_clean_altered(self) -> None:
        self.assertIn(
            "clean JSON was altered",
            json_violations({"v": "x"}, {"v": "y"}),
        )

    def test_json_quiet_when_a_float_reads_back_in_integer_form(self) -> None:
        self.assertEqual(
            json_violations({"v": 4.934925614736811e16}, {"v": 49349256147368110}),
            [],
        )

    def test_json_bool_is_not_an_equal_number(self) -> None:
        self.assertIn(
            "clean JSON was altered", json_violations({"v": 1.0}, {"v": True}),
        )

    def test_json_quiet_when_dirty_input_is_cleaned(self) -> None:
        self.assertEqual(
            json_violations({"v": ("a\x00", math.inf)}, {"v": ["a", None]}),
            [],
        )


if __name__ == "__main__":
    unittest.main()
