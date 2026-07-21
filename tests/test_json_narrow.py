"""Unit tests for the shared JSON-narrowing helpers (issue #809).

The helpers are one-liners, but three of their contracts are load-bearing and
easy to regress:

* ``json_dict`` / ``json_list`` GRACEFULLY DEGRADE a non-matching value to
  ``{}`` / ``[]`` rather than raising — several call sites depend on external
  JSON reading as absent, not crashing.
* ``json_dict`` on a plain dict is IDENTITY-PRESERVING (no element reshape).
* ``json_dict`` on a ``msgspec.Struct`` degrades to ``{}`` — where a raw
  ``msgspec.convert(struct, dict)`` RAISES ``ValidationError``. This is the
  concrete encoding of the #804 "convert is not identity" lesson: the guard
  turns a hard failure (or reshape) into graceful degradation.
"""

from __future__ import annotations

import unittest

import msgspec

from lib.json_narrow import (
    is_container_like,
    is_dict_like,
    is_list_like,
    is_object_list,
    is_str_object_dict,
    json_dict,
    json_list,
)


class _SampleStruct(msgspec.Struct):
    outcome: str
    request_id: int


class TestJsonDict(unittest.TestCase):
    def test_plain_dict_round_trips_identically(self) -> None:
        cases: list[dict[str, object]] = [
            {},
            {"a": 1, "b": "two"},
            {"nested": {"x": [1, 2, 3]}, "n": None},
        ]
        for value in cases:
            with self.subTest(value=value):
                self.assertEqual(json_dict(value), value)

    def test_non_dict_degrades_to_empty(self) -> None:
        # Graceful degradation — never an assertion — for every non-dict shape.
        for value in [None, 0, 1, "str", 3.5, [1, 2], (1, 2), True]:
            with self.subTest(value=value):
                self.assertEqual(json_dict(value), {})

    def test_struct_degrades_to_empty_where_raw_convert_raises(self) -> None:
        # The #804 lesson pinned as behaviour: a raw ``msgspec.convert`` of an
        # already-decoded value is NOT a safe no-op — on a Struct it raises
        # ValidationError. ``json_dict``'s isinstance guard degrades any non-dict
        # to ``{}`` instead, so it can never raise or silently reshape a
        # terminal-outcome payload mid-pipeline.
        struct = _SampleStruct(outcome="imported", request_id=42)
        with self.assertRaises(msgspec.ValidationError):
            msgspec.convert(struct, type=dict[str, object])
        self.assertEqual(json_dict(struct), {})


class TestJsonList(unittest.TestCase):
    def test_plain_list_round_trips_identically(self) -> None:
        cases: list[list[object]] = [[], [1, 2, 3], [{"a": 1}, "x", None]]
        for value in cases:
            with self.subTest(value=value):
                self.assertEqual(json_list(value), value)

    def test_non_list_degrades_to_empty(self) -> None:
        for value in [None, 0, "str", 3.5, {"a": 1}, (1, 2)]:
            with self.subTest(value=value):
                self.assertEqual(json_list(value), [])


class TestPlainBoolChecks(unittest.TestCase):
    def test_is_dict_like(self) -> None:
        for value, expected in [({"a": 1}, True), ({}, True), ([], False),
                                (None, False), ("s", False), ((1,), False)]:
            with self.subTest(value=value):
                self.assertIs(is_dict_like(value), expected)

    def test_is_list_like(self) -> None:
        for value, expected in [([], True), ([1], True), ({}, False),
                                (None, False), ("s", False), ((1,), False)]:
            with self.subTest(value=value):
                self.assertIs(is_list_like(value), expected)

    def test_is_container_like(self) -> None:
        for value, expected in [({}, True), ([], True), ((1,), True),
                                (None, False), ("s", False), (1, False)]:
            with self.subTest(value=value):
                self.assertIs(is_container_like(value), expected)


class TestTypeGuardChecks(unittest.TestCase):
    def test_is_str_object_dict_runtime_truth(self) -> None:
        for value, expected in [({"a": 1}, True), ({}, True), ([], False),
                                (None, False), ("s", False)]:
            with self.subTest(value=value):
                self.assertIs(is_str_object_dict(value), expected)

    def test_is_object_list_runtime_truth(self) -> None:
        for value, expected in [([], True), ([1], True), ({}, False),
                                (None, False), ("s", False)]:
            with self.subTest(value=value):
                self.assertIs(is_object_list(value), expected)

    def test_type_guards_actually_narrow(self) -> None:
        # Exercise the narrowing statically-and-at-runtime: the branch body
        # relies on the TypeGuard having narrowed ``value`` to a concrete type.
        value: object = {"k": "v"}
        if is_str_object_dict(value):
            self.assertEqual(list(value.items()), [("k", "v")])
        else:
            self.fail("expected dict narrowing")

        seq: object = [1, 2]
        if is_object_list(seq):
            self.assertEqual(seq[0], 1)
        else:
            self.fail("expected list narrowing")


if __name__ == "__main__":
    unittest.main()
