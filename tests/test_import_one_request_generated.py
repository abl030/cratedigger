"""Generated patrol for import_one's argv → ``ImportOneRequest`` adapter.

``main`` is a thin argv adapter now: every fact the import run uses comes off
one typed value built in ``ImportOneRequest.from_argv``. That makes the
adapter the single place an operator's flag can go missing or land on the
wrong field, and neither failure is loud — a dropped ``--beets-library-db``
just silently imports against ambient authority.

The flag spellings, kinds, and positionals below are DERIVED from the
dataclass and from the parser itself, never hand-listed: a field whose type
this module cannot classify raises rather than being skipped.
"""

from __future__ import annotations

import argparse
import dataclasses
import string
import types
import unittest
from unittest.mock import patch

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - loads the active profile
from harness.import_one import (
    DEFAULT_MAX_DISTANCE,
    ImportOneRequest,
    apply_max_distance,
    build_parser,
)
from lib.beets import FORCE_IMPORT_DISTANCE_THRESHOLD

#: The two positionals, in argv order. Taken from the parser rather than
#: assumed: ``parse_args`` refuses a run that supplies them out of order, so
#: a strategy that guessed would fail loudly rather than silently.
_POSITIONALS: tuple[str, ...] = ("path", "mb_release_id")


def _flag(field_name: str) -> str:
    """The long flag argparse derives for this destination."""
    return "--" + field_name.replace("_", "-")


def _optional_fields() -> tuple[dataclasses.Field[object], ...]:
    return tuple(
        field
        for field in dataclasses.fields(ImportOneRequest)
        if field.name not in _POSITIONALS
    )


def _kind(field: dataclasses.Field[object]) -> str:
    """Classify one optional field as ``switch``, ``int`` or ``str``.

    Fails closed: a field this cannot classify stops the module instead of
    quietly dropping out of the generated argv, which would leave the very
    flag most likely to be mis-wired unpatrolled.
    """
    annotation = field.type
    if annotation is bool:
        return "switch"
    if isinstance(annotation, types.UnionType):
        members = set(annotation.__args__) - {type(None)}
        if members == {int}:
            return "int"
        if members == {str}:
            return "str"
    raise AssertionError(
        f"ImportOneRequest.{field.name}: unclassifiable annotation "
        f"{annotation!r}; teach _kind about it rather than skipping it"
    )


#: Values this CLI can actually carry. A leading ``-`` is read by argparse as
#: the start of another flag, so no invocation of ``import_one.py`` can pass
#: one as a value; the same goes for a negative integer. Excluding them keeps
#: the world space to argv this parser accepts rather than argv it rejects.
_TOKEN = st.builds(
    lambda head, tail: head + tail,
    st.sampled_from(string.ascii_letters + string.digits + "/._"),
    st.text(
        alphabet=string.ascii_letters + string.digits + "/._-",
        max_size=23,
    ),
)


def _value_strategy(kind: str) -> st.SearchStrategy[object]:
    if kind == "int":
        return st.integers(min_value=0, max_value=4096)
    return _TOKEN


@st.composite
def _argv_worlds(draw: st.DrawFn) -> list[str]:
    """One complete argv: both positionals plus any subset of the flags."""
    argv: list[str] = [draw(_TOKEN), draw(_TOKEN)]
    for field in _optional_fields():
        kind = _kind(field)
        if not draw(st.booleans()):
            continue
        if kind == "switch":
            argv.append(_flag(field.name))
        else:
            argv.append(_flag(field.name))
            argv.append(str(draw(_value_strategy(kind))))
    return argv


def argv_adapter_violations(
    namespace: argparse.Namespace,
    request: ImportOneRequest,
) -> list[str]:
    """Every way one parsed argv can fail to reach the typed request.

    Accumulating rather than short-circuiting: each clause evaluates on
    every world, so an earlier violation cannot mask a later one.
    """
    violations: list[str] = []
    parsed = vars(namespace)
    fields = {field.name for field in dataclasses.fields(ImportOneRequest)}

    for dest in sorted(set(parsed) - fields):
        violations.append(
            f"V1 parser destination {dest!r} has no ImportOneRequest field"
        )
    for name in sorted(fields - set(parsed)):
        violations.append(
            f"V2 ImportOneRequest field {name!r} has no parser destination"
        )
    for name in sorted(fields & set(parsed)):
        if getattr(request, name) != parsed[name]:
            violations.append(
                f"V3 field {name!r} carries {getattr(request, name)!r} "
                f"but argv parsed {parsed[name]!r}"
            )

    expected_ceiling = (
        FORCE_IMPORT_DISTANCE_THRESHOLD
        if request.force
        else DEFAULT_MAX_DISTANCE
    )
    if apply_max_distance(request.force) != expected_ceiling:
        violations.append(
            f"V4 force={request.force} derives ceiling "
            f"{apply_max_distance(request.force)}, expected "
            f"{expected_ceiling}"
        )
    return violations


class TestArgvAdapter(unittest.TestCase):
    @given(_argv_worlds())
    def test_every_argv_world_reaches_the_typed_request(
        self, argv: list[str],
    ) -> None:
        namespace = build_parser().parse_args(argv)
        request = ImportOneRequest.from_namespace(namespace)

        self.assertEqual(argv_adapter_violations(namespace, request), [])

    @given(_argv_worlds())
    def test_from_argv_and_from_namespace_agree(
        self, argv: list[str],
    ) -> None:
        """``from_argv`` is ``parse_args`` plus ``from_namespace``, exactly."""
        self.assertEqual(
            ImportOneRequest.from_argv(argv),
            ImportOneRequest.from_namespace(build_parser().parse_args(argv)),
        )


class TestArgvAdapterCheckerTripsOnViolations(unittest.TestCase):
    """One message-asserting known-bad world per clause."""

    def _world(self) -> tuple[argparse.Namespace, ImportOneRequest]:
        namespace = build_parser().parse_args(["/album", "mbid-1"])
        return namespace, ImportOneRequest.from_namespace(namespace)

    def test_v1_trips_on_a_parser_destination_with_no_field(self) -> None:
        namespace, request = self._world()
        namespace.brand_new_flag = "x"

        self.assertIn(
            "V1 parser destination 'brand_new_flag' has no "
            "ImportOneRequest field",
            argv_adapter_violations(namespace, request),
        )

    def test_v2_trips_on_a_field_with_no_parser_destination(self) -> None:
        namespace, request = self._world()
        namespace = argparse.Namespace(**{
            dest: value
            for dest, value in vars(namespace).items()
            if dest != "target_format"
        })

        self.assertIn(
            "V2 ImportOneRequest field 'target_format' has no parser "
            "destination",
            argv_adapter_violations(namespace, request),
        )

    def test_v3_trips_on_a_cross_wired_field(self) -> None:
        namespace = build_parser().parse_args(
            ["/album", "mbid-1", "--beets-library-root", "/library"])
        cross_wired = dataclasses.replace(
            ImportOneRequest.from_namespace(namespace),
            beets_library_db="/library",
        )

        self.assertIn(
            "V3 field 'beets_library_db' carries '/library' but argv "
            "parsed None",
            argv_adapter_violations(namespace, cross_wired),
        )

    def test_v4_trips_when_the_derived_ceiling_disagrees_with_force(
        self,
    ) -> None:
        namespace, request = self._world()
        forced = dataclasses.replace(request, force=True)

        with patch(
            "tests.test_import_one_request_generated.apply_max_distance",
            lambda _force: DEFAULT_MAX_DISTANCE,
        ):
            violations = argv_adapter_violations(namespace, forced)

        self.assertTrue(
            any(v.startswith("V4 force=True") for v in violations),
            violations,
        )


class TestFieldKindClassification(unittest.TestCase):
    def test_every_optional_field_classifies(self) -> None:
        for field in _optional_fields():
            with self.subTest(field=field.name):
                self.assertIn(_kind(field), {"switch", "int", "str"})

    def test_an_unclassifiable_annotation_fails_closed(self) -> None:
        mystery = dataclasses.field()
        mystery.name = "mystery"
        mystery.type = float

        with self.assertRaisesRegex(AssertionError, "unclassifiable"):
            _kind(mystery)


if __name__ == "__main__":
    unittest.main()
