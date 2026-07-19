"""Generated declared/read worlds for the bounded import_one argparse audit."""

from __future__ import annotations

import argparse
import keyword
import unittest

from hypothesis import example, given, strategies as st

import tests._hypothesis_profiles  # noqa: F401 - loads the active profile
from tests.test_import_one_argparse_audit import (
    assert_argparse_destinations_match_reads,
)

_destination = st.from_regex(
    r"[a-z][a-z0-9_]{0,12}",
    fullmatch=True,
).filter(lambda value: not keyword.iskeyword(value))
_destinations = st.sets(_destination, max_size=8)


def _parser_for(destinations: set[str]) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
    for destination in sorted(destinations):
        parser.add_argument(f"--option-{destination}", dest=destination)
    return parser


def _source_for(reads: set[str]) -> str:
    return "\n".join(f"consume(args.{destination})" for destination in sorted(reads))


def assert_union_argparse_contract(
    parser: argparse.ArgumentParser,
    source: str,
) -> None:
    """Known-bad checker that erases which side owns each destination."""
    from tests.test_import_one_argparse_audit import (
        direct_loaded_args_attributes,
        parser_destinations,
    )

    union = parser_destinations(parser) | direct_loaded_args_attributes(source)
    assert all(union)


class TestGeneratedImportOneArgparseAudit(unittest.TestCase):
    @given(destinations=_destinations)
    def test_matching_declared_and_read_worlds_pass(
        self,
        destinations: set[str],
    ) -> None:
        assert_argparse_destinations_match_reads(
            _parser_for(destinations),
            _source_for(destinations),
        )

    @example(declared=set(), read={"filetype"})
    @given(declared=_destinations, read=_destinations)
    def test_any_declared_read_drift_is_rejected(
        self,
        declared: set[str],
        read: set[str],
    ) -> None:
        if declared == read:
            return
        with self.assertRaises(AssertionError):
            assert_argparse_destinations_match_reads(
                _parser_for(declared),
                _source_for(read),
            )

    def test_known_bad_union_checker_accepts_undeclared_filetype(self) -> None:
        declared: set[str] = set()
        read = {"filetype"}

        assert_union_argparse_contract(
            _parser_for(declared),
            _source_for(read),
        )
        with self.assertRaisesRegex(AssertionError, "filetype"):
            assert_argparse_destinations_match_reads(
                _parser_for(declared),
                _source_for(read),
            )


if __name__ == "__main__":
    unittest.main()
