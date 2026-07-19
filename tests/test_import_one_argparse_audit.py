"""Bounded argparse destination/read audit for ``harness/import_one.py``."""

from __future__ import annotations

import argparse
import ast
from pathlib import Path
import unittest
from unittest.mock import patch

from harness import import_one


IMPORT_ONE_PATH = Path("harness/import_one.py")


def parser_destinations(parser: argparse.ArgumentParser) -> frozenset[str]:
    """Return data destinations while rejecting unsupported parser shapes."""
    destinations: set[str] = set()
    for action in parser._actions:  # noqa: SLF001 - argparse has no public action API
        if isinstance(action, argparse._HelpAction):  # noqa: SLF001
            continue
        assert not isinstance(action, argparse._SubParsersAction), (  # noqa: SLF001
            "import_one argparse audit does not support subparsers"
        )
        assert action.default != argparse.SUPPRESS, (
            f"argparse.SUPPRESS default is not auditable for {action.dest!r}"
        )
        destinations.add(action.dest)
    return frozenset(destinations)


def direct_loaded_args_attributes(source: str) -> frozenset[str]:
    """Return only direct ``args.<attr>`` reads in the bounded source file."""
    return frozenset(
        node.attr
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Attribute)
        and isinstance(node.ctx, ast.Load)
        and isinstance(node.value, ast.Name)
        and node.value.id == "args"
    )


def assert_argparse_destinations_match_reads(
    parser: argparse.ArgumentParser,
    source: str,
) -> None:
    """Require exact parity between parser destinations and direct reads."""
    declared = parser_destinations(parser)
    read = direct_loaded_args_attributes(source)
    assert declared == read, (
        f"declared but unread argparse destinations: {sorted(declared - read)!r}; "
        f"read but undeclared args attributes: {sorted(read - declared)!r}"
    )


class TestImportOneArgparseAudit(unittest.TestCase):
    def test_real_import_one_parser_destinations_match_direct_reads(self) -> None:
        source = IMPORT_ONE_PATH.read_text(encoding="utf-8")

        assert_argparse_destinations_match_reads(import_one.build_parser(), source)

    def test_historical_filetype_read_is_rejected_including_conditionals(self) -> None:
        source = IMPORT_ONE_PATH.read_text(encoding="utf-8")

        with self.assertRaisesRegex(
            AssertionError,
            "read but undeclared.*filetype",
        ):
            assert_argparse_destinations_match_reads(
                import_one.build_parser(),
                source + "\nif args.filetype:\n    pass\n",
            )

    def test_hyphen_normalization_and_explicit_dest_use_real_actions(self) -> None:
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument("--target-format")
        parser.add_argument("--codec", dest="output_codec")

        self.assertEqual(
            parser_destinations(parser),
            frozenset({"target_format", "output_codec"}),
        )
        assert_argparse_destinations_match_reads(
            parser,
            "value = args.target_format\nother = args.output_codec\n",
        )

    def test_subparsers_are_rejected_by_the_bounded_checker(self) -> None:
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_subparsers(dest="command")

        with self.assertRaisesRegex(AssertionError, "does not support subparsers"):
            parser_destinations(parser)

    def test_suppress_defaults_are_rejected_by_the_bounded_checker(self) -> None:
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument("--maybe", default=argparse.SUPPRESS)

        with self.assertRaisesRegex(AssertionError, "SUPPRESS.*maybe"):
            parser_destinations(parser)

    def test_store_context_is_not_counted_as_a_read(self) -> None:
        self.assertEqual(
            direct_loaded_args_attributes("args.output = value\n"),
            frozenset(),
        )

    def test_main_gets_argv_from_the_one_parser_builder(self) -> None:
        class _ParserSentinel:
            def parse_args(self) -> argparse.Namespace:
                raise RuntimeError("parser sentinel")

        with (
            patch.object(import_one, "reset_umask"),
            patch.object(
                import_one,
                "build_parser",
                return_value=_ParserSentinel(),
            ) as build_parser,
            self.assertRaisesRegex(RuntimeError, "parser sentinel"),
        ):
            import_one.main()

        build_parser.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
