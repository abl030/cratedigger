"""Bounded argparse destination/read audit for ``harness/import_one.py``."""

from __future__ import annotations

import argparse
import ast
import unittest
from pathlib import Path
from unittest.mock import patch

from harness import import_one

IMPORT_ONE_PATH = Path("harness/import_one.py")


def parser_destinations(parser: argparse.ArgumentParser) -> frozenset[str]:
    """Return data destinations while rejecting unsupported parser shapes."""
    destinations: set[str] = set()
    for action in parser._actions:
        if isinstance(action, argparse._HelpAction):
            continue
        assert not isinstance(action, argparse._SubParsersAction), (
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


def direct_loaded_request_attributes(source: str) -> frozenset[str]:
    """Return direct ``request.<attr>`` reads in the bounded source file."""
    return frozenset(
        node.attr
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Attribute)
        and isinstance(node.ctx, ast.Load)
        and isinstance(node.value, ast.Name)
        and node.value.id == "request"
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


def assert_every_destination_is_consumed(
    parser: argparse.ArgumentParser,
    source: str,
) -> None:
    """Require every parser destination to be READ, not merely copied.

    ``assert_argparse_destinations_match_reads`` used to carry this for
    free: the ``args.<attr>`` reads it counts sat at their points of use, so
    deleting a flag's only consumer turned it red. Since #1313 all 18 of
    them live inside ``ImportOneRequest.from_namespace``, and that check
    now proves only that argv is COPIED into the dataclass — a flag parsed,
    stored, and never looked at again would pass it. This is the other
    half: the same bounded grammar, one attribute name further on.
    """
    declared = parser_destinations(parser)
    consumed = direct_loaded_request_attributes(source)
    assert declared <= consumed, (
        "argparse destinations copied into ImportOneRequest but never read "
        f"as request.<attr>: {sorted(declared - consumed)!r}"
    )


class TestImportOneArgparseAudit(unittest.TestCase):
    def test_real_import_one_parser_destinations_match_direct_reads(self) -> None:
        source = IMPORT_ONE_PATH.read_text(encoding="utf-8")

        assert_argparse_destinations_match_reads(import_one.build_parser(), source)

    def test_every_real_destination_is_consumed_not_just_copied(self) -> None:
        source = IMPORT_ONE_PATH.read_text(encoding="utf-8")

        assert_every_destination_is_consumed(import_one.build_parser(), source)

    def test_a_copied_but_unread_destination_is_rejected(self) -> None:
        """The known-bad world: parsed, stored on the request, never read."""
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument("--target-format")
        parser.add_argument("--never-read")

        with self.assertRaisesRegex(
            AssertionError,
            "never read as request.<attr>: \\['never_read'\\]",
        ):
            assert_every_destination_is_consumed(
                parser,
                "value = request.target_format\n",
            )

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
            # Mirrors the real ``parse_args(args=None)`` signature —
            # ``ImportOneRequest.from_argv`` forwards its argv argument, and
            # a sentinel that could not accept one would fail for the wrong
            # reason.
            def parse_args(
                self, args: list[str] | None = None,
            ) -> argparse.Namespace:
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
