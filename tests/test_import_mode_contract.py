"""Executable import-mode contracts for issue #737."""

from __future__ import annotations

import argparse
import inspect
import unittest

import msgspec

from lib.import_preview import ImportPreviewValues
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_TYPES,
    IMPORT_JOB_YOUTUBE,
)
from lib.quality import (
    AlbumQualityEvidenceDecisionFacts,
    full_pipeline_decision,
)
from scripts.pipeline_cli.routes_meta import _build_parser
from web.routes.imports import ROUTES


class TestImportModeContract(unittest.TestCase):
    def test_manual_import_action_is_absent(self) -> None:
        self.assertEqual(
            IMPORT_JOB_TYPES,
            frozenset({
                IMPORT_JOB_AUTOMATION,
                IMPORT_JOB_FORCE,
                IMPORT_JOB_YOUTUBE,
            }),
        )
        parser, _, _ = _build_parser()
        subcommands = next(
            action.choices
            for action in parser._actions
            if isinstance(action, argparse._SubParsersAction)
        )
        self.assertNotIn("manual-import", subcommands)
        self.assertNotIn(
            "/api/manual-import/import",
            {registration.path for registration in ROUTES},
        )

    def test_quality_decider_has_no_caller_mode_input(self) -> None:
        self.assertNotIn(
            "import_mode",
            inspect.signature(full_pipeline_decision).parameters,
        )
        self.assertNotIn(
            "import_mode",
            {field.name for field in msgspec.structs.fields(
                AlbumQualityEvidenceDecisionFacts
            )},
        )
        self.assertNotIn(
            "import_mode",
            {field.name for field in msgspec.structs.fields(ImportPreviewValues)},
        )

    def test_import_preview_has_no_import_mode_option(self) -> None:
        parser, _, _ = _build_parser()
        subcommands = next(
            action.choices
            for action in parser._actions
            if isinstance(action, argparse._SubParsersAction)
        )
        preview_options = {
            option
            for action in subcommands["import-preview"]._actions
            for option in action.option_strings
        }
        self.assertNotIn("--import-mode", preview_options)


if __name__ == "__main__":
    unittest.main()
