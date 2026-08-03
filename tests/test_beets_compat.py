"""Failure-closed import-shape pins for the Beets compatibility boundary."""

from __future__ import annotations

from types import ModuleType
from unittest import TestCase
from unittest.mock import patch

from harness import beets_compat


def _module(name: str, **members: object) -> ModuleType:
    module = ModuleType(name)
    for member, value in members.items():
        setattr(module, member, value)
    return module


class TestBeetsCompatibilityImports(TestCase):
    def test_partial_split_importer_fails_loudly(self) -> None:
        importer = _module("beets.importer", ImportSession=type("Session", (), {
            "resolve_duplicate": lambda self: None,
        }), ImportTask=type("Task", (), {}), Action=object())
        ui = _module("beets.ui", get_path_formats=lambda _: (), get_replacements=lambda: ())
        modules = {
            "beets.importer": importer,
            "beets.ui": ui,
            "beets.importer.session": _module("beets.importer.session", ImportSession=importer.ImportSession),
        }

        def import_module(name: str) -> ModuleType:
            if name == "beets.importer.tasks":
                raise ModuleNotFoundError(name=name)
            return modules[name]

        with (
            patch.object(beets_compat.importlib, "import_module", side_effect=import_module),
            self.assertRaisesRegex(beets_compat.BeetsCapabilityError, "partial"),
        ):
            beets_compat._load_capabilities()

    def test_nested_optional_import_error_preserves_its_cause(self) -> None:
        nested = ModuleNotFoundError("dependency vanished")
        nested.name = "dependency"

        def import_module(name: str) -> ModuleType:
            if name == "beets.importer.session":
                raise nested
            return _module(name)

        with (
            patch.object(beets_compat.importlib, "import_module", side_effect=import_module),
            self.assertRaises(beets_compat.BeetsCapabilityError) as caught,
        ):
            beets_compat._load_capabilities()
        self.assertIs(caught.exception.__cause__, nested)

    def test_exact_optional_pathbytes_absence_uses_bytes(self) -> None:
        # Current installed Beets supplies PathBytes; this pin qualifies the
        # narrow historical fallback by driving only that exact module absent.
        original = beets_compat.importlib.import_module

        def import_module(name: str) -> ModuleType:
            if name == "beets.util":
                raise ModuleNotFoundError(name=name)
            return original(name)

        with patch.object(beets_compat.importlib, "import_module", side_effect=import_module):
            capabilities = beets_compat._load_capabilities()
        self.assertIs(capabilities.path_bytes, bytes)

    def test_missing_modern_duplicate_actions_preserves_legacy_capability(self) -> None:
        original = beets_compat.importlib.import_module

        def import_module(name: str) -> ModuleType:
            if name == "beets.importer.actions":
                raise ModuleNotFoundError(name=name)
            return original(name)

        with (
            patch.object(beets_compat.importlib, "import_module", side_effect=import_module),
        ):
            capabilities = beets_compat._load_capabilities()
        self.assertEqual(capabilities.duplicate_era, "legacy")


if __name__ == "__main__":
    import unittest

    unittest.main()
