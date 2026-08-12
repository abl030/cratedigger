"""Failure-closed import-shape pins for the Beets compatibility boundary."""

from __future__ import annotations

import dataclasses
from types import ModuleType, SimpleNamespace
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

    def test_missing_modern_duplicate_actions_fails_loudly(self) -> None:
        original = beets_compat.importlib.import_module

        def import_module(name: str) -> ModuleType:
            if name == "beets.importer.actions":
                raise ModuleNotFoundError(name=name)
            return original(name)

        with (
            patch.object(beets_compat.importlib, "import_module", side_effect=import_module),
            self.assertRaisesRegex(beets_compat.BeetsCapabilityError, "DuplicateAction"),
        ):
            beets_compat._load_capabilities()


def _import_module_stubbing_task_class(task_class: type):
    """``import_module`` side effect swapping ``ImportTask`` for
    ``task_class`` — every other resolved module (session, ui, actions,
    library) stays real, so only the task-metadata era ambiguity check is
    under test."""
    original = beets_compat.importlib.import_module

    def import_module(name: str) -> ModuleType:
        if name == "beets.importer.tasks":
            return _module(name, ImportTask=task_class)
        return original(name)

    return import_module


def _import_module_stubbing_task(task_attrs: dict[str, object]):
    """Variant of :func:`_import_module_stubbing_task_class` for a
    synthetic class exposing exactly the given CLASS-level attributes."""
    return _import_module_stubbing_task_class(type("Task", (), task_attrs))


class TestBeetsCompatTaskMetadataEra(TestCase):
    """Fail-closed pins for the ``ImportTask`` metadata-access era
    (issue #1088): upstream PR #6681 replaced ``cur_artist``/``cur_album``
    with a cached ``source`` property. Key on attribute presence, never on
    ``__version__`` — mirrors the duplicate/library era ambiguity checks."""

    def test_both_task_metadata_attributes_present_fails_loudly(self) -> None:
        with (
            patch.object(
                beets_compat.importlib, "import_module",
                side_effect=_import_module_stubbing_task(
                    {"source": None, "cur_artist": None}),
            ),
            self.assertRaisesRegex(
                beets_compat.BeetsCapabilityError,
                "ImportTask metadata access is ambiguous"),
        ):
            beets_compat._load_capabilities()

    def test_neither_task_metadata_attribute_present_fails_loudly(self) -> None:
        with (
            patch.object(
                beets_compat.importlib, "import_module",
                side_effect=_import_module_stubbing_task({}),
            ),
            self.assertRaisesRegex(
                beets_compat.BeetsCapabilityError,
                "ImportTask metadata access is ambiguous"),
        ):
            beets_compat._load_capabilities()

    def test_modern_task_metadata_era_is_source(self) -> None:
        with patch.object(
            beets_compat.importlib, "import_module",
            side_effect=_import_module_stubbing_task({"source": None}),
        ):
            capabilities = beets_compat._load_capabilities()
        self.assertEqual(capabilities.task_metadata_era, "modern")

    def test_legacy_task_metadata_era_is_cur_artist(self) -> None:
        with patch.object(
            beets_compat.importlib, "import_module",
            side_effect=_import_module_stubbing_task({"cur_artist": None}),
        ):
            capabilities = beets_compat._load_capabilities()
        self.assertEqual(capabilities.task_metadata_era, "legacy")

    def test_pre_2_3_0_init_only_cur_artist_still_resolves_legacy(self) -> None:
        """The exact live defect the 19-leg compat matrix caught (#1088):
        v2.1.0/v2.2.0's real ``ImportTask`` assigns ``cur_artist``/
        ``cur_album`` only inside ``__init__`` — never as a class
        attribute (unlike v2.3.0 onward, which the cheap ``hasattr(cls,
        ...)`` check above relies on). Both class-level checks report
        False/False here, so this pins the throwaway-probe-instance
        fallback that must still resolve "legacy", not "ambiguous"."""
        class _InitOnlyTask:
            def __init__(self, toppath: object, paths: object, items: object) -> None:
                self.toppath = toppath
                self.paths = paths
                self.items = items
                self.cur_artist = None
                self.cur_album = None

        # Guard the fixture itself: the class-level check this pin exists
        # to route AROUND must actually see nothing, or the probe fallback
        # is untested dead code.
        self.assertFalse(hasattr(_InitOnlyTask, "cur_artist"))
        self.assertFalse(hasattr(_InitOnlyTask, "source"))

        with patch.object(
            beets_compat.importlib, "import_module",
            side_effect=_import_module_stubbing_task_class(_InitOnlyTask),
        ):
            capabilities = beets_compat._load_capabilities()
        self.assertEqual(capabilities.task_metadata_era, "legacy")

    def test_a_ctor_that_raises_on_the_probe_still_fails_closed(self) -> None:
        """Neither class-level attribute AND a constructor that can't be
        probed (an unrecognised future shape) must stay ambiguous, not
        silently resolve to legacy."""
        class _UnprobeableTask:
            def __init__(self, *_args: object, **_kwargs: object) -> None:
                raise RuntimeError("this era needs different construction")

        with (
            patch.object(
                beets_compat.importlib, "import_module",
                side_effect=_import_module_stubbing_task_class(_UnprobeableTask),
            ),
            self.assertRaisesRegex(
                beets_compat.BeetsCapabilityError,
                "ImportTask metadata access is ambiguous"),
        ):
            beets_compat._load_capabilities()


class TestBeetsCompatTaskDescription(TestCase):
    """``task_description`` reads the attribute the active era actually has
    and normalises falsy values to ``""`` — the wire contract
    ``cur_artist``/``cur_album`` (``lib/quality/wire_types.py``) never
    carries ``None``."""

    def test_modern_era_reads_source_artist_and_name(self) -> None:
        task = SimpleNamespace(source=SimpleNamespace(artist="A", name="B"))
        modern = dataclasses.replace(beets_compat.CAPABILITIES, task_metadata_era="modern")
        with patch.object(beets_compat, "CAPABILITIES", modern):
            self.assertEqual(beets_compat.task_description(task), ("A", "B"))

    def test_legacy_era_reads_cur_artist_and_cur_album(self) -> None:
        task = SimpleNamespace(cur_artist="A", cur_album="B")
        legacy = dataclasses.replace(beets_compat.CAPABILITIES, task_metadata_era="legacy")
        with patch.object(beets_compat, "CAPABILITIES", legacy):
            self.assertEqual(beets_compat.task_description(task), ("A", "B"))

    def test_modern_era_normalises_none_to_empty_string(self) -> None:
        task = SimpleNamespace(source=SimpleNamespace(artist=None, name=None))
        modern = dataclasses.replace(beets_compat.CAPABILITIES, task_metadata_era="modern")
        with patch.object(beets_compat, "CAPABILITIES", modern):
            self.assertEqual(beets_compat.task_description(task), ("", ""))

    def test_legacy_era_normalises_none_to_empty_string(self) -> None:
        task = SimpleNamespace(cur_artist=None, cur_album=None)
        legacy = dataclasses.replace(beets_compat.CAPABILITIES, task_metadata_era="legacy")
        with patch.object(beets_compat, "CAPABILITIES", legacy):
            self.assertEqual(beets_compat.task_description(task), ("", ""))


if __name__ == "__main__":
    import unittest

    unittest.main()
