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
                "ImportTask metadata access is ambiguous.*an unexpected upstream shape"),
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
                "ImportTask metadata access is ambiguous.*an unrecognised upstream release"),
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
        """v2.1.0/v2.2.0 shape: the live defect the 19-leg matrix caught (#1088)."""
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
                "ImportTask metadata access is ambiguous.*an unrecognised upstream release"),
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

    def test_a_raising_source_property_propagates_not_swallowed(self) -> None:
        """A raising ``source`` property must propagate, not swallow to ``("", "")`` (#1088 review round 2 finding 1).

        ``AttributeError`` specifically: it's the one exception a 3-arg
        ``getattr(obj, name, default)`` catches and converts into the
        default, and it's the realistic trigger — beets' real
        ``dbcore.db.Model.__getattr__`` (``LibModel``'s base) raises
        exactly this for an absent field, reachable from inside
        ``Source.from_items`` -> ``get_most_common_tags``'s per-field
        ``item.get(...)`` calls. A ``RuntimeError`` fixture here would NOT
        distinguish the fixed 2-arg ``getattr`` from the old 3-arg
        ``getattr(..., None)`` — both let a non-``AttributeError`` through
        unchanged, so that fixture proved nothing.
        """
        class _ExplodingSource:
            @property
            def source(self) -> object:
                raise AttributeError("no such field 'mb_albumid'")

        modern = dataclasses.replace(beets_compat.CAPABILITIES, task_metadata_era="modern")
        with (
            patch.object(beets_compat, "CAPABILITIES", modern),
            self.assertRaisesRegex(AttributeError, "no such field"),
        ):
            beets_compat.task_description(_ExplodingSource())


def _library_module_with_album(album_class: type) -> ModuleType:
    """A ``beets.library`` stand-in for the Album duplicates-query probes.

    ``Library`` keeps a callable ``get_replacements`` so the library-era
    check stays deterministically modern; only the ``Album`` shape varies.
    """
    return _module(
        "beets.library",
        Album=album_class,
        Library=type("Library", (), {"get_replacements": lambda self: None}),
    )


class TestBeetsCompatDuplicatesQueryEra(TestCase):
    """Fail-closed pins for the ``Album`` duplicate-lookup builder era
    (#1278 wx6 — the era decision that lived inline in
    ``beets_harness.py``): ``all_fields_query`` is an inherited model
    classmethod present in BOTH eras, so this is a precedence probe — a
    callable ``duplicates_query`` decides modern, mirroring upstream
    ``ImportTask``'s own duplicate lookup — never the siblings'
    exactly-one ambiguity check."""

    def test_modern_era_when_album_has_duplicates_query(self) -> None:
        album = type("Album", (), {"duplicates_query": lambda self, keys: None})
        with patch.object(beets_compat, "library", _library_module_with_album(album)):
            capabilities = beets_compat._load_capabilities()
        self.assertEqual(capabilities.duplicates_query_era, "modern")

    def test_modern_wins_when_both_builders_are_present(self) -> None:
        """The realistic modern shape: ``all_fields_query`` never went away,
        so its presence must not drag a modern Beets onto the legacy path."""
        album = type("Album", (), {
            "duplicates_query": lambda self, keys: None,
            "all_fields_query": classmethod(lambda cls, by: None),
        })
        with patch.object(beets_compat, "library", _library_module_with_album(album)):
            capabilities = beets_compat._load_capabilities()
        self.assertEqual(capabilities.duplicates_query_era, "modern")

    def test_legacy_era_when_only_all_fields_query_is_present(self) -> None:
        album = type("Album", (), {
            "all_fields_query": classmethod(lambda cls, by: None),
        })
        with patch.object(beets_compat, "library", _library_module_with_album(album)):
            capabilities = beets_compat._load_capabilities()
        self.assertEqual(capabilities.duplicates_query_era, "legacy")

    def test_neither_builder_present_fails_loudly(self) -> None:
        album = type("Album", (), {})
        with (
            patch.object(beets_compat, "library", _library_module_with_album(album)),
            self.assertRaisesRegex(
                beets_compat.BeetsCapabilityError,
                "duplicate lookup.*unrecognised upstream release"),
        ):
            beets_compat._load_capabilities()


class TestBeetsCompatAlbumDuplicatesQuery(TestCase):
    """``album_duplicates_query`` builds the duplicate-detection query with
    the builder the active era actually has, replacing ``beets_harness``'s
    own inline ``hasattr(library.Album, "duplicates_query")`` probe
    (#1278 wx6)."""

    def test_modern_era_calls_the_instance_builder_with_keys(self) -> None:
        class _Album:
            def duplicates_query(self, keys: list[str]) -> object:
                return ("modern-query", tuple(keys))

        modern = dataclasses.replace(
            beets_compat.CAPABILITIES, duplicates_query_era="modern")
        with patch.object(beets_compat, "CAPABILITIES", modern):
            query = beets_compat.album_duplicates_query(
                _Album(), ["mb_albumid", "discogs_albumid"])
        self.assertEqual(
            query, ("modern-query", ("mb_albumid", "discogs_albumid")))

    def test_modern_era_without_callable_builder_fails_loudly(self) -> None:
        modern = dataclasses.replace(
            beets_compat.CAPABILITIES, duplicates_query_era="modern")
        with (
            patch.object(beets_compat, "CAPABILITIES", modern),
            self.assertRaisesRegex(
                beets_compat.BeetsCapabilityError, "duplicates_query"),
        ):
            beets_compat.album_duplicates_query(object(), ["mb_albumid"])

    def test_legacy_era_builds_the_field_mapping_in_key_order(self) -> None:
        class _Album:
            def __init__(self) -> None:
                self.fields: dict[str, object] = {
                    "mb_albumid": "mb-123", "discogs_albumid": 0}

            def __getitem__(self, key: str) -> object:
                return self.fields[key]

            @classmethod
            def all_fields_query(cls, by: dict[str, object]) -> object:
                return ("legacy-query", by)

        legacy = dataclasses.replace(
            beets_compat.CAPABILITIES, duplicates_query_era="legacy")
        with patch.object(beets_compat, "CAPABILITIES", legacy):
            query = beets_compat.album_duplicates_query(
                _Album(), ["mb_albumid", "discogs_albumid"])
        self.assertEqual(
            query,
            ("legacy-query", {"mb_albumid": "mb-123", "discogs_albumid": 0}))

    def test_legacy_era_without_callable_builder_fails_loudly(self) -> None:
        class _Album:
            all_fields_query = "not callable"

            def __getitem__(self, key: str) -> object:
                return None

        legacy = dataclasses.replace(
            beets_compat.CAPABILITIES, duplicates_query_era="legacy")
        with (
            patch.object(beets_compat, "CAPABILITIES", legacy),
            self.assertRaisesRegex(
                beets_compat.BeetsCapabilityError, "all_fields_query"),
        ):
            beets_compat.album_duplicates_query(_Album(), ["mb_albumid"])


if __name__ == "__main__":
    import unittest

    unittest.main()
