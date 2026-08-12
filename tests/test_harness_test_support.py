"""Import-state fidelity tests for synthetic Beets harness modules."""

from __future__ import annotations

import subprocess
import sys
import textwrap
import unittest
from types import ModuleType
from unittest.mock import MagicMock, patch

from tests.harness_test_support import isolated_beets_harness


def _mock_modules():
    modules = {
        "beets": MagicMock(),
        "beets.config": MagicMock(),
        "beets.library": MagicMock(),
        "beets.plugins": MagicMock(),
        "beets.ui": MagicMock(),
        "beets.importer": MagicMock(),
        "beets.importer.actions": MagicMock(),
        "beets.importer.session": MagicMock(),
        "beets.importer.tasks": MagicMock(),
        "beets.autotag": MagicMock(),
        "beets.dbcore": MagicMock(),
        "beets.util": MagicMock(),
    }
    modules["beets.ui"].get_path_formats = None
    modules["beets.ui"].get_replacements = None
    modules["beets.importer.session"].ImportSession = type(
        "ImportSession", (object,), {"resolve_duplicate": lambda *_args: None},
    )
    # Legacy task-metadata shape (cur_artist/cur_album), matching the
    # legacy resolve_duplicate hook above — one ImportTask attribute must
    # exist or harness/beets_compat.py's task-metadata era check (#1088)
    # fails closed as ambiguous.
    modules["beets.importer.tasks"].ImportTask = type(
        "ImportTask", (object,), {"cur_artist": None, "cur_album": None},
    )
    return modules


class TestIsolatedBeetsHarness(unittest.TestCase):
    def test_fresh_harness_uses_supplied_compatibility_modules(self) -> None:
        modules = _mock_modules()
        session = modules["beets.importer.session"].ImportSession
        task = modules["beets.importer.tasks"].ImportTask

        with isolated_beets_harness(modules) as harness:
            compatibility = harness.beets_compat
            self.assertIs(compatibility, sys.modules["harness.beets_compat"])
            self.assertIs(compatibility.CAPABILITIES.importer_session, session)
            self.assertIs(compatibility.CAPABILITIES.import_task, task)
            self.assertIs(harness.HarnessImportSession.__bases__[0], session)
            self.assertIs(harness.BeetsImportTask, task)

    def test_exception_restores_missing_parent_child_attribute(self) -> None:
        parent = ModuleType("beets")
        self.assertNotIn("config", vars(parent))
        with patch.dict(sys.modules, {"beets": parent}):
            with (
                self.assertRaisesRegex(RuntimeError, "body failure"),
                isolated_beets_harness(_mock_modules()),
            ):
                raise RuntimeError("body failure")
            self.assertIs(sys.modules["beets"], parent)
            self.assertNotIn("config", vars(parent))

    def test_nested_mock_state_and_exception_restore_in_fresh_process(self) -> None:
        script = textwrap.dedent("""
            import sys
            from unittest.mock import MagicMock
            from tests.harness_test_support import isolated_beets_harness

            def modules():
                result = {
                    "beets": MagicMock(), "beets.config": MagicMock(),
                    "beets.library": MagicMock(), "beets.plugins": MagicMock(),
                    "beets.ui": MagicMock(), "beets.importer": MagicMock(),
                    "beets.importer.actions": MagicMock(),
                    "beets.importer.session": MagicMock(),
                    "beets.importer.tasks": MagicMock(), "beets.autotag": MagicMock(),
                    "beets.dbcore": MagicMock(), "beets.util": MagicMock(),
                }
                result["beets.ui"].get_path_formats = None
                result["beets.ui"].get_replacements = None
                result["beets.importer.session"].ImportSession = type(
                    "ImportSession", (object,), {"resolve_duplicate": lambda *_: None},
                )
                result["beets.importer.tasks"].ImportTask = type(
                    "ImportTask", (object,), {"cur_artist": None, "cur_album": None},
                )
                return result

            assert "harness" not in sys.modules
            outer_modules = modules()
            inner_modules = modules()
            with isolated_beets_harness(outer_modules) as outer:
                outer_compat = outer.beets_compat
                try:
                    with isolated_beets_harness(inner_modules) as inner:
                        assert inner.beets_compat.CAPABILITIES.importer_session is inner_modules["beets.importer.session"].ImportSession
                        raise RuntimeError("restore outer state")
                except RuntimeError as error:
                    assert str(error) == "restore outer state"
                assert sys.modules["harness.beets_harness"] is outer
                assert sys.modules["harness.beets_compat"] is outer_compat
                assert sys.modules["harness"].beets_harness is outer
                assert sys.modules["harness"].beets_compat is outer_compat
            assert "harness" not in sys.modules
            assert "harness.beets_harness" not in sys.modules
            assert "harness.beets_compat" not in sys.modules
        """)
        subprocess.run([sys.executable, "-c", script], check=True, text=True)


if __name__ == "__main__":
    unittest.main()
