"""Pins the retained wire type and retirement of selector destruction."""

from __future__ import annotations

import re
import unittest
from pathlib import Path

import lib.beets_album_op as beets_album_op
from lib.beets_album_op import BeetsOpFailure


REPO_ROOT = Path(__file__).resolve().parent.parent


class TestTypedReturnContract(unittest.TestCase):
    def test_op_failure_fields_and_legacy_selector_default(self) -> None:
        failure = BeetsOpFailure(
            reason="timeout",
            detail="timed out after 30s",
            selector="id:42",
        )
        self.assertEqual(failure.reason, "timeout")
        self.assertEqual(failure.detail, "timed out after 30s")
        self.assertEqual(failure.selector, "id:42")
        self.assertEqual(
            BeetsOpFailure(reason="nonzero_rc", detail="rc=1").selector,
            "",
        )

    def test_op_failure_is_frozen(self) -> None:
        failure = BeetsOpFailure(reason="timeout", detail="x")
        with self.assertRaises(Exception):
            failure.detail = "y"  # type: ignore[misc]


class TestSelectorDestructiveBypassRetired(unittest.TestCase):
    PATTERNS = (
        re.compile(r'["\']beet["\']\s*,\s*["\'](?:remove|move)["\']'),
        re.compile(r'beet_bin\s*\(\s*\)\s*,\s*["\'](?:remove|move)["\']'),
        re.compile(r'BEET_BIN\s*,\s*["\'](?:remove|move)["\']'),
    )

    def test_selector_helpers_and_cleanup_module_are_absent(self) -> None:
        self.assertFalse((REPO_ROOT / "lib" / "release_cleanup.py").exists())
        for name in (
            "BeetsAlbumHandle",
            "BeetsOpResult",
            "remove_album",
            "remove_by_selector",
        ):
            with self.subTest(name=name):
                self.assertFalse(hasattr(beets_album_op, name))

    def test_production_never_constructs_raw_beet_remove_or_move(self) -> None:
        paths = [REPO_ROOT / "cratedigger.py"]
        for directory in ("lib", "harness", "scripts", "web"):
            paths.extend((REPO_ROOT / directory).rglob("*.py"))
        offending: list[str] = []
        for path in paths:
            text = path.read_text(encoding="utf-8")
            for lineno, line in enumerate(text.splitlines(), start=1):
                if any(pattern.search(line) for pattern in self.PATTERNS):
                    offending.append(
                        f"{path.relative_to(REPO_ROOT)}:{lineno}: {line.strip()}",
                    )
        self.assertEqual(
            offending,
            [],
            "selector-based Beets mutation bypasses pinned exact delete:\n"
            + "\n".join(offending),
        )


if __name__ == "__main__":
    unittest.main()
