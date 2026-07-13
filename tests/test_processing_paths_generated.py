"""Generated filesystem-boundary checks for processing paths.

Invariant: every component emitted by ``stage_to_ai_path`` fits ext4's
255-byte component cap, preserves its request suffix, is deterministic, and
does not collapse distinct overlong metadata onto one staging directory.
"""

from __future__ import annotations

import os
import unittest

from hypothesis import example, given, strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.processing_paths import stage_to_ai_path


_UNICODE_METADATA = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",), max_codepoint=0x2FFFF),
    max_size=400,
)


def assert_stage_path_safe(path: str, staging_dir: str, request_id: int) -> None:
    """Check the staging component-size, suffix, and containment contract."""
    relative = os.path.relpath(path, staging_dir)
    if relative == os.pardir or relative.startswith(f"{os.pardir}{os.sep}"):
        raise AssertionError(f"stage path escaped its root: {path!r}")
    components = relative.split(os.sep)
    for component in components:
        size = len(component.encode("utf-8"))
        if size > 255:
            raise AssertionError(
                f"stage component is {size} bytes, exceeds ext4's 255-byte cap"
            )
    suffix = f" [request-{request_id}]"
    if not components[-1].endswith(suffix):
        raise AssertionError(
            f"album component {components[-1]!r} lost request suffix {suffix!r}"
        )


def assert_distinct_stage_paths(first: str, second: str) -> None:
    """Check that distinct metadata did not collapse to one directory."""
    if first == second:
        raise AssertionError(f"distinct metadata collapsed to {first!r}")


class TestStagePathProperties(unittest.TestCase):
    @given(
        artist=_UNICODE_METADATA,
        title=_UNICODE_METADATA,
        request_id=st.integers(min_value=1, max_value=2**63 - 1),
        auto_import=st.booleans(),
    )
    @example(
        artist="⣎⡇ꉺლ༽இ•̛)ྀ◞ ༎ຶ ༽ৣৢ؞ৢ؞ؖ ꉺლ",
        title="ʅ" + "͡" * 182,
        request_id=42,
        auto_import=True,
    )
    def test_components_are_bounded_suffix_preserved_and_deterministic(
        self, artist: str, title: str, request_id: int, auto_import: bool,
    ) -> None:
        kwargs = {
            "artist": artist,
            "title": title,
            "staging_dir": "/staging",
            "request_id": request_id,
            "auto_import": auto_import,
        }
        first = stage_to_ai_path(**kwargs)
        second = stage_to_ai_path(**kwargs)

        assert_stage_path_safe(first, "/staging", request_id)
        self.assertEqual(first, second)

    @given(
        artist=_UNICODE_METADATA,
        title=_UNICODE_METADATA,
        request_id=st.integers(min_value=1, max_value=2**63 - 1),
    )
    @example(
        artist="Artist",
        title="ʅ" + "͡" * 182,
        request_id=42,
    )
    def test_distinct_titles_remain_distinct(
        self, artist: str, title: str, request_id: int,
    ) -> None:
        common = {
            "artist": artist,
            "staging_dir": "/staging",
            "request_id": request_id,
            "auto_import": True,
        }
        first = stage_to_ai_path(title=f"{title}A", **common)
        second = stage_to_ai_path(title=f"{title}B", **common)

        assert_distinct_stage_paths(first, second)


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    def test_stage_path_checker_rejects_overlong_component(self):
        bad = f"/staging/auto-import/Artist/{'x' * 256} [request-42]"
        with self.assertRaises(AssertionError):
            assert_stage_path_safe(bad, "/staging", 42)

    def test_distinctness_checker_rejects_collapsed_paths(self):
        with self.assertRaises(AssertionError):
            assert_distinct_stage_paths("/same", "/same")


if __name__ == "__main__":
    unittest.main()
