"""Generated filesystem-boundary checks for processing paths.

Invariant: every component emitted by ``stage_to_ai_path`` fits ext4's
255-byte component cap, preserves its request suffix, is deterministic, and
does not collapse distinct overlong metadata onto one staging directory.
"""

from __future__ import annotations

import os
import unittest

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.processing_paths import MAX_PATH_COMPONENT_BYTES, stage_to_ai_path
from lib.staged_album import staged_filename
from tests.helpers import make_download_file

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


def staged_name_violations(name: str, basename: str) -> list[str]:
    """Collect every way a staged filename breaks its contract.

    Accumulating rather than short-circuiting so one violation can never
    mask another, and so each clause is independently reachable by a
    known-bad self-test.
    """
    violations: list[str] = []
    size = len(name.encode("utf-8"))
    if size > MAX_PATH_COMPONENT_BYTES:
        violations.append(
            f"staged name is {size} bytes, over the "
            f"{MAX_PATH_COMPONENT_BYTES}-byte cap"
        )
    if name in {"", ".", ".."}:
        violations.append(f"degenerate staged name {name!r} that _safe_relpath rejects")
    if "/" in name or "\\" in name:
        violations.append(f"staged name retained a path separator: {name!r}")
    if len(basename.encode("utf-8")) <= MAX_PATH_COMPONENT_BYTES and name != basename:
        violations.append(
            f"rewrote {basename!r} to {name!r} although it already fit"
        )
    stem, dot, extension = basename.rpartition(".")
    if (
        dot
        and stem
        and len(extension.encode("utf-8")) <= 16
        and not name.endswith(f"{dot}{extension}")
    ):
        violations.append(f"dropped extension {extension!r} from {basename!r}")
    return violations


def assert_staged_name_safe(name: str, basename: str) -> None:
    """Check the staged-filename byte, degeneracy, separator and extension contract."""
    violations = staged_name_violations(name, basename)
    if violations:
        raise AssertionError("; ".join(violations))


_FILENAME_TEXT = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",), max_codepoint=0x2FFFF)
    .filter(lambda c: c not in "/\\"),
    max_size=400,
)


class TestStagedFilenameProperties(unittest.TestCase):
    """Patrol ``staged_filename`` over peer-controlled names (request 8867)."""

    @given(
        directory=_FILENAME_TEXT,
        basename=_FILENAME_TEXT.filter(lambda s: s not in {"", ".", ".."}),
        disc=st.one_of(st.none(), st.integers(min_value=1, max_value=9)),
    )
    @example(directory="Album", basename="中" * 128 + ".flac", disc=None)
    @example(directory="Album", basename="ʅ" + "͡" * 200 + ".flac", disc=None)
    @example(directory="Album", basename="中" * 128 + ".flac", disc=2)
    @example(directory="Album", basename="01 - Track.flac", disc=None)
    def test_staged_names_are_bounded_and_deterministic(
        self, directory: str, basename: str, disc: int | None,
    ) -> None:
        file = make_download_file(filename=f"user\\{directory}\\{basename}")
        expected_source = basename
        if disc is not None:
            file.disk_no = disc
            file.disk_count = 2
            expected_source = f"Disk {disc} - {basename}"

        first = staged_filename(file)

        assert_staged_name_safe(first, expected_source)
        self.assertEqual(first, staged_filename(file))

    @given(basename=_FILENAME_TEXT.filter(lambda s: s not in {"", ".", ".."}))
    @example(basename="中" * 128 + ".flac")
    def test_distinct_remote_names_stay_distinct(self, basename: str) -> None:
        first = staged_filename(
            make_download_file(filename=f"user\\Album\\A{basename}"))
        second = staged_filename(
            make_download_file(filename=f"user\\Album\\B{basename}"))

        assert_distinct_stage_paths(first, second)


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    def test_stage_path_checker_rejects_overlong_component(self):
        bad = f"/staging/auto-import/Artist/{'x' * 256} [request-42]"
        with self.assertRaises(AssertionError):
            assert_stage_path_safe(bad, "/staging", 42)

    def test_distinctness_checker_rejects_collapsed_paths(self):
        with self.assertRaises(AssertionError):
            assert_distinct_stage_paths("/same", "/same")

    def test_staged_checker_rejects_overlong_name(self):
        with self.assertRaisesRegex(AssertionError, "over the 255-byte cap"):
            assert_staged_name_safe("x" * 256, "x" * 256)

    def test_staged_checker_rejects_degenerate_name(self):
        with self.assertRaisesRegex(AssertionError, "degenerate staged name"):
            assert_staged_name_safe("..", "..")

    def test_staged_checker_rejects_retained_separator(self):
        with self.assertRaisesRegex(AssertionError, "retained a path separator"):
            assert_staged_name_safe("a/b", "a/b")

    def test_staged_checker_rejects_rewriting_a_name_that_already_fit(self):
        with self.assertRaisesRegex(AssertionError, "although it already fit"):
            assert_staged_name_safe("changed.flac", "original.flac")

    def test_staged_checker_rejects_dropped_extension(self):
        with self.assertRaisesRegex(AssertionError, "dropped extension"):
            assert_staged_name_safe("x" * 200, f"{'x' * 300}.flac")


if __name__ == "__main__":
    unittest.main()
