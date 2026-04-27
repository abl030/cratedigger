"""Tests for ``lib.beets_album_op`` (issue #133).

Two groups:

1. **Behavior tests** on ``remove_album`` / ``move_album`` /
   ``remove_by_selector``: subprocess clean exit, timeout, OSError,
   non-zero rc, and the ``fix_library_modes`` side effect for moves.
   Subprocess mocked via ``patch('lib.beets_album_op.sp.run', ...)``
   following the pattern from ``tests/test_release_cleanup.py``.

2. **Contract guard** (``TestBeetOpArgvIsCentralised``): greps every
   ``.py`` file in the repo for ``"beet", "remove"`` / ``"beet", "move"``
   argv fragments. The only hits allowed are this module and tests.
   The acceptance criterion from issue #133: "No callsite constructs
   its own ``beet remove -a -d id:<N>`` argv."
"""

from __future__ import annotations

import os
import re
import subprocess as sp
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from lib.beets_album_op import (BeetsAlbumHandle, BeetsOpFailure,
                                BeetsOpResult, move_album, remove_album,
                                remove_by_selector)

REPO_ROOT = Path(__file__).resolve().parent.parent


def _ok(stdout: str = "", stderr: str = "") -> MagicMock:
    return MagicMock(returncode=0, stdout=stdout, stderr=stderr)


def _rc(rc: int, stderr: str = "") -> MagicMock:
    return MagicMock(returncode=rc, stdout="", stderr=stderr)


# ---------------------------------------------------------------------------
# Typed return contract
# ---------------------------------------------------------------------------


class TestTypedReturnContract(unittest.TestCase):

    def test_op_failure_fields(self) -> None:
        """``BeetsOpFailure`` exposes reason, detail, selector."""
        f = BeetsOpFailure(
            reason="timeout", detail="timed out after 30s", selector="id:42")
        self.assertEqual(f.reason, "timeout")
        self.assertEqual(f.detail, "timed out after 30s")
        self.assertEqual(f.selector, "id:42")

    def test_op_failure_selector_defaults_to_empty(self) -> None:
        """Default ``selector=""`` keeps JSON round-trip backwards compatible
        with old ``PostflightInfo.disambiguation_failure`` rows that
        predate the field being added."""
        f = BeetsOpFailure(reason="nonzero_rc", detail="rc=1")
        self.assertEqual(f.selector, "")

    def test_op_failure_is_frozen(self) -> None:
        f = BeetsOpFailure(reason="timeout", detail="x")
        with self.assertRaises(Exception):
            # FrozenInstanceError subclasses AttributeError
            f.detail = "y"  # type: ignore[misc]

    def test_handle_wraps_album_id(self) -> None:
        """``BeetsAlbumHandle`` is a typed wrapper for the beets numeric
        primary key. The class exists (vs. a bare int) so callsites
        are self-documenting and future field additions don't break
        callsite signatures."""
        h = BeetsAlbumHandle(album_id=42)
        self.assertEqual(h.album_id, 42)

    def test_result_ok(self) -> None:
        r = BeetsOpResult(success=True, new_path="/Beets/Artist/Album")
        self.assertTrue(r.success)
        self.assertIsNone(r.failure)
        self.assertEqual(r.new_path, "/Beets/Artist/Album")

    def test_result_failure(self) -> None:
        f = BeetsOpFailure(reason="timeout", detail="x", selector="id:1")
        r = BeetsOpResult(success=False, failure=f)
        self.assertFalse(r.success)
        self.assertIs(r.failure, f)
        self.assertIsNone(r.new_path)


# ---------------------------------------------------------------------------
# remove_album
# ---------------------------------------------------------------------------


class TestRemoveAlbum(unittest.TestCase):

    @patch("lib.beets_album_op.sp.run")
    def test_clean_exit_returns_success(self, mock_run: MagicMock) -> None:
        mock_run.return_value = _ok()
        r = remove_album(BeetsAlbumHandle(album_id=42))
        self.assertTrue(r.success)
        self.assertIsNone(r.failure)

    @patch("lib.beets_album_op.sp.run")
    def test_argv_uses_album_mode_and_delete_by_default(
            self, mock_run: MagicMock) -> None:
        """``beet remove -a -d id:<N>`` — album mode + delete files by default."""
        mock_run.return_value = _ok()
        remove_album(BeetsAlbumHandle(album_id=42))
        argv = mock_run.call_args.args[0]
        self.assertEqual(argv[1:], ["remove", "-a", "-d", "id:42"])

    @patch("lib.beets_album_op.sp.run")
    def test_passes_affirmative_stdin_to_beets_prompt(
            self, mock_run: MagicMock) -> None:
        """``beet remove`` prompts "Really? (yes/[no])" before deleting.

        Live 2026-04-21: running from systemd (no tty) with stdin inherited,
        the prompt read EOF and exited rc=1 with "stdin stream ended while
        input required" — every upgrade post-import cleanup silently failed,
        leaving split-brain 2-row states. Fix: always pipe ``y\\n`` to stdin.
        """
        mock_run.return_value = _ok()
        remove_album(BeetsAlbumHandle(album_id=42))
        self.assertEqual(mock_run.call_args.kwargs.get("input"), "y\n")

    @patch("lib.beets_album_op.sp.run")
    def test_delete_files_false_omits_dash_d(
            self, mock_run: MagicMock) -> None:
        """Untag-only mode is available even though no production caller uses it."""
        mock_run.return_value = _ok()
        remove_album(BeetsAlbumHandle(album_id=42), delete_files=False)
        argv = mock_run.call_args.args[0]
        self.assertNotIn("-d", argv)
        self.assertEqual(argv[1:], ["remove", "-a", "id:42"])

    @patch("lib.beets_album_op.sp.run")
    def test_timeout_is_typed_failure(self, mock_run: MagicMock) -> None:
        mock_run.side_effect = sp.TimeoutExpired(
            cmd=["beet", "remove"], timeout=30)
        r = remove_album(BeetsAlbumHandle(album_id=42))
        self.assertFalse(r.success)
        assert r.failure is not None
        self.assertEqual(r.failure.reason, "timeout")
        self.assertEqual(r.failure.selector, "id:42")
        self.assertIn("30s", r.failure.detail)

    @patch("lib.beets_album_op.sp.run")
    def test_oserror_is_typed_failure(self, mock_run: MagicMock) -> None:
        mock_run.side_effect = FileNotFoundError("beet")
        r = remove_album(BeetsAlbumHandle(album_id=42))
        self.assertFalse(r.success)
        assert r.failure is not None
        self.assertEqual(r.failure.reason, "exception")
        self.assertEqual(r.failure.selector, "id:42")
        self.assertIn("FileNotFoundError", r.failure.detail)

    @patch("lib.beets_album_op.sp.run")
    def test_nonzero_rc_is_typed_failure(self, mock_run: MagicMock) -> None:
        mock_run.return_value = _rc(1, stderr="bad selector\n")
        r = remove_album(BeetsAlbumHandle(album_id=42))
        self.assertFalse(r.success)
        assert r.failure is not None
        self.assertEqual(r.failure.reason, "nonzero_rc")
        self.assertIn("rc=1", r.failure.detail)
        self.assertEqual(r.failure.selector, "id:42")


# ---------------------------------------------------------------------------
# move_album
# ---------------------------------------------------------------------------


class TestMoveAlbum(unittest.TestCase):

    def _beets(self, new_path: str | None = "/Beets/Artist/Album [2007]"
               ) -> MagicMock:
        beets = MagicMock()
        beets.get_album_path_by_id.return_value = new_path
        return beets

    @patch("lib.beets_album_op.sp.run")
    def test_clean_exit_reads_new_path_and_repairs_perms(
            self, mock_run: MagicMock) -> None:
        # ``fix_library_modes`` is imported lazily inside ``move_album``;
        # patch the source module so the deferred lookup resolves to the mock.
        mock_run.return_value = _ok()
        with patch("lib.permissions.fix_library_modes") as mock_fix:
            r = move_album(BeetsAlbumHandle(album_id=42), self._beets())
        self.assertTrue(r.success)
        self.assertEqual(r.new_path, "/Beets/Artist/Album [2007]")
        mock_fix.assert_called_once_with("/Beets/Artist/Album [2007]")

    @patch("lib.beets_album_op.sp.run")
    def test_argv_uses_album_mode_pk_selector(
            self, mock_run: MagicMock) -> None:
        mock_run.return_value = _ok()
        beets = self._beets()
        with patch("lib.permissions.fix_library_modes"):
            move_album(BeetsAlbumHandle(album_id=42), beets)
        argv = mock_run.call_args.args[0]
        self.assertEqual(argv[1:], ["move", "-a", "id:42"])
        # -d must NEVER appear on a move
        self.assertNotIn("-d", argv)

    @patch("lib.beets_album_op.sp.run")
    def test_timeout_skips_perm_repair_and_returns_failure(
            self, mock_run: MagicMock) -> None:
        mock_run.side_effect = sp.TimeoutExpired(
            cmd=["beet", "move"], timeout=120)
        beets = self._beets()
        with patch("lib.permissions.fix_library_modes") as mock_fix:
            r = move_album(BeetsAlbumHandle(album_id=42), beets)
        self.assertFalse(r.success)
        assert r.failure is not None
        self.assertEqual(r.failure.reason, "timeout")
        self.assertIsNone(r.new_path)
        beets.get_album_path_by_id.assert_not_called()
        mock_fix.assert_not_called()

    @patch("lib.beets_album_op.sp.run")
    def test_nonzero_rc_skips_perm_repair(self, mock_run: MagicMock) -> None:
        mock_run.return_value = _rc(2, stderr="no matching albums\n")
        beets = self._beets()
        with patch("lib.permissions.fix_library_modes") as mock_fix:
            r = move_album(BeetsAlbumHandle(album_id=42), beets)
        self.assertFalse(r.success)
        assert r.failure is not None
        self.assertEqual(r.failure.reason, "nonzero_rc")
        beets.get_album_path_by_id.assert_not_called()
        mock_fix.assert_not_called()

    @patch("lib.beets_album_op.sp.run")
    def test_missing_path_after_move_returns_none_but_success(
            self, mock_run: MagicMock) -> None:
        """If the album row vanished between move and lookup (should not
        happen in normal operation) the move still reports success; the
        caller loses the new path but we don't synthesize a failure."""
        mock_run.return_value = _ok()
        beets = self._beets(new_path=None)
        with patch("lib.permissions.fix_library_modes") as mock_fix:
            r = move_album(BeetsAlbumHandle(album_id=42), beets)
        self.assertTrue(r.success)
        self.assertIsNone(r.new_path)
        mock_fix.assert_not_called()


# ---------------------------------------------------------------------------
# remove_by_selector
# ---------------------------------------------------------------------------


class TestRemoveBySelector(unittest.TestCase):

    @patch("lib.beets_album_op.sp.run")
    def test_passes_arbitrary_selector_and_uses_album_mode(
            self, mock_run: MagicMock) -> None:
        mock_run.return_value = _ok()
        result = remove_by_selector("mb_albumid:abc-uuid")
        self.assertIsNone(result)
        argv = mock_run.call_args.args[0]
        self.assertEqual(
            argv[1:], ["remove", "-a", "-d", "mb_albumid:abc-uuid"])

    @patch("lib.beets_album_op.sp.run")
    def test_non_id_selector_failure_records_selector(
            self, mock_run: MagicMock) -> None:
        mock_run.return_value = _rc(1, stderr="boom\n")
        f = remove_by_selector("discogs_albumid:12856590")
        assert f is not None
        self.assertEqual(f.reason, "nonzero_rc")
        self.assertEqual(f.selector, "discogs_albumid:12856590")


# ---------------------------------------------------------------------------
# Argv centralisation contract guard
# ---------------------------------------------------------------------------


class TestBeetOpArgvIsCentralised(unittest.TestCase):
    """Issue #133 acceptance: grep the repo for raw ``beet remove``/``beet move``
    argv construction. The only allowed callsites are this module and
    tests (tests intentionally write argv as literals to verify shapes).

    Enforcement mechanism: walk the Python source tree and fail if any
    file outside the allowlist contains a matching pattern. New callers
    must route through ``lib.beets_album_op``.

    Patterns matched (each catches a different natural construction):
    - Literal string:   ``"beet", "remove"`` or ``'beet', 'move'``
    - Wrapper function: ``beet_bin(), "remove"`` / ``beet_bin(), "move"``
    - Wrapper constant: ``BEET_BIN, "remove"`` / ``BEET_BIN, "move"``

    ``beet_bin()`` is the canonical way most of the codebase resolves the
    binary (``lib/util.py::beet_bin``), so catching that shape too was
    the most likely bypass (the first version of this guard only
    matched the literal ``"beet"`` form — the entire production code
    base used ``beet_bin()`` and would have slipped through silently).

    Inevitable gaps: fully-dynamic argv (``[cmd, verb, *flags]`` where
    cmd/verb are variables) can't be caught by grep. That's fine — the
    guard covers every natural-looking new callsite; a truly obfuscated
    argv construction would require deliberate effort to add and stand
    out in code review on its own.
    """

    PATTERNS = [
        # Literal "beet" / 'beet' — shape used by test argv assertions.
        re.compile(r'["\']beet["\']\s*,\s*["\'](?:remove|move)["\']'),
        # beet_bin() wrapper — the production code's canonical form.
        re.compile(r'beet_bin\s*\(\s*\)\s*,\s*["\'](?:remove|move)["\']'),
        # BEET_BIN constant alias — legacy, may still live in some
        # harness-adjacent code paths.
        re.compile(r'BEET_BIN\s*,\s*["\'](?:remove|move)["\']'),
    ]

    # Files allowed to construct raw ``beet remove``/``beet move`` argv.
    # The op module itself is the only production allowlist entry.
    # Test modules are allowed to write argv literals in assertions —
    # they document the expected shape and would be unreadable forced
    # through the op wrapper.
    ALLOWED_FILES = frozenset({
        "lib/beets_album_op.py",
        # Tests that assert argv shapes — intentional literals:
        "tests/test_beets_album_op.py",
        "tests/test_release_cleanup.py",
        "tests/test_disambiguation.py",
    })

    # Directories ignored entirely (not Python source we own):
    IGNORE_DIRS = {
        ".git", "__pycache__", ".venv", "venv", "result",
        "node_modules", ".mypy_cache", ".pytest_cache",
    }

    def test_allowlist_entries_still_exist(self) -> None:
        """Fail loud if an allowlist entry is stale (file renamed or
        removed). Prevents the allowlist silently protecting a file
        that no longer exists while the rename now bypasses the guard
        with a new path."""
        for rel in self.ALLOWED_FILES:
            abs_path = REPO_ROOT / rel
            self.assertTrue(
                abs_path.is_file(),
                f"ALLOWED_FILES entry {rel!r} does not exist. Remove "
                f"stale entries; the file may have been renamed.")

    def test_no_file_outside_allowlist_constructs_beet_argv(self) -> None:
        offending: list[tuple[str, int, str]] = []
        for root, dirs, files in os.walk(REPO_ROOT):
            dirs[:] = [d for d in dirs if d not in self.IGNORE_DIRS]
            for name in files:
                if not name.endswith(".py"):
                    continue
                abs_path = Path(root) / name
                rel = abs_path.relative_to(REPO_ROOT).as_posix()
                if rel in self.ALLOWED_FILES:
                    continue
                try:
                    text = abs_path.read_text(encoding="utf-8")
                except (OSError, UnicodeDecodeError):
                    continue
                for lineno, line in enumerate(text.splitlines(), start=1):
                    for pat in self.PATTERNS:
                        if pat.search(line):
                            offending.append((rel, lineno, line.strip()))
        if offending:
            lines = [
                f"  {rel}:{lineno}: {text}" for rel, lineno, text in offending]
            self.fail(
                "The following files construct raw `beet remove` / "
                "`beet move` argv outside the allowlist. Route them "
                "through lib.beets_album_op (remove_album / move_album / "
                "remove_by_selector) or, if the grep is a false positive, "
                "add the file to ALLOWED_FILES with a comment.\n"
                + "\n".join(lines))


if __name__ == "__main__":
    unittest.main()
