"""Generated companion for #663's descriptor-path authority pins.

The deterministic pins in ``test_path_authority.py`` cover the named attack
shapes.  This property ranges over arbitrary safe leaf names and both regular
and symlink targets: only the same descriptor-rooted regular file is readable.
"""

from __future__ import annotations

import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import tests._hypothesis_profiles  # noqa: F401
from hypothesis import given
from hypothesis import strategies as st

from lib.fs_authority import (
    FilesystemAuthorityError,
    open_directory_path,
    open_private_processing_root,
    open_regular_relative,
)
from lib.import_preview import (
    _snapshot_authorized_directory,
    remove_preview_snapshot,
)


_SAFE_COMPONENTS = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyz0123456789_-",
    min_size=1,
    max_size=32,
)


class TestGeneratedDescriptorAuthority(unittest.TestCase):
    @given(name=_SAFE_COMPONENTS, symlink_target=st.booleans())
    def test_only_regular_file_at_the_authorized_descriptor_is_readable(
        self,
        name: str,
        symlink_target: bool,
    ) -> None:
        with tempfile.TemporaryDirectory() as parent:
            root = os.path.join(parent, "root")
            outside = os.path.join(parent, "outside")
            os.mkdir(root)
            with open(outside, "wb") as handle:
                handle.write(b"outside")
            candidate = os.path.join(root, name)
            if symlink_target:
                os.symlink(outside, candidate)
            else:
                with open(candidate, "wb") as handle:
                    handle.write(b"owned")

            with open_directory_path(root) as root_fd:
                if symlink_target:
                    with self.assertRaises(FilesystemAuthorityError):
                        open_regular_relative(root_fd, name)
                else:
                    opened = open_regular_relative(root_fd, name)
                    try:
                        self.assertEqual(os.read(opened.fd, 16), b"owned")
                    finally:
                        opened.close()

    @given(unsafe_ancestor=st.booleans())
    def test_private_root_acceptance_tracks_ancestor_writability(
        self, unsafe_ancestor: bool,
    ) -> None:
        with tempfile.TemporaryDirectory(dir=os.getcwd()) as parent:
            source = os.path.join(parent, "source")
            container = os.path.join(parent, "container")
            processing = os.path.join(container, "processing")
            os.mkdir(source)
            os.mkdir(container, 0o777 if unsafe_ancestor else 0o755)
            os.chmod(container, 0o777 if unsafe_ancestor else 0o755)
            os.mkdir(processing, 0o700)
            if unsafe_ancestor:
                with self.assertRaises(FilesystemAuthorityError):
                    with open_private_processing_root(processing, source):
                        pass
            else:
                with open_private_processing_root(processing, source):
                    pass

    @given(entry_count=st.integers(min_value=0, max_value=6))
    def test_preview_snapshot_total_entry_limit_is_global(
        self, entry_count: int,
    ) -> None:
        """Nested traversal has one total ceiling, not per-directory limits."""
        with tempfile.TemporaryDirectory(dir=os.getcwd()) as parent:
            source = os.path.join(parent, "source")
            processing = os.path.join(parent, "processing")
            os.mkdir(source)
            os.mkdir(processing, 0o700)
            os.mkdir(os.path.join(processing, "albums"), 0o700)
            preview = os.path.join(processing, "preview")
            os.mkdir(preview, 0o700)
            for index in range(entry_count):
                nested = os.path.join(source, f"nested-{index}")
                os.mkdir(nested)
                with open(os.path.join(nested, "track.mp3"), "wb") as handle:
                    handle.write(b"audio")
            cfg = MagicMock()
            cfg.slskd_download_dir = source
            cfg.processing_dir = processing
            with patch("lib.import_preview._PREVIEW_MAX_ENTRIES", 3):
                if entry_count * 2 > 3:
                    with self.assertRaisesRegex(
                        FilesystemAuthorityError, "entry limit",
                    ):
                        _snapshot_authorized_directory(source, cfg)
                    self.assertEqual(os.listdir(preview), [])
                else:
                    snapshot = _snapshot_authorized_directory(source, cfg)
                    try:
                        self.assertEqual(len(os.listdir(snapshot)), entry_count)
                    finally:
                        remove_preview_snapshot(snapshot, cfg)


if __name__ == "__main__":
    unittest.main()
