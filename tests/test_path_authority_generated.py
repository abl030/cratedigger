"""Generated companion for #663's descriptor-path authority pins.

The deterministic pins in ``test_path_authority.py`` cover the named attack
shapes.  This property ranges over arbitrary safe leaf names and both regular
and symlink targets: only the same descriptor-rooted regular file is readable.
"""

from __future__ import annotations

import os
import tempfile
import unittest

import tests._hypothesis_profiles  # noqa: F401
from hypothesis import given
from hypothesis import strategies as st

from lib.fs_authority import (
    FilesystemAuthorityError,
    open_directory_path,
    open_regular_relative,
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


if __name__ == "__main__":
    unittest.main()
