"""Generated patrol for corrupt automation quarantine-root selection."""

from __future__ import annotations

import os
import tempfile
import unittest

from hypothesis import assume, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from tests.test_dispatch_core import TestDispatchCoreOrchestration


def assert_quarantine_root_selection(
    *,
    observed: str | None,
    processing_albums: str,
    staging: str,
) -> None:
    """Independent oracle: owned quarantine stays on its processing root."""
    if observed != processing_albums or observed == staging:
        raise AssertionError(
            "owned quarantine escaped its canonical processing root"
        )


class TestDispatchQuarantineRootGenerated(unittest.TestCase):
    @given(
        staging_name=st.from_regex(r"[a-z]{1,8}", fullmatch=True),
        slskd_name=st.from_regex(r"[a-z]{1,8}", fullmatch=True),
    )
    def test_real_dispatch_uses_processing_for_distinct_configured_roots(
        self, staging_name: str, slskd_name: str,
    ) -> None:
        # Identical external roots are discarded so the world still proves
        # Incoming and slskd placement are irrelevant to owner-local cleanup.
        assume(staging_name != slskd_name)
        with tempfile.TemporaryDirectory() as root:
            processing = os.path.join(root, "processing")
            processing_albums = os.path.join(processing, "albums")
            staging = os.path.join(root, staging_name)
            slskd = os.path.join(root, slskd_name)
            os.makedirs(processing_albums)
            world = TestDispatchCoreOrchestration()._dispatch(
                candidate_kwargs={"audio_corrupt": True},
                beets_staging_dir=staging,
                slskd_download_dir=slskd,
                processing_dir=processing,
                path_parent=processing_albums,
                finalize=False,
            )
            cleanup = world["result"].post_commit_cleanup
            assert cleanup is not None
            assert_quarantine_root_selection(
                observed=cleanup.audio_quarantine_root,
                processing_albums=processing_albums,
                staging=staging,
            )

    def test_oracle_rejects_external_staging_root(self) -> None:
        with self.assertRaisesRegex(AssertionError, "escaped"):
            assert_quarantine_root_selection(
                observed="/configured/staging",
                processing_albums="/private/processing/albums",
                staging="/configured/staging",
            )
