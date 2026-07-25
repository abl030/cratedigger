"""Generated patrol for corrupt automation quarantine-root selection."""

from __future__ import annotations

import unittest

from hypothesis import given, strategies as st

from tests.test_dispatch_core import TestDispatchCoreOrchestration


def assert_quarantine_root_selection(*, observed: str | None, staging: str, slskd: str) -> None:
    """Independent oracle: corrupt automation quarantine belongs on staging."""
    if observed != staging or observed == slskd:
        raise AssertionError("corrupt automation quarantine selected slskd instead of staging")


class TestDispatchQuarantineRootGenerated(unittest.TestCase):
    @given(
        staging_name=st.from_regex(r"[a-z]{1,8}", fullmatch=True),
        slskd_name=st.from_regex(r"[a-z]{1,8}", fullmatch=True),
    )
    def test_real_dispatch_uses_staging_for_distinct_configured_roots(
        self, staging_name: str, slskd_name: str,
    ) -> None:
        if staging_name == slskd_name:
            return
        staging = f"/configured/{staging_name}"
        slskd = f"/configured/{slskd_name}"
        world = TestDispatchCoreOrchestration()._dispatch(
            candidate_kwargs={"audio_corrupt": True},
            beets_staging_dir=staging,
            slskd_download_dir=slskd,
        )
        cleanup = world["result"].post_commit_cleanup
        assert cleanup is not None
        assert_quarantine_root_selection(
            observed=cleanup.audio_quarantine_root, staging=staging, slskd=slskd,
        )

    def test_oracle_rejects_slskd_root(self) -> None:
        with self.assertRaisesRegex(AssertionError, "slskd"):
            assert_quarantine_root_selection(
                observed="/configured/slskd", staging="/configured/staging",
                slskd="/configured/slskd",
            )
