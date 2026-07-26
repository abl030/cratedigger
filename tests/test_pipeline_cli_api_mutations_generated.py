"""Generated boundary proof for API-backed CLI response handling (CD-QUAL-01)."""

from __future__ import annotations

import io
import unittest
from unittest.mock import patch

import tests._hypothesis_profiles  # noqa: F401
from hypothesis import given, strategies as st

from scripts.pipeline_cli import api_mutations


def _status_has_expected_exit(status: int, exit_code: int) -> bool:
    """Checker kept separate so its known-bad test proves it can fail."""
    expected = 0 if 200 <= status < 300 else 2 if status == 404 else 3 if status in (400, 422) else 4 if status == 409 else 5
    return exit_code == expected


class _Response:
    def __init__(self, status: int) -> None:
        self.status = status

    def read(self) -> bytes:
        return b'{"status":"route"}'

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *_values: object) -> None:
        return None


class TestApiMutationGenerated(unittest.TestCase):
    @given(status=st.integers(min_value=100, max_value=599))
    def test_real_relay_obeys_every_http_status_class(self, status: int) -> None:
        with patch("scripts.pipeline_cli.api_mutations.urllib.request.OpenerDirector.open",
                   return_value=_Response(status)), patch("sys.stdout", new_callable=io.StringIO):
            actual = api_mutations._relay("http://api", api_mutations._ApiMutation(
                path="/api/pipeline/upgrade", body={"mb_release_id": "r"}))
        self.assertTrue(_status_has_expected_exit(status, actual))

    def test_known_bad_checker_self_test(self) -> None:
        self.assertFalse(_status_has_expected_exit(404, 0))
