"""Unit tests for web/wrong_match_file_service.py's pure refusal classifier.

Issue #1099: the whole-root open (``_opened_wrong_match_root``) and the
single-file stream resolve (``resolve_wrong_match_stream_file``) both need
to answer 404/422/503 for the same refusal reason, never drifting on "what
does this errno mean". ``_classify_wrong_match_refusal`` is the one place
that decides, composed only from ``lib.fs_authority``'s three owner
predicates.
"""
import unittest
from typing import ClassVar, get_args

from lib.fs_authority import FsAuthorityCode
from web.wrong_match_file_service import _classify_wrong_match_refusal


class TestWrongMatchRefusalClassification(unittest.TestCase):
    """Exhaustive deterministic table over every declared ``FsAuthorityCode``."""

    #: Expected verdict per code. A new code added to the ``FsAuthorityCode``
    #: ``Literal`` without a matching entry here fails
    #: ``test_table_covers_every_declared_code`` RED, before it can silently
    #: inherit the wrong HTTP status.
    EXPECTED: ClassVar[dict[str, str]] = {
        # errno_proves_absence() True — the only codes allowed to claim a
        # definitive negative (404).
        "missing": "not_found",
        "not_a_directory": "not_found",
        # is_containment_refusal() True — a security-boundary decision, not
        # a verdict about existence (422).
        "unsafe_symlink": "refused",
        "not_regular_file": "refused",
        "path_escape": "refused",
        "untrusted_ownership": "refused",
        # refusal_is_indeterminate() True — a genuine, retryable world
        # failure that proved nothing (503).
        "open_failed": "unavailable",
        "read_failed": "unavailable",
        "write_failed": "unavailable",
        # Residual/unclassified code — never a definitive claim (503).
        "unspecified": "unavailable",
        # not_configured (issue #1176) is is_containment_refusal() True —
        # unreachable through THIS classifier in practice (only
        # lib.fs_authority.open_configured_local_import_directory raises
        # it), but the table is exhaustive over the whole declared
        # vocabulary regardless of which producer actually reaches it.
        "not_configured": "refused",
    }

    def test_table_covers_every_declared_code(self) -> None:
        self.assertEqual(set(self.EXPECTED), set(get_args(FsAuthorityCode)))

    def test_classification_matches_the_table(self) -> None:
        for code, expected in self.EXPECTED.items():
            with self.subTest(code=code):
                self.assertEqual(
                    _classify_wrong_match_refusal(code),  # pyright: ignore[reportArgumentType]
                    expected,
                )

    def test_known_bad_classifier_is_caught(self) -> None:
        """A mutant that calls every non-absence code retryable must be
        distinguishable from the real classifier on a containment code —
        that was the #1099 defect: ``refusal_is_indeterminate`` alone
        cannot tell "refused" from "unavailable".
        """
        def _bad_classify(code: FsAuthorityCode) -> str:
            from lib.fs_authority import errno_proves_absence
            return "not_found" if errno_proves_absence(code) else "unavailable"

        containment_codes = {
            "unsafe_symlink", "not_regular_file",
            "path_escape", "untrusted_ownership", "not_configured",
        }
        for code in containment_codes:
            with self.subTest(code=code):
                self.assertNotEqual(
                    _bad_classify(code),  # pyright: ignore[reportArgumentType]
                    _classify_wrong_match_refusal(code),  # pyright: ignore[reportArgumentType]
                    f"the known-bad classifier must diverge from the real "
                    f"one on {code!r}",
                )


if __name__ == "__main__":
    unittest.main()
