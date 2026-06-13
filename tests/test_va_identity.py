"""Tests for lib/va_identity.py — split_va_query()."""

import unittest

from lib.va_identity import split_va_query


class TestSplitVaQuery(unittest.TestCase):
    """Free-text VA-token detection for the browse search builders (#199)."""

    CASES = [
        # (desc, query, expected_remainder, expected_va)
        ("phrase trailing", "Rock Christmas Various Artists", "Rock Christmas", True),
        ("phrase leading", "Various Artists Rock Christmas", "Rock Christmas", True),
        ("phrase mid-query", "Rock Various Artists Christmas", "Rock Christmas", True),
        ("case insensitive", "rock christmas VARIOUS artists", "rock christmas", True),
        ("parenthesised phrase", "Rock Christmas (Various Artists)", "Rock Christmas", True),
        ("bracketed phrase", "Rock Christmas [Various Artists]", "Rock Christmas", True),
        ("phrase only", "Various Artists", "", True),
        ("bare various exact", "various", "", True),
        ("bare various exact mixed case", "Various", "", True),
        ("no va tokens", "Rock Christmas", "Rock Christmas", False),
        # "Various <word>" is a real title shape (Leonard Cohen's
        # "Various Positions") — only the full phrase or an exact bare
        # "various" counts as VA intent.
        ("leonard cohen guard", "Various Positions", "Various Positions", False),
        ("trailing bare various untouched", "Rock Christmas Various", "Rock Christmas Various", False),
        ("artist named various-ish", "Various Blends", "Various Blends", False),
        ("whitespace collapsed", "Rock   Various Artists   Christmas", "Rock Christmas", True),
        # Stripping must not leave the remainder more Lucene-hostile
        # than the raw query was: dangling boolean operators at the
        # edges are trimmed, and bracket pairs emptied by the strip are
        # removed to a fix-point (nested pairs included).
        ("trailing dangling AND trimmed", "best of AND Various Artists", "best of", True),
        ("leading dangling OR trimmed", "Various Artists OR holiday", "holiday", True),
        ("interior AND preserved", "Rock AND Roll Various Artists", "Rock AND Roll", True),
        ("lowercase and is a term, kept", "this and that Various Artists", "this and that", True),
        ("nested brackets fixpoint", "Christmas ((Various Artists))", "Christmas", True),
    ]

    def test_split_va_query(self) -> None:
        for desc, query, expected_remainder, expected_va in self.CASES:
            with self.subTest(desc=desc):
                remainder, is_va = split_va_query(query)
                self.assertEqual(remainder, expected_remainder)
                self.assertEqual(is_va, expected_va)


if __name__ == "__main__":
    unittest.main()
