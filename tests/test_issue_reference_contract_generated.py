"""Generated patrol of the GitHub issue-reference contract."""

from __future__ import annotations

import unittest

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers active profile
from scripts.audit_issue_references import find_closing_issue_references


_CLOSERS = (
    "close",
    "closes",
    "closed",
    "fix",
    "fixes",
    "fixed",
    "resolve",
    "resolves",
    "resolved",
)


def _case_variant(word: str, uppercase: list[bool]) -> str:
    return "".join(
        character.upper() if use_upper else character.lower()
        for character, use_upper in zip(word, uppercase, strict=True)
    )


_ISSUE_NUMBERS = st.one_of(
    st.sampled_from((1, 598, 609, 637)),
    st.integers(min_value=1, max_value=999_999_999),
)


@st.composite
def _closing_references(draw):
    issue_number = draw(_ISSUE_NUMBERS)
    reference_kind = draw(st.sampled_from(("same", "cross", "url")))
    if draw(st.booleans()):
        before_colon = draw(st.sampled_from(("", " ", "\t", "\r\n")))
        after_colon = draw(
            st.sampled_from(("", " ", "  ", "\t", "\n", "\r\n", " \t"))
        )
        separator = f"{before_colon}:{after_colon}"
    else:
        separator = draw(
            st.sampled_from((" ", "  ", "\t", "\n", "\r\n", " \t"))
        )
    uppercase = draw(st.lists(st.booleans(), min_size=8, max_size=8))
    return issue_number, reference_kind, separator, uppercase


class TestGeneratedIssueReferenceContract(unittest.TestCase):
    @given(world=_closing_references())
    @example(
        world=(
            598,
            "same",
            " ",
            [True, False, False, False, False, False, False, False],
        )
    )
    @example(
        world=(
            609,
            "same",
            " ",
            [True, False, False, False, False, False, False, False],
        )
    )
    def test_every_closing_keyword_reference_is_rejected(self, world) -> None:
        issue_number, reference_kind, separator, uppercase = world
        reference = {
            "same": f"#{issue_number}",
            "cross": f"abl030/cratedigger#{issue_number}",
            "url": (
                "https://github.com/abl030/cratedigger/issues/"
                f"{issue_number}"
            ),
        }[reference_kind]
        for canonical_keyword in _CLOSERS:
            keyword = _case_variant(
                canonical_keyword, uppercase[:len(canonical_keyword)]
            )
            body = f"{keyword}{separator}{reference}"
            violations = find_closing_issue_references(body)
            self.assertEqual(len(violations), 1, body)
            self.assertEqual(
                violations[0].keyword.lower(), canonical_keyword, body
            )
            self.assertEqual(violations[0].reference, reference, body)

    @given(
        issue_number=_ISSUE_NUMBERS,
        prefix=st.sampled_from(("Refs", "REFS", "refs", "Reference")),
    )
    def test_non_closing_references_remain_valid(
        self, issue_number: int, prefix: str
    ) -> None:
        body = (
            f"{prefix} #{issue_number}\n"
            f"https://github.com/abl030/cratedigger/issues/{issue_number}"
        )
        self.assertEqual(find_closing_issue_references(body), ())


if __name__ == "__main__":
    unittest.main()
