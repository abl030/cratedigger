"""Deterministic pins for issue-closing reference audits."""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import unittest

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


def assert_skill_reference_contract(skill_text: str) -> None:
    """Assert the tracked skill never auto-closes an issue from a PR."""
    violations = find_closing_issue_references(skill_text)
    if violations:
        rendered = ", ".join(
            f"{item.keyword} {item.reference} at "
            f"{item.line}:{item.column}"
            for item in violations
        )
        raise AssertionError(
            f"orchestrator skill contains auto-closing references: {rendered}"
        )


class TestIssueReferenceContract(unittest.TestCase):
    def test_real_premature_close_incidents_are_rejected(self) -> None:
        for body in ("Closes #598", "Closes #609"):
            with self.subTest(body=body):
                violations = find_closing_issue_references(body)
                self.assertEqual(len(violations), 1)
                self.assertEqual(violations[0].reference, body.split()[-1])

    def test_every_github_closer_family_is_rejected(self) -> None:
        for keyword in _CLOSERS:
            for body in (
                f"{keyword} #637",
                f"{keyword.upper()}: abl030/cratedigger#637",
                f"{keyword.title()}:\nhttps://github.com/abl030/cratedigger/issues/637",
            ):
                with self.subTest(body=body):
                    self.assertEqual(len(find_closing_issue_references(body)), 1)

    def test_canonical_non_closing_references_pass(self) -> None:
        safe = (
            "Refs #637\n"
            "https://github.com/abl030/cratedigger/issues/637\n"
            "Reference: abl030/cratedigger#637\n"
            "The implementation closes a workflow gap without an issue ref.\n"
            "prefixCloses #637 is not a GitHub keyword token.\n"
            "Closesish #637 is not a GitHub keyword token.\n"
            "éCloses #637 is not a standalone GitHub keyword token."
        )
        self.assertEqual(find_closing_issue_references(safe), ())

    def test_planted_known_bad_proves_checker_trips(self) -> None:
        violations = find_closing_issue_references(
            "Release notes\n\nFiXeS:\tablz030/cratedigger#609"
        )
        self.assertEqual(
            [(violation.keyword.lower(), violation.reference)
             for violation in violations],
            [("fixes", "ablz030/cratedigger#609")],
        )

    def test_tracked_orchestrator_skill_enforces_contract(self) -> None:
        skill = (
            Path(__file__).parents[1]
            / ".claude"
            / "skills"
            / "orchestrate-issue"
            / "SKILL.md"
        ).read_text(encoding="utf-8")
        assert_skill_reference_contract(skill)

    def test_planted_skill_fault_proves_content_audit_trips(self) -> None:
        skill = (
            Path(__file__).parents[1]
            / ".claude"
            / "skills"
            / "orchestrate-issue"
            / "SKILL.md"
        ).read_text(encoding="utf-8")
        with self.assertRaisesRegex(
            AssertionError, "contains auto-closing references"
        ):
            assert_skill_reference_contract(skill + "\nCloses #609\n")

    def test_cli_reads_stdin_and_reports_bad_reference_location(self) -> None:
        script = (
            Path(__file__).parents[1]
            / "scripts"
            / "audit_issue_references.py"
        )
        safe = subprocess.run(
            [sys.executable, str(script)],
            input="Refs #637\nhttps://github.com/abl030/cratedigger/issues/637",
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual((safe.returncode, safe.stderr), (0, ""))

        bad = subprocess.run(
            [sys.executable, str(script)],
            input="Release body\nCloses #609",
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(bad.returncode, 1)
        self.assertIn("stdin:2:1:", bad.stderr)
        self.assertIn("Closes #609", bad.stderr)


if __name__ == "__main__":
    unittest.main()
