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
    """Assert the tracked skill owns the safe reference/release sequence."""
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

    normalized_skill = " ".join(skill_text.split())
    required_fragments = (
        "use canonical `Refs #N` or a plain issue URL",
        "PR body and every branch commit message",
        "python3 scripts/audit_issue_references.py",
        "A reviewer `CLEAN` is final",
        "one ordinary branch push through the repository pre-push hook",
        "Fill available agent slots with independent implementation and review work",
        "Serialize only when a concrete dependency",
        "post-switch successor cycle",
        "Only after the signed tag push succeeds",
    )
    missing = [
        fragment for fragment in required_fragments
        if fragment not in normalized_skill
    ]
    if missing:
        raise AssertionError(
            "orchestrator skill is missing reference/release contract: "
            + ", ".join(missing)
        )

    release_section = normalized_skill.partition(
        "## 9. Tag and close"
    )[2]
    forbidden_fragments = (
        "CRATEDIGGER_TEST_ARTIFACT",
        "binding `CLEAN`",
        "full suite",
        "artifact verification",
        "exact final merge SHA",
        "mandatory post-ship reflection",
        "Search for stale old-owner imports",
    )
    present_forbidden = [
        fragment for fragment in forbidden_fragments
        if fragment in normalized_skill
    ]
    if present_forbidden:
        raise AssertionError(
            "orchestrator skill still duplicates release gates: "
            + ", ".join(present_forbidden)
        )

    tag_marker = "Push the signed tag with `--no-verify`"
    close_marker = "Only after the signed tag push succeeds"
    if not release_section:
        raise AssertionError("orchestrator skill has no release section")
    missing_release_markers = [
        marker for marker in (tag_marker, close_marker)
        if marker not in release_section
    ]
    if missing_release_markers:
        raise AssertionError(
            "orchestrator skill is missing ordered release markers: "
            + ", ".join(missing_release_markers)
        )
    if release_section.find(tag_marker) >= release_section.find(close_marker):
        raise AssertionError(
            "orchestrator skill must push the verified tag before closure"
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

    def test_skill_contract_is_independent_of_markdown_wrapping(self) -> None:
        skill = (
            Path(__file__).parents[1]
            / ".claude"
            / "skills"
            / "orchestrate-issue"
            / "SKILL.md"
        ).read_text(encoding="utf-8")
        wrapped = skill.replace(
            "Push the signed tag with `--no-verify`",
            "Push the signed tag\nwith `--no-verify`",
        )
        assert_skill_reference_contract(wrapped)

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

        with self.assertRaisesRegex(
            AssertionError, "still duplicates release gates"
        ):
            assert_skill_reference_contract(
                skill + "\nRun the full suite again after merge.\n"
            )

        missing_push = skill.replace(
            "Push the signed tag with `--no-verify`", "Push the release tag"
        )
        with self.assertRaisesRegex(
            AssertionError, "missing ordered release markers"
        ):
            assert_skill_reference_contract(missing_push)

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
