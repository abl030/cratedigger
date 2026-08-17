"""Contracts for the comment-stripping source reader (issues #1172, #1186)."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tests._source_pins import (
    UnknownPinnedFormat,
    pinned_source,
    strip_fenced_comments,
    strip_line_comments,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
DEPLOY_SKILL = REPO_ROOT / ".claude" / "skills" / "deploy" / "SKILL.md"


class TestStripLineComments(unittest.TestCase):
    def test_a_commented_line_is_removed(self) -> None:
        self.assertEqual(strip_line_comments("# stopIfChanged = false;", ("#",)), "")

    def test_an_indented_comment_is_removed(self) -> None:
        """Nix and shell alike nest, so a disabled line is almost always
        indented rather than at column zero."""
        self.assertEqual(
            strip_line_comments("      # stopIfChanged = false;", ("#",)), "",
        )

    def test_a_trailing_comment_keeps_its_code(self) -> None:
        source = "NoNewPrivileges = true; # no setuid exec"
        self.assertIn("NoNewPrivileges = true;", strip_line_comments(source, ("#",)))

    def test_sql_uses_its_own_prefix(self) -> None:
        source = "-- CREATE INDEX disabled;\nCREATE INDEX live;"
        stripped = strip_line_comments(source, ("--",))
        self.assertNotIn("CREATE INDEX disabled;", stripped)
        self.assertIn("CREATE INDEX live;", stripped)

    def test_a_hash_does_not_comment_sql(self) -> None:
        """Applying the wrong prefix must not silently strip real code."""
        self.assertIn("# not a comment here", strip_line_comments(
            "# not a comment here", ("--",)))

    def test_a_first_line_shebang_survives(self) -> None:
        """A shebang is a functional directive, not a disabled line. Some
        tests read a script, mutate it, and write it back out executable —
        dropping the shebang there would corrupt the fixture."""
        source = "#!/usr/bin/env bash\n# disabled\nreal\n"
        stripped = strip_line_comments(source, ("#",))
        self.assertIn("#!/usr/bin/env bash", stripped)
        self.assertNotIn("# disabled", stripped)

    def test_a_shebang_below_the_first_line_is_still_a_comment(self) -> None:
        """``#!`` only means anything on line 1; anywhere else it is an
        ordinary comment and must not become an escape hatch."""
        source = "real\n#!/usr/bin/env bash\n"
        self.assertNotIn("#!/usr/bin/env bash", strip_line_comments(source, ("#",)))

    def test_a_comment_is_blanked_not_deleted(self) -> None:
        """Deleting would splice the surrounding lines together and could
        satisfy a multi-line pin the real file does not contain — trading one
        false green for another. It would also shift line numbers, and two
        pinning modules ``ast.parse`` the source they pin."""
        stripped = strip_line_comments("alpha\n# gone\nbeta\n", ("#",))
        self.assertNotIn("alpha\nbeta", stripped)
        self.assertEqual(stripped, "alpha\n\nbeta")

    def test_line_continuations_stay_adjacent(self) -> None:
        """Multi-line pins match only because the lines stay adjacent and in
        order — three working pins depend on this, so nothing may reflow."""
        source = 'exec ruff check \\\n  --output-format full \\\n  "$@"\n'
        self.assertIn(
            'exec ruff check \\\n  --output-format full \\\n  "$@"',
            strip_line_comments(source, ("#",)),
        )


class TestStripFencedComments(unittest.TestCase):
    """#1186: in Markdown the exposure is inside fenced code blocks, where
    ``#`` is a shell comment — NOT in prose, where it is a heading."""

    def test_a_shell_comment_inside_a_fence_is_removed(self) -> None:
        source = "```bash\n# fleet-deploy doc2\nreal-command\n```"
        stripped = strip_fenced_comments(source)
        self.assertNotIn("fleet-deploy doc2", stripped)
        self.assertIn("real-command", stripped)

    def test_a_heading_in_prose_survives(self) -> None:
        """Section headings are load-bearing anchors — several pins slice the
        runbook on them. Stripping these would break the document."""
        source = "## Database migrations\n\nordinary prose\n"
        self.assertIn("## Database migrations", strip_fenced_comments(source))

    def test_a_heading_after_a_closed_fence_survives(self) -> None:
        """Fence state must actually toggle back off; a stuck-open scanner
        would eat every heading in the rest of the file."""
        source = "```bash\n# disabled\n```\n\n## Still A Heading\n"
        stripped = strip_fenced_comments(source)
        self.assertNotIn("# disabled", stripped)
        self.assertIn("## Still A Heading", stripped)

    def test_an_inline_hash_inside_a_fence_survives(self) -> None:
        """``sed 's#/cratedigger.py##'`` is real runbook code carrying a
        literal ``#``. A to-end-of-line stripper would corrupt it, which is
        why this is deliberately line-start only."""
        source = "```bash\nsed 's#/cratedigger.py##'\n```"
        self.assertIn("sed 's#/cratedigger.py##'", strip_fenced_comments(source))

    def test_the_fence_markers_themselves_survive(self) -> None:
        source = "```bash\nreal-command\n```"
        self.assertEqual(strip_fenced_comments(source).count("```"), 2)


class TestPinnedSourceDispatch(unittest.TestCase):
    def _write(self, name: str, body: str) -> Path:
        directory = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: None)
        path = directory / name
        path.write_text(body, encoding="utf-8")
        return path

    def test_shell_suffix_strips_hash(self) -> None:
        path = self._write("x.sh", "# exec real\nexec live\n")
        stripped = pinned_source(path)
        self.assertNotIn("exec real", stripped)
        self.assertIn("exec live", stripped)

    def test_sql_suffix_strips_double_dash(self) -> None:
        path = self._write("x.sql", "-- CREATE INDEX gone;\nCREATE INDEX live;\n")
        self.assertNotIn("CREATE INDEX gone;", pinned_source(path))

    def test_markdown_suffix_uses_the_fence_rule(self) -> None:
        path = self._write("x.md", "# Heading\n\n```bash\n# gone\nlive\n```\n")
        stripped = pinned_source(path)
        self.assertIn("# Heading", stripped)
        self.assertNotIn("# gone", stripped)

    def test_json_is_treated_as_jsonc(self) -> None:
        """Pyright accepts ``//`` comments in pyrightconfig*.json, verified
        against the pinned pyright. Assuming "JSON has no comments" would have
        left exactly that file unguarded."""
        path = self._write("x.json", '{\n  // "reportShadowedImports": "error",\n}\n')
        self.assertNotIn("reportShadowedImports", pinned_source(path))

    def test_an_undeclared_suffix_fails_closed(self) -> None:
        """Returning raw source for an unknown format would reinstate the very
        defect this module removes, so it raises instead."""
        path = self._write("x.toml", "key = 'value'\n")
        with self.assertRaisesRegex(UnknownPinnedFormat, r"\.toml"):
            pinned_source(path)


class TestAgainstTheRealDeployRunbook(unittest.TestCase):
    """End-to-end over the real file, driving the exact #1186 mutant."""

    def test_commenting_out_a_runbook_step_defeats_its_pin(self) -> None:
        """`test_skill_calls_tracked_verifier_for_successor_cycle` pins this
        step. Deleting it goes RED; commenting it out inside the fence left the
        pin GREEN, which is #1186's founding measurement."""
        step = 'verify-migrate-ran "$PRE_SWITCH_MIGRATE_INVOCATION"'
        raw = DEPLOY_SKILL.read_text(encoding="utf-8")
        # The runbook invokes it twice (ordinary and strict-held deploys), and
        # a pin is only defeated once EVERY occurrence is disabled.
        self.assertEqual(raw.count(step), 2)

        commented = raw.replace(f"  {step}", f"  # {step}")
        # The defect: still present, as comment text.
        self.assertIn(step, commented)
        # The fix: absent from what the pin actually reads.
        self.assertNotIn(step, strip_fenced_comments(commented))

    def test_real_runbook_headings_and_steps_both_survive_stripping(self) -> None:
        """A must-still-work guard: the stripper must not fail closed on the
        real document by eating headings or live commands."""
        stripped = pinned_source(DEPLOY_SKILL)
        self.assertIn("## Database migrations", stripped)
        self.assertIn("env -u SSH_AUTH_SOCK fleet-deploy doc2", stripped)
        self.assertIn("scripts/verify_cratedigger_cycle.sh", stripped)


if __name__ == "__main__":
    unittest.main()
