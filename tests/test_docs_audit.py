"""Docs-freshness structural audits (issues #570 and #590).

CLAUDE.md's "New Work Checklist" row for a documented surface says the doc
update ships in the SAME PR as the code, not a follow-up. These checks
are the automatic forcing function: each is a structural coverage gate
between a documented surface and the doc that's supposed to describe it, so
a code PR that adds a beets plugin / CLI subcommand / module option without
touching docs fails the suite instead of drifting silently.

Mirrors the existing audit-test house style (tests/test_skip_audit.py,
tests/test_stopwords_audit.py, tests/test_lambda_audit.py,
tests/web/test_route_audit.py): deterministic, no network, real repo files
read straight off disk.

    - TestBeetsPluginDocCoverage — module plugins match the primer table.
    - TestPipelineCliDocCoverage — CLI commands appear in the CLI doc.
    - TestDocLinksResolve — repo-local markdown links resolve.
    - TestModuleOptionDescriptions — module options carry descriptions.
    - TestLivingCodeReferences — living repo paths and symbols resolve.
    - TestSkillInstructionCodeReferences — tracked skill paths resolve.
    - TestBacktickedCallReferences — project-shaped call names still exist.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scripts.pipeline_cli.routes_meta import (  # noqa: E402
    _build_parser,
    _collect_cli_routes,
)
from tests._docs_reference_audit import (  # noqa: E402
    REMOVAL_STABLE_REPO_ROOTS,
    REMOVAL_STABLE_ROOT_FILES,
    broken_repo_references,
    broken_skill_instruction_references,
    lib_docstrings,
    living_doc_files,
    missing_call_references,
    python_code_identifiers,
    tracked_skill_instruction_files,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
DOCS_DIR = REPO_ROOT / "docs"
MODULE_NIX = REPO_ROOT / "nix" / "module.nix"
BEETS_PRIMER = DOCS_DIR / "beets-primer.md"
DEBUGGING_CLI = DOCS_DIR / "debugging-cli.md"
RETIRED_INTEGRATION_TERMS = ("mee" + "lo", "lid" + "arr")


def _retired_integration_counts(text: str) -> dict[str, int]:
    """Count retired integration names without preserving them as literals."""
    lowered = text.lower()
    return {
        term: lowered.count(term)
        for term in RETIRED_INTEGRATION_TERMS
        if term in lowered
    }


class TestReferenceScannerKnownBadCases(unittest.TestCase):
    """Synthetic violations prove each reference check constrains input."""

    def test_missing_repo_path_is_rejected(self) -> None:
        findings = broken_repo_references(
            REPO_ROOT / "README.md",
            "See `lib/_missing_issue_590.py`.",
            REPO_ROOT,
        )
        self.assertEqual(len(findings), 1)

    def test_missing_repo_path_in_discovered_tools_root_is_rejected(self) -> None:
        findings = broken_repo_references(
            REPO_ROOT / "README.md",
            "See `tools/_missing_issue_590.py`.",
            REPO_ROOT,
        )
        self.assertEqual(len(findings), 1)

    def test_missing_repo_path_in_absent_registered_root_is_rejected(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            findings = broken_repo_references(
                root / "README.md",
                "See `tools/_missing_issue_590.py`.",
                root,
            )
        self.assertEqual(len(findings), 1)

    def test_untracked_top_level_directory_is_not_a_repo_root(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "src").mkdir()
            findings = broken_repo_references(
                root / "README.md",
                "The external project implements this in `src/import.rs`.",
                root,
            )
        self.assertEqual(findings, [])

    def test_missing_repo_path_with_dot_slash_is_rejected(self) -> None:
        findings = broken_repo_references(
            REPO_ROOT / "README.md",
            "See `./lib/_missing_issue_590.py`.",
            REPO_ROOT,
        )
        self.assertEqual(len(findings), 1)

    def test_missing_root_metadata_paths_are_rejected(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / "README.md"
            findings = broken_repo_references(
                source,
                "See `flake.lock` and `TODO-missing-issue-590.md`.",
                root,
            )
        self.assertEqual(len(findings), 2)

    def test_missing_ordinary_root_file_is_rejected(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            findings = broken_repo_references(
                root / "README.md",
                "See `cratedigger.py`.",
                root,
            )
        self.assertEqual(len(findings), 1)

    def test_missing_symbol_is_rejected(self) -> None:
        findings = broken_repo_references(
            REPO_ROOT / "README.md",
            "See `lib/search.py::_missing_issue_590_symbol`.",
            REPO_ROOT,
        )
        self.assertEqual(len(findings), 1)

    def test_missing_repo_path_in_fenced_skill_command_is_rejected(self) -> None:
        findings = broken_skill_instruction_references(
            REPO_ROOT / ".claude" / "skills" / "check" / "SKILL.md",
            """Run the check:\n```bash\npyright lib/_missing_issue_620.py\n```\n""",
            REPO_ROOT,
        )
        self.assertEqual(len(findings), 1)

    def test_commonmark_fence_variants_cannot_hide_stale_paths(self) -> None:
        cases = {
            "indented backticks": (
                "   ```bash\npyright lib/_missing_indented_issue_620.py\n   ```\n"
            ),
            "long backticks": (
                "````bash\npyright lib/_missing_long_issue_620.py\n`````\n"
            ),
            "tilde fence": (
                "~~~bash\npyright lib/_missing_tilde_issue_620.py\n~~~\n"
            ),
        }
        path = REPO_ROOT / ".claude" / "skills" / "check" / "SKILL.md"
        for label, text in cases.items():
            with self.subTest(label=label):
                findings = broken_skill_instruction_references(
                    path,
                    text,
                    REPO_ROOT,
                )
                self.assertEqual(len(findings), 1)

    def test_missing_root_file_in_fenced_skill_command_is_rejected(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / ".claude" / "skills" / "check" / "SKILL.md"
            findings = broken_skill_instruction_references(
                source,
                "```bash\npyright album_source.py\n```\n",
                root,
            )
        self.assertTrue(any(
            "missing path album_source.py" in finding
            for finding in findings
        ))

    def test_missing_inline_repo_path_in_skill_is_rejected(self) -> None:
        findings = broken_skill_instruction_references(
            REPO_ROOT / ".claude" / "skills" / "check" / "SKILL.md",
            "See `lib/_missing_inline_issue_620.py`.",
            REPO_ROOT,
        )
        self.assertEqual(len(findings), 1)

    def test_missing_path_in_inline_skill_command_is_rejected_once(self) -> None:
        findings = broken_skill_instruction_references(
            REPO_ROOT / ".claude" / "skills" / "check" / "SKILL.md",
            "Run `pyright lib/_missing_inline_command_issue_620.py`.",
            REPO_ROOT,
        )
        self.assertEqual(len(findings), 1)

    def test_upstream_beets_doc_path_is_excluded_only_for_beets_skill(self) -> None:
        text = "See `docs/reference/_missing_upstream_issue_620.rst`."
        beets_findings = broken_skill_instruction_references(
            REPO_ROOT / ".claude" / "skills" / "beets-docs" / "SKILL.md",
            text,
            REPO_ROOT,
        )
        check_findings = broken_skill_instruction_references(
            REPO_ROOT / ".claude" / "skills" / "check" / "SKILL.md",
            text,
            REPO_ROOT,
        )
        self.assertEqual(beets_findings, [])
        self.assertEqual(len(check_findings), 1)

    def test_cratedigger_doc_in_beets_skill_is_not_exempt(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / ".claude" / "skills" / "beets-docs" / "SKILL.md"
            findings = broken_skill_instruction_references(
                source,
                "See `docs/beets-primer.md`.",
                root,
            )
        self.assertTrue(any(
            "missing path docs/beets-primer.md" in finding
            for finding in findings
        ))

    def test_nonexistent_call_identifier_is_rejected(self) -> None:
        findings = missing_call_references(
            REPO_ROOT / "lib" / "search.py",
            "Calls ``_missing_issue_590_call()``.",
            REPO_ROOT,
            {"real_call"},
            {},
            scope="test_scope",
        )
        self.assertEqual(len(findings), 1)

    def test_non_call_identifier_does_not_satisfy_call_reference(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "fixture.py").write_text(
                "non_call_identifier = 1\n",
                encoding="utf-8",
            )
            identifiers = python_code_identifiers(root)
        findings = missing_call_references(
            REPO_ROOT / "lib" / "search.py",
            "Calls ``non_call_identifier()``.",
            REPO_ROOT,
            identifiers,
            {},
            scope="test_scope",
        )
        self.assertEqual(len(findings), 1)

    def test_call_allowlist_is_scoped_to_one_docstring(self) -> None:
        identifier = "missing_scoped_call"
        allowlist = {
            f"lib/search.py::first_scope::{identifier}": "Historical in first scope.",
        }
        allowed = missing_call_references(
            REPO_ROOT / "lib" / "search.py",
            f"Calls ``{identifier}()``.",
            REPO_ROOT,
            set(),
            allowlist,
            scope="first_scope",
        )
        rejected = missing_call_references(
            REPO_ROOT / "lib" / "search.py",
            f"Calls ``{identifier}()``.",
            REPO_ROOT,
            set(),
            allowlist,
            scope="second_scope",
        )
        self.assertEqual(allowed, [])
        self.assertEqual(len(rejected), 1)


# Genuinely historical call names may stay only with a one-line explanation.
# Keys are ``repo-relative path::enclosing scope::identifier`` so line edits
# do not churn them and one exemption cannot mask another docstring.
STALE_CALL_REFERENCE_ALLOWLIST: dict[str, str] = {
    "lib/download.py::harvest_terminal_transfer_evidence::remove_completed_downloads":
        "Past-tense #589 docstring explains the unsafe bulk cleanup replaced by owned purging.",
    "lib/slskd_transfers.py::purge_completed_transfers::remove_completed_downloads":
        "Past-tense #589 docstring explains the unsafe bulk cleanup replaced by per-id purging.",
}


def _all_missing_call_references(
    allowlist: dict[str, str],
) -> list[str]:
    identifiers = python_code_identifiers(REPO_ROOT)
    missing: set[str] = set()
    for path, scope, docstring in lib_docstrings(REPO_ROOT):
        missing.update(missing_call_references(
            path,
            docstring,
            REPO_ROOT,
            identifiers,
            allowlist,
            scope=scope,
        ))
    return sorted(missing)


class TestRetiredIntegrationReferences(unittest.TestCase):
    """Retired integrations stay absent outside immutable schema history."""

    def test_scanner_flags_a_synthetic_reference(self) -> None:
        term = RETIRED_INTEGRATION_TERMS[0]
        self.assertEqual(
            _retired_integration_counts(f"obsolete notifier: {term}"),
            {term: 1},
        )

    def test_current_tree_has_only_immutable_schema_mentions(self) -> None:
        bridge_term = RETIRED_INTEGRATION_TERMS[1]
        expected = {
            Path("migrations/001_initial.sql"): {bridge_term: 3},
            Path("migrations") / f"022_drop_{bridge_term}_columns.sql": {
                bridge_term: 7,
            },
            Path("tests/test_migrator.py"): {bridge_term: 6},
        }
        tracked = subprocess.check_output(
            ["git", "ls-files", "-z"],
            cwd=REPO_ROOT,
        ).decode().split("\0")
        actual: dict[Path, dict[str, int]] = {}
        for relative in filter(None, tracked):
            relative_path = Path(relative)
            path = REPO_ROOT / relative_path
            if not path.is_file():
                continue
            text = relative + "\n" + path.read_text(
                encoding="utf-8",
                errors="ignore",
            )
            counts = _retired_integration_counts(text)
            if counts:
                actual[relative_path] = counts

        self.assertEqual(
            actual,
            expected,
            "Retired integration reference escaped immutable migration "
            "history; remove the reference or update the exact historical "
            "contract deliberately.",
        )


class TestLivingCodeReferences(unittest.TestCase):
    """Living docs may refer only to paths and symbols that still exist."""

    def test_repo_paths_and_path_symbols_resolve(self) -> None:
        findings: list[str] = []
        for path in living_doc_files(REPO_ROOT):
            findings.extend(broken_repo_references(
                path,
                path.read_text(encoding="utf-8"),
                REPO_ROOT,
            ))
        self.assertEqual(
            findings,
            [],
            "Stale repo path/symbol reference(s) in living docs:\n  - "
            + "\n  - ".join(findings),
        )

    def test_living_doc_scope_is_not_vacuous(self) -> None:
        files = living_doc_files(REPO_ROOT)
        self.assertIn(REPO_ROOT / "CLAUDE.md", files)
        self.assertIn(REPO_ROOT / "README.md", files)
        self.assertIn(REPO_ROOT / ".claude" / "rules" / "code-quality.md", files)
        self.assertIn(REPO_ROOT / "docs" / "beets-primer.md", files)
        self.assertNotIn(
            REPO_ROOT / "docs" / "plans"
            / "2026-05-28-001-feat-youtube-rescue-ingest-api-plan.md",
            files,
        )

    def test_frozen_tree_readmes_are_excluded_after_union(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            living = root / "docs" / "guide" / "nested" / "README.md"
            frozen = [
                root / "docs" / dirname / "nested" / "README.md"
                for dirname in ("plans", "brainstorms", "solutions")
            ]
            for path in [living, *frozen]:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("# Test\n", encoding="utf-8")
            files = living_doc_files(root)
        self.assertIn(living, files)
        for path in frozen:
            self.assertNotIn(path, files)

    def test_tracked_top_level_surfaces_are_registered(self) -> None:
        result = subprocess.run(
            ["git", "ls-files", "-z"],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        tracked_paths = [
            Path(value) for value in result.stdout.split("\0") if value
        ]
        tracked_roots = {
            path.parts[0] for path in tracked_paths if len(path.parts) > 1
        }
        tracked_root_files = {
            path.name for path in tracked_paths if len(path.parts) == 1
        }
        self.assertEqual(
            tracked_roots - REMOVAL_STABLE_REPO_ROOTS,
            set(),
            "Register new tracked top-level directories in the removal-stable "
            "repo-root set.",
        )
        self.assertEqual(
            tracked_root_files - REMOVAL_STABLE_ROOT_FILES,
            set(),
            "Register new tracked root files in the removal-stable file set.",
        )


class TestSkillInstructionCodeReferences(unittest.TestCase):
    """Tracked repo-owned skill instructions may only name live paths."""

    def test_repo_paths_resolve(self) -> None:
        findings: list[str] = []
        for path in tracked_skill_instruction_files(REPO_ROOT):
            findings.extend(broken_skill_instruction_references(
                path,
                path.read_text(encoding="utf-8"),
                REPO_ROOT,
            ))
        self.assertEqual(
            findings,
            [],
            "Stale repo path reference(s) in tracked skill instructions:\n  - "
            + "\n  - ".join(findings),
        )

    def test_scope_is_repo_owned_tracked_skills_only(self) -> None:
        files = tracked_skill_instruction_files(REPO_ROOT)
        for skill_name in ("beets-docs", "check", "debug-download", "deploy"):
            self.assertIn(
                REPO_ROOT / ".claude" / "skills" / skill_name / "SKILL.md",
                files,
            )
        for path in files:
            relative = path.relative_to(REPO_ROOT)
            self.assertEqual(relative.parts[:2], (".claude", "skills"))
            self.assertEqual(relative.name, "SKILL.md")

    def test_known_bad_stale_path_in_tracked_skill_is_rejected(self) -> None:
        path = REPO_ROOT / ".claude" / "skills" / "check" / "SKILL.md"
        self.assertIn(path, tracked_skill_instruction_files(REPO_ROOT))
        text = path.read_text(encoding="utf-8")
        stale_text = text.replace(
            "scripts/run_tests.sh",
            "lib/_missing_issue_620.py",
            1,
        )
        self.assertNotEqual(stale_text, text)
        findings = broken_skill_instruction_references(
            path,
            stale_text,
            REPO_ROOT,
        )
        self.assertTrue(any(
            "missing path lib/_missing_issue_620.py" in finding
            for finding in findings
        ))

    def test_generated_plugin_and_untracked_skill_trees_are_excluded(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tracked = root / ".claude" / "skills" / "owned" / "SKILL.md"
            untracked = root / ".claude" / "skills" / "local" / "SKILL.md"
            plugin = (
                root / ".codex" / "plugins" / "cache" / "external"
                / "SKILL.md"
            )
            for path in (tracked, untracked, plugin):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("# Test\n", encoding="utf-8")
            subprocess.run(["git", "init", "-q"], cwd=root, check=True)
            subprocess.run(
                [
                    "git", "add", "--",
                    tracked.relative_to(root).as_posix(),
                    plugin.relative_to(root).as_posix(),
                ],
                cwd=root,
                check=True,
            )
            files = tracked_skill_instruction_files(root)
        self.assertIn(tracked, files)
        self.assertNotIn(untracked, files)
        self.assertNotIn(plugin, files)


class TestBacktickedCallReferences(unittest.TestCase):
    """Project-shaped calls in lib docstrings must exist."""

    def test_call_identifiers_exist_in_python_code(self) -> None:
        missing = _all_missing_call_references(STALE_CALL_REFERENCE_ALLOWLIST)
        self.assertEqual(
            missing,
            [],
            "Backticked snake_case call reference(s) have no Python identifier. "
            "Fix live prose, or allowlist deliberate history with a one-line "
            "rationale:\n  - " + "\n  - ".join(missing),
        )

    def test_allowlist_entries_still_need_exemption(self) -> None:
        unresolved = set(_all_missing_call_references({}))
        stale = sorted(set(STALE_CALL_REFERENCE_ALLOWLIST) - unresolved)
        self.assertEqual(
            stale,
            [],
            "Stale call-reference allowlist entries; remove them:\n  - "
            + "\n  - ".join(stale),
        )

    def test_allowlist_rationales_are_one_nonempty_line(self) -> None:
        invalid = sorted(
            key for key, rationale in STALE_CALL_REFERENCE_ALLOWLIST.items()
            if not rationale.strip() or "\n" in rationale
        )
        self.assertEqual(
            invalid,
            [],
            "Call-reference allowlist entries need a one-line rationale:\n  - "
            + "\n  - ".join(invalid),
        )

    def test_scan_inputs_are_not_vacuous(self) -> None:
        identifiers = python_code_identifiers(REPO_ROOT)
        self.assertIn("dispatch_import_core", identifiers)
        self.assertIn("PipelineDB", identifiers)
        docstrings = lib_docstrings(REPO_ROOT)
        self.assertTrue(any(path == REPO_ROOT / "lib" / "search.py"
                            for path, _scope, _docstring in docstrings))


# ======================================================================
# Check 1 — beets plugin <-> docs/beets-primer.md coverage
# ======================================================================

_PLUGINS_LINE_RE = re.compile(r'plugins\s*=\s*"([^"]+)"')
_PLUGIN_TABLE_ROW_RE = re.compile(r"^\|\s*`([a-zA-Z_]+)`\s*\|", re.MULTILINE)


def _module_beets_plugins(module_nix_text: str) -> list[str]:
    """Return the ordered plugin tokens from the `plugins = "..."` line."""
    m = _PLUGINS_LINE_RE.search(module_nix_text)
    if m is None:
        raise AssertionError(
            'could not find beets `plugins = "..."` line in nix/module.nix')
    return m.group(1).split()


def _documented_beets_plugins(beets_primer_text: str) -> set[str]:
    """Return plugin names with a row in the "Active Plugins" table."""
    heading = "### Active Plugins"
    idx = beets_primer_text.index(heading)
    rest = beets_primer_text[idx + len(heading):]
    next_heading = re.search(r"\n#{2,3} ", rest)
    table_text = rest[:next_heading.start()] if next_heading else rest
    return set(_PLUGIN_TABLE_ROW_RE.findall(table_text))


def _missing_plugin_docs(plugins: list[str], documented: set[str]) -> list[str]:
    return [p for p in plugins if p not in documented]


class TestBeetsPluginDocCoverage(unittest.TestCase):
    """Every beets plugin the module loads needs a row in the doc table.

    Regression: nix/module.nix's `plugins` string gained `edit` and
    `inline` (issue #570) with no matching doc row — this check would
    have failed CI on that PR.
    """

    def test_every_module_plugin_has_a_doc_table_row(self) -> None:
        plugins = _module_beets_plugins(MODULE_NIX.read_text(encoding="utf-8"))
        documented = _documented_beets_plugins(
            BEETS_PRIMER.read_text(encoding="utf-8"))
        missing = _missing_plugin_docs(plugins, documented)
        self.assertEqual(
            missing, [],
            f"nix/module.nix loads beets plugin(s) {missing} with no row "
            f"in docs/beets-primer.md's 'Active Plugins' table. Add one "
            f"describing what the plugin does.",
        )

    def test_scan_is_not_vacuous(self) -> None:
        """Guard against a doc heading rename silently emptying the scan."""
        plugins = _module_beets_plugins(MODULE_NIX.read_text(encoding="utf-8"))
        documented = _documented_beets_plugins(
            BEETS_PRIMER.read_text(encoding="utf-8"))
        self.assertGreaterEqual(len(plugins), 10)
        self.assertGreaterEqual(len(documented), 10)

    def test_missing_plugin_docs_helper_flags_a_bogus_plugin(self) -> None:
        """Known-bad self-test: the diff helper actually has teeth."""
        missing = _missing_plugin_docs(
            ["musicbrainz", "bogus570"], {"musicbrainz"})
        self.assertEqual(missing, ["bogus570"])


# ======================================================================
# Check 2 — pipeline-cli parser leaves <-> docs/debugging-cli.md capability list
# ======================================================================

_CAPABILITY_HEADING = "## Command capability surface"


def _cli_route_names() -> list[str]:
    """Derive command capabilities from the parser's route walker."""
    parser, _, _ = _build_parser()
    return sorted(str(row["subcommand"]) for row in _collect_cli_routes(parser))


def _documented_cli_capabilities(doc_text: str) -> list[str]:
    """Read the authoritative bounded capability list, not prose mentions."""
    try:
        section = doc_text.split(_CAPABILITY_HEADING, 1)[1]
    except IndexError:
        return []
    next_heading = section.find("\n## ")
    if next_heading != -1:
        section = section[:next_heading]
    return sorted(re.findall(r"^- `pipeline-cli ([^`]+)` —", section, re.MULTILINE))


def _capability_surface_drift(
    expected: list[str], documented: list[str],
) -> tuple[list[str], list[str]]:
    """Return missing parser commands and stale document commands."""
    return (
        sorted(set(expected) - set(documented)),
        sorted(set(documented) - set(expected)),
    )


class TestPipelineCliDocCoverage(unittest.TestCase):
    """The authoritative capability list is exactly the parser leaf surface."""

    def test_capability_list_matches_every_parser_command_exactly(self) -> None:
        actual = _documented_cli_capabilities(
            DEBUGGING_CLI.read_text(encoding="utf-8"))
        expected = _cli_route_names()
        missing, stale = _capability_surface_drift(expected, actual)
        self.assertEqual(
            actual, expected,
            "docs/debugging-cli.md's Command capability surface must match "
            "pipeline-cli routes exactly; update docs for additions and removals. "
            f"missing={missing}, stale={stale}",
        )

    def test_scan_is_not_vacuous(self) -> None:
        self.assertGreaterEqual(len(_cli_route_names()), 30)

    def test_capability_helper_flags_a_bogus_command(self) -> None:
        """Known-bad self-test: the diff helper actually has teeth."""
        missing, stale = _capability_surface_drift(
            ["show"], ["show", "bogus570cmd"],
        )
        self.assertEqual(missing, [])
        self.assertEqual(stale, ["bogus570cmd"])


# ======================================================================
# Check 3 — dead repo-local markdown links
# ======================================================================

_MD_LINK_RE = re.compile(r"\]\(([^)]+)\)")
# Any `<scheme>:` prefix (http://, https://, mailto:, ftp:, ...) — these are
# never repo-local, so they're treated as external/non-checkable regardless
# of what follows the colon.
_URL_SCHEME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*:")


def _iter_doc_files() -> list[Path]:
    files = [REPO_ROOT / "README.md", REPO_ROOT / "CLAUDE.md"]
    files.extend(sorted(DOCS_DIR.rglob("*.md")))
    return files


def _is_repo_local_link(target_no_anchor: str) -> bool:
    """Return True if ``target_no_anchor`` (scheme-less, anchor-stripped)
    plausibly points at a repo-local path — i.e. it looks like a path or a
    filename rather than bare prose. Deliberately broad (issue #570): any
    ``/`` or ``.`` qualifies, not just a narrow extension/prefix allowlist,
    so `.sql`, `migrations/...`, `tests/...`, and other repo-local links
    outside the old allowlist get validated too."""
    return "/" in target_no_anchor or "." in target_no_anchor


def _broken_links_in_text(path: Path, text: str, repo_root: Path) -> list[str]:
    """Return ``"<relpath> -> <target>"`` for every dead repo-local link in
    ``text`` (which was read from ``path``). Resolution tries the linking
    file's own directory first, then falls back to ``repo_root`` — matching
    the two conventions actually used across this repo's docs."""
    offenders = []
    for m in _MD_LINK_RE.finditer(text):
        target = m.group(1).strip()
        if not target:
            continue
        if target.startswith("#"):
            continue
        if _URL_SCHEME_RE.match(target):
            continue
        target_no_anchor = target.split("#", 1)[0]
        if not target_no_anchor:
            continue
        if not _is_repo_local_link(target_no_anchor):
            continue
        resolved_relative = path.parent / target_no_anchor
        resolved_root = repo_root / target_no_anchor
        if resolved_relative.exists() or resolved_root.exists():
            continue
        try:
            rel = path.relative_to(repo_root)
        except ValueError:
            rel = path
        offenders.append(f"{rel} -> {target}")
    return offenders


class TestDocLinksResolve(unittest.TestCase):
    """Every repo-local markdown link target must exist on disk.

    Regression: docs/plans/2026-04-27-002-feat-provisional-lossless-grind-up-plan.md
    linked lib/quality.py, lib/preimport.py, lib/import_dispatch.py, and
    lib/pipeline_db.py — all four renamed/moved (into lib/quality/,
    lib/measurement.py, lib/dispatch/, and the lib/pipeline_db/ package)
    since the plan was written. This check catches exactly that drift.
    """

    def test_no_dead_repo_local_links(self) -> None:
        offenders: list[str] = []
        for path in _iter_doc_files():
            text = path.read_text(encoding="utf-8")
            offenders.extend(_broken_links_in_text(path, text, REPO_ROOT))
        self.assertEqual(
            offenders, [],
            "Dead repo-local markdown link(s) — fix the path or remove "
            "the link:\n  - " + "\n  - ".join(offenders),
        )

    def test_scan_is_not_vacuous(self) -> None:
        files = _iter_doc_files()
        self.assertGreaterEqual(len(files), 10)

    def test_broken_links_helper_flags_a_bogus_target(self) -> None:
        """Known-bad self-test: the diff helper actually has teeth."""
        offenders = _broken_links_in_text(
            REPO_ROOT / "README.md",
            "See [bogus](docs/_bogus_570_nonexistent.md) for details.",
            REPO_ROOT,
        )
        self.assertEqual(len(offenders), 1)

    def test_broken_links_helper_ignores_external_and_anchor_links(self) -> None:
        text = (
            "See [ext](https://example.com/x.md), "
            "[mail](mailto:a@b.com), and [anchor](#section) — none local."
        )
        offenders = _broken_links_in_text(REPO_ROOT / "README.md", text, REPO_ROOT)
        self.assertEqual(offenders, [])


# ======================================================================
# Check 4 — nix/module.nix option `description` coverage (ratchet)
# ======================================================================

# Pre-existing gaps at audit-creation time (issue #570). Ratchet: no NEW option may join this
# list (test_no_new_options_without_description catches it); entries
# should shrink to zero as each option earns a real description
# (test_allowlist_entries_still_missing_description catches staleness).
OPTIONS_WITHOUT_DESCRIPTION_OK: frozenset[str] = frozenset({
    "slskd.deleteSearches",                       # TODO: document
    "redis.port",                                 # TODO: document
    "web.port",                                   # TODO: document
    "web.redis.port",                              # TODO: document
    "notifiers.plex.url",                         # TODO: document
    "notifiers.plex.tokenFile",                   # TODO: document
    "notifiers.jellyfin.url",                     # TODO: document
    "notifiers.jellyfin.tokenFile",               # TODO: document
    "releaseSettings.useMostCommonTracknum",      # TODO: document
    "releaseSettings.allowMultiDisc",             # TODO: document
    "releaseSettings.acceptedCountries",          # TODO: document
    "releaseSettings.skipRegionCheck",            # TODO: document
    "releaseSettings.acceptedFormats",            # TODO: document
    "searchSettings.maximumPeerQueue",            # TODO: document
    "searchSettings.minimumPeerUploadSpeed",      # TODO: document
    "searchSettings.minimumFilenameMatchRatio",   # TODO: document
    "searchSettings.ignoredUsers",                # TODO: document
    "searchSettings.searchForTracks",             # TODO: document
    "searchSettings.albumPrependArtist",          # TODO: document
    "searchSettings.trackPrependArtist",          # TODO: document
    "searchSettings.searchType",                  # TODO: document
    "searchSettings.parallelSearches",            # TODO: document
    "searchSettings.titleBlacklist",              # TODO: document
    "searchSettings.searchBlacklist",             # TODO: document
    "downloadSettings.downloadFiltering",         # TODO: document
    "downloadSettings.useExtensionWhitelist",     # TODO: document
    "downloadSettings.extensionsWhitelist",       # TODO: document
    "qualityRanks.bitrateMetric",                 # TODO: document
    "qualityRanks.withinRankToleranceKbps",       # TODO: document
    "logging.level",                              # TODO: document
    "logging.format",                             # TODO: document
    "logging.datefmt",                            # TODO: document
})

_BARE_ATTRSET_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*\{\s*$")
_MKOPTION_RE = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)\s*=\s*mkOption\s*\{")


def _dotted_path_by_line(text: str) -> list[str]:
    """Return, for each 0-based line index, the dotted attrset path open at
    that point — tracked via a brace-depth stack over bare ``name = {``
    attrset openers. This file's formatting always puts one brace-opener
    per line, so simple per-line brace counting stays balanced even
    through ``${...}`` string interpolations (each interpolation's braces
    are self-balanced pairs)."""
    lines = text.splitlines()
    stack: list[tuple[str, int]] = []
    depth = 0
    paths: list[str] = []
    for line in lines:
        paths.append(".".join(name for name, _ in stack))
        m = _BARE_ATTRSET_RE.match(line)
        if m:
            stack.append((m.group(1), depth))
        depth += line.count("{") - line.count("}")
        while stack and depth <= stack[-1][1]:
            stack.pop()
    return paths


def _has_nonempty_description(mkoption_block: str) -> bool:
    m = re.search(r"\bdescription\s*=\s*", mkoption_block)
    if m is None:
        return False
    rest = mkoption_block[m.end():]
    if rest.startswith("''"):
        end = rest.index("''", 2)
        return bool(rest[2:end].strip())
    if rest.startswith('"'):
        i = 1
        while rest[i] != '"' or rest[i - 1] == "\\":
            i += 1
        return bool(rest[1:i].strip())
    return False  # unrecognised value form — treat conservatively as missing


def _find_mkoptions_missing_description(text: str) -> list[tuple[str, int]]:
    """Return ``(dotted_path, 1-based line number)`` for every
    ``mkOption { ... }`` block in ``text`` whose ``description`` field is
    absent or empty."""
    line_paths = _dotted_path_by_line(text)
    missing: list[tuple[str, int]] = []
    for m in _MKOPTION_RE.finditer(text):
        name = m.group(1)
        start = m.end() - 1  # index of the opening "{"
        d = 0
        i = start
        while True:
            c = text[i]
            if c == "{":
                d += 1
            elif c == "}":
                d -= 1
                if d == 0:
                    break
            i += 1
        block = text[start:i + 1]
        line_no = text.count("\n", 0, m.start())
        prefix = line_paths[line_no] if line_no < len(line_paths) else ""
        full_path = f"{prefix}.{name}" if prefix else name
        if not _has_nonempty_description(block):
            missing.append((full_path, line_no + 1))
    return missing


class TestModuleOptionDescriptions(unittest.TestCase):
    """Every mkOption in nix/module.nix must carry a non-empty description.

    docs/nixos-module.md explicitly defers the full option set to the
    module source ("full set in nix/module.nix") — the in-code
    description IS the documentation for most options, so a description-
    free option is effectively undocumented.

    Ratchet, not a hard gate (issue #570): pre-existing options lacked
    a description when this audit was written. See
    OPTIONS_WITHOUT_DESCRIPTION_OK. No NEW option may join that list.
    """

    def test_no_new_options_without_description(self) -> None:
        missing = _find_mkoptions_missing_description(
            MODULE_NIX.read_text(encoding="utf-8"))
        unexpected = sorted(
            f"{path} (line {line})" for path, line in missing
            if path not in OPTIONS_WITHOUT_DESCRIPTION_OK
        )
        self.assertEqual(
            unexpected, [],
            "New mkOption(s) without a description — add one, or if the "
            "gap is genuinely pre-existing, add to "
            "OPTIONS_WITHOUT_DESCRIPTION_OK:\n  - "
            + "\n  - ".join(unexpected),
        )

    def test_allowlist_entries_still_missing_description(self) -> None:
        """Catch stale ratchet entries — once an option earns a real
        description, its allowlist row must be deleted (mirrors
        test_lambda_audit.py's staleness guard)."""
        missing_paths = {
            path for path, _ in _find_mkoptions_missing_description(
                MODULE_NIX.read_text(encoding="utf-8"))
        }
        stale = sorted(OPTIONS_WITHOUT_DESCRIPTION_OK - missing_paths)
        self.assertEqual(
            stale, [],
            "OPTIONS_WITHOUT_DESCRIPTION_OK has stale entries that now "
            "have a description — delete them from the allowlist:\n  - "
            + "\n  - ".join(stale),
        )

    def test_scan_is_not_vacuous(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        # 104 mkOption blocks at write time; a healthy floor guards
        # against the regex silently matching nothing.
        self.assertGreaterEqual(len(_MKOPTION_RE.findall(text)), 60)

    def test_scanner_flags_a_synthetic_option_without_description(self) -> None:
        """Known-bad self-test: plant a violating option and prove the
        detector trips, alongside a sibling that has a description."""
        snippet = (
            "  foo = {\n"
            "    bar = mkOption {\n"
            "      type = types.str;\n"
            '      default = "x";\n'
            "    };\n"
            "    baz = mkOption {\n"
            "      type = types.str;\n"
            '      default = "y";\n'
            '      description = "has one";\n'
            "    };\n"
            "  };\n"
        )
        missing = _find_mkoptions_missing_description(snippet)
        self.assertEqual([p for p, _ in missing], ["foo.bar"])


if __name__ == "__main__":
    unittest.main()
