"""Docs-freshness structural audits (issue #570).

CLAUDE.md's "New Work Checklist" row for a documented surface says the doc
update ships in the SAME PR as the code, not a follow-up. These four checks
are the automatic forcing function: each is a structural coverage gate
between a documented surface and the doc that's supposed to describe it, so
a code PR that adds a beets plugin / CLI subcommand / module option without
touching docs fails the suite instead of drifting silently.

Mirrors the existing audit-test house style (tests/test_skip_audit.py,
tests/test_stopwords_audit.py, tests/test_lambda_audit.py,
tests/web/test_route_audit.py): deterministic, no network, real repo files
read straight off disk.

    1. TestBeetsPluginDocCoverage    — nix/module.nix `plugins` string <->
       docs/beets-primer.md "Active Plugins" table.
    2. TestPipelineCliDocCoverage    — every pipeline-cli top-level
       subcommand (introspected from routes_meta._build_parser(), never
       regexed) <-> docs/debugging-cli.md.
    3. TestDocLinksResolve           — every repo-local markdown link in
       README.md / CLAUDE.md / docs/**/*.md resolves to a real file.
    4. TestModuleOptionDescriptions  — every `mkOption { ... }` in
       nix/module.nix carries a non-empty `description`. Seeded as a
       ratchet (37 pre-existing gaps at write time — see
       OPTIONS_WITHOUT_DESCRIPTION_OK); no NEW option may join without one.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import unittest
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scripts.pipeline_cli.routes_meta import _build_parser  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
DOCS_DIR = REPO_ROOT / "docs"
MODULE_NIX = REPO_ROOT / "nix" / "module.nix"
BEETS_PRIMER = DOCS_DIR / "beets-primer.md"
DEBUGGING_CLI = DOCS_DIR / "debugging-cli.md"


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
# Check 2 — pipeline-cli subcommand <-> docs/debugging-cli.md coverage
# ======================================================================

# Genuinely-internal subcommands exempt from the operator debugging doc.
# Keep MINIMAL — prefer adding a doc row over adding an entry here.
UNDOCUMENTED_CLI_OK: frozenset[str] = frozenset()


def _top_level_cli_subcommand_names() -> list[str]:
    """Introspect argparse's own subparser choices — never regex the CLI."""
    parser, _, _ = _build_parser()
    sub_action = next(
        a for a in parser._actions  # noqa: SLF001
        if isinstance(a, argparse._SubParsersAction)  # noqa: SLF001
    )
    return sorted(sub_action.choices.keys())


def _undocumented_subcommands(
    names: list[str], doc_text: str, ok: frozenset[str],
) -> list[str]:
    missing = []
    for name in names:
        if name in ok:
            continue
        if not re.search(rf"\b{re.escape(name)}\b", doc_text):
            missing.append(name)
    return missing


class TestPipelineCliDocCoverage(unittest.TestCase):
    """Every pipeline-cli top-level subcommand needs an operator-facing
    mention in docs/debugging-cli.md (or a rationale in
    UNDOCUMENTED_CLI_OK)."""

    def test_every_subcommand_is_mentioned_in_debugging_cli_doc(self) -> None:
        names = _top_level_cli_subcommand_names()
        doc_text = DEBUGGING_CLI.read_text(encoding="utf-8")
        missing = _undocumented_subcommands(names, doc_text, UNDOCUMENTED_CLI_OK)
        self.assertEqual(
            missing, [],
            f"pipeline-cli subcommand(s) {missing} have no mention in "
            f"docs/debugging-cli.md. Add a line/table row (preferred), or "
            f"add to UNDOCUMENTED_CLI_OK with a one-line rationale if it's "
            f"genuinely not an operator-debugging surface.",
        )

    def test_scan_is_not_vacuous(self) -> None:
        names = _top_level_cli_subcommand_names()
        self.assertGreaterEqual(len(names), 20)

    def test_undocumented_subcommands_helper_flags_a_bogus_command(self) -> None:
        """Known-bad self-test: the diff helper actually has teeth."""
        missing = _undocumented_subcommands(
            ["show", "bogus570cmd"], "pipeline-cli show <id>", frozenset())
        self.assertEqual(missing, ["bogus570cmd"])


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

# Pre-existing gaps at audit-creation time (issue #570) — 37 options
# declared before this scan existed. Ratchet: no NEW option may join this
# list (test_no_new_options_without_description catches it); entries
# should shrink to zero as each option earns a real description
# (test_allowlist_entries_still_missing_description catches staleness).
OPTIONS_WITHOUT_DESCRIPTION_OK: frozenset[str] = frozenset({
    "slskd.deleteSearches",                       # TODO: document
    "redis.port",                                 # TODO: document
    "web.port",                                   # TODO: document
    "web.redis.port",                              # TODO: document
    "notifiers.meelo.url",                        # TODO: document
    "notifiers.meelo.usernameFile",               # TODO: document
    "notifiers.meelo.passwordFile",               # TODO: document
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
    "searchSettings.numberOfAlbumsToGrab",        # TODO: document
    "searchSettings.titleBlacklist",              # TODO: document
    "searchSettings.searchBlacklist",             # TODO: document
    "downloadSettings.downloadFiltering",         # TODO: document
    "downloadSettings.useExtensionWhitelist",     # TODO: document
    "downloadSettings.extensionsWhitelist",       # TODO: document
    "qualityRanks.gateMinRank",                   # TODO: document
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

    Ratchet, not a hard gate (issue #570): 37 pre-existing options lacked
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
