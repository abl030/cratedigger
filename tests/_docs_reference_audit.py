"""Structural reference scanners for the living-docs audit."""

from __future__ import annotations

import ast
import re
import subprocess
from pathlib import Path

_FROZEN_DOC_DIRS = frozenset({"plans", "brainstorms", "solutions"})
REMOVAL_STABLE_REPO_ROOTS = frozenset({
    ".agents",
    ".claude",
    ".codex",
    "docs",
    "examples",
    "harness",
    "lib",
    "migrations",
    "nix",
    "scripts",
    "tests",
    "tools",
    "web",
})
REMOVAL_STABLE_ROOT_FILES = frozenset({
    ".gitignore",
    ".mcp.json",
    "AGENTS.md",
    "CLAUDE.md",
    "LICENSE",
    "README.md",
    "TODO-audio-quality.md",
    "TODO-type-safety.md",
    "TODO.md",
    "album_source.py",
    "cratedigger.py",
    "flake.lock",
    "flake.nix",
    "pyrightconfig.json",
    "pyrightconfig.production.json",
    "pyrightconfig.strict-production.json",
    "shell.nix",
})
_REMOVAL_STABLE_ROOT_FILE_FAMILIES_RE = re.compile(
    r"TODO(?:[-_A-Za-z0-9]*)?\.md$"
)
_CODE_SPAN_RE = re.compile(
    r"(?<!`)(?P<fence>`{1,2})(?!`)(?P<code>[^`\n]+?)(?P=fence)(?!`)"
)
_FENCE_OPEN_RE = re.compile(
    r"^ {0,3}(?P<fence>`{3,}|~{3,})(?P<info>[^\r\n]*)$",
)
_FENCED_COMMAND_TOKEN_RE = re.compile(
    r"(?<![A-Za-z0-9_./~${}-])"
    r"(?P<code>(?:\./)?[A-Za-z0-9_.-]+"
    r"(?:/[A-Za-z0-9_.*?{}\[\]<>$-]+)*"
    r"(?:#[A-Za-z0-9_.-]+)?(?::\d+(?:-\d+)?)?)"
    r"(?![A-Za-z0-9_./-])"
)
_PY_SYMBOL_REFERENCE_RE = re.compile(
    r"^(?P<path>(?:[A-Za-z0-9_.-]+/)*[A-Za-z0-9_.-]+\.py)"
    r"::(?P<symbol>[A-Za-z_][A-Za-z0-9_.]*)"
    r"(?:\([^`\n]*\))?$"
)
_CALL_FORM_RE = re.compile(
    r"^(?P<identifier>_?[a-z][a-z0-9_]*_[a-z0-9_]*)\([^`\n]*\)$"
)
_LINE_SUFFIX_RE = re.compile(r":\d+(?:-\d+)?$")
_TEMPLATE_MARKERS_RE = re.compile(r"[*?\[\]{}<>$]|(?:^|/)N{2,}[A-Za-z0-9_-]*")

# These RST families are relative to the Beets source tree resolved by the
# beets-docs skill, not Cratedigger. The source and exact upstream families
# are both pinned so Cratedigger-owned ``docs/`` references remain audited.
_SKILL_EXTERNAL_REFERENCE_RULES = {
    ".claude/skills/beets-docs/SKILL.md": (
        re.compile(
            r"docs/(?:(?:reference|plugins|guides)/[^/]+\.rst|faq\.rst)"
        ),
        "Beets RST path resolved below the skill's BEETS_SRC directory.",
    ),
}


def _relative(path: Path, repo_root: Path) -> str:
    try:
        return path.relative_to(repo_root).as_posix()
    except ValueError:
        return path.as_posix()


def _code_spans(text: str) -> list[tuple[str, int]]:
    """Return inline-code contents with their 1-based source line."""
    return [
        (match.group("code").strip(), text.count("\n", 0, match.start()) + 1)
        for match in _CODE_SPAN_RE.finditer(text)
    ]


def _command_token_spans(
    text: str,
    *,
    first_line: int,
) -> list[tuple[str, int]]:
    """Return safely delimited command tokens with source lines."""
    return [
        (
            match.group("code"),
            first_line + text.count("\n", 0, match.start("code")),
        )
        for match in _FENCED_COMMAND_TOKEN_RE.finditer(text)
    ]


def _fenced_command_token_spans(text: str) -> list[tuple[str, int]]:
    """Return tokens inside CommonMark fenced code blocks."""
    lines = text.splitlines(keepends=True)
    spans: list[tuple[str, int]] = []
    index = 0
    while index < len(lines):
        opening_line = lines[index].rstrip("\r\n")
        opening = _FENCE_OPEN_RE.fullmatch(opening_line)
        if opening is None:
            index += 1
            continue

        fence = opening.group("fence")
        if fence[0] == "`" and "`" in opening.group("info"):
            index += 1
            continue
        closing_re = re.compile(
            rf"^ {{0,3}}{re.escape(fence[0])}{{{len(fence)},}}[ \t]*$"
        )
        content_start = index + 1
        closing_index = content_start
        while closing_index < len(lines):
            candidate = lines[closing_index].rstrip("\r\n")
            if closing_re.fullmatch(candidate):
                break
            closing_index += 1

        block_text = "".join(lines[content_start:closing_index])
        spans.extend(_command_token_spans(
            block_text,
            first_line=content_start + 1,
        ))
        index = closing_index + 1
    return spans


def _is_external_skill_reference(rel_source: str, code: str) -> bool:
    """Return whether a path is explicitly relative to an upstream tree."""
    rule = _SKILL_EXTERNAL_REFERENCE_RULES.get(rel_source)
    if rule is None:
        return False
    pattern, _rationale = rule
    return pattern.fullmatch(code) is not None


def _is_repo_path_candidate(value: str, repo_root: Path) -> bool:
    if "/" not in value:
        return ((repo_root / value).is_file()
                or value in REMOVAL_STABLE_ROOT_FILES
                or bool(_REMOVAL_STABLE_ROOT_FILE_FAMILIES_RE.fullmatch(value)))
    root, _remainder = value.split("/", 1)
    return root in REMOVAL_STABLE_REPO_ROOTS


def _normalise_repo_path(value: str, repo_root: Path) -> str | None:
    """Return a literal repo path, excluding glob/template conventions."""
    without_anchor = value.split("#", 1)[0]
    if without_anchor.startswith("./"):
        without_anchor = without_anchor[2:]
    without_line = _LINE_SUFFIX_RE.sub("", without_anchor)
    if not _is_repo_path_candidate(without_line, repo_root):
        return None
    if _TEMPLATE_MARKERS_RE.search(without_line):
        return None
    return without_line.rstrip("/")


def _symbol_occurs(symbol: str, source_text: str) -> bool:
    """Check the referenced leaf symbol as an identifier in its target."""
    leaf = symbol.rsplit(".", 1)[-1]
    return re.search(rf"\b{re.escape(leaf)}\b", source_text) is not None


def living_doc_files(repo_root: Path) -> list[Path]:
    """Return documentation whose code references must stay current."""
    files = {repo_root / "CLAUDE.md"}
    files.update(repo_root.rglob("README.md"))
    files.update((repo_root / ".claude" / "rules").rglob("*.md"))
    for path in (repo_root / "docs").rglob("*.md"):
        relative = path.relative_to(repo_root / "docs")
        if relative.parts and relative.parts[0] in _FROZEN_DOC_DIRS:
            continue
        files.add(path)
    living: list[Path] = []
    for path in files:
        repo_relative = path.relative_to(repo_root)
        if repo_relative.parts[:2] == (".claude", "worktrees"):
            continue
        try:
            docs_relative = path.relative_to(repo_root / "docs")
        except ValueError:
            pass
        else:
            if docs_relative.parts and docs_relative.parts[0] in _FROZEN_DOC_DIRS:
                continue
        living.append(path)
    return sorted(living)


def tracked_skill_instruction_files(repo_root: Path) -> list[Path]:
    """Return git-tracked, repo-owned skill instruction files only."""
    result = subprocess.run(
        ["git", "ls-files", "-z", "--", ".claude/skills/**/SKILL.md"],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )
    return sorted(
        repo_root / value
        for value in result.stdout.split("\0")
        if value
    )


def _broken_repo_reference(
    path: Path,
    code: str,
    line: int,
    repo_root: Path,
) -> str | None:
    rel_source = _relative(path, repo_root)
    normalised_code = code[2:] if code.startswith("./") else code
    symbol_match = _PY_SYMBOL_REFERENCE_RE.fullmatch(normalised_code)
    if symbol_match is not None:
        repo_path = symbol_match.group("path")
        symbol = symbol_match.group("symbol")
        target = repo_root / repo_path
        if not target.is_file():
            return f"{rel_source}:{line}: missing path {repo_path}"
        target_text = target.read_text(encoding="utf-8")
        if not _symbol_occurs(symbol, target_text):
            return f"{rel_source}:{line}: {repo_path} has no symbol {symbol}"
        return None

    repo_path = _normalise_repo_path(code, repo_root)
    if repo_path is None or (repo_root / repo_path).exists():
        return None
    return f"{rel_source}:{line}: missing path {repo_path}"


def broken_repo_references(
    path: Path,
    text: str,
    repo_root: Path,
) -> list[str]:
    """Return unresolved repo-path and ``path.py::symbol`` references."""
    findings: list[str] = []
    for code, line in _code_spans(text):
        finding = _broken_repo_reference(path, code, line, repo_root)
        if finding is not None:
            findings.append(finding)
    return findings


def broken_skill_instruction_references(
    path: Path,
    text: str,
    repo_root: Path,
) -> list[str]:
    """Return stale repo paths in tracked skill prose and command blocks."""
    rel_source = _relative(path, repo_root)
    spans: list[tuple[str, int]] = []
    for code, line in _code_spans(text):
        spans.append((code, line))
        spans.extend(_command_token_spans(code, first_line=line))
    spans.extend(_fenced_command_token_spans(text))

    findings: list[str] = []
    seen_findings: set[str] = set()
    for code, line in spans:
        if _is_external_skill_reference(rel_source, code):
            continue
        finding = _broken_repo_reference(path, code, line, repo_root)
        if finding is not None and finding not in seen_findings:
            findings.append(finding)
            seen_findings.add(finding)
    return findings


def missing_call_references(
    path: Path,
    text: str,
    repo_root: Path,
    code_identifiers: set[str],
    allowlist: dict[str, str],
    *,
    scope: str,
) -> list[str]:
    """Return unresolved backticked snake_case call-form identifiers."""
    rel_source = _relative(path, repo_root)
    missing: set[str] = set()
    for code, _line in _code_spans(text):
        match = _CALL_FORM_RE.fullmatch(code)
        if match is None:
            continue
        identifier = match.group("identifier")
        key = f"{rel_source}::{scope}::{identifier}"
        if identifier not in code_identifiers and key not in allowlist:
            missing.add(key)
    return sorted(missing)


def python_code_identifiers(repo_root: Path) -> set[str]:
    """Collect callable definitions/imports and names actually called."""
    identifiers: set[str] = set()
    roots = [repo_root / name for name in ("harness", "lib", "scripts", "tests", "web")]
    paths = sorted(repo_root.glob("*.py"))
    for root in roots:
        paths.extend(sorted(root.rglob("*.py")))
    for path in paths:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                identifiers.add(node.name)
            elif isinstance(node, ast.alias):
                identifiers.add(node.asname or node.name.rsplit(".", 1)[-1])
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    identifiers.add(node.func.id)
                elif isinstance(node.func, ast.Attribute):
                    identifiers.add(node.func.attr)
    return identifiers


_DocstringNode = ast.Module | ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef


def _scoped_docstring_nodes(
    tree: ast.Module,
) -> list[tuple[str, _DocstringNode]]:
    """Return docstring-capable nodes with stable qualified scopes."""
    nodes: list[tuple[str, _DocstringNode]] = [("<module>", tree)]

    def visit_body(body: list[ast.stmt], parents: tuple[str, ...]) -> None:
        for node in body:
            if not isinstance(
                node,
                (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef),
            ):
                continue
            names = (*parents, node.name)
            nodes.append((".".join(names), node))
            visit_body(node.body, names)

    visit_body(tree.body, ())
    return nodes


def lib_docstrings(repo_root: Path) -> list[tuple[Path, str, str]]:
    """Return ``(path, scope, docstring)`` records under ``lib/``."""
    sources: list[tuple[Path, str, str]] = []
    for path in sorted((repo_root / "lib").rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for scope, node in _scoped_docstring_nodes(tree):
            docstring = ast.get_docstring(node, clean=False)
            if docstring is not None:
                sources.append((path, scope, docstring))
    return sources
