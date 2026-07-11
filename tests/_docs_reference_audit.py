"""Structural reference scanners for the living-docs audit."""

from __future__ import annotations

import ast
import re
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
    "shell.nix",
})
_REMOVAL_STABLE_ROOT_FILE_FAMILIES_RE = re.compile(
    r"TODO(?:[-_A-Za-z0-9]*)?\.md$"
)
_CODE_SPAN_RE = re.compile(
    r"(?<!`)(?P<fence>`{1,2})(?!`)(?P<code>[^`\n]+?)(?P=fence)(?!`)"
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


def broken_repo_references(
    path: Path,
    text: str,
    repo_root: Path,
) -> list[str]:
    """Return unresolved repo-path and ``path.py::symbol`` references."""
    findings: list[str] = []
    rel_source = _relative(path, repo_root)
    for code, line in _code_spans(text):
        normalised_code = code[2:] if code.startswith("./") else code
        symbol_match = _PY_SYMBOL_REFERENCE_RE.fullmatch(normalised_code)
        if symbol_match is not None:
            repo_path = symbol_match.group("path")
            symbol = symbol_match.group("symbol")
            target = repo_root / repo_path
            if not target.is_file():
                findings.append(
                    f"{rel_source}:{line}: missing path {repo_path}"
                )
                continue
            target_text = target.read_text(encoding="utf-8")
            if not _symbol_occurs(symbol, target_text):
                findings.append(
                    f"{rel_source}:{line}: {repo_path} has no symbol {symbol}"
                )
            continue

        repo_path = _normalise_repo_path(code, repo_root)
        if repo_path is None:
            continue
        if not (repo_root / repo_path).exists():
            findings.append(f"{rel_source}:{line}: missing path {repo_path}")
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
