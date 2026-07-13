"""Fail-closed audit for the repository's small default-patch grammar."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


@dataclass(frozen=True, order=True)
class DefaultPatchFinding:
    """One direct call made without its captured dependency injected."""

    test_path: str
    line: int
    callable_path: str
    patched_target: str
    injectable_keyword: str


@dataclass(frozen=True)
class _Capture:
    callable_path: str
    keyword: str
    targets: frozenset[str]
    position: int | None
    production_path: str
    line: int
    unsupported: bool
    ambiguous: str | None


@dataclass(frozen=True)
class _Imports:
    candidates: Mapping[str, frozenset[str]]
    direct: frozenset[str]
    rebounds: Mapping[str, int]


@dataclass(frozen=True)
class _Use:
    capture: _Capture
    call: ast.Call
    error_line: int | None = None
    error: str | None = None


@dataclass(frozen=True)
class _Patch:
    call: ast.Call
    region: tuple[ast.AST, ...]
    manual: bool = False


@dataclass(frozen=True)
class _Evidence:
    targets: frozenset[str] | None
    owners: frozenset[str] | None
    attributes: frozenset[str]
    supported: bool
    error: str


_PRODUCTION_ROOTS = (
    "lib",
    "web",
    "harness",
    "scripts",
    "cratedigger.py",
    "album_source.py",
)
_PATCH_PREFIX = "unittest.mock.patch"


def _module_path(path: str) -> str:
    parts = list(Path(path).with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _dotted(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        owner = _dotted(node.value)
        if owner:
            return f"{owner}.{node.attr}"
    return None


def _imports(tree: ast.AST) -> _Imports:
    candidates: dict[str, set[str]] = {}
    direct: set[str] = set()
    imported_lines: dict[str, list[int]] = {}
    rebound_lines: dict[str, list[int]] = {}
    for node in ast.walk(tree):
        pairs: list[tuple[str, str]] = []
        if isinstance(node, ast.Import):
            for alias in node.names:
                bound = alias.asname or alias.name.split(".")[0]
                target = alias.name if alias.asname else bound
                pairs.append((bound, target))
        elif isinstance(node, ast.ImportFrom) and node.level == 0:
            pairs.extend(
                (
                    alias.asname or alias.name,
                    f"{node.module or ''}.{alias.name}".strip("."),
                )
                for alias in node.names
                if alias.name != "*"
            )
        for bound, target in pairs:
            candidates.setdefault(bound, set()).add(target)
            direct.add(bound)
            imported_lines.setdefault(bound, []).append(getattr(node, "lineno", 0))
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            rebound_lines.setdefault(node.name, []).append(node.lineno)
        elif isinstance(node, (ast.Assign, ast.AnnAssign, ast.NamedExpr)):
            targets: list[ast.expr]
            if isinstance(node, ast.Assign):
                targets = node.targets
            else:
                targets = [node.target]
            for target in targets:
                if isinstance(target, ast.Name):
                    rebound_lines.setdefault(target.id, []).append(
                        getattr(node, "lineno", 0),
                    )
    rebounds = {
        name: min(line for line in lines if line not in imported_lines.get(name, ()))
        for name, lines in rebound_lines.items()
        if name in imported_lines
        and any(line not in imported_lines[name] for line in lines)
    }
    return _Imports(
        candidates={key: frozenset(value) for key, value in candidates.items()},
        direct=frozenset(direct),
        rebounds=rebounds,
    )


def _resolve(name: str, imports: _Imports) -> frozenset[str]:
    root, dot, tail = name.partition(".")
    roots = imports.candidates.get(root)
    if not roots:
        return frozenset((name,))
    return frozenset(
        target + (dot + tail if dot else "") for target in roots
    )


def _references(node: ast.expr) -> tuple[str, ...]:
    names = {
        dotted
        for child in ast.walk(node)
        if isinstance(child, (ast.Name, ast.Attribute))
        and (dotted := _dotted(child))
    }
    return tuple(
        sorted(
            name
            for name in names
            if not any(other.startswith(f"{name}.") for other in names)
        )
    )


def _captures_for_definition(
    definition: ast.FunctionDef | ast.AsyncFunctionDef,
    *,
    callable_path: str,
    module: str,
    path: str,
    imports: _Imports,
    bound_method: bool,
) -> list[_Capture]:
    captures: list[_Capture] = []
    positional = [*definition.args.posonlyargs, *definition.args.args]
    offset = len(positional) - len(definition.args.defaults)
    defaults: list[tuple[ast.arg, ast.expr, int | None]] = [
        (argument, default, index - int(bound_method))
        for index, (argument, default) in enumerate(
            zip(positional[offset:], definition.args.defaults),
            start=offset,
        )
    ]
    defaults.extend(
        (argument, default, None)
        for argument, default in zip(
            definition.args.kwonlyargs,
            definition.args.kw_defaults,
        )
        if default is not None
    )
    for argument, default, position in defaults:
        references = _references(default)
        if not references:
            continue
        targets: set[str] = set()
        ambiguous: str | None = None
        for reference in references:
            root = reference.partition(".")[0]
            resolved = _resolve(reference, imports)
            if "." not in reference:
                targets.add(f"{module}.{reference}")
            targets.update(resolved)
            if len(imports.candidates.get(root, ())) > 1:
                ambiguous = root
        captures.append(
            _Capture(
                callable_path=callable_path,
                keyword=argument.arg,
                targets=frozenset(targets),
                position=position,
                production_path=path,
                line=default.lineno,
                unsupported=_dotted(default) is None,
                ambiguous=ambiguous,
            ),
        )
    return captures


def _production_captures(sources: Mapping[str, str]) -> tuple[_Capture, ...]:
    captures: list[_Capture] = []
    for path, source in sorted(sources.items()):
        tree = ast.parse(source, filename=path)
        module = _module_path(path)
        imports = _imports(tree)
        for statement in tree.body:
            if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)):
                captures.extend(
                    _captures_for_definition(
                        statement,
                        callable_path=f"{module}.{statement.name}",
                        module=module,
                        path=path,
                        imports=imports,
                        bound_method=False,
                    ),
                )
            elif isinstance(statement, ast.ClassDef):
                for member in statement.body:
                    if not isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        continue
                    captures.extend(
                        _captures_for_definition(
                            member,
                            callable_path=f"{module}.{statement.name}.{member.name}",
                            module=module,
                            path=path,
                            imports=imports,
                            bound_method=True,
                        ),
                    )
    return tuple(captures)


def _patch_name(call: ast.Call, imports: _Imports) -> str | None:
    dotted = _dotted(call.func)
    if not dotted:
        return None
    resolved = _resolve(dotted, imports)
    matches = [name for name in resolved if name.startswith(_PATCH_PREFIX)]
    return matches[0] if len(matches) == 1 else None


def _patches(tree: ast.AST, imports: _Imports) -> tuple[_Patch, ...]:
    found: list[_Patch] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.With, ast.AsyncWith)):
            for index, item in enumerate(node.items):
                call = item.context_expr
                if isinstance(call, ast.Call) and _patch_name(call, imports):
                    found.append(
                        _Patch(
                            call,
                            tuple(
                                [
                                    other.context_expr
                                    for other in node.items[index + 1 :]
                                ]
                                + node.body
                            ),
                        ),
                    )
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for decorator in node.decorator_list:
                if isinstance(decorator, ast.Call) and _patch_name(decorator, imports):
                    found.append(_Patch(decorator, tuple(node.body)))
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            value = node.value
            if isinstance(value, ast.Call) and _patch_name(value, imports):
                found.append(_Patch(value, (tree,), manual=True))
    return tuple(found)


def _direct_capture(
    expression: ast.expr,
    captures: tuple[_Capture, ...],
    imports: _Imports,
) -> tuple[_Capture, ...]:
    name = _dotted(expression)
    if not name:
        return ()
    resolved = _resolve(name, imports)
    return tuple(
        capture
        for capture in captures
        if capture.callable_path in resolved
        or (
            capture.callable_path.endswith(".__init__")
            and capture.callable_path.removesuffix(".__init__") in resolved
        )
    )


def _assigned_uses(
    tree: ast.AST,
    captures: tuple[_Capture, ...],
    imports: _Imports,
) -> tuple[dict[str, tuple[_Capture, int]], dict[str, tuple[str, int]]]:
    aliases: dict[str, tuple[_Capture, int]] = {}
    instances: dict[str, tuple[str, int]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        value = node.value
        if value is None:
            continue
        for target in targets:
            if not isinstance(target, ast.Name):
                continue
            direct = _direct_capture(value, captures, imports)
            if len(direct) == 1:
                aliases[target.id] = (direct[0], node.lineno)
            if isinstance(value, ast.Call):
                name = _dotted(value.func)
                if not name:
                    continue
                for resolved in _resolve(name, imports):
                    instances[target.id] = (resolved, node.lineno)
    return aliases, instances


def _uses(
    patch: _Patch,
    captures: tuple[_Capture, ...],
    imports: _Imports,
    aliases: Mapping[str, tuple[_Capture, int]],
    instances: Mapping[str, tuple[str, int]],
) -> tuple[_Use, ...]:
    uses: list[_Use] = []
    seen: set[tuple[int, str]] = set()
    for root in patch.region:
        for node in ast.walk(root):
            if not isinstance(node, ast.Call):
                continue
            for capture in _direct_capture(node.func, captures, imports):
                key = (id(node), capture.callable_path)
                if key in seen:
                    continue
                seen.add(key)
                syntactic = _dotted(node.func) or capture.callable_path
                binding = syntactic.partition(".")[0]
                candidates = imports.candidates.get(binding, ())
                if len(candidates) > 1:
                    uses.append(
                        _Use(
                            capture,
                            node,
                            node.lineno,
                            f"imported callable binding {binding} has conflicting imports",
                        ),
                    )
                elif binding in imports.rebounds:
                    uses.append(
                        _Use(
                            capture,
                            node,
                            imports.rebounds[binding],
                            f"imported callable binding {binding} is rebound",
                        ),
                    )
                else:
                    uses.append(_Use(capture, node))
            name = _dotted(node.func)
            if not name:
                continue
            root_name, dot, member = name.partition(".")
            if not dot and root_name in aliases:
                capture, line = aliases[root_name]
                uses.append(
                    _Use(
                        capture,
                        node,
                        line,
                        f"{capture.callable_path} must be called directly",
                    ),
                )
            if dot and root_name in instances:
                class_path, line = instances[root_name]
                for capture in captures:
                    if capture.callable_path == f"{class_path}.{member}":
                        uses.append(
                            _Use(
                                capture,
                                node,
                                line,
                                f"assigned instance call {name} cannot be proven",
                            ),
                        )
    return tuple(uses)


def _owner(node: ast.expr, imports: _Imports) -> tuple[frozenset[str] | None, bool]:
    name = _dotted(node)
    if not name:
        return None, False
    root = name.partition(".")[0]
    if root not in imports.candidates:
        return None, False
    resolved = _resolve(name.removesuffix(".__dict__"), imports)
    direct = root in imports.direct and root not in imports.rebounds
    return resolved, direct and len(imports.candidates.get(root, ())) == 1


def _evidence(call: ast.Call, imports: _Imports) -> _Evidence:
    patch_name = _patch_name(call, imports) or ""
    kind = patch_name.removeprefix(_PATCH_PREFIX).lstrip(".")
    if not kind:
        if call.args and isinstance(call.args[0], ast.Constant) and isinstance(
            call.args[0].value,
            str,
        ):
            return _Evidence(
                frozenset((call.args[0].value,)),
                None,
                frozenset(),
                True,
                "",
            )
        return _Evidence(
            None,
            None,
            frozenset(),
            False,
            "patch target must be a string literal",
        )
    if kind == "object":
        owners, direct = _owner(call.args[0], imports) if call.args else (None, False)
        attribute = (
            call.args[1].value
            if len(call.args) > 1
            and isinstance(call.args[1], ast.Constant)
            and isinstance(call.args[1].value, str)
            else None
        )
        targets = (
            frozenset(f"{owner}.{attribute}" for owner in owners)
            if owners and attribute
            else None
        )
        if not attribute:
            return _Evidence(
                targets,
                owners,
                frozenset(),
                False,
                "patch.object attribute must be a string literal",
            )
        return _Evidence(
            targets,
            owners,
            frozenset((attribute,)),
            direct,
            "patch.object owner must be a direct import",
        )
    if kind in {"dict", "multiple"}:
        owner_node = call.args[0] if call.args else None
        owners, _ = _owner(owner_node, imports) if owner_node else (None, False)
        attributes: set[str] = set()
        for keyword in call.keywords:
            if keyword.arg is not None and keyword.arg not in {"clear", "values"}:
                attributes.add(keyword.arg)
        values = next(
            (keyword.value for keyword in call.keywords if keyword.arg == "values"),
            None,
        )
        if isinstance(values, ast.Dict):
            attributes.update(
                key.value
                for key in values.keys
                if isinstance(key, ast.Constant) and isinstance(key.value, str)
            )
        targets = (
            frozenset(f"{owner}.{attribute}" for owner in owners for attribute in attributes)
            if owners and attributes
            else None
        )
        suffix = next(iter(sorted(attributes)), "unknown")
        return _Evidence(
            targets,
            owners,
            frozenset(attributes),
            False,
            f"patch.{kind} overlaps captured target {suffix}",
        )
    return _Evidence(
        None,
        None,
        frozenset(),
        False,
        f"unsupported patch helper patch.{kind}",
    )


def _relevant(evidence: _Evidence, capture: _Capture) -> bool:
    if evidence.targets is not None:
        return bool(evidence.targets & capture.targets)
    if evidence.owners is not None:
        return any(
            any(target.startswith(f"{owner}.") for owner in evidence.owners)
            for target in capture.targets
        )
    if evidence.attributes:
        return any(
            target.rpartition(".")[2] in evidence.attributes
            for target in capture.targets
        )
    return True


def _error(path: str, line: int, message: str) -> ValueError:
    return ValueError(
        f"{path}:{line}: unsupported definition-default patch syntax: {message}",
    )


def find_ineffective_default_patches(
    production_sources: Mapping[str, str],
    test_sources: Mapping[str, str],
) -> tuple[DefaultPatchFinding, ...]:
    """Check direct captured-default calls inside canonical patch regions."""
    captures = _production_captures(production_sources)
    findings: set[DefaultPatchFinding] = set()
    for path, source in sorted(test_sources.items()):
        tree = ast.parse(source, filename=path)
        imports = _imports(tree)
        aliases, instances = _assigned_uses(tree, captures, imports)
        for patch in _patches(tree, imports):
            evidence = _evidence(patch.call, imports)
            for use in _uses(patch, captures, imports, aliases, instances):
                capture = use.capture
                if not _relevant(evidence, capture):
                    continue
                if use.error:
                    raise _error(path, use.error_line or use.call.lineno, use.error)
                injected = any(
                    keyword.arg == capture.keyword for keyword in use.call.keywords
                )
                if injected:
                    continue
                if capture.position is not None and len(use.call.args) > capture.position:
                    raise _error(
                        path,
                        use.call.lineno,
                        f"inject {capture.keyword} as an explicit keyword",
                    )
                if patch.manual:
                    raise _error(
                        path,
                        patch.call.lineno,
                        "captured-default patches must be direct with-items or test decorators",
                    )
                if not evidence.supported:
                    raise _error(path, patch.call.lineno, evidence.error)
                if capture.ambiguous:
                    raise ValueError(
                        f"{capture.production_path}:{capture.line}: unsupported "
                        f"definition-default expression for {capture.callable_path}."
                        f"{capture.keyword}: binding {capture.ambiguous} has "
                        "conflicting imports",
                    )
                if capture.unsupported:
                    raise ValueError(
                        f"{capture.production_path}:{capture.line}: unsupported "
                        f"definition-default expression for {capture.callable_path}."
                        f"{capture.keyword}",
                    )
                patched_target = next(iter(sorted(evidence.targets or capture.targets)))
                findings.add(
                    DefaultPatchFinding(
                        test_path=path,
                        line=use.call.lineno,
                        callable_path=capture.callable_path,
                        patched_target=patched_target,
                        injectable_keyword=capture.keyword,
                    ),
                )
    return tuple(sorted(findings))


def _repository_sources(repo_root: Path) -> tuple[dict[str, str], dict[str, str]]:
    production: dict[str, str] = {}
    tests: dict[str, str] = {}
    for path in repo_root.rglob("*.py"):
        relative = path.relative_to(repo_root).as_posix()
        if any(part.startswith(".") for part in path.relative_to(repo_root).parts):
            continue
        if relative.startswith("tests/"):
            if not Path(relative).name.startswith("_"):
                tests[relative] = path.read_text()
        elif any(
            relative == root or relative.startswith(f"{root}/")
            for root in _PRODUCTION_ROOTS
        ):
            production[relative] = path.read_text()
    return production, tests


def repository_default_patch_findings(
    repo_root: Path,
) -> tuple[DefaultPatchFinding, ...]:
    """Run the audit against the checked-out repository."""
    production, tests = _repository_sources(repo_root)
    return find_ineffective_default_patches(production, tests)
