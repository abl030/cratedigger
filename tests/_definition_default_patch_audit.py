"""Fail-closed audit for the repository's canonical default-patch grammar."""

from __future__ import annotations

import ast
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


@dataclass(frozen=True, order=True)
class DefaultPatchFinding:
    """One canonical call made without its captured dependency injected."""

    test_path: str
    line: int
    callable_path: str
    patched_target: str
    injectable_keyword: str


@dataclass(frozen=True)
class _Capture:
    callable_path: str
    injectable_keyword: str
    targets: frozenset[str]
    positional_only: bool
    positional_index: int | None


@dataclass(frozen=True)
class _PatchRegion:
    call: ast.Call
    target: str
    body: tuple[ast.stmt, ...]
    function_scope: ast.FunctionDef | ast.AsyncFunctionDef | None


_PRODUCTION_ROOTS = (
    "lib",
    "web",
    "harness",
    "scripts",
    "cratedigger.py",
    "album_source.py",
)


def _module_path(relative_path: str) -> str:
    parts = list(Path(relative_path).with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _dotted_name(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        owner = _dotted_name(node.value)
        if owner is not None:
            return f"{owner}.{node.attr}"
    return None


def _bind_imports(
    statements: Iterable[ast.stmt],
) -> tuple[dict[str, str], set[str]]:
    """Collect direct canonical imports; conflicting bindings are ambiguous."""
    bindings: dict[str, str] = {}
    ambiguous: set[str] = set()
    for statement in statements:
        names: list[tuple[str, str]] = []
        if isinstance(statement, ast.Import):
            names.extend(
                (
                    alias.asname or alias.name.split(".")[0],
                    alias.name if alias.asname else alias.name.split(".")[0],
                )
                for alias in statement.names
            )
        elif isinstance(statement, ast.ImportFrom):
            if statement.level != 0 or any(
                alias.name == "*" for alias in statement.names
            ):
                ambiguous.update(
                    alias.asname or alias.name for alias in statement.names
                )
                continue
            module = statement.module or ""
            names.extend(
                (alias.asname or alias.name, f"{module}.{alias.name}")
                for alias in statement.names
            )
        for bound, resolved in names:
            prior = bindings.get(bound)
            if prior is not None and prior != resolved:
                ambiguous.add(bound)
            else:
                bindings[bound] = resolved
    return bindings, ambiguous


def _resolve_dotted(name: str, bindings: Mapping[str, str]) -> str:
    root, separator, remainder = name.partition(".")
    resolved_root = bindings.get(root, root)
    return resolved_root + (separator + remainder if separator else "")


def _default_targets(
    default: ast.expr,
    *,
    module: str,
    bindings: Mapping[str, str],
) -> frozenset[str]:
    dotted = _dotted_name(default)
    if dotted is None:
        return frozenset()
    resolved = _resolve_dotted(dotted, bindings)
    local = f"{module}.{dotted}"
    root = dotted.partition(".")[0]
    return frozenset((local, resolved)) if root in bindings else frozenset((local,))


def _definition_captures(
    definition: ast.FunctionDef | ast.AsyncFunctionDef,
    *,
    callable_path: str,
    module: str,
    bindings: Mapping[str, str],
) -> tuple[_Capture, ...]:
    captures: list[_Capture] = []
    positional = [*definition.args.posonlyargs, *definition.args.args]
    offset = len(positional) - len(definition.args.defaults)
    for index, default in enumerate(definition.args.defaults, start=offset):
        targets = _default_targets(default, module=module, bindings=bindings)
        if targets:
            captures.append(
                _Capture(
                    callable_path=callable_path,
                    injectable_keyword=positional[index].arg,
                    targets=targets,
                    positional_only=index < len(definition.args.posonlyargs),
                    positional_index=index,
                ),
            )
    for argument, default in zip(
        definition.args.kwonlyargs,
        definition.args.kw_defaults,
    ):
        if default is None:
            continue
        targets = _default_targets(default, module=module, bindings=bindings)
        if targets:
            captures.append(
                _Capture(callable_path, argument.arg, targets, False, None),
            )
    return tuple(captures)


def _captured_defaults(
    production_sources: Mapping[str, str],
) -> tuple[_Capture, ...]:
    captures: list[_Capture] = []
    for relative_path, source in production_sources.items():
        module = _module_path(relative_path)
        if not module:
            continue
        tree = ast.parse(source, filename=relative_path)
        bindings, _ = _bind_imports(tree.body)
        for statement in tree.body:
            definitions: list[
                tuple[ast.FunctionDef | ast.AsyncFunctionDef, str]
            ] = []
            if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)):
                definitions.append((statement, f"{module}.{statement.name}"))
            elif isinstance(statement, ast.ClassDef):
                definitions.extend(
                    (member, f"{module}.{statement.name}.{member.name}")
                    for member in statement.body
                    if isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef))
                )
            for definition, callable_path in definitions:
                captures.extend(
                    _definition_captures(
                        definition,
                        callable_path=callable_path,
                        module=module,
                        bindings=bindings,
                    ),
                )
    return tuple(captures)


def _parents(tree: ast.AST) -> dict[ast.AST, ast.AST]:
    return {
        child: parent
        for parent in ast.walk(tree)
        for child in ast.iter_child_nodes(parent)
    }


def _nearest_function(
    node: ast.AST,
    parents: Mapping[ast.AST, ast.AST],
) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
    current = parents.get(node)
    while current is not None:
        if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return current
        current = parents.get(current)
    return None


def _scope_imports(
    tree: ast.Module,
    function: ast.FunctionDef | ast.AsyncFunctionDef | None,
    parents: Mapping[ast.AST, ast.AST],
) -> Iterable[ast.stmt]:
    imports = [
        node
        for node in tree.body
        if isinstance(node, (ast.Import, ast.ImportFrom))
    ]
    if function is not None:
        imports.extend(
            node
            for node in ast.walk(function)
            if isinstance(node, (ast.Import, ast.ImportFrom))
            and _nearest_function(node, parents) is function
        )
    return imports


def _scope_rebound_names(
    tree: ast.Module,
    function: ast.FunctionDef | ast.AsyncFunctionDef | None,
    parents: Mapping[ast.AST, ast.AST],
) -> dict[str, int]:
    root: ast.AST = function if function is not None else tree
    rebound = {
        node.id: node.lineno
        for node in ast.walk(root)
        if isinstance(node, ast.Name)
        and isinstance(node.ctx, ast.Store)
        and _nearest_function(node, parents) is function
    }
    if function is not None:
        arguments = [
            *function.args.posonlyargs,
            *function.args.args,
            *function.args.kwonlyargs,
        ]
        if function.args.vararg is not None:
            arguments.append(function.args.vararg)
        if function.args.kwarg is not None:
            arguments.append(function.args.kwarg)
        rebound.update({argument.arg: argument.lineno for argument in arguments})
    return rebound


def _patch_target(
    call: ast.Call,
    *,
    test_path: str,
    bindings: Mapping[str, str],
) -> str | None:
    dotted = _dotted_name(call.func)
    if dotted is None:
        return None
    resolved = _resolve_dotted(dotted, bindings)
    if resolved == "unittest.mock.patch":
        if not call.args or not (
            isinstance(call.args[0], ast.Constant)
            and isinstance(call.args[0].value, str)
        ):
            raise ValueError(
                f"{test_path}:{call.lineno}: unsupported definition-default "
                "patch syntax: patch target must be a string literal"
            )
        return call.args[0].value
    if resolved != "unittest.mock.patch.object" or len(call.args) < 2:
        return None
    owner = _dotted_name(call.args[0])
    attribute = call.args[1]
    if owner is None or not (
        isinstance(attribute, ast.Constant)
        and isinstance(attribute.value, str)
    ):
        return None
    return f"{_resolve_dotted(owner, bindings)}.{attribute.value}"


def _canonical_patch_spellings(tree: ast.Module) -> frozenset[str]:
    """Return the exact direct/qualified patch names imported in this file."""
    spellings: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.level == 0:
            if node.module == "unittest.mock":
                for alias in node.names:
                    if alias.name == "patch":
                        bound = alias.asname or alias.name
                        spellings.update((bound, f"{bound}.object"))
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "unittest.mock" and alias.asname:
                    spellings.update(
                        (f"{alias.asname}.patch", f"{alias.asname}.patch.object"),
                    )
    return frozenset(spellings)


def _patch_region(
    call: ast.Call,
    *,
    target: str,
    parents: Mapping[ast.AST, ast.AST],
) -> _PatchRegion | None:
    parent = parents.get(call)
    if isinstance(parent, ast.withitem) and parent.context_expr is call:
        with_node = parents.get(parent)
        if isinstance(with_node, (ast.With, ast.AsyncWith)):
            return _PatchRegion(
                call,
                target,
                tuple(with_node.body),
                _nearest_function(with_node, parents),
            )
    if isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        if call in parent.decorator_list:
            return _PatchRegion(
                call,
                target,
                tuple(parent.body),
                parent if isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef)) else None,
            )
    return None


def _walk_statements(statements: tuple[ast.stmt, ...]) -> list[ast.AST]:
    return [node for statement in statements for node in ast.walk(statement)]


def _scope_uses_capture(
    tree: ast.Module,
    *,
    function_scope: ast.FunctionDef | ast.AsyncFunctionDef | None,
    capture: _Capture,
    bindings: Mapping[str, str],
    parents: Mapping[ast.AST, ast.AST],
) -> bool:
    return any(
        isinstance(node, (ast.Name, ast.Attribute))
        and (dotted := _dotted_name(node)) is not None
        and _resolve_dotted(dotted, bindings) == capture.callable_path
        and _nearest_function(node, parents) is function_scope
        for node in ast.walk(tree)
    )


def _reference_bindings(
    node: ast.AST,
    *,
    outer_scope: ast.FunctionDef | ast.AsyncFunctionDef | None,
    outer_bindings: Mapping[str, str],
    parents: Mapping[ast.AST, ast.AST],
) -> tuple[dict[str, str], set[str]]:
    bindings = dict(outer_bindings)
    scope = _nearest_function(node, parents)
    if scope is outer_scope or scope is None:
        return bindings, set()
    imports = [
        candidate
        for candidate in ast.walk(scope)
        if isinstance(candidate, (ast.Import, ast.ImportFrom))
        and _nearest_function(candidate, parents) is scope
    ]
    nested, ambiguous = _bind_imports(imports)
    for name, resolved in nested.items():
        prior = bindings.get(name)
        if prior is not None and prior != resolved:
            ambiguous.add(name)
        bindings[name] = resolved
    return bindings, ambiguous


def _validate_scope_capture_syntax(
    *,
    test_path: str,
    tree: ast.Module,
    function_scope: ast.FunctionDef | ast.AsyncFunctionDef | None,
    capture: _Capture,
    bindings: Mapping[str, str],
    rebound_names: Mapping[str, int],
    parents: Mapping[ast.AST, ast.AST],
) -> None:
    for name, line in rebound_names.items():
        resolved = bindings.get(name)
        if resolved is not None and (
            capture.callable_path == resolved
            or capture.callable_path.startswith(f"{resolved}.")
        ):
            raise ValueError(
                f"{test_path}:{line}: unsupported definition-default patch "
                f"syntax: imported callable binding {name} is rebound"
            )
    root: ast.AST = function_scope if function_scope is not None else tree
    for node in ast.walk(root):
        if not isinstance(node, (ast.Name, ast.Attribute)):
            continue
        dotted = _dotted_name(node)
        reference_bindings, ambiguous = _reference_bindings(
            node,
            outer_scope=function_scope,
            outer_bindings=bindings,
            parents=parents,
        )
        if dotted is None or (
            _resolve_dotted(dotted, reference_bindings) != capture.callable_path
        ):
            continue
        if ambiguous:
            names = ", ".join(sorted(ambiguous))
            raise ValueError(
                f"{test_path}:{node.lineno}: unsupported definition-default "
                f"patch syntax: conflicting imports for {names}"
            )
        parent = parents.get(node)
        if not isinstance(parent, ast.Call) or parent.func is not node:
            raise ValueError(
                f"{test_path}:{node.lineno}: unsupported definition-default "
                f"patch syntax: {capture.callable_path} must be called directly"
            )
        if _nearest_function(parent, parents) is not function_scope:
            raise ValueError(
                f"{test_path}:{parent.lineno}: unsupported definition-default "
                "patch syntax: captured call cannot cross a nested function boundary"
            )


def _check_direct_usage(
    *,
    test_path: str,
    region: _PatchRegion,
    capture: _Capture,
    bindings: Mapping[str, str],
    parents: Mapping[ast.AST, ast.AST],
) -> list[DefaultPatchFinding]:
    findings: list[DefaultPatchFinding] = []
    for node in _walk_statements(region.body):
        if not isinstance(node, (ast.Name, ast.Attribute)):
            continue
        dotted = _dotted_name(node)
        reference_bindings, ambiguous = _reference_bindings(
            node,
            outer_scope=region.function_scope,
            outer_bindings=bindings,
            parents=parents,
        )
        if dotted is None or (
            _resolve_dotted(dotted, reference_bindings) != capture.callable_path
        ):
            continue
        if ambiguous:
            names = ", ".join(sorted(ambiguous))
            raise ValueError(
                f"{test_path}:{node.lineno}: unsupported definition-default "
                f"patch syntax: conflicting imports for {names}"
            )
        parent = parents.get(node)
        if not isinstance(parent, ast.Call) or parent.func is not node:
            raise ValueError(
                f"{test_path}:{node.lineno}: unsupported definition-default "
                f"patch syntax: {capture.callable_path} must be called directly"
            )
        if _nearest_function(parent, parents) is not region.function_scope:
            raise ValueError(
                f"{test_path}:{parent.lineno}: unsupported definition-default "
                "patch syntax: captured call cannot cross a nested function boundary"
            )
        if any(keyword.arg is None for keyword in parent.keywords):
            raise ValueError(
                f"{test_path}:{parent.lineno}: unsupported definition-default "
                f"patch syntax: inject {capture.injectable_keyword} as an explicit keyword"
            )
        if any(isinstance(argument, ast.Starred) for argument in parent.args):
            raise ValueError(
                f"{test_path}:{parent.lineno}: unsupported definition-default "
                f"patch syntax: inject {capture.injectable_keyword} as an explicit keyword"
            )
        injected = any(
            keyword.arg == capture.injectable_keyword
            for keyword in parent.keywords
        )
        if capture.positional_only:
            raise ValueError(
                f"{test_path}:{parent.lineno}: unsupported definition-default "
                "patch syntax: positional-only captured defaults are outside the "
                "canonical injection grammar"
            )
        if (
            capture.positional_index is not None
            and len(parent.args) > capture.positional_index
        ):
            raise ValueError(
                f"{test_path}:{parent.lineno}: unsupported definition-default "
                f"patch syntax: inject {capture.injectable_keyword} as an explicit keyword"
            )
        if not injected:
            findings.append(
                DefaultPatchFinding(
                    test_path=test_path,
                    line=parent.lineno,
                    callable_path=capture.callable_path,
                    patched_target=region.target,
                    injectable_keyword=capture.injectable_keyword,
                ),
            )
    return findings


def find_ineffective_default_patches(
    production_sources: Mapping[str, str],
    test_sources: Mapping[str, str],
) -> tuple[DefaultPatchFinding, ...]:
    """Audit direct literal patches, direct imports, and explicit keyword DI.

    This is intentionally a grammar, not an evaluator. Relevant manual
    patchers, callable aliases, nested execution, dynamic targets, ``**kwargs``,
    and positional-only injection fail with a source-local diagnostic.
    """
    captures = _captured_defaults(production_sources)
    by_target: dict[str, list[_Capture]] = {}
    for capture in captures:
        for target in capture.targets:
            by_target.setdefault(target, []).append(capture)

    findings: list[DefaultPatchFinding] = []
    for test_path, source in test_sources.items():
        tree = ast.parse(source, filename=test_path)
        parents = _parents(tree)
        patch_spellings = _canonical_patch_spellings(tree)
        scope_data: dict[
            ast.FunctionDef | ast.AsyncFunctionDef | None,
            tuple[dict[str, str], set[str], dict[str, int]],
        ] = {}

        def data_for_scope(
            scope: ast.FunctionDef | ast.AsyncFunctionDef | None,
        ) -> tuple[dict[str, str], set[str], dict[str, int]]:
            if scope not in scope_data:
                imports = _scope_imports(tree, scope, parents)
                bindings, ambiguous = _bind_imports(imports)
                scope_data[scope] = (
                    bindings,
                    ambiguous,
                    _scope_rebound_names(tree, scope, parents),
                )
            return scope_data[scope]

        for call in (node for node in ast.walk(tree) if isinstance(node, ast.Call)):
            if _dotted_name(call.func) not in patch_spellings:
                continue
            parent = parents.get(call)
            binding_scope = (
                _nearest_function(parent, parents)
                if isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef))
                and call in parent.decorator_list
                else _nearest_function(call, parents)
            )
            patch_bindings, patch_ambiguous, patch_rebound = data_for_scope(
                binding_scope,
            )
            target = _patch_target(
                call,
                test_path=test_path,
                bindings=patch_bindings,
            )
            if target is None:
                continue
            target_captures = by_target.get(target, ())
            if not target_captures:
                continue
            raw_patch = _dotted_name(call.func)
            assert raw_patch is not None
            patch_root = raw_patch.partition(".")[0]
            if patch_root in patch_rebound:
                raise ValueError(
                    f"{test_path}:{patch_rebound[patch_root]}: unsupported "
                    f"definition-default patch syntax: patch binding {patch_root} "
                    "is rebound"
                )
            if patch_ambiguous:
                names = ", ".join(sorted(patch_ambiguous))
                raise ValueError(
                    f"{test_path}:{call.lineno}: unsupported definition-default "
                    f"patch syntax: conflicting imports for {names}"
                )
            region = _patch_region(call, target=target, parents=parents)
            usage_scope = (
                region.function_scope if region is not None else binding_scope
            )
            bindings, ambiguous, rebound_names = data_for_scope(usage_scope)
            if ambiguous:
                names = ", ".join(sorted(ambiguous))
                raise ValueError(
                    f"{test_path}:{call.lineno}: unsupported definition-default "
                    f"patch syntax: conflicting imports for {names}"
                )
            for capture in target_captures:
                _validate_scope_capture_syntax(
                    test_path=test_path,
                    tree=tree,
                    function_scope=usage_scope,
                    capture=capture,
                    bindings=bindings,
                    rebound_names=rebound_names,
                    parents=parents,
                )
            if region is None:
                if any(
                    _scope_uses_capture(
                        tree,
                        function_scope=usage_scope,
                        capture=capture,
                        bindings=bindings,
                        parents=parents,
                    )
                    for capture in target_captures
                ):
                    raise ValueError(
                        f"{test_path}:{call.lineno}: unsupported definition-default "
                        "patch syntax: captured-default patches must be direct "
                        "with-items or test decorators"
                    )
                continue
            for capture in target_captures:
                findings.extend(
                    _check_direct_usage(
                        test_path=test_path,
                        region=region,
                        capture=capture,
                        bindings=bindings,
                        parents=parents,
                    ),
                )
    return tuple(sorted(set(findings)))


def _read_repository_sources(
    repo_root: Path,
) -> tuple[dict[str, str], dict[str, str]]:
    production_sources: dict[str, str] = {}
    for root_name in _PRODUCTION_ROOTS:
        root = repo_root / root_name
        paths = root.rglob("*.py") if root.is_dir() else (root,)
        for path in paths:
            if path.is_file():
                relative = path.relative_to(repo_root).as_posix()
                production_sources[relative] = path.read_text(encoding="utf-8")
    test_sources = {
        path.relative_to(repo_root).as_posix(): path.read_text(encoding="utf-8")
        for path in (repo_root / "tests").rglob("*.py")
        if path.is_file()
    }
    return production_sources, test_sources


def repository_default_patch_findings(
    repo_root: Path,
) -> tuple[DefaultPatchFinding, ...]:
    """Run the grammar across every production and test Python source."""
    production_sources, test_sources = _read_repository_sources(repo_root)
    return find_ineffective_default_patches(production_sources, test_sources)
