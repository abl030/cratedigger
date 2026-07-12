"""Source-local audit for ineffective patches of captured defaults."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


@dataclass(frozen=True, order=True)
class DefaultPatchFinding:
    """One call made under an ineffective patch of its captured default."""

    test_path: str
    line: int
    callable_path: str
    patched_target: str
    injectable_keyword: str


@dataclass(frozen=True)
class _CapturedDefault:
    callable_path: str
    injectable_keyword: str
    targets: frozenset[str]


_PRODUCTION_ROOTS = (
    "lib",
    "web",
    "harness",
    "scripts",
    "cratedigger.py",
    "album_source.py",
)


def _module_path(relative_path: str) -> str:
    path = Path(relative_path)
    parts = list(path.with_suffix("").parts)
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


def _resolve_dotted(name: str, aliases: Mapping[str, str]) -> str:
    root, separator, remainder = name.partition(".")
    resolved_root = aliases.get(root, root)
    return resolved_root + (separator + remainder if separator else "")


def _import_aliases(tree: ast.Module) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for statement in tree.body:
        if isinstance(statement, ast.Import):
            for alias in statement.names:
                bound = alias.asname or alias.name.split(".")[0]
                aliases[bound] = alias.name if alias.asname else bound
        elif isinstance(statement, ast.ImportFrom) and statement.level == 0:
            module = statement.module or ""
            for alias in statement.names:
                if alias.name == "*":
                    continue
                aliases[alias.asname or alias.name] = f"{module}.{alias.name}"
    return aliases


def _default_targets(
    default: ast.expr,
    *,
    module: str,
    aliases: Mapping[str, str],
) -> frozenset[str]:
    dotted = _dotted_name(default)
    if dotted is None:
        return frozenset()
    resolved = _resolve_dotted(dotted, aliases)
    targets = {f"{module}.{dotted}", resolved}
    return frozenset(target for target in targets if target)


def _definition_captures(
    definition: ast.FunctionDef | ast.AsyncFunctionDef,
    *,
    callable_path: str,
    module: str,
    aliases: Mapping[str, str],
) -> tuple[_CapturedDefault, ...]:
    captures: list[_CapturedDefault] = []
    positional = [*definition.args.posonlyargs, *definition.args.args]
    defaulted_positional = positional[-len(definition.args.defaults):]
    for argument, default in zip(defaulted_positional, definition.args.defaults):
        targets = _default_targets(default, module=module, aliases=aliases)
        if targets:
            captures.append(
                _CapturedDefault(callable_path, argument.arg, targets),
            )
    for argument, default in zip(
        definition.args.kwonlyargs,
        definition.args.kw_defaults,
    ):
        if default is None:
            continue
        targets = _default_targets(default, module=module, aliases=aliases)
        if targets:
            captures.append(
                _CapturedDefault(callable_path, argument.arg, targets),
            )
    return tuple(captures)


def _captured_defaults(
    production_sources: Mapping[str, str],
) -> dict[str, tuple[_CapturedDefault, ...]]:
    captures_by_callable: dict[str, list[_CapturedDefault]] = {}
    for relative_path, source in production_sources.items():
        module = _module_path(relative_path)
        if not module:
            continue
        tree = ast.parse(source, filename=relative_path)
        aliases = _import_aliases(tree)
        for statement in tree.body:
            if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)):
                definitions = ((statement, f"{module}.{statement.name}"),)
            elif isinstance(statement, ast.ClassDef):
                definitions = tuple(
                    (member, f"{module}.{statement.name}.{member.name}")
                    for member in statement.body
                    if isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef))
                )
            else:
                continue
            for definition, callable_path in definitions:
                for capture in _definition_captures(
                    definition,
                    callable_path=callable_path,
                    module=module,
                    aliases=aliases,
                ):
                    captures_by_callable.setdefault(callable_path, []).append(capture)
    return {
        callable_path: tuple(captures)
        for callable_path, captures in captures_by_callable.items()
    }


def _assigned_names(node: ast.expr) -> set[str]:
    if isinstance(node, ast.Name):
        return {node.id}
    if isinstance(node, (ast.Tuple, ast.List)):
        return {
            name
            for element in node.elts
            for name in _assigned_names(element)
        }
    return set()


class _TestPatchVisitor(ast.NodeVisitor):
    def __init__(
        self,
        *,
        test_path: str,
        captures: Mapping[str, tuple[_CapturedDefault, ...]],
    ) -> None:
        self.test_path = test_path
        self.captures = captures
        self.aliases: dict[str, str] = {}
        self.active_patch_targets: tuple[str, ...] = ()
        self.findings: list[DefaultPatchFinding] = []

    def _resolved_expression(self, node: ast.expr) -> str | None:
        dotted = _dotted_name(node)
        if dotted is None:
            return None
        return _resolve_dotted(dotted, self.aliases)

    def _patch_target(self, node: ast.expr) -> str | None:
        if not isinstance(node, ast.Call):
            return None
        function = self._resolved_expression(node.func)
        if function is None:
            return None
        if function.endswith(".patch") or function == "patch":
            if node.args and isinstance(node.args[0], ast.Constant):
                target = node.args[0].value
                return target if isinstance(target, str) else None
        if function.endswith(".patch.object") or function == "patch.object":
            if len(node.args) < 2:
                return None
            owner = self._resolved_expression(node.args[0])
            attribute = node.args[1]
            if (
                owner is not None
                and isinstance(attribute, ast.Constant)
                and isinstance(attribute.value, str)
            ):
                return f"{owner}.{attribute.value}"
        return None

    def _keyword_is_injected(self, call: ast.Call, keyword: str) -> bool:
        for argument in call.keywords:
            if argument.arg == keyword:
                return True
            if argument.arg is None and isinstance(argument.value, ast.Dict):
                for key in argument.value.keys:
                    if isinstance(key, ast.Constant) and key.value == keyword:
                        return True
        return False

    def _visit_function(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
    ) -> None:
        decorator_targets = tuple(
            target
            for decorator in node.decorator_list
            if (target := self._patch_target(decorator)) is not None
        )
        prior_aliases = self.aliases
        prior_targets = self.active_patch_targets
        self.aliases = dict(prior_aliases)
        arguments = [
            *node.args.posonlyargs,
            *node.args.args,
            *node.args.kwonlyargs,
        ]
        if node.args.vararg is not None:
            arguments.append(node.args.vararg)
        if node.args.kwarg is not None:
            arguments.append(node.args.kwarg)
        for argument in arguments:
            self.aliases.pop(argument.arg, None)
        self.active_patch_targets = decorator_targets
        for statement in node.body:
            self.visit(statement)
        self.aliases = prior_aliases
        self.active_patch_targets = prior_targets

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._visit_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._visit_function(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        for statement in node.body:
            if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)):
                self.visit(statement)

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            bound = alias.asname or alias.name.split(".")[0]
            self.aliases[bound] = alias.name if alias.asname else bound

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.level != 0:
            return
        module = node.module or ""
        for alias in node.names:
            if alias.name == "*":
                continue
            self.aliases[alias.asname or alias.name] = f"{module}.{alias.name}"

    def visit_With(self, node: ast.With) -> None:
        targets = tuple(
            target
            for item in node.items
            if (target := self._patch_target(item.context_expr)) is not None
        )
        prior_targets = self.active_patch_targets
        self.active_patch_targets += targets
        for statement in node.body:
            self.visit(statement)
        self.active_patch_targets = prior_targets

    def visit_AsyncWith(self, node: ast.AsyncWith) -> None:
        targets = tuple(
            target
            for item in node.items
            if (target := self._patch_target(item.context_expr)) is not None
        )
        prior_targets = self.active_patch_targets
        self.active_patch_targets += targets
        for statement in node.body:
            self.visit(statement)
        self.active_patch_targets = prior_targets

    def visit_Assign(self, node: ast.Assign) -> None:
        self.visit(node.value)
        for target in node.targets:
            for name in _assigned_names(target):
                self.aliases.pop(name, None)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if node.value is not None:
            self.visit(node.value)
        for name in _assigned_names(node.target):
            self.aliases.pop(name, None)

    def visit_Call(self, node: ast.Call) -> None:
        callable_path = self._resolved_expression(node.func)
        if callable_path is not None and self.active_patch_targets:
            for capture in self.captures.get(callable_path, ()):
                matching_targets = sorted(
                    capture.targets.intersection(self.active_patch_targets),
                )
                if (
                    matching_targets
                    and not self._keyword_is_injected(
                        node,
                        capture.injectable_keyword,
                    )
                ):
                    self.findings.append(
                        DefaultPatchFinding(
                            test_path=self.test_path,
                            line=node.lineno,
                            callable_path=callable_path,
                            patched_target=matching_targets[0],
                            injectable_keyword=capture.injectable_keyword,
                        ),
                    )
        self.generic_visit(node)


def find_ineffective_default_patches(
    production_sources: Mapping[str, str],
    test_sources: Mapping[str, str],
) -> tuple[DefaultPatchFinding, ...]:
    """Return calls whose active patch cannot replace a captured default."""
    captures = _captured_defaults(production_sources)
    findings: list[DefaultPatchFinding] = []
    for test_path, source in test_sources.items():
        tree = ast.parse(source, filename=test_path)
        visitor = _TestPatchVisitor(test_path=test_path, captures=captures)
        visitor.visit(tree)
        findings.extend(visitor.findings)
    return tuple(sorted(set(findings)))


def assert_default_patch_invariant(
    findings: tuple[DefaultPatchFinding, ...],
    *,
    expected_valid: bool,
) -> None:
    """Assert that the audit verdict matches a synthetic world's oracle."""
    actual_valid = not findings
    if actual_valid != expected_valid:
        raise AssertionError(
            f"default-patch verdict mismatch: expected_valid={expected_valid} "
            f"actual_valid={actual_valid} findings={findings!r}"
        )


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
    """Run the audit across every production and test Python source file."""
    production_sources, test_sources = _read_repository_sources(repo_root)
    return find_ineffective_default_patches(production_sources, test_sources)
