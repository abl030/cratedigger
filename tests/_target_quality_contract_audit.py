"""Structural audit for explicit bare-MP3 target bitrate modes."""

from __future__ import annotations

import ast
from dataclasses import dataclass


_TARGET_CONTRACT_MODULES = frozenset(
    {"lib.quality", "lib.quality.evidence_types"}
)


@dataclass(frozen=True)
class TargetContractCallViolation:
    """One target-contract call whose format can be bare MP3 without a mode."""

    relative_path: str
    line: int


def _resolved_import_from_module(
    relative_path: str,
    node: ast.ImportFrom,
) -> str | None:
    if node.level == 0:
        return node.module
    package_parts = relative_path.replace("\\", "/").split("/")[:-1]
    retained_count = len(package_parts) - node.level + 1
    if retained_count < 0:
        return None
    parts = package_parts[:retained_count]
    if node.module:
        parts.extend(node.module.split("."))
    return ".".join(parts)


@dataclass(frozen=True)
class _ScopeBindings:
    bound_names: frozenset[str]
    class_names: frozenset[str]
    module_names: frozenset[str]


@dataclass(frozen=True)
class _ScopeFrame:
    bindings: _ScopeBindings
    parent: _ScopeFrame | None
    kind: str


class _BindingCollector(ast.NodeVisitor):
    """Collect bindings owned by one lexical scope, never its child scopes."""

    def __init__(self, relative_path: str, argument_names: set[str]) -> None:
        self.relative_path = relative_path
        self.other_names = set(argument_names)
        self.class_names: set[str] = set()
        self.module_names: set[str] = set()
        self.module_roots: set[str] = set()

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        imported_module = _resolved_import_from_module(self.relative_path, node)
        for alias in node.names:
            if alias.name == "*":
                continue
            bound_name = alias.asname or alias.name
            if (
                imported_module in _TARGET_CONTRACT_MODULES
                and alias.name == "TargetQualityContract"
            ):
                self.class_names.add(bound_name)
            else:
                self.other_names.add(bound_name)

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            bound_name = alias.asname or alias.name.split(".", 1)[0]
            if alias.name in _TARGET_CONTRACT_MODULES:
                access_name = alias.asname or alias.name
                self.module_names.add(access_name)
                self.module_roots.add(bound_name)
            else:
                self.other_names.add(bound_name)

    def visit_Name(self, node: ast.Name) -> None:
        if isinstance(node.ctx, ast.Store):
            self.other_names.add(node.id)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.other_names.add(node.name)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.other_names.add(node.name)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.other_names.add(node.name)

    def visit_Lambda(self, node: ast.Lambda) -> None:
        return

    def visit_ListComp(self, node: ast.ListComp) -> None:
        return

    def visit_SetComp(self, node: ast.SetComp) -> None:
        return

    def visit_DictComp(self, node: ast.DictComp) -> None:
        return

    def visit_GeneratorExp(self, node: ast.GeneratorExp) -> None:
        return

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
        if node.name:
            self.other_names.add(node.name)
        self.generic_visit(node)

    def visit_MatchAs(self, node: ast.MatchAs) -> None:
        if node.name:
            self.other_names.add(node.name)
        self.generic_visit(node)

    def visit_MatchStar(self, node: ast.MatchStar) -> None:
        if node.name:
            self.other_names.add(node.name)

    def visit_MatchMapping(self, node: ast.MatchMapping) -> None:
        if node.rest:
            self.other_names.add(node.rest)
        self.generic_visit(node)


def _target_contract_bindings(
    body: list[ast.stmt],
    relative_path: str,
    *,
    argument_names: set[str] | None = None,
) -> _ScopeBindings:
    collector = _BindingCollector(relative_path, argument_names or set())
    for node in body:
        collector.visit(node)
    class_names = collector.class_names - collector.other_names
    module_names = {
        name
        for name in collector.module_names
        if name.split(".", 1)[0] not in collector.other_names
    }
    bound_names = (
        collector.other_names
        | collector.class_names
        | collector.module_roots
    )
    return _ScopeBindings(
        bound_names=frozenset(bound_names),
        class_names=frozenset(class_names),
        module_names=frozenset(module_names),
    )


def _dotted_name(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = _dotted_name(node.value)
        return f"{prefix}.{node.attr}" if prefix else None
    return None


def _name_is_target_class(name: str, frame: _ScopeFrame) -> bool:
    current: _ScopeFrame | None = frame
    while current is not None:
        if name in current.bindings.bound_names:
            return name in current.bindings.class_names
        current = current.parent
    return False


def _name_is_target_module(name: str, frame: _ScopeFrame) -> bool:
    root_name = name.split(".", 1)[0]
    current: _ScopeFrame | None = frame
    while current is not None:
        if root_name in current.bindings.bound_names:
            return name in current.bindings.module_names
        current = current.parent
    return False


def _is_target_contract_factory(call: ast.Call, frame: _ScopeFrame) -> bool:
    func = call.func
    if not isinstance(func, ast.Attribute) or func.attr != "from_format":
        return False
    owner = func.value
    if isinstance(owner, ast.Name):
        return _name_is_target_class(owner.id, frame)
    if not isinstance(owner, ast.Attribute) or owner.attr != "TargetQualityContract":
        return False
    module_name = _dotted_name(owner.value)
    return bool(module_name and _name_is_target_module(module_name, frame))


def _format_argument(call: ast.Call) -> ast.expr | None:
    if call.args:
        return call.args[0]
    return next(
        (
            keyword.value
            for keyword in call.keywords
            if keyword.arg == "format_hint"
        ),
        None,
    )


def _argument_names(arguments: ast.arguments) -> set[str]:
    names = {
        argument.arg
        for argument in (
            *arguments.posonlyargs,
            *arguments.args,
            *arguments.kwonlyargs,
        )
    }
    if arguments.vararg is not None:
        names.add(arguments.vararg.arg)
    if arguments.kwarg is not None:
        names.add(arguments.kwarg.arg)
    return names


class _TargetContractCallVisitor(ast.NodeVisitor):
    def __init__(self, relative_path: str, tree: ast.Module) -> None:
        self.relative_path = relative_path
        self.frame = _ScopeFrame(
            _target_contract_bindings(tree.body, relative_path),
            parent=None,
            kind="module",
        )
        self.violations: list[TargetContractCallViolation] = []

    def _enter_scope(
        self,
        body: list[ast.stmt],
        *,
        kind: str,
        argument_names: set[str] | None = None,
    ) -> _ScopeFrame:
        parent = self.frame
        if kind == "function":
            while parent.kind == "class" and parent.parent is not None:
                parent = parent.parent
        previous = self.frame
        self.frame = _ScopeFrame(
            _target_contract_bindings(
                body,
                self.relative_path,
                argument_names=argument_names,
            ),
            parent=parent,
            kind=kind,
        )
        return previous

    def _visit_function_header(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
    ) -> None:
        for decorator in node.decorator_list:
            self.visit(decorator)
        for default in (*node.args.defaults, *node.args.kw_defaults):
            if default is not None:
                self.visit(default)
        for argument in (
            *node.args.posonlyargs,
            *node.args.args,
            *node.args.kwonlyargs,
        ):
            if argument.annotation is not None:
                self.visit(argument.annotation)
        if node.args.vararg and node.args.vararg.annotation is not None:
            self.visit(node.args.vararg.annotation)
        if node.args.kwarg and node.args.kwarg.annotation is not None:
            self.visit(node.args.kwarg.annotation)
        if node.returns is not None:
            self.visit(node.returns)

    def _visit_function(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
    ) -> None:
        self._visit_function_header(node)
        previous = self._enter_scope(
            node.body,
            kind="function",
            argument_names=_argument_names(node.args),
        )
        for statement in node.body:
            self.visit(statement)
        self.frame = previous

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._visit_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._visit_function(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        for decorator in node.decorator_list:
            self.visit(decorator)
        for base in node.bases:
            self.visit(base)
        for keyword in node.keywords:
            self.visit(keyword.value)
        previous = self._enter_scope(node.body, kind="class")
        for statement in node.body:
            self.visit(statement)
        self.frame = previous

    def visit_Lambda(self, node: ast.Lambda) -> None:
        for default in (*node.args.defaults, *node.args.kw_defaults):
            if default is not None:
                self.visit(default)
        previous = self._enter_scope(
            [],
            kind="function",
            argument_names=_argument_names(node.args),
        )
        self.visit(node.body)
        self.frame = previous

    def visit_Call(self, node: ast.Call) -> None:
        if _is_target_contract_factory(node, self.frame):
            has_mode = any(
                keyword.arg == "projected_is_cbr" for keyword in node.keywords
            )
            format_argument = _format_argument(node)
            literal_is_self_describing = (
                isinstance(format_argument, ast.Constant)
                and isinstance(format_argument.value, str)
                and format_argument.value.strip().lower() != "mp3"
            )
            if not has_mode and not literal_is_self_describing:
                self.violations.append(
                    TargetContractCallViolation(self.relative_path, node.lineno)
                )
        self.generic_visit(node)


def target_contract_call_violations(
    relative_path: str,
    source: str,
) -> tuple[TargetContractCallViolation, ...]:
    """Find calls that could receive bare MP3 without naming its mode.

    Literal non-MP3 labels are statically self-describing. Dynamic labels can
    carry bare MP3, so they must explicitly supply ``projected_is_cbr`` even
    when ``None`` is the honest fail-closed fact at that boundary.
    """

    tree = ast.parse(source, filename=relative_path)
    visitor = _TargetContractCallVisitor(relative_path, tree)
    visitor.visit(tree)
    return tuple(visitor.violations)
