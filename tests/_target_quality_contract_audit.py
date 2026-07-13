"""Fail-closed structural audit for explicit bare-MP3 target modes.

Only direct calls through canonical imports are supported.  Rebinding a
proven import or letting the class/factory escape into a dynamic alias is a
violation: once the call shape becomes dynamic, this small audit can no longer
prove that a possible bare ``MP3`` label names ``projected_is_cbr``.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field


_TARGET_CONTRACT_MODULES = frozenset(
    {"lib.quality", "lib.quality.evidence_types"}
)


@dataclass(frozen=True)
class TargetContractCallViolation:
    """One target-contract use whose explicit mode cannot be proven."""

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


def _dotted_name(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = _dotted_name(node.value)
        return f"{prefix}.{node.attr}" if prefix else None
    return None


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


@dataclass
class _Scope:
    parent: _Scope | None
    kind: str
    bound_names: set[str] = field(default_factory=set)
    class_names: set[str] = field(default_factory=set)
    module_names: set[str] = field(default_factory=set)

    def resolves_class(self, name: str) -> bool:
        scope: _Scope | None = self
        while scope is not None:
            if name in scope.bound_names:
                return name in scope.class_names
            scope = scope.parent
        return False

    def resolves_module(self, name: str) -> bool:
        root = name.split(".", 1)[0]
        scope: _Scope | None = self
        while scope is not None:
            if root in scope.bound_names:
                return name in scope.module_names
            scope = scope.parent
        return False


class _AuditVisitor(ast.NodeVisitor):
    """Track only canonical imports and reject unsupported identity changes."""

    def __init__(self, relative_path: str) -> None:
        self.relative_path = relative_path
        self.scope = _Scope(parent=None, kind="module")
        self.violations: list[TargetContractCallViolation] = []

    def _violate(self, node: ast.AST) -> None:
        self.violations.append(
            TargetContractCallViolation(
                self.relative_path,
                getattr(node, "lineno", 1),
            )
        )

    def _bind_other(self, name: str, node: ast.AST) -> None:
        owns_target = name in self.scope.class_names or any(
            module.split(".", 1)[0] == name
            for module in self.scope.module_names
        )
        if owns_target:
            self._violate(node)
        self.scope.bound_names.add(name)
        self.scope.class_names.discard(name)
        self.scope.module_names = {
            module
            for module in self.scope.module_names
            if module.split(".", 1)[0] != name
        }

    def _bind_class(self, name: str, node: ast.AST) -> None:
        if name in self.scope.bound_names and name not in self.scope.class_names:
            self._violate(node)
        self.scope.bound_names.add(name)
        self.scope.class_names.add(name)

    def _bind_module(self, name: str, root: str, node: ast.AST) -> None:
        owns_root = any(
            module.split(".", 1)[0] == root
            for module in self.scope.module_names
        )
        if root in self.scope.bound_names and not owns_root:
            self._violate(node)
        self.scope.bound_names.add(root)
        self.scope.module_names.add(name)

    def _is_target_class(self, node: ast.expr) -> bool:
        if isinstance(node, ast.Name):
            return self.scope.resolves_class(node.id)
        if not isinstance(node, ast.Attribute):
            return False
        if node.attr != "TargetQualityContract":
            return False
        module_name = _dotted_name(node.value)
        return bool(module_name and self.scope.resolves_module(module_name))

    def _is_target_factory(self, node: ast.expr) -> bool:
        return (
            isinstance(node, ast.Attribute)
            and node.attr == "from_format"
            and self._is_target_class(node.value)
        )

    def _visit_body_in_scope(
        self,
        body: list[ast.stmt],
        *,
        kind: str,
        bound_names: set[str] | None = None,
    ) -> None:
        parent = self.scope
        if kind == "function":
            while parent.kind == "class" and parent.parent is not None:
                parent = parent.parent
        previous = self.scope
        self.scope = _Scope(parent=parent, kind=kind)
        for name in bound_names or set():
            self.scope.bound_names.add(name)
        for statement in body:
            self.visit(statement)
        self.scope = previous

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
                self._bind_class(bound_name, node)
            elif imported_module == "lib" and alias.name == "quality":
                self._bind_module(bound_name, bound_name, node)
            else:
                self._bind_other(bound_name, node)

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            bound_name = alias.asname or alias.name.split(".", 1)[0]
            if alias.name in _TARGET_CONTRACT_MODULES:
                access_name = alias.asname or alias.name
                self._bind_module(access_name, bound_name, node)
            elif (
                alias.asname is None
                and alias.name.startswith("lib.")
                and any(
                    module.split(".", 1)[0] == bound_name
                    for module in self.scope.module_names
                )
            ):
                # A peer import preserves the already-proven package root.
                continue
            else:
                self._bind_other(bound_name, node)

    def visit_Name(self, node: ast.Name) -> None:
        if isinstance(node.ctx, ast.Store):
            self._bind_other(node.id, node)

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
        if node.name:
            self._bind_other(node.name, node)
        self.generic_visit(node)

    def visit_MatchAs(self, node: ast.MatchAs) -> None:
        if node.name:
            self._bind_other(node.name, node)
        self.generic_visit(node)

    def visit_MatchStar(self, node: ast.MatchStar) -> None:
        if node.name:
            self._bind_other(node.name, node)

    def visit_MatchMapping(self, node: ast.MatchMapping) -> None:
        if node.rest:
            self._bind_other(node.rest, node)
        self.generic_visit(node)

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
        self._bind_other(node.name, node)
        self._visit_body_in_scope(
            node.body,
            kind="function",
            bound_names=_argument_names(node.args),
        )

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
        self._bind_other(node.name, node)
        self._visit_body_in_scope(node.body, kind="class")

    def visit_Lambda(self, node: ast.Lambda) -> None:
        for default in (*node.args.defaults, *node.args.kw_defaults):
            if default is not None:
                self.visit(default)
        previous = self.scope
        parent = previous
        while parent.kind == "class" and parent.parent is not None:
            parent = parent.parent
        self.scope = _Scope(parent=parent, kind="function")
        self.scope.bound_names.update(_argument_names(node.args))
        self.visit(node.body)
        self.scope = previous

    def visit_Call(self, node: ast.Call) -> None:
        if self._is_target_factory(node.func):
            has_mode = any(
                keyword.arg == "projected_is_cbr" for keyword in node.keywords
            )
            format_argument = (
                node.args[0]
                if node.args
                else next(
                    (
                        keyword.value
                        for keyword in node.keywords
                        if keyword.arg == "format_hint"
                    ),
                    None,
                )
            )
            literal_is_self_describing = (
                isinstance(format_argument, ast.Constant)
                and isinstance(format_argument.value, str)
                and format_argument.value.strip().lower() != "mp3"
            )
            if not has_mode and not literal_is_self_describing:
                self._violate(node)
            # The factory reference is supported only in this direct-call
            # shape.  Visiting it as an attribute would report an escape.
            for argument in node.args:
                self.visit(argument)
            for keyword in node.keywords:
                self.visit(keyword.value)
            return
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if self._is_target_factory(node):
            self._violate(node)
            return
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        if self._is_target_class(node.value):
            self._violate(node)
        else:
            self.visit(node.value)
        for target in node.targets:
            self._bind_assignment_target(target, node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        self.visit(node.annotation)
        if node.value is not None:
            if self._is_target_class(node.value):
                self._violate(node)
            else:
                self.visit(node.value)
        self._bind_assignment_target(node.target, node)

    def visit_NamedExpr(self, node: ast.NamedExpr) -> None:
        if self._is_target_class(node.value):
            self._violate(node)
        else:
            self.visit(node.value)
        self._bind_assignment_target(node.target, node)

    def _bind_assignment_target(self, target: ast.expr, node: ast.AST) -> None:
        if isinstance(target, ast.Name):
            self._bind_other(target.id, node)
            return
        if isinstance(target, (ast.Tuple, ast.List)):
            for element in target.elts:
                self._bind_assignment_target(element, node)
            return
        self.visit(target)


def target_contract_call_violations(
    relative_path: str,
    source: str,
) -> tuple[TargetContractCallViolation, ...]:
    """Find target-contract uses whose bare-MP3 mode is not explicit."""

    tree = ast.parse(source, filename=relative_path)
    visitor = _AuditVisitor(relative_path)
    visitor.visit(tree)
    return tuple(visitor.violations)
