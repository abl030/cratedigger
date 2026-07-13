"""Fail-closed audit for explicit bare-MP3 target modes.

Only canonical imports and direct ``TargetQualityContract.from_format`` calls
are supported. Star imports, rebindings, or target class/module/factory values
escaping that shape are violations; arbitrary Python data flow is out of scope.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass


_TARGET_MODULES = frozenset({"lib.quality", "lib.quality.evidence_types"})


@dataclass(frozen=True)
class TargetContractCallViolation:
    relative_path: str
    line: int


def _import_from_module(path: str, node: ast.ImportFrom) -> str | None:
    if node.level == 0:
        return node.module
    package = path.replace("\\", "/").split("/")[:-1]
    retained = len(package) - node.level + 1
    if retained < 0:
        return None
    parts = package[:retained]
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


def _arguments(args: ast.arguments) -> tuple[ast.arg, ...]:
    values = (*args.posonlyargs, *args.args, *args.kwonlyargs)
    if args.vararg:
        values += (args.vararg,)
    if args.kwarg:
        values += (args.kwarg,)
    return values


@dataclass(frozen=True)
class _Bindings:
    bound: frozenset[str]
    classes: frozenset[str]
    modules: frozenset[str]
    package_roots: frozenset[str]
    violations: tuple[int, ...]


@dataclass(frozen=True)
class _Frame:
    bindings: _Bindings
    parent: _Frame | None
    kind: str


class _Collector(ast.NodeVisitor):
    """Collect one lexical scope without descending into child scopes."""

    def __init__(self, path: str, arguments: tuple[ast.arg, ...]) -> None:
        self.path = path
        self.other: set[str] = set()
        self.other_lines: dict[str, set[int]] = {}
        self.classes: set[str] = set()
        self.modules: set[str] = set()
        self.module_roots: set[str] = set()
        self.package_roots: set[str] = set()
        self.bad_lines: set[int] = set()
        for argument in arguments:
            self._other(argument.arg, argument)

    def _other(self, name: str, node: ast.AST) -> None:
        self.other.add(name)
        self.other_lines.setdefault(name, set()).add(
            getattr(node, "lineno", 1)
        )

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        module = _import_from_module(self.path, node)
        for alias in node.names:
            if alias.name == "*":
                if module in _TARGET_MODULES:
                    self.bad_lines.add(node.lineno)
                continue
            name = alias.asname or alias.name
            if module in _TARGET_MODULES and alias.name == "TargetQualityContract":
                self.classes.add(name)
            elif (
                (module == "lib" and alias.name == "quality")
                or (module == "lib.quality" and alias.name == "evidence_types")
            ):
                self.modules.add(name)
                self.module_roots.add(name)
            else:
                self._other(name, node)

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            root = alias.asname or alias.name.split(".", 1)[0]
            if alias.name in _TARGET_MODULES:
                self.modules.add(alias.asname or alias.name)
                self.module_roots.add(root)
            elif alias.asname is None and (
                alias.name == "lib" or alias.name.startswith("lib.")
            ):
                self.package_roots.add("lib")
            else:
                self._other(root, node)

    def visit_Name(self, node: ast.Name) -> None:
        if isinstance(node.ctx, ast.Store):
            self._other(node.id, node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._other(node.name, node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._other(node.name, node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self._other(node.name, node)

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
            self._other(node.name, node)
        self.generic_visit(node)

    def visit_MatchAs(self, node: ast.MatchAs) -> None:
        if node.name:
            self._other(node.name, node)
        self.generic_visit(node)

    def visit_MatchStar(self, node: ast.MatchStar) -> None:
        if node.name:
            self._other(node.name, node)

    def visit_MatchMapping(self, node: ast.MatchMapping) -> None:
        if node.rest:
            self._other(node.rest, node)
        self.generic_visit(node)


def _bindings(
    body: list[ast.stmt],
    path: str,
    arguments: tuple[ast.arg, ...] = (),
) -> _Bindings:
    collector = _Collector(path, arguments)
    for statement in body:
        collector.visit(statement)
    target_roots = collector.classes | {
        module.split(".", 1)[0] for module in collector.modules
    }
    rebound_lines = {
        line
        for name in target_roots & collector.other
        for line in collector.other_lines[name]
    }
    package_roots = collector.package_roots - collector.other
    return _Bindings(
        bound=frozenset(
            collector.other
            | collector.classes
            | collector.module_roots
            | package_roots
        ),
        classes=frozenset(collector.classes),
        modules=frozenset(collector.modules),
        package_roots=frozenset(package_roots),
        violations=tuple(sorted(collector.bad_lines | rebound_lines)),
    )


def _target_class_name(name: str, frame: _Frame) -> bool:
    current: _Frame | None = frame
    while current:
        if name in current.bindings.bound:
            return name in current.bindings.classes
        current = current.parent
    return False


def _target_module_name(name: str, frame: _Frame) -> bool:
    root = name.split(".", 1)[0]
    current: _Frame | None = frame
    while current:
        if root in current.bindings.bound:
            if name in current.bindings.modules:
                return True
            if root in current.bindings.package_roots:
                current = current.parent
                continue
            return False
        current = current.parent
    return False


def _target_module_root(name: str, frame: _Frame) -> bool:
    current: _Frame | None = frame
    while current:
        if name in current.bindings.bound:
            if any(
                module.split(".", 1)[0] == name
                for module in current.bindings.modules
            ):
                return True
            if name in current.bindings.package_roots:
                current = current.parent
                continue
            return False
        current = current.parent
    return False


def _target_class(node: ast.expr, frame: _Frame) -> bool:
    if isinstance(node, ast.Name):
        return _target_class_name(node.id, frame)
    if not isinstance(node, ast.Attribute) or node.attr != "TargetQualityContract":
        return False
    module = _dotted_name(node.value)
    return bool(module and _target_module_name(module, frame))


def _target_factory(node: ast.expr, frame: _Frame) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and node.attr == "from_format"
        and _target_class(node.value, frame)
    )


class _Audit(ast.NodeVisitor):
    def __init__(self, path: str, tree: ast.Module) -> None:
        self.path = path
        self.frame = _Frame(_bindings(tree.body, path), None, "module")
        self.violations: list[TargetContractCallViolation] = []
        self._report_scope()

    def _violate(self, node: ast.AST) -> None:
        self._violate_line(getattr(node, "lineno", 1))

    def _violate_line(self, line: int) -> None:
        self.violations.append(TargetContractCallViolation(self.path, line))

    def _report_scope(self) -> None:
        for line in self.frame.bindings.violations:
            self._violate_line(line)

    def _enter(
        self,
        body: list[ast.stmt],
        kind: str,
        arguments: tuple[ast.arg, ...] = (),
    ) -> _Frame:
        parent = self.frame
        if kind == "function":
            while parent.kind == "class" and parent.parent:
                parent = parent.parent
        previous = self.frame
        self.frame = _Frame(_bindings(body, self.path, arguments), parent, kind)
        self._report_scope()
        return previous

    def visit_Import(self, node: ast.Import) -> None:
        return

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        return

    def _visit_function(
        self, node: ast.FunctionDef | ast.AsyncFunctionDef
    ) -> None:
        for decorator in node.decorator_list:
            self.visit(decorator)
        for default in (*node.args.defaults, *node.args.kw_defaults):
            if default:
                self.visit(default)
        previous = self._enter(node.body, "function", _arguments(node.args))
        for statement in node.body:
            self.visit(statement)
        self.frame = previous

    visit_FunctionDef = _visit_function
    visit_AsyncFunctionDef = _visit_function

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        for expression in (*node.decorator_list, *node.bases):
            self.visit(expression)
        for keyword in node.keywords:
            self.visit(keyword.value)
        previous = self._enter(node.body, "class")
        for statement in node.body:
            self.visit(statement)
        self.frame = previous

    def visit_Lambda(self, node: ast.Lambda) -> None:
        for default in (*node.args.defaults, *node.args.kw_defaults):
            if default:
                self.visit(default)
        previous = self._enter([], "function", _arguments(node.args))
        self.visit(node.body)
        self.frame = previous

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        # An annotation may name the type; only its runtime value is audited.
        if node.value:
            self.visit(node.value)

    def visit_Call(self, node: ast.Call) -> None:
        if _target_factory(node.func, self.frame):
            has_mode = any(
                keyword.arg == "projected_is_cbr" for keyword in node.keywords
            )
            format_arg = node.args[0] if node.args else next(
                (
                    keyword.value
                    for keyword in node.keywords
                    if keyword.arg == "format_hint"
                ),
                None,
            )
            explicit_label = (
                isinstance(format_arg, ast.Constant)
                and isinstance(format_arg.value, str)
                and format_arg.value.strip().lower() != "mp3"
            )
            if not has_mode and not explicit_label:
                self._violate(node)
            for argument in node.args:
                self.visit(argument)
            for keyword in node.keywords:
                self.visit(keyword.value)
            return
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        dotted = _dotted_name(node)
        if (
            _target_factory(node, self.frame)
            or _target_class(node, self.frame)
            or bool(dotted and _target_module_name(dotted, self.frame))
        ):
            self._violate(node)
            return
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:
        if isinstance(node.ctx, ast.Load) and (
            _target_class_name(node.id, self.frame)
            or _target_module_root(node.id, self.frame)
        ):
            self._violate(node)


def target_contract_call_violations(
    relative_path: str,
    source: str,
) -> tuple[TargetContractCallViolation, ...]:
    tree = ast.parse(source, filename=relative_path)
    visitor = _Audit(relative_path, tree)
    visitor.visit(tree)
    return tuple(visitor.violations)
