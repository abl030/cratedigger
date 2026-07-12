"""Source-local audit for ineffective patches of captured defaults."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Mapping


_DescriptorKind = Literal["module", "instance", "class", "static"]
_CallAccess = Literal["module", "constructor", "class", "instance"]


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
    parameter_kind: Literal[
        "positional_only",
        "positional_or_keyword",
        "keyword_only",
    ]
    positional_index: int | None
    descriptor_kind: _DescriptorKind


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
    descriptor_kind: _DescriptorKind,
) -> tuple[_CapturedDefault, ...]:
    captures: list[_CapturedDefault] = []
    positional = [*definition.args.posonlyargs, *definition.args.args]
    first_default_index = len(positional) - len(definition.args.defaults)
    for default_offset, default in enumerate(definition.args.defaults):
        positional_index = first_default_index + default_offset
        argument = positional[positional_index]
        parameter_kind: Literal["positional_only", "positional_or_keyword"] = (
            "positional_only"
            if positional_index < len(definition.args.posonlyargs)
            else "positional_or_keyword"
        )
        targets = _default_targets(default, module=module, aliases=aliases)
        if targets:
            captures.append(
                _CapturedDefault(
                    callable_path,
                    argument.arg,
                    targets,
                    parameter_kind,
                    positional_index,
                    descriptor_kind,
                ),
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
                _CapturedDefault(
                    callable_path,
                    argument.arg,
                    targets,
                    "keyword_only",
                    None,
                    descriptor_kind,
                ),
            )
    return tuple(captures)


def _captured_defaults(
    production_sources: Mapping[str, str],
) -> tuple[dict[str, tuple[_CapturedDefault, ...]], frozenset[str]]:
    captures_by_callable: dict[str, list[_CapturedDefault]] = {}
    class_paths: set[str] = set()
    for relative_path, source in production_sources.items():
        module = _module_path(relative_path)
        if not module:
            continue
        tree = ast.parse(source, filename=relative_path)
        aliases = _import_aliases(tree)
        for statement in tree.body:
            definitions: tuple[
                tuple[
                    ast.FunctionDef | ast.AsyncFunctionDef,
                    str,
                    _DescriptorKind,
                ],
                ...,
            ]
            if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)):
                definitions = ((statement, f"{module}.{statement.name}", "module"),)
            elif isinstance(statement, ast.ClassDef):
                class_paths.add(f"{module}.{statement.name}")
                definitions = tuple(
                    (
                        member,
                        f"{module}.{statement.name}.{member.name}",
                        "static"
                        if any(
                            _dotted_name(decorator) == "staticmethod"
                            for decorator in member.decorator_list
                        )
                        else "class"
                        if any(
                            _dotted_name(decorator) == "classmethod"
                            for decorator in member.decorator_list
                        )
                        else "instance",
                    )
                    for member in statement.body
                    if isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef))
                )
            else:
                continue
            for definition, callable_path, descriptor_kind in definitions:
                for capture in _definition_captures(
                    definition,
                    callable_path=callable_path,
                    module=module,
                    aliases=aliases,
                    descriptor_kind=descriptor_kind,
                ):
                    captures_by_callable.setdefault(callable_path, []).append(capture)
    return (
        {
            callable_path: tuple(captures)
            for callable_path, captures in captures_by_callable.items()
        },
        frozenset(class_paths),
    )


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
        class_paths: frozenset[str],
    ) -> None:
        self.test_path = test_path
        self.captures = captures
        self.class_paths = class_paths
        self.aliases: dict[str, str] = {}
        self.instances: dict[str, str] = {}
        self.local_functions: dict[
            str,
            ast.FunctionDef | ast.AsyncFunctionDef,
        ] = {}
        self.function_depth = 0
        self.active_local_functions: set[int] = set()
        self.comprehension_walrus_bindings: list[set[str]] = []
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
        if function == "unittest.mock.patch":
            if node.args and isinstance(node.args[0], ast.Constant):
                target = node.args[0].value
                return target if isinstance(target, str) else None
        if function == "unittest.mock.patch.object":
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

    def _dependency_is_injected(
        self,
        call: ast.Call,
        capture: _CapturedDefault,
        *,
        call_access: _CallAccess,
    ) -> bool:
        for argument in call.keywords:
            if (
                capture.parameter_kind != "positional_only"
                and argument.arg == capture.injectable_keyword
            ):
                return True
            if (
                capture.parameter_kind != "positional_only"
                and argument.arg is None
                and isinstance(argument.value, ast.Dict)
            ):
                for key in argument.value.keys:
                    if (
                        isinstance(key, ast.Constant)
                        and key.value == capture.injectable_keyword
                    ):
                        return True
        if capture.parameter_kind == "keyword_only":
            return False
        explicit_prefix = 0
        for argument in call.args:
            if isinstance(argument, ast.Starred):
                break
            explicit_prefix += 1
        supplied = explicit_prefix
        if call_access == "constructor":
            supplied += 1
        elif capture.descriptor_kind == "instance" and call_access == "instance":
            supplied += 1
        elif (
            capture.descriptor_kind == "class"
            and call_access in {"class", "instance"}
        ):
            supplied += 1
        assert capture.positional_index is not None
        if supplied > capture.positional_index:
            return True
        return False

    def _constructed_class(self, node: ast.expr) -> str | None:
        if not isinstance(node, ast.Call):
            return None
        target = self._resolved_expression(node.func)
        return target if target in self.class_paths else None

    def _resolved_call_target(
        self,
        node: ast.expr,
    ) -> tuple[
        str | None,
        _CallAccess,
    ]:
        """Resolve calls without interprocedural type inference.

        The source-local instance boundary is deliberately explicit: a bound
        method is known when its receiver was assigned directly from a class
        constructor in the same lexical scope, or when the constructor call is
        the receiver expression itself. Factory returns and attribute dataflow
        remain unresolved rather than guessed.
        """
        direct = self._resolved_expression(node)
        if direct is not None and direct in self.captures:
            captures = self.captures[direct]
            access: Literal["module", "class"] = (
                "module"
                if captures[0].descriptor_kind == "module"
                else "class"
            )
            return direct, access
        if direct is not None and direct in self.class_paths:
            constructor = f"{direct}.__init__"
            return constructor, "constructor"
        if isinstance(node, ast.Attribute):
            if isinstance(node.value, ast.Name):
                instance_class = self.instances.get(node.value.id)
                if instance_class is not None:
                    return f"{instance_class}.{node.attr}", "instance"
            instance_class = self._constructed_class(node.value)
            if instance_class is not None:
                return f"{instance_class}.{node.attr}", "instance"
        return direct, "module"

    def _invalidate_binding(self, target: ast.expr) -> None:
        for name in _assigned_names(target):
            self.aliases.pop(name, None)
            self.instances.pop(name, None)
            self.local_functions.pop(name, None)

    def _visit_function(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        *,
        inherit_call_site_patches: bool = False,
    ) -> None:
        decorator_targets = tuple(
            target
            for decorator in node.decorator_list
            if (target := self._patch_target(decorator)) is not None
        )
        prior_aliases = self.aliases
        prior_instances = self.instances
        prior_local_functions = self.local_functions
        prior_comprehension_walrus_bindings = self.comprehension_walrus_bindings
        prior_targets = self.active_patch_targets
        self.aliases = dict(prior_aliases)
        self.instances = dict(prior_instances)
        self.local_functions = {}
        self.comprehension_walrus_bindings = []
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
            self.instances.pop(argument.arg, None)
        self.active_patch_targets = (
            prior_targets if inherit_call_site_patches else ()
        ) + decorator_targets
        self.function_depth += 1
        for statement in node.body:
            self.visit(statement)
        self.function_depth -= 1
        self.aliases = prior_aliases
        self.instances = prior_instances
        self.local_functions = prior_local_functions
        self.comprehension_walrus_bindings = prior_comprehension_walrus_bindings
        self.active_patch_targets = prior_targets

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        if self.function_depth:
            self.local_functions[node.name] = node
            self.aliases.pop(node.name, None)
            self.instances.pop(node.name, None)
            return
        self._visit_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        if self.function_depth:
            self.local_functions[node.name] = node
            self.aliases.pop(node.name, None)
            self.instances.pop(node.name, None)
            return
        self._visit_function(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        for statement in node.body:
            if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)):
                self.visit(statement)

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            bound = alias.asname or alias.name.split(".")[0]
            self.aliases[bound] = alias.name if alias.asname else bound
            self.instances.pop(bound, None)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.level != 0:
            return
        module = node.module or ""
        for alias in node.names:
            if alias.name == "*":
                continue
            bound = alias.asname or alias.name
            self.aliases[bound] = f"{module}.{alias.name}"
            self.instances.pop(bound, None)

    def visit_With(self, node: ast.With) -> None:
        targets: list[str] = []
        for item in node.items:
            target = self._patch_target(item.context_expr)
            if target is not None:
                targets.append(target)
            if item.optional_vars is not None:
                self._invalidate_binding(item.optional_vars)
        prior_targets = self.active_patch_targets
        self.active_patch_targets += tuple(targets)
        for statement in node.body:
            self.visit(statement)
        self.active_patch_targets = prior_targets

    def visit_AsyncWith(self, node: ast.AsyncWith) -> None:
        targets: list[str] = []
        for item in node.items:
            target = self._patch_target(item.context_expr)
            if target is not None:
                targets.append(target)
            if item.optional_vars is not None:
                self._invalidate_binding(item.optional_vars)
        prior_targets = self.active_patch_targets
        self.active_patch_targets += tuple(targets)
        for statement in node.body:
            self.visit(statement)
        self.active_patch_targets = prior_targets

    def visit_Assign(self, node: ast.Assign) -> None:
        self.visit(node.value)
        constructed_class = self._constructed_class(node.value)
        for target in node.targets:
            for name in _assigned_names(target):
                self.aliases.pop(name, None)
                self.local_functions.pop(name, None)
                if constructed_class is None:
                    self.instances.pop(name, None)
                else:
                    self.instances[name] = constructed_class

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if node.value is not None:
            self.visit(node.value)
        constructed_class = (
            self._constructed_class(node.value) if node.value is not None else None
        )
        for name in _assigned_names(node.target):
            self.aliases.pop(name, None)
            self.local_functions.pop(name, None)
            if constructed_class is None:
                self.instances.pop(name, None)
            else:
                self.instances[name] = constructed_class

    def visit_Call(self, node: ast.Call) -> None:
        callable_path, call_access = self._resolved_call_target(node.func)
        if callable_path is not None and self.active_patch_targets:
            for capture in self.captures.get(callable_path, ()):
                matching_targets = sorted(
                    capture.targets.intersection(self.active_patch_targets),
                )
                if (
                    matching_targets
                    and not self._dependency_is_injected(
                        node,
                        capture,
                        call_access=call_access,
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
        if isinstance(node.func, ast.Name):
            local_function = self.local_functions.get(node.func.id)
            function_id = id(local_function) if local_function is not None else None
            if (
                local_function is not None
                and function_id not in self.active_local_functions
            ):
                assert function_id is not None
                self.active_local_functions.add(function_id)
                try:
                    self._visit_function(
                        local_function,
                        inherit_call_site_patches=True,
                    )
                finally:
                    self.active_local_functions.remove(function_id)
        self.generic_visit(node)

    def visit_For(self, node: ast.For) -> None:
        self.visit(node.iter)
        self._invalidate_binding(node.target)
        for statement in [*node.body, *node.orelse]:
            self.visit(statement)

    def visit_AsyncFor(self, node: ast.AsyncFor) -> None:
        self.visit(node.iter)
        self._invalidate_binding(node.target)
        for statement in [*node.body, *node.orelse]:
            self.visit(statement)

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
        if node.type is not None:
            self.visit(node.type)
        if node.name is not None:
            self._invalidate_binding(ast.Name(id=node.name))
        for statement in node.body:
            self.visit(statement)

    def visit_NamedExpr(self, node: ast.NamedExpr) -> None:
        self.visit(node.value)
        names = _assigned_names(node.target)
        self._invalidate_binding(node.target)
        for bindings in self.comprehension_walrus_bindings:
            bindings.update(names)

    def _visit_comprehension(
        self,
        generators: list[ast.comprehension],
        values: tuple[ast.expr, ...],
    ) -> None:
        prior_aliases = self.aliases
        prior_instances = self.instances
        prior_local_functions = self.local_functions
        self.aliases = dict(prior_aliases)
        self.instances = dict(prior_instances)
        self.local_functions = dict(prior_local_functions)
        self.comprehension_walrus_bindings.append(set())
        for generator in generators:
            self.visit(generator.iter)
            self._invalidate_binding(generator.target)
            for condition in generator.ifs:
                self.visit(condition)
        for value in values:
            self.visit(value)
        persistent_bindings = self.comprehension_walrus_bindings.pop()
        self.aliases = prior_aliases
        self.instances = prior_instances
        self.local_functions = prior_local_functions
        for name in persistent_bindings:
            self._invalidate_binding(ast.Name(id=name))

    def visit_ListComp(self, node: ast.ListComp) -> None:
        self._visit_comprehension(node.generators, (node.elt,))

    def visit_SetComp(self, node: ast.SetComp) -> None:
        self._visit_comprehension(node.generators, (node.elt,))

    def visit_GeneratorExp(self, node: ast.GeneratorExp) -> None:
        self._visit_comprehension(node.generators, (node.elt,))

    def visit_DictComp(self, node: ast.DictComp) -> None:
        self._visit_comprehension(node.generators, (node.key, node.value))


def find_ineffective_default_patches(
    production_sources: Mapping[str, str],
    test_sources: Mapping[str, str],
) -> tuple[DefaultPatchFinding, ...]:
    """Return calls whose active patch cannot replace a captured default.

    Patch context managers/decorators count only when their binding resolves
    exactly to ``unittest.mock.patch`` or ``unittest.mock.patch.object``.
    Same-named helpers, parameters, and object attributes are not inferred to
    be mocks. Nested local functions are evaluated only when called directly
    by their local name, using patch state at that call site. Calls through
    returned, assigned, or passed callback aliases remain outside the bounded
    source-local analysis rather than inheriting definition-time patch state.
    """
    captures, class_paths = _captured_defaults(production_sources)
    findings: list[DefaultPatchFinding] = []
    for test_path, source in test_sources.items():
        tree = ast.parse(source, filename=test_path)
        visitor = _TestPatchVisitor(
            test_path=test_path,
            captures=captures,
            class_paths=class_paths,
        )
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
