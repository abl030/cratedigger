"""Structural audit for explicit bare-MP3 target bitrate modes."""

from __future__ import annotations

import ast
from dataclasses import dataclass


@dataclass(frozen=True)
class TargetContractCallViolation:
    """One target-contract call whose format can be bare MP3 without a mode."""

    relative_path: str
    line: int


def _target_contract_bindings(tree: ast.Module) -> tuple[set[str], set[str]]:
    class_names = {"TargetQualityContract"}
    module_names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name == "TargetQualityContract":
                    class_names.add(alias.asname or alias.name)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in {"lib.quality", "lib.quality.evidence_types"}:
                    module_names.add(alias.asname or alias.name)
    return class_names, module_names


def _dotted_name(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = _dotted_name(node.value)
        return f"{prefix}.{node.attr}" if prefix else None
    return None


def _is_target_contract_factory(
    call: ast.Call,
    *,
    class_names: set[str],
    module_names: set[str],
) -> bool:
    func = call.func
    if not isinstance(func, ast.Attribute) or func.attr != "from_format":
        return False
    owner = func.value
    if isinstance(owner, ast.Name):
        return owner.id in class_names
    if not isinstance(owner, ast.Attribute) or owner.attr != "TargetQualityContract":
        return False
    return _dotted_name(owner.value) in module_names


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
    class_names, module_names = _target_contract_bindings(tree)
    violations: list[TargetContractCallViolation] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not _is_target_contract_factory(
            node,
            class_names=class_names,
            module_names=module_names,
        ):
            continue
        if any(keyword.arg == "projected_is_cbr" for keyword in node.keywords):
            continue
        format_argument = _format_argument(node)
        literal_is_self_describing = (
            isinstance(format_argument, ast.Constant)
            and isinstance(format_argument.value, str)
            and format_argument.value.strip().lower() != "mp3"
        )
        if not literal_is_self_describing:
            violations.append(
                TargetContractCallViolation(relative_path, node.lineno)
            )
    return tuple(violations)
