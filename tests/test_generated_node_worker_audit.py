"""Audit that generated properties never pay one Node startup per example.

Generated modules may exercise real JavaScript, but a literal Node subprocess in
such a module is target-amplified by Hypothesis and entropy sharding.  The
canonical boundary is ``tests.node_jsonl_worker.NodeJsonlWorker``: one strict,
fail-closed child per isolated Python target.
"""

from __future__ import annotations

import ast
import unittest
from pathlib import Path

TESTS_DIR = Path(__file__).resolve().parent
_NODE_EXECUTABLES = {"node", "nodejs"}
_SUBPROCESS_CALLS = {"run", "Popen", "check_call", "check_output"}


def direct_node_launch_lines(source: str, *, label: str) -> list[int]:
    """Return literal ``subprocess.<launch>(['node', ...])`` line numbers."""
    tree = ast.parse(source, filename=label)
    lines: list[int] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        function = node.func
        if not (
            isinstance(function, ast.Attribute)
            and isinstance(function.value, ast.Name)
            and function.value.id == "subprocess"
            and function.attr in _SUBPROCESS_CALLS
            and node.args
        ):
            continue
        command = node.args[0]
        if not isinstance(command, (ast.List, ast.Tuple)) or not command.elts:
            continue
        executable = command.elts[0]
        if (
            isinstance(executable, ast.Constant)
            and isinstance(executable.value, str)
            and Path(executable.value).name in _NODE_EXECUTABLES
        ):
            lines.append(node.lineno)
    return sorted(lines)


def audit_generated_node_launches(root: Path = TESTS_DIR) -> list[str]:
    offenders: list[str] = []
    for path in sorted(root.glob("test_*_generated.py")):
        source = path.read_text(encoding="utf-8")
        for line in direct_node_launch_lines(source, label=path.name):
            offenders.append(f"{path.name}:{line}")
    return offenders


class TestGeneratedNodeWorkerAudit(unittest.TestCase):
    def test_generated_properties_have_no_per_example_node_launches(self) -> None:
        self.assertEqual(
            audit_generated_node_launches(),
            [],
            "Generated tests must reuse one fail-closed NodeJsonlWorker per "
            "Python target instead of launching Node in every Hypothesis world",
        )

    def test_checker_rejects_the_historical_literal_node_subprocess(self) -> None:
        source = """
import subprocess

def real_boundary(payload):
    return subprocess.run(
        ["node", "--input-type=module", "--eval", "script"],
        input=payload,
    )
"""
        self.assertEqual(
            direct_node_launch_lines(source, label="planted.py"),
            [5],
        )

    def test_checker_accepts_the_target_scoped_worker(self) -> None:
        source = """
from tests.node_jsonl_worker import NodeJsonlWorker

def real_boundary(worker, payload):
    return worker.request("parse", payload)
"""
        self.assertEqual(
            direct_node_launch_lines(source, label="compliant.py"),
            [],
        )


if __name__ == "__main__":
    unittest.main()
