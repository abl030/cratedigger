"""Keep direct production request insertion behind issue #791's service."""

from __future__ import annotations

import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ALLOWED_FILES = {
    ROOT / "lib" / "request_creation_service.py",
    ROOT / "lib" / "pipeline_db" / "requests.py",
}
ALLOWED_FUNCTIONS = {
    ROOT / "lib" / "ephemeral_postgres.py": {"seed_transition_request"},
}


class TestRequestCreationCallAudit(unittest.TestCase):
    def test_only_creation_service_or_replace_storage_calls_add_request(self) -> None:
        offenders: list[str] = []
        for path in [*ROOT.joinpath("lib").rglob("*.py"), *ROOT.joinpath("web").rglob("*.py"), *ROOT.joinpath("scripts").rglob("*.py")]:
            if path in ALLOWED_FILES:
                continue
            tree = ast.parse(path.read_text(), filename=str(path))
            allowed_calls = {
                id(node)
                for function in ast.walk(tree)
                if (
                    isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and function.name in ALLOWED_FUNCTIONS.get(path, set())
                )
                for node in ast.walk(function)
            }
            for node in ast.walk(tree):
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "add_request"
                    and id(node) not in allowed_calls
                ):
                    offenders.append(f"{path.relative_to(ROOT)}:{node.lineno}")
        self.assertEqual(offenders, [])
