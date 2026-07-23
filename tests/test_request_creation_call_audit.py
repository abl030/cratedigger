"""Keep direct production request insertion behind issue #791's service."""

from __future__ import annotations

import ast
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
ALLOWED = {
    ROOT / "lib" / "request_creation_service.py",
    ROOT / "lib" / "pipeline_db" / "requests.py",
}


class TestRequestCreationCallAudit(unittest.TestCase):
    def test_only_creation_service_or_replace_storage_calls_add_request(self) -> None:
        offenders: list[str] = []
        for path in [*ROOT.joinpath("lib").rglob("*.py"), *ROOT.joinpath("web").rglob("*.py"), *ROOT.joinpath("scripts").rglob("*.py")]:
            if path in ALLOWED:
                continue
            tree = ast.parse(path.read_text(), filename=str(path))
            for node in ast.walk(tree):
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "add_request"
                ):
                    offenders.append(f"{path.relative_to(ROOT)}:{node.lineno}")
        self.assertEqual(offenders, [])
