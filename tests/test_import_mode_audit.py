"""Import-mode ratchet: quality is mode-blind and every exception is audited."""

from __future__ import annotations

import ast
import argparse
from dataclasses import dataclass
import hashlib
import inspect
from pathlib import Path
import unittest

from hypothesis import given
from hypothesis import strategies as st
import msgspec

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_TYPES,
    IMPORT_JOB_YOUTUBE,
)
from lib.import_preview import ImportPreviewValues
from lib.quality import (
    AlbumQualityEvidenceDecisionFacts,
    full_pipeline_decision,
)
from scripts.pipeline_cli.routes_meta import _build_parser
from web.routes.imports import ROUTES


REPO_ROOT = Path(__file__).resolve().parents[1]
_PRODUCTION_PATHS = (
    "lib",
    "scripts",
    "web",
    "harness",
    "cratedigger.py",
    "album_source.py",
)
_MODE_NAMES = frozenset({
    "force",
    "import_mode",
    "IMPORT_JOB_FORCE",
    "IMPORT_JOB_MANUAL",
    "FORCE_IMPORT_SCENARIOS",
    "FORCE_MANUAL_SCENARIOS",
    "preserve_source",
    "source_is_disposable",
})
_MODE_LITERALS = frozenset({
    "auto_import",
    "automation_import",
    "force_import",
    "manual_import",
})


@dataclass(frozen=True)
class ModeGate:
    path: str
    scope: str
    line: int
    condition: str
    fingerprint: str

    @property
    def key(self) -> tuple[str, str, int, str]:
        return (self.path, self.scope, self.line, self.fingerprint)


@dataclass(frozen=True)
class ModeGateAuthority:
    link: str
    quote: str
    reason: str


_DISTANCE_ONLY = ModeGateAuthority(
    link="https://github.com/abl030/cratedigger/issues/711#issuecomment-5000425284",
    quote=(
        "Force/manual imports hit `verified_lossless_locked` exactly like "
        "automatic candidates — force-import bypasses the beets distance "
        "and NOTHING else, completing D19's principle."
    ),
    reason="Force may alter only the Beets candidate-distance threshold.",
)
_AUDITED_MODE_SET = ModeGateAuthority(
    link="https://github.com/abl030/cratedigger/issues/737",
    quote=(
        "Today's legitimate set is small: distance bypass, `preserve_source`, "
        "resume-guard stamping, audit outcome labels, nested-layout handling."
    ),
    reason="This is an explicitly named automatic/force structural difference.",
)
_FORCE_ONLY_ACTIVE_MODE = ModeGateAuthority(
    link="https://github.com/abl030/cratedigger/issues/737#issuecomment-5010653922",
    quote=(
        "Delivery consequence for the revised sequence: step 2's import-mode "
        "ratchet covers automatic versus force-import only and removes "
        "`manual-import`."
    ),
    reason="The active operator import branch is force-import only.",
)
_PRESERVE_FORCE_SOURCE = ModeGateAuthority(
    link="https://github.com/abl030/cratedigger/issues/111",
    quote=(
        "The original files are deleted at ~line 544/547 *before* the quality "
        "decision runs at line 985."
    ),
    reason="Force-import must retain the operator's source through decision time.",
)
_FORCE_CLEANUP_BOUNDARY = ModeGateAuthority(
    link="https://github.com/abl030/cratedigger/issues/89",
    quote=(
        "On **force-import** / **manual-import**, the ``path`` passed in is "
        "already the ``failed_imports/…`` folder."
    ),
    reason="Force source cleanup is destructive unless Beets moved the files.",
)
_OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT = ModeGateAuthority(
    link="https://github.com/abl030/cratedigger/issues/737#issuecomment-5010653922",
    quote=(
        "Only the operator may set or clear `unsearchable`; the pipeline must "
        "never assign it automatically."
    ),
    reason="A rejected force attempt must not rewrite operator-owned request state.",
)
_HISTORICAL_MANUAL_READS = ModeGateAuthority(
    link="https://github.com/abl030/cratedigger/issues/737#issuecomment-5010653922",
    quote=(
        "Delete its API, CLI, import-job type, worker branches, and active "
        "tests; historical `manual_import` audit rows may remain readable."
    ),
    reason="This read-only branch preserves historical audit rendering.",
)


# Populated only with surviving, operator-authorized automatic/force
# differences. A new caller-identity conditional fails until its authority is
# reviewed and recorded here.
MODE_GATE_REGISTRY: dict[tuple[str, str, int, str], ModeGateAuthority] = {
    ("harness/import_one.py", "target_cleanup_decision", 476,
     "4796e2c7a7d737b9"): _PRESERVE_FORCE_SOURCE,
    ("harness/import_one.py", "main", 1577,
     "1ad02461ea8af4a2"): _DISTANCE_ONLY,
    ("harness/import_one.py", "main", 2127,
     "76ba9b5f5116a1e8"): _PRESERVE_FORCE_SOURCE,
    ("harness/import_one.py", "main", 2001,
     "54723607d5081ee1"): _PRESERVE_FORCE_SOURCE,
    ("harness/import_one.py", "main", 2020,
     "54723607d5081ee1"): _PRESERVE_FORCE_SOURCE,
    ("lib/dispatch/core.py", "dispatch_import_core", 411,
     "77666a53867810de"): _AUDITED_MODE_SET,
    ("lib/dispatch/core.py", "dispatch_import_core", 445,
     "f0bd2ce87c0656c5"): _PRESERVE_FORCE_SOURCE,
    ("lib/dispatch/entry_points.py", "_dispatch_import_from_db_locked", 214,
     "fc49e3ff31db631b"): _DISTANCE_ONLY,
    ("lib/dispatch/entry_points.py", "_dispatch_import_from_db_locked", 224,
     "027e505b9b19ba1e"): _AUDITED_MODE_SET,
    ("lib/dispatch/entry_points.py", "_dispatch_import_from_db_locked", 227,
     "027e505b9b19ba1e"): _AUDITED_MODE_SET,
    ("lib/dispatch/entry_points.py", "_dispatch_import_from_db_locked", 228,
     "bfdbc42f821a1db3"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/helpers.py", "_should_cleanup_path", 47,
     "77666a53867810de"): _FORCE_CLEANUP_BOUNDARY,
    ("lib/dispatch/subprocess_runner.py", "build_import_one_command", 54,
     "ecd75225e973c696"): _DISTANCE_ONLY,
    ("lib/dispatch/subprocess_runner.py", "build_import_one_command", 56,
     "4796e2c7a7d737b9"): _PRESERVE_FORCE_SOURCE,
    ("lib/import_preview.py", "preview_import_from_download_log", 2277,
     "fc49e3ff31db631b"): _DISTANCE_ONLY,
    ("lib/import_queue.py", "validate_payload", 264,
     "6a090b8735c2ff6a"): _FORCE_ONLY_ACTIVE_MODE,
    ("scripts/import_preview_worker.py", "_front_gate_source_path", 183,
     "e45cab5ab5498692"): _FORCE_ONLY_ACTIVE_MODE,
    ("scripts/import_preview_worker.py", "_preview_input", 298,
     "e45cab5ab5498692"): _FORCE_ONLY_ACTIVE_MODE,
    ("scripts/import_preview_worker.py", "_preview_input", 306,
     "fc49e3ff31db631b"): _DISTANCE_ONLY,
    ("scripts/importer.py", "_force_job_wrong_match_payload", 67,
     "33db16a2e07e50e3"): _FORCE_ONLY_ACTIVE_MODE,
    ("scripts/importer.py", "execute_import_job", 177,
     "e45cab5ab5498692"): _FORCE_ONLY_ACTIVE_MODE,
    ("scripts/pipeline_cli/imports.py", "cmd_import_preview", 219,
     "6c3f26a9499a61f6"): _DISTANCE_ONLY,
    ("web/classify.py", "_classify", 872,
     "8fab0fe8c44bd8ed"): _AUDITED_MODE_SET,
    ("web/routes/imports.py", "post_import_preview", 846,
     "1be94be9f44ac8cf"): _DISTANCE_ONLY,
    ("web/routes/pipeline.py", "_project_current_library_have", 70,
     "44ba87e6f7f70835"): _HISTORICAL_MANUAL_READS,
    ("web/routes/pipeline.py", "_project_linked_import_evidence", 160,
     "28aab077c4104581"): _HISTORICAL_MANUAL_READS,
}


def _scope_name(node: ast.AST, parents: dict[ast.AST, ast.AST]) -> str:
    current = node
    while current in parents:
        current = parents[current]
        if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return current.name
    return "<module>"


def _is_import_mode_condition(condition: ast.expr) -> bool:
    names = {
        node.id if isinstance(node, ast.Name) else node.attr
        for node in ast.walk(condition)
        if isinstance(node, (ast.Name, ast.Attribute))
    }
    literals = {
        node.value
        for node in ast.walk(condition)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }
    return bool(names & _MODE_NAMES or literals & _MODE_LITERALS)


def _is_force_grant(keyword: str, value: ast.expr) -> bool:
    """Recognize a local, explicit force-only permission at a call seam."""
    if keyword == "force":
        return not (
            isinstance(value, ast.Name)
            or (isinstance(value, ast.Constant) and value.value is False)
        )
    if keyword == "requeue_on_failure":
        return isinstance(value, ast.Constant) and value.value is False
    if keyword in {"scenario", "outcome_label"}:
        return (
            isinstance(value, ast.Constant)
            and value.value == "force_import"
        )
    return False


def mode_gates_in_source(source: str, *, path: str) -> list[ModeGate]:
    tree = ast.parse(source, filename=path)
    parents = {
        child: parent
        for parent in ast.walk(tree)
        for child in ast.iter_child_nodes(parent)
    }
    conditions: list[tuple[ast.AST, ast.expr, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.If, ast.IfExp, ast.While)):
            conditions.append((node, node.test, ast.unparse(node.test)))
        elif isinstance(node, ast.comprehension):
            conditions.extend(
                (node, condition, ast.unparse(condition))
                for condition in node.ifs
            )
        elif isinstance(node, ast.match_case) and node.guard is not None:
            conditions.append((node, node.guard, ast.unparse(node.guard)))

    condition_nodes = {condition for _, condition, _ in conditions}
    for node in ast.walk(tree):
        if isinstance(node, ast.Compare) and _is_import_mode_condition(node):
            current: ast.AST | None = node
            covered = False
            while current in parents:
                current = parents[current]
                if current in condition_nodes:
                    covered = True
                    break
            if not covered:
                conditions.append((node, node, ast.unparse(node)))
        elif (
            isinstance(node, ast.keyword)
            and node.arg is not None
            and _is_force_grant(node.arg, node.value)
        ):
            conditions.append((node, node.value, f"{node.arg}={ast.unparse(node.value)}"))
        elif isinstance(node, ast.Dict):
            for key, value in zip(node.keys, node.values):
                if (
                    isinstance(key, ast.Constant)
                    and isinstance(key.value, str)
                    and _is_force_grant(key.value, value)
                ):
                    conditions.append(
                        (node, value, f"{key.value!r}: {ast.unparse(value)}")
                    )

    findings: list[ModeGate] = []
    seen: set[tuple[int, str]] = set()
    for node, condition, presentation in conditions:
        if not (
            _is_import_mode_condition(condition)
            or _is_force_grant(
                presentation.partition("=")[0],
                condition,
            )
            or _is_force_grant(
                presentation.split(":", 1)[0].strip("'"),
                condition,
            )
        ):
            continue
        identity = (int(getattr(condition, "lineno", 0)), presentation)
        if identity in seen:
            continue
        seen.add(identity)
        normalized = ast.dump(condition, include_attributes=False)
        findings.append(ModeGate(
            path=path,
            scope=_scope_name(node, parents),
            line=int(getattr(condition, "lineno", 0)),
            condition=presentation,
            fingerprint=hashlib.sha256(normalized.encode()).hexdigest()[:16],
        ))
    return findings


def production_mode_gates() -> list[ModeGate]:
    files: set[Path] = set()
    for relative in _PRODUCTION_PATHS:
        path = REPO_ROOT / relative
        if path.is_dir():
            files.update(path.rglob("*.py"))
        else:
            files.add(path)
    findings: list[ModeGate] = []
    for path in sorted(files):
        findings.extend(mode_gates_in_source(
            path.read_text(),
            path=str(path.relative_to(REPO_ROOT)),
        ))
    return findings


def assert_mode_gate_registry_complete(
    findings: list[ModeGate],
    registry: dict[tuple[str, str, int, str], ModeGateAuthority],
) -> None:
    found = {finding.key for finding in findings}
    registered = set(registry)
    missing = [finding for finding in findings if finding.key not in registered]
    stale = sorted(registered - found)
    if missing or stale:
        detail = [
            *(f"unregistered: {item.path}:{item.line} "
              f"{item.scope}: {item.condition}" for item in missing),
            *(f"stale registry entry: {item!r}" for item in stale),
        ]
        raise AssertionError("\n".join(detail))


class TestImportModeContract(unittest.TestCase):
    def test_manual_import_action_is_absent(self):
        self.assertEqual(
            IMPORT_JOB_TYPES,
            frozenset({
                IMPORT_JOB_AUTOMATION,
                IMPORT_JOB_FORCE,
                IMPORT_JOB_YOUTUBE,
            }),
        )
        parser, _, _ = _build_parser()
        subcommands = next(
            action.choices
            for action in parser._actions
            if isinstance(action, argparse._SubParsersAction)
        )
        self.assertNotIn("manual-import", subcommands)
        self.assertNotIn(
            "/api/manual-import/import",
            {registration.path for registration in ROUTES},
        )

    def test_quality_decider_has_no_caller_mode_input(self):
        self.assertNotIn(
            "import_mode",
            inspect.signature(full_pipeline_decision).parameters,
        )
        self.assertNotIn(
            "import_mode",
            {field.name for field in msgspec.structs.fields(
                AlbumQualityEvidenceDecisionFacts
            )},
        )
        self.assertNotIn(
            "import_mode",
            {field.name for field in msgspec.structs.fields(ImportPreviewValues)},
        )

    def test_every_mode_gate_has_exact_authority(self):
        assert_mode_gate_registry_complete(
            production_mode_gates(),
            MODE_GATE_REGISTRY,
        )
        for authority in MODE_GATE_REGISTRY.values():
            self.assertTrue(authority.link.startswith("https://github.com/"))
            self.assertTrue(authority.quote.strip())
            self.assertTrue(authority.reason.strip())


class TestImportModeAuditGenerated(unittest.TestCase):
    @given(
        name=st.sampled_from(sorted(_MODE_NAMES)),
        negated=st.booleans(),
        attribute=st.booleans(),
    )
    def test_detector_finds_generated_mode_conditions(
        self,
        name,
        negated,
        attribute,
    ):
        expression = f"args.{name}" if attribute else name
        condition = f"not {expression}" if negated else expression
        findings = mode_gates_in_source(
            f"def generated():\n    if {condition}:\n        return 1\n",
            path="generated.py",
        )
        self.assertEqual(len(findings), 1)

    @given(keyword=st.sampled_from((
        "force",
        "requeue_on_failure",
        "scenario",
        "outcome_label",
    )))
    def test_detector_finds_generated_force_grants(self, keyword):
        value = {
            "force": "True",
            "requeue_on_failure": "False",
            "scenario": repr("force_import"),
            "outcome_label": repr("force_import"),
        }[keyword]
        findings = mode_gates_in_source(
            f"def generated(target):\n    target({keyword}={value})\n",
            path="generated.py",
        )
        self.assertEqual(len(findings), 1)

    def test_registry_checker_trips_on_planted_unregistered_gate(self):
        planted = mode_gates_in_source(
            "def mutant(force):\n    if force:\n        return 'bypass'\n",
            path="mutant.py",
        )
        with self.assertRaises(AssertionError):
            assert_mode_gate_registry_complete(planted, {})


if __name__ == "__main__":
    unittest.main()
