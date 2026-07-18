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
    "requeue_on_failure",
    "requeue_to_wanted",
    "allow_request_requeue",
    "operator_stop_status",
    "IMPORT_JOB_AUTOMATION",
    "IMPORT_JOB_FORCE",
    "IMPORT_JOB_MANUAL",
    "FORCE_IMPORT_SCENARIOS",
    "FORCE_MANUAL_SCENARIOS",
    "preserve_source",
    "source_is_disposable",
})
_LIFECYCLE_REQUEUE_KEYWORDS = frozenset({
    "requeue",
    "requeue_on_failure",
    "requeue_to_wanted",
    "allow_request_requeue",
})
_MODE_ASSIGNMENT_GRANTS = frozenset({"operator_stop_status"})
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
    def key(self) -> tuple[str, str, int, str, str]:
        return (
            self.path,
            self.scope,
            self.line,
            self.condition,
            self.fingerprint,
        )


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
    reason=(
        "No pipeline attempt, failed or successful, may rewrite current "
        "operator-owned search state."
    ),
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
MODE_GATE_REGISTRY: dict[
    tuple[str, str, int, str, str],
    ModeGateAuthority,
] = {
    ("harness/import_one.py", "target_cleanup_decision", 476,
     "preserve_source", "4796e2c7a7d737b9"): _PRESERVE_FORCE_SOURCE,
    ("harness/import_one.py", "main", 1577,
     "args.force", "1ad02461ea8af4a2"): _DISTANCE_ONLY,
    ("harness/import_one.py", "main", 2130,
     "not keep_lossless and target_cleanup_decision(target_achieved, has_target, converted, preserve_source=args.preserve_source)",
     "76ba9b5f5116a1e8"): _PRESERVE_FORCE_SOURCE,
    ("harness/import_one.py", "main", 2004,
     "args.preserve_source and (not keep_lossless) and (converted > 0)",
     "54723607d5081ee1"): _PRESERVE_FORCE_SOURCE,
    ("harness/import_one.py", "main", 2023,
     "args.preserve_source and (not keep_lossless) and (converted > 0)",
     "54723607d5081ee1"): _PRESERVE_FORCE_SOURCE,
    ("harness/import_one.py", "main", 1749,
     "(has_target or args.preserve_source) and converted > 0",
     "f066d6386305e215"): _PRESERVE_FORCE_SOURCE,
    ("harness/import_one.py", "main", 2132,
     "preserve_source=args.preserve_source", "833a2f1fee949dce"): _PRESERVE_FORCE_SOURCE,
    ("harness/import_one.py", "main", 1723,
     "keep_source=has_target or args.preserve_source",
     "100ad7c36bb48b60"): _PRESERVE_FORCE_SOURCE,
    ("lib/dispatch/core.py", "dispatch_import_core", 121,
     "force", "ecd75225e973c696"): _AUDITED_MODE_SET,
    ("lib/dispatch/core.py", "dispatch_import_core", 419,
     "scenario not in FORCE_IMPORT_SCENARIOS", "77666a53867810de"): _AUDITED_MODE_SET,
    ("lib/dispatch/core.py", "dispatch_import_core", 266,
     "requeue_on_failure", "3992c39583766481"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/core.py", "dispatch_import_core", 452,
     "force=force", "ecd75225e973c696"): _DISTANCE_ONLY,
    ("lib/dispatch/core.py", "dispatch_import_core", 453,
     "preserve_source=scenario in FORCE_IMPORT_SCENARIOS", "f0bd2ce87c0656c5"): _PRESERVE_FORCE_SOURCE,
    ("lib/dispatch/core.py", "dispatch_import_core", 258,
     "requeue_to_wanted=requeue_on_failure", "3992c39583766481"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/core.py", "dispatch_import_core", 263,
     "message='Installed HAVE analysis failed; ' + ('request returned to wanted for a future retry' if requeue_on_failure else 'request lifecycle was preserved')",
     "578796b8e26d1267"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/core.py", "dispatch_import_core", 453,
     "scenario in FORCE_IMPORT_SCENARIOS", "f0bd2ce87c0656c5"): _PRESERVE_FORCE_SOURCE,
    ("lib/dispatch/core.py", "dispatch_import_core", 482,
     "requeue=requeue_on_failure", "3992c39583766481"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/core.py", "dispatch_import_core", 920,
     "requeue=requeue_on_failure", "3992c39583766481"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/core.py", "dispatch_import_core", 940,
     "requeue=requeue_on_failure", "3992c39583766481"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/core.py", "dispatch_import_core", 389,
     "requeue_on_failure=requeue_on_failure", "3992c39583766481"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/core.py", "dispatch_import_core", 748,
     "requeue=requeue_on_failure", "3992c39583766481"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/entry_points.py", "_dispatch_import_from_db_locked", 214,
     "force=True", "fc49e3ff31db631b"): _DISTANCE_ONLY,
    ("lib/dispatch/entry_points.py", "_dispatch_import_from_db_locked", 224,
     "scenario='force_import'", "027e505b9b19ba1e"): _AUDITED_MODE_SET,
    ("lib/dispatch/entry_points.py", "_dispatch_import_from_db_locked", 227,
     "outcome_label='force_import'", "027e505b9b19ba1e"): _AUDITED_MODE_SET,
    ("lib/dispatch/entry_points.py", "_dispatch_import_from_db_locked", 228,
     "requeue_on_failure=False", "bfdbc42f821a1db3"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/helpers.py", "_should_cleanup_path", 47,
     "scenario not in FORCE_IMPORT_SCENARIOS", "77666a53867810de"): _FORCE_CLEANUP_BOUNDARY,
    ("lib/dispatch/manifest_guard.py", "_guard_reject", 113,
     "requeue=False", "bfdbc42f821a1db3"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/outcome_actions.py", "_finalize_request_and_log_rejection", 384,
     "requeue_to_wanted and request_id is not None", "e2bfed59bcd70346"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/outcome_actions.py", "_record_have_analysis_error", 647,
     "requeue_to_wanted", "6779ab3f329c7d06"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/outcome_actions.py", "_record_preview_measurement_failed", 574,
     "requeue_to_wanted", "6779ab3f329c7d06"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/outcome_actions.py", "_reject_import_from_evidence_decision", 133,
     "requeue=requeue_on_failure and (not action.preserve_imported)",
     "327ecf5f07a4066e"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/outcome_actions.py", "_record_have_analysis_error", 668,
     "requeue_to_wanted=requeue_to_wanted", "6779ab3f329c7d06"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/outcome_actions.py", "_record_have_analysis_error", 669,
     "record_validation_attempt=requeue_to_wanted", "6779ab3f329c7d06"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/outcome_actions.py", "_record_preview_measurement_failed", 573,
     "request_transition=transitions.RequestTransition.to_wanted() if requeue_to_wanted else None",
     "d3781a96bd337810"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/subprocess_runner.py", "build_import_one_command", 54,
     "force", "ecd75225e973c696"): _DISTANCE_ONLY,
    ("lib/dispatch/subprocess_runner.py", "build_import_one_command", 56,
     "preserve_source", "4796e2c7a7d737b9"): _PRESERVE_FORCE_SOURCE,
    ("lib/dispatch/subprocess_runner.py", "run_import_one", 112,
     "force=force", "ecd75225e973c696"): _DISTANCE_ONLY,
    ("lib/dispatch/subprocess_runner.py", "run_import_one", 113,
     "preserve_source=preserve_source", "4796e2c7a7d737b9"): _PRESERVE_FORCE_SOURCE,
    ("lib/download_validation.py", "_handle_valid_result", 414,
     "scenario=bv_result.scenario or 'auto_import'", "15d41612865d1df1"): _AUDITED_MODE_SET,
    ("lib/download_validation.py", "_handle_valid_result", 389,
     "scenario=bv_result.scenario or 'auto_import'", "15d41612865d1df1"): _AUDITED_MODE_SET,
    ("lib/import_preview.py", "preview_import_from_download_log", 2277,
     "force=True", "fc49e3ff31db631b"): _DISTANCE_ONLY,
    ("lib/import_preview.py", "preview_import_from_path", 2131,
     "force=force", "ecd75225e973c696"): _DISTANCE_ONLY,
    ("lib/import_preview.py", "preview_import_from_path", 2132,
     "preserve_source=True", "fc49e3ff31db631b"): _PRESERVE_FORCE_SOURCE,
    ("lib/import_preview.py", "measure_and_persist_candidate_evidence", 1672,
     "force=force", "ecd75225e973c696"): _DISTANCE_ONLY,
    ("lib/import_preview.py", "measure_and_persist_candidate_evidence", 1673,
     "preserve_source=True", "fc49e3ff31db631b"): _PRESERVE_FORCE_SOURCE,
    ("lib/import_queue.py", "validate_payload", 264,
     "job_type == IMPORT_JOB_FORCE", "6a090b8735c2ff6a"): _FORCE_ONLY_ACTIVE_MODE,
    ("scripts/import_preview_worker.py", "_front_gate_source_path", 183,
     "job.job_type == IMPORT_JOB_FORCE", "e45cab5ab5498692"): _FORCE_ONLY_ACTIVE_MODE,
    ("scripts/import_preview_worker.py", "_front_gate_source_path", 203,
     "job.job_type == IMPORT_JOB_AUTOMATION", "c9ff5a21646b597c"): _FORCE_ONLY_ACTIVE_MODE,
    ("scripts/import_preview_worker.py", "_preview_input", 298,
     "job.job_type == IMPORT_JOB_FORCE", "e45cab5ab5498692"): _FORCE_ONLY_ACTIVE_MODE,
    ("scripts/import_preview_worker.py", "_preview_input", 314,
     "job.job_type == IMPORT_JOB_AUTOMATION", "c9ff5a21646b597c"): _FORCE_ONLY_ACTIVE_MODE,
    ("scripts/import_preview_worker.py", "_preview_input", 306,
     "'force': True", "fc49e3ff31db631b"): _DISTANCE_ONLY,
    ("scripts/import_preview_worker.py", "_handle_measurement_failed", 406,
     "requeue_to_wanted=job.job_type == IMPORT_JOB_AUTOMATION",
     "c9ff5a21646b597c"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("scripts/import_preview_worker.py", "_handle_measurement_failed", 406,
     "job.job_type == IMPORT_JOB_AUTOMATION", "c9ff5a21646b597c"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("scripts/importer.py", "_force_job_wrong_match_payload", 67,
     "job.job_type != IMPORT_JOB_FORCE", "33db16a2e07e50e3"): _FORCE_ONLY_ACTIVE_MODE,
    ("scripts/importer.py", "execute_import_job", 177,
     "job.job_type == IMPORT_JOB_FORCE", "e45cab5ab5498692"): _FORCE_ONLY_ACTIVE_MODE,
    ("scripts/importer.py", "execute_import_job", 218,
     "job.job_type == IMPORT_JOB_AUTOMATION", "c9ff5a21646b597c"): _FORCE_ONLY_ACTIVE_MODE,
    ("scripts/pipeline_cli/imports.py", "cmd_import_preview", 219,
     "force=not args.no_force", "6c3f26a9499a61f6"): _DISTANCE_ONLY,
    ("web/classify.py", "_classify", 875,
     "entry.outcome == 'force_import'", "8fab0fe8c44bd8ed"): _AUDITED_MODE_SET,
    ("web/routes/imports.py", "post_import_preview", 846,
     "force=bool(body.get('force', True))", "1be94be9f44ac8cf"): _DISTANCE_ONLY,
    ("web/routes/pipeline.py", "_project_current_library_have", 70,
     "item.get('outcome') in ('success', 'force_import', 'manual_import')", "44ba87e6f7f70835"): _HISTORICAL_MANUAL_READS,
    ("web/routes/pipeline.py", "_project_linked_import_evidence", 160,
     "successor.get('outcome') not in ('success', 'force_import', 'manual_import')", "28aab077c4104581"): _HISTORICAL_MANUAL_READS,
}


def _scope_name(node: ast.AST, parents: dict[ast.AST, ast.AST]) -> str:
    current = node
    while current in parents:
        current = parents[current]
        if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return current.name
    return "<module>"


def _is_import_mode_condition(
    condition: ast.AST,
    *,
    mode_names: frozenset[str] = _MODE_NAMES,
) -> bool:
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
    return bool(names & mode_names or literals & _MODE_LITERALS)


def _is_mode_grant(
    keyword: str,
    value: ast.expr,
    *,
    mode_names: frozenset[str] = _MODE_NAMES,
) -> bool:
    """Recognize explicit or forwarded caller-mode authority at a seam."""
    if _is_import_mode_condition(value, mode_names=mode_names):
        return True
    if keyword in {"force", "preserve_source"}:
        return not (isinstance(value, ast.Constant) and value.value is False)
    if keyword in _LIFECYCLE_REQUEUE_KEYWORDS:
        return isinstance(value, ast.Constant) and value.value is False
    if keyword in _MODE_ASSIGNMENT_GRANTS:
        return not (isinstance(value, ast.Constant) and value.value is None)
    return False


def _scope_owner(node: ast.AST, parents: dict[ast.AST, ast.AST]) -> ast.AST:
    current = node
    while current in parents:
        current = parents[current]
        if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return current
    while current in parents:
        current = parents[current]
    return current


def _assigned_names(target: ast.AST) -> set[str]:
    return {
        node.id
        for node in ast.walk(target)
        if isinstance(node, ast.Name)
    }


def _is_simple_mode_alias(
    value: ast.expr,
) -> bool:
    """Return whether an expression would hide caller-mode authority."""
    if isinstance(value, (ast.Name, ast.Attribute, ast.Constant)):
        return _is_import_mode_condition(value)
    if isinstance(value, ast.UnaryOp):
        return _is_simple_mode_alias(value.operand)
    if isinstance(value, ast.BoolOp):
        return any(_is_simple_mode_alias(item) for item in value.values)
    if isinstance(value, ast.Compare):
        return _is_import_mode_condition(value)
    if isinstance(value, ast.IfExp):
        if (
            isinstance(value.body, ast.Constant)
            and isinstance(value.body.value, bool)
            and isinstance(value.orelse, ast.Constant)
            and isinstance(value.orelse.value, bool)
            and _is_import_mode_condition(value.test)
        ):
            return True
        return (
            _is_simple_mode_alias(value.body)
            or _is_simple_mode_alias(value.orelse)
        )
    return False


def _reject_mode_aliases(tree: ast.AST, *, path: str) -> None:
    """Keep the audit grammar direct: canonical names only, no aliases.

    Flow-sensitive alias analysis is a poor security boundary. Production
    gates must spell the canonical mode/lifecycle name or literal at the gate;
    assignments may only target one of those canonical names directly.
    """
    violations: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Assign, ast.AnnAssign, ast.NamedExpr)):
            continue
        targets = list(node.targets) if isinstance(node, ast.Assign) else [node.target]
        value = node.value
        if value is None or not _is_simple_mode_alias(value):
            continue
        assigned = set().union(*(
            _assigned_names(target) for target in targets
        ))
        hidden = sorted(assigned - _MODE_NAMES - _MODE_ASSIGNMENT_GRANTS)
        if hidden:
            violations.append(
                f"{path}:{node.lineno}: mode aliases are forbidden: "
                + ", ".join(hidden)
            )
    if violations:
        raise AssertionError("\n".join(violations))


def mode_gates_in_source(source: str, *, path: str) -> list[ModeGate]:
    tree = ast.parse(source, filename=path)
    parents = {
        child: parent
        for parent in ast.walk(tree)
        for child in ast.iter_child_nodes(parent)
    }
    _reject_mode_aliases(tree, path=path)
    conditions: list[tuple[ast.AST, ast.AST, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.If, ast.IfExp, ast.While)):
            conditions.append((node, node.test, ast.unparse(node.test)))
        elif isinstance(node, ast.comprehension):
            conditions.extend(
                (node, condition, ast.unparse(condition))
                for condition in node.ifs
            )
        elif isinstance(node, ast.match_case):
            if node.guard is not None:
                conditions.append((node, node.guard, ast.unparse(node.guard)))
            if _is_import_mode_condition(node.pattern):
                conditions.append((
                    node.pattern,
                    node.pattern,
                    f"case {ast.unparse(node.pattern)}",
                ))

    condition_nodes = {condition for _, condition, _ in conditions}
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Compare)
            and _is_import_mode_condition(node)
        ):
            current: ast.AST | None = node
            covered = False
            while current in parents:
                current = parents[current]
                if current in condition_nodes:
                    covered = True
                    break
            if not covered:
                conditions.append((node, node, ast.unparse(node)))
        elif isinstance(node, (ast.Assign, ast.AnnAssign, ast.NamedExpr)):
            targets = (
                list(node.targets)
                if isinstance(node, ast.Assign)
                else [node.target]
            )
            value = node.value
            if value is None:
                continue
            assigned = set().union(*(
                _assigned_names(target) for target in targets
            ))
            for target_name in sorted(assigned & _MODE_ASSIGNMENT_GRANTS):
                conditions.append((
                    node,
                    value,
                    f"{target_name}={ast.unparse(value)}",
                ))
        elif (
            isinstance(node, ast.keyword)
            and node.arg is not None
            and _is_mode_grant(
                node.arg,
                node.value,
            )
        ):
            conditions.append((node, node.value, f"{node.arg}={ast.unparse(node.value)}"))
        elif isinstance(node, ast.Dict):
            for key, value in zip(node.keys, node.values):
                if (
                    isinstance(key, ast.Constant)
                    and isinstance(key.value, str)
                    and _is_mode_grant(
                        key.value,
                        value,
                    )
                ):
                    conditions.append(
                        (node, value, f"{key.value!r}: {ast.unparse(value)}")
                    )

    findings: list[ModeGate] = []
    seen: set[tuple[int, str, str]] = set()
    seen_condition_presentations: set[tuple[int, str]] = set()
    for node, condition, presentation in conditions:
        condition_presentation = (id(condition), presentation)
        if condition_presentation in seen_condition_presentations:
            continue
        seen_condition_presentations.add(condition_presentation)
        if not (
            _is_import_mode_condition(condition)
            or _is_mode_grant(
                presentation.partition("=")[0],
                condition,  # type: ignore[arg-type]
            )
            or _is_mode_grant(
                presentation.split(":", 1)[0].strip("'"),
                condition,  # type: ignore[arg-type]
            )
        ):
            continue
        normalized = ast.dump(condition, include_attributes=False)
        fingerprint = hashlib.sha256(normalized.encode()).hexdigest()[:16]
        identity = (
            int(getattr(condition, "lineno", 0)),
            presentation,
            fingerprint,
        )
        if identity in seen:
            position = (
                int(getattr(condition, "lineno", 0)),
                int(getattr(condition, "col_offset", 0)),
            )
            fingerprint = hashlib.sha256(
                f"{normalized}@{position[0]}:{position[1]}".encode()
            ).hexdigest()[:16]
            identity = (
                int(getattr(condition, "lineno", 0)),
                presentation,
                fingerprint,
            )
        seen.add(identity)
        findings.append(ModeGate(
            path=path,
            scope=_scope_name(node, parents),
            line=int(getattr(condition, "lineno", 0)),
            condition=presentation,
            fingerprint=fingerprint,
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
    registry: dict[tuple[str, str, int, str, str], ModeGateAuthority],
) -> None:
    found = {finding.key for finding in findings}
    registered = set(registry)
    missing = [finding for finding in findings if finding.key not in registered]
    stale = sorted(registered - found)
    if missing or stale:
        detail = [
            *(f"unregistered: {item.key!r}" for item in missing),
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
        parser, _, _ = _build_parser()
        subcommands = next(
            action.choices
            for action in parser._actions
            if isinstance(action, argparse._SubParsersAction)
        )
        preview_options = {
            option
            for action in subcommands["import-preview"]._actions
            for option in action.option_strings
        }
        self.assertNotIn("--import-mode", preview_options)

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

    def test_detector_closes_direct_mode_grant_escape_hatches(self):
        mutants = {
            "manual literal": "target(scenario='manual_import')",
            "source preservation": "target(preserve_source=True)",
            "aliased force": "target(force=forwarded_force)",
        }
        for label, call in mutants.items():
            with self.subTest(label=label):
                findings = mode_gates_in_source(
                    f"def mutant(forwarded_force):\n    {call}\n",
                    path="mutant.py",
                )
                self.assertEqual(len(findings), 1)

    def test_detector_distinguishes_two_grants_on_the_same_line(self):
        findings = mode_gates_in_source(
            "def mutant(flag, target):\n"
            "    target(force=flag); target(force=flag)\n",
            path="mutant.py",
        )
        self.assertEqual(len(findings), 2)
        self.assertEqual(len({finding.key for finding in findings}), 2)
        with self.assertRaises(AssertionError):
            assert_mode_gate_registry_complete(
                findings,
                {findings[0].key: _AUDITED_MODE_SET},
            )

    def test_detector_rejects_mode_aliases(self):
        mutants = {
            "boolean alias": (
                "def mutant(force):\n"
                "    operator_import = force\n"
                "    if operator_import:\n"
                "        return 1\n"
            ),
            "literal alias": (
                "def mutant(job_type):\n"
                "    force_type = 'force_import'\n"
                "    if job_type == force_type:\n"
                "        return 1\n"
            ),
            "conditional overwrite": (
                "def mutant(force, condition):\n"
                "    alias = force\n"
                "    if condition:\n"
                "        alias = False\n"
                "    if alias:\n"
                "        return 1\n"
            ),
        }
        for label, source in mutants.items():
            with self.subTest(label=label):
                with self.assertRaisesRegex(AssertionError, "aliases are forbidden"):
                    mode_gates_in_source(source, path="mutant.py")

        findings = mode_gates_in_source(
            "def mutant(requeue_on_failure, target):\n"
            "    target(requeue=requeue_on_failure)\n",
            path="mutant.py",
        )
        self.assertEqual(len(findings), 1)

    def test_detector_recognizes_lifecycle_grants(self):
        mutants = {
            "literal requeue stop": (
                "def mutant(target):\n"
                "    target(requeue=False)\n"
            ),
            "automation-only requeue": (
                "def mutant(job, target):\n"
                "    target(requeue_to_wanted="
                "job.job_type == IMPORT_JOB_AUTOMATION)\n"
            ),
            "operator-stop assignment": (
                "def mutant(row):\n"
                "    operator_stop_status = (\n"
                "        'manual' if row.get('status') == 'manual' else None\n"
                "    )\n"
            ),
        }
        for label, source in mutants.items():
            with self.subTest(label=label):
                findings = mode_gates_in_source(source, path="mutant.py")
                expected = 2 if label == "automation-only requeue" else 1
                self.assertEqual(len(findings), expected)

    def test_detector_recognizes_match_case_mode_gate(self):
        findings = mode_gates_in_source(
            "def mutant(mode):\n"
            "    match mode:\n"
            "        case 'force_import' | 'auto_import':\n"
            "            return 1\n",
            path="mutant.py",
        )
        self.assertEqual(len(findings), 1)


if __name__ == "__main__":
    unittest.main()
