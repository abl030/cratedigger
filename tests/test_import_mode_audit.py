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
MODE_GATE_REGISTRY: dict[
    tuple[str, str, int, str, str],
    ModeGateAuthority,
] = {
    ("harness/import_one.py", "target_cleanup_decision", 476,
     "preserve_source", "4796e2c7a7d737b9"): _PRESERVE_FORCE_SOURCE,
    ("harness/import_one.py", "main", 1577,
     "args.force", "1ad02461ea8af4a2"): _DISTANCE_ONLY,
    ("harness/import_one.py", "main", 2127,
     "not keep_lossless and target_cleanup_decision(target_achieved, has_target, converted, preserve_source=args.preserve_source)",
     "76ba9b5f5116a1e8"): _PRESERVE_FORCE_SOURCE,
    ("harness/import_one.py", "main", 2001,
     "args.preserve_source and (not keep_lossless) and (converted > 0)",
     "54723607d5081ee1"): _PRESERVE_FORCE_SOURCE,
    ("harness/import_one.py", "main", 2020,
     "args.preserve_source and (not keep_lossless) and (converted > 0)",
     "54723607d5081ee1"): _PRESERVE_FORCE_SOURCE,
    ("harness/import_one.py", "main", 2129,
     "preserve_source=args.preserve_source", "833a2f1fee949dce"): _PRESERVE_FORCE_SOURCE,
    ("lib/dispatch/core.py", "dispatch_import_core", 129,
     "force", "ecd75225e973c696"): _AUDITED_MODE_SET,
    ("lib/dispatch/core.py", "dispatch_import_core", 427,
     "scenario not in FORCE_IMPORT_SCENARIOS", "77666a53867810de"): _AUDITED_MODE_SET,
    ("lib/dispatch/core.py", "dispatch_import_core", 274,
     "requeue_on_failure", "3992c39583766481"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/core.py", "dispatch_import_core", 460,
     "force=force", "ecd75225e973c696"): _DISTANCE_ONLY,
    ("lib/dispatch/core.py", "dispatch_import_core", 461,
     "preserve_source=scenario in FORCE_IMPORT_SCENARIOS", "f0bd2ce87c0656c5"): _PRESERVE_FORCE_SOURCE,
    ("lib/dispatch/core.py", "dispatch_import_core", 266,
     "requeue_to_wanted=requeue_on_failure", "3992c39583766481"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/core.py", "dispatch_import_core", 271,
     "message='Installed HAVE analysis failed; ' + ('request returned to wanted for a future retry' if requeue_on_failure else 'request lifecycle was preserved')",
     "578796b8e26d1267"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/core.py", "dispatch_import_core", 461,
     "scenario in FORCE_IMPORT_SCENARIOS", "f0bd2ce87c0656c5"): _PRESERVE_FORCE_SOURCE,
    ("lib/dispatch/core.py", "dispatch_import_core", 490,
     "requeue=requeue_on_failure", "3992c39583766481"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/core.py", "dispatch_import_core", 930,
     "requeue=requeue_on_failure", "3992c39583766481"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/core.py", "dispatch_import_core", 950,
     "requeue=requeue_on_failure", "3992c39583766481"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/core.py", "dispatch_import_core", 397,
     "requeue_on_failure=requeue_on_failure", "3992c39583766481"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/core.py", "dispatch_import_core", 756,
     "requeue=requeue_on_failure", "3992c39583766481"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/core.py", "dispatch_import_core", 824,
     "operator_stop_status=operator_stop_status", "a103aeeafcc220c8"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/core.py", "dispatch_import_core", 841,
     "operator_stop_status=operator_stop_status", "a103aeeafcc220c8"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
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
    ("lib/dispatch/outcome_actions.py", "_finalize_request_and_log_rejection", 385,
     "requeue_to_wanted and request_id is not None", "e2bfed59bcd70346"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/outcome_actions.py", "_record_have_analysis_error", 648,
     "requeue_to_wanted", "6779ab3f329c7d06"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/outcome_actions.py", "_record_preview_measurement_failed", 575,
     "requeue_to_wanted", "6779ab3f329c7d06"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/outcome_actions.py", "_record_have_analysis_error", 669,
     "requeue_to_wanted=requeue_to_wanted", "6779ab3f329c7d06"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/outcome_actions.py", "_record_have_analysis_error", 670,
     "record_validation_attempt=requeue_to_wanted", "6779ab3f329c7d06"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/outcome_actions.py", "_record_preview_measurement_failed", 574,
     "request_transition=transitions.RequestTransition.to_wanted() if requeue_to_wanted else None",
     "d3781a96bd337810"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/post_import.py", "_run_or_stage_quality_gate", 79,
     "pending is None and operator_stop_status is None", "2459cf69276fda4e"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/post_import.py", "_run_or_stage_quality_gate", 92,
     "operator_stop_status is not None and plan.transition.target_status == 'wanted'",
     "24fc6d9382e8b3a7"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/post_import.py", "_apply_post_import_search_action", 182,
     "operator_stop_status is not None", "8885d281623f057c"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/post_import.py", "_run_or_stage_quality_gate", 95,
     "operator_stop_status != 'manual'", "748f5ba45d08801c"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
    ("lib/dispatch/post_import.py", "_apply_post_import_search_action", 183,
     "operator_stop_status != 'manual'", "748f5ba45d08801c"): _OPERATOR_OWNS_SEARCH_SHORT_CIRCUIT,
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
    ("scripts/import_preview_worker.py", "_preview_input", 298,
     "job.job_type == IMPORT_JOB_FORCE", "e45cab5ab5498692"): _FORCE_ONLY_ACTIVE_MODE,
    ("scripts/import_preview_worker.py", "_preview_input", 306,
     "'force': True", "fc49e3ff31db631b"): _DISTANCE_ONLY,
    ("scripts/importer.py", "_force_job_wrong_match_payload", 67,
     "job.job_type != IMPORT_JOB_FORCE", "33db16a2e07e50e3"): _FORCE_ONLY_ACTIVE_MODE,
    ("scripts/importer.py", "execute_import_job", 177,
     "job.job_type == IMPORT_JOB_FORCE", "e45cab5ab5498692"): _FORCE_ONLY_ACTIVE_MODE,
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
    condition: ast.expr,
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
    if keyword == "requeue_on_failure":
        return isinstance(value, ast.Constant) and value.value is False
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
    *,
    mode_names: frozenset[str],
) -> bool:
    if isinstance(value, ast.Name):
        return value.id in mode_names
    if isinstance(value, ast.Attribute):
        return value.attr in mode_names
    if isinstance(value, ast.Constant) and isinstance(value.value, str):
        return value.value in _MODE_LITERALS
    if isinstance(value, ast.UnaryOp) and isinstance(value.op, ast.Not):
        return _is_simple_mode_alias(value.operand, mode_names=mode_names)
    return False


def _mode_aliases_by_scope(
    tree: ast.AST,
    parents: dict[ast.AST, ast.AST],
) -> dict[ast.AST, frozenset[str]]:
    """Propagate simple assignment aliases within one lexical function."""

    scopes = {
        _scope_owner(node, parents)
        for node in ast.walk(tree)
    }
    aliases: dict[ast.AST, frozenset[str]] = {}
    for scope in scopes:
        names = set(_MODE_NAMES)
        changed = True
        while changed:
            changed = False
            for node in ast.walk(scope):
                if _scope_owner(node, parents) is not scope:
                    continue
                targets: list[ast.AST] = []
                value: ast.expr | None = None
                if isinstance(node, ast.Assign):
                    targets = list(node.targets)
                    value = node.value
                elif isinstance(node, ast.AnnAssign):
                    targets = [node.target]
                    value = node.value
                elif isinstance(node, ast.NamedExpr):
                    targets = [node.target]
                    value = node.value
                if value is None or not _is_simple_mode_alias(
                    value,
                    mode_names=frozenset(names),
                ):
                    continue
                discovered = set().union(*(
                    _assigned_names(target) for target in targets
                ))
                if not discovered <= names:
                    names.update(discovered)
                    changed = True
        aliases[scope] = frozenset(names)
    return aliases


def mode_gates_in_source(source: str, *, path: str) -> list[ModeGate]:
    tree = ast.parse(source, filename=path)
    parents = {
        child: parent
        for parent in ast.walk(tree)
        for child in ast.iter_child_nodes(parent)
    }
    aliases_by_scope = _mode_aliases_by_scope(tree, parents)

    def names_for(node: ast.AST) -> frozenset[str]:
        return aliases_by_scope[_scope_owner(node, parents)]
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
        if (
            isinstance(node, ast.Compare)
            and _is_import_mode_condition(node, mode_names=names_for(node))
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
        elif (
            isinstance(node, ast.keyword)
            and node.arg is not None
            and _is_mode_grant(
                node.arg,
                node.value,
                mode_names=names_for(node),
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
                        mode_names=names_for(node),
                    )
                ):
                    conditions.append(
                        (node, value, f"{key.value!r}: {ast.unparse(value)}")
                    )

    findings: list[ModeGate] = []
    seen: set[tuple[int, str, str]] = set()
    for node, condition, presentation in conditions:
        mode_names = names_for(node)
        if not (
            _is_import_mode_condition(condition, mode_names=mode_names)
            or _is_mode_grant(
                presentation.partition("=")[0],
                condition,
                mode_names=mode_names,
            )
            or _is_mode_grant(
                presentation.split(":", 1)[0].strip("'"),
                condition,
                mode_names=mode_names,
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
            continue
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
            "    target(force=flag, preserve_source=flag)\n",
            path="mutant.py",
        )
        self.assertEqual(len(findings), 2)
        self.assertEqual(len({finding.key for finding in findings}), 2)
        with self.assertRaises(AssertionError):
            assert_mode_gate_registry_complete(
                findings,
                {findings[0].key: _AUDITED_MODE_SET},
            )

    def test_detector_propagates_simple_mode_aliases(self):
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
            "forwarded lifecycle": (
                "def mutant(requeue_on_failure, target):\n"
                "    target(requeue=requeue_on_failure)\n"
            ),
        }
        for label, source in mutants.items():
            with self.subTest(label=label):
                findings = mode_gates_in_source(source, path="mutant.py")
                self.assertEqual(len(findings), 1)


if __name__ == "__main__":
    unittest.main()
