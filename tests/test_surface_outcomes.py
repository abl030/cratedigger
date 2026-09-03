"""Convention + registry audit for CLI ⇄ API surface-outcome maps (#1278).

``lib/surface_outcomes.py`` owns the repository's HTTP-status → exit-code
convention. Services own exactly one outcome table — the outcome →
HTTP-status map — and derive their CLI exit-code map through the shared
convention, so the two surfaces agree branch for branch by construction.

The registry audit here patrols every registered outcome-map-owning
module: statuses stay inside the documented vocabulary, exit maps agree
with the convention, and a declared outcome ``Literal`` matches its map's
domain. The discovery sweep keeps the registry exact in both directions
across ``lib/*_service.py`` plus ``EXTRA_SWEPT_MODULES``, so a new
service-owned outcome map cannot ship unaudited. Route-side maps,
function-local dicts, and names outside the ``*_HTTP_STATUS`` /
``*_EXIT_CODE(S)`` grammar are outside this bounded sweep — the remaining
#1278 item-3 ladders live there until they move behind service tables.
"""

from __future__ import annotations

import importlib
import re
import unittest
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar, get_args

from lib.surface_outcomes import (
    KNOWN_HTTP_STATUSES,
    exit_code_for_http_status,
    exit_codes_from_http,
)

REPO_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Registry — hand-maintained data, kept exact by the discovery sweep below.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RegisteredOutcomeMaps:
    """One outcome-map-owning module.

    ``exit_attr`` is ``None`` for thin CLI HTTP adapters, whose exit code is
    derived from the live response status at relay time
    (``lib.surface_outcomes.exit_code_for_http_status`` via
    ``scripts/pipeline_cli/api_mutations.py``) rather than from an exported
    map. ``outcome_literal`` names the module's outcome ``Literal`` when one
    exists and its args are exactly the map's domain.
    """

    module: str
    http_attr: str
    exit_attr: str | None = None
    outcome_literal: str | None = None


#: Non-service modules that own outcome maps. The discovery sweep unions
#: these with the ``lib/*_service.py`` glob INDEPENDENTLY of the registry:
#: deriving the swept set from the registry would let deleting a
#: registration also delete its module from the sweep, silently opting the
#: module out of the "no unregistered map" direction (reader finding R3 on
#: the founding PR).
EXTRA_SWEPT_MODULES: tuple[str, ...] = ("lib.beets_tag_sync",)

REGISTRY: tuple[RegisteredOutcomeMaps, ...] = (
    RegisteredOutcomeMaps(
        "lib.incomplete_mark_service",
        "INCOMPLETE_MARK_HTTP_STATUS",
        "INCOMPLETE_MARK_EXIT_CODES",
        "IncompleteMarkOutcome",
    ),
    RegisteredOutcomeMaps(
        "lib.youtube_ingest_service",
        "OUTCOME_HTTP_STATUS",
        "OUTCOME_EXIT_CODE",
        "SubmitOutcome",
    ),
    RegisteredOutcomeMaps(
        "lib.youtube_album_service",
        "OUTCOME_HTTP_STATUS",
        "OUTCOME_EXIT_CODE",
    ),
    RegisteredOutcomeMaps(
        "lib.set_intent_service",
        "SET_INTENT_HTTP_STATUS",
        "SET_INTENT_EXIT_CODES",
        "SetIntentOutcome",
    ),
    RegisteredOutcomeMaps(
        "lib.convergence_service",
        "STOP_CONVERGED_SEARCH_HTTP_STATUS",
        "STOP_CONVERGED_SEARCH_EXIT_CODES",
        "StopConvergenceOutcome",
    ),
    # Search-plan is five actions whose outcome strings collide, so each
    # owns its own table (issue #1278 item 3); the module has no per-action
    # Literal — the per-action key sets are pinned in
    # tests/test_search_plan_service.py.
    RegisteredOutcomeMaps(
        "lib.search_plan_service",
        "SEARCH_PLAN_REGENERATE_HTTP_STATUS",
        "SEARCH_PLAN_REGENERATE_EXIT_CODES",
    ),
    RegisteredOutcomeMaps(
        "lib.search_plan_service",
        "SEARCH_PLAN_DRY_RUN_HTTP_STATUS",
        "SEARCH_PLAN_DRY_RUN_EXIT_CODES",
    ),
    RegisteredOutcomeMaps(
        "lib.search_plan_service",
        "SEARCH_PLAN_SATURATION_HTTP_STATUS",
        "SEARCH_PLAN_SATURATION_EXIT_CODES",
    ),
    RegisteredOutcomeMaps(
        "lib.search_plan_service",
        "SEARCH_PLAN_ADVANCE_HTTP_STATUS",
        "SEARCH_PLAN_ADVANCE_EXIT_CODES",
    ),
    RegisteredOutcomeMaps(
        "lib.search_plan_service",
        "SEARCH_PLAN_HISTORY_HTTP_STATUS",
        "SEARCH_PLAN_HISTORY_EXIT_CODES",
    ),
    RegisteredOutcomeMaps("lib.force_import_service", "FORCE_IMPORT_HTTP_STATUS"),
    RegisteredOutcomeMaps("lib.local_import_service", "LOCAL_IMPORT_HTTP_STATUS"),
    RegisteredOutcomeMaps("lib.merge_rekey_service", "MERGE_REKEY_HTTP_STATUS"),
    RegisteredOutcomeMaps("lib.beets_tag_sync", "TAG_SYNC_HTTP_STATUS"),
    # `integrity_failed` is a real outcome of `world_audit_outcome` but is
    # deliberately NOT a member of this map (see its docstring in
    # lib/world_audit_service.py) — exit 1 for an HTTP-200 outcome cannot
    # obey the ordinary status-derived convention this registry enforces,
    # mirroring the retag-divergence audit's own unregistered
    # `divergence_found` -> 1 case.
    RegisteredOutcomeMaps(
        "lib.world_audit_service",
        "WORLD_AUDIT_HTTP_STATUS",
        "WORLD_AUDIT_EXIT_CODES",
    ),
)


_MAP_ATTR_RE = re.compile(r"[A-Z0-9_]*(_HTTP_STATUS|_EXIT_CODES?)$")


def _outcome_map_attrs(module_name: str) -> set[str]:
    """Names of module-level str→int dicts spelled like outcome maps.

    Leading-underscore names are NOT skipped (mutant-runner finding E3 on
    the founding PR: a private rename dodged the sweep entirely). The
    grammar itself stays the bound — a dict named outside the
    ``*_HTTP_STATUS`` / ``*_EXIT_CODE(S)`` suffixes is invisible here, as
    the module docstring states.
    """
    module = importlib.import_module(module_name)
    found: set[str] = set()
    for name, value in vars(module).items():
        if not _MAP_ATTR_RE.fullmatch(name.lstrip("_")):
            continue
        if isinstance(value, dict) and all(
            isinstance(k, str) and isinstance(v, int) for k, v in value.items()
        ):
            found.add(name)
    return found


# ---------------------------------------------------------------------------
# Checker — module-level and accumulating, so every clause evaluates and the
# self-tests below can drive each clause directly.
#
# Honesty note (reader finding R4 on the founding PR): for a registry whose
# exit maps are all DERIVED via ``exit_codes_from_http``, the key-agreement
# and exit-convention clauses cannot fire through registered worlds — both
# sides of those comparisons move together. They are fail-closed
# legislation for a future hand-written exit map; the self-tests below
# prove each clause trips when driven directly, and the clauses with
# present-day teeth are the undocumented-status and outcome-``Literal``
# ones. The convention function itself is guarded by
# ``TestExitCodeForHttpStatus`` and the per-service value pins.
# ---------------------------------------------------------------------------


def outcome_map_violations(
    label: str,
    http_status: Mapping[str, int],
    exit_codes: Mapping[str, int] | None,
    outcomes: frozenset[str] | None,
) -> list[str]:
    violations: list[str] = []
    for outcome, status in sorted(http_status.items()):
        if status not in KNOWN_HTTP_STATUSES:
            violations.append(
                f"{label}: outcome {outcome!r} uses undocumented HTTP status {status}"
            )
    if exit_codes is not None:
        if set(exit_codes) != set(http_status):
            drift = sorted(set(exit_codes) ^ set(http_status))
            violations.append(
                f"{label}: exit-code map keys diverge from the HTTP map: {drift}"
            )
        for outcome, status in sorted(http_status.items()):
            if outcome not in exit_codes:
                continue
            expected = exit_code_for_http_status(status)
            if exit_codes[outcome] != expected:
                violations.append(
                    f"{label}: outcome {outcome!r} exit code "
                    f"{exit_codes[outcome]} breaks the {status}->{expected} "
                    "convention"
                )
    if outcomes is not None and set(http_status) != outcomes:
        drift = sorted(outcomes ^ set(http_status))
        violations.append(
            f"{label}: declared outcome vocabulary diverges from the HTTP map: "
            f"{drift}"
        )
    return violations


# ---------------------------------------------------------------------------
# Convention unit tests
# ---------------------------------------------------------------------------


class TestExitCodeForHttpStatus(unittest.TestCase):
    """The documented pairs: 2xx/0, 400/3, 404/2, 409/4, 422/3, 503/5."""

    CASES: ClassVar[list[tuple[str, int, int]]] = [
        ("success", 200, 0),
        ("accepted", 202, 0),
        ("edge of 2xx", 299, 0),
        ("just below the 2xx band buckets to transient", 199, 5),
        ("just above the 2xx band buckets to transient", 300, 5),
        ("input validation", 400, 3),
        ("not found", 404, 2),
        ("wrong state", 409, 4),
        ("semantic violation", 422, 3),
        ("transient", 503, 5),
        ("unmapped 4xx buckets to transient", 410, 5),
        ("server error buckets to transient", 500, 5),
    ]

    def test_default_table(self) -> None:
        for desc, status, expected in self.CASES:
            with self.subTest(desc=desc, status=status):
                self.assertEqual(exit_code_for_http_status(status), expected)

    def test_exit_overrides_pin_exact_statuses_only(self) -> None:
        self.assertEqual(exit_code_for_http_status(410, {410: 4}), 4)
        self.assertEqual(exit_code_for_http_status(404, {410: 4}), 2)

    def test_exit_overrides_take_precedence_over_the_default_table(self) -> None:
        # Mutant-runner finding A2 on the founding PR: both production
        # override dicts happen to use statuses the default table never
        # maps (410/500), so only this pin holds the documented contract —
        # an override wins even for an in-table status.
        self.assertEqual(exit_code_for_http_status(409, {409: 3}), 3)

    def test_known_statuses_are_the_documented_vocabulary(self) -> None:
        self.assertEqual(
            KNOWN_HTTP_STATUSES, frozenset({200, 202, 400, 404, 409, 422, 503})
        )


class TestExitCodesFromHttp(unittest.TestCase):
    def test_derives_branch_for_branch(self) -> None:
        derived = exit_codes_from_http(
            {"ok": 200, "missing": 404, "busy": 409, "bad": 422, "down": 503}
        )
        self.assertEqual(
            derived, {"ok": 0, "missing": 2, "busy": 4, "bad": 3, "down": 5}
        )

    def test_refuses_undocumented_status(self) -> None:
        with self.assertRaisesRegex(ValueError, r"'exploded'.*500"):
            exit_codes_from_http({"ok": 200, "exploded": 500})


# ---------------------------------------------------------------------------
# Registry audit
# ---------------------------------------------------------------------------


class TestRegisteredOutcomeMapsConform(unittest.TestCase):
    def test_every_registered_map_conforms(self) -> None:
        violations: list[str] = []
        for entry in REGISTRY:
            module = importlib.import_module(entry.module)
            http_status = getattr(module, entry.http_attr)
            exit_codes = (
                getattr(module, entry.exit_attr) if entry.exit_attr else None
            )
            outcomes = (
                frozenset(get_args(getattr(module, entry.outcome_literal)))
                if entry.outcome_literal
                else None
            )
            violations.extend(
                outcome_map_violations(
                    f"{entry.module}.{entry.http_attr}",
                    http_status,
                    exit_codes,
                    outcomes,
                )
            )
        if violations:
            self.fail("\n".join(violations))

    def test_registry_matches_discovered_maps_exactly(self) -> None:
        """Both directions fail closed: no unregistered map, no stale entry.

        The swept set never derives from the registry (see
        ``EXTRA_SWEPT_MODULES``), and every registered module must sit
        inside it — otherwise a registration outside the sweep would make
        the stale-entry direction unfalsifiable for that module.
        """
        service_modules = {
            f"lib.{path.stem}"
            for path in (REPO_ROOT / "lib").glob("*_service.py")
        }
        swept = sorted(service_modules | set(EXTRA_SWEPT_MODULES))
        registered_modules = {entry.module for entry in REGISTRY}
        self.assertLessEqual(
            registered_modules,
            set(swept),
            "registered module outside the discovery sweep — add it to "
            "EXTRA_SWEPT_MODULES so both audit directions can see it",
        )
        discovered = {
            f"{module_name}.{attr}"
            for module_name in swept
            for attr in _outcome_map_attrs(module_name)
        }
        registered: set[str] = set()
        for entry in REGISTRY:
            registered.add(f"{entry.module}.{entry.http_attr}")
            if entry.exit_attr:
                registered.add(f"{entry.module}.{entry.exit_attr}")
        self.assertEqual(
            discovered,
            registered,
            "outcome maps drifted from the registry: unregistered maps "
            f"{sorted(discovered - registered)}, stale registrations "
            f"{sorted(registered - discovered)}",
        )


# ---------------------------------------------------------------------------
# Known-bad self-tests — one world per checker clause, each tripping exactly
# its own clause (the checker accumulates, so single-violation worlds prove
# every other clause passes on the same world).
# ---------------------------------------------------------------------------


class TestOutcomeMapCheckerTripsOnViolations(unittest.TestCase):
    def test_undocumented_status_trips(self) -> None:
        violations = outcome_map_violations(
            "w", {"ok": 200, "boom": 500}, None, None
        )
        self.assertEqual(len(violations), 1)
        self.assertIn("undocumented HTTP status 500", violations[0])

    def test_key_drift_trips(self) -> None:
        violations = outcome_map_violations(
            "w", {"ok": 200}, {"ok": 0, "extra": 2}, None
        )
        self.assertEqual(len(violations), 1)
        self.assertIn("exit-code map keys diverge", violations[0])
        self.assertIn("extra", violations[0])

    def test_convention_break_trips(self) -> None:
        violations = outcome_map_violations(
            "w", {"ok": 200, "missing": 404}, {"ok": 0, "missing": 1}, None
        )
        self.assertEqual(len(violations), 1)
        self.assertIn("breaks the 404->2 convention", violations[0])

    def test_literal_drift_trips(self) -> None:
        violations = outcome_map_violations(
            "w", {"ok": 200}, None, frozenset({"ok", "ghost"})
        )
        self.assertEqual(len(violations), 1)
        self.assertIn("declared outcome vocabulary diverges", violations[0])
        self.assertIn("ghost", violations[0])

    def test_clean_world_is_silent(self) -> None:
        self.assertEqual(
            outcome_map_violations(
                "w", {"ok": 200, "missing": 404}, {"ok": 0, "missing": 2},
                frozenset({"ok", "missing"}),
            ),
            [],
        )


if __name__ == "__main__":
    unittest.main()
