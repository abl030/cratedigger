"""Structural ratchet: every ``album_requests`` UPDATE freezes replacements."""

from __future__ import annotations

import ast
import hashlib
import re
import unittest
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_ROOTS = ("lib", "scripts", "web")
_SCOPE_NODES = (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)
_SQL_IDENT = r'(?:"[^"]+"|[a-z_][\w$]*)'
_UPDATE_ALBUM_REQUESTS = re.compile(
    rf"\bupdate\s+(?:{_SQL_IDENT}\s*\.\s*)?"
    r'(?:"album_requests"|album_requests)(?=\s|$)',
    re.IGNORECASE,
)
_GUARDED_WRITE_METHODS = frozenset({
    "record_field_resolution",
    "update_request_fields",
    "update_track_artists",
})
_GUARDED_RESULT_HANDLERS = frozenset({
    "_request_fields_applied_or_respond",
    "request_fields_cas_conflict",
})


@dataclass(frozen=True)
class _SqlFinding:
    line: int
    sql: str
    fingerprint: str
    category: str = "unguarded"
    scope: str | None = None
    exact_source_status_cas: bool = False
    canonical_params: bool = False
    direct_static_sql: bool = False
    album_request_update: bool = False


@dataclass(frozen=True)
class _AlbumRequestUpdate:
    guarded: bool
    sets_status: bool
    exact_source_status_cas: bool
    invalid_status_cas: bool = False
    canonical_params: bool = False
    direct_static_sql: bool = False


# Every unresolved production SQL builder is an exact, reviewed exception.
# The key is (path, fingerprint of the exact execute SQL/params AST); the
# value holds one rationale per matching call site, and the audit requires
# the live distinct-call-site count to equal the rationale count exactly —
# so inserting code above a reviewed statement no longer churns every entry
# below it (#1258 item 4), while adding or removing a same-fingerprint call
# still fails loudly. Rationales are kept in ascending source-line order as
# a maintenance convention; the audit checks only the counts, not per-site
# attribution. Dynamic calls bind the whole enclosing scope. Reviewed
# lifecycle calls go further: their fingerprint also binds the normalized
# enclosing method AST, path, and method name, so any same-method
# control-flow or binding change invalidates review even when the execute
# call itself is byte-identical. Known residual (#1258 review F6): a STATIC
# SQL constant outside a lifecycle seam fingerprints SQL+params only, with
# no scope binding — two such calls in different functions of one file
# would share a key and become interchangeable to the count check. No
# registered entry has that shape today (all multi-site keys are dynamic,
# single-scope); if one ever registers, revisit this key design. The
# ratchet does not infer parameter dataflow: transition SQL must use the
# canonical direct call grammar below.
_REVIEWED_DYNAMIC_SQL_CALLS: dict[tuple[str, str], tuple[str, ...]] = {
    ("lib/pipeline_db/_core.py", "472331a54ebaf9a6"): (
        (
            "shared execute wrapper forwards caller-owned SQL with the caller's "
            "unchanged positional or mapping parameters"
        ),
        (
            "single reconnect retry forwards the same caller-owned SQL and "
            "unchanged positional or mapping parameters outside atomic scopes"
        ),
    ),
    ("lib/pipeline_db/_core.py", "04ca6be85bb75a81"): (
        (
            "shared execute wrapper forwards parameterless caller-owned SQL"
        ),
        (
            "single reconnect retry forwards the same parameterless caller-owned "
            "SQL outside atomic scopes"
        ),
    ),
    ("lib/pipeline_db/_core.py", "cc41f306ff6d2fc6"): (
        (
            "owner-session liveness probe interpolates only a positive integer "
            "statement timeout before a fixed SELECT"
        ),
    ),
    ("scripts/pipeline_cli/query.py", "f1e566c44edc8feb"): (
        (
            "deliberate raw-write operator seam, gated by exact "
            "--write --confirm WRITE rather than a lifecycle-safe typed mutation"
        ),
        (
            "default raw-query seam runs in the transaction-enforced read-only "
            "scope on the live connection"
        ),
    ),
    ("lib/pipeline_db/terminal_outcomes.py", "5fa18d2c1737583f"): (
        (
            "terminal metadata keys use the existing validated request-field "
            "vocabulary (issue #784: `dumps=lambda value: msgspec.json.encode("
            "value).decode()` replaced with the shared `_msgspec_json_dumps` "
            "helper from `_shared.py` — same encoder, same output, no SQL change)"
        ),
    ),
    ("lib/pipeline_db/terminal_outcomes.py", "acecdeb5d9931e7f"): (
        (
            "terminal attempt kind is restricted to the fixed retry-counter "
            "vocabulary. Issue #1278 item 7 re-keyed this "
            "(cd644e51f3670265 -> acecdeb5d9931e7f) for the same exponent clamp "
            "described on requests.py's twin above: "
            "``POWER(2, LEAST(COALESCE(col, 0), %s))`` with the bound "
            "``SEARCH_BACKOFF_MAX_EXPONENT``, value-identical below the "
            "double-precision overflow point and unchanged in guard, "
            "placeholder discipline, and counter vocabulary"
        ),
    ),
    ("lib/pipeline_db/dashboard.py", "6a6227ef5b3e4640"): (
        (
            "issue #1348: the cycle-metrics INSERT interpolates one "
            "module-level constant, ``_CYCLE_METRIC_COLUMNS`` -- four literal "
            "column names plus ``lib.cycle_counters.COUNTER_NAMES``, which is "
            "``CycleCounters``'s own dataclass field names -- and a ``%s`` "
            "placeholder list of exactly that length. Both slots are fixed at "
            "import from Python identifiers; no caller value, request field or "
            "runtime string can reach the statement text, and every value is "
            "still a bound parameter. The target is ``cycle_metrics``, a "
            "telemetry table with no request lifecycle column, so this write "
            "cannot touch album_requests whatever the column list says. The "
            "rendered statement is pinned verbatim in "
            "tests/test_pipeline_db_column_contract.py::"
            "TestCycleMetricsInsertStatement, and the column names are proven "
            "to be real ``cycle_metrics`` columns by the CycleCounters "
            "contract in the same file"
        ),
    ),
    ("lib/pipeline_db/download_log.py", "95d18a3931276ae1"): (
        (
            "get_log's three outcome variants were three verbatim copies of one "
            "SELECT; issue #829 Phase 5 PR4 collapsed them to a single template "
            "whose two slots are the shared candidate-evidence column block and "
            "an outcome filter drawn from a closed literal map. Issue #962 adds "
            "only read-only exact-release identity projections; no slot takes "
            "caller input or mutates album_requests. Issue #1022 projects a "
            "shared row's current-only lineage to NULL on this read surface. "
            "Issue #1176 PR3's ``local_import`` Literal member above shifted "
            "this line only — fingerprint unchanged, confirming the SQL itself "
            "is untouched. Issue #1278 item 7 DID move this fingerprint "
            "(cba0d6d56f1878ac -> 95d18a3931276ae1) without touching a "
            "character of the SQL: a non-constant SQL argument fingerprints "
            "over DYNAMIC_SCOPE, the whole enclosing function's AST, and "
            "``get_log``'s body now calls the module-level "
            "``overlay_evidence_onto_download_log_row`` where it used to call "
            "``self._overlay_evidence_onto_download_log_row``. Same template, "
            "same two closed slots, still read-only over album_requests"
        ),
    ),
    ("lib/pipeline_db/download_log.py", "2d2cba8bbf4b379f"): (
        (
            "validation key is selected from a closed server-owned vocabulary "
            "(#867 intentionally added terminal/evidence projection and moved final "
            "classification after same-path DISTINCT). Issue #829 PR4/N3 CHANGED "
            "this statement: two read-only column blocks generated by "
            "``accusation_evidence_columns`` from a closed (table, prefix) pair, "
            "plus a LEFT JOIN on the request's own current_evidence_id — no "
            "caller input reaches the SQL and album_requests is still only read. "
            "Issue #1077 F2 CHANGED this statement again: dropped the "
            "``e.audio_corrupt AS candidate_audio_corrupt`` projection — an "
            "incidental fact about linked evidence used to hide an otherwise-kept, "
            "visible row, which the removal fixes; still read-only, no new caller "
            "input. Issue #1176 PR1's ``log_download`` ``source`` parameter and "
            "PR3's ``local_import`` Literal member above each shifted this line "
            "only — fingerprint unchanged, confirming the SQL itself is untouched"
        ),
    ),
    ("lib/pipeline_db/download_log.py", "e0154e89026dc8ef"): (
        (
            "validation key is selected from a closed server-owned vocabulary "
            "(issue #835, issue #829 PR4, the source-semantic proof gate, issue "
            "#1077 F2's column removal, issue #1176 PR1's ``log_download`` "
            "``source`` parameter, and PR3's ``local_import`` Literal member "
            "above each shifted this line only)"
        ),
    ),
    ("lib/pipeline_db/download_log.py", "13517e08e7db52f3"): (
        (
            "validation key is closed vocabulary and IN list is value placeholders "
            "(issue #835, issue #829 PR4, the source-semantic proof gate, issue "
            "#1077 F2's column removal, issue #1176 PR1's ``log_download`` "
            "``source`` parameter, and PR3's ``local_import`` Literal member "
            "above each shifted this line only)"
        ),
    ),
    ("lib/pipeline_db/download_log.py", "d87a36ba1d1768e7"): (
        (
            "JSON path key is selected from a closed server-owned vocabulary "
            "(issue #835, issue #829 PR4, the source-semantic proof gate, issue "
            "#1077 F2's column removal, issue #1176 PR1's ``log_download`` "
            "``source`` parameter, and PR3's ``local_import`` Literal member "
            "above each shifted this line only)"
        ),
    ),
    ("lib/pipeline_db/import_jobs.py", "ecf3d1844c67f653"): (
        (
            "optional job filter is a fixed literal WHERE clause "
            "(issue #1089's automation_recovery_debris import, review round 2's "
            "_default_force_action_copy_path helper, and review round 3's "
            "RecoveryDebrisReport import, above each shifted this line only — "
            "fingerprint unchanged, confirming the SQL itself is untouched)"
        ),
    ),
    ("lib/pipeline_db/import_jobs.py", "d020bd0235c95c4a"): (
        (
            "claim exclusion predicate is assembled from fixed literal clauses "
            "(issue #1089's automation_recovery_debris import, review round 2's "
            "_default_force_action_copy_path helper, and review round 3's "
            "RecoveryDebrisReport import, above each shifted this line only — "
            "fingerprint unchanged, confirming the SQL itself is untouched)"
        ),
    ),
    # Issue #1313 candidate 1 collapsed the eight hand-written claim
    # statements (four job routes x two lanes) onto three lane-taking
    # implementations, the same closed-literal-template shape download_log's
    # get_log entry above already carries. The single interpolated slot in
    # each is ``_claim_assignments_sql(lane)``, rendered entirely from the
    # frozen column names on ``lib/import_job_lane.py``'s two ``JobLane``
    # values; the worker id is the only bound value it emits, no slot takes
    # caller input, and every statement updates ``import_jobs`` alone. The
    # rendered SET clause is SEMANTICALLY identical to the literals it
    # replaced — same columns, same assignments, same single placeholder —
    # but NOT byte-identical: the three preview request-scoped/automation
    # claims previously wrapped ``COALESCE(preview_started_at, NOW())``
    # across four lines and the renderer emits it on one.
    # ``tests/test_import_job_lane.py::TestClaimAssignmentsSql`` pins the
    # rendered text per lane against a literal in the test; nothing compares
    # it to the pre-#1313 statements, which no longer exist.
    ("lib/pipeline_db/import_jobs.py", "8680520d87d8ab14"): (
        (
            "unguarded YouTube claim: the lane renders the SET clause; the "
            "WHERE gate binds job id, job type and the lane's entry "
            "preview_status as parameters"
        ),
    ),
    ("lib/pipeline_db/import_jobs.py", "ac3ef11e608f7018"): (
        (
            "request-scoped force/local claim: the lane renders the SET "
            "clause inside an already-locked transaction whose request and "
            "job guards are both fixed literal WHERE clauses"
        ),
    ),
    ("lib/pipeline_db/import_jobs.py", "06aa673d447c4aab"): (
        (
            "automation owner claim: the lane renders the SET clause and the "
            "execution-lease stamp is a fixed literal fragment identical in "
            "both lanes"
        ),
    ),
    ("lib/pipeline_db/misc.py", "12cfdd83a367c90e"): (
        (
            "track-count batch IN list contains only psycopg value placeholders"
        ),
    ),
    ("lib/beets_db.py", "4b59d19eb8727dff"): (
        (
            "issue #1203 item 2: get_current_album_directories's batch IN list "
            "contains only sqlite3 '?' value placeholders (album_ids, a list of "
            "int primary keys already resolved by _matching_album_ids). This is "
            "the deployment-owned Beets SQLite items table, not the pipeline DB "
            "-- album_requests is not reachable from this connection at all, "
            "and the query is a read-only SELECT"
        ),
    ),
    ("lib/pipeline_db/misc.py", "0a14fd5e6252e398"): (
        (
            "bulk VALUES fragment contains only fixed value-placeholder tuples "
            "(issue #784: add_denylist/get_denylisted_users annotated above, "
            "shifting this line; no SQL change)"
        ),
    ),
    ("lib/pipeline_db/misc.py", "07ec7dc8e19f1ee0"): (
        (
            "triage joins and predicates are selected from closed service enums "
            "(issue #978 uses a fixed request-local LATERAL convergence function "
            "for the converged cohort; all values remain parameters and "
            "album_requests is still only read)"
        ),
    ),
    ("lib/pipeline_db/requests.py", "b84b3af3ecbbf089"): (
        (
            "INSERT columns derive from the fixed AddRequestInput schema "
            "and values remain one placeholder per validated schema field"
        ),
    ),
    ("lib/pipeline_db/requests.py", "ead47926ac19037a"): (
        (
            "request-by-id uses the fixed shared presentation projection and one "
            "value placeholder"
        ),
    ),
    ("lib/pipeline_db/requests.py", "5d62850ba552ff76"): (
        (
            "cardinality-preserving library candidate lookup composes only the "
            "fixed presentation and capture/evidence projections with two value "
            "array parameters, then filters strict identities in Python; the "
            "Library contract no longer selects structured CD proof while the "
            "pointed current-evidence release-id gate remains for exact "
            "verified/provisional facts. Issue #1176 PR1 round 1 retired the "
            "dead ``job_type = 'manual_import'`` arm from the embedded "
            "``_CAPTURE_AND_EVIDENCE_SELECT`` predicate (zero live import_jobs "
            "rows ever carried it; migration 080 also drops it from the "
            "job_type CHECK); round 2 added ``'local_import'`` to the same list "
            "(a successful local import is a capture too, decided in review) — "
            "net line position unchanged from before PR1. Neither edit touches "
            "the interpolation site itself, so the fingerprint (which "
            "normalizes the ``{dynamic}`` slot) is unaffected by either change"
        ),
    ),
    ("lib/pipeline_db/requests.py", "fc57192d01989af4"): (
        (
            "MusicBrainz request lookup uses the fixed shared presentation "
            "projection and one value placeholder"
        ),
    ),
    ("lib/pipeline_db/requests.py", "0ad0e7484937cd31"): (
        (
            "Discogs request lookup uses the fixed shared presentation projection "
            "and one value placeholder"
        ),
    ),
    ("lib/pipeline_db/requests.py", "327e39bd024d50d3"): (
        (
            "replacement-chain lookup uses the fixed shared presentation "
            "projection and one value placeholder"
        ),
    ),
    ("lib/pipeline_db/requests.py", "1bd5cbde29149322"): (
        (
            "release-id lookup selects one of two fixed identity predicates "
            "and uses the fixed shared presentation projection"
        ),
    ),
    ("lib/pipeline_db/requests.py", "d1da142f4a1a30a8"): (
        (
            "non-replaced listing uses the fixed shared presentation projection "
            "with one static lifecycle predicate"
        ),
    ),
    ("lib/pipeline_db/requests.py", "bc05e500065af93a"): (
        (
            "metadata keys are validated identifiers, lifecycle fields are reserved, "
            "values use one typed JSONB record parameter, and the exact active "
            "source plus absent processing owner are guarded"
        ),
    ),
    ("lib/pipeline_db/requests.py", "943205ae40bba7e6"): (
        (
            "metadata keys are validated identifiers, lifecycle fields are reserved, "
            "values use one typed JSONB record parameter, and any processing owner "
            "causes the guarded update to report a conflict"
        ),
    ),
    ("lib/pipeline_db/requests.py", "890d0f2e35ffd73c"): (
        (
            "optional LIMIT is normalized through int before interpolation "
            "and the base wanted query is static"
        ),
    ),
    ("lib/pipeline_db/requests.py", "bf514491f423d3be"): (
        (
            "ORDER is selected from two literals and LIMIT remains a value placeholder "
            "over the fixed shared presentation projection"
        ),
    ),
    ("lib/pipeline_db/requests.py", "93f3043b99b3ec7c"): (
        (
            "request search composes only one fixed optional status predicate over "
            "the fixed presentation projection and value placeholders"
        ),
    ),
    ("lib/pipeline_db/requests.py", "724128efb25b8439"): (
        (
            "artist request lookup uses the fixed presentation and capture/evidence "
            "projections with a static UUID-aware fallback predicate; the Library "
            "contract no longer selects structured CD proof while the pointed "
            "current-evidence release-id gate remains for exact verified/provisional "
            "facts. Issue #1176 PR1 round 1 retired the dead "
            "``job_type = 'manual_import'`` arm from the embedded "
            "``_CAPTURE_AND_EVIDENCE_SELECT`` predicate; round 2 added "
            "``'local_import'`` to the same list — the interpolation site is "
            "unchanged throughout, so the fingerprint is unaffected by either "
            "change"
        ),
    ),
    ("lib/pipeline_db/requests.py", "f59ded429883f2ec"): (
        (
            "artist-name fallback uses the fixed presentation and capture/evidence "
            "projections with one escaped value placeholder; the Library contract "
            "no longer selects structured CD proof while the pointed current-evidence "
            "release-id gate remains for exact verified/provisional facts. Issue "
            "#1176 PR1 round 1 retired the dead ``job_type = 'manual_import'`` arm "
            "from the embedded ``_CAPTURE_AND_EVIDENCE_SELECT`` predicate; round 2 "
            "added ``'local_import'`` to the same list — the interpolation site is "
            "unchanged throughout, so the fingerprint is unaffected by either change"
        ),
    ),
    ("lib/pipeline_db/requests.py", "1cbaa87e89b44252"): (
        (
            "attempt kind is validated against the fixed retry-counter vocabulary "
            "and every value remains a direct placeholder; an attached processing "
            "owner makes the compare-and-set a zero-write conflict. Issue #1278 "
            "item 7 re-keyed this (fdbd2821ab3cbb5a -> 1cbaa87e89b44252) for a "
            "real SQL change: the doubling exponent is now clamped by a bound "
            "``SEARCH_BACKOFF_MAX_EXPONENT`` placeholder "
            "(``POWER(2, LEAST(COALESCE(col, 0), %s))``) because PostgreSQL "
            "resolves POWER to double precision and the whole "
            "``30 * POWER(2, counter)`` product raised "
            "``value out of range: overflow`` once a counter reached 1020 "
            "(measured live 2026-08-31; 1020 is base-dependent — bare "
            "POWER(2, n) survives to 1023). The clamp changes no value below "
            "that point — the surrounding LEAST already capped every exponent "
            "past 3 — and the WHERE guard, the placeholder discipline, and "
            "the counter vocabulary are untouched"
        ),
    ),
    ("lib/pipeline_db/terminal_outcomes.py", "c4d426397b1774f9"): (
        (
            "processing-terminal metadata keys use the validated request-field "
            "vocabulary while exact request and owner predicates retain authority; "
            "the static owner-clearing status CAS remains the final request write "
            "(review #2: identity moved because the enclosing method now reads "
            "retry-counter policy from the canonical VALID_TRANSITIONS table "
            "instead of zeroing counters inline — no SQL-shape change here. Issue "
            "#1176 PR1 round 2 added a ``WHEN job_type = 'local_import'`` arm to "
            "``_insert_terminal_download_audit``'s unrelated download_log-facing "
            "CASE, earlier in this same file — that statement never mentions "
            "album_requests and is outside this audit's scope, but it shifted "
            "this line only. Issue #1278 item 7 moved this identity again "
            "(ebb50341a8d836f6 -> c4d426397b1774f9) with no SQL-shape change: "
            "this scope's own retry-backoff arithmetic now calls the shared "
            "``decisions.search_backoff_minutes`` instead of restating "
            "``min(BASE * 2 ** prior, MAX)`` inline, and a lifecycle-bound "
            "fingerprint hashes the whole enclosing method AST)"
        ),
    ),
}


# Status changes are a narrower boundary than ordinary guarded metadata.  Each
# approved call below must live in a typed lifecycle/Replace seam and perform
# an exact compare-and-set against the source status.  Exact keys are populated
# beside the implementation they review; movement or SQL-shape drift fails the
# ratchet just like the dynamic-SQL exceptions above.
_REVIEWED_STATUS_SQL_CALLS: dict[tuple[str, str], tuple[str, ...]] = {
    ("lib/pipeline_db/convergence.py", "0b7d6e3ed7b568c1"): (
        (
            "explicit operator stop atomically compares the complete opaque "
            "request-local signal token and rechecks current-evidence authority "
            "against the target row version after any lock wait while CASing "
            "wanted to the reversible unsearchable state"
        ),
    ),
    ("lib/pipeline_db/import_jobs.py", "71e0271f65123747"): (
        (
            "atomic download-to-processing handoff CASes the immutable download "
            "witness while installing the exact automation owner "
            "(issue #1089's automation_recovery_debris import, review round 2's "
            "_default_force_action_copy_path helper, and review round 3's "
            "RecoveryDebrisReport import, above each shifted this line only — "
            "fingerprint unchanged, confirming the SQL itself is untouched)"
        ),
    ),
    ("lib/pipeline_db/terminal_outcomes.py", "56802c71d0fd3622"): (
        (
            "atomic terminal transition mirrors typed wanted CAS inside one transaction"
        ),
    ),
    ("lib/pipeline_db/terminal_outcomes.py", "249bfbdab2b02ac4"): (
        (
            "atomic preview recovery accepts only downloading as its exact source"
        ),
    ),
    ("lib/pipeline_db/terminal_outcomes.py", "4f0561c784e817e9"): (
        (
            "atomic terminal import CASes status with rescue audit in the same transaction"
        ),
    ),
    ("lib/pipeline_db/terminal_outcomes.py", "9b11fb540dfe44e3"): (
        (
            "atomic terminal typed transition CASes the source status selected by the DAG"
        ),
    ),
    ("lib/pipeline_db/terminal_outcomes.py", "93a08ead2aed7cf8"): (
        (
            "automation terminalization performs the final exact processing-owner "
            "CAS and clears the owner in the same static request write "
            "(review #2: retry counters are now policy-derived placeholders read "
            "from the canonical VALID_TRANSITIONS table rather than unconditional "
            "zeros, so `processing -> wanted` retains them and automatic backoff "
            "keeps growing; the exact `status = 'processing' AND "
            "active_automation_import_job_id = %s` predicate and the "
            "owner-clearing final write are unchanged. Issue #1176 PR1 round 2's "
            "``local_import`` CASE arm in ``_insert_terminal_download_audit``, "
            "earlier in this file, shifted this line only — that statement is "
            "download_log-facing and never mentions album_requests. Issue "
            "#1278 item 7 moved this identity again "
            "(6674811fa5453c86 -> 93a08ead2aed7cf8) with no SQL-shape change, "
            "for the same reason as its dynamic sibling above: this scope's "
            "retry-backoff arithmetic now calls the shared "
            "``decisions.search_backoff_minutes``, and a lifecycle-bound "
            "fingerprint binds the whole enclosing method AST)"
        ),
    ),
    ("lib/pipeline_db/requests.py", "a2f3083f8cbe8885"): (
        (
            "Replace holds the row lock and CASes the captured active source status "
            "only when no processing owner exists"
        ),
    ),
    ("lib/pipeline_db/requests.py", "b74f9eb518948ae5"): (
        (
            "operator idempotence uses a no-op CAS against the observed status "
            "and refuses an active processing owner"
        ),
    ),
    ("lib/pipeline_db/requests.py", "94c8caa29b5f3093"): (
        (
            "ordinary typed transitions CAS the source status selected by the DAG "
            "and refuse an active processing owner"
        ),
    ),
    ("lib/pipeline_db/requests.py", "cd2c8644115e82f6"): (
        (
            "typed imported transition CASes status with rescue audit atomically "
            "and refuses an active processing owner"
        ),
    ),
    ("lib/pipeline_db/requests.py", "745b1dc37147f0f5"): (
        (
            "typed reset-to-wanted transition CASes its captured source status; "
            "the Bad Rip priority timestamp is a static CASE update in the same CAS, "
            "and an active processing owner is refused"
        ),
    ),
    ("lib/pipeline_db/requests.py", "3490139cad98e85e"): (
        (
            "automatic recovery accepts only downloading as its exact source "
            "without widening processing authority"
        ),
    ),
    ("lib/pipeline_db/requests.py", "a0853139ff6dd9ad"): (
        (
            "typed download claim accepts only the explicit wanted source status "
            "and installs one immutable active download state"
        ),
    ),
    ("lib/pipeline_db/requests.py", "34dd9d8beb763829"): (
        (
            "plan-aware download claim uses an exact wanted source predicate "
            "plus exact persisted-plan witnesses"
        ),
    ),
}


_STATUS_MUTATING_SEAMS = frozenset({
    "_mark_request_replaced",
    "update_status",
    "mark_imported_with_rescue",
    "reset_to_wanted",
    "reset_downloading_to_wanted",
    "set_downloading",
    "set_downloading_if_plan_current",
})


def _enclosing_scope(
    node: ast.AST,
    parents: dict[ast.AST, ast.AST],
) -> ast.AST:
    current = node
    while not isinstance(current, _SCOPE_NODES):
        current = parents[current]
    return current


def _simple_assignments(
    tree: ast.Module,
    call: ast.Call,
    parents: dict[ast.AST, ast.AST],
) -> dict[str, tuple[ast.expr, ...]]:
    """Resolve simple module/local SQL constants visible at ``call``."""
    call_scope = _enclosing_scope(call, parents)
    values: dict[str, tuple[ast.expr, ...]] = {}
    module_candidates: list[tuple[int, str, ast.expr]] = []
    local_candidates: list[tuple[int, str, ast.expr]] = []
    for node in ast.walk(tree):
        target: ast.expr | None = None
        value: ast.expr | None = None
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target, value = node.targets[0], node.value
        elif isinstance(node, ast.AnnAssign):
            target, value = node.target, node.value
        elif (
            isinstance(node, ast.AugAssign)
            and isinstance(node.target, ast.Name)
        ):
            # Retain the mutation as a reaching definition.  Referring back
            # to the same name deliberately resolves as partial/unknown;
            # ``sql = 'SELECT 1'; sql += dynamic`` must not be mistaken for
            # the original static SELECT.
            target = node.target
            value = ast.BinOp(
                left=ast.Name(id=node.target.id, ctx=ast.Load()),
                op=node.op,
                right=node.value,
            )
        if not isinstance(target, ast.Name) or value is None:
            continue
        scope = _enclosing_scope(node, parents)
        line = getattr(node, "lineno", 0)
        if scope is tree:
            module_candidates.append((line, target.id, value))
        elif scope is call_scope and line < call.lineno:
            local_candidates.append((line, target.id, value))
    for candidates in (module_candidates, local_candidates):
        grouped: dict[str, list[ast.expr]] = {}
        for _, name, value in sorted(candidates):
            grouped.setdefault(name, []).append(value)
        for name, definitions in grouped.items():
            # A local assignment shadows a module constant, but every local
            # reaching definition remains possible. This is deliberately
            # branch-conservative: an unguarded initial SQL value cannot be
            # hidden by a guarded assignment in only one branch.
            values[name] = tuple(definitions)
    return values


def _sql_variants(
    node: ast.expr,
    values: dict[str, tuple[ast.expr, ...]],
    resolving: frozenset[str] = frozenset(),
) -> tuple[set[str], bool]:
    """Return conservative SQL strings plus whether any part is unresolved."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return {node.value}, False
    if isinstance(node, (ast.Name, ast.Attribute)):
        if isinstance(node, ast.Attribute) and not (
            isinstance(node.value, ast.Name)
            and node.value.id in {"self", "cls"}
        ):
            return set(), True
        name = node.id if isinstance(node, ast.Name) else node.attr
        if name in resolving or name not in values:
            return set(), True
        variants: set[str] = set()
        unresolved = False
        for definition in values[name]:
            found, partial = _sql_variants(
                definition, values, resolving | {name},
            )
            variants.update(found)
            unresolved = unresolved or partial
        return variants, unresolved
    if isinstance(node, ast.JoinedStr):
        variants = {""}
        unresolved = False
        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                fragments = {value.value}
            elif isinstance(value, ast.FormattedValue):
                fragments, partial = _sql_variants(
                    value.value, values, resolving,
                )
                if not fragments:
                    fragments = {"{dynamic}"}
                unresolved = unresolved or partial
            else:
                fragments = {"{dynamic}"}
                unresolved = True
            variants = {
                prefix + fragment
                for prefix in variants
                for fragment in fragments
            }
        return variants, unresolved
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, left_partial = _sql_variants(node.left, values, resolving)
        right, right_partial = _sql_variants(node.right, values, resolving)
        if not left:
            left = {"{dynamic}"}
        if not right:
            right = {"{dynamic}"}
        return (
            {a + b for a in left for b in right},
            left_partial or right_partial,
        )
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "format"
    ):
        templates, partial = _sql_variants(node.func.value, values, resolving)
        if not templates:
            return set(), True
        return {
            re.sub(r"\{[^{}]*\}", "{dynamic}", template)
            for template in templates
        }, True
    return set(), True


def _is_execute_forwarder(node: ast.expr, scope: ast.AST) -> bool:
    """The DB primitive forwards SQL; its production callers are audited."""
    return (
        isinstance(node, ast.Name)
        and isinstance(scope, (ast.FunctionDef, ast.AsyncFunctionDef))
        and scope.name == "_execute"
        and any(arg.arg == node.id for arg in scope.args.args)
    )


def _name_is_file_read(
    node: ast.expr,
    values: dict[str, tuple[ast.expr, ...]],
) -> bool:
    if not isinstance(node, ast.Name):
        return False
    definitions = values.get(node.id, ())
    return bool(definitions) and all(
        isinstance(definition, ast.Call)
        and isinstance(definition.func, ast.Attribute)
        and definition.func.attr == "read"
        for definition in definitions
    )


def _sql_argument(node: ast.Call) -> ast.expr | None:
    """Return an execute call's SQL expression, positional or keyword."""
    if node.args:
        return node.args[0]
    for keyword in node.keywords:
        if keyword.arg in {"sql", "query"}:
            return keyword.value
    return None


def _execute_params_argument(node: ast.Call) -> ast.expr | None:
    """Return an execute call's value-parameter expression, if present."""
    if len(node.args) >= 2:
        return node.args[1]
    for keyword in node.keywords:
        if keyword.arg in {"params", "parameters", "vars"}:
            return keyword.value
    return None


_ACTIVE_REQUEST_STATUSES = frozenset({
    "wanted", "downloading", "processing", "imported", "unsearchable",
})


def _direct_execute_params(node: ast.Call) -> tuple[ast.expr, ...] | None:
    """Accept only an unstarred tuple/list literal at the execute call."""
    argument = _execute_params_argument(node)
    if argument is None:
        return ()
    if not isinstance(argument, (ast.Tuple, ast.List)):
        return None
    if any(isinstance(element, ast.Starred) for element in argument.elts):
        return None
    return tuple(argument.elts)


def _walk_same_scope(scope: ast.AST) -> list[ast.AST]:
    """Walk one function/module body without entering nested scopes."""
    found: list[ast.AST] = []
    pending = list(ast.iter_child_nodes(scope))
    while pending:
        node = pending.pop()
        found.append(node)
        if isinstance(node, _SCOPE_NODES):
            continue
        pending.extend(ast.iter_child_nodes(node))
    return found


def _canonical_source_status(node: ast.expr, scope: ast.AST) -> bool:
    """Accept an active literal or an untouched source-status argument."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value in _ACTIVE_REQUEST_STATUSES
    if not (
        isinstance(node, ast.Name)
        and node.id in {"expected_status", "source_status"}
        and isinstance(scope, (ast.FunctionDef, ast.AsyncFunctionDef))
    ):
        return False
    argument_names = {
        argument.arg
        for argument in (
            *scope.args.posonlyargs,
            *scope.args.args,
            *scope.args.kwonlyargs,
        )
    }
    if node.id not in argument_names:
        return False
    return not any(
        isinstance(candidate, ast.Name)
        and candidate.id == node.id
        and isinstance(candidate.ctx, ast.Store)
        for candidate in _walk_same_scope(scope)
    )


def _placeholder_binds_canonical_source(
    tokens: list[tuple[str, int]],
    placeholder_index: int,
    direct_params: tuple[ast.expr, ...] | None,
    scope: ast.AST,
) -> bool:
    """Map a placeholder position to the direct call-site argument."""
    if direct_params is None:
        return False
    ordinal = sum(
        token == "%s" for token, _ in tokens[:placeholder_index + 1]
    ) - 1
    if ordinal < 0 or ordinal >= len(direct_params):
        return False
    return _canonical_source_status(direct_params[ordinal], scope)


def _strip_sql_comments(sql: str) -> str:
    """Remove SQL comments without treating comment text as a predicate."""
    output: list[str] = []
    index = 0
    quote: str | None = None
    while index < len(sql):
        char = sql[index]
        following = sql[index + 1] if index + 1 < len(sql) else ""
        if quote is not None:
            output.append(char)
            if char == quote:
                if following == quote:
                    output.append(following)
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        if char in {"'", '"'}:
            quote = char
            output.append(char)
            index += 1
            continue
        if char == "-" and following == "-":
            index += 2
            while index < len(sql) and sql[index] not in "\r\n":
                index += 1
            output.append(" ")
            continue
        if char == "/" and following == "*":
            index += 2
            while index + 1 < len(sql):
                if sql[index] == "*" and sql[index + 1] == "/":
                    index += 2
                    break
                index += 1
            output.append(" ")
            continue
        output.append(char)
        index += 1
    return "".join(output)


_SQL_TOKEN = re.compile(
    r"'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|<>|!=|%s|"
    r"[a-z_][\w$]*|\{dynamic\}|[().,;=*{}]",
    re.IGNORECASE,
)


def _sql_tokens(sql: str) -> list[tuple[str, int]]:
    """Tokenise enough PostgreSQL UPDATE syntax to correlate its guard."""
    tokens: list[tuple[str, int]] = []
    depth = 0
    for match in _SQL_TOKEN.finditer(_strip_sql_comments(sql)):
        token = match.group(0)
        if token == ")":
            depth = max(0, depth - 1)
        tokens.append((token, depth))
        if token == "(":
            depth += 1
    return tokens


def _identifier(token: str) -> str | None:
    if token.startswith("'"):
        return None
    if token.startswith('"') and token.endswith('"'):
        return token[1:-1].replace('""', '"').lower()
    if re.fullmatch(r"[a-z_][\w$]*", token, re.IGNORECASE):
        return token.lower()
    return None


def _target_status_predicates(
    tokens: list[tuple[str, int]],
    *,
    start: int,
    end: int,
    depth: int,
    target_alias: str,
    target_name: str,
    direct_params: tuple[ast.expr, ...] | None,
    scope: ast.AST,
) -> tuple[bool, bool, bool]:
    """Return terminal guard, exact-source CAS, and invalid status CAS."""
    active_values = {
        "'wanted'",
        "'downloading'",
        "'processing'",
        "'imported'",
        "'unsearchable'",
    }
    if any(
        token_depth == depth and token.lower() == "or"
        for token, token_depth in tokens[start:end]
    ):
        # A top-level OR can make a textual target guard non-constraining:
        # ``status != 'replaced' OR status = 'replaced'``.
        return False, False, True
    terminal_guard = False
    exact_source_status_cas = False
    invalid_status_cas = False
    index = start
    while index < end:
        token, token_depth = tokens[index]
        if token_depth != depth:
            index += 1
            continue
        left_end = index
        left_is_target = False
        if (
            _identifier(token) == "status"
            and not (
                index > start
                and tokens[index - 1] == (".", depth)
            )
        ):
            left_is_target = True
        elif index + 2 < end:
            qualifier = _identifier(token)
            dot, dot_depth = tokens[index + 1]
            column, column_depth = tokens[index + 2]
            if (
                qualifier in {target_alias, target_name}
                and dot == "."
                and dot_depth == depth
                and _identifier(column) == "status"
                and column_depth == depth
            ):
                left_is_target = True
                left_end = index + 2
        if not left_is_target or left_end + 2 >= end:
            index += 1
            continue
        operator, operator_depth = tokens[left_end + 1]
        value, value_depth = tokens[left_end + 2]
        normalized_value = value.lower()
        if operator_depth == depth and value_depth == depth:
            if operator in {"!=", "<>"} and normalized_value == "'replaced'":
                terminal_guard = True
            if operator == "=":
                if normalized_value in active_values:
                    exact_source_status_cas = True
                elif normalized_value == "%s":
                    if _placeholder_binds_canonical_source(
                        tokens,
                        left_end + 2,
                        direct_params,
                        scope,
                    ):
                        exact_source_status_cas = True
                    else:
                        invalid_status_cas = True
                else:
                    invalid_status_cas = True
        index = left_end + 1
    return terminal_guard, exact_source_status_cas, invalid_status_cas


def _set_clause_assigns_status(
    tokens: list[tuple[str, int]],
    *,
    start: int,
    end: int,
    depth: int,
) -> bool:
    """Recognise a target-column ``status = ...`` assignment in SET."""
    at_assignment_start = True
    index = start
    while index < end:
        token, token_depth = tokens[index]
        if token_depth != depth:
            index += 1
            continue
        if token == ",":
            at_assignment_start = True
            index += 1
            continue
        if at_assignment_start:
            if (
                _identifier(token) == "status"
                and index + 1 < end
                and tokens[index + 1] == ("=", depth)
            ):
                return True
            at_assignment_start = False
        index += 1
    return False


def _album_request_update_details(
    sql: str,
    direct_params: tuple[ast.expr, ...] | None = (),
    scope: ast.AST | None = None,
    *,
    canonical_params: bool = True,
    direct_static_sql: bool = True,
) -> list[_AlbumRequestUpdate]:
    """Describe guards and status mutation for every targeted UPDATE."""
    if scope is None:
        scope = ast.Module(body=[], type_ignores=[])
    tokens = _sql_tokens(sql)
    results: list[_AlbumRequestUpdate] = []
    for update_index, (token, depth) in enumerate(tokens):
        if token.lower() != "update":
            continue
        cursor = update_index + 1
        if cursor < len(tokens) and tokens[cursor][0].lower() == "only":
            cursor += 1
        target_parts: list[str] = []
        identifier = (
            _identifier(tokens[cursor][0])
            if cursor < len(tokens) and tokens[cursor][1] == depth
            else None
        )
        if identifier is None:
            continue
        target_parts.append(identifier)
        cursor += 1
        while (
            cursor + 1 < len(tokens)
            and tokens[cursor] == (".", depth)
            and tokens[cursor + 1][1] == depth
            and _identifier(tokens[cursor + 1][0]) is not None
        ):
            target_parts.append(_identifier(tokens[cursor + 1][0]) or "")
            cursor += 2
        target_name = target_parts[-1]
        if target_name != "album_requests":
            continue

        target_alias = target_name
        if cursor < len(tokens) and tokens[cursor][1] == depth:
            if tokens[cursor][0].lower() == "as":
                cursor += 1
                if cursor < len(tokens):
                    target_alias = _identifier(tokens[cursor][0]) or target_name
                    cursor += 1
            elif tokens[cursor][0].lower() != "set":
                possible_alias = _identifier(tokens[cursor][0])
                if possible_alias is not None:
                    target_alias = possible_alias
                    cursor += 1

        set_index = next((
            index for index in range(cursor, len(tokens))
            if tokens[index][1] == depth and tokens[index][0].lower() == "set"
        ), None)
        if set_index is None:
            results.append(_AlbumRequestUpdate(False, False, False))
            continue
        statement_end = next((
            index for index in range(set_index + 1, len(tokens))
            if (
                tokens[index][1] < depth
                or (tokens[index][1] == depth and tokens[index][0] == ";")
            )
        ), len(tokens))
        where_index = next((
            index for index in range(set_index + 1, statement_end)
            if tokens[index][1] == depth and tokens[index][0].lower() == "where"
        ), None)
        sets_status = _set_clause_assigns_status(
            tokens,
            start=set_index + 1,
            end=where_index if where_index is not None else statement_end,
            depth=depth,
        )
        terminal_guard = False
        exact_source_status_cas = False
        invalid_status_cas = False
        if where_index is not None:
            terminal_guard, exact_source_status_cas, invalid_status_cas = (
                _target_status_predicates(
                tokens,
                start=where_index + 1,
                end=statement_end,
                    depth=depth,
                    target_alias=target_alias,
                    target_name=target_name,
                    direct_params=direct_params,
                    scope=scope,
                )
            )
        results.append(_AlbumRequestUpdate(
            guarded=(terminal_guard or exact_source_status_cas)
            and not invalid_status_cas,
            sets_status=sets_status,
            exact_source_status_cas=exact_source_status_cas,
            invalid_status_cas=invalid_status_cas,
            canonical_params=canonical_params,
            direct_static_sql=direct_static_sql,
        ))
    return results


def _album_request_update_guards(sql: str) -> list[bool]:
    return [detail.guarded for detail in _album_request_update_details(sql)]


def _expression_mentions_status_assignment(
    sql_argument: ast.expr,
    values: dict[str, tuple[ast.expr, ...]],
) -> bool:
    """Find status SET fragments in the SQL expression's reaching defs."""
    pending = list(ast.walk(sql_argument))
    seen: set[str] = set()
    while pending:
        node = pending.pop()
        if (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
        ):
            lowered = node.value.lower()
            for match in re.finditer(r"\bstatus\s*=", lowered):
                before = lowered[:match.start()]
                last_set = before.rfind("set")
                last_where = before.rfind("where")
                if last_set > last_where or (
                    last_set == -1 and last_where == -1
                ):
                    return True
        name = (
            node.id if isinstance(node, ast.Name)
            else node.attr if isinstance(node, ast.Attribute)
            else None
        )
        if name is None or name in seen or name not in values:
            continue
        seen.add(name)
        for definition in values[name]:
            pending.extend(ast.walk(definition))
    return False


def _sql_call_fingerprint(
    sql_argument: ast.expr,
    params_argument: ast.expr | None,
    scope: ast.AST,
    *,
    source_path: str,
    bind_lifecycle_scope: bool,
) -> str:
    """Hash a reviewed SQL seam at the strongest applicable boundary."""
    parts = [
        "SQL",
        ast.dump(sql_argument, include_attributes=False),
        "PARAMS",
        (
            ast.dump(params_argument, include_attributes=False)
            if params_argument is not None
            else "<none>"
        ),
    ]
    if bind_lifecycle_scope:
        scope_name = (
            scope.name
            if isinstance(scope, (ast.FunctionDef, ast.AsyncFunctionDef))
            else "<module>"
        )
        parts.extend((
            "LIFECYCLE_PATH",
            source_path,
            "LIFECYCLE_SCOPE_NAME",
            scope_name,
            "LIFECYCLE_SCOPE_AST",
            ast.dump(scope, include_attributes=False),
        ))
    elif not (
        isinstance(sql_argument, ast.Constant)
        and isinstance(sql_argument.value, str)
    ):
        parts.extend((
            "DYNAMIC_SCOPE",
            ast.dump(scope, include_attributes=False),
        ))
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()[:16]


def _unguarded_album_request_update_findings(
    source: str,
    *,
    source_path: str = "<memory>",
) -> list[_SqlFinding]:
    tree = ast.parse(source)
    parents = {
        child: parent
        for parent in ast.walk(tree)
        for child in ast.iter_child_nodes(parent)
    }
    offending: list[_SqlFinding] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = (
            node.func.attr
            if isinstance(node.func, ast.Attribute)
            else node.func.id
            if isinstance(node.func, ast.Name)
            else None
        )
        if name not in {"execute", "_execute"}:
            continue
        sql_argument = _sql_argument(node)
        if sql_argument is None:
            continue
        params_argument = _execute_params_argument(node)
        values = _simple_assignments(tree, node, parents)
        direct_params = _direct_execute_params(node)
        variants, unresolved = _sql_variants(sql_argument, values)
        scope = _enclosing_scope(node, parents)
        scope_name = (
            scope.name
            if isinstance(scope, (ast.FunctionDef, ast.AsyncFunctionDef))
            else None
        )
        expression_sets_status = _expression_mentions_status_assignment(
            sql_argument,
            values,
        )
        fingerprint = _sql_call_fingerprint(
            sql_argument,
            params_argument,
            scope,
            source_path=source_path,
            bind_lifecycle_scope=(
                expression_sets_status
                or scope_name in _STATUS_MUTATING_SEAMS
            ),
        )
        canonical_params = direct_params is not None
        direct_static_sql = (
            isinstance(sql_argument, ast.Constant)
            and isinstance(sql_argument.value, str)
        )
        if not variants:
            if (
                _is_execute_forwarder(sql_argument, scope)
                or _name_is_file_read(sql_argument, values)
            ):
                continue
            offending.append(_SqlFinding(
                node.lineno,
                "<unresolved dynamic SQL capable of updating album_requests>",
                fingerprint,
                category=(
                    "status_dynamic"
                    if expression_sets_status
                    or scope_name in _STATUS_MUTATING_SEAMS
                    else "dynamic"
                ),
                scope=scope_name,
                canonical_params=canonical_params,
                direct_static_sql=direct_static_sql,
                album_request_update=False,
            ))
            continue
        for sql in sorted(variants):
            normalized = " ".join(sql.lower().split())
            details = _album_request_update_details(
                sql,
                direct_params,
                scope,
                canonical_params=canonical_params,
                direct_static_sql=direct_static_sql,
            )
            sets_status = bool(details) and (
                expression_sets_status
                or any(detail.sets_status for detail in details)
            )
            exact_source_status_cas = bool(details) and all(
                detail.exact_source_status_cas
                for detail in details
                if detail.sets_status or sets_status
            )
            if details and not canonical_params:
                offending.append(_SqlFinding(
                    node.lineno,
                    normalized,
                    fingerprint,
                    category="status" if sets_status else "noncanonical",
                    scope=scope_name,
                    exact_source_status_cas=False,
                    canonical_params=False,
                    direct_static_sql=direct_static_sql,
                    album_request_update=True,
                ))
                continue
            if details and not direct_static_sql and not unresolved:
                offending.append(_SqlFinding(
                    node.lineno,
                    normalized,
                    fingerprint,
                    category="status" if sets_status else "noncanonical",
                    scope=scope_name,
                    exact_source_status_cas=exact_source_status_cas,
                    canonical_params=canonical_params,
                    direct_static_sql=False,
                    album_request_update=True,
                ))
                continue
            if details and not all(detail.guarded for detail in details):
                offending.append(_SqlFinding(
                    node.lineno,
                    normalized,
                    fingerprint,
                    category="status" if sets_status else "unguarded",
                    scope=scope_name,
                    exact_source_status_cas=exact_source_status_cas,
                    canonical_params=canonical_params,
                    direct_static_sql=direct_static_sql,
                    album_request_update=True,
                ))
                continue
            if unresolved:
                # Fail closed on every unresolved fragment. A suffix can add
                # another statement or weaken an earlier WHERE guard; its
                # location in the partial string does not make it safe.
                offending.append(_SqlFinding(
                    node.lineno,
                    normalized,
                    fingerprint,
                    category=(
                        "status_dynamic"
                        if sets_status or scope_name in _STATUS_MUTATING_SEAMS
                        else "dynamic"
                    ),
                    scope=scope_name,
                    exact_source_status_cas=exact_source_status_cas,
                    canonical_params=canonical_params,
                    direct_static_sql=direct_static_sql,
                    album_request_update=bool(details),
                ))
                continue
            if sets_status:
                offending.append(_SqlFinding(
                    node.lineno,
                    normalized,
                    fingerprint,
                    category="status",
                    scope=scope_name,
                    exact_source_status_cas=exact_source_status_cas,
                    canonical_params=canonical_params,
                    direct_static_sql=direct_static_sql,
                    album_request_update=True,
                ))
                continue
            if details:
                continue
            if _UPDATE_ALBUM_REQUESTS.search(_strip_sql_comments(normalized)):
                # The lexical matcher saw the target but the bounded parser
                # could not prove which UPDATE owns it.
                offending.append(_SqlFinding(
                    node.lineno,
                    normalized,
                    fingerprint,
                    scope=scope_name,
                    canonical_params=canonical_params,
                    direct_static_sql=direct_static_sql,
                    album_request_update=True,
                ))
    return offending


def _unguarded_album_request_updates(source: str) -> list[tuple[int, str]]:
    return [
        (finding.line, finding.sql)
        for finding in _unguarded_album_request_update_findings(source)
    ]


def _guarded_result_controls_handling(
    node: ast.Call,
    parents: dict[ast.AST, ast.AST],
) -> bool:
    """Prove a guarded result reaches a condition, assertion, or return."""

    def reaches_control(current: ast.AST) -> bool:
        while current in parents and not isinstance(current, ast.stmt):
            parent = parents[current]
            if isinstance(parent, ast.Call):
                handler_name = (
                    parent.func.attr
                    if isinstance(parent.func, ast.Attribute)
                    else parent.func.id
                    if isinstance(parent.func, ast.Name)
                    else None
                )
                if handler_name not in _GUARDED_RESULT_HANDLERS:
                    return False
            if isinstance(parent, ast.Return):
                return True
            if isinstance(parent, ast.Assert) and current is parent.test:
                return True
            if isinstance(parent, (ast.If, ast.While)) and current is parent.test:
                return any(
                    not isinstance(statement, ast.Pass)
                    for statement in (*parent.body, *parent.orelse)
                )
            current = parent
        return False

    current: ast.AST = node
    if reaches_control(current):
        return True
    while current in parents and not isinstance(current, ast.stmt):
        current = parents[current]

    statement = current
    target: ast.Name | None = None
    if (
        isinstance(statement, ast.Assign)
        and len(statement.targets) == 1
        and isinstance(statement.targets[0], ast.Name)
    ):
        target = statement.targets[0]
    elif isinstance(statement, ast.AnnAssign) and isinstance(
        statement.target, ast.Name,
    ):
        target = statement.target
    if target is None:
        return False

    scope = _enclosing_scope(node, parents)
    assigned_line = getattr(statement, "lineno", 0)
    candidate_uses = [
        use
        for use in ast.walk(scope)
        if (
            isinstance(use, ast.Name)
            and isinstance(use.ctx, ast.Load)
            and use.id == target.id
            and use.lineno > assigned_line
            and _enclosing_scope(use, parents) is scope
        )
    ]
    stores = [
        store
        for store in ast.walk(scope)
        if (
            isinstance(store, ast.Name)
            and isinstance(store.ctx, ast.Store)
            and store.id == target.id
            and store.lineno > assigned_line
            and _enclosing_scope(store, parents) is scope
        )
    ]
    for use in candidate_uses:
        if any(assigned_line < store.lineno < use.lineno for store in stores):
            continue
        if reaches_control(use):
            return True
    return False


def _ignored_guarded_write_results(source: str) -> list[tuple[int, str]]:
    """Find guarded writes without proven success/conflict handling."""
    tree = ast.parse(source)
    parents = {
        child: parent
        for parent in ast.walk(tree)
        for child in ast.iter_child_nodes(parent)
    }
    ignored: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in _GUARDED_WRITE_METHODS
        ):
            continue
        if (
            node.func.attr == "record_field_resolution"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "deferred"
        ):
            # `_DeferredRecorder` is an in-memory queue whose append always
            # succeeds; only its later `pdb.record_field_resolution` flush is
            # a guarded database write.
            continue
        if not _guarded_result_controls_handling(node, parents):
            ignored.append((node.lineno, node.func.attr))
    return ignored


def _render_sites(sites: set[tuple[str, int, str]]) -> str:
    """Render distinct call sites grouped per registry key, for diagnostics.

    The registry key dropped the line component (#1258 item 4), so the
    exact-and-live failure message names the live source lines here instead.
    """
    grouped: dict[tuple[str, str], list[int]] = {}
    for rel, line, fingerprint in sites:
        grouped.setdefault((rel, fingerprint), []).append(line)
    return "\n".join(
        f"  {rel}:{fingerprint}: lines {sorted(lines)}"
        for (rel, fingerprint), lines in sorted(grouped.items())
    )


class TestReplacedWriteAudit(unittest.TestCase):
    def test_every_album_request_update_has_terminal_or_exact_status_guard(self):
        offending: list[str] = []
        reviewed_dynamic: Counter[tuple[str, str]] = Counter()
        reviewed_status: Counter[tuple[str, str]] = Counter()
        # The finder emits one finding per dynamic slot, so a single call
        # site can appear several times with one (line, fingerprint). Count
        # distinct call SITES against the registries — the line identifies a
        # site within this scan; only the registry key omits it.
        dynamic_sites: set[tuple[str, int, str]] = set()
        status_sites: set[tuple[str, int, str]] = set()
        for root_name in PRODUCTION_ROOTS:
            for path in sorted((REPO_ROOT / root_name).rglob("*.py")):
                rel = path.relative_to(REPO_ROOT).as_posix()
                for finding in _unguarded_album_request_update_findings(
                    path.read_text(encoding="utf-8"),
                    source_path=rel,
                ):
                    key = (rel, finding.fingerprint)
                    site = (rel, finding.line, finding.fingerprint)
                    if finding.category.startswith("status"):
                        if (
                            not finding.exact_source_status_cas
                            or not finding.canonical_params
                            or not finding.direct_static_sql
                            or key not in _REVIEWED_STATUS_SQL_CALLS
                        ):
                            offending.append(
                                f"{rel}:{finding.line}:{finding.fingerprint}: "
                                f"{finding.category}:{finding.scope}: "
                                f"exact_source_cas="
                                f"{finding.exact_source_status_cas}: "
                                f"canonical_params="
                                f"{finding.canonical_params}: "
                                f"direct_static_sql="
                                f"{finding.direct_static_sql}: "
                                f"{finding.sql}"
                            )
                            continue
                        if site not in status_sites:
                            status_sites.add(site)
                            reviewed_status[key] += 1
                        if finding.category == "status_dynamic":
                            if key not in _REVIEWED_DYNAMIC_SQL_CALLS:
                                offending.append(
                                    f"{rel}:{finding.line}:"
                                    f"{finding.fingerprint}: status builder "
                                    "also lacks dynamic-SQL review"
                                )
                                continue
                            if site not in dynamic_sites:
                                dynamic_sites.add(site)
                                reviewed_dynamic[key] += 1
                        continue
                    if key in _REVIEWED_DYNAMIC_SQL_CALLS and (
                        not finding.album_request_update
                        or finding.canonical_params
                    ):
                        if site not in dynamic_sites:
                            dynamic_sites.add(site)
                            reviewed_dynamic[key] += 1
                        continue
                    offending.append(
                        f"{rel}:{finding.line}:{finding.fingerprint}: "
                        f"{finding.sql}"
                    )
        self.assertEqual(
            offending,
            [],
            "Every album_requests UPDATE must prove the row is not replaced; "
            "status mutation additionally requires an approved typed seam "
            "with exact source-status CAS. Offending writes:\n"
            + "\n".join(offending),
        )
        self.assertEqual(
            dict(reviewed_dynamic),
            {
                key: len(rationales)
                for key, rationales in _REVIEWED_DYNAMIC_SQL_CALLS.items()
            },
            "Reviewed dynamic-SQL exceptions must remain exact and live: "
            "one rationale per live call site.\nLive call sites:\n"
            + _render_sites(dynamic_sites),
        )
        self.assertEqual(
            dict(reviewed_status),
            {
                key: len(rationales)
                for key, rationales in _REVIEWED_STATUS_SQL_CALLS.items()
            },
            "Reviewed status-transition SQL calls must remain exact and "
            "live: one rationale per live call site.\nLive call sites:\n"
            + _render_sites(status_sites),
        )

    def test_reviewed_dynamic_sql_rationales_are_nonempty(self):
        self.assertTrue(_REVIEWED_DYNAMIC_SQL_CALLS)
        for key, rationales in _REVIEWED_DYNAMIC_SQL_CALLS.items():
            self.assertTrue(rationales, f"empty rationale tuple for {key}")
            for rationale in rationales:
                self.assertTrue(
                    rationale.strip(), f"missing rationale for {key}")

    def test_reviewed_status_sql_rationales_are_nonempty(self):
        self.assertTrue(_REVIEWED_STATUS_SQL_CALLS)
        for key, rationales in _REVIEWED_STATUS_SQL_CALLS.items():
            self.assertTrue(rationales, f"empty rationale tuple for {key}")
            for rationale in rationales:
                self.assertTrue(
                    rationale.strip(), f"missing rationale for {key}")

    def test_every_guarded_write_result_is_consumed(self):
        ignored: list[str] = []
        for root_name in PRODUCTION_ROOTS:
            for path in sorted((REPO_ROOT / root_name).rglob("*.py")):
                rel = path.relative_to(REPO_ROOT).as_posix()
                for line, method in _ignored_guarded_write_results(
                    path.read_text(encoding="utf-8"),
                ):
                    ignored.append(f"{rel}:{line}: {method}")
        self.assertEqual(
            ignored,
            [],
            "Guarded writer results must be handled explicitly:\n"
            + "\n".join(ignored),
        )

    def test_known_bad_ignored_guarded_write_is_rejected(self):
        source = '''
def thaw(db, request_id):
    db.update_request_fields(request_id, release_group_year=1999)
'''
        self.assertEqual(len(_ignored_guarded_write_results(source)), 1)

    def test_assigned_but_unchecked_guarded_write_is_rejected(self):
        source = '''
def thaw(db, request_id):
    applied = db.update_request_fields(request_id, release_group_year=1999)
    log(applied)
'''
        self.assertEqual(len(_ignored_guarded_write_results(source)), 1)

    def test_tuple_assigned_guarded_write_is_rejected(self):
        source = '''
def thaw(db, request_id):
    applied, label = (
        db.update_request_fields(request_id, release_group_year=1999),
        "metadata",
    )
'''
        self.assertEqual(len(_ignored_guarded_write_results(source)), 1)

    def test_unchecked_walrus_guarded_write_is_rejected(self):
        source = '''
def thaw(db, request_id):
    (applied := db.update_request_fields(
        request_id, release_group_year=1999,
    ))
'''
        self.assertEqual(len(_ignored_guarded_write_results(source)), 1)

    def test_checked_assignment_and_returned_result_are_accepted(self):
        source = '''
def guarded(db, request_id):
    applied = db.update_request_fields(request_id, release_group_year=1999)
    if not applied:
        return conflict()
    return True

def forwarded(db, request_id):
    return db.update_request_fields(request_id, release_group_year=1999)
'''
        self.assertEqual(_ignored_guarded_write_results(source), [])

    def test_result_hidden_in_arbitrary_condition_call_is_rejected(self):
        source = '''
def thaw(db, request_id):
    applied = db.update_request_fields(request_id, release_group_year=1999)
    if log_and_return_true(applied):
        return True
'''
        self.assertEqual(len(_ignored_guarded_write_results(source)), 1)

    def test_pass_only_condition_is_not_conflict_handling(self):
        source = '''
def thaw(db, request_id):
    applied = db.update_request_fields(request_id, release_group_year=1999)
    if applied:
        pass
'''
        self.assertEqual(len(_ignored_guarded_write_results(source)), 1)

    def test_known_bad_unguarded_update_is_rejected(self):
        source = """
def thaw(cur, request_id):
    cur.execute(
        \"UPDATE album_requests SET current_evidence_id = %s WHERE id = %s\",
        (9, request_id),
    )
"""
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_status_assignment_does_not_masquerade_as_where_guard(self):
        source = """
def thaw(cur, request_id):
    cur.execute(
        "UPDATE album_requests SET status = %s WHERE id = %s",
        ("wanted", request_id),
    )
"""
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_status_assignment_with_only_terminal_guard_is_rejected(self):
        source = """
def thaw(cur, request_id):
    cur.execute(
        "UPDATE album_requests SET status = 'unsearchable' "
        "WHERE id = %s AND status != 'replaced'",
        (request_id,),
    )
"""
        findings = _unguarded_album_request_update_findings(source)
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].category, "status")
        self.assertFalse(findings[0].exact_source_status_cas)

    def test_unapproved_status_assignment_with_exact_cas_is_rejected(self):
        source = """
def thaw(cur, request_id, source_status):
    cur.execute(
        "UPDATE album_requests SET status = 'unsearchable' "
        "WHERE id = %s AND status = %s",
        (request_id, source_status),
    )
"""
        findings = _unguarded_album_request_update_findings(source)
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].category, "status")
        self.assertTrue(findings[0].exact_source_status_cas)

    def test_status_cas_bound_to_target_status_is_not_exact_source(self):
        source = """
def thaw(cur, request_id, source_status, target_status):
    cur.execute(
        "UPDATE album_requests SET status = %s "
        "WHERE id = %s AND status = %s AND status != 'replaced'",
        (target_status, request_id, target_status),
    )
"""
        findings = _unguarded_album_request_update_findings(source)
        self.assertEqual(len(findings), 1)
        self.assertFalse(findings[0].exact_source_status_cas)

    def test_swapped_status_cas_parameters_are_not_exact_source(self):
        source = """
def thaw(cur, request_id, source_status, target_status):
    cur.execute(
        "UPDATE album_requests SET status = %s "
        "WHERE id = %s AND status = %s AND status != 'replaced'",
        (target_status, source_status, request_id),
    )
"""
        findings = _unguarded_album_request_update_findings(source)
        self.assertEqual(len(findings), 1)
        self.assertFalse(findings[0].exact_source_status_cas)

    def test_dynamic_status_cas_parameters_are_not_exact_source(self):
        source = """
def thaw(cur, request_id, source_status, target_status):
    cur.execute(
        "UPDATE album_requests SET status = %s "
        "WHERE id = %s AND status = %s AND status != 'replaced'",
        build_params(target_status, request_id, source_status),
    )
"""
        findings = _unguarded_album_request_update_findings(source)
        self.assertEqual(len(findings), 1)
        self.assertFalse(findings[0].exact_source_status_cas)

    def test_valid_source_status_binding_is_exact_source(self):
        source = """
def thaw(cur, request_id, source_status, target_status):
    cur.execute(
        "UPDATE album_requests SET status = %s "
        "WHERE id = %s AND status = %s AND status != 'replaced'",
        (target_status, request_id, source_status),
    )
"""
        findings = _unguarded_album_request_update_findings(source)
        self.assertEqual(len(findings), 1)
        self.assertTrue(findings[0].exact_source_status_cas)

    def test_source_named_binding_derived_from_target_is_not_exact(self):
        source = """
def thaw(cur, request_id, target_status):
    source_status = target_status
    cur.execute(
        "UPDATE album_requests SET status = %s "
        "WHERE id = %s AND status = %s AND status != 'replaced'",
        (target_status, request_id, source_status),
    )
"""
        findings = _unguarded_album_request_update_findings(source)
        self.assertEqual(len(findings), 1)
        self.assertFalse(findings[0].exact_source_status_cas)

    def test_status_seam_fingerprint_includes_parameter_bindings(self):
        source_binding = """
def thaw(cur, request_id, source_status, target_status):
    cur.execute(
        "UPDATE album_requests SET status = %s "
        "WHERE id = %s AND status = %s AND status != 'replaced'",
        (target_status, request_id, source_status),
    )
"""
        target_binding = source_binding.replace(
            "(target_status, request_id, source_status)",
            "(target_status, request_id, target_status)",
        )
        source_finding = _unguarded_album_request_update_findings(
            source_binding,
        )[0]
        target_finding = _unguarded_album_request_update_findings(
            target_binding,
        )[0]
        self.assertNotEqual(
            source_finding.fingerprint,
            target_finding.fingerprint,
        )
        self.assertTrue(source_finding.exact_source_status_cas)
        self.assertFalse(target_finding.exact_source_status_cas)

    def test_status_scope_fingerprint_covers_alias_definition(self):
        source_definition = """
def thaw(cur, request_id, expected_status, target_status):
    source_status = expected_status
    cur.execute(
        "UPDATE album_requests SET status = %s "
        "WHERE id = %s AND status = %s AND status != 'replaced'",
        (target_status, request_id, source_status),
    )
"""
        target_definition = source_definition.replace(
            "source_status = expected_status",
            "source_status = target_status",
        )
        source_finding = _unguarded_album_request_update_findings(
            source_definition,
        )[0]
        target_finding = _unguarded_album_request_update_findings(
            target_definition,
        )[0]
        self.assertNotEqual(
            source_finding.fingerprint,
            target_finding.fingerprint,
        )
        self.assertFalse(source_finding.exact_source_status_cas)
        self.assertFalse(target_finding.exact_source_status_cas)

    def test_status_scope_fingerprint_covers_match_capture(self):
        canonical = """
def thaw(cur, request_id, expected_status, target_status):
    cur.execute(
        "UPDATE album_requests SET status = %s "
        "WHERE id = %s AND status = %s",
        (target_status, request_id, expected_status),
    )
"""
        captured = canonical.replace(
            "    cur.execute(",
            "    match target_status:\n"
            "        case expected_status:\n"
            "            pass\n"
            "    cur.execute(",
        )
        canonical_finding = _unguarded_album_request_update_findings(
            canonical,
            source_path="lib/example.py",
        )[0]
        captured_finding = _unguarded_album_request_update_findings(
            captured,
            source_path="lib/example.py",
        )[0]

        # Match captures are not ast.Name(Store), so the narrow canonical-call
        # check alone cannot see this reassignment. Whole-method review does.
        self.assertTrue(canonical_finding.exact_source_status_cas)
        self.assertTrue(captured_finding.exact_source_status_cas)
        self.assertNotEqual(
            canonical_finding.fingerprint,
            captured_finding.fingerprint,
        )

    def test_status_scope_fingerprint_covers_nested_nonlocal_reassignment(self):
        canonical = """
def thaw(cur, request_id, expected_status, target_status):
    cur.execute(
        "UPDATE album_requests SET status = %s "
        "WHERE id = %s AND status = %s",
        (target_status, request_id, expected_status),
    )
"""
        reassigned = canonical.replace(
            "    cur.execute(",
            "    def rewrite_source():\n"
            "        nonlocal expected_status\n"
            "        expected_status = target_status\n"
            "    rewrite_source()\n"
            "    cur.execute(",
        )
        canonical_finding = _unguarded_album_request_update_findings(
            canonical,
            source_path="lib/example.py",
        )[0]
        reassigned_finding = _unguarded_album_request_update_findings(
            reassigned,
            source_path="lib/example.py",
        )[0]

        # The canonical-call walk intentionally does not enter nested scopes;
        # the normalized outer-method AST still makes the review key fail.
        self.assertTrue(canonical_finding.exact_source_status_cas)
        self.assertTrue(reassigned_finding.exact_source_status_cas)
        self.assertNotEqual(
            canonical_finding.fingerprint,
            reassigned_finding.fingerprint,
        )

    def test_production_lifecycle_review_rejects_same_line_helper_insertion(self):
        rel = "lib/pipeline_db/requests.py"
        source = (REPO_ROOT / rel).read_text(encoding="utf-8")
        baseline = next(
            finding
            for finding in _unguarded_album_request_update_findings(
                source,
                source_path=rel,
            )
            if finding.scope == "reset_to_wanted"
            and finding.category == "status"
        )
        self.assertIn(
            (rel, baseline.fingerprint),
            _REVIEWED_STATUS_SQL_CALLS,
        )
        needle = (
            '        if expected_status == "replaced":\n'
            "            return False\n"
            "        now = datetime.now(UTC)\n"
        )
        replacement = (
            '        if expected_status == "replaced":\n'
            "            return False\n"
            "        helper = lambda value: value; "
            "now = datetime.now(UTC)\n"
        )
        self.assertEqual(source.count(needle), 1)
        mutated_source = source.replace(needle, replacement)
        mutated = next(
            finding
            for finding in _unguarded_album_request_update_findings(
                mutated_source,
                source_path=rel,
            )
            if finding.scope == "reset_to_wanted"
            and finding.category == "status"
        )

        self.assertEqual(mutated.line, baseline.line)
        self.assertTrue(mutated.exact_source_status_cas)
        self.assertTrue(mutated.canonical_params)
        self.assertTrue(mutated.direct_static_sql)
        self.assertNotEqual(mutated.fingerprint, baseline.fingerprint)
        self.assertNotIn(
            (rel, mutated.fingerprint),
            _REVIEWED_STATUS_SQL_CALLS,
        )

    def test_count_clause_trips_on_an_added_same_fingerprint_site(self):
        """Known-bad self-test for the rationale-count clause (#1258 F4).

        A verbatim duplicate of the whole enclosing method re-spells its
        execute calls with IDENTICAL fingerprints (same normalized scope
        AST), so only the distinct-call-site count can notice — the exact
        world the count clause exists for. Keys are the registry's own
        multi-rationale _core.py entries, so fingerprint churn updates
        both together.
        """
        rel = "lib/pipeline_db/_core.py"
        source = (REPO_ROOT / rel).read_text(encoding="utf-8")

        def sites_per_key(src: str) -> dict[tuple[str, str], set[int]]:
            grouped: dict[tuple[str, str], set[int]] = {}
            for finding in _unguarded_album_request_update_findings(
                src,
                source_path=rel,
            ):
                grouped.setdefault(
                    (rel, finding.fingerprint), set(),
                ).add(finding.line)
            return grouped

        method = next(
            node
            for node in ast.walk(ast.parse(source))
            if isinstance(node, ast.FunctionDef)
            and node.name == "_execute_locked"
        )
        lines = source.splitlines(keepends=True)
        block = "".join(lines[method.lineno - 1:method.end_lineno])
        mutated_source = "".join(
            lines[:method.end_lineno] + ["\n", block]
            + lines[method.end_lineno:]
        )

        keys = [
            key
            for key, rationales in _REVIEWED_DYNAMIC_SQL_CALLS.items()
            if key[0] == rel and len(rationales) > 1
        ]
        self.assertTrue(keys, "expected multi-rationale _core.py entries")
        baseline = sites_per_key(source)
        added = sites_per_key(mutated_source)
        for key in keys:
            with self.subTest(fingerprint=key[1]):
                registered = len(_REVIEWED_DYNAMIC_SQL_CALLS[key])
                self.assertEqual(len(baseline[key]), registered)
                self.assertGreater(len(added[key]), registered)

    def test_params_alias_is_noncanonical(self):
        source = """
def thaw(cur, request_id, expected_status, target_status):
    params = (target_status, request_id, expected_status)
    cur.execute(
        "UPDATE album_requests SET status = %s "
        "WHERE id = %s AND status = %s",
        params,
    )
"""
        finding = _unguarded_album_request_update_findings(source)[0]
        self.assertFalse(finding.canonical_params)
        self.assertFalse(finding.exact_source_status_cas)

    def test_mutated_params_list_is_noncanonical(self):
        for mutation in (
            "params.append(expected_status)",
            "params.extend([expected_status])",
        ):
            with self.subTest(mutation=mutation):
                source = f"""
def thaw(cur, request_id, expected_status, target_status):
    params = [target_status, request_id]
    {mutation}
    cur.execute(
        "UPDATE album_requests SET status = %s "
        "WHERE id = %s AND status = %s",
        params,
    )
"""
                finding = _unguarded_album_request_update_findings(source)[0]
                self.assertFalse(finding.canonical_params)
                self.assertFalse(finding.exact_source_status_cas)

    def test_append_before_params_reassignment_is_noncanonical(self):
        source = """
def thaw(cur, request_id, expected_status, target_status):
    params = [target_status, request_id]
    params.append(target_status)
    params = [target_status, request_id, expected_status]
    cur.execute(
        "UPDATE album_requests SET status = %s "
        "WHERE id = %s AND status = %s",
        params,
    )
"""
        finding = _unguarded_album_request_update_findings(source)[0]
        self.assertFalse(finding.canonical_params)
        self.assertFalse(finding.exact_source_status_cas)

    def test_destructured_source_status_is_not_canonical(self):
        source = """
def thaw(cur, request_id, expected_status, target_status):
    source_status, ignored = (expected_status, None)
    cur.execute(
        "UPDATE album_requests SET status = %s "
        "WHERE id = %s AND status = %s",
        (target_status, request_id, source_status),
    )
"""
        finding = _unguarded_album_request_update_findings(source)[0]
        self.assertTrue(finding.canonical_params)
        self.assertFalse(finding.exact_source_status_cas)

    def test_reassigned_source_argument_is_not_canonical(self):
        source = """
def thaw(cur, request_id, expected_status, target_status):
    expected_status = target_status
    cur.execute(
        "UPDATE album_requests SET status = %s "
        "WHERE id = %s AND status = %s",
        (target_status, request_id, expected_status),
    )
"""
        finding = _unguarded_album_request_update_findings(source)[0]
        self.assertFalse(finding.exact_source_status_cas)

    def test_subscript_source_status_is_not_canonical(self):
        source = """
def thaw(cur, request_id, expected_statuses, target_status):
    cur.execute(
        "UPDATE album_requests SET status = %s "
        "WHERE id = %s AND status = %s",
        (target_status, request_id, expected_statuses[0]),
    )
"""
        finding = _unguarded_album_request_update_findings(source)[0]
        self.assertFalse(finding.exact_source_status_cas)

    def test_status_sql_alias_is_noncanonical(self):
        source = """
def thaw(cur, request_id, expected_status, target_status):
    sql = (
        "UPDATE album_requests SET status = %s "
        "WHERE id = %s AND status = %s"
    )
    cur.execute(sql, (target_status, request_id, expected_status))
"""
        finding = _unguarded_album_request_update_findings(source)[0]
        self.assertFalse(finding.direct_static_sql)

    def test_dynamic_status_assignment_is_rejected(self):
        source = """
def thaw(cur, request_id, source_status, target_status):
    assignment = "status = " + target_status
    cur.execute(
        f"UPDATE album_requests SET {assignment} "
        "WHERE id = %s AND status = %s",
        (request_id, source_status),
    )
"""
        findings = _unguarded_album_request_update_findings(source)
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].category, "status_dynamic")
        self.assertTrue(findings[0].exact_source_status_cas)

    def test_local_sql_variable_is_resolved(self):
        source = '''
def thaw(cur, request_id):
    sql = "UPDATE album_requests SET current_evidence_id = %s WHERE id = %s"
    cur.execute(sql, (9, request_id))
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_module_sql_constant_concatenation_is_resolved(self):
        source = '''
PREFIX = "UPDATE album_requests SET "
SQL = PREFIX + "current_evidence_id = %s WHERE id = %s"

def thaw(cur, request_id):
    cur.execute(SQL, (9, request_id))
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_schema_qualified_quoted_update_is_rejected(self):
        source = '''
def thaw(cur, request_id):
    cur.execute(
        'UPDATE "public"."album_requests" SET current_evidence_id = %s '
        'WHERE id = %s',
        (9, request_id),
    )
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_placeholder_status_guard_is_accepted_for_metadata(self):
        source = '''
def thaw(cur, request_id):
    cur.execute(
        "UPDATE album_requests SET current_evidence_id = %s "
        "WHERE id = %s AND status = %s",
        (9, request_id, "imported"),
    )
'''
        self.assertEqual(_unguarded_album_request_updates(source), [])

    def test_metadata_status_guard_bound_to_replaced_is_rejected(self):
        source = '''
def thaw(cur, request_id):
    cur.execute(
        "UPDATE album_requests SET current_evidence_id = %s "
        "WHERE id = %s AND status != 'replaced' AND status = %s",
        (9, request_id, "replaced"),
    )
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_metadata_status_guard_bound_to_target_is_rejected(self):
        source = '''
def thaw(cur, request_id, target_status):
    cur.execute(
        "UPDATE album_requests SET current_evidence_id = %s "
        "WHERE id = %s AND status != 'replaced' AND status = %s",
        (9, request_id, target_status),
    )
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_metadata_status_guard_bound_to_source_is_accepted(self):
        source = '''
def thaw(cur, request_id, source_status):
    cur.execute(
        "UPDATE album_requests SET current_evidence_id = %s "
        "WHERE id = %s AND status != 'replaced' AND status = %s",
        (9, request_id, source_status),
    )
'''
        self.assertEqual(_unguarded_album_request_updates(source), [])

    def test_partially_dynamic_sql_with_known_target_fails_closed(self):
        source = '''
def thaw(cur, request_id):
    suffix = build_suffix()
    sql = "UPDATE album_requests " + suffix
    cur.execute(sql, (request_id,))
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_unknown_sql_builder_fails_closed(self):
        source = '''
def thaw(cur):
    sql = build_sql()
    cur.execute(sql)
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_keyword_dynamic_sql_fails_closed(self):
        source = '''
def thaw(db, dynamic):
    db._execute(sql=dynamic)
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_augmented_dynamic_sql_fails_closed(self):
        source = '''
def thaw(db, dynamic_suffix):
    sql = "SELECT 1"
    sql += dynamic_suffix
    db._execute(sql)
'''
        self.assertGreaterEqual(
            len(_unguarded_album_request_updates(source)), 1,
        )

    def test_augmented_builder_call_fails_closed(self):
        source = '''
def thaw(db):
    sql = "SELECT 1"
    sql += build_suffix()
    db._execute(sql)
'''
        self.assertGreaterEqual(
            len(_unguarded_album_request_updates(source)), 1,
        )

    def test_augmented_f_string_fails_closed(self):
        source = '''
def thaw(db, suffix):
    sql = "SELECT 1"
    sql += f"{suffix}"
    db._execute(sql)
'''
        self.assertGreaterEqual(
            len(_unguarded_album_request_updates(source)), 1,
        )

    def test_select_plus_dynamic_clause_fails_closed(self):
        source = '''
def thaw(db, dynamic_clause):
    db._execute("SELECT 1 " + dynamic_clause)
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_select_f_string_suffix_fails_closed(self):
        source = '''
def thaw(db, suffix):
    db._execute(f"SELECT 1 {suffix}")
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_select_format_suffix_fails_closed(self):
        source = '''
def thaw(db, suffix):
    db._execute("SELECT 1 {}".format(suffix))
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_unresolved_sql_parameter_fails_closed(self):
        source = '''
def thaw(cur, sql):
    cur.execute(sql)
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_unrelated_object_attribute_does_not_resolve_module_constant(self):
        source = '''
SQL = "SELECT 1"

def thaw(db):
    db._execute(db.SQL)
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_dynamic_f_string_table_fails_closed(self):
        source = '''
def thaw(cur, table):
    cur.execute(f"UPDATE {table} SET current_evidence_id = NULL")
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_dynamic_format_suffix_with_known_target_fails_closed(self):
        source = '''
def thaw(cur, suffix):
    cur.execute("UPDATE album_requests SET {}".format(suffix))
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_dynamic_tail_cannot_weaken_static_guard(self):
        f_string_source = '''
def thaw(cur, request_id, suffix):
    cur.execute(
        f"UPDATE album_requests SET reasoning = 'late' "
        f"WHERE id = %s AND status != 'replaced' {suffix}",
        (request_id,),
    )
'''
        format_source = '''
def thaw(cur, request_id, suffix):
    cur.execute(
        "UPDATE album_requests SET reasoning = 'late' "
        "WHERE id = %s AND status != 'replaced' {}".format(suffix),
        (request_id,),
    )
'''
        self.assertEqual(
            len(_unguarded_album_request_updates(f_string_source)), 1,
        )
        self.assertEqual(
            len(_unguarded_album_request_updates(format_source)), 1,
        )

    def test_fully_dynamic_format_statement_fails_closed(self):
        source = '''
def thaw(cur, verb, table, suffix):
    cur.execute("{} {} SET {}".format(verb, table, suffix))
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_conditional_guarded_reassignment_does_not_hide_unguarded_sql(self):
        source = '''
def thaw(cur, request_id, guarded):
    sql = "UPDATE album_requests SET current_evidence_id = %s WHERE id = %s"
    if guarded:
        sql = (
            "UPDATE album_requests SET current_evidence_id = %s "
            "WHERE id = %s AND status != 'replaced'"
        )
    cur.execute(sql, (9, request_id))
'''
        self.assertGreaterEqual(
            len(_unguarded_album_request_updates(source)), 1,
        )

    def test_other_table_status_guard_does_not_guard_target(self):
        source = '''
def thaw(cur, request_id):
    cur.execute(
        "UPDATE album_requests ar SET reasoning = jobs.reason "
        "FROM jobs WHERE ar.id = %s AND jobs.status != 'replaced'",
        (request_id,),
    )
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_subquery_status_guard_does_not_guard_target(self):
        source = '''
def thaw(cur, request_id):
    cur.execute(
        "UPDATE album_requests SET reasoning = 'late' WHERE id = %s "
        "AND EXISTS (SELECT 1 FROM jobs WHERE status != 'replaced')",
        (request_id,),
    )
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_comment_status_guard_does_not_guard_target(self):
        source = '''
def thaw(cur, request_id):
    cur.execute(
        "UPDATE album_requests SET reasoning = 'late' WHERE id = %s "
        "/* status != 'replaced' */",
        (request_id,),
    )
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_top_level_or_makes_target_guard_non_constraining(self):
        source = '''
def thaw(cur, request_id):
    cur.execute(
        "UPDATE album_requests SET reasoning = 'late' WHERE id = %s "
        "AND status != 'replaced' OR status = 'replaced'",
        (request_id,),
    )
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_unrelated_dynamic_statement_still_fails_closed(self):
        source = '''
def update_log(cur, suffix):
    cur.execute("UPDATE download_log SET {}".format(suffix))
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_static_unrelated_sql_is_accepted(self):
        source = '''
def update_log(cur, request_id):
    cur.execute(
        "UPDATE download_log SET outcome = 'failed' WHERE request_id = %s",
        (request_id,),
    )
'''
        self.assertEqual(_unguarded_album_request_updates(source), [])

    def test_exact_active_status_guard_is_accepted(self):
        source = """
def guarded(cur, request_id):
    cur.execute(
        \"UPDATE album_requests SET current_evidence_id = %s \"
        \"WHERE id = %s AND status = 'imported'\",
        (9, request_id),
    )
"""
        self.assertEqual(_unguarded_album_request_updates(source), [])

    def test_terminal_guard_with_qualified_status_is_accepted(self):
        source = """
def guarded(cur, request_id):
    cur.execute(
        "UPDATE public.album_requests SET current_evidence_id = %s "
        "WHERE id = %s AND album_requests.status <> 'replaced'",
        (9, request_id),
    )
"""
        self.assertEqual(_unguarded_album_request_updates(source), [])


if __name__ == "__main__":
    unittest.main()
