"""Rule A audit — every PipelineDB write method needs a real-PG round-trip test.

Codifies ``.claude/rules/test-fidelity.md`` § "Rule A — Production-shape write
contract" as an executable rule. When a new ``upsert_* / add_* / update_* /
record_* / set_* / mark_*`` method ships without a corresponding round-trip
test in ``tests/test_pipeline_db.py``, this audit fails and CI blocks the
merge.

Round 2 P0-1 motivated the rule: ``upsert_youtube_album_mapping`` silently
dropped ``album_title`` because the INSERT column list didn't include it and
``psycopg2.extras.execute_values`` ignores extra dict keys. The Fake-based
test stored the dict verbatim so the divergence was invisible. A
round-trip test against real PostgreSQL would have failed instantly. This
audit makes that test mandatory for new code; existing legacy methods are
allowlisted with a documented rationale.

Scope decision: this audit is forward-looking, not retrofit. The bulk of
the 38 existing write methods predate the rule and don't yet have round-trip
guards — backfilling all of them is out of scope. The allowlist captures
that historical reality with one-line rationales; the audit's job is to
make the rule enforceable for NEW writes (the YT mapping methods this PR
added, and everything that ships after).
"""
from __future__ import annotations

import ast
import pathlib
import re
import unittest

import lib.pipeline_db as pdb_mod


# Methods on PipelineDB that mutate state. Anything matching one of these
# prefixes (and not allowlisted below) must have a round-trip guard.
WRITE_METHOD_PREFIXES = (
    "upsert_",
    "add_",
    "insert_",
    "update_",
    "record_",
    "set_",
    "mark_",
)


# Allowlist — methods that genuinely don't need a Rule A guard, or that
# pre-date the rule and are out of scope for this PR. Each entry must
# carry a one-line rationale. The presence of an entry here means
# "we acknowledge the rule but defer the round-trip test"; backfilling
# the missing tests is tracked as future work.
ALLOWLIST: dict[str, str] = {
    # Counter / status-only writers — no dict payload to round-trip.
    "add_cooldown":
        "scalar timestamps + username, no dict payload",
    "add_denylist":
        "scalar args, single-column writes",
    "mark_import_job_completed":
        "status transition only",
    "mark_import_job_failed":
        "status transition only",
    "mark_import_job_preview_importable":
        "status transition only",
    "mark_import_subprocess_started":
        "status transition only",
    "mark_imported_with_rescue":
        "status transition + rescue timestamp",
    "record_attempt":
        "counter increment + timestamp",
    "record_consumed_search_attempt":
        "counter increment + timestamp",
    "record_cycle_metrics":
        "scalar metrics, no dict payload",
    "record_non_consuming_search_attempt":
        "counter increment + timestamp",
    "set_downloading":
        "status transition only",
    "set_downloading_if_plan_current":
        "status transition only",
    "set_unfindable_category":
        "single-column write",
    "update_download_state":
        "single-column status update",
    "update_download_state_current_path":
        "single-column path update",
    "update_download_state_if_downloading":
        "single-column status update",
    "update_status":
        "single-column status update",
    "set_download_log_candidate_evidence":
        "single FK column update",
    "set_import_job_candidate_evidence":
        "single FK column update",
    "set_request_current_evidence":
        "single FK column update",
    # Writers that DON'T fit a typed-payload round-trip by design
    # (permanent — NOT a TODO; #382 Layer 1 analysis):
    "update_request_fields":
        "dynamic field=value writer -- caller-determined column set, no fixed "
        "payload to round-trip. Its typed caller (update_spectral_state) and "
        "the importer's RequestV0ProbeStateUpdate fields (which reach the row "
        "via finalize_request -> mark_imported_with_rescue/update_status) ARE "
        "column-checked by tests/test_pipeline_db_column_contract.py.",
    "update_track_artists":
        "positional list[str|None] driving a single-scalar-column UPDATE; no "
        "column-list payload to round-trip.",
    # Fixed-column dict/kwargs writers — follow the AddRequestInput pattern
    # (#382 Layer 1: derive the INSERT from a typed payload + column contract)
    # in a follow-up. The flat ones are ALREADY column-checked by
    # tests/test_pipeline_db_column_contract.py.
    "add_bad_audio_hashes":
        "TODO: derive-from-Struct + round-trip. BadAudioHashInput already "
        "column-checked by test_pipeline_db_column_contract.py.",
    "record_artist_probe":
        "TODO: backfill round-trip test (legacy method, predates rule)",
    "record_field_resolution":
        "TODO: backfill round-trip test (legacy method, predates rule)",
    "set_tracks":
        "TODO: backfill round-trip test (legacy method, predates rule)",
    "update_spectral_state":
        "TODO: backfill round-trip test. RequestSpectralStateUpdate already "
        "column-checked by test_pipeline_db_column_contract.py.",
    "upsert_album_quality_evidence":
        "TODO: backfill round-trip test (legacy method, predates rule)",
    # Methods whose round-trip tests exist but read via an asymmetric
    # seam the audit's auto-detector can't see. Both write into
    # ``download_log`` and round-trip via ``get_download_log_entry`` —
    # the audit looks for ``get_youtube_running`` / ``get_youtube_terminal``
    # which don't exist (and shouldn't — the table is ``download_log``,
    # not per-outcome). Round-trip guards live at
    # ``tests/test_pipeline_db.py::TestYoutubeIngestDownloadLog``.
    "insert_youtube_running":
        "round-trip via get_download_log_entry; tested in TestYoutubeIngestDownloadLog",
    "update_youtube_terminal":
        "round-trip via get_download_log_entry; tested in TestYoutubeIngestDownloadLog",
    "record_search_id":
        "round-trip via get_unswept_search_ids (the ledger's only reader); "
        "tested in TestSearchLedgerRoundTrip::test_record_round_trip_preserves_every_field",
    "mark_search_ids_deleted":
        "round-trip via raw SELECT on slskd_search_ledger + get_unswept "
        "exclusion; tested in TestSearchLedgerRoundTrip",
    "record_transfer_enqueue":
        "round-trip via raw SELECT on slskd_transfer_ledger; tested in "
        "TestTransferLedgerRoundTrip::"
        "test_record_transfer_enqueue_round_trip_preserves_every_field",
}


# Heuristics that identify a Rule A guard inside a test method body:
# the test must (a) call the write method, AND (b) call the
# corresponding read method (or perform a SELECT) to round-trip back.
# This is intentionally lenient — the audit's job is to catch
# obviously-missing guards, not to nit-pick test-name conventions.
_ROUND_TRIP_TEST_HINTS = (
    "round_trip",
    "preserves",
    "every_field",
)


def _enumerate_write_methods() -> list[str]:
    """Reflect over ``PipelineDB`` and pull out write methods."""
    return [
        n for n in dir(pdb_mod.PipelineDB)
        if any(n.startswith(p) for p in WRITE_METHOD_PREFIXES)
        and not n.startswith("_")
    ]


def _find_round_trip_tests_for_method(method_name: str,
                                      tree: ast.Module) -> list[str]:
    """Return test functions in ``tree`` that exercise ``method_name``
    AND contain a round-trip read.

    Detection rule: the test body must contain BOTH a Call to
    ``<x>.<method_name>(...)`` and a Call to the corresponding ``get_*``
    or ``list_*`` method (or a SELECT statement). The heuristic
    prefers test methods whose name includes a hint
    (``round_trip``, ``preserves``, ``every_field``).
    """
    # Derive the read-side counterpart from the write name.
    read_candidates = set()
    if method_name.startswith("upsert_"):
        suffix = method_name[len("upsert_"):]
        read_candidates.update({f"get_{suffix}", f"list_{suffix}"})
    elif method_name.startswith("add_"):
        suffix = method_name[len("add_"):]
        # Be liberal; many add_X methods read back via list_X / get_X.
        read_candidates.update({f"get_{suffix}", f"list_{suffix}"})
    elif method_name.startswith("insert_"):
        suffix = method_name[len("insert_"):]
        read_candidates.update({f"get_{suffix}", f"list_{suffix}"})
    elif method_name.startswith("update_"):
        suffix = method_name[len("update_"):]
        read_candidates.update({f"get_{suffix}", f"list_{suffix}"})
    elif method_name.startswith("set_"):
        suffix = method_name[len("set_"):]
        read_candidates.update({f"get_{suffix}", f"list_{suffix}"})
    elif method_name.startswith("record_") or method_name.startswith("mark_"):
        suffix = method_name.split("_", 1)[1]
        read_candidates.update({f"get_{suffix}", f"list_{suffix}"})

    hits: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        if not node.name.startswith("test_"):
            continue
        wrote = False
        read_back = False
        for sub in ast.walk(node):
            if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Attribute):
                if sub.func.attr == method_name:
                    wrote = True
                if sub.func.attr in read_candidates:
                    read_back = True
        # SELECT-as-roundtrip — accept any SELECT statement against the
        # method's bare suffix table when the test name hints round-trip.
        body_text = ast.dump(node)
        if any(hint in node.name for hint in _ROUND_TRIP_TEST_HINTS):
            if "SELECT" in body_text or "_query" in body_text:
                read_back = True
        if wrote and read_back:
            hits.append(node.name)
    return hits


class TestPipelineDBWriteAudit(unittest.TestCase):
    """Audit that every write method has at least one Rule A round-trip
    test, OR is explicitly allowlisted with a one-line rationale.

    Add a new write method to PipelineDB ⇒ the audit fails until you
    either:
      1. Add a real-PG round-trip test in ``tests/test_pipeline_db.py``
         (preferred — see ``TestYoutubeAlbumMappings::
         test_upsert_round_trip_preserves_every_field`` for the
         canonical shape), OR
      2. Add an entry to ``ALLOWLIST`` here with a documented reason.

    There is no third option. A new write that ships without a guard
    is the round-2 P0-1 bug class waiting to recur.
    """

    @classmethod
    def setUpClass(cls) -> None:
        # Parse tests/test_pipeline_db.py once and reuse the AST. Derive the
        # path from this test file's own location — robust to lib.pipeline_db
        # being a package (#379) rather than a single module.
        test_path = pathlib.Path(__file__).resolve().parent / "test_pipeline_db.py"
        cls._test_tree = ast.parse(test_path.read_text())

    def test_every_write_method_has_a_round_trip_guard(self) -> None:
        unguarded: list[str] = []
        for name in _enumerate_write_methods():
            if name in ALLOWLIST:
                continue
            hits = _find_round_trip_tests_for_method(name, self._test_tree)
            if not hits:
                unguarded.append(name)
        self.assertEqual(
            unguarded, [],
            msg=(
                "These PipelineDB write methods have no round-trip "
                "test (Rule A — .claude/rules/test-fidelity.md). "
                "Either add a real-PG round-trip test in "
                "tests/test_pipeline_db.py OR add the method to "
                "ALLOWLIST in tests/test_pipeline_db_write_audit.py "
                "with a one-line rationale:\n  - "
                + "\n  - ".join(sorted(unguarded))
            ),
        )

    def test_allowlist_entries_match_real_methods(self) -> None:
        """Catch stale allowlist entries — a method that was renamed
        or deleted but left its allowlist row behind."""
        real_methods = set(_enumerate_write_methods())
        stale = [name for name in ALLOWLIST if name not in real_methods]
        self.assertEqual(
            stale, [],
            msg=(
                "ALLOWLIST contains stale entries that don't match "
                "any current PipelineDB method:\n  - "
                + "\n  - ".join(sorted(stale))
            ),
        )

    def test_allowlist_rationales_are_non_empty(self) -> None:
        empty = [name for name, reason in ALLOWLIST.items()
                 if not reason.strip()]
        self.assertEqual(
            empty, [],
            msg=(
                "ALLOWLIST entries must carry a one-line rationale:\n  - "
                + "\n  - ".join(sorted(empty))
            ),
        )

    def test_youtube_mapping_write_is_actually_guarded(self) -> None:
        """Smoke test that the YT mapping write (the round 2 P0-1
        canonical case) IS guarded — protects against an accidental
        allowlist addition silently re-introducing the bug class.
        """
        hits = _find_round_trip_tests_for_method(
            "upsert_youtube_album_mapping", self._test_tree)
        self.assertIn(
            "test_upsert_round_trip_preserves_every_field",
            hits,
            msg=(
                "Round 2 P0-1's canonical round-trip test "
                "(TestYoutubeAlbumMappings::"
                "test_upsert_round_trip_preserves_every_field) MUST "
                "exist; this is the bug-class guard the audit was "
                "written for."
            ),
        )


# Silence the unused-import audit — ``re`` is reserved for future
# variants of the round-trip-test detection heuristic (e.g. regex over
# SQL strings).
_ = re


if __name__ == "__main__":
    unittest.main()
