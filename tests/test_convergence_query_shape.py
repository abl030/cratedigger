"""Structural guards for request-local convergence query scaling."""

from __future__ import annotations

import inspect
import unittest
from pathlib import Path

from lib.pipeline_db.convergence import _ConvergenceMixin
from lib.pipeline_db.misc import _MiscMixin
from lib.triage_service import list_triage

_ROOT = Path(__file__).resolve().parent.parent


class TestConvergenceQueryShape(unittest.TestCase):
    def test_migration_derivation_is_request_local_and_partially_indexed(self) -> None:
        sql = (_ROOT / "migrations/071_convergence_signal.sql").read_text()
        self.assertIn("request.id = target_request_id", sql)
        self.assertIn("dl.request_id = request.id", sql)
        self.assertIn("idx_download_log_convergence_candidates", sql)
        self.assertIn("idx_download_log_candidate_evidence_attribution", sql)
        self.assertIn("idx_import_jobs_candidate_evidence_attribution", sql)
        self.assertIn("WHERE candidate_evidence_direct IS TRUE", sql)
        self.assertIn("AND source = 'slskd'", sql)
        self.assertIn("AND beets_scenario = 'strong_match'", sql)
        self.assertIn("AND beets_distance <= 0.15", sql)
        self.assertIn("candidate_contributor_usernames TEXT[]", sql)
        self.assertIn(
            "COALESCE(\n                "
            "CARDINALITY(candidate_contributor_usernames), 0",
            sql,
        )
        self.assertIn("UNNEST(\n        attempt.contributor_usernames", sql)
        self.assertNotIn("REGEXP_SPLIT_TO_TABLE", sql)
        self.assertIn("attempt.observation_count >= 5", sql)

    def test_converged_triage_pages_in_sql_before_bulk_projection(self) -> None:
        db_source = inspect.getsource(_MiscMixin.list_triage_page)
        service_source = inspect.getsource(list_triage)
        self.assertIn("JOIN LATERAL", db_source)
        self.assertIn("derive_request_convergence_signal(ar.id)", db_source)
        self.assertIn("ORDER BY ar.id ASC LIMIT %s", db_source)
        self.assertNotIn("converged_request_ids", db_source)
        self.assertNotIn("get_convergence_signals(None)", service_source)

    def test_exact_id_reads_use_unnest_plus_request_local_lateral(self) -> None:
        source = inspect.getsource(_ConvergenceMixin.get_convergence_signals)
        self.assertIn("UNNEST(%s::BIGINT[])", source)
        self.assertIn("derive_request_convergence_signal(request.id)", source)

    def test_stop_cas_rechecks_target_row_current_evidence_after_lock_wait(self) -> None:
        source = inspect.getsource(_ConvergenceMixin.stop_search_for_convergence)
        self.assertIn(
            "request.current_evidence_id =\n"
            "                      signal.authority_current_evidence_id",
            source,
        )


if __name__ == "__main__":
    unittest.main()
