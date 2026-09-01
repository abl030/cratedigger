"""The composed FakePipelineDB class."""
from __future__ import annotations

from tests.fakes.pipeline_db._core import _FakeCoreMixin
from tests.fakes.pipeline_db.cleanup_journal import _FakeCleanupJournalMixin
from tests.fakes.pipeline_db.convergence import _FakeConvergenceMixin
from tests.fakes.pipeline_db.dashboard import _FakeDashboardMixin
from tests.fakes.pipeline_db.download_log import _FakeDownloadLogMixin
from tests.fakes.pipeline_db.evidence import _FakeEvidenceMixin
from tests.fakes.pipeline_db.import_jobs import _FakeImportJobsMixin
from tests.fakes.pipeline_db.jellyfin_pins import _FakeJellyfinPinsMixin
from tests.fakes.pipeline_db.misc import _FakeMiscMixin
from tests.fakes.pipeline_db.plex_pins import _FakePlexPinsMixin
from tests.fakes.pipeline_db.requests import _FakeRequestsMixin
from tests.fakes.pipeline_db.search_ledger import _FakeSearchLedgerMixin
from tests.fakes.pipeline_db.search_plan import _FakeSearchPlanMixin
from tests.fakes.pipeline_db.terminal_outcomes import _FakeTerminalOutcomesMixin
from tests.fakes.pipeline_db.transfer_ledger import _FakeTransferLedgerMixin
from tests.fakes.pipeline_db.youtube import _FakeYoutubeMixin


class FakePipelineDB(
    _FakeCoreMixin,
    _FakeImportJobsMixin,
    _FakeCleanupJournalMixin,
    _FakeConvergenceMixin,
    _FakeRequestsMixin,
    _FakeEvidenceMixin,
    _FakeDownloadLogMixin,
    _FakeYoutubeMixin,
    _FakeSearchPlanMixin,
    _FakeDashboardMixin,
    _FakePlexPinsMixin,
    _FakeJellyfinPinsMixin,
    _FakeMiscMixin,
    _FakeSearchLedgerMixin,
    _FakeTransferLedgerMixin,
    _FakeTerminalOutcomesMixin,
):
    """In-memory fake for PipelineDB — records mutations for test assertions.

    Stores request rows in a dict keyed by request_id. Mutations update the
    row in place so tests can inspect final state.

    Usage:
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        # ... run orchestration code with db ...
        assert db.request(42)["status"] == "imported"
        assert len(db.download_logs) == 1
        assert db.download_logs[0].outcome == "success"
    """

    # All behaviour is provided by the cluster mixins above; this
    # class only fixes the MRO. See tests/fakes/pipeline_db/_base.py
    # and the sibling cluster modules.
