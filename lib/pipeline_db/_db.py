"""The composed PipelineDB class."""
from lib.pipeline_db._core import _CoreMixin
from lib.pipeline_db.import_jobs import _ImportJobsMixin
from lib.pipeline_db.requests import _RequestsMixin
from lib.pipeline_db.evidence import _EvidenceMixin
from lib.pipeline_db.download_log import _DownloadLogMixin
from lib.pipeline_db.youtube import _YoutubeMixin
from lib.pipeline_db.search_plan import _SearchPlanMixin
from lib.pipeline_db.dashboard import _DashboardMixin
from lib.pipeline_db.plex_pins import _PlexPinsMixin
from lib.pipeline_db.misc import _MiscMixin
from lib.pipeline_db.search_ledger import _SearchLedgerMixin
from lib.pipeline_db.transfer_ledger import _TransferLedgerMixin


class PipelineDB(
    _CoreMixin,
    _ImportJobsMixin,
    _RequestsMixin,
    _EvidenceMixin,
    _DownloadLogMixin,
    _YoutubeMixin,
    _SearchPlanMixin,
    _DashboardMixin,
    _PlexPinsMixin,
    _MiscMixin,
    _SearchLedgerMixin,
    _TransferLedgerMixin,
):
    """PostgreSQL-backed pipeline database.

    Schema migrations are NOT this class's responsibility. They live in
    ``migrations/*.sql`` and are applied by ``lib.migrator.apply_migrations``,
    which the deploy systemd unit ``cratedigger-db-migrate.service`` runs on every
    ``nixos-rebuild switch``. Construct this class against an already-migrated
    database.
    """

    # All behaviour is provided by the cluster mixins above; this
    # class only fixes the MRO. See lib/pipeline_db/_core.py and
    # the sibling cluster modules.
    pass
