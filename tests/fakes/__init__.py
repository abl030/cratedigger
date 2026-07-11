"""Lightweight fakes for stateful collaborators.

Package split (#445 item 5) — one module per collaborator; this
__init__ preserves the original flat surface so
from tests.fakes import X keeps working for every top-level name
that lived in the old single-file tests/fakes.py.
"""

from tests.fakes._shared import _EPOCH, _PERTH_TZ, _as_datetime, _utcnow
from tests.fakes.beets import FakeBeetsDB
from tests.fakes.cursors import FakeCursor
from tests.fakes.dispatch import DispatchCoreCall, RecordingDispatchCore
from tests.fakes.download import ProcessAlbumCall, RecordingProcessAlbum
from tests.fakes.lookups import FakeDiscogsLookup, FakeMBLookup, http_error
from tests.fakes.pipeline_db import (
    FakePipelineDB,
    FakePipelineDBSource,
    _FakeSearchPlanItemRow,
    _FakeSearchPlanRow,
)
from tests.fakes.rows import (
    DenylistEntry,
    DownloadLogRow,
    FieldResolutionRow,
    SearchLogRow,
    UserCooldownRow,
)
from tests.fakes.slskd import (
    CancelDownloadCall,
    EnqueueCall,
    FakeSlskdAPI,
    FakeSlskdSearches,
    FakeSlskdTransfers,
    FakeSlskdUsers,
    SearchTextCall,
)
from tests.fakes.ytmusic import FakeYTMusic

__all__ = [
    "CancelDownloadCall",
    "DenylistEntry",
    "DownloadLogRow",
    "DispatchCoreCall",
    "EnqueueCall",
    "FakeBeetsDB",
    "FakeCursor",
    "FakeDiscogsLookup",
    "FakeMBLookup",
    "FakePipelineDB",
    "FakePipelineDBSource",
    "FakeSlskdAPI",
    "FakeSlskdSearches",
    "FakeSlskdTransfers",
    "FakeSlskdUsers",
    "FakeYTMusic",
    "FieldResolutionRow",
    "SearchLogRow",
    "SearchTextCall",
    "RecordingDispatchCore",
    "ProcessAlbumCall",
    "RecordingProcessAlbum",
    "UserCooldownRow",
    "http_error",
]
