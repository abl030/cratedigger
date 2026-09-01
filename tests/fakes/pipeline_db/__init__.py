"""FakePipelineDB, the in-memory PipelineDB stand-in.

Records state transitions, log rows, denylist entries, and cooldowns
in-memory. Use it in orchestration tests to assert domain outcomes
instead of MagicMock call shapes.

One cluster module per ``lib/pipeline_db`` concern, composed in
``_db.py`` the way production composes ``PipelineDB`` in
``lib/pipeline_db/_db.py`` (#1313). Shared state and the cross-cluster
contract live in ``_base.py``. ``from tests.fakes.pipeline_db import X``
still resolves every name the single flat module exported.
"""
from tests.fakes.pipeline_db._db import FakePipelineDB
from tests.fakes.pipeline_db.search_plan import (
    _FakeSearchPlanItemRow,
    _FakeSearchPlanRow,
)
from tests.fakes.pipeline_db.source import FakePipelineDBSource

__all__ = [
    "FakePipelineDB",
    "FakePipelineDBSource",
    "_FakeSearchPlanItemRow",
    "_FakeSearchPlanRow",
]
