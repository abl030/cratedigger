"""FakePipelineDB, the in-memory PipelineDB stand-in.

Records state transitions, log rows, denylist entries, and cooldowns
in-memory. Use it in orchestration tests to assert domain outcomes
instead of MagicMock call shapes.

One cluster module per ``lib/pipeline_db`` concern, composed in
``_db.py`` the way production composes ``PipelineDB`` in
``lib/pipeline_db/_db.py`` (#1313). Shared state and the cross-cluster
contract live in ``_base.py``.

``__all__`` below is the package's whole importable surface, and it is
the four names the flat module deliberately exported. Incidental
module-level bindings the flat file happened to expose (``_utcnow``,
``_jsonb_column``, ``_FakeTerminalTransitionsDB``, and ~160 others it
had imported or defined) do NOT resolve on the package: they live in
the cluster module that uses them. That matters most for ``patch``,
whose target must be the module whose code reads the name.
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
