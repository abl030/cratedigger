"""FakePipelineDB, the in-memory PipelineDB stand-in.

Records state transitions, log rows, denylist entries, and cooldowns
in-memory. Use it in orchestration tests to assert domain outcomes
instead of MagicMock call shapes.

One cluster module per ``lib/pipeline_db`` concern, composed in
``_db.py`` the way production composes ``PipelineDB`` in
``lib/pipeline_db/_db.py`` (#1313). Shared state and the cross-cluster
contract live in ``_base.py``.

``__all__`` below is the package's deliberate export surface, and it is
the four names the flat module deliberately exported. The flat file also
bound about 200 other names at module scope (measured by AST over
``0c81190e:tests/fakes/pipeline_db.py``), every one of them reachable as
``tests.fakes.pipeline_db.<name>``. Those do NOT resolve on the package:
``_utcnow``, ``_jsonb_column``, ``_FakeTerminalTransitionsDB`` and the
rest now live in the cluster module that uses them. That matters most
for ``patch``, whose target must be the module whose code reads the name.
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
