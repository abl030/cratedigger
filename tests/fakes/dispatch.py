"""Typed test double for the core import-dispatch callable."""

from __future__ import annotations

from dataclasses import dataclass, field

from lib.config import CratediggerConfig
from lib.dispatch import DispatchCoreFn, DispatchOutcome, QualityGateFn
from lib.dispatch.quality_gate import _check_quality_gate_core
from lib.dispatch.types import DispatchDB, DispatchRequest
from lib.import_execution import CancellationToken


@dataclass(frozen=True)
class DispatchCoreCall:
    """One recorded ``dispatch_import_core`` invocation.

    Since issue #1277 the whole description of the import is the single
    ``request`` field, so a test asserts ``call.request.scenario`` rather
    than reading a flattened copy of 25 kwargs that had to be kept in step
    with the signature by hand.
    """

    request: DispatchRequest
    db: DispatchDB
    cfg: CratediggerConfig | None
    quality_gate_fn: QualityGateFn
    cancellation_token: CancellationToken | None


@dataclass
class RecordingDispatchCore:
    """Record exact dispatch calls while returning a production-shaped result."""

    outcome: DispatchOutcome = field(default_factory=lambda: DispatchOutcome(
        success=True,
        message="recorded test dispatch",
    ))
    calls: list[DispatchCoreCall] = field(default_factory=list)

    def __call__(
        self,
        request: DispatchRequest,
        db: DispatchDB,
        *,
        cfg: CratediggerConfig | None = None,
        quality_gate_fn: QualityGateFn = _check_quality_gate_core,
        cancellation_token: CancellationToken | None = None,
    ) -> DispatchOutcome:
        self.calls.append(DispatchCoreCall(
            request=request,
            db=db,
            cfg=cfg,
            quality_gate_fn=quality_gate_fn,
            cancellation_token=cancellation_token,
        ))
        return self.outcome


_recorder_conformance: DispatchCoreFn = RecordingDispatchCore()
