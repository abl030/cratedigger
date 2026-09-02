"""CratediggerContext — runtime state container for the pipeline engine.

Replaces module-level globals in cratedigger.py. Functions extracted to
lib/download.py, lib/dispatch/, etc. receive a CratediggerContext
as their first parameter instead of reading globals.
"""

from __future__ import annotations

import threading
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from lib.cycle_counters import CycleCounters

if TYPE_CHECKING:
    from datetime import datetime

    from cratedigger import TrackRecord
    from lib.config import CratediggerConfig
    from lib.download_ownership import DownloadOwnershipWriter
    from lib.enqueue import ClaimedQueueKeysRegistry
    from lib.peer_cache import PeerCache


@runtime_checkable
class PipelineDBSource(Protocol):
    """Structural surface of the pipeline DB source used throughout the engine.

    Production implementation is ``album_source.DatabaseSource``; tests use
    ``tests.fakes.FakePipelineDBSource``. The protocol lets either satisfy
    the ``CratediggerContext.pipeline_db_source`` slot without the test fake
    having to inherit from the production class (which would require a DSN
    constructor it doesn't need).
    """

    def _get_db(self) -> Any: ...
    def get_tracks(self, album_record: Any) -> list[TrackRecord]: ...
    def get_wanted_searchable(
        self,
        generator_id: str,
        limit: int | None = None,
        *,
        title_blacklist: Sequence[str] = (),
    ) -> list[Any]: ...
    def mark_done(
        self,
        album_record: Any,
        bv_result: Any,
        dest_path: Any = None,
        download_info: Any = None,
        import_job_id: int | None = None,
    ) -> Any: ...
    def reject_and_requeue(
        self,
        album_record: Any,
        bv_result: Any,
        usernames: Any = None,
        download_info: Any = None,
        search_filetype_override: Any = None,
        cooled_down_users: set[str] | None = None,
        import_job_id: int | None = None,
    ) -> Any: ...
    def close(self) -> None: ...


@dataclass(frozen=True, kw_only=True)
class CycleCollaborators:
    """The collaborators a pipeline-cycle context is built from (issue #1313).

    **Every field is required and keyword-only.** That is the whole point:
    a construction that forgets one does not type-check and does not run.
    PR #1280 shipped exactly that defect — a second, inline
    ``CratediggerContext(...)`` in ``_run_phase1`` silently omitted
    ``download_ownership``, which made every download-timeout cleanup fail
    CLOSED under the ownership gate that same PR introduced, with the whole
    suite green. An 888-line hand-registered AST audit
    (``tests/test_context_construction_audit.py``) held the set of
    construction sites to catch a repeat. Its several clauses all served one
    constraint on production, which this type now carries for free: that the
    kwarg is *present* at every site. The rest of that module policed its own
    registry (exactness, alias binding, per-scope construction counts, a
    human-written reason per entry), and none of it outlives the registry.

    Requiredness constrains presence, not value: ``download_ownership=None``
    is still spellable, and still fails the ownership gate closed. What it
    buys is that the omission cannot be silent — a collaborator now has to
    be named, in the diff, at every site.

    This is the world that can reach a destructive slskd call
    (``lib.slskd_transfers.cancel_and_delete`` and the convergence sweeps),
    so it carries the slskd client, the ledger-ownership writer that gate
    consults, and the same-cycle claim registry. Its slskd-less sibling is
    ``WorkerCollaborators``.

    Derive a context from another (``cratedigger.build_phase1_context``,
    ``lib.enqueue.prepare_find_download_context``) by CONSTRUCTING a new
    value naming all six fields, never by ``dataclasses.replace`` —
    measured against pyright 1.1.412, ``replace()`` kwargs are checked for
    neither name nor type, so a derivation through it would be exactly as
    unchecked as the omission this type exists to remove.
    """

    cfg: CratediggerConfig
    slskd: Any  # lib.slskd_client.SlskdClient — Any so tests can wire FakeSlskdAPI
    pipeline_db_source: PipelineDBSource
    # Concretely typed (#1313): every consumer reaches it through
    # ``ctx.download_ownership``, and none of them needs a wider surface
    # than the writer's own. ``None`` means "not wired", which every
    # ownership-gated destructive path already fails closed on.
    download_ownership: DownloadOwnershipWriter | None
    # One instance is constructed per cycle (main()'s owner collaborators)
    # and threaded into every find-download worker context by reference,
    # the same pattern as download_ownership above (issue #1178 PR2 review
    # F7 -- was a module-global dict). Real-typed via the TYPE_CHECKING
    # import above (lib.enqueue never imports lib.context at runtime, only
    # under its own TYPE_CHECKING guard, so this carries no import cycle).
    claimed_queue_keys_registry: ClaimedQueueKeysRegistry | None
    peer_cache: PeerCache | None


@dataclass(frozen=True, kw_only=True)
class WorkerCollaborators:
    """The collaborators an out-of-cycle worker context is built from.

    The serial importer (``scripts/importer.py``) and the preview worker
    (``scripts/import_preview_worker.py``) run outside the cycle and hold
    no slskd client, so no destructive slskd path is reachable from a
    context they build. Under the deleted construction audit that was a
    hand-written ``wires_download_ownership=False`` registry entry plus a
    prose ``why``; here it is a type fact — the fields simply do not exist,
    and the pass-through properties below answer ``None``.

    A future change that gives either worker a slskd client has to swap
    this type for ``CycleCollaborators``, which cannot be constructed
    without also naming an ownership writer.
    """

    cfg: CratediggerConfig
    pipeline_db_source: PipelineDBSource

    @property
    def slskd(self) -> None:
        return None

    @property
    def download_ownership(self) -> None:
        return None

    @property
    def claimed_queue_keys_registry(self) -> None:
        return None

    @property
    def peer_cache(self) -> None:
        return None


#: The closed set of collaborator worlds a ``CratediggerContext`` can carry.
#: Closed on purpose: a third world has to be declared here, which is where
#: someone reviewing "can this context reach a destructive slskd call?" looks.
Collaborators = CycleCollaborators | WorkerCollaborators


@dataclass
class CratediggerContext:
    """All runtime state needed by the pipeline engine.

    Two halves, deliberately typed differently (issue #1313):

    - ``collaborators`` — the wired-in dependencies, a frozen value whose
      every field is required. Read through the pass-through properties
      below, so ``ctx.cfg`` / ``ctx.slskd`` / ``ctx.download_ownership``
      and friends still resolve exactly as they always have.
    - everything else — per-cycle scratch: caches, timers, the counters,
      and three worker-local slots. These are mutable and default to empty
      BECAUSE that is correct: a fresh cycle's scratch is empty, and a
      caller that omits one has not forgotten anything.

    The rest of the scratch stays inline rather than nested behind a second
    value: the forwarding it takes part in is deliberately selective and
    partly BY REFERENCE (``build_phase1_context`` shares
    ``cooled_down_users`` so a cooldown Phase 1 discovers reaches Phase 2;
    ``prepare_find_download_context`` shares ``folder_cache``,
    ``browse_coordinator`` and its lock), so a "fresh per thread, never
    forwarded" scratch value would be wrong, and a nested-but-forwardable
    one would only rename what those two functions already spell out field
    by field.

    ``counters`` is the one part that DID separate (issue #1348), and that
    objection is exactly what it escapes: the counters are ints and floats,
    so they cannot be shared by reference at all. Each worker gets a fresh
    value and the owner merges the totals back through
    ``lib.enqueue.FindDownloadMetrics``.
    """

    collaborators: Collaborators

    @property
    def cfg(self) -> CratediggerConfig:
        return self.collaborators.cfg

    @property
    def slskd(self) -> Any:
        return self.collaborators.slskd

    @property
    def pipeline_db_source(self) -> PipelineDBSource:
        return self.collaborators.pipeline_db_source

    @property
    def download_ownership(self) -> DownloadOwnershipWriter | None:
        return self.collaborators.download_ownership

    @property
    def claimed_queue_keys_registry(self) -> ClaimedQueueKeysRegistry | None:
        return self.collaborators.claimed_queue_keys_registry

    @property
    def peer_cache(self) -> PeerCache | None:
        return self.collaborators.peer_cache

    # --- Runtime caches (reset each cycle) ---
    search_cache: dict[int, Any] = field(
        default_factory=lambda: {},  # noqa: PIE807 - preserves contextual generic type
    )
    folder_cache: dict[str, Any] = field(
        default_factory=lambda: {},  # noqa: PIE807 - preserves contextual generic type
    )
    user_upload_speed: dict[str, int] = field(
        default_factory=lambda: {},  # noqa: PIE807 - preserves contextual generic type
    )
    broken_user: set[str] = field(default_factory=lambda: set())
    search_dir_audio_count: dict[str, dict[str, int]] = (
        field(
            default_factory=lambda: {},  # noqa: PIE807 - preserves contextual generic type
        ))
    negative_matches: set[tuple[str, str, int, str]] = (
        field(default_factory=lambda: set()))
    current_album_cache: dict[int, Any] = field(
        default_factory=lambda: {},  # noqa: PIE807 - preserves contextual generic type
    )
    denied_users_cache: dict[int, set[str]] = field(
        default_factory=lambda: {},  # noqa: PIE807 - preserves contextual generic type
    )
    cooled_down_users: set[str] = field(default_factory=lambda: set())
    prefetched_album_tracks: dict[int, list[TrackRecord]] = field(
        default_factory=lambda: {},  # noqa: PIE807 - preserves contextual generic type
    )
    peer_cache_negative_skips: set[tuple[str, str]] = (
        field(default_factory=lambda: set()))
    # Distinct peers cold-browsed this cycle; flushed to the
    # peer_observations roster at end of cycle (#227).
    peer_observations: set[str] = field(default_factory=lambda: set())

    # --- Per-cycle counters (issue #1348). ---
    # Every number the cycle accumulates, declared once in
    # lib/cycle_counters.py. The summary line, the cycle_metrics row and
    # lib.enqueue.FindDownloadMetrics all read this value; none of them
    # enumerates the counter names by hand any more.
    counters: CycleCounters = field(default_factory=CycleCounters)

    # --- Cycle wall-clock anchors. ---
    # Set by run_cycle() as the cycle body starts; read by the registered
    # end-of-cycle close-out steps (lib/cycle_summary.py) to derive elapsed
    # time and the metrics row's started_at without the cycle body threading
    # the values through each call.
    cycle_started_at: datetime | None = None
    cycle_start: float = 0.0

    # --- Per-cycle HAVE evidence enrichment budget. ---
    # Download-phase failures opportunistically measure the request's
    # on-disk copy (missing spectral / V0 research); this bounds how many
    # such measurements one cycle may run so failure bursts never balloon
    # the loop. Skip-if-complete costs nothing and is not budgeted.
    evidence_enrichment_budget: int = 2

    # --- Shared browse boundary ---
    # Lazily initialised by lib.browse so tests that directly pass max_workers
    # to the fan-out primitive keep their local cap. Worker contexts share this
    # object with the owner context to make browse_global_max_workers global.
    browse_coordinator: Any = None
    browse_coordinator_lock: threading.Lock = field(default_factory=threading.Lock)

    # Worker-local plan-execution snapshot. Set on per-album worker
    # contexts by ``prepare_find_download_context`` so the find_download
    # worker can validate the request's active plan is still current
    # before claiming download ownership. Stale completions (request
    # was regenerated mid-flight) skip the claim. Owner-thread context
    # never sets this.
    active_plan_execution: Any = None
