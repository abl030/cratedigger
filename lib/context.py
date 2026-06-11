"""CratediggerContext — runtime state container for the pipeline engine.

Replaces module-level globals in cratedigger.py. Functions extracted to
lib/download.py, lib/import_dispatch.py, etc. receive a CratediggerContext
as their first parameter instead of reading globals.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import threading
from typing import Any, Protocol, TYPE_CHECKING, runtime_checkable

if TYPE_CHECKING:
    from lib.config import CratediggerConfig


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
    def get_tracks(self, album_record: Any) -> list[dict[str, Any]]: ...
    def get_wanted_searchable(
        self, generator_id: str, limit: int | None = None,
    ) -> list[Any]: ...
    def mark_done(
        self,
        album_record: Any,
        bv_result: Any,
        dest_path: Any = None,
        download_info: Any = None,
    ) -> None: ...
    def reject_and_requeue(
        self,
        album_record: Any,
        bv_result: Any,
        usernames: Any = None,
        download_info: Any = None,
        search_filetype_override: Any = None,
        cooled_down_users: set[str] | None = None,
    ) -> int | None: ...
    def close(self) -> None: ...


@dataclass
class CratediggerContext:
    """All runtime state needed by the pipeline engine."""

    # --- Core dependencies (set once in main()) ---
    cfg: CratediggerConfig
    slskd: Any  # slskd_api.SlskdClient — Any to avoid import
    pipeline_db_source: PipelineDBSource
    download_ownership: Any = None

    # --- Runtime caches (reset each cycle) ---
    search_cache: dict[int, Any] = field(default_factory=dict)
    folder_cache: dict[str, Any] = field(default_factory=dict)
    user_upload_speed: dict[str, int] = field(default_factory=dict)
    broken_user: list[str] = field(default_factory=list)
    search_dir_audio_count: dict[str, dict[str, int]] = field(default_factory=dict)
    negative_matches: set[tuple[str, str, int, str]] = field(default_factory=set)
    current_album_cache: dict[int, Any] = field(default_factory=dict)
    denied_users_cache: dict[int, set[str]] = field(default_factory=dict)
    cooled_down_users: set[str] = field(default_factory=set)
    prefetched_album_tracks: dict[int, list[Any]] = field(default_factory=dict)
    peer_cache: Any = None
    peer_cache_negative_skips: set[tuple[str, str]] = field(default_factory=set)
    # Distinct peers cold-browsed this cycle; flushed to the
    # peer_observations roster at end of cycle (#227).
    peer_observations: set[str] = field(default_factory=set)

    # --- Per-cycle timing accumulators (issue #198 U1 instrumentation).
    # browse / match are wrapped at the call sites in lib/matching.py;
    # search is wrapped around _search_and_queue_parallel in cratedigger.py.
    #
    # peers_browsed counts actual cold slskd directory submissions from the
    # primary fan-out path. Redis hits, Redis negative skips, and duplicate
    # callers that join existing in-flight browses do not increment it.
    # peers_browsed_lazy tracks residual cold submissions from the fallback
    # path in lib/matching.py.
    browse_time_s: float = 0.0
    match_time_s: float = 0.0
    search_time_s: float = 0.0
    cache_pos_hits: int = 0
    cache_neg_hits: int = 0
    cache_misses: int = 0
    cache_errors: int = 0
    cache_fuse_tripped: int = 0
    cache_write_errors: int = 0
    peers_browsed: int = 0
    peers_browsed_lazy: int = 0
    fanout_waves: int = 0

    # --- Per-cycle search-watchdog firing count (issue #212).
    # Incremented once per `SearchResult` whose `watchdog_fired=True`.
    # Replaces the `cycle_deadline_skipped` counter that fed the rolled-back
    # `cycle_max_runtime_s` cycle-entry gate. Healthy steady-state is 0–1
    # per cycle; >3 sustained warrants investigation.
    cycle_searches_watchdog_killed: int = 0

    # --- Per-cycle find_download pipeline counters (issue #217). ---
    find_download_queued: int = 0
    find_download_completed: int = 0
    find_download_drain_time_s: float = 0.0

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
