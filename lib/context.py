"""CratediggerContext — runtime state container for the pipeline engine.

Replaces module-level globals in cratedigger.py. Functions extracted to
lib/download.py, lib/import_dispatch.py, etc. receive a CratediggerContext
as their first parameter instead of reading globals.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from album_source import DatabaseSource
    from lib.config import CratediggerConfig


@dataclass
class CratediggerContext:
    """All runtime state needed by the pipeline engine."""

    # --- Core dependencies (set once in main()) ---
    cfg: CratediggerConfig
    slskd: Any  # slskd_api.SlskdClient — Any to avoid import
    pipeline_db_source: DatabaseSource

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

    # --- Cache timestamps (epoch floats, for per-entry TTL eviction) ---
    _folder_cache_ts: dict[str, dict[str, float]] = field(default_factory=dict)
    _upload_speed_ts: dict[str, float] = field(default_factory=dict)
    _dir_audio_count_ts: dict[str, dict[str, float]] = field(default_factory=dict)

    # --- Per-cycle timing accumulators (issue #198 U1 instrumentation).
    # browse / match are wrapped at the call sites in lib/matching.py;
    # search is wrapped around _search_and_queue_parallel in cratedigger.py;
    # cache_load is set once by lib/cache.load_caches.
    #
    # peers_browsed is the fan-out path's count (every (user, dir) submitted
    # to a wave, success or failure). peers_browsed_lazy is the fallback
    # path in lib/matching.py — it fires when fan-out left a (user, dir)
    # unwritten via _browse_one's exception swallow. Splitting them avoids
    # double-counting that same (user, dir) when the lazy path retries it.
    browse_time_s: float = 0.0
    match_time_s: float = 0.0
    search_time_s: float = 0.0
    cache_load_s: float = 0.0
    peers_browsed: int = 0
    peers_browsed_lazy: int = 0
    fanout_waves: int = 0

    # --- Cycle deadline (issue #198 follow-up to the 2026-05-02 rollback).
    # Absolute epoch seconds; None means "no client-side cap" (the rollback
    # default). Set in main() at cycle_start from cfg.cycle_max_runtime_s.
    # Search-phase entry gates check `time.time() > cycle_deadline` and
    # stop submitting *new* work — in-flight searches always finish.
    cycle_deadline: float | None = None
    cycle_deadline_skipped: int = 0


def compute_cycle_deadline(cfg, now: float) -> float | None:
    """Return the absolute deadline for this cycle, or None if disabled.

    `cfg.cycle_max_runtime_s <= 0` means the cap is opted out — keeping the
    2026-05-02 rollback behaviour where slskd's own timeouts are the sole
    authority. Anything positive becomes `now + cap`.
    """
    cap = getattr(cfg, "cycle_max_runtime_s", 0)
    if cap <= 0:
        return None
    return now + cap
