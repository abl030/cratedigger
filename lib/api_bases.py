"""Process-startup wiring for the mirror API bases (tier-2 plan U6/KTD6).

``lib/mb_api.py`` and ``lib/discogs_api.py`` hold their bases as module globals.
``cratedigger-web`` sets them in ``web/server.py::main()`` (flags win over
config), but they are ALSO imported by headless processes — pipeline-cli
(add --discogs, distance, Replace, field resolution) and
the youtube-ingest worker. Every such entry point must call
``configure_api_bases_from_runtime_config()`` at startup, or it silently
runs against the module defaults (public MB / Discogs-unset) instead of
the operator's mirrors.
"""

from __future__ import annotations

PUBLIC_MB_ORIGIN = "https://musicbrainz.org"


def mb_ws2_base(origin: str) -> str:
    """WS/2 base from an MB origin (scheme://host[:port], no path)."""
    return (origin or PUBLIC_MB_ORIGIN).rstrip("/") + "/ws/2"


PUBLIC_MB_WS2_BASE = mb_ws2_base(PUBLIC_MB_ORIGIN)


def configure_api_bases_from_runtime_config() -> None:
    """Point lib.mb_api / lib.discogs_api at the runtime config's mirror origins.

    Reads [MusicBrainz] api_base and [Discogs] api_base from the runtime
    config.ini (rendered by the NixOS module). Missing config soft-fails
    to the stranger posture: public MB, Discogs browse off.
    """
    import lib.discogs_api
    import lib.mb_api
    from lib.config import read_runtime_config

    cfg = read_runtime_config()
    lib.mb_api.MB_API_BASE = mb_ws2_base(cfg.musicbrainz_api_base)
    if cfg.discogs_api_base:
        lib.discogs_api.DISCOGS_API_BASE = cfg.discogs_api_base
