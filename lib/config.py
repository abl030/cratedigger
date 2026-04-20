"""Cratedigger configuration dataclass.

Replaces the 50+ module-level globals in cratedigger.py with a single
frozen dataclass. Constructed once from config.ini via from_ini().
"""

import configparser
import os
import threading
from dataclasses import dataclass, field
from typing import Optional, TYPE_CHECKING

from lib.quality import QualityRankConfig

if TYPE_CHECKING:
    from lib.quality import AudioFileSpec


# --- Secret file reader (issue #117) ---
#
# Secrets (slskd API key, notifier credentials) must NOT sit plaintext in the
# rendered /var/lib/cratedigger/config.ini. Instead, the config stores a *_file path
# pointing at an out-of-band secret (sops-nix, agenix, raw file, etc.) and the
# Python pipeline reads it on demand here. The in-process cache avoids
# re-reading on every notifier call while still picking up rotations across
# process restarts.

_SECRET_CACHE: dict[str, str] = {}
_SECRET_CACHE_LOCK = threading.Lock()


def read_secret_file(path: str) -> str:
    """Read a single-line secret from a file, strip trailing whitespace,
    and cache the value for subsequent calls in the same process.

    The cache key is the path, so rotating the backing file requires either
    a process restart or an explicit invalidate_secret_cache() call.
    """
    with _SECRET_CACHE_LOCK:
        cached = _SECRET_CACHE.get(path)
        if cached is not None:
            return cached
    with open(path, "r", encoding="utf-8") as f:
        value = f.read().strip()
    with _SECRET_CACHE_LOCK:
        _SECRET_CACHE[path] = value
    return value


def invalidate_secret_cache() -> None:
    """Clear the in-process secret cache. Tests call this between cases to
    avoid cross-test contamination; production callers generally don't need it."""
    with _SECRET_CACHE_LOCK:
        _SECRET_CACHE.clear()


@dataclass(frozen=True)
class CratediggerConfig:
    """All configuration values, read-only after initialization."""

    # --- Slskd ---
    slskd_api_key: str = ""
    slskd_api_key_file: str = ""
    slskd_host_url: str = "http://localhost:5030"
    slskd_url_base: str = "/"
    slskd_download_dir: str = ""
    stalled_timeout: int = 3600
    remote_queue_timeout: int = 300
    delete_searches: bool = True

    # --- Search ---
    ignored_users: tuple[str, ...] = ()
    minimum_match_ratio: float = 0.5
    page_size: int = 10
    search_blacklist: tuple[str, ...] = ()
    album_prepend_artist: bool = False
    track_prepend_artist: bool = False
    search_timeout: int = 5000
    maximum_peer_queue: int = 50
    minimum_peer_upload_speed: int = 0
    search_for_tracks: bool = False
    parallel_searches: int = 8
    browse_parallelism: int = 4
    title_blacklist: tuple[str, ...] = ()

    # --- Release ---
    use_most_common_tracknum: bool = True
    allow_multi_disc: bool = True
    accepted_countries: tuple[str, ...] = (
        "Europe", "Japan", "United Kingdom", "United States",
        "[Worldwide]", "Australia", "Canada",
    )
    skip_region_check: bool = False
    accepted_formats: tuple[str, ...] = ("CD", "Digital Media", "Vinyl")

    # --- Download ---
    download_filtering: bool = False
    use_extension_whitelist: bool = False
    extensions_whitelist: tuple[str, ...] = ("txt", "nfo", "jpg")
    allowed_filetypes: tuple[str, ...] = ("flac", "mp3")

    # --- Beets ---
    beets_validation_enabled: bool = False
    beets_harness_path: str = ""
    beets_distance_threshold: float = 0.15
    beets_staging_dir: str = ""
    audio_check_mode: str = "normal"
    beets_tracking_file: str = ""
    verified_lossless_target: str = ""  # Target format after verified lossless (e.g. "opus 128", "mp3 v2")

    # --- Quality Ranks (codec-aware comparison model, issue #60) ---
    quality_ranks: QualityRankConfig = field(default_factory=QualityRankConfig.defaults)

    # --- Pipeline DB ---
    pipeline_db_enabled: bool = False
    pipeline_db_dsn: str = "postgresql://cratedigger@localhost/cratedigger"

    # --- Meelo ---
    meelo_url: Optional[str] = None
    meelo_username: Optional[str] = None
    meelo_password: Optional[str] = None
    meelo_username_file: str = ""
    meelo_password_file: str = ""

    # --- Plex ---
    plex_url: Optional[str] = None
    plex_token: Optional[str] = None
    plex_token_file: str = ""
    plex_library_section_id: Optional[str] = None
    plex_path_map: Optional[str] = None  # "local_prefix:container_prefix" e.g. "/mnt/virtio/Music/Beets:/prom_music"

    # --- Jellyfin ---
    jellyfin_url: Optional[str] = None
    jellyfin_token: Optional[str] = None
    jellyfin_token_file: str = ""
    jellyfin_library_id: Optional[str] = None  # optional; full refresh if unset

    # --- Paths (derived from args) ---
    var_dir: str = "."
    lock_file_path: str = ""
    config_file_path: str = ""

    # --- Derived (computed once at init) ---
    _allowed_specs: "tuple[AudioFileSpec, ...]" = ()

    def __post_init__(self) -> None:
        from lib.quality import parse_filetype_config
        object.__setattr__(
            self, "_allowed_specs",
            tuple(parse_filetype_config(s) for s in self.allowed_filetypes),
        )

    @property
    def allowed_specs(self) -> "tuple[AudioFileSpec, ...]":
        return self._allowed_specs

    # --- Secret resolution (issue #117) ---
    #
    # Prefer *_file paths over legacy plaintext fields so the rendered
    # config.ini never has to embed credentials. Each resolver is exactly
    # one line + delegation to the shared _resolve_secret helper — this is
    # intentional: it keeps a single spelling of the precedence rule
    # (file over direct) that every notifier and client relies on.

    def _resolve_secret(self, direct: Optional[str], file_path: str) -> Optional[str]:
        if file_path:
            return read_secret_file(file_path)
        return direct or None

    def resolved_slskd_api_key(self) -> str:
        return self._resolve_secret(self.slskd_api_key, self.slskd_api_key_file) or ""

    def resolved_meelo_username(self) -> Optional[str]:
        return self._resolve_secret(self.meelo_username, self.meelo_username_file)

    def resolved_meelo_password(self) -> Optional[str]:
        return self._resolve_secret(self.meelo_password, self.meelo_password_file)

    def resolved_plex_token(self) -> Optional[str]:
        return self._resolve_secret(self.plex_token, self.plex_token_file)

    def resolved_jellyfin_token(self) -> Optional[str]:
        return self._resolve_secret(self.jellyfin_token, self.jellyfin_token_file)

    @classmethod
    def from_ini(cls, config: configparser.RawConfigParser,
                 config_dir: str = ".", var_dir: str = ".") -> "CratediggerConfig":
        """Parse a ConfigParser into a CratediggerConfig.

        Reproduces the exact same parsing logic as main() in cratedigger.py.
        """
        def get(section, key, fallback=""):
            return config.get(section, key, fallback=fallback)

        def getbool(section, key, fallback=False):
            return config.getboolean(section, key, fallback=fallback)

        def getint(section, key, fallback=0):
            return config.getint(section, key, fallback=fallback)

        def getfloat(section, key, fallback=0.0):
            return config.getfloat(section, key, fallback=fallback)

        def split_csv(section, key, fallback=""):
            raw = get(section, key, fallback)
            return tuple(s.strip() for s in raw.split(",") if s.strip())

        # Filetypes parsing
        raw_filetypes = get("Search Settings", "allowed_filetypes", "flac,mp3")
        if "," in raw_filetypes:
            allowed_filetypes = tuple(s.strip() for s in raw_filetypes.split(",") if s.strip())
        else:
            allowed_filetypes = (raw_filetypes.strip(),)

        # Ignored users
        ignored_raw = get("Search Settings", "ignored_users", "")
        ignored_users = tuple(u.strip() for u in ignored_raw.split(",") if u.strip())

        # Blacklists
        search_bl_raw = get("Search Settings", "search_blacklist", "")
        search_blacklist = tuple(w.strip() for w in search_bl_raw.split(",") if w.strip())
        title_bl_raw = get("Search Settings", "title_blacklist", "")
        title_blacklist = tuple(w.strip() for w in title_bl_raw.split(",") if w.strip())

        return cls(
            # Slskd
            slskd_api_key=get("Slskd", "api_key"),
            slskd_api_key_file=get("Slskd", "api_key_file"),
            slskd_host_url=get("Slskd", "host_url", "http://localhost:5030"),
            slskd_url_base=get("Slskd", "url_base", "/"),
            slskd_download_dir=get("Slskd", "download_dir"),
            stalled_timeout=getint("Slskd", "stalled_timeout", 3600),
            remote_queue_timeout=getint("Slskd", "remote_queue_timeout", 300),
            delete_searches=getbool("Slskd", "delete_searches", True),
            # Search
            ignored_users=ignored_users,
            minimum_match_ratio=getfloat("Search Settings", "minimum_filename_match_ratio", 0.5),
            page_size=getint("Search Settings", "number_of_albums_to_grab", 10),
            search_blacklist=search_blacklist,
            album_prepend_artist=getbool("Search Settings", "album_prepend_artist", False),
            track_prepend_artist=getbool("Search Settings", "track_prepend_artist", False),
            search_timeout=getint("Search Settings", "search_timeout", 5000),
            maximum_peer_queue=getint("Search Settings", "maximum_peer_queue", 50),
            minimum_peer_upload_speed=getint("Search Settings", "minimum_peer_upload_speed", 0),
            search_for_tracks=getbool("Search Settings", "search_for_tracks", False),
            parallel_searches=getint("Search Settings", "parallel_searches", 8),
            browse_parallelism=min(getint("Search Settings", "browse_parallelism", 4), 8),
            title_blacklist=title_blacklist,
            # Release
            use_most_common_tracknum=getbool("Release Settings", "use_most_common_tracknum", True),
            allow_multi_disc=getbool("Release Settings", "allow_multi_disc", True),
            accepted_countries=split_csv("Release Settings", "accepted_countries",
                                         "Europe,Japan,United Kingdom,United States,[Worldwide],Australia,Canada"),
            skip_region_check=getbool("Release Settings", "skip_region_check", False),
            accepted_formats=split_csv("Release Settings", "accepted_formats",
                                       "CD,Digital Media,Vinyl"),
            # Download
            download_filtering=getbool("Download Settings", "download_filtering", False),
            use_extension_whitelist=getbool("Download Settings", "use_extension_whitelist", False),
            extensions_whitelist=split_csv("Download Settings", "extensions_whitelist", "txt,nfo,jpg"),
            allowed_filetypes=allowed_filetypes,
            # Beets
            beets_validation_enabled=getbool("Beets Validation", "enabled", False),
            beets_harness_path=get("Beets Validation", "harness_path", ""),
            beets_distance_threshold=getfloat("Beets Validation", "distance_threshold", 0.15),
            beets_staging_dir=get("Beets Validation", "staging_dir", ""),
            audio_check_mode=get("Beets Validation", "audio_check", "normal"),
            beets_tracking_file=get("Beets Validation", "tracking_file", ""),
            verified_lossless_target=get("Beets Validation", "verified_lossless_target", ""),
            # Quality Ranks — codec-aware comparison policy. Missing section
            # yields the default QualityRankConfig (see lib/quality.py).
            quality_ranks=QualityRankConfig.from_ini(config),
            # Pipeline DB
            pipeline_db_enabled=getbool("Pipeline DB", "enabled", False),
            pipeline_db_dsn=get("Pipeline DB", "dsn", "postgresql://cratedigger@localhost/cratedigger"),
            # Meelo
            meelo_url=get("Meelo", "url") or None,
            meelo_username=get("Meelo", "username") or None,
            meelo_password=get("Meelo", "password") or None,
            meelo_username_file=get("Meelo", "username_file"),
            meelo_password_file=get("Meelo", "password_file"),
            # Plex
            plex_url=get("Plex", "url") or None,
            plex_token=get("Plex", "token") or None,
            plex_token_file=get("Plex", "token_file"),
            plex_library_section_id=get("Plex", "library_section_id") or None,
            plex_path_map=get("Plex", "path_map") or None,
            # Jellyfin
            jellyfin_url=get("Jellyfin", "url") or None,
            jellyfin_token=get("Jellyfin", "token") or None,
            jellyfin_token_file=get("Jellyfin", "token_file"),
            jellyfin_library_id=get("Jellyfin", "library_id") or None,
            # Paths
            var_dir=var_dir,
            lock_file_path=os.path.join(var_dir, ".cratedigger.lock"),
            config_file_path=os.path.join(config_dir, "config.ini"),
        )


DEFAULT_RUNTIME_CONFIG_PATH = "/var/lib/cratedigger/config.ini"


def _runtime_config_path(config_path: str | None = None) -> str:
    """Resolve the active runtime config.ini path."""
    return config_path or os.environ.get("CRATEDIGGER_RUNTIME_CONFIG") or DEFAULT_RUNTIME_CONFIG_PATH


def read_runtime_config(config_path: str | None = None) -> CratediggerConfig:
    """Read the active runtime config.ini into a full CratediggerConfig.

    Manual-import, force-import, the CLI, and web simulator all need the same
    runtime config the main cratedigger process reads.

    Missing config (file does not exist) is a soft failure — return a default
    CratediggerConfig so callers degrade safely. This covers test environments and
    the very first deploy before the prestart has rendered the file.

    Unreadable config (file exists but PermissionError) raises loudly. This
    is a deployment bug — usually config.ini's mode is too restrictive for
    the calling user. Silently returning empty config previously masked
    issue #117 (force-import via pipeline-cli failed with cryptic
    "/home/abl030/import_one.py not found" because beets_harness_path was
    empty). Surfacing the real cause beats the silent path-resolution
    fallout downstream.
    """
    path = _runtime_config_path(config_path)
    if not path or not os.path.exists(path):
        return CratediggerConfig()

    parser = configparser.RawConfigParser()
    try:
        parser.read(path)
    except configparser.Error:
        return CratediggerConfig()
    except PermissionError as exc:
        raise PermissionError(
            f"Cannot read {path} — check file mode / group ownership. "
            "On the upstream NixOS module, set services.cratedigger.configMode "
            "(default 0600) and services.cratedigger.configGroup so the calling "
            "user can read it. See issue #117."
        ) from exc

    runtime_dir = os.path.dirname(path)
    return CratediggerConfig.from_ini(
        parser,
        config_dir=runtime_dir,
        var_dir=runtime_dir,
    )


def read_runtime_rank_config(config_path: str | None = None) -> QualityRankConfig:
    """Read the active runtime QualityRankConfig."""
    return read_runtime_config(config_path).quality_ranks


def read_verified_lossless_target(config_path: str | None = None) -> str:
    """Read verified_lossless_target from the runtime config file.

    Manual-import and force-import run outside the main cratedigger process, so they
    need a small helper to discover the same runtime setting. Callers may pass an
    explicit path, otherwise the standard doc2 runtime config is used.
    """
    return read_runtime_config(config_path).verified_lossless_target
