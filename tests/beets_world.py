"""Shared real-Beets scratch world for stateful lifecycle testing (#743)."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
import json
import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from beets import config as beets_config
from beets import library as beets_library
from beets import plugins as beets_plugins
from beets.util import MoveOperation
from mediafile import MediaFile

from lib.release_identity import detect_release_source
from lib.world_invariants import LibraryAlbumSnapshot


@dataclass(frozen=True)
class ShippedBeetsWorldConfig:
    default_path_template: str
    album_fields: tuple[tuple[str, str], ...]
    duplicate_album_keys: tuple[str, ...]


@dataclass(frozen=True)
class BeetsWorldRelease:
    release_id: str
    artist: str
    album: str
    year: int
    codec: str = "flac"
    track_count: int = 2
    label: str = ""
    catalognum: str = ""
    albumdisambig: str = ""
    releasegroupdisambig: str = ""
    track_titles: tuple[str, ...] = ()

    def track_title(self, track: int) -> str:
        if self.track_titles:
            if len(self.track_titles) != self.track_count:
                raise ValueError(
                    "track_titles length must equal track_count in scratch world"
                )
            return self.track_titles[track - 1]
        return f"Track {track}"


def extract_shipped_beets_world_config(
    repo_root: str | os.PathLike[str],
) -> ShippedBeetsWorldConfig:
    """Extract the path/duplicate contract from the Nix module source."""

    module_path = Path(repo_root) / "nix" / "module.nix"
    source = module_path.read_text(encoding="utf-8")

    default_match = re.search(
        r'default\s*=\s*"(\$albumartist[^"]+)";',
        source,
    )
    if default_match is None:
        raise AssertionError("paths.default template not found in nix/module.nix")

    album_fields = tuple(sorted(re.findall(
        r'album_fields\.(\w+)\s*=\s*"([^"]+)";',
        source,
    )))
    if not any(name == "path_disambig" for name, _ in album_fields):
        raise AssertionError(
            "album_fields.path_disambig not extracted from nix/module.nix "
            f"(got {[name for name, _ in album_fields]!r})"
        )

    duplicate_block = re.search(
        r"duplicate_keys\s*=\s*\{\s*"
        r"album\s*=\s*\[([^\]]+)\]",
        source,
        flags=re.DOTALL,
    )
    if duplicate_block is None:
        raise AssertionError(
            "import.duplicate_keys.album not found in nix/module.nix"
        )
    duplicate_keys = tuple(re.findall(r'"([^"]+)"', duplicate_block.group(1)))
    if set(duplicate_keys) != {"mb_albumid", "discogs_albumid"}:
        raise AssertionError(
            "shipped duplicate keys must be exact release identifiers; "
            f"got {duplicate_keys!r}"
        )

    return ShippedBeetsWorldConfig(
        default_path_template=default_match.group(1),
        album_fields=album_fields,
        duplicate_album_keys=duplicate_keys,
    )


def build_subprocess_beets_config(
    shipped: ShippedBeetsWorldConfig,
    *,
    library_root: Path,
    library_db: Path,
    import_log: Path,
    mirror_url: str,
) -> dict[str, Any]:
    """Render the load-bearing shipped contract for a disposable harness."""

    parsed = urlsplit(mirror_url)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ValueError("mirror URL must be an http(s) origin without credentials")
    return {
        "directory": str(library_root),
        "library": str(library_db),
        "asciify_paths": True,
        "clutter": [
            "Thumbs.DB",
            "Thumbs.db",
            ".DS_Store",
            "*.jpg",
            "*.png",
            "AlbumArt*",
            "Folder.*",
            "desktop.ini",
            "cratedigger.json",
        ],
        "import": {
            "copy": False,
            "write": True,
            "move": True,
            "timid": False,
            "incremental": False,
            "log": str(import_log),
            "languages": ["en"],
            "duplicate_keys": {
                "album": list(shipped.duplicate_album_keys),
                "item": ["artist", "title"],
            },
        },
        "paths": {
            "default": shipped.default_path_template,
            "singleton": "Non-Album/$artist/$title",
            "comp": (
                "Compilations/$album%aunique{albumartist album,path_disambig}/"
                "$track $title"
            ),
        },
        "album_fields": dict(shipped.album_fields),
        "musicbrainz": {
            "host": parsed.netloc,
            "https": parsed.scheme == "https",
            "ratelimit": 100,
        },
        "match": {
            "ignore_video_tracks": False,
            "strong_rec_thresh": 0.10,
            "medium_rec_thresh": 0.25,
            "preferred": {
                "countries": ["AU", "US", "GB|UK"],
                "media": ["Digital Media|File", "CD"],
                "original_year": True,
            },
        },
        # The scratch profile intentionally loads only lookup + path plugins.
        # Production fetchart/lyrics/scrub hooks would add unrelated external
        # writes/network effects to an exact-ID mirror contract test.
        "plugins": ["musicbrainz", "inline"],
        "chroma": {"auto": False},
    }


class BeetsWorld:
    """One isolated Beets SQLite database plus real tagged audio files."""

    def __init__(
        self,
        repo_root: str | os.PathLike[str],
        *,
        subprocess_mirror_url: str | None = None,
    ) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="cratedigger_beets_world_")
        self._closed = False
        self.root = Path(self._tmp.name)
        self.library_root = self.root / "library"
        self.incoming_root = self.root / "incoming"
        self.library_db = self.root / "beets-library.db"
        self.beets_config_dir = self.root / "beets-config"
        self.library_root.mkdir()
        self.incoming_root.mkdir()
        self.shipped = extract_shipped_beets_world_config(repo_root)
        self._import_counter = 0
        try:
            self._configure_beets()
            self.library = beets_library.Library(
                str(self.library_db),
                str(self.library_root),
            )
            if subprocess_mirror_url is not None:
                self._configure_subprocess(subprocess_mirror_url)
        except BaseException:
            self._closed = True
            self._tmp.cleanup()
            raise

    def _configure_beets(self) -> None:
        beets_config["directory"].set(str(self.library_root))
        beets_config["library"].set(str(self.library_db))
        beets_config["asciify_paths"].set(True)
        beets_config["clutter"].set([
            "Thumbs.DB",
            "Thumbs.db",
            ".DS_Store",
            "*.jpg",
            "*.png",
            "AlbumArt*",
            "Folder.*",
            "desktop.ini",
            "cratedigger.json",
        ])
        beets_config["import"]["duplicate_keys"]["album"].set(
            list(self.shipped.duplicate_album_keys)
        )
        beets_config["paths"]["default"].set(
            self.shipped.default_path_template
        )
        beets_config["plugins"].set(["inline"])
        for name, expression in self.shipped.album_fields:
            beets_config["album_fields"][name].set(expression)
        beets_plugins.load_plugins()
        getters = beets_plugins.album_field_getters()
        if "path_disambig" not in getters:
            raise RuntimeError(
                "real Beets inline plugin did not load shipped path_disambig; "
                "run the world model in a fresh interpreter"
            )

    def _configure_subprocess(self, mirror_url: str) -> None:
        self.beets_config_dir.mkdir()
        config = build_subprocess_beets_config(
            self.shipped,
            library_root=self.library_root,
            library_db=self.library_db,
            import_log=self.root / "beets-import.log",
            mirror_url=mirror_url,
        )
        (self.beets_config_dir / "config.yaml").write_text(
            json.dumps(config, indent=2) + "\n",
            encoding="utf-8",
        )

    @contextmanager
    def subprocess_environment(self) -> Iterator[None]:
        """Point one synchronous harness call at this scratch Beets world."""

        if not self.beets_config_dir.is_dir():
            raise RuntimeError("scratch subprocess Beets config is not enabled")
        prior = os.environ.get("BEETSDIR")
        prior_db = os.environ.get("BEETS_DB")
        prior_runtime = os.environ.get("CRATEDIGGER_RUNTIME_CONFIG")
        os.environ["BEETSDIR"] = str(self.beets_config_dir)
        os.environ["BEETS_DB"] = str(self.library_db)
        # lib.util.beets_subprocess_env deliberately gives the deployed
        # runtime config precedence over BEETSDIR. Mask that config while the
        # test subprocess runs, otherwise invoking this profile on doc2 could
        # select the deployed Beets config despite the scratch override.
        os.environ["CRATEDIGGER_RUNTIME_CONFIG"] = str(
            self.beets_config_dir / "runtime-config-disabled.ini"
        )
        try:
            yield
        finally:
            if prior is None:
                os.environ.pop("BEETSDIR", None)
            else:
                os.environ["BEETSDIR"] = prior
            if prior_db is None:
                os.environ.pop("BEETS_DB", None)
            else:
                os.environ["BEETS_DB"] = prior_db
            if prior_runtime is None:
                os.environ.pop("CRATEDIGGER_RUNTIME_CONFIG", None)
            else:
                os.environ["CRATEDIGGER_RUNTIME_CONFIG"] = prior_runtime

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            # Pinned Beets exposes connection cleanup as Database._close().
            # Closing it explicitly keeps repeated Hypothesis worlds from
            # leaking one SQLite connection apiece.
            self.library._close()
        finally:
            self._tmp.cleanup()

    def __enter__(self) -> "BeetsWorld":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    @staticmethod
    def _decode_path(path: object) -> str:
        if isinstance(path, bytes):
            return os.fsdecode(path)
        if isinstance(path, str):
            return path
        if isinstance(path, os.PathLike):
            return os.fsdecode(os.fspath(path))
        raise TypeError(f"unsupported Beets path type: {type(path).__name__}")

    def _absolute_path(self, path: object) -> str:
        decoded = self._decode_path(path)
        if os.path.isabs(decoded):
            return decoded
        return str(self.library_root / decoded)

    def _make_audio(self, path: Path, *, codec: str, frequency: int) -> None:
        if shutil.which("ffmpeg") is None:
            raise RuntimeError("ffmpeg is required; run inside nix-shell")
        codec_key = codec.casefold()
        if codec_key == "flac":
            encoder_args = ["-c:a", "flac"]
        elif codec_key == "mp3":
            encoder_args = ["-c:a", "libmp3lame", "-b:a", "192k"]
        elif codec_key == "opus":
            encoder_args = ["-c:a", "libopus", "-b:a", "128k"]
        else:
            raise ValueError(f"unsupported scratch-world codec {codec!r}")
        subprocess.run(
            [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                f"sine=frequency={frequency}:duration=0.08",
                *encoder_args,
                str(path),
            ],
            check=True,
            capture_output=True,
        )

    def _tag_audio(
        self,
        path: Path,
        release: BeetsWorldRelease,
        *,
        track: int,
    ) -> None:
        media = MediaFile(path)
        media.artist = release.artist
        media.albumartist = release.artist
        media.album = release.album
        media.title = release.track_title(track)
        media.track = track
        media.tracktotal = release.track_count
        media.disc = 1
        media.disctotal = 1
        media.year = release.year
        media.save()

    def _release_identity_values(self, release_id: str) -> dict[str, object]:
        if detect_release_source(release_id) == "discogs":
            return {
                "mb_albumid": "",
                "discogs_albumid": int(release_id),
            }
        return {
            "mb_albumid": release_id,
            "discogs_albumid": 0,
        }

    def _album_release_id(self, album: object) -> str:
        discogs_id = getattr(album, "discogs_albumid", 0)
        if discogs_id:
            return str(discogs_id)
        return str(getattr(album, "mb_albumid", "") or "")

    def import_release(
        self,
        release: BeetsWorldRelease,
        *,
        source_dir: str | os.PathLike[str] | None = None,
    ) -> LibraryAlbumSnapshot:
        """Stage real audio, then perform the Beets add/remove/move sequence."""

        source_path = self.stage_release(release, source_dir=source_dir)
        return self.import_staged_release(release, source_path)

    def stage_release(
        self,
        release: BeetsWorldRelease,
        *,
        source_dir: str | os.PathLike[str] | None = None,
    ) -> Path:
        """Create a tagged real-audio candidate without touching Beets."""

        if release.track_count < 1:
            raise ValueError("track_count must be positive")
        self._import_counter += 1
        if source_dir is None:
            source_path = (
                self.incoming_root / f"attempt-{self._import_counter:04d}"
            )
        else:
            source_path = Path(source_dir)
        source_path.mkdir(parents=True)

        extension = release.codec.casefold()
        for track in range(1, release.track_count + 1):
            path = source_path / f"{track:02d} Track {track}.{extension}"
            self._make_audio(
                path,
                codec=release.codec,
                frequency=300 + (self._import_counter * 20) + track,
            )
            self._tag_audio(path, release, track=track)
        return source_path

    def import_staged_release(
        self,
        release: BeetsWorldRelease,
        source_dir: str | os.PathLike[str],
    ) -> LibraryAlbumSnapshot:
        """Import an already-staged candidate through real Beets models."""

        source_path = Path(source_dir)
        identity_values = self._release_identity_values(release.release_id)
        items: list[beets_library.Item] = []
        extension = release.codec.casefold()
        for track in range(1, release.track_count + 1):
            path = source_path / f"{track:02d} Track {track}.{extension}"
            if not path.is_file():
                raise FileNotFoundError(path)
            item = beets_library.Item.from_path(str(path))
            item.update({
                "artist": release.artist,
                "albumartist": release.artist,
                "album": release.album,
                "title": release.track_title(track),
                "track": track,
                "tracktotal": release.track_count,
                "disc": 1,
                "disctotal": 1,
                "year": release.year,
                "label": release.label,
                "catalognum": release.catalognum,
                "albumdisambig": release.albumdisambig,
                "releasegroupdisambig": release.releasegroupdisambig,
                **identity_values,
            })
            items.append(item)

        duplicate_albums = [
            album
            for album in self.library.albums()
            if self._album_release_id(album) == release.release_id
        ]
        album = self.library.add_album(items)
        for duplicate in duplicate_albums:
            duplicate.remove(delete=True)
        album.move(MoveOperation.MOVE)
        return self.snapshot_album(album)

    def remove_release(self, release_id: str) -> int:
        """Delete every exact-release album through Beets' real model API."""

        matches = [
            album
            for album in self.library.albums()
            if self._album_release_id(album) == release_id
        ]
        for album in matches:
            album.remove(delete=True)
        return len(matches)

    def snapshot_album(self, album: object) -> LibraryAlbumSnapshot:
        raw_items = list(album.items())  # type: ignore[attr-defined]
        item_paths = tuple(
            self._absolute_path(item.path)
            for item in raw_items
        )
        if not item_paths:
            album_path = ""
        else:
            album_path = os.path.dirname(item_paths[0])
        return LibraryAlbumSnapshot(
            album_id=int(album.id),  # type: ignore[attr-defined]
            release_id=self._album_release_id(album),
            album_path=album_path,
            item_paths=item_paths,
        )

    def snapshots(self) -> tuple[LibraryAlbumSnapshot, ...]:
        return tuple(self.snapshot_album(album) for album in self.library.albums())


__all__ = [
    "BeetsWorld",
    "BeetsWorldRelease",
    "ShippedBeetsWorldConfig",
    "build_subprocess_beets_config",
    "extract_shipped_beets_world_config",
]
