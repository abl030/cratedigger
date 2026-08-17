"""The small structural compatibility boundary for supported Beets releases.

This module intentionally owns the upstream seams Cratedigger calls. Nothing
outside it decides an upstream era from a version string.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable
from dataclasses import dataclass
from types import ModuleType
from typing import TYPE_CHECKING, Protocol, TypeGuard

import msgspec
from beets import library

if TYPE_CHECKING:
    from beets.importer.actions import DuplicateAction


class BeetsCapabilityError(RuntimeError):
    """The loaded Beets does not expose one complete supported capability set."""


_discogs_original_subtrack_position: Callable[..., object] | None = None
_discogs_original_merge_subtracks: Callable[..., object] | None = None


def _discogs_duration_seconds(value: object) -> int | None:
    """Parse Discogs' bounded ``M:SS`` duration without version internals."""

    if not isinstance(value, str):
        return None
    pieces = value.split(":")
    if len(pieces) != 2:
        return None
    try:
        minutes, seconds = (int(piece) for piece in pieces)
    except ValueError:
        return None
    if minutes < 0 or not 0 <= seconds < 60:
        return None
    return minutes * 60 + seconds


def configure_discogs_subtracks(*, preserve_flat: bool) -> None:
    """Install Cratedigger's narrow Discogs physical-track compatibility.

    Upstream intentionally coalesces consecutive flat ``A2.1``/``A2.2``
    entries because they can describe one physical track. Its merge retains
    only the first component's duration, however, so a genuine one-file
    composite is matched against an incomplete program. Always sum complete
    component durations.

    When a default candidate would leave admitted audio unmapped, controllers
    rerun one safe observational pass with ``preserve_flat=True``. That mode
    keeps flat indexed entries separate while leaving nested ``IndexTrack``
    handling on Beets' original path. The patch is process-local and is
    installed before plugin loading in the harness child.
    """

    module = _required_module("beetsplug.discogs")
    plugin_class = _class(module, "DiscogsPlugin")
    if plugin_class is None:
        raise BeetsCapabilityError("Beets Discogs plugin lacks DiscogsPlugin")

    global _discogs_original_subtrack_position
    global _discogs_original_merge_subtracks
    if _discogs_original_subtrack_position is None:
        original = getattr(plugin_class, "_subtrack_position", None)
        if not callable(original):
            raise BeetsCapabilityError(
                "Beets Discogs plugin lacks callable _subtrack_position"
            )
        _discogs_original_subtrack_position = original
    if _discogs_original_merge_subtracks is None:
        original = getattr(plugin_class, "_merge_subtracks", None)
        if not callable(original):
            raise BeetsCapabilityError(
                "Beets Discogs plugin lacks callable _merge_subtracks"
            )
        _discogs_original_merge_subtracks = original

    original_merge = _discogs_original_merge_subtracks

    def merge_complete_program(
        subtracks: list[dict[str, object]],
    ) -> dict[str, object]:
        merged_value = original_merge(subtracks)
        if not isinstance(merged_value, dict):
            raise BeetsCapabilityError(
                "Beets Discogs _merge_subtracks returned a non-dict"
            )
        merged = msgspec.convert(merged_value, type=dict[str, object])
        seconds = [
            _discogs_duration_seconds(track.get("duration"))
            for track in subtracks
        ]
        if len(seconds) > 1 and all(value is not None for value in seconds):
            total = sum(value for value in seconds if value is not None)
            merged["duration"] = f"{total // 60}:{total % 60:02d}"
        return merged

    type.__setattr__(
        plugin_class,
        "_merge_subtracks",
        staticmethod(merge_complete_program),
    )
    if preserve_flat:
        def keep_flat_entries_separate(
            _self: object,
            _track: dict[str, object],
        ) -> None:
            return None

        type.__setattr__(
            plugin_class,
            "_subtrack_position",
            keep_flat_entries_separate,
        )
    else:
        type.__setattr__(
            plugin_class,
            "_subtrack_position",
            _discogs_original_subtrack_position,
        )


class _ConfigValue(Protocol):
    def as_filename(self) -> str: ...


class _BeetsConfig(Protocol):
    def __getitem__(self, key: str) -> _ConfigValue: ...


def _required_module(name: str) -> ModuleType:
    try:
        return importlib.import_module(name)
    except ImportError as exc:
        raise BeetsCapabilityError(f"Beets module {name!r} is unavailable") from exc


def _optional_module(name: str) -> ModuleType | None:
    """Return an optional module only when *that exact module* is absent."""
    try:
        return importlib.import_module(name)
    except ModuleNotFoundError as exc:
        if exc.name == name:
            return None
        raise BeetsCapabilityError(f"Beets module {name!r} failed while importing") from exc
    except ImportError as exc:
        raise BeetsCapabilityError(f"Beets module {name!r} failed while importing") from exc


def _class(module: ModuleType, name: str) -> type[object] | None:
    value = getattr(module, name, None)
    return value if isinstance(value, type) else None


def _construct(ctor: Callable[..., object], *args: object) -> object:
    """Call ``ctor`` with ``args``, ignoring its statically-inferred arity.

    A ``type[object]``-typed value (a resolved-but-unknown-shape class like
    ``ImportTask``) appears to pyright as taking zero constructor
    arguments, since ``object.__init__`` takes none. Routing the call
    through this ``Callable[..., object]``-typed parameter — rather than
    calling the class value directly — is what makes pyright honour the
    wider shape; a local variable re-annotated the same way keeps the
    narrower type from the assigned expression instead.
    """
    return ctor(*args)


@dataclass(frozen=True)
class BeetsCapabilities:
    importer_session: type[object]
    import_task: type[object]
    action: object
    duplicate_action: object | None
    path_bytes: type[bytes]
    duplicate_era: str
    library_era: str
    task_metadata_era: str

    @property
    def era(self) -> str:
        return (
            f"{self.duplicate_era}-duplicates/{self.library_era}-library/"
            f"{self.task_metadata_era}-task-metadata"
        )


def _load_capabilities() -> BeetsCapabilities:
    importer = _required_module("beets.importer")
    ui = _required_module("beets.ui")

    session_module = _optional_module("beets.importer.session")
    task_module = _optional_module("beets.importer.tasks")
    if (session_module is None) != (task_module is None):
        raise BeetsCapabilityError(
            "Beets importer split is partial; ImportSession and ImportTask must move together"
        )
    if session_module is None:
        session_module = importer
        task_module = importer
    assert task_module is not None

    session = _class(session_module, "ImportSession")
    task = _class(task_module, "ImportTask")
    action = getattr(importer, "Action", getattr(importer, "action", None))
    if session is None or task is None or action is None:
        raise BeetsCapabilityError("Beets importer lacks ImportSession, ImportTask, or Action")

    actions_module = _optional_module("beets.importer.actions")
    duplicate_action = (
        getattr(actions_module, "DuplicateAction", None)
        if actions_module is not None
        else None
    )
    modern_duplicates = callable(getattr(session, "get_duplicate_action", None))
    legacy_duplicates = callable(getattr(session, "resolve_duplicate", None))
    if modern_duplicates == legacy_duplicates:
        raise BeetsCapabilityError(
            "Beets duplicate hooks are ambiguous; expected exactly one of "
            "get_duplicate_action or resolve_duplicate"
        )
    if modern_duplicates and duplicate_action is None:
        raise BeetsCapabilityError("modern duplicate hook has no DuplicateAction enum")

    modern_library = callable(getattr(library.Library, "get_replacements", None))
    legacy_library = callable(getattr(ui, "get_path_formats", None)) and callable(
        getattr(ui, "get_replacements", None)
    )
    if modern_library == legacy_library:
        raise BeetsCapabilityError(
            "Beets Library construction is ambiguous; expected exactly one "
            "configured constructor era"
        )

    # Task-metadata era (issue #1088): attribute presence, never __version__.
    modern_task_metadata = hasattr(task, "source")
    legacy_task_metadata = hasattr(task, "cur_artist")
    if not modern_task_metadata and not legacy_task_metadata:
        # v2.1.0/v2.2.0: cur_artist/cur_album are set only inside __init__.
        try:
            legacy_task_metadata = hasattr(
                _construct(task, None, None, None), "cur_artist")
        except Exception:  # noqa: BLE001 — an unexpected ctor shape fails closed below
            legacy_task_metadata = False
    if modern_task_metadata and legacy_task_metadata:
        raise BeetsCapabilityError(
            "Beets ImportTask metadata access is ambiguous: both source and "
            "cur_artist are present — an unexpected upstream shape; "
            "investigate before trusting either era"
        )
    if not modern_task_metadata and not legacy_task_metadata:
        raise BeetsCapabilityError(
            "Beets ImportTask metadata access is ambiguous: neither source "
            "nor cur_artist is present, even via a construction probe — an "
            "unrecognised upstream release"
        )

    util_module = _optional_module("beets.util")
    path_bytes_candidate = (
        getattr(util_module, "PathBytes", bytes) if util_module is not None else bytes
    )
    path_bytes = path_bytes_candidate if isinstance(path_bytes_candidate, type) else bytes
    return BeetsCapabilities(
        importer_session=session,
        import_task=task,
        action=action,
        duplicate_action=duplicate_action,
        path_bytes=path_bytes,
        duplicate_era="modern" if modern_duplicates else "legacy",
        library_era="modern" if modern_library else "legacy",
        task_metadata_era="modern" if modern_task_metadata else "legacy",
    )


CAPABILITIES = _load_capabilities()


def configured_library(config: _BeetsConfig) -> library.Library:
    """Open a library preserving the active Beets era's configured paths."""
    ui = _required_module("beets.ui")
    item = config.__getitem__
    library_path = item("library").as_filename()
    directory = item("directory").as_filename()
    if CAPABILITIES.library_era == "modern":
        result = library.Library(library_path, directory)
        if not getattr(result, "path_formats", None) or getattr(result, "replacements", None) is None:
            raise BeetsCapabilityError("modern Library did not derive configured paths/replacements")
        return result
    get_path_formats = _module_callable(ui, "get_path_formats")
    get_replacements = _module_callable(ui, "get_replacements")
    constructor = _legacy_library_constructor()
    return constructor(library_path, directory, get_path_formats(item("paths")), get_replacements())


def duplicate_outcome(decision: str, task: object) -> DuplicateAction | None:
    """Apply Cratedigger's ``remove|skip`` decision to the active hook era."""
    remove = decision == "remove"
    if CAPABILITIES.duplicate_era == "legacy":
        vars(task)["should_remove_duplicates"] = remove
        return None
    if CAPABILITIES.duplicate_action is None:
        raise BeetsCapabilityError("modern duplicate hook has no DuplicateAction enum")
    return getattr(CAPABILITIES.duplicate_action, "REMOVE" if remove else "SKIP")


def task_description(task: object) -> tuple[str, str]:
    """Return ``(artist, album)`` for ``task`` across both metadata eras.

    ``CAPABILITIES.task_metadata_era`` already proved the attribute this
    branch reads exists, so a raising ``source`` cached_property propagates
    instead of being swallowed into ``""`` — no ``getattr`` default.
    """
    if CAPABILITIES.task_metadata_era == "modern":
        source = getattr(task, "source")  # noqa: B009 - task is untyped object; era already proved this attribute exists
        return (source.artist or "", source.name or "")
    return (
        getattr(task, "cur_artist") or "",  # noqa: B009 - task is untyped object; era already proved this attribute exists
        getattr(task, "cur_album") or "",  # noqa: B009 - task is untyped object; era already proved this attribute exists
    )


def capability_report() -> dict[str, str]:
    return {
        "duplicate_era": CAPABILITIES.duplicate_era,
        "library_era": CAPABILITIES.library_era,
        "task_metadata_era": CAPABILITIES.task_metadata_era,
        "era": CAPABILITIES.era,
    }


def _module_callable(module: ModuleType, name: str) -> Callable[..., object]:
    candidate = getattr(module, name)
    if not callable(candidate):
        raise BeetsCapabilityError(f"Beets module callable {name!r} is unavailable")
    return candidate


def _legacy_library_constructor() -> Callable[[str, str, object, object], library.Library]:
    name = "Library"
    candidate = getattr(library, name)
    if not _is_legacy_library_constructor(candidate):
        raise BeetsCapabilityError("legacy Beets Library constructor is unavailable")
    return candidate


def _is_legacy_library_constructor(
    candidate: object,
) -> TypeGuard[Callable[[str, str, object, object], library.Library]]:
    return callable(candidate)
