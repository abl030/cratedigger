"""The small structural compatibility boundary for supported Beets releases.

This module intentionally owns the upstream seams Cratedigger calls. Nothing
outside it decides an upstream era from a version string.
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from types import ModuleType
from typing import TYPE_CHECKING

from beets import library

if TYPE_CHECKING:
    from beets.importer.actions import DuplicateAction


class BeetsCapabilityError(RuntimeError):
    """The loaded Beets does not expose one complete supported capability set."""


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


@dataclass(frozen=True)
class BeetsCapabilities:
    importer_session: type[object]
    import_task: type[object]
    action: object
    duplicate_action: object | None
    path_bytes: type[bytes]
    duplicate_era: str
    library_era: str

    @property
    def era(self) -> str:
        return f"{self.duplicate_era}-duplicates/{self.library_era}-library"


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
    )


CAPABILITIES = _load_capabilities()


def configured_library(config: object) -> library.Library:
    """Open a library preserving the active Beets era's configured paths."""
    ui = _required_module("beets.ui")
    item = getattr(config, "__getitem__")
    library_path = item("library").as_filename()
    directory = item("directory").as_filename()
    if CAPABILITIES.library_era == "modern":
        result = library.Library(library_path, directory)
        if not getattr(result, "path_formats", None) or getattr(result, "replacements", None) is None:
            raise BeetsCapabilityError("modern Library did not derive configured paths/replacements")
        return result
    path_formats = getattr(ui, "get_path_formats")(item("paths"))
    replacements = getattr(ui, "get_replacements")()
    return getattr(library, "Library")(library_path, directory, path_formats, replacements)


def duplicate_outcome(decision: str, task: object) -> DuplicateAction | None:
    """Apply Cratedigger's ``remove|skip`` decision to the active hook era."""
    remove = decision == "remove"
    if CAPABILITIES.duplicate_era == "legacy":
        setattr(task, "should_remove_duplicates", remove)
        return None
    if CAPABILITIES.duplicate_action is None:
        raise BeetsCapabilityError("modern duplicate hook has no DuplicateAction enum")
    return getattr(CAPABILITIES.duplicate_action, "REMOVE" if remove else "SKIP")


def capability_report() -> dict[str, str]:
    return {
        "duplicate_era": CAPABILITIES.duplicate_era,
        "library_era": CAPABILITIES.library_era,
        "era": CAPABILITIES.era,
    }
