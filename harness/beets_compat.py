"""The small structural compatibility boundary for supported Beets releases.

This module intentionally owns the three upstream seams Cratedigger calls:
importer symbols, configured ``Library`` construction, and duplicate outcomes.
Nothing outside it needs to know an upstream release number.
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Any


class BeetsCapabilityError(RuntimeError):
    """The loaded Beets has a shape we cannot safely mutate through."""


@dataclass(frozen=True)
class BeetsCapabilities:
    importer_session: type[Any]
    import_task: type[Any]
    action: Any
    duplicate_action: Any | None
    path_bytes: type[bytes]
    duplicate_era: str
    library_era: str

    @property
    def era(self) -> str:
        return f"{self.duplicate_era}-duplicates/{self.library_era}-library"


def _load_capabilities() -> BeetsCapabilities:
    try:
        importer = importlib.import_module("beets.importer")
        library = importlib.import_module("beets.library")
        ui = importlib.import_module("beets.ui")
    except ImportError as exc:
        raise BeetsCapabilityError("Beets importer/library modules are unavailable") from exc

    try:
        session_module = importlib.import_module("beets.importer.session")
        task_module = importlib.import_module("beets.importer.tasks")
    except ImportError:
        session_module = importer
        task_module = importer
    session = getattr(session_module, "ImportSession", getattr(importer, "ImportSession", None))
    task = getattr(task_module, "ImportTask", getattr(importer, "ImportTask", None))
    action = getattr(importer, "Action", None)
    if action is None:
        action = getattr(importer, "action", None)
    if not isinstance(session, type) or not isinstance(task, type) or action is None:
        raise BeetsCapabilityError("Beets importer lacks ImportSession, ImportTask, or Action")

    try:
        duplicate_action = getattr(
            importlib.import_module("beets.importer.actions"), "DuplicateAction"
        )
    except ImportError:
        duplicate_action = None

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

    try:
        path_bytes = getattr(importlib.import_module("beets.util"), "PathBytes")
    except (AttributeError, ImportError):
        path_bytes: type[bytes] = bytes
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


def configured_library(config: Any) -> Any:
    """Open a library preserving the active Beets era's configured paths."""
    library: Any = importlib.import_module("beets.library")
    ui: Any = importlib.import_module("beets.ui")

    library_path = config["library"].as_filename()
    directory = config["directory"].as_filename()
    if CAPABILITIES.library_era == "modern":
        result = library.Library(library_path, directory)
        if not getattr(result, "path_formats", None) or getattr(result, "replacements", None) is None:
            raise BeetsCapabilityError("modern Library did not derive configured paths/replacements")
        return result
    path_formats = ui.get_path_formats(config["paths"])
    replacements = ui.get_replacements()
    return library.Library(library_path, directory, path_formats, replacements)


def duplicate_outcome(decision: str, task: Any) -> Any | None:
    """Apply Cratedigger's ``remove|skip`` decision to the active hook era."""
    remove = decision == "remove"
    if CAPABILITIES.duplicate_era == "legacy":
        task.should_remove_duplicates = remove
        return None
    assert CAPABILITIES.duplicate_action is not None
    return CAPABILITIES.duplicate_action.REMOVE if remove else CAPABILITIES.duplicate_action.SKIP


def capability_report() -> dict[str, str]:
    return {
        "duplicate_era": CAPABILITIES.duplicate_era,
        "library_era": CAPABILITIES.library_era,
        "era": CAPABILITIES.era,
    }
