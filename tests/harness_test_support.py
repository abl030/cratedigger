"""Import harness modules against synthetic Beets modules without global leaks."""

from __future__ import annotations

import importlib
import json
import os
import sys
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from types import ModuleType
from unittest.mock import MagicMock

_HARNESS_MODULES = (
    "harness.beets_harness",
    "harness.discogs_patches",
    "harness.beets_compat",
)


def legacy_import_task_stub() -> type:
    """A bare ``ImportTask`` stand-in carrying the legacy task-metadata
    attributes. Every harness mock-module fixture that swaps in a synthetic
    ``beets.importer.tasks.ImportTask`` needs one attribute present or
    ``beets_compat.py``'s era ambiguity check (issue #1088) fails closed."""
    return type("ImportTask", (object,), {"cur_artist": None, "cur_album": None})


def modern_album_stub() -> type:
    """A bare ``library.Album`` stand-in pinning the modern duplicates-query
    era. Every harness mock-module fixture that swaps in a synthetic
    ``beets.library`` needs exactly one of the two duplicate-lookup builders
    present or ``beets_compat.py``'s era ambiguity check (#1278 wx6) fails
    closed — a bare ``MagicMock`` manufactures BOTH via auto-attributes."""
    return type("Album", (object,), {"duplicates_query": lambda self, keys: None})


def beets_module_mocks() -> dict[str, MagicMock]:
    """The synthetic beets module set the harness unit tests import against.

    Shared by ``tests/test_harness_serialization.py`` and
    ``tests/test_harness_wire_contract_audit.py`` — the real-beets import +
    API contract lives in ``tests/test_harness_beets2_contract.py``. The
    customizations keep ``beets_compat``'s era detection deterministic:
    the ``beets.ui`` legacy getters are set to None so exactly one library
    era (modern) is detected, a real ``ImportSession`` class exposes only
    ``resolve_duplicate`` (legacy duplicate era, and subclassing works),
    ``legacy_import_task_stub`` pins the legacy task-metadata era, and
    ``modern_album_stub`` pins the modern duplicates-query era.
    Callers may add further attributes to the returned mocks before
    entering ``isolated_beets_harness``.
    """
    mocks: dict[str, MagicMock] = {
        name: MagicMock()
        for name in (
            "beets",
            "beets.config",
            "beets.library",
            "beets.plugins",
            "beets.ui",
            "beets.importer",
            "beets.importer.actions",
            "beets.importer.session",
            "beets.importer.tasks",
            "beets.autotag",
            "beets.dbcore",
            "beets.util",
        )
    }
    mocks["beets.ui"].get_path_formats = None
    mocks["beets.ui"].get_replacements = None
    mocks["beets.importer.session"].ImportSession = type(
        "ImportSession", (object,), {"resolve_duplicate": lambda *_args: None},
    )
    mocks["beets.importer.tasks"].ImportTask = legacy_import_task_stub()
    mocks["beets.library"].Album = modern_album_stub()
    # ``from beets import config, library, plugins`` (beets_harness.py)
    # resolves the PARENT module's attributes, which on a bare MagicMock
    # parent are divergent auto-children, not the sys.modules entries the
    # stubs above were pinned on — bind all three names the statement
    # imports, as the real package machinery would. Dotted-module imports
    # (``importlib``, ``from beets.autotag import ...``) already resolve
    # the sys.modules entries and need no binding.
    mocks["beets"].config = mocks["beets.config"]
    mocks["beets"].library = mocks["beets.library"]
    mocks["beets"].plugins = mocks["beets.plugins"]
    return mocks


@contextmanager
def isolated_beets_harness(modules: Mapping[str, ModuleType]) -> Generator[ModuleType]:
    """Return a fresh harness bound to mocks, restoring all import state after."""
    package_existed = "harness" in sys.modules
    package = importlib.import_module("harness")
    module_names = (*modules, *_HARNESS_MODULES)
    prior_modules = {name: sys.modules[name] for name in module_names if name in sys.modules}
    prior_parent_attributes: list[tuple[object, str, bool, object]] = []
    for name in modules:
        parent_name, _, attribute = name.rpartition(".")
        parent = sys.modules.get(parent_name)
        if parent is not None:
            parent_vars = vars(parent)
            prior_parent_attributes.append(
                (parent, attribute, attribute in parent_vars, parent_vars.get(attribute)),
            )
    prior_attributes = {
        name.rpartition(".")[2]: vars(package)[name.rpartition(".")[2]]
        for name in _HARNESS_MODULES
        if name.rpartition(".")[2] in vars(package)
    }
    for name in module_names:
        sys.modules.pop(name, None)
    for attribute in _HARNESS_MODULES:
        vars(package).pop(attribute.rpartition(".")[2], None)
    sys.modules.update(modules)
    try:
        yield importlib.import_module("harness.beets_harness")
    finally:
        for name in module_names:
            sys.modules.pop(name, None)
        sys.modules.update(prior_modules)
        for parent, attribute, existed, previous in prior_parent_attributes:
            if existed:
                setattr(parent, attribute, previous)
            else:
                vars(parent).pop(attribute, None)
        if package_existed:
            for attribute in _HARNESS_MODULES:
                vars(package).pop(attribute.rpartition(".")[2], None)
            vars(package).update(prior_attributes)
        else:
            sys.modules.pop("harness", None)


# ---------------------------------------------------------------------------
# Real-Beets candidate injection for the pretend/incremental contract shims
# (issue #1088). Shared by tests/test_harness_beets2_contract.py's
# ``_PRETEND_SOURCE_PURITY_CONTRACT`` and ``_MATRIX_EXTERNAL_STATEFILE_
# CONTRACT`` — both write this SAME sitecustomize source into a subprocess
# whose PYTHONPATH puts it ahead of the real harness wrapper, so a single
# fake AlbumInfo candidate reaches Beets' real importer without depending
# on a live MusicBrainz service.
# ---------------------------------------------------------------------------

CANDIDATE_INJECTION_ARTIST = "Purity Artist"
CANDIDATE_INJECTION_ALBUM = "Purity Album"
CANDIDATE_INJECTION_ALBUM_ID = "11111111-2222-3333-4444-555555555555"
CANDIDATE_INJECTION_TRACK_TITLE = "Source"
CANDIDATE_INJECTION_TRACK_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


def write_candidate_injection_sitecustomize(shim_dir: str, receipt_path: str) -> None:
    """Write a ``sitecustomize.py`` that installs one fake ``AlbumInfo``.

    Enumerates the two named upstream candidate-injection seams — legacy
    ``beets.autotag.hooks.album_candidates`` (v2.1.0-v2.3.1) and modern
    ``beets.metadata_plugins.candidates`` (v2.4.0-tip; byte-identical to
    the admitted 2.13.1) — and asserts EXACTLY ONE exists before patching
    it, failing closed in both directions. Both are reached via plain
    module-attribute access from beets' own ``match.py``, so replacing the
    module attribute is visible to the real importer regardless of era;
    the injected candidate accepts either call shape (legacy passes 5
    positional args including ``extra_tags``, modern passes 4).

    CPython's ``site.execsitecustomize`` reduces ANY uncaught exception
    raised here to a single non-fatal stderr line — never a subprocess
    failure — so the pre-fix shim (``from beets.autotag import mb``, a
    module removed in beets 2.4.0) silently installed NOTHING on every
    admitted or tip Beets: zero real candidates ever reached a task, and
    the contract's own claim that it "supplies one structurally valid
    provider result" was false for ~11 months with nothing failing
    (test-fidelity.md Rule C). The receipt file at ``receipt_path`` is the
    only thing that can prove a seam actually installed — callers MUST
    assert it exists via :func:`read_candidate_injection_receipt`.
    """
    os.makedirs(shim_dir, exist_ok=True)
    content = f'''\
import json


def _install():
    def _candidates(items, artist, album, va_likely, extra_tags=None):
        from beets.autotag.hooks import AlbumInfo, TrackInfo
        return [AlbumInfo(
            album={CANDIDATE_INJECTION_ALBUM!r},
            artist={CANDIDATE_INJECTION_ARTIST!r},
            album_id={CANDIDATE_INJECTION_ALBUM_ID!r},
            tracks=[TrackInfo(
                title={CANDIDATE_INJECTION_TRACK_TITLE!r},
                artist={CANDIDATE_INJECTION_ARTIST!r},
                track_id={CANDIDATE_INJECTION_TRACK_ID!r},
                index=1,
            )],
        )]

    try:
        from beets import metadata_plugins as _modern
    except ImportError:
        _modern = None
    modern_seam = _modern is not None and hasattr(_modern, "candidates")

    try:
        from beets.autotag import hooks as _legacy
    except ImportError:
        _legacy = None
    legacy_seam = _legacy is not None and hasattr(_legacy, "album_candidates")

    if modern_seam == legacy_seam:
        raise RuntimeError(
            "candidate-injection seam is ambiguous: modern=%r legacy=%r"
            % (modern_seam, legacy_seam))

    if modern_seam:
        _modern.candidates = _candidates
        seam = "modern"
    else:
        _legacy.album_candidates = _candidates
        seam = "legacy"

    with open({receipt_path!r}, "w", encoding="utf-8") as handle:
        json.dump({{"seam": seam}}, handle)


_install()
'''
    with open(os.path.join(shim_dir, "sitecustomize.py"), "w", encoding="utf-8") as handle:
        handle.write(content)


def read_candidate_injection_receipt(receipt_path: str) -> dict[str, object]:
    """Read and validate the receipt ``write_candidate_injection_sitecustomize``
    writes.

    Asserts (not a bare ``FileNotFoundError``) so a failure names exactly
    what's missing — the receipt's existence IS the proof the shim ran at
    all, since ``site.execsitecustomize`` swallows its own traceback.
    """
    assert os.path.exists(receipt_path), (
        f"candidate-injection receipt missing at {receipt_path} — the "
        "sitecustomize shim did not run to completion (or raised before "
        "writing it); site.execsitecustomize swallows that traceback")
    with open(receipt_path, encoding="utf-8") as handle:
        receipt: dict[str, object] = json.load(handle)
    assert receipt.get("seam") in ("modern", "legacy"), receipt
    return receipt
