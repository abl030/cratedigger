"""The single action-time quality-evidence action-file writer (issue #859).

Both the importer (``lib.dispatch.evidence_gate``) and the preview worker
(``lib.import_preview``) hand ``import_one.py`` a
``QualityEvidenceActionPayload`` via ``--quality-evidence-action-file``.
Before this module existed the two call sites had DIVERGED: the importer
wrote to a private ``tempfile.NamedTemporaryFile`` outside any album
directory, while the preview worker wrote ``preview-spectral-evidence.json``
directly INTO whatever directory it was previewing. Once #858 pointed
automation preview at the Cratedigger-owned canonical album under
``processing/albums/``, that second writer poisoned the canonical
directory: ``_materialize_processing_dir``'s ``_canonical_manifest_complete``
requires EXACT set equality between the directory listing and the download
manifest, so the leaked sidecar made every rematerialize attempt return
``MaterializeGuarded`` and stalled the request in ``downloading`` forever.

This module is the ONE writer (".claude/rules/code-quality.md" § "No
Parallel Code Paths"). A canonical processing album must remain an exact
media manifest — no preview JSON, action file, or other control-plane
artifact ever belongs inside it, whatever preview or import action ran
against it. The fix is relocating the sidecar outside every album
directory, never allowlisting it into the manifest-completeness check.

Watch import cycles: this module imports only from ``lib.quality`` and the
stdlib — never ``lib.dispatch`` or ``lib.import_preview``, both of which
import this module.
"""

from __future__ import annotations

import logging
import os
import tempfile

import msgspec

from lib.quality import QualityEvidenceActionPayload

logger = logging.getLogger("cratedigger")


def write_quality_evidence_action_file(payload: QualityEvidenceActionPayload) -> str:
    """Write the action-time evidence payload consumed by ``import_one.py``.

    Always a private ``tempfile.NamedTemporaryFile`` OUTSIDE any album
    directory — never inside a Cratedigger-owned processing album (see
    module docstring).
    """
    handle = tempfile.NamedTemporaryFile(
        prefix="cratedigger-quality-evidence-action-",
        suffix=".json",
        delete=False,
    )
    try:
        handle.write(msgspec.json.encode(payload))
        return handle.name
    finally:
        handle.close()


def remove_quality_evidence_action_file(path: str | None) -> None:
    """Best-effort cleanup of a written action file — never raises."""
    if not path:
        return
    try:
        os.unlink(path)
    except OSError:
        logger.debug(
            "Failed to remove quality evidence action file %s",
            path,
            exc_info=True,
        )
