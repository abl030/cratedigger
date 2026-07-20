"""Pre-mutation validation for the active Beets configuration envelope."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


class BeetsConfigError(RuntimeError):
    """The configured Beets file set cannot be read exactly as declared."""


def _read_mapping(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise BeetsConfigError(
            f"cannot read Beets config {path}: {type(exc).__name__}: {exc}"
        ) from exc
    try:
        value = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        raise BeetsConfigError(f"invalid Beets YAML {path}: {exc}") from exc
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise BeetsConfigError(f"Beets config {path} must contain a mapping")
    return value


def validate_beets_config(config_dir: str) -> None:
    """Require the main config and every declared include to be readable.

    Beets logs an unreadable ``include`` and continues with reduced/default
    plugin configuration. That fail-open behavior is inappropriate before a
    destructive library operation: #777 showed it can enter plugin OAuth,
    consume stdin, and pollute a child protocol while still proceeding.
    """
    if not config_dir:
        raise BeetsConfigError("BEETSDIR is empty")
    root = Path(config_dir)
    config_path = root / "config.yaml"
    config = _read_mapping(config_path)
    declared = config.get("include", ())
    if isinstance(declared, str):
        includes = (declared,)
    elif isinstance(declared, list) and all(
        isinstance(item, str) for item in declared
    ):
        includes = tuple(declared)
    elif declared in (None, ()):
        includes = ()
    else:
        raise BeetsConfigError(
            f"Beets config {config_path} include must be a string or list"
        )
    for declared_path in includes:
        include_path = Path(declared_path)
        if not include_path.is_absolute():
            include_path = root / include_path
        _read_mapping(include_path)
