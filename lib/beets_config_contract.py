"""Pre-mutation validation for the active Beets configuration envelope."""

from __future__ import annotations

from pathlib import Path
import subprocess as sp
from typing import Any

import yaml


class BeetsConfigError(RuntimeError):
    """The configured Beets file set cannot be read exactly as declared."""


def _read_mapping(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
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


def _plugin_names(value: object, *, source: Path) -> frozenset[str]:
    if value in (None, "", []):
        return frozenset()
    if isinstance(value, str):
        return frozenset(value.split())
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return frozenset(value)
    raise BeetsConfigError(
        f"Beets config {source} plugins must be a string or list"
    )


def validate_beets_config(config_dir: str) -> frozenset[str]:
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
    configured_plugins = _plugin_names(config.get("plugins"), source=config_path)
    for declared_path in includes:
        include_path = Path(declared_path)
        if not include_path.is_absolute():
            include_path = root / include_path
        included = _read_mapping(include_path)
        # Beets/Confuse gives later includes higher priority. A declared
        # ``plugins`` value therefore replaces, rather than extends, the
        # lower-priority value from config.yaml or an earlier include.
        if "plugins" in included:
            configured_plugins = _plugin_names(
                included["plugins"], source=include_path,
            )
    return configured_plugins


def validate_beets_plugins_loaded(
    beet: str,
    env: dict[str, str],
    configured_plugins: frozenset[str],
    *,
    timeout: int,
) -> None:
    """Require pinned Beets to load every configured plugin before mutation."""
    if not configured_plugins:
        return
    try:
        proc = sp.run(
            [beet, "version"],
            capture_output=True,
            text=True,
            errors="replace",
            timeout=timeout,
            env=env,
            stdin=sp.DEVNULL,
        )
    except (sp.TimeoutExpired, OSError) as exc:
        raise BeetsConfigError(
            f"Beets plugin preflight failed: {type(exc).__name__}: {exc}"
        ) from exc
    if proc.returncode != 0:
        detail = (proc.stderr or "").strip().splitlines()
        last = detail[-1] if detail else f"rc={proc.returncode}"
        raise BeetsConfigError(f"Beets plugin preflight failed: {last}")
    plugin_lines = [
        line for line in (proc.stdout or "").splitlines()
        if line.startswith("plugins:")
    ]
    loaded = frozenset(
        name.strip()
        for line in plugin_lines
        for name in line.split(":", 1)[1].split(",")
        if name.strip()
    )
    missing = sorted(configured_plugins - loaded)
    if missing:
        raise BeetsConfigError(
            "configured Beets plugins failed to load: " + ", ".join(missing)
        )
