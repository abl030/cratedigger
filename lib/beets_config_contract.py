"""Pre-mutation validation for the active Beets configuration envelope."""

from __future__ import annotations

from pathlib import Path

import msgspec
import yaml


class BeetsConfigError(RuntimeError):
    """The configured Beets file set cannot be read exactly as declared."""


def _read_mapping(path: Path) -> dict[str, object]:
    try:
        raw = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise BeetsConfigError(
            f"cannot read Beets config {path}: {type(exc).__name__}: {exc}"
        ) from exc
    try:
        value: object = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        raise BeetsConfigError(f"invalid Beets YAML {path}: {exc}") from exc
    if value is None:
        return {}
    try:
        return msgspec.convert(value, type=dict[str, object])
    except msgspec.ValidationError as exc:
        raise BeetsConfigError(
            f"Beets config {path} must contain a mapping"
        ) from exc


def _plugin_names(value: object, *, source: Path) -> frozenset[str]:
    if value in (None, "", []):
        return frozenset()
    if isinstance(value, str):
        return frozenset(value.split())
    if isinstance(value, list):
        try:
            return frozenset(msgspec.convert(value, type=list[str]))
        except msgspec.ValidationError:
            pass
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
    elif isinstance(declared, list):
        try:
            includes = tuple(msgspec.convert(declared, type=list[str]))
        except msgspec.ValidationError as exc:
            raise BeetsConfigError(
                f"Beets config {config_path} include must be a string or list"
            ) from exc
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
