"""Portable startup contract for an externally owned Beets authority."""

from __future__ import annotations

import errno
import hashlib
import importlib
import os
import pkgutil
import sqlite3
import stat
import sys
from collections.abc import Callable, Generator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol
from urllib.parse import urlsplit

import confuse
import msgspec
import yaml
from beets import IncludeLazyConfig
from beets import __version__ as beets_version

from lib.config import CratediggerConfig

BeetsRole = Literal["main", "importer", "preview", "web"]

SAFE_DEFAULT_PATH = (
    "$albumartist/$year - $album%aunique{albumartist album,path_disambig}/"
    "$track $title"
)
SAFE_COMP_PATH = (
    "Compilations/$album%aunique{albumartist album,path_disambig}/$track $title"
)
SAFE_SINGLETON_PATH = "Non-Album/$artist/$title"
SAFE_PATH_DISAMBIG = (
    "albumdisambig or releasegroupdisambig or catalognum or label or str(year)"
)
SAFE_DUPLICATE_KEYS = frozenset(("mb_albumid", "discogs_albumid"))


class BeetsConfigError(RuntimeError):
    """The configured Beets file set cannot be read exactly as declared."""


class _DuplicateKeyError(ValueError):
    pass


class _UnhashableKeyError(ValueError):
    pass


class ContractFinding(msgspec.Struct, frozen=True):
    """One token-free checker-owned diagnostic."""

    code: str
    message: str


class BeetsAuthority(msgspec.Struct, frozen=True):
    """Resolved deployment-neutral authority admitted by the checker."""

    config_dir: str
    library: str
    directory: str
    state_file: str
    python: str
    secret_include: str
    beets_version: str
    beets_package: str


class BeetsPluginContract(msgspec.Struct, frozen=True):
    """Bounded active-plugin facts safe to expose in owned output."""

    musicbrainz: bool = False
    permissions: bool = False
    inline: bool = False
    discogs: bool = False
    convert: bool = False


class BeetsConfigReport(msgspec.Struct, frozen=True):
    """Stable JSON report; secret values and arbitrary config are excluded."""

    ok: bool
    role: str
    authority: BeetsAuthority
    plugin_contract: BeetsPluginContract
    hard_failures: tuple[ContractFinding, ...]
    warnings: tuple[ContractFinding, ...]
    fingerprint: str


class _UniqueKeyLoader(yaml.SafeLoader):
    pass


class _YamlObjectConstructor(Protocol):
    def construct_object(self, node: yaml.Node, deep: bool = False) -> object: ...


def _construct_yaml_object(
    loader: _YamlObjectConstructor,
    node: yaml.Node,
    *,
    deep: bool,
) -> object:
    return loader.construct_object(node, deep=deep)


@dataclass(frozen=True)
class _DeclaredPath:
    """One path's lexical identity and resolved target."""

    lexical: Path
    resolved: Path


def _construct_unique_mapping(
    loader: _UniqueKeyLoader,
    node: yaml.MappingNode,
    deep: bool = False,
) -> dict[object, object]:
    result: dict[object, object] = {}
    for key_node, value_node in node.value:
        key = _construct_yaml_object(loader, key_node, deep=deep)
        try:
            duplicate = key in result
        except TypeError as exc:
            raise _UnhashableKeyError("unhashable YAML mapping key") from exc
        if duplicate:
            raise _DuplicateKeyError(f"duplicate YAML key: {key!r}")
        result[key] = _construct_yaml_object(loader, value_node, deep=deep)
    return result


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def _path(value: str) -> Path:
    try:
        return Path(value).expanduser().resolve()
    except (OSError, RuntimeError, ValueError) as exc:
        raise BeetsConfigError(
            f"invalid Beets authority path: {type(exc).__name__}: {exc}"
        ) from exc


def _invocation_path(value: str) -> Path:
    """Normalize an executable spelling without resolving its environment.

    A conventional virtualenv's ``bin/python`` is a symlink to the base
    interpreter.  Resolving that symlink changes Python's prefix discovery and
    can therefore select a different site-packages set when a child is
    launched.  The admitted authority is the exact path used to invoke this
    process, not merely another directory entry for the same executable inode.
    """
    try:
        expanded = Path(value).expanduser()
        lexical = Path(os.path.abspath(expanded))
        if "\x00" in os.fspath(lexical):
            raise ValueError("embedded null byte")
        return lexical
    except (OSError, RuntimeError, ValueError) as exc:
        raise BeetsConfigError(
            f"invalid Beets authority path: {type(exc).__name__}: {exc}"
        ) from exc


def _declared_path(value: str, *, relative_to: Path | None = None) -> _DeclaredPath:
    try:
        expanded = Path(value).expanduser()
        if not expanded.is_absolute():
            expanded = (
                relative_to if relative_to is not None else Path.cwd()
            ) / expanded
        lexical = Path(os.path.abspath(expanded))
        return _DeclaredPath(lexical=lexical, resolved=lexical.resolve())
    except (OSError, RuntimeError, ValueError) as exc:
        raise BeetsConfigError(
            f"invalid Beets authority path: {type(exc).__name__}: {exc}"
        ) from exc


def _authority(cfg: CratediggerConfig) -> BeetsAuthority:
    beets_module = importlib.import_module("beets")
    module_file = getattr(beets_module, "__file__", "") or ""
    return BeetsAuthority(
        config_dir=str(_path(cfg.beets_config_dir)) if cfg.beets_config_dir else "",
        library=str(_path(cfg.beets_library_db)) if cfg.beets_library_db else "",
        directory=str(_path(cfg.beets_directory)) if cfg.beets_directory else "",
        state_file=str(_path(cfg.beets_state_file)) if cfg.beets_state_file else "",
        python=(
            str(_invocation_path(cfg.beets_python)) if cfg.beets_python else ""
        ),
        secret_include=(
            str(_path(cfg.beets_secret_include)) if cfg.beets_secret_include else ""
        ),
        beets_version=beets_version,
        beets_package=str(Path(module_file).resolve().parent) if module_file else "",
    )


def _finding(code: str, message: str) -> ContractFinding:
    return ContractFinding(code=code, message=message)


def _can_open_for_write(path: Path) -> bool:
    try:
        fd = os.open(path, os.O_WRONLY | getattr(os, "O_CLOEXEC", 0))
    except PermissionError:
        return False
    except OSError as exc:
        if exc.errno == errno.EROFS:
            return False
        raise BeetsConfigError(
            f"cannot determine whether Beets authority is writable {path}: "
            f"{type(exc).__name__}: {exc}"
        ) from exc
    else:
        os.close(fd)
        return True


def _entry_is_replaceable(path: Path) -> bool:
    """Whether this identity can rename or replace one directory entry."""
    if path == path.parent:
        return False
    parent = path.parent
    if not os.access(parent, os.W_OK | os.X_OK):
        return False
    try:
        parent_stat = parent.stat()
        entry_stat = path.lstat()
    except OSError:
        return True
    if not parent_stat.st_mode & stat.S_ISVTX:
        return True
    uid = os.geteuid()
    return uid == 0 or uid in (parent_stat.st_uid, entry_stat.st_uid)


def _has_replaceable_component(path: Path) -> bool:
    """Check every entry from the filesystem root through the declared leaf."""
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        if _entry_is_replaceable(current):
            return True
    return False


def _has_app_owned_component(path: Path) -> bool:
    """Whether the application UID owns any entry in this declared path."""
    uid = os.geteuid()
    current = Path(path.anchor)
    try:
        if current.lstat().st_uid == uid:
            return True
    except OSError:
        return True
    for part in path.parts[1:]:
        current /= part
        try:
            if current.lstat().st_uid == uid:
                return True
        except OSError:
            return True
    return False


def _has_app_owned_ancestor(path: Path) -> bool:
    """Whether the application UID can chmod a parent then replace this entry."""
    return _has_app_owned_component(path.parent)


def _immutable_declared_file(path: _DeclaredPath) -> bool:
    return (
        not _can_open_for_write(path.resolved)
        and not _has_replaceable_component(path.lexical)
        and not _has_replaceable_component(path.resolved)
        and not _has_app_owned_component(path.lexical)
        and not _has_app_owned_component(path.resolved)
    )


def _nonreplaceable_declared_path(path: _DeclaredPath) -> bool:
    return (
        not _has_replaceable_component(path.lexical)
        and not _has_replaceable_component(path.resolved)
        and not _has_app_owned_ancestor(path.lexical)
        and not _has_app_owned_ancestor(path.resolved)
    )


def _read_yaml_mapping(path: Path) -> dict[str, object]:
    try:
        raw = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise BeetsConfigError(
            f"cannot read Beets config {path}: {type(exc).__name__}: {exc}"
        ) from exc
    try:
        value: object = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        raise BeetsConfigError(
            f"invalid Beets YAML {path}: {type(exc).__name__}"
        ) from None
    if value is None:
        return {}
    try:
        return msgspec.convert(value, type=dict[str, object])
    except msgspec.ValidationError as exc:
        raise BeetsConfigError(f"Beets config {path} must contain a mapping") from exc


def _read_secret(path: Path) -> tuple[dict[str, object] | None, ContractFinding | None]:
    try:
        raw = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise BeetsConfigError(
            f"cannot read Beets config {path}: {type(exc).__name__}: {exc}"
        ) from exc
    try:
        value: object = yaml.load(raw, Loader=_UniqueKeyLoader)
    except _DuplicateKeyError:
        return None, _finding(
            "secret_duplicate_key",
            f"designated Beets secret include {path} contains a duplicate key",
        )
    except _UnhashableKeyError as exc:
        raise BeetsConfigError(
            f"invalid Beets YAML {path}: unhashable YAML mapping key"
        ) from exc
    except yaml.YAMLError as exc:
        raise BeetsConfigError(
            f"invalid Beets YAML {path}: {type(exc).__name__}"
        ) from None
    try:
        mapping = msgspec.convert(value, type=dict[str, object])
    except msgspec.ValidationError:
        return None, _finding(
            "secret_schema",
            "designated Beets secret include must contain only discogs.user_token",
        )
    if set(mapping) != {"discogs"}:
        return mapping, _finding(
            "secret_schema",
            "designated Beets secret include must contain only discogs.user_token",
        )
    try:
        discogs = msgspec.convert(
            mapping.get("discogs"),
            type=dict[str, object],
        )
    except msgspec.ValidationError:
        return mapping, _finding(
            "secret_schema",
            "designated Beets secret include must contain only discogs.user_token",
        )
    if set(discogs) != {"user_token"}:
        return mapping, _finding(
            "secret_schema",
            "designated Beets secret include must contain only discogs.user_token",
        )
    token = discogs.get("user_token")
    if not isinstance(token, str) or not token.strip():
        return mapping, _finding(
            "discogs_token_missing",
            "designated Beets secret include must contain a nonempty scalar token",
        )
    return mapping, None


def _declares_discogs_token(config: dict[str, object]) -> bool:
    discogs = config.get("discogs")
    return isinstance(discogs, dict) and "user_token" in discogs


def _declared_includes(
    config_dir: _DeclaredPath,
    designated_secret: _DeclaredPath,
) -> tuple[tuple[Path, ...], tuple[ContractFinding, ...]]:
    main = _declared_path("config.yaml", relative_to=config_dir.lexical)
    config = _read_yaml_mapping(main.lexical)
    issues: list[ContractFinding] = []
    if _declares_discogs_token(config):
        issues.append(_finding(
            "discogs_token_outside_secret_include",
            "discogs.user_token may only be declared by the designated secret include",
        ))
    if not _immutable_declared_file(main):
        issues.append(_finding(
            "mutable_main_config",
            "Beets main config is writable or replaceable by this process",
        ))
    declared: object = config.get("include", [])
    if not isinstance(declared, list):
        return (), (_finding(
            "include_shape",
            "Beets include must be a YAML list of path strings",
        ),)
    try:
        declared_paths = msgspec.convert(declared, type=list[str])
    except msgspec.ValidationError:
        return (), (_finding(
            "include_shape",
            "Beets include must be a YAML list of path strings",
        ),)
    include_sources = tuple(
        _declared_path(value, relative_to=config_dir.lexical)
        for value in declared_paths
    )
    includes = tuple(source.resolved for source in include_sources)
    count = sum(
        source.resolved == designated_secret.resolved for source in include_sources
    )
    if count != 1:
        issues.append(_finding(
            "secret_include_count",
            "Beets config must include the designated secret path exactly once",
        ))
    secret_source_mutable = not _immutable_declared_file(designated_secret)
    if secret_source_mutable:
        issues.append(_finding(
            "mutable_secret_include",
            "designated Beets secret include is writable or replaceable",
        ))
    for source in include_sources:
        include = source.lexical
        if source.resolved == designated_secret.resolved:
            if not _immutable_declared_file(source) and not secret_source_mutable:
                issues.append(_finding(
                    "mutable_secret_include",
                    "designated Beets secret include is writable or replaceable",
                ))
            included_config, secret_issue = _read_secret(include)
            if included_config is not None and "include" in included_config:
                issues.append(_finding(
                    "included_include_forbidden",
                    "included Beets config files may not declare include",
                ))
            if secret_issue is not None:
                issues.append(secret_issue)
        else:
            included_config = _read_yaml_mapping(include)
            if _declares_discogs_token(included_config):
                issues.append(_finding(
                    "discogs_token_outside_secret_include",
                    "discogs.user_token may only be declared by the designated secret include",
                ))
            if "include" in included_config:
                issues.append(_finding(
                    "included_include_forbidden",
                    "included Beets config files may not declare include",
                ))
            if not _immutable_declared_file(source):
                issues.append(_finding(
                    "mutable_include",
                    "non-secret Beets include is writable or replaceable",
                ))
    return includes, tuple(issues)


@contextmanager
def _effective_config(config_dir: Path) -> Generator[IncludeLazyConfig]:
    """Read Beets' exact Confuse view in a fresh, isolated config object."""
    prior = os.environ.get("BEETSDIR")
    os.environ["BEETSDIR"] = str(config_dir)
    try:
        active = IncludeLazyConfig("beets", "beets")
        active.read(user=True, defaults=True)
        yield active
    finally:
        if prior is None:
            os.environ.pop("BEETSDIR", None)
        else:
            os.environ["BEETSDIR"] = prior


def _available_plugins() -> frozenset[str]:
    """Enumerate only the admitted Beets package's plugin directory."""
    beets_module = importlib.import_module("beets")
    module_file = getattr(beets_module, "__file__", "") or ""
    if not module_file:
        return frozenset()
    plugin_dir = Path(module_file).resolve().parent.parent / "beetsplug"
    return frozenset(
        module.name for module in pkgutil.iter_modules((str(plugin_dir),))
    )


def _active_plugin_names(active: IncludeLazyConfig) -> tuple[str, ...]:
    """Mirror pinned Beets get_plugin_names without mutating global paths."""
    plugins = tuple(dict.fromkeys(active["plugins"].as_str_seq()))
    try:
        disabled: set[str] = set(active["disabled_plugins"].as_str_seq())
    except confuse.NotFoundError:
        disabled = set()
    mb_enabled = active["musicbrainz"].flatten().get("enabled")
    if mb_enabled and "musicbrainz" not in plugins:
        plugins += ("musicbrainz",)
    elif mb_enabled is False:
        disabled.add("musicbrainz")
    return tuple(plugin for plugin in plugins if plugin not in disabled)


def _plugin_contract(plugins: frozenset[str]) -> BeetsPluginContract:
    return BeetsPluginContract(
        musicbrainz="musicbrainz" in plugins,
        permissions="permissions" in plugins,
        inline="inline" in plugins,
        discogs="discogs" in plugins,
        convert="convert" in plugins,
    )


def _optional_bool(active: IncludeLazyConfig, section: str, key: str) -> bool:
    try:
        return active[section][key].get(bool)
    except confuse.NotFoundError:
        return False


def _same_executable(expected: str) -> bool:
    if not expected:
        return False
    return _invocation_path(expected) == _invocation_path(sys.executable)


def _state_access_issues(
    state_source: _DeclaredPath,
    role: BeetsRole,
) -> tuple[ContractFinding, ...]:
    state = state_source.resolved
    if not state.is_absolute():
        return (_finding("state_relative", "Beets state file must be absolute"),)
    issues: list[ContractFinding] = []
    if not _nonreplaceable_declared_path(state_source):
        issues.append(_finding(
            "state_replaceable",
            "Beets state path is replaceable by this process",
        ))
    if not state.is_file():
        issues.append(_finding(
            "state_not_regular",
            "Beets state path must be an existing regular file",
        ))
        return tuple(issues)
    try:
        with state.open("rb"):
            pass
    except OSError:
        issues.append(_finding(
            "state_unreadable",
            "Beets state file is not readable by this process",
        ))
        return tuple(issues)
    if role != "importer":
        try:
            state_owned_by_reader = state.stat().st_uid == os.geteuid()
        except OSError:
            state_owned_by_reader = True
        if state_owned_by_reader:
            issues.append(_finding(
                "state_owned_by_reader",
                f"{role} owns the Beets state file and can make it writable",
            ))
    writable = _can_open_for_write(state)
    if role == "importer" and not writable:
        issues.append(_finding(
            "state_not_writable_by_importer",
            "importer cannot open the Beets state file for writing",
        ))
    if role != "importer" and writable:
        issues.append(_finding(
            "state_writable_by_reader",
            f"{role} can open the Beets state file for writing",
        ))
    return tuple(issues)


def _library_db_access_issues(library: Path) -> tuple[ContractFinding, ...]:
    """Prove an existing Beets catalog without creating or locking one."""
    if not library.is_file():
        return (_finding(
            "library_not_regular",
            "Beets library database must be an existing regular file",
        ),)
    try:
        connection = sqlite3.connect(
            f"{library.as_uri()}?mode=ro&immutable=1",
            uri=True,
        )
        try:
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type = 'table' AND name IN ('albums', 'items')"
                )
            }
        finally:
            connection.close()
    except sqlite3.Error:
        return (_finding(
            "library_unreadable",
            "Beets library database is not a readable SQLite catalog",
        ),)
    if tables != {"albums", "items"}:
        return (_finding(
            "library_schema_missing",
            "Beets library database is missing its required catalog tables",
        ),)
    return ()


def _effective_paths(active: IncludeLazyConfig) -> tuple[str, str, str]:
    return (
        str(_path(active["library"].as_filename())),
        str(_path(active["directory"].as_filename())),
        str(_path(active["statefile"].as_filename())),
    )


def _endpoint(active: IncludeLazyConfig) -> str:
    scheme = "https" if active["musicbrainz"]["https"].get(bool) else "http"
    return f"{scheme}://{active['musicbrainz']['host'].as_str()}"


def _report(
    *,
    role: BeetsRole,
    authority: BeetsAuthority,
    plugin_contract: BeetsPluginContract | None = None,
    hard: tuple[ContractFinding, ...] = (),
    warnings: tuple[ContractFinding, ...] = (),
    fingerprint_values: object = None,
    secret_token_present: bool = False,
) -> BeetsConfigReport:
    bounded_plugins = plugin_contract or BeetsPluginContract()
    fingerprint_payload = {
        "authority": msgspec.to_builtins(authority),
        "plugin_contract": msgspec.to_builtins(bounded_plugins),
        "secret_schema": "discogs.user_token",
        "secret_token_present": secret_token_present,
        "contract": fingerprint_values,
    }
    fingerprint = hashlib.sha256(
        msgspec.json.encode(fingerprint_payload, order="sorted")
    ).hexdigest()
    return BeetsConfigReport(
        ok=not hard,
        role=role,
        authority=authority,
        plugin_contract=bounded_plugins,
        hard_failures=hard,
        warnings=warnings,
        fingerprint=fingerprint,
    )


def check_beets_config(
    cfg: CratediggerConfig,
    *,
    role: BeetsRole,
    available_plugins: Callable[[], frozenset[str]] = _available_plugins,
) -> BeetsConfigReport:
    """Validate declared files, then Beets' exact active effective config."""
    authority = _authority(cfg)
    preflight: list[ContractFinding] = []
    required = {
        "config_dir": authority.config_dir,
        "library": authority.library,
        "directory": authority.directory,
        "state_file": authority.state_file,
        "python": authority.python,
        "secret_include": authority.secret_include,
    }
    for name, value in required.items():
        if not value:
            preflight.append(_finding(
                "runtime_authority_missing",
                f"runtime [Beets] authority {name} is required",
            ))
    if cfg.beets_state_file and not os.path.isabs(cfg.beets_state_file):
        preflight.append(_finding("state_relative", "Beets state file must be absolute"))
    if preflight:
        return _report(role=role, authority=authority, hard=tuple(preflight))

    runtime_config = _declared_path(cfg.config_file_path)
    if not runtime_config.lexical.is_file():
        preflight.append(_finding(
            "runtime_config_missing", "strict runtime config file is missing"
        ))
    elif not _immutable_declared_file(runtime_config):
        preflight.append(_finding(
            "mutable_runtime_config",
            "strict runtime config is writable or replaceable by this process",
        ))
    config_dir_source = _declared_path(cfg.beets_config_dir)
    config_dir = config_dir_source.resolved
    secret = _declared_path(cfg.beets_secret_include)
    if not config_dir_source.lexical.is_dir():
        preflight.append(_finding("config_dir_missing", "BEETSDIR must be an existing directory"))
    else:
        _, declared_issues = _declared_includes(config_dir_source, secret)
        preflight.extend(declared_issues)
    if preflight:
        return _report(role=role, authority=authority, hard=tuple(preflight))

    hard: list[ContractFinding] = []
    warnings: list[ContractFinding] = []
    state_source = _declared_path(cfg.beets_state_file)
    state = state_source.resolved
    try:
        if os.path.commonpath((str(config_dir), str(state))) == str(config_dir):
            hard.append(_finding(
                "state_inside_config_dir", "Beets state file must be outside BEETSDIR"
            ))
    except ValueError:
        pass
    hard.extend(_state_access_issues(state_source, role))
    library_path = _path(authority.library)
    try:
        state_aliases_library = os.path.samefile(state, library_path)
    except OSError:
        state_aliases_library = False
    if state_aliases_library:
        hard.append(_finding(
            "state_library_alias",
            "Beets state file must not alias the library database",
        ))
    hard.extend(_library_db_access_issues(library_path))
    if not _path(authority.directory).is_dir():
        hard.append(_finding(
            "directory_not_directory",
            "Beets library root must be an existing directory",
        ))
    python_source = _declared_path(authority.python)
    if not _immutable_declared_file(python_source):
        hard.append(_finding(
            "mutable_python",
            "[Beets] Python interpreter is writable or replaceable by this process",
        ))
    if not _same_executable(authority.python):
        hard.append(_finding(
            "python_mismatch", "active Python interpreter differs from [Beets] python"
        ))

    with _effective_config(config_dir) as active:
        effective_state_raw = active["statefile"].as_str()
        if not Path(effective_state_raw).is_absolute():
            hard.append(_finding(
                "effective_state_relative",
                "effective Beets statefile must be declared as an absolute path",
            ))
        library, directory, effective_state = _effective_paths(active)
        for code, actual, expected, label in (
            ("library_mismatch", library, authority.library, "library database"),
            ("directory_mismatch", directory, authority.directory, "library root"),
            ("state_mismatch", effective_state, authority.state_file, "state file"),
        ):
            if actual != expected:
                hard.append(_finding(code, f"effective Beets {label} differs from runtime authority"))

        plugins = _active_plugin_names(active)
        configured = frozenset(plugins)
        plugin_contract = _plugin_contract(configured)
        plugin_paths = tuple(active["pluginpath"].as_str_seq(split=False))
        if plugin_paths:
            hard.append(_finding(
                "pluginpath_unsupported",
                "effective Beets pluginpath must be empty",
            ))
        available_plugin_names = available_plugins()
        if configured - available_plugin_names:
            hard.append(_finding(
                "plugin_unavailable",
                "one or more configured Beets plugins are unavailable",
            ))
        for plugin in ("musicbrainz", "permissions", "inline"):
            if plugin not in configured:
                hard.append(_finding(
                    f"{plugin}_plugin_missing", f"required Beets plugin is not active: {plugin}"
                ))

        if "discogs" in configured:
            token = active["discogs"]["user_token"].as_str()
            if not token.strip():
                hard.append(_finding(
                    "discogs_token_missing", "active Discogs plugin requires a nonempty token"
                ))

        duplicate_keys = active["import"]["duplicate_keys"]["album"].as_str_seq()
        if len(duplicate_keys) != 2 or frozenset(duplicate_keys) != SAFE_DUPLICATE_KEYS:
            hard.append(_finding(
                "duplicate_keys_unsafe",
                "album duplicate keys must be exactly mb_albumid and discogs_albumid",
            ))
        for key in ("autotag", "move", "write"):
            if active["import"][key].get(bool) is not True:
                hard.append(_finding(
                    f"import_{key}_disabled", f"effective import.{key} must be true"
                ))

        path_formats = active["paths"].flatten()
        if set(path_formats) != {"default", "singleton", "comp"}:
            hard.append(_finding(
                "paths_keys_unsupported",
                "effective Beets paths contains unsupported query-specific formats",
            ))
        default_path = active["paths"]["default"].as_str()
        singleton_path = active["paths"]["singleton"].as_str()
        comp_path = active["paths"]["comp"].as_str()
        path_disambig = active["album_fields"]["path_disambig"].as_str()
        if default_path != SAFE_DEFAULT_PATH:
            hard.append(_finding("default_path_unsafe", "effective default album path is not admitted"))
        if singleton_path != SAFE_SINGLETON_PATH:
            hard.append(_finding(
                "singleton_path_unsafe",
                "effective singleton path is not admitted",
            ))
        if comp_path != SAFE_COMP_PATH:
            hard.append(_finding("comp_path_unsafe", "effective compilation album path is not admitted"))
        if path_disambig != SAFE_PATH_DISAMBIG:
            hard.append(_finding("path_disambig_unsafe", "effective path_disambig expression is not admitted"))

        if active["permissions"]["file"].as_str() != "0664":
            hard.append(_finding("permissions_file_unsafe", "effective permissions.file must be 0664"))
        if active["permissions"]["dir"].as_str() != "02775":
            hard.append(_finding("permissions_dir_unsafe", "effective permissions.dir must be 02775"))
        if "convert" in configured:
            for key in ("auto", "auto_keep"):
                if _optional_bool(active, "convert", key):
                    hard.append(_finding(
                        f"convert_{key}_conflict",
                        f"convert.{key} conflicts with the Cratedigger import harness",
                    ))

        configured_endpoint = _endpoint(active)
        expected_endpoint = cfg.musicbrainz_api_base.rstrip("/")
        try:
            parsed_expected = urlsplit(expected_endpoint)
            # Accessing port also validates malformed bracket/port syntax.
            _ = parsed_expected.port
            endpoint_matches = (
                parsed_expected.scheme in ("http", "https")
                and bool(parsed_expected.netloc)
                and configured_endpoint == expected_endpoint
            )
        except ValueError:
            endpoint_matches = False
        if not endpoint_matches:
            warnings.append(_finding(
                "musicbrainz_endpoint_drift",
                "effective MusicBrainz endpoint differs from the runtime authority",
            ))

        fingerprint_values = {
            "import": {key: active["import"][key].get(bool) for key in ("autotag", "move", "write")},
            "duplicate_keys": tuple(duplicate_keys),
            "paths": (default_path, singleton_path, comp_path, path_disambig),
            "permissions": (
                active["permissions"]["file"].as_str(),
                active["permissions"]["dir"].as_str(),
            ),
            "musicbrainz_endpoint": configured_endpoint,
        }

    return _report(
        role=role,
        authority=authority,
        plugin_contract=plugin_contract,
        hard=tuple(hard),
        warnings=tuple(warnings),
        fingerprint_values=fingerprint_values,
        secret_token_present=True,
    )
