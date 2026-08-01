"""One startup-only adapter for Cratedigger's external Beets contract."""

from __future__ import annotations

import configparser
import logging
from dataclasses import replace

import confuse
import msgspec

from lib.beets_config_contract import (
    BeetsConfigError,
    BeetsRole,
    check_beets_config,
)
from lib.config import (
    CratediggerConfig,
    install_admitted_runtime_config,
    read_runtime_config_strict,
)


class BeetsStartupError(RuntimeError):
    """The process must exit before creating application state."""


def enforce_beets_startup(
    *,
    role: BeetsRole,
    config_path: str,
    runtime_dir: str,
    logger: logging.Logger,
) -> CratediggerConfig:
    """Strictly load and admit Beets once for a top-level application.

    The four process entrypoints call this after parsing and logging setup and
    before their first application effect.  Actions and children inherit that
    admitted process configuration and deliberately do not call this adapter.
    """
    try:
        cfg = read_runtime_config_strict(config_path, runtime_dir)
    except (
        OSError,
        UnicodeError,
        configparser.Error,
        confuse.ConfigError,
        msgspec.ValidationError,
        ValueError,
    ) as exc:
        # Native parser/load context is intentionally retained (issue #759
        # KD9). It is not copied into the typed, token-free report surface.
        logger.error("Beets configuration load failed: %s", exc)
        raise BeetsStartupError("Beets configuration load failed") from exc

    try:
        report = check_beets_config(cfg, role=role)
    except (
        OSError,
        UnicodeError,
        configparser.Error,
        confuse.ConfigError,
        msgspec.ValidationError,
        BeetsConfigError,
    ) as exc:
        logger.error("Beets configuration check failed: %s", exc)
        raise BeetsStartupError("Beets configuration check failed") from exc

    for warning in report.warnings:
        logger.warning(
            "Beets configuration warning [%s] %s",
            warning.code,
            warning.message,
        )
    for failure in report.hard_failures:
        logger.error(
            "Beets configuration rejected [%s] %s",
            failure.code,
            failure.message,
        )
    if not report.ok:
        raise BeetsStartupError("external Beets authority was rejected")
    admitted = replace(
        cfg,
        beets_config_dir=report.authority.config_dir,
        beets_library_db=report.authority.library,
        beets_directory=report.authority.directory,
        beets_state_file=report.authority.state_file,
        beets_python=report.authority.python,
        beets_secret_include=report.authority.secret_include,
    )
    install_admitted_runtime_config(config_path, admitted)
    logger.info(
        "Beets configuration admitted for %s (fingerprint=%s)",
        role,
        report.fingerprint,
    )
    return admitted
