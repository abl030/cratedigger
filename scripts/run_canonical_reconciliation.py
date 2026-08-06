#!/usr/bin/env python3
"""Daily MusicBrainz merge-reconciliation oneshot (#1059).

Runs as ``cratedigger-canonical-reconcile.service`` on a daily timer, in its
own unit rather than inside ``cratedigger-unfindable.service``: that unit
gates on slskd reachability and fails fast on an outage, which would
silently stop merge reconciliation for as long as slskd was down, and it is
deliberately isolated so the never-stop-searching invariant stays enforceable
at the systemd level.

Sweeps every non-``replaced`` request. Per-row outcomes stream to the journal
so ``journalctl -u cratedigger-canonical-reconcile`` shows progress during
the run rather than ten minutes of silence followed by a summary.

Timer-driven with ``restartIfChanged = false``, so it only ``wants``+``after``
the migrate unit and gates on schema currency itself — the same shape as
``cratedigger`` and ``cratedigger-unfindable``.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from lib.canonical_release_service import (
    CanonicalReconcileResult,
    CanonicalReleaseService,
    configure_reconciliation_mirror,
)
from lib.config import read_runtime_config
from lib.migrator import SchemaBehindError, assert_schema_current
from lib.pipeline_db import DEFAULT_DSN, PipelineDB

logger = logging.getLogger("cratedigger-canonical")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Reconcile stored acquisition release ids against MusicBrainz "
            "merge state, storing any declared survivor."
        ),
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("PIPELINE_DB_DSN", DEFAULT_DSN),
        help="Pipeline DB DSN.",
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Runtime config path (defaults to the deployed immutable one).",
    )
    return parser


def _log_result(result: CanonicalReconcileResult) -> None:
    if result.changed:
        logger.info(
            "resolved request %s: %s -> %s",
            result.request_id,
            result.acquisition_release_id,
            result.canonical_release_id,
        )


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    args = _parser().parse_args()
    cfg = (
        read_runtime_config(args.config)
        if args.config
        else read_runtime_config()
    )

    if configure_reconciliation_mirror(cfg.musicbrainz_api_base) is None:
        logger.error(
            "musicbrainz.apiBase (%r) is unset or public MusicBrainz; "
            "refusing to sweep the library. Reconciliation is a local-mirror "
            "feature — a whole-library sweep against musicbrainz.org is "
            "~8,500 unthrottled requests.",
            cfg.musicbrainz_api_base,
        )
        return 1

    try:
        assert_schema_current(args.dsn)
    except SchemaBehindError as exc:
        logger.error(
            "Pipeline DB schema is behind: missing migration version(s) %s. "
            "Refusing to sweep against an inconsistent schema.",
            exc.missing_versions,
        )
        return 1

    db = PipelineDB(args.dsn)
    try:
        sweep = CanonicalReleaseService(db).reconcile_all(
            on_result=_log_result,
        )
    finally:
        db.close()

    logger.info(
        "canonical sweep complete: scanned=%s changed=%s outcomes=%s",
        sweep.scanned,
        sweep.changed,
        dict(sorted(sweep.outcome_counts.items())),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
