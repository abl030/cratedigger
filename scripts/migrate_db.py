#!/usr/bin/env python3
"""Apply pending Pipeline DB migrations.

Thin entry point around :func:`lib.migrator.apply_migrations`. Reads
numbered ``.sql`` files from ``migrations/`` and applies any not yet
recorded in the ``schema_migrations`` table.

Idempotent: running it on an already-up-to-date database is a no-op.

Usage:
    migrate_db.py --dsn postgresql://cratedigger@host/db
    PIPELINE_DB_DSN=... migrate_db.py
    migrate_db.py --migrations-dir /path/to/migrations
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.migrator import DEFAULT_MIGRATIONS_DIR, apply_migrations

DEFAULT_DSN = os.environ.get(
    "PIPELINE_DB_DSN",
    "postgresql://cratedigger@192.168.100.11:5432/cratedigger",
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dsn",
        default=DEFAULT_DSN,
        help="PostgreSQL DSN (default from PIPELINE_DB_DSN env or prod URL)",
    )
    parser.add_argument(
        "--migrations-dir",
        default=DEFAULT_MIGRATIONS_DIR,
        help=f"Directory containing NNN_name.sql files (default: {DEFAULT_MIGRATIONS_DIR})",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    logger = logging.getLogger("migrate_db")

    logger.info("Applying pending migrations from %s", args.migrations_dir)
    try:
        applied = apply_migrations(args.dsn, args.migrations_dir)
    except Exception as exc:
        logger.error("Migration failed: %s", exc)
        return 1

    if applied:
        logger.info("Applied %d migration(s):", len(applied))
        for mig in applied:
            logger.info("  - %s", mig.label)
    else:
        logger.info("Schema is up to date — nothing to apply.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
