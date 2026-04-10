"""Minimal versioned SQL migrator for the Pipeline DB.

Discovers numbered SQL migration files in ``migrations/``, applies any not
yet recorded in the ``schema_migrations`` tracking table, and records each
applied version. Each migration file runs in its own transaction.

Migration filename format: ``NNN_short_name.sql``, e.g.::

    migrations/001_initial.sql
    migrations/002_add_my_column.sql

Adding a schema change is a single PR step:

    1. Create the next-numbered ``.sql`` file in ``migrations/``.
    2. The deploy systemd unit (``soularr-db-migrate.service``) will run it
       on the next ``nixos-rebuild switch`` via :func:`apply_migrations`.

The migrator never edits or re-runs an applied migration. If you need to
change a migration that already shipped, write a new one.
"""

from __future__ import annotations

import glob
import logging
import os
import re
from dataclasses import dataclass

import psycopg2

logger = logging.getLogger(__name__)

DEFAULT_MIGRATIONS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "migrations",
)

_FILENAME_RE = re.compile(r"^(\d{3,})_([A-Za-z0-9_]+)\.sql$")

_TRACKING_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""


@dataclass(frozen=True)
class Migration:
    """A single migration file on disk."""
    version: int
    name: str
    path: str

    @property
    def label(self) -> str:
        return f"{self.version:03d}_{self.name}"


def discover_migrations(migrations_dir: str) -> list[Migration]:
    """Scan ``migrations_dir`` and return migrations sorted by version.

    Raises ``ValueError`` on a malformed filename or duplicate version.
    """
    if not os.path.isdir(migrations_dir):
        raise FileNotFoundError(f"Migrations directory not found: {migrations_dir}")

    found: list[Migration] = []
    for path in sorted(glob.glob(os.path.join(migrations_dir, "*.sql"))):
        filename = os.path.basename(path)
        m = _FILENAME_RE.match(filename)
        if not m:
            raise ValueError(
                f"Migration filename {filename!r} does not match NNN_name.sql"
            )
        version = int(m.group(1))
        name = m.group(2)
        found.append(Migration(version=version, name=name, path=path))

    seen: dict[int, str] = {}
    for mig in found:
        if mig.version in seen:
            raise ValueError(
                f"Duplicate migration version {mig.version}: "
                f"{seen[mig.version]} and {mig.name}"
            )
        seen[mig.version] = mig.name

    return sorted(found, key=lambda m: m.version)


def _ensure_tracking_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(_TRACKING_TABLE_SQL)
    conn.commit()


def _applied_versions(conn) -> set[int]:
    with conn.cursor() as cur:
        cur.execute("SELECT version FROM schema_migrations")
        return {row[0] for row in cur.fetchall()}


def apply_migrations(
    dsn: str,
    migrations_dir: str | None = None,
) -> list[Migration]:
    """Apply any unapplied migrations to the database at ``dsn``.

    Returns the list of newly applied migrations (may be empty).
    Idempotent: running it on an up-to-date DB is a no-op.

    Each migration runs in its own transaction. The version is recorded in
    ``schema_migrations`` in the same transaction as the migration body, so a
    crash mid-migration leaves the schema unchanged AND unrecorded — the next
    run will retry it.
    """
    target_dir = migrations_dir or DEFAULT_MIGRATIONS_DIR
    migrations = discover_migrations(target_dir)

    conn = psycopg2.connect(dsn, connect_timeout=10)
    try:
        conn.autocommit = False
        # Don't sit on a lock forever if a migration tries to ALTER a busy table.
        with conn.cursor() as cur:
            cur.execute("SET lock_timeout TO '30s'")
        conn.commit()

        _ensure_tracking_table(conn)
        applied = _applied_versions(conn)

        newly_applied: list[Migration] = []
        for mig in migrations:
            if mig.version in applied:
                logger.debug("Skipping %s (already applied)", mig.label)
                continue
            logger.info("Applying migration %s", mig.label)
            with open(mig.path) as f:
                sql = f.read()
            try:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    cur.execute(
                        "INSERT INTO schema_migrations (version, name) "
                        "VALUES (%s, %s)",
                        (mig.version, mig.name),
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                logger.exception("Migration %s failed", mig.label)
                raise
            newly_applied.append(mig)

        return newly_applied
    finally:
        conn.close()
