"""Layer 1 of the test-fidelity hardening (#382): write-payload column contract.

For every typed write payload that maps to a table, assert its persisted
column names are a SUBSET of that table's real columns (queried live from
``information_schema``). This catches the "field present in the payload but
absent from the table" half of the ``album_title`` drift class: adding a
field to ``AddRequestInput`` (or renaming a column under a spectral/V0 update)
without a matching migration fails here, loudly, instead of silently writing
to / reading back ``NULL``.

It pairs with two sibling guards that close the other half (a column the table
has but the write omits):
  * ``add_request`` derives its INSERT column list directly from
    ``AddRequestInput``'s fields (``lib/pipeline_db/requests.py``), so the SQL
    can't omit a payload field; and
  * the Layer-2 round-trip write audit (``test_pipeline_db_write_audit.py``)
    requires a real-PG round-trip per write method.

Follows the ephemeral-PostgreSQL harness used by ``test_pipeline_db.py``
(conftest boots PG + applies migrations; in the dev shell the DSN is always
set, so this never skips — see CLAUDE.md § "Skipped tests are an anti-pattern").
"""
import dataclasses
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))
import conftest  # noqa: F401 — sets TEST_DB_DSN env var

from lib.pipeline_db import (
    AddRequestInput,
    BadAudioHashInput,
    PipelineDB,
    RequestSpectralStateUpdate,
    RequestV0ProbeStateUpdate,
)
from lib.quality import SpectralMeasurement, V0ProbeEvidence

TEST_DSN = os.environ.get("TEST_DB_DSN")


def requires_postgres(cls):
    if not TEST_DSN:
        return unittest.skip("TEST_DB_DSN not set — skipping PostgreSQL tests")(cls)
    return cls


def _dataclass_columns(struct_cls) -> set[str]:
    """A flat write dataclass whose field names ARE column names."""
    return {f.name for f in dataclasses.fields(struct_cls)}


def _spectral_update_columns() -> set[str]:
    """The album_requests columns a fully-populated spectral update writes."""
    sm = SpectralMeasurement(grade="EXCELLENT", bitrate_kbps=900)
    update = RequestSpectralStateUpdate(last_download=sm, current=sm)
    return set(update.as_update_fields().keys())


def _v0_update_columns() -> set[str]:
    """The album_requests columns a fully-populated V0-probe update writes."""
    v0 = V0ProbeEvidence(
        kind="cbr", min_bitrate_kbps=320,
        avg_bitrate_kbps=320, median_bitrate_kbps=320,
    )
    update = RequestV0ProbeStateUpdate(current_lossless_source=v0)
    return set(update.as_update_fields().keys())


# (payload label, target table, the column names the payload persists)
CONTRACTS: list[tuple[str, str, set[str]]] = [
    ("AddRequestInput", "album_requests", _dataclass_columns(AddRequestInput)),
    ("BadAudioHashInput", "bad_audio_hashes", _dataclass_columns(BadAudioHashInput)),
    ("RequestSpectralStateUpdate", "album_requests", _spectral_update_columns()),
    ("RequestV0ProbeStateUpdate", "album_requests", _v0_update_columns()),
]


@requires_postgres
class TestWritePayloadColumnContract(unittest.TestCase):
    db: PipelineDB

    @classmethod
    def setUpClass(cls) -> None:
        cls.db = PipelineDB(TEST_DSN)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.db.close()

    def _table_columns(self, table: str) -> set[str]:
        cur = self.db._execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = %s",
            (table,),
        )
        return {r["column_name"] for r in cur.fetchall()}

    def test_payload_columns_are_a_subset_of_table_columns(self) -> None:
        for label, table, cols in CONTRACTS:
            with self.subTest(payload=label):
                table_cols = self._table_columns(table)
                self.assertTrue(
                    table_cols, f"table {table!r} reports no columns")
                missing = cols - table_cols
                self.assertEqual(
                    missing, set(),
                    f"{label} persists column(s) {sorted(missing)} that do "
                    f"not exist on {table!r}. Add a migration for them, or fix "
                    f"the payload field name. (#382 Layer 1 column contract.)",
                )

    def test_contracts_are_non_empty(self) -> None:
        """Guard the guard: a payload that introspected to zero columns
        (e.g. a renamed dataclass) would make the subset check vacuous."""
        for label, _table, cols in CONTRACTS:
            with self.subTest(payload=label):
                self.assertTrue(cols, f"{label} resolved to no columns")


if __name__ == "__main__":
    unittest.main()
