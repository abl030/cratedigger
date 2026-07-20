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

import msgspec

sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401 — sets TEST_DB_DSN env var

from lib.pipeline_db import (
    AddRequestInput,
    AlbumRequestRow,
    BadAudioHashInput,
    PersistedYoutubeRow,
    PipelineDB,
    RequestSpectralStateUpdate,
    RequestV0ProbeStateUpdate,
    TransferLedgerRow,
    album_request_row,
)
from tests.helpers import make_request_row
from lib.quality import SpectralMeasurement, V0ProbeEvidence

# conftest boots an ephemeral PostgreSQL and exports TEST_DB_DSN for the whole
# suite, so this runs unconditionally — NO skip gate (CLAUDE.md § "Skipped tests
# are an anti-pattern"; the programmatic `unittest.skip()` form used elsewhere is
# also invisible to test_skip_audit.py). If the DSN is genuinely absent,
# setUpClass's connection fails loudly — the intended "a test runs or it doesn't
# exist" behaviour.
TEST_DSN = os.environ.get("TEST_DB_DSN")


def _dataclass_columns(struct_cls) -> set[str]:
    """A flat write dataclass whose field names ARE column names."""
    return {f.name for f in dataclasses.fields(struct_cls)}


def _struct_columns(struct_cls) -> set[str]:
    """A flat write ``msgspec.Struct`` whose field names ARE column names."""
    return {f.name for f in msgspec.structs.fields(struct_cls)}


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
    ("PersistedYoutubeRow", "youtube_album_mappings",
     _struct_columns(PersistedYoutubeRow)),
    ("TransferLedgerRow", "slskd_transfer_ledger",
     _struct_columns(TransferLedgerRow)),
]


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

    def test_album_request_row_matches_table_columns_exactly(self) -> None:
        """Read-projection parity (#765 phase 6): ``AlbumRequestRow`` is the
        typed view of ``SELECT * FROM album_requests``, so its keys must
        EQUAL the table's columns — a new migration column fails here until
        the row type learns it in the same PR (and vice versa)."""
        table_cols = self._table_columns("album_requests")
        row_keys = set(AlbumRequestRow.__annotations__)
        self.assertEqual(
            row_keys, table_cols,
            "AlbumRequestRow drifted from album_requests: "
            f"missing={sorted(table_cols - row_keys)} "
            f"stale={sorted(row_keys - table_cols)}",
        )

    def test_make_request_row_builder_matches_row_type(self) -> None:
        """The shared builder must produce every ``AlbumRequestRow`` key —
        production-shape fidelity for every test that seeds request rows
        (this immediately caught the builder lacking ``final_format``)."""
        built = set(make_request_row())
        row_keys = set(AlbumRequestRow.__annotations__)
        self.assertEqual(
            built, row_keys,
            f"builder missing={sorted(row_keys - built)} "
            f"extra={sorted(built - row_keys)}",
        )

    def test_builder_row_survives_the_runtime_validator(self) -> None:
        """``album_request_row`` must accept a production-shaped builder row
        — pins the msgspec-convert boundary against value-type drift."""
        converted = album_request_row(make_request_row())
        self.assertEqual(converted["status"], "wanted")

    def test_bogus_row_is_rejected_by_the_runtime_validator(self) -> None:
        """Known-bad self-test: wrong-typed column value must raise."""
        bad = dict(make_request_row())
        bad["search_attempts"] = "three"
        with self.assertRaises(msgspec.ValidationError):
            album_request_row(bad)

    def test_bogus_struct_field_is_caught_by_the_subset_check(self) -> None:
        """Known-bad self-test: a synthetic ``msgspec.Struct`` with one
        bogus field NOT on ``youtube_album_mappings`` must be reported by
        the subset check as missing — proves the guard has teeth rather
        than vacuously passing every payload."""
        class _BogusYoutubeRow(msgspec.Struct, kw_only=True):
            yt_browse_id: str = ""
            definitely_not_a_real_column: str = ""

        table_cols = self._table_columns("youtube_album_mappings")
        bogus_cols = _struct_columns(_BogusYoutubeRow)
        missing = bogus_cols - table_cols
        self.assertEqual(missing, {"definitely_not_a_real_column"})


if __name__ == "__main__":
    unittest.main()
