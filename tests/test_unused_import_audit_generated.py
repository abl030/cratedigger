"""Generated qualification of the source-local Ruff gate."""

from __future__ import annotations

import keyword
import unittest

from hypothesis import given, strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers suite/push/fuzz tiers
from tests.test_unused_import_audit import (
    assert_import_liveness,
    ruff_findings,
)


_IDENTIFIERS = st.from_regex(
    r"[A-Za-z_][A-Za-z0-9_]{0,15}",
    fullmatch=True,
).filter(
    lambda value: not keyword.iskeyword(value) and value != "__debug__"
)


def _aggregate_name_fault(
    findings: tuple[dict[str, object], ...],
    *,
    imported_name: str,
    peer_source: str,
) -> tuple[dict[str, object], ...]:
    """Plant the old bug: a peer's name use erases the local diagnostic."""
    if imported_name in peer_source:
        return ()
    return findings


class TestGeneratedUnusedImportAudit(unittest.TestCase):
    @given(
        imported_name=_IDENTIFIERS,
        import_style=st.sampled_from(("from", "alias")),
        local_shape=st.sampled_from((
            "unused",
            "direct_use",
            "parameter_shadow",
            "rebound_use",
            "comprehension_shadow",
            "nested_global_use",
        )),
    )
    def test_only_a_same_binding_reference_keeps_an_import_live(
        self,
        imported_name: str,
        import_style: str,
        local_shape: str,
    ) -> None:
        if import_style == "from":
            source = f"from dependency import {imported_name}\n"
        else:
            source = f"import dependency as {imported_name}\n"
        if local_shape == "direct_use":
            source += f"print({imported_name})\n"
        elif local_shape == "parameter_shadow":
            source += (
                f"def inspect({imported_name}): return {imported_name}\n"
            )
        elif local_shape == "rebound_use":
            source += f"{imported_name} = object()\nprint({imported_name})\n"
        elif local_shape == "comprehension_shadow":
            source += (
                f"values = [{imported_name} for {imported_name} in candidates]\n"
            )
        elif local_shape == "nested_global_use":
            source += f"def inspect(): return {imported_name}\n"
        peer_source = f"{imported_name} = object()\nprint({imported_name})\n"

        findings = ruff_findings({
            "lib/importing.py": source,
            "lib/peer.py": peer_source,
        })

        assert_import_liveness(
            findings,
            relative_path="lib/importing.py",
            import_is_live=local_shape in {"direct_use", "nested_global_use"},
        )

    def test_checker_rejects_the_aggregate_name_fault(self) -> None:
        imported_name = "shared_name"
        peer_source = "shared_name = object()\nprint(shared_name)\n"
        findings = ruff_findings({
            "lib/importing.py": "from dependency import shared_name\n",
            "lib/peer.py": peer_source,
        })

        fault_findings = _aggregate_name_fault(
            findings,
            imported_name=imported_name,
            peer_source=peer_source,
        )

        with self.assertRaises(AssertionError):
            assert_import_liveness(
                fault_findings,
                relative_path="lib/importing.py",
                import_is_live=False,
            )


if __name__ == "__main__":
    unittest.main()
