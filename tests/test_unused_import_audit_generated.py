"""Generated qualification of the source-local Ruff gate."""

from __future__ import annotations

import keyword
import unittest

from hypothesis import example, given, strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers suite/push/fuzz tiers
from tests.test_unused_import_audit import (
    assert_redundant_alias_baseline,
    assert_import_liveness,
    redundant_aliases,
    ruff_findings,
)


_SCAFFOLDING_NAMES = frozenset({
    "__debug__",
    "candidates",
    "dependency",
    "inspect",
    "object",
})

_IDENTIFIERS = st.from_regex(
    r"[A-Za-z_][A-Za-z0-9_]{0,15}",
    fullmatch=True,
).filter(
    lambda value: not keyword.iskeyword(value) and value not in _SCAFFOLDING_NAMES
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
        names=st.lists(_IDENTIFIERS, min_size=2, max_size=5, unique=True),
        delta=st.sampled_from(("duplicate", "expansion", "stale_expected")),
        import_style=st.sampled_from(("from", "import")),
    )
    @example(
        names=["existing_pin", "peer_used_pin"],
        delta="expansion",
        import_style="from",
    )
    @example(
        names=["existing_pin", "stale_pin"],
        delta="stale_expected",
        import_style="import",
    )
    @example(
        names=["duplicated_pin", "peer_pin"],
        delta="duplicate",
        import_style="from",
    )
    def test_any_redundant_alias_baseline_delta_is_rejected(
        self,
        names: list[str],
        delta: str,
        import_style: str,
    ) -> None:
        baseline_names = names[:-1]
        changed_name = names[-1]

        def import_line(name: str) -> str:
            if import_style == "from":
                return f"from dependency import {name} as {name}\n"
            return f"import {name} as {name}\n"

        changed_identity = (
            "dependency" if import_style == "from" else "",
            changed_name,
        )
        baseline_source = "".join(import_line(name) for name in baseline_names)
        source = baseline_source
        expected = frozenset(redundant_aliases(baseline_source))
        if delta == "expansion":
            source += import_line(changed_name)
        elif delta == "stale_expected":
            expected |= frozenset({changed_identity})
        else:
            source += import_line(baseline_names[0])
        peer_source = f"{changed_name} = object()\nprint({changed_name})\n"

        findings = ruff_findings({
            "lib/importing.py": source,
            "lib/peer.py": peer_source,
        })
        if delta != "duplicate":
            assert_import_liveness(
                findings,
                relative_path="lib/importing.py",
                import_is_live=True,
            )
        with self.assertRaises(AssertionError):
            assert_redundant_alias_baseline(source, expected)

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
    @example(imported_name="unused_pin", import_style="from", local_shape="unused")
    @example(imported_name="direct_pin", import_style="alias", local_shape="direct_use")
    @example(
        imported_name="parameter_pin",
        import_style="from",
        local_shape="parameter_shadow",
    )
    @example(imported_name="rebound_pin", import_style="alias", local_shape="rebound_use")
    @example(
        imported_name="comprehension_pin",
        import_style="from",
        local_shape="comprehension_shadow",
    )
    @example(
        imported_name="nested_pin",
        import_style="alias",
        local_shape="nested_global_use",
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
