"""Fixed-clock contracts for the Beets compatibility release manifest."""

from __future__ import annotations

import datetime as dt
import importlib.util
import json
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "refresh_beets_compat_releases.py"
SPEC = importlib.util.spec_from_file_location("beets_compat_releases", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
refresh = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(refresh)


def release(tag: str, published_at: str, *, draft: bool = False, prerelease: bool = False) -> dict[str, object]:
    return {"tag_name": tag, "published_at": published_at, "draft": draft, "prerelease": prerelease}


class TestBeetsCompatReleaseManifest(unittest.TestCase):
    as_of = dt.date(2026, 8, 3)

    def test_selector_is_inclusive_final_and_deterministic(self) -> None:
        selected = refresh.select_final_releases([
            release("v2.13.1", "2026-08-03T23:59:59Z"),
            release("v2.13.0", "2024-08-04T00:00:00Z"),
            release("v2.12.9", "2024-08-02T00:00:00Z"),
            release("v2.14.0rc1", "2026-08-02T00:00:00Z", prerelease=True),
            release("v2.14.0", "2026-08-02T00:00:00Z", draft=True),
        ], as_of=self.as_of)
        self.assertEqual([entry["tag_name"] for entry in selected], ["v2.13.0", "v2.13.1"])

    def test_duplicate_qualifying_tag_fails_loudly(self) -> None:
        with self.assertRaisesRegex(ValueError, "duplicate qualifying"):
            refresh.select_final_releases([
                release("v2.13.1", "2026-08-01T00:00:00Z"),
                release("v2.13.1", "2026-08-02T00:00:00Z"),
            ], as_of=self.as_of)

    def test_renderer_and_validator_reject_known_bad_manifest(self) -> None:
        entries = refresh.resolve_entries(
            [release("v2.12.0", "2025-01-01T00:00:00Z"), release("v2.13.1", "2026-07-29T10:47:07Z")],
            as_of=self.as_of,
            resolve_revision=lambda tag: "a" * 40 if tag == "v2.12.0" else "b" * 40,
            prefetch=lambda _rev: "sha256-" + "A" * 43 + "=",
        )
        rendered = refresh.render_manifest(entries)
        self.assertEqual(rendered, refresh.render_manifest(json.loads(rendered)))
        bad = [dict(entry) for entry in entries]
        bad[0]["tag"] = "v2.14.0rc1"
        with self.assertRaisesRegex(ValueError, "disagree"):
            refresh.validate_manifest(bad)

    def test_committed_manifest_is_canonical_snapshot(self) -> None:
        manifest = REPO_ROOT / "nix" / "beets-compat-releases.json"
        entries = json.loads(manifest.read_text(encoding="utf-8"))
        refresh.validate_manifest(entries)
        self.assertEqual(manifest.read_text(encoding="utf-8"), refresh.render_manifest(entries))
        self.assertEqual([entry["tag"] for entry in entries], [
            "v2.1.0", "v2.2.0", "v2.3.0", "v2.3.1", "v2.4.0", "v2.5.0", "v2.5.1",
            "v2.6.0", "v2.6.1", "v2.6.2", "v2.7.0", "v2.7.1", "v2.8.0", "v2.9.0",
            "v2.10.0", "v2.11.0", "v2.12.0", "v2.13.0", "v2.13.1",
        ])


if __name__ == "__main__":
    unittest.main()
