"""R6 made mechanical: no Home-Manager coupling anywhere in the codebase.

Tier-2 plan U5 deleted every HM seam — the ``HOME=/home/<user>``
impersonation in ``beets_subprocess_env``, the
``/etc/profiles/per-user/.../bin/beet`` fallbacks, and the harness's
``.beet-wrapped`` scraping. "Deleted, not supplemented" only stays true if
nothing can quietly reintroduce a per-user-profile path, so this guard
asserts absence across every python/shell source in lib/, harness/,
scripts/ and web/. Docs and plan files are exempt (history is allowed to
name the old paths); code is not.
"""

from __future__ import annotations

import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

FORBIDDEN = (
    "/home/abl030",
    "/etc/profiles/per-user",
    ".beet-wrapped",
)

SOURCE_DIRS = ("lib", "harness", "scripts", "web")
SOURCE_SUFFIXES = {".py", ".sh"}


class TestNoHomeManagerCoupling(unittest.TestCase):
    def test_no_hm_paths_in_source(self) -> None:
        offenders: list[str] = []
        for dirname in SOURCE_DIRS:
            for path in sorted((REPO_ROOT / dirname).rglob("*")):
                if path.suffix not in SOURCE_SUFFIXES or not path.is_file():
                    continue
                text = path.read_text(encoding="utf-8", errors="replace")
                for lineno, line in enumerate(text.splitlines(), start=1):
                    for pattern in FORBIDDEN:
                        if pattern in line:
                            offenders.append(
                                f"{path.relative_to(REPO_ROOT)}:{lineno}: {line.strip()}"
                            )
        self.assertEqual(
            offenders, [],
            "Home-Manager coupling reintroduced (tier-2 plan R6 forbids "
            "per-user-profile paths in code — beets is module-owned via "
            "BEETSDIR + [Beets] config keys):\n" + "\n".join(offenders),
        )


if __name__ == "__main__":
    unittest.main()
