"""Auditable contract for the deliberate Plex/Jellyfin sibling design."""
from __future__ import annotations

import unittest

import lib.jellyfin_pin_service
import lib.plex_pin_service


class TestPinServiceSiblingContract(unittest.TestCase):
    def test_both_modules_document_deliberate_duplication_and_third_backend_rule(self):
        for module in (lib.plex_pin_service, lib.jellyfin_pin_service):
            with self.subTest(module=module.__name__):
                doc = module.__doc__ or ""
                self.assertIn("Deliberate sibling duplication", doc)
                self.assertIn("third backend", doc.lower())


if __name__ == "__main__":
    unittest.main()
