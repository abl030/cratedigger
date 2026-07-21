import unittest


class TestReleaseIdentity(unittest.TestCase):
    def test_from_fields_normalizes_and_tags_identity(self):
        from lib.release_identity import ReleaseIdentity

        cases = [
            (
                "musicbrainz uuid wins and normalizes case",
                " 89AD4AC3-39F7-470E-963A-56509C546377 ",
                "12856590",
                ("musicbrainz", "89ad4ac3-39f7-470e-963a-56509c546377"),
            ),
            (
                "explicit discogs field supplies identity when primary blank",
                "",
                " 0012856590 ",
                ("discogs", "12856590"),
            ),
            (
                "legacy numeric primary is treated as discogs",
                "0012856590",
                "",
                ("discogs", "12856590"),
            ),
            (
                "blank and zero sentinel produce no identity",
                "",
                "0",
                None,
            ),
            (
                "unknown text produces no identity",
                "not-a-real-id",
                "",
                None,
            ),
        ]

        for desc, release_id, discogs_release_id, expected in cases:
            with self.subTest(desc=desc):
                identity = ReleaseIdentity.from_fields(release_id, discogs_release_id)
                if expected is None:
                    self.assertIsNone(identity)
                    continue
                assert identity is not None
                self.assertEqual((identity.source, identity.release_id), expected)
                self.assertEqual(identity.key, expected)

    def test_frontend_release_id_uses_same_normalization(self):
        from lib.release_identity import frontend_release_id

        self.assertEqual(
            frontend_release_id(" 89AD4AC3-39F7-470E-963A-56509C546377 ", "0"),
            "89ad4ac3-39f7-470e-963a-56509c546377",
        )
        self.assertEqual(frontend_release_id("", "0012856590"), "12856590")
        self.assertIsNone(frontend_release_id("", "0"))

    def test_strict_fields_require_one_valid_exact_identity(self):
        from lib.release_identity import ReleaseIdentity

        self.assertEqual(
            ReleaseIdentity.from_strict_fields("0012856590", "12856590"),
            ReleaseIdentity(source="discogs", release_id="12856590"),
        )
        self.assertEqual(
            ReleaseIdentity.from_strict_fields(
                "89AD4AC3-39F7-470E-963A-56509C546377", "0",
            ),
            ReleaseIdentity(
                source="musicbrainz",
                release_id="89ad4ac3-39f7-470e-963a-56509c546377",
            ),
        )
        for primary, secondary in (
            ("89ad4ac3-39f7-470e-963a-56509c546377", "12856590"),
            ("malformed", "12856590"),
            ("", ""),
        ):
            with self.subTest(primary=primary, secondary=secondary):
                self.assertIsNone(
                    ReleaseIdentity.from_strict_fields(primary, secondary),
                )


if __name__ == "__main__":
    unittest.main()
