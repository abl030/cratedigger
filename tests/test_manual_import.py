"""Tests for lib.manual_import — folder scanning, matching, and import execution."""

import unittest
from lib.manual_import import (
    FolderInfo,
    ImportRequest,
    ManualImportResult,
    parse_folder_name,
    match_folders_to_requests,
    parse_import_result_stdout,
)


class TestParseFolderName(unittest.TestCase):
    """Tests for extracting artist/album from unstructured folder names."""

    def test_artist_dash_album(self) -> None:
        result = parse_folder_name("The Mountain Goats - Deserters")
        self.assertEqual(result.artist, "The Mountain Goats")
        self.assertEqual(result.album, "Deserters")

    def test_album_with_year_in_parens(self) -> None:
        result = parse_folder_name("Deserters (2022)")
        self.assertEqual(result.album, "Deserters")
        self.assertEqual(result.artist, "")

    def test_artist_dash_year_dash_album(self) -> None:
        result = parse_folder_name("Doves - 2002 - The Last Broadcast")
        self.assertEqual(result.artist, "Doves")
        self.assertEqual(result.album, "The Last Broadcast")

    def test_artist_dash_bracketed_year_album(self) -> None:
        result = parse_folder_name("Four Tet - [2012] Pink {Hostess Entertainment}")
        self.assertEqual(result.artist, "Four Tet")
        self.assertIn("Pink", result.album)

    def test_scene_release(self) -> None:
        result = parse_folder_name("Courtney_Marie_Andrews-Valentine-WEB-2026-QUAVER")
        self.assertEqual(result.artist, "Courtney Marie Andrews")
        self.assertEqual(result.album, "Valentine")

    def test_plain_album_name(self) -> None:
        result = parse_folder_name("My Beautiful Dark Twisted Fantasy")
        self.assertEqual(result.album, "My Beautiful Dark Twisted Fantasy")
        self.assertEqual(result.artist, "")

    def test_empty_string(self) -> None:
        result = parse_folder_name("")
        self.assertEqual(result.artist, "")
        self.assertEqual(result.album, "")

    def test_year_prefix(self) -> None:
        result = parse_folder_name("1987 Sister")
        self.assertEqual(result.album, "Sister")
        self.assertEqual(result.artist, "")


class TestMatchFoldersToRequests(unittest.TestCase):
    """Tests for fuzzy matching folders against pipeline requests."""

    def _req(self, id: int, artist: str, album: str) -> ImportRequest:
        return ImportRequest(
            id=id,
            artist_name=artist,
            album_title=album,
            mb_release_id="mbid-" + str(id),
        )

    def test_exact_match(self) -> None:
        folders = [FolderInfo(name="Deserters (2022)", path="/tmp/Deserters (2022)",
                              artist="The Mountain Goats", album="Deserters",
                              file_count=107)]
        requests = [self._req(1, "The Mountain Goats", "Deserters")]
        matches = match_folders_to_requests(folders, requests)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].folder.name, "Deserters (2022)")
        self.assertEqual(matches[0].request.id, 1)
        self.assertGreater(matches[0].score, 0.5)

    def test_no_match(self) -> None:
        folders = [FolderInfo(name="Random Album", path="/tmp/Random Album",
                              artist="", album="Random Album", file_count=10)]
        requests = [self._req(1, "The Mountain Goats", "Deserters")]
        matches = match_folders_to_requests(folders, requests)
        self.assertEqual(len(matches), 0)

    def test_multiple_requests_best_match(self) -> None:
        folders = [FolderInfo(name="Doves - 2002 - The Last Broadcast", path="/tmp/x",
                              artist="Doves", album="The Last Broadcast", file_count=12)]
        requests = [
            self._req(1, "Doves", "The Last Broadcast"),
            self._req(2, "Doves", "Lost Souls"),
            self._req(3, "The National", "Boxer"),
        ]
        matches = match_folders_to_requests(folders, requests)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].request.id, 1)

    def test_scene_release_matches(self) -> None:
        folders = [FolderInfo(name="scene", path="/tmp/scene",
                              artist="Courtney Marie Andrews", album="Valentine",
                              file_count=10)]
        requests = [self._req(1, "Courtney Marie Andrews", "Valentine")]
        matches = match_folders_to_requests(folders, requests)
        self.assertEqual(len(matches), 1)

    def test_empty_inputs(self) -> None:
        self.assertEqual(match_folders_to_requests([], []), [])


class TestParseImportResultStdout(unittest.TestCase):
    """Tests for extracting ImportResult JSON from import_one.py stdout."""

    def test_extracts_json(self) -> None:
        stdout = 'some log\n__IMPORT_RESULT__{"decision":"import","exit_code":0}\nmore log\n'
        result = parse_import_result_stdout(stdout)
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.decision, "import")
        self.assertEqual(result.exit_code, 0)

    def test_no_sentinel(self) -> None:
        result = parse_import_result_stdout("just some logs\n")
        self.assertIsNone(result)

    def test_malformed_json(self) -> None:
        result = parse_import_result_stdout("__IMPORT_RESULT__{bad json\n")
        self.assertIsNone(result)


class TestManualImportResult(unittest.TestCase):
    def test_success(self) -> None:
        r = ManualImportResult(success=True, exit_code=0, message="Imported OK")
        self.assertTrue(r.success)

    def test_failure(self) -> None:
        r = ManualImportResult(success=False, exit_code=2, message="beets failed",
                               import_result_json='{"decision":"error"}')
        self.assertFalse(r.success)
        self.assertIsNotNone(r.import_result_json)


if __name__ == "__main__":
    unittest.main()
