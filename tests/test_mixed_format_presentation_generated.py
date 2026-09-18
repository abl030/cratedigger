"""Mixed-format presentation follows all tracks and measured output facts."""

from __future__ import annotations

import re
import unittest
from pathlib import Path

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.quality import AudioQualityMeasurement
from tests.evidence_helpers import make_album_quality_evidence
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row, make_web_runtime
from tests.node_jsonl_worker import NodeJsonlWorker
from tests.web._harness import _FakeDbWebServerCase
from web.runtime import install_runtime, runtime

_WORKER = r"""
import {toggleDetail} from './web/js/pipeline.js';
import {renderDownloadHistoryItem} from './web/js/history.js';
import {domStub, element, stubGlobals} from './tests/js_harness.mjs';

async function handle(operation, payload) {
  if (operation === 'history') return renderDownloadHistoryItem(payload);
  if (operation !== 'detail') throw new Error('unknown operation');
  const panel = element();
  const {restore} = stubGlobals({
    document: domStub({4242: panel}),
    fetch: async () => ({ok: true, status: 200, json: async () => ({
      request: {id: 4242, status: 'wanted', verified_lossless: false, ...payload.request},
      current_library: {state: 'unique', path: '/music/mixed', album_id: 1},
      beets_tracks: payload.formats.map(format => ({format, bitrate: 128000})),
      tracks: [], history: [], last_search: null,
    })}),
  });
  try {
    await toggleDetail(4242);
    return panel.innerHTML;
  } finally {
    restore();
  }
}
"""


class TestMixedFormatPresentation(unittest.TestCase):
    def setUp(self) -> None:
        self.worker = NodeJsonlWorker(_WORKER, cwd=Path(__file__).resolve().parents[1])
        self.addCleanup(self.worker.close)

    def _render(self, operation: str, payload: object) -> str:
        rendered = self.worker.request(operation, payload)
        assert isinstance(rendered, str)
        self.assertNotIn("Failed to load details", rendered)
        return rendered

    @given(
        formats=st.lists(st.sampled_from(("AAC", "MP3", "Opus", "FLAC")), min_size=2, max_size=12),
        current_grade=st.sampled_from((None, "suspect")),
        current_bitrate=st.one_of(st.none(), st.integers(min_value=32, max_value=320)),
        last_bitrate=st.one_of(st.none(), st.integers(min_value=32, max_value=320)),
    )
    @example(formats=["AAC", "MP3"], current_grade=None, current_bitrate=96, last_bitrate=128)
    @example(formats=["MP3"], current_grade="suspect", current_bitrate=None, last_bitrate=96)
    def test_detail_quality_reports_every_codec_independently_of_order(
        self, formats: list[str], current_grade: str | None,
        current_bitrate: int | None, last_bitrate: int | None,
    ) -> None:
        request = {
            "current_spectral_grade": current_grade,
            "current_spectral_bitrate": current_bitrate,
            "current_spectral_accusation_admissible": True,
            "last_download_spectral_grade": "likely_transcode",
            "last_download_spectral_bitrate": last_bitrate,
            "last_download_spectral_accusation_admissible": False,
            "last_download_spectral_accusation_withheld": "codec_unresolved",
        }
        html = self._render("detail", {"formats": formats, "request": request})
        reverse = self._render("detail", {"formats": list(reversed(formats)), "request": request})
        row = re.search(r'>Quality</span><span class="p-detail-value">(.*?)</span>', html)
        reversed_row = re.search(r'>Quality</span><span class="p-detail-value">(.*?)</span>', reverse)
        assert row is not None and reversed_row is not None
        self.assertEqual(row.group(1), reversed_row.group(1))
        unique = sorted({codec.upper() for codec in formats})
        if len(unique) > 1:
            self.assertTrue(row.group(1).startswith(" + ".join(unique) + " (mixed) 128k avg"))
        else:
            self.assertNotIn("mixed", row.group(1))
        bitrate = current_bitrate if current_grade else last_bitrate
        self.assertEqual(re.findall(r"~(\d+)kbps", row.group(1)), [str(bitrate)] if bitrate else [])
        if current_grade:
            self.assertIn("spectral: suspect", row.group(1))
            self.assertNotIn("codec unresolved", row.group(1))
        else:
            self.assertIn("spectral: likely transcode", row.group(1))
            self.assertIn("codec unresolved", row.group(1))

    @given(
        source=st.sampled_from(("AAC", "MP3", "Opus")),
        output=st.sampled_from(("AAC", "MP3", "Opus")),
        contract=st.sampled_from((None, "opus 128", "mp3 v0")),
        source_rate=st.integers(min_value=32, max_value=512),
    )
    @example(source="AAC", output="MP3", contract=None, source_rate=127)
    def test_stored_format_uses_output_but_keeps_explicit_target_contracts(
        self, source: str, output: str, contract: str | None, source_rate: int,
    ) -> None:
        html = self._render("history", {
            "outcome": "success", "created_at": "2026-09-18T02:10:00+08:00",
            "source_format": source, "source_avg_bitrate": source_rate,
            "final_format": contract or source, "materialized_format": output,
        })
        row = re.search(r'>Stored as</span><span class="p-hist-value">(.*?)</span>', html)
        assert row is not None
        self.assertEqual(row.group(1), contract.upper() + " contract" if contract else output.upper())
        self.assertIn(source.upper() + f" avg {source_rate}kbps", html)


class TestCurrentSpectralProjection(_FakeDbWebServerCase):
    @given(
        current_grade=st.sampled_from((None, "genuine", "suspect", "likely_transcode")),
        current_bitrate=st.integers(min_value=32, max_value=320),
        previous_bitrate=st.integers(min_value=32, max_value=320),
    )
    @example(current_grade=None, current_bitrate=128, previous_bitrate=96)
    def test_detail_grade_and_bitrate_come_from_the_current_evidence(
        self, current_grade: str | None, current_bitrate: int, previous_bitrate: int,
    ) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=100, mb_release_id="test-mbid-0100",
            current_spectral_grade="likely_transcode",
            current_spectral_bitrate=previous_bitrate,
        ))
        evidence = make_album_quality_evidence(
            mb_release_id="test-mbid-0100",
            measurement=AudioQualityMeasurement(
                format="MP3", min_bitrate_kbps=320, avg_bitrate_kbps=320,
                spectral_grade=current_grade,
                spectral_bitrate_kbps=current_bitrate if current_grade else None,
                codec_family="mp3" if current_grade else None,
            ),
        )
        db.upsert_album_quality_evidence(evidence)
        stored = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.assertTrue(db.set_request_current_evidence(100, stored.id))

        with install_runtime(make_web_runtime(runtime(), db=db)):
            status, data = self._get("/api/pipeline/100")

        self.assertEqual(status, 200)
        self.assertEqual(data["request"]["current_spectral_grade"], current_grade)
        self.assertEqual(
            data["request"]["current_spectral_bitrate"],
            current_bitrate if current_grade else None,
        )


if __name__ == "__main__":
    unittest.main()
