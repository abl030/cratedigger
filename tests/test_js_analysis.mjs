/**
 * Unit tests for web/js/analysis.js — the unique-track analysis overlay
 * (issue #575 PR4): chip rendering + recording-dot computation.
 * Run with: node tests/test_js_analysis.mjs
 */

import { analysisChipHtml, computeRecordingDots, renderRecordingsBlock } from '../web/js/analysis.js';

let passed = 0;
let failed = 0;

function assertEqual(actual, expected, msg) {
  if (actual === expected) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg} - expected '${expected}', got '${actual}'`);
  }
}

function assertContains(haystack, needle, msg) {
  if (haystack.includes(needle)) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg} - '${needle}' not in output`);
  }
}

console.log('analysisChipHtml() — coverage precedence');
{
  assertContains(analysisChipHtml({ covered_by: 'Some <Comp>', unique_track_count: 3 }),
    'covered by Some &lt;Comp&gt;', 'covered_by wins over unique count, escaped');
  assertContains(analysisChipHtml({ covered_by: null, unique_track_count: 9 }),
    '9 unique', 'unique count chip');
  assertContains(analysisChipHtml({ covered_by: null, unique_track_count: 0 }),
    '0 unique', 'zero-unique chip');
}

console.log('computeRecordingDots() — membership + exclusives');
{
  // Two pressings: P0 has r1,r2; P1 has r2,r3. r1 exclusive to P0,
  // r3 exclusive to P1, r2 shared.
  const rg = {
    pressings: [
      { release_id: 'p0', recording_ids: ['r1', 'r2'] },
      { release_id: 'p1', recording_ids: ['r2', 'r3'] },
    ],
    tracks: [
      { recording_id: 'r1', title: 'One', unique: true },
      { recording_id: 'r2', title: 'Two', unique: true },
      { recording_id: 'r3', title: 'Three', unique: true },
    ],
  };
  const { trackToPressings, pressingExclusiveCounts, totalPressings } = computeRecordingDots(rg);
  assertEqual(totalPressings, 2, 'two pressings');
  assertEqual(trackToPressings['r1'].join(','), '0', 'r1 only on P0');
  assertEqual(trackToPressings['r2'].join(','), '0,1', 'r2 on both');
  assertEqual(trackToPressings['r3'].join(','), '1', 'r3 only on P1');
  assertEqual(pressingExclusiveCounts.join(','), '1,1', 'one exclusive each');
}

console.log('renderRecordingsBlock() — markers stay with titles');
{
  const rg = {
    pressings: [
      { release_id: 'p0', recording_ids: ['r1', 'r2'] },
      { release_id: 'p1', recording_ids: ['r2'] },
    ],
    tracks: [
      { recording_id: 'r1', title: 'Only On P0', unique: true },
      { recording_id: 'r2', title: 'Everywhere', unique: true },
      { recording_id: 'r9', title: 'Comp Track', unique: false, also_on: ['Best Of'] },
    ],
  };
  const html = renderRecordingsBlock(rg);
  assertContains(html, 'Recordings:', 'heading present');
  // Single-span rows: marker and title inside one <span> (the flex
  // justify-between fix from PR3's screenshot loop).
  assertContains(html, '●</span></span>Only On P0', 'dot adjacent to partial-coverage title');
  assertContains(html, '★</span> Everywhere', 'star adjacent to all-pressings title');
  assertContains(html, 'also on: Best Of', 'non-unique row keeps also-on note');
  assertEqual(renderRecordingsBlock({ tracks: [] }), '', 'no tracks -> empty');
}

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed > 0 ? 1 : 0);
