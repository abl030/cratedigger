/**
 * Unit tests for web/js/discography.js pure helpers.
 * Run with: node tests/test_js_discography.mjs
 */

import { synthesizeMasterlessRow } from '../web/js/discography.js';

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

console.log('synthesizeMasterlessRow() — overlay fields survive the synthesis');
{
  // The live bug (request 8838, Deloris "Feather Figure/Elastic Bones"):
  // the payload carried pipeline_status=wanted but the synthetic pressing
  // row dropped it, rendering a green "Add request" on an
  // already-requested release.
  const row = synthesizeMasterlessRow({
    id: '8317023',
    title: 'Feather Figure/Elastic Bones',
    date: '2005-06-00',
    country: 'Australia',
    status: 'Official',
    formats: [{ name: 'CD' }],
    tracks: new Array(10).fill({ title: 't' }),
    labels: [{ id: 1, name: 'Dot Dash' }],
    release_group_id: null,
    in_library: false,
    beets_album_id: null,
    pipeline_status: 'wanted',
    pipeline_id: 8838,
  });
  assertEqual(row.pipeline_status, 'wanted', 'pipeline_status forwarded');
  assertEqual(row.pipeline_id, 8838, 'pipeline_id forwarded');
  assertEqual(row.in_library, false, 'in_library forwarded');
  assertEqual(row.beets_album_id, null, 'beets_album_id forwarded');
  assertEqual(row.id, '8317023', 'id kept');
  assertEqual(row.title, 'Feather Figure/Elastic Bones', 'title kept');
  assertEqual(row.format, 'CD', 'formats joined');
  assertEqual(row.track_count, 10, 'track count derived');
  assertEqual(row.status, 'Official', 'status kept');
}

console.log('synthesizeMasterlessRow() — in-library payload keeps quality fields');
{
  const row = synthesizeMasterlessRow({
    id: '999',
    title: 'Owned One',
    tracks: [],
    formats: [],
    in_library: true,
    beets_album_id: 42,
    pipeline_status: 'imported',
    pipeline_id: 7,
    library_format: 'FLAC',
    library_min_bitrate: 900,
    library_rank: 'lossless',
  });
  assertEqual(row.in_library, true, 'in_library true forwarded');
  assertEqual(row.beets_album_id, 42, 'beets_album_id forwarded');
  assertEqual(row.library_format, 'FLAC', 'library_format forwarded');
  assertEqual(row.library_min_bitrate, 900, 'library_min_bitrate forwarded');
  assertEqual(row.library_rank, 'lossless', 'library_rank forwarded');
  assertEqual(row.format, '?', 'empty formats fall back to ?');
}

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed > 0 ? 1 : 0);
