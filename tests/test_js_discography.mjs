/**
 * Unit tests for web/js/discography.js pure helpers.
 * Run with: node tests/test_js_discography.mjs
 */

import { synthesizeMasterlessRow, splitPressings, statusChipHtml } from '../web/js/discography.js';

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

console.log('splitPressings() — owned/in-flight pressings are never hidden (The Meadowlands pin)');
{
  // Live confusion (request 4228, The Wrens "The Meadowlands"): the
  // library copy is the 2002 US Promotion pressing, which the old split
  // buried inside the collapsed Bootleg / Promo section — the expansion
  // contradicted the row's "in library · wanted" badges.
  const rows = [
    { id: 'cef6b0f6', status: 'Promotion', in_library: true, pipeline_status: 'wanted' },
    { id: '2aa0ae0e', status: 'Bootleg', in_library: false, pipeline_status: null },
    { id: 'fef45b67', status: 'Official', in_library: false, pipeline_status: 'downloading' },
    { id: 'a0fadcc2', status: 'Official', in_library: false, pipeline_status: null },
  ];
  const { visible, hidden } = splitPressings(rows);
  assertEqual(visible.some(r => r.id === 'cef6b0f6'), true, 'owned promo is visible');
  assertEqual(hidden.some(r => r.id === 'cef6b0f6'), false, 'owned promo not hidden');
  assertEqual(hidden.length, 1, 'only the unowned bootleg is hidden');
  assertEqual(hidden[0].id, '2aa0ae0e', 'unowned bootleg stays in the collapsed bucket');
  assertEqual(visible.length, 3, 'officials + owned promo visible');
}

console.log('splitPressings() — partition + hoist invariants over the status/ownership space');
{
  const statuses = [undefined, '', 'Official', 'Promotion', 'Bootleg', 'Pseudo-Release'];
  const ownerships = [
    { in_library: false, pipeline_status: null },
    { in_library: true, pipeline_status: null },
    { in_library: false, pipeline_status: 'downloading' },
    { in_library: true, pipeline_status: 'wanted' },
    { in_library: false, pipeline_status: 'imported' },
    { in_library: false, pipeline_status: 'manual' },
    { in_library: false, pipeline_status: 'replaced' },
    { in_library: true, pipeline_status: 'replaced' },
  ];
  let n = 0;
  const rows = [];
  for (const status of statuses) {
    for (const own of ownerships) {
      rows.push({ id: `r${n++}`, status, ...own });
    }
  }
  const { visible, hidden } = splitPressings(rows);
  assertEqual(visible.length + hidden.length, rows.length, 'every row lands in exactly one bucket');
  for (const r of rows) {
    const inVisible = visible.includes(r);
    const inHidden = hidden.includes(r);
    assertEqual(inVisible !== inHidden, true, `${r.id} in exactly one bucket`);
    // 'replaced' is the terminal frozen-audit status — an abandoned
    // request is NOT an active claim on the pressing and must not pin it.
    const owned = r.in_library === true
      || (!!r.pipeline_status && r.pipeline_status !== 'replaced');
    const official = r.status === 'Official' || !r.status;
    if (owned || official) {
      assertEqual(inVisible, true, `${r.id} (status=${r.status}, owned=${owned}) must be visible`);
    } else {
      assertEqual(inHidden, true, `${r.id} (status=${r.status}, unowned non-official) must be hidden`);
    }
  }
}

console.log('splitPressings() — a replaced-only pipeline row does not pin (frozen audit, badge-less)');
{
  // badges.js renders no badge for 'replaced', so hoisting such a row
  // would put an unexplained bootleg in the main list. Only in_library
  // or an ACTIVE pipeline status pins.
  const rows = [
    { id: 'abandoned', status: 'Bootleg', in_library: false, pipeline_status: 'replaced' },
    { id: 'owned-abandoned', status: 'Promotion', in_library: true, pipeline_status: 'replaced' },
  ];
  const { visible, hidden } = splitPressings(rows);
  assertEqual(hidden.length, 1, 'unowned replaced bootleg stays collapsed');
  assertEqual(hidden[0].id, 'abandoned', 'the replaced-only row is the hidden one');
  assertEqual(visible.length, 1, 'library ownership still pins a replaced row');
  assertEqual(visible[0].id, 'owned-abandoned', 'in_library wins over replaced');
}

console.log('splitPressings() — known-bad self-check: the OLD split violates the hoist invariant');
{
  // Prove the assertion above actually constrains something: the
  // pre-fix split (status-only) hides the owned promo.
  const rows = [{ id: 'x', status: 'Promotion', in_library: true, pipeline_status: null }];
  const oldHidden = rows.filter(r => r.status && r.status !== 'Official');
  assertEqual(oldHidden.length, 1, 'old split hides the owned promo (the bug)');
  assertEqual(splitPressings(rows).hidden.length, 0, 'new split does not');
}

console.log('statusChipHtml() — non-official pressings get a provenance chip');
{
  assertEqual(statusChipHtml('Official'), '', 'Official -> no chip');
  assertEqual(statusChipHtml(''), '', 'empty -> no chip');
  assertEqual(statusChipHtml(undefined), '', 'missing -> no chip');
  assertEqual(statusChipHtml('Promotion').includes('promo'), true, 'Promotion -> promo chip');
  assertEqual(statusChipHtml('Promotion').includes('badge-nonofficial'), true, 'chip uses the nonofficial badge class');
  assertEqual(statusChipHtml('Bootleg').includes('bootleg'), true, 'Bootleg -> bootleg chip');
  assertEqual(statusChipHtml('Pseudo-Release').includes('pseudo-release'), true, 'other statuses lowercased verbatim');
}

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed > 0 ? 1 : 0);
