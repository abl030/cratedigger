/**
 * Unit tests for web/js/grouping.js — pure classification + section render.
 * Run with: node tests/test_js_grouping.mjs
 */

import { classify, renderTypedSections, SECTION_ORDER } from '../web/js/grouping.js';

let passed = 0;
let failed = 0;

function assertEqual(actual, expected, msg) {
  if (actual === expected) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg} — expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
}

function assertContains(haystack, needle, msg) {
  if (haystack.includes(needle)) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg} — '${needle}' not found in output`);
  }
}

console.log('classify()');
// MB-style RGs (type + secondary_types)
assertEqual(classify({ type: 'Album', secondary_types: [] }), 'Albums', 'plain Album');
assertEqual(classify({ type: 'EP', secondary_types: [] }), 'EPs', 'plain EP');
assertEqual(classify({ type: 'Single', secondary_types: [] }), 'Singles', 'plain Single');
assertEqual(classify({ type: 'Album', secondary_types: ['Compilation'] }), 'Compilations', 'compilation wins over Album');
assertEqual(classify({ type: 'Album', secondary_types: ['Live'] }), 'Live', 'live wins over Album');
assertEqual(classify({ type: 'Album', secondary_types: ['Remix'] }), 'Remixes', 'remix wins');
assertEqual(classify({ type: 'Album', secondary_types: ['DJ-mix'] }), 'DJ Mixes', 'DJ-mix wins');
assertEqual(classify({ type: 'Album', secondary_types: ['Demo'] }), 'Demos', 'demo wins');
assertEqual(classify({ type: 'Album', secondary_types: ['Mixtape/Street'] }), 'Other', 'unknown secondary -> Other');

// Analysis-style (primary_type)
assertEqual(classify({ primary_type: 'Album' }), 'Albums', 'analysis primary_type Album');
assertEqual(classify({ primary_type: 'Single' }), 'Singles', 'analysis primary_type Single');

// Library-style (lowercase from beets albumtype)
assertEqual(classify({ type: 'album' }), 'Albums', 'beets lowercase album');
assertEqual(classify({ type: 'ep' }), 'EPs', 'beets lowercase ep');
assertEqual(classify({ type: 'single' }), 'Singles', 'beets lowercase single');
assertEqual(classify({ type: 'compilation' }), 'Compilations', 'beets compilation');
assertEqual(classify({ type: 'soundtrack' }), 'Compilations', 'beets soundtrack -> Compilations');
assertEqual(classify({ type: 'live' }), 'Live', 'beets live');
assertEqual(classify({ type: '' }), 'Other', 'empty -> Other');
assertEqual(classify({}), 'Other', 'no type -> Other');

console.log('SECTION_ORDER');
assertEqual(SECTION_ORDER[0], 'Albums', 'Albums is first in order');
assertEqual(SECTION_ORDER[2], 'Singles', 'Singles is third');

console.log('renderTypedSections()');
const rows = [
  { id: 'a1', title: 'First Album', type: 'Album', first_release_date: '2001' },
  { id: 'a2', title: 'Second Album', type: 'Album', first_release_date: '2003' },
  { id: 'e1', title: 'An EP', type: 'EP', first_release_date: '2002' },
  { id: 's1', title: 'A Single', type: 'Single', first_release_date: '2000' },
];
const html = renderTypedSections(rows, (r) => `<div data-id="${r.id}">${r.title}</div>`);

// Each section header rendered with its count
assertContains(html, 'Albums <span class="type-count">2</span>', 'Albums section header has count 2');
assertContains(html, 'EPs <span class="type-count">1</span>', 'EPs section header');
assertContains(html, 'Singles <span class="type-count">1</span>', 'Singles section header');

// Within Albums, sorted by date — first comes before second
const firstIdx = html.indexOf('First Album');
const secondIdx = html.indexOf('Second Album');
assertEqual(firstIdx < secondIdx, true, 'within Albums, oldest first');

// Section order: Albums before EPs before Singles
const albumsIdx = html.indexOf('Albums <span');
const epsIdx = html.indexOf('EPs <span');
const singlesIdx = html.indexOf('Singles <span');
assertEqual(albumsIdx < epsIdx, true, 'Albums section appears before EPs section');
assertEqual(epsIdx < singlesIdx, true, 'EPs section appears before Singles section');

// Albums section is open by default
assertContains(html, '<div class="type-body open">', 'Albums section is open by default');

console.log('renderTypedSections() with custom classify');
const compareRows = [
  { mb: { type: 'Album', first_release_date: '2001' }, discogs: { type: 'Album' } },
  { mb: { type: 'EP', first_release_date: '2002' }, discogs: null },
];
const cmpHtml = renderTypedSections(
  compareRows,
  (p) => `<div>${p.mb?.first_release_date || '?'}</div>`,
  {
    classify: (p) => classify(p.mb || p.discogs),
    dateOf: (p) => String((p.mb || p.discogs).first_release_date || ''),
  },
);
assertContains(cmpHtml, 'Albums <span class="type-count">1</span>', 'compare: Albums bucket counted');
assertContains(cmpHtml, 'EPs <span class="type-count">1</span>', 'compare: EPs bucket counted');

console.log('renderTypedSections() with defaultOpen=null');
const closedHtml = renderTypedSections(rows, (r) => '', { defaultOpen: null });
assertEqual(closedHtml.includes('<div class="type-body open">'), false,
  'no section is open when defaultOpen=null');

console.log('renderTypedSections() empty input');
const emptyHtml = renderTypedSections([], (r) => '');
assertEqual(emptyHtml, '', 'empty input -> empty output');

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
