/**
 * Unit tests for web/js/grouping.js — pure classification + section render.
 * Run with: node tests/test_js_grouping.mjs
 */

import { classify, renderTypedSections, SECTION_ORDER } from '../web/js/grouping.js';

import { suite } from './js_harness.mjs';

const t = suite(import.meta.url);

t.section('classify()');
// MB-style RGs (type + secondary_types)
t.equal(classify({ type: 'Album', secondary_types: [] }), 'Albums', 'plain Album');
t.equal(classify({ type: 'EP', secondary_types: [] }), 'EPs', 'plain EP');
t.equal(classify({ type: 'Single', secondary_types: [] }), 'Singles', 'plain Single');
t.equal(classify({ type: 'Album', secondary_types: ['Compilation'] }), 'Compilations', 'compilation wins over Album');
t.equal(classify({ type: 'Album', secondary_types: ['Live'] }), 'Live', 'live wins over Album');
t.equal(classify({ type: 'Album', secondary_types: ['Remix'] }), 'Remixes', 'remix wins');
t.equal(classify({ type: 'Album', secondary_types: ['DJ-mix'] }), 'DJ Mixes', 'DJ-mix wins');
t.equal(classify({ type: 'Album', secondary_types: ['Demo'] }), 'Demos', 'demo wins');
t.equal(classify({ type: 'Album', secondary_types: ['Mixtape/Street'] }), 'Other', 'unknown secondary -> Other');

// Normalized artist rows: structural membership is authoritative. The legacy
// representative scalar is never allowed to invent an Album/EP/Single.
t.equal(classify({
  type: 'Album', primary_types: [], secondary_types: [], format_qualifiers: [],
}), 'Other', 'empty structural evidence defeats representative scalar Album');
t.equal(classify({
  type: 'Other', primary_types: ['EP'], secondary_types: [], format_qualifiers: [],
}), 'EPs', 'positive structural EP evidence owns the section');
t.equal(classify({
  type: 'Album', primary_types: [], secondary_types: [],
  format_qualifiers: ['Compilation'],
}), 'Compilations', 'Discogs Compilation qualifier stays a compilation');
t.equal(classify({
  primary_types: ['Album'], secondary_types: [], format_qualifiers: ['Remix'],
  display_primary_types: ['Album'], display_secondary_types: ['Live'],
  display_format_qualifiers: ['Remix', 'Demo'],
}), 'Live', 'paired display Live evidence overrides selected Album/Remix evidence');
t.equal(classify({
  primary_types: [], secondary_types: [], format_qualifiers: [],
  display_primary_types: ['EP'], display_secondary_types: [],
  display_format_qualifiers: [],
}), 'EPs', 'paired display structural evidence fills a selected unknown type');

// Analysis-style (primary_type)
t.equal(classify({ primary_type: 'Album' }), 'Albums', 'analysis primary_type Album');
t.equal(classify({ primary_type: 'Single' }), 'Singles', 'analysis primary_type Single');

// Library-style (lowercase from beets albumtype)
t.equal(classify({ type: 'album' }), 'Albums', 'beets lowercase album');
t.equal(classify({ type: 'ep' }), 'EPs', 'beets lowercase ep');
t.equal(classify({ type: 'single' }), 'Singles', 'beets lowercase single');
t.equal(classify({ type: 'compilation' }), 'Compilations', 'beets compilation');
t.equal(classify({ type: 'soundtrack' }), 'Compilations', 'beets soundtrack -> Compilations');
t.equal(classify({ type: 'live' }), 'Live', 'beets live');
t.equal(classify({ type: '' }), 'Other', 'empty -> Other');
t.equal(classify({}), 'Other', 'no type -> Other');

t.section('SECTION_ORDER');
t.equal(SECTION_ORDER[0], 'Albums', 'Albums is first in order');
t.equal(SECTION_ORDER[2], 'Singles', 'Singles is third');

t.section('renderTypedSections()');
const rows = [
  { id: 'a1', title: 'First Album', type: 'Album', first_release_date: '2001' },
  { id: 'a2', title: 'Second Album', type: 'Album', first_release_date: '2003' },
  { id: 'e1', title: 'An EP', type: 'EP', first_release_date: '2002' },
  { id: 's1', title: 'A Single', type: 'Single', first_release_date: '2000' },
];
const html = renderTypedSections(rows, (r) => `<div data-id="${r.id}">${r.title}</div>`);

// Each section header rendered with its count
t.contains(html, 'Albums <span class="type-count">2</span>', 'Albums section header has count 2');
t.contains(html, 'EPs <span class="type-count">1</span>', 'EPs section header');
t.contains(html, 'Singles <span class="type-count">1</span>', 'Singles section header');

// Within Albums, sorted by date — first comes before second
const firstIdx = html.indexOf('First Album');
const secondIdx = html.indexOf('Second Album');
t.equal(firstIdx < secondIdx, true, 'within Albums, oldest first');

// Section order: Albums before EPs before Singles
const albumsIdx = html.indexOf('Albums <span');
const epsIdx = html.indexOf('EPs <span');
const singlesIdx = html.indexOf('Singles <span');
t.equal(albumsIdx < epsIdx, true, 'Albums section appears before EPs section');
t.equal(epsIdx < singlesIdx, true, 'EPs section appears before Singles section');

// Albums section is open by default
t.contains(html, '<div class="type-body open">', 'Albums section is open by default');

t.section('renderTypedSections() with custom classify');
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
t.contains(cmpHtml, 'Albums <span class="type-count">1</span>', 'compare: Albums bucket counted');
t.contains(cmpHtml, 'EPs <span class="type-count">1</span>', 'compare: EPs bucket counted');

t.section('renderTypedSections() with defaultOpen=null');
const closedHtml = renderTypedSections(rows, (r) => '', { defaultOpen: null });
t.equal(closedHtml.includes('<div class="type-body open">'), false,
  'no section is open when defaultOpen=null');

t.section('renderTypedSections() with multiple explicit open sections');
const selectedHtml = renderTypedSections(rows, (r) => `<div>${r.title}</div>`, {
  defaultOpen: null,
  openSections: ['EPs', 'Singles'],
});
function typeIsOpen(html, type) {
  const start = html.indexOf(`${type} <span class="type-count">`);
  if (start < 0) return false;
  const body = html.slice(start).match(/<div class="type-body([^"]*)">/);
  return Boolean(body && body[1].split(/\s+/).includes('open'));
}
t.equal(typeIsOpen(selectedHtml, 'Albums'), false,
  'Albums stays closed when it is not selected');
t.equal(typeIsOpen(selectedHtml, 'EPs'), true,
  'EPs opens when selected');
t.equal(typeIsOpen(selectedHtml, 'Singles'), true,
  'Singles opens when selected');

t.section('renderTypedSections() empty input');
const emptyHtml = renderTypedSections([], (r) => '');
t.equal(emptyHtml, '', 'empty input -> empty output');

t.done();
