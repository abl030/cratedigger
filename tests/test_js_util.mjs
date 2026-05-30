/**
 * Unit tests for web/js/util.js — pure utility functions.
 * Run with: node tests/test_js_util.mjs
 */

import { qualityLabel, qualityLabelShort, toAWST, awstDate, awstTime, awstDateTime, esc, jsArg, overrideToIntent, detectSource, externalReleaseUrl, sourceLabel, manualReasonLabel, renderForensicBlock, parsePastedId, youtubeSectionState, consoleEmphasis } from '../web/js/util.js';
import { state } from '../web/js/state.js';
import { applyLabelFilters, sortByYearDesc, buildLabelSearchUrl, buildLabelDetailUrl, loadLabelReleases, parseYear, renderLabelLinks, distinctFormats, renderPaginationControls, renderLabelRows } from '../web/js/labels.js';
import { __test__ as longTailTest } from '../web/js/long_tail.js';

let passed = 0;
let failed = 0;

function assert(condition, msg) {
  if (condition) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg}`);
  }
}

function assertEqual(actual, expected, msg) {
  if (actual === expected) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg} — expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
}

// --- qualityLabel tests ---
console.log('qualityLabel()');
assertEqual(qualityLabel('FLAC', 1000), 'FLAC', 'FLAC ignores bitrate');
assertEqual(qualityLabel('ALAC', 800), 'ALAC', 'ALAC ignores bitrate');
assertEqual(qualityLabel('MP3', 320), 'MP3 320', 'MP3 320kbps');
assertEqual(qualityLabel('MP3', 295), 'MP3 320', 'MP3 295 rounds to 320');
assertEqual(qualityLabel('MP3', 245), 'MP3 V0', 'MP3 245 = V0');
assertEqual(qualityLabel('MP3', 220), 'MP3 V0', 'MP3 220 = V0');
assertEqual(qualityLabel('MP3', 190), 'MP3 V2', 'MP3 190 = V2');
assertEqual(qualityLabel('MP3', 170), 'MP3 V2', 'MP3 170 = V2');
assertEqual(qualityLabel('MP3', 128), 'MP3 128k', 'MP3 128 shows raw');
assertEqual(qualityLabel('MP3', 0), 'MP3', 'MP3 0 bitrate = just format');
assertEqual(qualityLabel('MP3', null), 'MP3', 'MP3 null bitrate = just format');
assertEqual(qualityLabel(null, 320), '?', 'null format = ?');
assertEqual(qualityLabel('', 320), '?', 'empty format = ?');
assertEqual(qualityLabel('MP3,FLAC', 250), 'MP3 V0', 'comma-separated uses first');

// --- qualityLabelShort tests ---
console.log('qualityLabelShort()');
assertEqual(qualityLabelShort('MP3', 245), 'M V0', 'MP3 245 -> M V0');
assertEqual(qualityLabelShort('MP3', 190), 'M V2', 'MP3 190 -> M V2');
assertEqual(qualityLabelShort('MP3', 320), 'M 320', 'MP3 320 -> M 320');
assertEqual(qualityLabelShort('MP3', 128), 'M 128', 'MP3 128 -> M 128');
assertEqual(qualityLabelShort('FLAC', 1000), 'F', 'FLAC -> F (no bitrate suffix)');
assertEqual(qualityLabelShort('ALAC', 800), 'AL', 'ALAC -> AL');
assertEqual(qualityLabelShort('WAV', 1411), 'W', 'WAV -> W');
assertEqual(qualityLabelShort('Opus', 128), 'O 128', 'Opus 128 -> O 128');
assertEqual(qualityLabelShort('AAC', 192), 'A 192', 'AAC 192 -> A 192');
assertEqual(qualityLabelShort('OGG', 192), 'OG 192', 'OGG -> OG');
assertEqual(qualityLabelShort('', 320), '?', 'empty format');
assertEqual(qualityLabelShort(null, 320), '?', 'null format');
assertEqual(qualityLabelShort('MP3', 0), 'M', 'zero bitrate shows format only');
assertEqual(qualityLabelShort('MP3', null), 'M', 'null bitrate shows format only');

// --- toAWST tests ---
console.log('toAWST()');
// UTC midnight = 8am AWST
assertEqual(toAWST('2026-04-01T00:00:00Z'), '2026-04-01T08:00:00', 'UTC midnight = 08:00 AWST');
assertEqual(toAWST('2026-04-01T16:00:00Z'), '2026-04-02T00:00:00', 'UTC 16:00 = next day 00:00 AWST');
assertEqual(toAWST('2026-12-31T20:00:00Z'), '2027-01-01T04:00:00', 'year boundary');

// --- awstDate tests ---
console.log('awstDate()');
assertEqual(awstDate('2026-04-01T00:00:00Z'), '2026-04-01', 'date from UTC midnight');

// --- awstTime tests ---
console.log('awstTime()');
assertEqual(awstTime('2026-04-01T00:00:00Z'), '08:00', 'time from UTC midnight');

// --- awstDateTime tests ---
console.log('awstDateTime()');
assertEqual(awstDateTime('2026-04-01T00:00:00Z'), '2026-04-01 08:00', 'datetime from UTC midnight');

// --- esc tests ---
console.log('esc()');
assertEqual(esc('hello'), 'hello', 'plain text unchanged');
assertEqual(esc('<script>alert(1)</script>'), '&lt;script&gt;alert(1)&lt;/script&gt;', 'escapes HTML tags');
assertEqual(esc('a & b'), 'a &amp; b', 'escapes ampersand');
assertEqual(esc('"quotes"'), '&quot;quotes&quot;', 'escapes double quotes');
assertEqual(esc("Guns N' Roses"), 'Guns N&#39; Roses', 'escapes single quotes');
assertEqual(esc('back\\slash'), 'back&#92;slash', 'escapes backslashes');
assertEqual(esc("it\\'s"), 'it&#92;&#39;s', 'escapes backslash+quote combo');
assertEqual(esc(''), '', 'empty string');
assertEqual(esc(null), '', 'null returns empty');
assertEqual(esc(undefined), '', 'undefined returns empty');

// --- overrideToIntent tests ---
console.log('overrideToIntent()');
assertEqual(overrideToIntent(null), 'default', 'null → default');
assertEqual(overrideToIntent(undefined), 'default', 'undefined → default');
assertEqual(overrideToIntent(''), 'default', 'empty string → default');
assertEqual(overrideToIntent('lossless'), 'lossless', '"lossless" → lossless');
assertEqual(overrideToIntent('flac'), 'lossless', '"flac" (backward compat) → lossless');
assertEqual(overrideToIntent('flac,mp3 v0,mp3 320'), 'default', 'CSV → default');
assertEqual(overrideToIntent('unknown'), 'default', 'unknown → default');

// --- jsArg tests ---
console.log('jsArg()');
assertEqual(jsArg("Kid A's"), '&quot;Kid A&#39;s&quot;', 'encodes apostrophes inside JS string literal');
assertEqual(jsArg(null), '&quot;&quot;', 'null becomes empty string literal');

// --- detectSource tests ---
console.log('detectSource()');
assertEqual(detectSource('89ad4ac3-39f7-470e-963a-56509c546377'), 'musicbrainz', 'UUID → musicbrainz');
assertEqual(detectSource(' 89AD4AC3-39F7-470E-963A-56509C546377 '), 'musicbrainz', 'UUID whitespace/case normalizes');
assertEqual(detectSource('2048516'), 'discogs', 'numeric → discogs');
assertEqual(detectSource(' 0012856590 '), 'discogs', 'numeric whitespace/leading zeros normalize');
assertEqual(detectSource(''), 'unknown', 'empty → unknown');
assertEqual(detectSource('0'), 'unknown', 'zero sentinel → unknown');
assertEqual(detectSource(null), 'unknown', 'null → unknown');
assertEqual(detectSource(undefined), 'unknown', 'undefined → unknown');
assertEqual(detectSource('NONE'), 'unknown', 'NONE → unknown');

// --- externalReleaseUrl tests ---
console.log('externalReleaseUrl()');
assertEqual(
  externalReleaseUrl('89ad4ac3-39f7-470e-963a-56509c546377'),
  'https://musicbrainz.org/release/89ad4ac3-39f7-470e-963a-56509c546377',
  'MB UUID → musicbrainz.org'
);
assertEqual(
  externalReleaseUrl('2048516'),
  'https://www.discogs.com/release/2048516',
  'Discogs numeric → discogs.com'
);
assertEqual(
  externalReleaseUrl('not-a-real-id'),
  '',
  'unknown id → empty external URL'
);

// --- sourceLabel tests ---
console.log('sourceLabel()');
assertEqual(sourceLabel('89ad4ac3-39f7-470e-963a-56509c546377'), 'MusicBrainz', 'UUID → MusicBrainz');
assertEqual(sourceLabel('2048516'), 'Discogs', 'numeric → Discogs');
assertEqual(sourceLabel('not-a-real-id'), '', 'unknown id → empty source label');

// --- parseYear tests ---
console.log('parseYear()');
assertEqual(parseYear('2003'), 2003, 'year-only string');
assertEqual(parseYear('2003-04-15'), 2003, 'full ISO date');
assertEqual(parseYear('2003-04'), 2003, 'year-month');
assertEqual(parseYear(''), null, 'empty string → null');
assertEqual(parseYear(null), null, 'null → null');
assertEqual(parseYear(undefined), null, 'undefined → null');
assertEqual(parseYear('not-a-year'), null, 'garbage → null');

// --- buildLabelSearchUrl tests ---
console.log('buildLabelSearchUrl()');
assertEqual(buildLabelSearchUrl('hymen'), '/api/discogs/label/search?q=hymen', 'simple query');
assertEqual(buildLabelSearchUrl('warp records'), '/api/discogs/label/search?q=warp%20records', 'spaces encoded');
assertEqual(buildLabelSearchUrl('a&b'), '/api/discogs/label/search?q=a%26b', 'special chars encoded');
assertEqual(buildLabelSearchUrl('björk'), '/api/discogs/label/search?q=bj%C3%B6rk', 'unicode encoded');

// --- buildLabelDetailUrl tests ---
console.log('buildLabelDetailUrl()');
assertEqual(buildLabelDetailUrl('757'), '/api/discogs/label/757', 'no opts: no query string');
assertEqual(
  buildLabelDetailUrl('757', { include_sublabels: true }),
  '/api/discogs/label/757?include_sublabels=true',
  'include_sublabels=true emitted');
assertEqual(
  buildLabelDetailUrl('757', { include_sublabels: false }),
  '/api/discogs/label/757?include_sublabels=false',
  'include_sublabels=false emitted');
assertEqual(
  buildLabelDetailUrl('757', { include_sublabels: true, page: 2, per_page: 50 }),
  '/api/discogs/label/757?include_sublabels=true&page=2&per_page=50',
  'pagination params emitted in order');
assertEqual(
  buildLabelDetailUrl(757, { page: 3 }),
  '/api/discogs/label/757?page=3',
  'numeric labelId coerced to string');
assertEqual(
  buildLabelDetailUrl('757', { include_sublabels: undefined, page: undefined, per_page: undefined }),
  '/api/discogs/label/757',
  'undefined opts produce no params');

async function captureLoadLabelUrl(opts) {
  const originalFetch = globalThis.fetch;
  let seenUrl = '';
  globalThis.fetch = async (url) => {
    seenUrl = String(url);
    return { ok: true, json: async () => ({ ok: true }) };
  };
  try {
    await loadLabelReleases('757', opts);
    return seenUrl;
  } finally {
    globalThis.fetch = originalFetch;
  }
}

assertEqual(
  await captureLoadLabelUrl({ page: 1 }),
  '/api/discogs/label/757?page=1',
  'default label load omits include_sublabels so route auto-flip can run');
assertEqual(
  await captureLoadLabelUrl({ include_sublabels: true, page: 2 }),
  '/api/discogs/label/757?include_sublabels=true&page=2',
  'explicit include_sublabels=true is preserved');
assertEqual(
  await captureLoadLabelUrl({ include_sublabels: false, page: 2 }),
  '/api/discogs/label/757?include_sublabels=false&page=2',
  'explicit include_sublabels=false is preserved');

// --- renderPaginationControls tests ---
console.log('renderPaginationControls()');
assertEqual(renderPaginationControls(1, 1), '', 'pages=1 → empty');
assertEqual(renderPaginationControls(1, 0), '', 'pages=0 → empty');
const ctrl_p1_of_5 = renderPaginationControls(1, 5);
assert(ctrl_p1_of_5.includes('Page 1 of 5'), 'p1/5: position label rendered');
assert(ctrl_p1_of_5.includes('disabled'), 'p1/5: prev button is disabled');
assert(ctrl_p1_of_5.includes('window.goToLabelPage(2)'), 'p1/5: next button targets page 2');
const ctrl_p5_of_5 = renderPaginationControls(5, 5);
assert(ctrl_p5_of_5.includes('window.goToLabelPage(4)'), 'p5/5: prev button targets page 4');
assert(ctrl_p5_of_5.match(/disabled/g).length === 1, 'p5/5: only next button is disabled');
const ctrl_p3_of_5 = renderPaginationControls(3, 5);
assert(!ctrl_p3_of_5.includes('disabled'), 'p3/5: neither button disabled');
assert(ctrl_p3_of_5.includes('window.goToLabelPage(2)'), 'p3/5: prev → page 2');
assert(ctrl_p3_of_5.includes('window.goToLabelPage(4)'), 'p3/5: next → page 4');

// --- renderLabelRows tests ---
console.log('renderLabelRows()');
{
  state.labelFilters = { yearMin: null, yearMax: null, format: '', hideHeld: false };
  const body = { innerHTML: '' };
  const container = {
    _releases: [
      {
        id: '12856590',
        title: 'Greetings From Birmingham',
        artist_name: 'Scorn',
        date: '2000',
        format: 'Vinyl',
        primary_type: 'Other',
        in_library: false,
      },
    ],
    _hasAnySubLabel: false,
    querySelector: (selector) => selector === '#browse-label-rows' ? body : null,
  };
  renderLabelRows(container);
  assert(body.innerHTML.includes('Greetings From Birmingham'), 'label row renders release title');
  assert(body.innerHTML.includes('window.toggleReleaseDetail(&quot;12856590&quot;)'),
    'label row opens exact Discogs release details');
  assert(body.innerHTML.includes('id="reldet-12856590"'),
    'label row renders matching release-detail container');
  assert(!body.innerHTML.includes('window.loadReleaseGroup(&quot;12856590&quot;'),
    'label row does not route Discogs release id through release-group loader');
}

// --- applyLabelFilters tests ---
console.log('applyLabelFilters()');
const ROWS = [
  { id: '1', title: 'A', date: '2000-01-01', format: 'CD',  in_library: false },
  { id: '2', title: 'B', date: '2001-06-15', format: 'LP',  in_library: true  },
  { id: '3', title: 'C', date: '2002',       format: 'CD, EP', in_library: false },
  { id: '4', title: 'D', date: '2003-04-01', format: 'LP, Album', in_library: true },
  { id: '5', title: 'E', date: '2004-12-01', format: 'Vinyl', in_library: false },
  { id: '6', title: 'F', date: '',           format: 'CD',  in_library: false },
];

assertEqual(applyLabelFilters(ROWS, {}).length, 6, 'empty filters returns all rows');
assertEqual(applyLabelFilters(ROWS, { yearMin: null, yearMax: null, format: '', hideHeld: false }).length, 6, 'null/empty filters returns all rows');

const yearFilt = applyLabelFilters(ROWS, { yearMin: 2001, yearMax: 2003 });
assertEqual(yearFilt.length, 3, 'year [2001..2003] inclusive matches 3 rows');
assertEqual(yearFilt.map(r => r.id).join(','), '2,3,4', 'year filter keeps correct rows');

const yearOnlyMin = applyLabelFilters(ROWS, { yearMin: 2003 });
assertEqual(yearOnlyMin.map(r => r.id).join(','), '4,5', 'yearMin alone (drops empty-date row when filtered)');

const yearOnlyMax = applyLabelFilters(ROWS, { yearMax: 2001 });
assertEqual(yearOnlyMax.map(r => r.id).join(','), '1,2', 'yearMax alone');

// Empty-date rows survive year filtering ONLY when no year filter applied
const emptyDateNoFilter = applyLabelFilters(ROWS, { format: '' });
assertEqual(emptyDateNoFilter.find(r => r.id === '6') !== undefined, true,
  'empty-date row survives when no year filter applied');
const emptyDateYearFilter = applyLabelFilters(ROWS, { yearMin: 2000, yearMax: 2010 });
assertEqual(emptyDateYearFilter.find(r => r.id === '6'), undefined,
  'empty-date row dropped when year filter active');

const fmtLP = applyLabelFilters(ROWS, { format: 'LP' });
assertEqual(fmtLP.map(r => r.id).join(','), '2,4', 'format LP matches substring');
const fmtCD = applyLabelFilters(ROWS, { format: 'CD' });
assertEqual(fmtCD.map(r => r.id).join(','), '1,3,6', 'format CD matches substring');
const fmtEmpty = applyLabelFilters(ROWS, { format: '' });
assertEqual(fmtEmpty.length, 6, 'empty format means no filter');

const hideHeld = applyLabelFilters(ROWS, { hideHeld: true });
assertEqual(hideHeld.map(r => r.id).join(','), '1,3,5,6', 'hideHeld excludes in_library:true');

// All filters layered
const layered = applyLabelFilters(ROWS, { yearMin: 2000, yearMax: 2003, format: 'CD', hideHeld: true });
assertEqual(layered.map(r => r.id).join(','), '1,3', 'layered filters intersect correctly');

// --- sortByYearDesc tests ---
console.log('sortByYearDesc()');
const SORTED = sortByYearDesc([
  { id: '1', date: '2003-04-01' },
  { id: '2', date: '2001-01-01' },
  { id: '3', date: '' },
  { id: '4', date: '2003-12-31' },
  { id: '5', date: null },
]);
assertEqual(SORTED.map(r => r.id).join(','), '1,4,2,3,5',
  'year desc; missing year sorts last; equal-year stable by input order');

// stability across equal years
const STABLE = sortByYearDesc([
  { id: 'a', date: '2010' },
  { id: 'b', date: '2010' },
  { id: 'c', date: '2010' },
]);
assertEqual(STABLE.map(r => r.id).join(','), 'a,b,c', 'equal years preserve input order (stable)');

// does not mutate input
const ORIG = [{ id: '1', date: '2000' }, { id: '2', date: '2010' }];
sortByYearDesc(ORIG);
assertEqual(ORIG[0].id, '1', 'sortByYearDesc does not mutate input');

// --- renderLabelLinks tests (U7) ---
console.log('renderLabelLinks()');

// Single Discogs-style label (id + name) → clickable link.
const hymen = renderLabelLinks([{ id: 757, name: 'Hymen Records' }]);
assert(hymen.includes('Hymen Records'), 'renders the label name');
assert(hymen.includes('data-label-id="757"'), 'tags the link with data-label-id="757"');
assert(hymen.includes('window.openLabelDetail'), 'wires window.openLabelDetail call');
assert(hymen.includes('class="label-link"'), 'tags the anchor with the label-link class');
assert(/<a\b/i.test(hymen), 'renders an anchor element');

// Empty input → empty string.
assertEqual(renderLabelLinks([]), '', 'empty array → empty string');
assertEqual(renderLabelLinks(null), '', 'null → empty string');
assertEqual(renderLabelLinks(undefined), '', 'undefined → empty string');

// MB-style label (no id) → plain text, no anchor.
const mbOnly = renderLabelLinks([{ name: 'Some MB Label' }]);
assertEqual(mbOnly, 'Some MB Label', 'MB-style (no id) renders plain text');
assert(!/<a\b/i.test(mbOnly), 'MB-style renders no anchor element');

// id explicitly null → plain text (Phase B placeholder).
const mbExplicitNull = renderLabelLinks([{ id: null, name: 'MB Label' }]);
assertEqual(mbExplicitNull, 'MB Label', 'explicit id=null renders plain text');

// Multiple labels with usable IDs → comma-separated links.
const warpDual = renderLabelLinks([
  { id: 757, name: 'Warp Records' },
  { id: 758, name: 'Warp Singles' },
]);
assert(warpDual.includes('Warp Records'), 'first label name rendered');
assert(warpDual.includes('Warp Singles'), 'second label name rendered');
assert(warpDual.includes('data-label-id="757"'), 'first link has correct id attr');
assert(warpDual.includes('data-label-id="758"'), 'second link has correct id attr');
assertEqual((warpDual.match(/<a\b/gi) || []).length, 2, 'two anchor elements rendered');
assert(warpDual.includes('</a>, <a'), 'anchors are separated by ", "');

// Mixed: one with id (link), one without (text).
const mixed = renderLabelLinks([
  { id: 757, name: 'Hymen Records' },
  { name: 'Plaintext Co.' },
]);
assert(mixed.includes('Hymen Records'), 'mixed: linked name present');
assert(mixed.includes('Plaintext Co.'), 'mixed: plain name present');
assertEqual((mixed.match(/<a\b/gi) || []).length, 1, 'mixed: only the id-bearing entry becomes a link');

// XSS guard — name with <script> is escaped, no raw tag in output.
const xss = renderLabelLinks([{ id: 1, name: '<script>alert(1)</script>' }]);
assert(!xss.includes('<script>'), 'XSS guard: raw <script> tag not present in output');
assert(xss.includes('&lt;script&gt;'), 'XSS guard: angle brackets entity-escaped');

// XSS guard via name with quotes — should not break out of jsArg().
const xssQuote = renderLabelLinks([{ id: 1, name: 'Bad", alert(1), "X' }]);
assert(!xssQuote.includes('", alert'), 'XSS guard: quote escapes prevent attribute break-out');
assert(xssQuote.includes('&quot;'), 'XSS guard: double quotes are entity-escaped');

// Empty / falsy entries skipped.
assertEqual(renderLabelLinks([null, undefined, { id: 1, name: '' }, { id: 2, name: 'OK' }]),
  '<a href="#" class="label-link" data-label-id="2" onclick="event.stopPropagation(); event.preventDefault(); window.openLabelDetail(&quot;2&quot;, &quot;OK&quot;)">OK</a>',
  'null/undefined/empty-name entries are skipped');

// Numeric-string id is honored.
const stringId = renderLabelLinks([{ id: '12345', name: 'String ID Label' }]);
assert(stringId.includes('data-label-id="12345"'), 'string id is preserved');
assert(stringId.includes('<a'), 'string id renders as link');

// --- distinctFormats tests (review-fix #9) ---
console.log('distinctFormats()');

// Empty input → empty array.
const emptyFmts = distinctFormats([]);
assertEqual(Array.isArray(emptyFmts), true, 'empty input returns an array');
assertEqual(emptyFmts.length, 0, 'empty input → empty array');

// Single row, single format.
assertEqual(distinctFormats([{ format: 'CD' }]).join(','), 'CD',
  'single row single format');

// Duplicates dedup'd; sorted alphabetically.
const dups = distinctFormats([
  { format: 'CD' }, { format: 'CD' }, { format: 'LP' }, { format: 'CD' },
]);
assertEqual(dups.join(','), 'CD,LP', 'duplicates collapse, sort applied');

// Multi-value formats (joined Discogs string) split on commas.
const multi = distinctFormats([
  { format: 'LP, Album' },
  { format: 'CD, EP' },
  { format: 'Vinyl, LP' }, // LP appears in two rows, dedup'd
]);
assertEqual(multi.join(','), 'Album,CD,EP,LP,Vinyl',
  'comma-joined formats split, dedup, alphabetized');

// Whitespace trimmed; empty tokens dropped.
const ws = distinctFormats([
  { format: '  CD  ,  LP ,, ' },
  { format: '' },
]);
assertEqual(ws.join(','), 'CD,LP', 'whitespace trimmed, empty tokens dropped');

// Missing/null format field on a row — row skipped, no crash.
const nullFmt = distinctFormats([
  { format: null },
  { format: undefined },
  { /* no format key */ },
  { format: 'CD' },
]);
assertEqual(nullFmt.join(','), 'CD', 'null/undefined/missing format fields skipped');

// All missing → empty.
assertEqual(distinctFormats([{}, { format: '' }]).join(','), '',
  'no usable formats → empty array');

// --- applyLabelFilters NaN year guard tests (review-fix #10) ---
console.log('applyLabelFilters() NaN year guard');

const NAN_ROWS = [
  { id: '1', date: '2000-01-01', format: 'CD', in_library: false },
  { id: '2', date: '2010-01-01', format: 'CD', in_library: false },
  { id: '3', date: '',           format: 'CD', in_library: false },
];

// Explicit NaN bounds must behave as "no bound", not "drop everything".
const nanMin = applyLabelFilters(NAN_ROWS, { yearMin: NaN });
assertEqual(nanMin.length, 3, 'NaN yearMin treated as no lower bound');

const nanMax = applyLabelFilters(NAN_ROWS, { yearMax: NaN });
assertEqual(nanMax.length, 3, 'NaN yearMax treated as no upper bound');

const nanBoth = applyLabelFilters(NAN_ROWS, { yearMin: NaN, yearMax: NaN });
assertEqual(nanBoth.length, 3, 'both NaN bounds → no filter');

// NaN min + valid max → max still applies, undated still drops.
const mixedNan = applyLabelFilters(NAN_ROWS, { yearMin: NaN, yearMax: 2005 });
assertEqual(mixedNan.map(r => r.id).join(','), '1',
  'valid yearMax with NaN yearMin still filters correctly');

// --- manualReasonLabel tests ---
console.log('manualReasonLabel()');
assertEqual(manualReasonLabel(null), '', 'null → empty string');
assertEqual(manualReasonLabel(undefined), '', 'undefined → empty string');
assertEqual(manualReasonLabel(''), '', 'empty string → empty string');
assertEqual(manualReasonLabel('search_exhausted'), 'search exhausted',
  'search_exhausted → friendly label');
assertEqual(manualReasonLabel('custom_reason'), 'custom_reason',
  'unknown reason passes through unchanged');

// --- renderForensicBlock tests ---
console.log('renderForensicBlock()');
const noBlock = renderForensicBlock(null);
assert(noBlock.includes('No search forensic data yet'),
  'null last_search → "no forensic data" message');
assert(noBlock.includes('p-forensic'),
  'null last_search still wraps in .p-forensic for layout');

const emptyTopBlock = renderForensicBlock({
  variant: 'v1_year', final_state: 'Completed', outcome: 'no_match',
  top_candidates: [],
});
assert(emptyTopBlock.includes('v1_year'),
  'variant tag rendered');
assert(emptyTopBlock.includes('Completed'),
  'final_state rendered');
assert(emptyTopBlock.includes('No candidates captured'),
  'empty top_candidates → no-candidates body');

const populatedBlock = renderForensicBlock({
  variant: 'default', final_state: 'Completed', outcome: 'no_match',
  top_candidates: [
    { username: 'alice', dir: 'A\\Album', filetype: 'flac',
      matched_tracks: 26, total_tracks: 26, avg_ratio: 0.95,
      missing_titles: [], file_count: 26 },
    { username: 'bob', dir: 'B\\Album', filetype: 'mp3',
      matched_tracks: 22, total_tracks: 26, avg_ratio: 0.80,
      missing_titles: ['x'], file_count: 22 },
  ],
});
assert(populatedBlock.includes('alice'), 'first candidate username rendered');
assert(populatedBlock.includes('bob'), 'second candidate username rendered');
assert(populatedBlock.includes('26/26'),
  'matched/total rendered for first row');
assert(populatedBlock.includes('0.95'),
  'avg_ratio rendered to 2 decimals');
assert(populatedBlock.includes('flac'),
  'filetype rendered');

// HTML-escape coverage — adversarial username/dir must not leak markup.
const xssBlock = renderForensicBlock({
  variant: 'default', final_state: 'Completed', outcome: 'no_match',
  top_candidates: [{
    username: '<script>x</script>', dir: '"><img>', filetype: 'flac',
    matched_tracks: 1, total_tracks: 1, avg_ratio: 0,
    missing_titles: [], file_count: 1,
  }],
});
assert(!xssBlock.includes('<script>x</script>'),
  'malicious username escaped');
assert(!xssBlock.includes('"><img>'),
  'malicious dir escaped');

// --- youtubeSectionState tests (U4 four-state classifier) ---
console.log('youtubeSectionState()');
// null / undefined / non-object → never_run (the side-effectful GET has
// not been run; U4 must NOT auto-call it).
assertEqual(youtubeSectionState(null).state, 'never_run', 'null → never_run');
assertEqual(youtubeSectionState(undefined).state, 'never_run', 'undefined → never_run');
// A truthy object with no `outcome` field is a malformed/failed resolve
// (its outcome is not "ok"), NOT never_run — only null/undefined defaults
// to the not-yet-run state.
assertEqual(youtubeSectionState({}).state, 'resolver_failed', 'object with no outcome → resolver_failed');
assertEqual(youtubeSectionState(null).stale, false, 'never_run is never stale');
// ok + releases → resolved_with_matrix.
const ytMatrix = youtubeSectionState({
  outcome: 'ok',
  youtube_releases: [{ yt_browse_id: 'MPREb_x', distances: [] }],
  from_cache: false,
});
assertEqual(ytMatrix.state, 'resolved_with_matrix', 'ok + releases>0 → resolved_with_matrix');
assertEqual(ytMatrix.stale, false, 'fresh matrix not stale');
assertEqual(ytMatrix.message, '', 'fresh matrix needs no message');
// ok + empty releases → resolved_empty.
const ytEmpty = youtubeSectionState({ outcome: 'ok', youtube_releases: [], from_cache: false });
assertEqual(ytEmpty.state, 'resolved_empty', 'ok + releases==0 → resolved_empty');
assert(ytEmpty.message.includes('Not on YouTube Music'), 'resolved_empty surfaces "not on YouTube Music" copy');
// ok + missing youtube_releases key → resolved_empty (no releases).
assertEqual(youtubeSectionState({ outcome: 'ok' }).state, 'resolved_empty', 'ok + no releases key → resolved_empty');
// transient / 503 outcomes → resolver_failed.
assertEqual(youtubeSectionState({ outcome: 'transient' }).state, 'resolver_failed', 'transient → resolver_failed');
assertEqual(youtubeSectionState({ outcome: 'unresolved_timeout' }).state, 'resolver_failed', 'unresolved_timeout → resolver_failed');
assertEqual(youtubeSectionState({ outcome: 'unresolved_mirror_unavailable' }).state, 'resolver_failed', 'mirror unavailable → resolver_failed');
assertEqual(youtubeSectionState({ outcome: 'not_found' }).state, 'resolver_failed', 'not_found → resolver_failed');
const ytFail = youtubeSectionState({ outcome: 'transient', error_message: 'mirror down' });
assert(ytFail.message.includes('mirror down'), 'resolver_failed surfaces the error_message');
// from_cache + error_message → staleness flag on an otherwise-resolved state.
const ytStaleMatrix = youtubeSectionState({
  outcome: 'ok',
  youtube_releases: [{ yt_browse_id: 'MPREb_y', distances: [] }],
  from_cache: true,
  error_message: 'live YT fetch failed; served cache',
});
assertEqual(ytStaleMatrix.state, 'resolved_with_matrix', 'cached matrix still resolves with matrix');
assertEqual(ytStaleMatrix.stale, true, 'from_cache + error_message sets the staleness flag');
assert(ytStaleMatrix.message.includes('stale'), 'stale matrix surfaces a staleness message');
// from_cache WITHOUT error_message is a clean cache hit, not stale.
assertEqual(
  youtubeSectionState({ outcome: 'ok', youtube_releases: [{ yt_browse_id: 'z', distances: [] }], from_cache: true }).stale,
  false,
  'from_cache alone (no error_message) is not stale',
);

// --- consoleEmphasis tests (U4 band-aware emphasis selector) ---
console.log('consoleEmphasis()');
assertEqual(consoleEmphasis({ band: 'missing' }).lead, 'unfindable', 'Missing band leads with unfindable panel');
assertEqual(consoleEmphasis({ band: 'MISSING' }).lead, 'unfindable', 'Missing band is case-insensitive');
assertEqual(consoleEmphasis({ band: '' }).lead, 'unfindable', 'no band (treated Missing-like) leads with unfindable');
assertEqual(consoleEmphasis({}).lead, 'unfindable', 'missing band key leads with unfindable');
assertEqual(consoleEmphasis(null).lead, 'unfindable', 'null row leads with unfindable');
assertEqual(consoleEmphasis({ band: 'poor' }).lead, 'band_vs_intent', 'on-disk band leads with band-vs-intent');
assertEqual(consoleEmphasis({ band: 'transparent' }).lead, 'band_vs_intent', 'on-disk transparent leads with band-vs-intent');
// An on-disk row carrying an unfindable_category still leads with unfindable
// (the operator's first question is "why stuck", even if a copy exists).
assertEqual(
  consoleEmphasis({ band: 'poor', unfindable_category: 'wrong_pressing_available' }).lead,
  'unfindable',
  'on-disk row with an unfindable_category leads with unfindable',
);

// --- parsePastedId tests (search-by-ID) ---
console.log('parsePastedId()');

function assertParse(input, expected, msg) {
  const actual = parsePastedId(input);
  assertEqual(JSON.stringify(actual), JSON.stringify(expected), msg);
}

// Bare IDs (kind unknown — resolver disambiguates server-side)
assertParse(
  'c1f6a2c9-bcba-4e69-96f5-233c85b2830a',
  { family: 'mb', kind: 'unknown', id: 'c1f6a2c9-bcba-4e69-96f5-233c85b2830a' },
  'bare MB UUID lowercase',
);
assertParse(
  'C1F6A2C9-BCBA-4E69-96F5-233C85B2830A',
  { family: 'mb', kind: 'unknown', id: 'c1f6a2c9-bcba-4e69-96f5-233c85b2830a' },
  'bare MB UUID uppercase normalised to lowercase',
);
assertParse(
  '32457180',
  { family: 'discogs', kind: 'unknown', id: '32457180' },
  'bare Discogs digits',
);
assertParse(
  '1',
  { family: 'discogs', kind: 'unknown', id: '1' },
  'single digit accepted (Discogs ID space starts at 1)',
);
assertParse(
  '123456789012',
  { family: 'discogs', kind: 'unknown', id: '123456789012' },
  '12-digit Discogs ID at boundary',
);

// MB URLs — type disambiguated by URL path
assertParse(
  'https://musicbrainz.org/release/c1f6a2c9-bcba-4e69-96f5-233c85b2830a',
  { family: 'mb', kind: 'release', id: 'c1f6a2c9-bcba-4e69-96f5-233c85b2830a' },
  'MB release URL with https',
);
assertParse(
  'http://musicbrainz.org/release/c1f6a2c9-bcba-4e69-96f5-233c85b2830a',
  { family: 'mb', kind: 'release', id: 'c1f6a2c9-bcba-4e69-96f5-233c85b2830a' },
  'MB release URL with http',
);
assertParse(
  'musicbrainz.org/release/c1f6a2c9-bcba-4e69-96f5-233c85b2830a',
  { family: 'mb', kind: 'release', id: 'c1f6a2c9-bcba-4e69-96f5-233c85b2830a' },
  'MB release URL without protocol',
);
assertParse(
  'https://musicbrainz.org/release-group/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
  { family: 'mb', kind: 'release-group', id: 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee' },
  'MB release-group URL',
);
assertParse(
  'https://musicbrainz.org/release/c1f6a2c9-bcba-4e69-96f5-233c85b2830a/',
  { family: 'mb', kind: 'release', id: 'c1f6a2c9-bcba-4e69-96f5-233c85b2830a' },
  'MB URL with trailing slash',
);
assertParse(
  'https://musicbrainz.org/release/c1f6a2c9-bcba-4e69-96f5-233c85b2830a?source=foo',
  { family: 'mb', kind: 'release', id: 'c1f6a2c9-bcba-4e69-96f5-233c85b2830a' },
  'MB URL with querystring',
);
assertParse(
  'https://musicbrainz.org/release/c1f6a2c9-bcba-4e69-96f5-233c85b2830a#discs',
  { family: 'mb', kind: 'release', id: 'c1f6a2c9-bcba-4e69-96f5-233c85b2830a' },
  'MB URL with fragment',
);

// Discogs URLs — type disambiguated by URL path
assertParse(
  'https://www.discogs.com/release/32457180',
  { family: 'discogs', kind: 'release', id: '32457180' },
  'Discogs release URL with www',
);
assertParse(
  'https://discogs.com/release/32457180',
  { family: 'discogs', kind: 'release', id: '32457180' },
  'Discogs release URL without www',
);
assertParse(
  'https://www.discogs.com/release/32457180-Various-Rock-Christmas-The-Very-Best-Of',
  { family: 'discogs', kind: 'release', id: '32457180' },
  'Discogs release URL with slug',
);
assertParse(
  'https://www.discogs.com/master/3673686',
  { family: 'discogs', kind: 'master', id: '3673686' },
  'Discogs master URL',
);
assertParse(
  'https://www.discogs.com/master/3673686-Slug-Words',
  { family: 'discogs', kind: 'master', id: '3673686' },
  'Discogs master URL with slug',
);
assertParse(
  'https://www.discogs.com/release/32457180?utm_source=share',
  { family: 'discogs', kind: 'release', id: '32457180' },
  'Discogs URL with querystring',
);

// Whitespace handling
assertParse(
  '  c1f6a2c9-bcba-4e69-96f5-233c85b2830a  ',
  { family: 'mb', kind: 'unknown', id: 'c1f6a2c9-bcba-4e69-96f5-233c85b2830a' },
  'bare UUID with surrounding whitespace',
);
assertParse(
  '\thttps://www.discogs.com/release/32457180\n',
  { family: 'discogs', kind: 'release', id: '32457180' },
  'URL with tab/newline padding',
);

// Embedded /release/ in slug — first canonical match wins, no false positive
assertParse(
  'https://www.discogs.com/release/32457180-Various-release-of-the-year',
  { family: 'discogs', kind: 'release', id: '32457180' },
  'embedded "release" word in slug does not confuse parser',
);

// Garbage / invalid
assertParse('hello world', null, 'random text rejected');
assertParse('', null, 'empty string rejected');
assertParse('   ', null, 'whitespace-only rejected');
assertParse('abc123', null, 'mixed alphanumeric rejected');
assertParse(
  'c1f6a2c9bcba4e6996f5233c85b2830a',
  null,
  '32-char UUID without dashes rejected',
);
assertParse('1234567890123', null, '13-digit numeric rejected (out of range)');

// Non-canonical hosts (deferred per Scope Boundaries)
assertParse(
  'https://beta.musicbrainz.org/release/c1f6a2c9-bcba-4e69-96f5-233c85b2830a',
  null,
  'beta.musicbrainz.org subdomain rejected (deferred)',
);
assertParse(
  'https://mbid.eu/c1f6a2c9-bcba-4e69-96f5-233c85b2830a',
  null,
  'mbid.eu short URL rejected (deferred)',
);
assertParse(
  'https://www.discogs.com/sell/release/32457180',
  null,
  'Discogs marketplace URL rejected (deferred per Scope Boundaries)',
);

// ============================================================
// replace_picker.js — U8
// ============================================================
import {
  renderPressingsList,
  renderRequestsList,
  renderConfirmDialog,
  renderStandardHeader,
  renderInvertedHeader,
  renderTracklist,
  renderSourcePanel,
  formatLength,
  pickBestDistance,
  formatDistanceBadge,
  runWithConcurrency,
  esc as replaceEsc,
} from '../web/js/replace_picker.js';

assertEqual(
  renderPressingsList([], 'whatever').includes('No pressings'),
  true,
  'renderPressingsList empty → friendly message',
);

const sample = [
  { id: 'aaa', title: 'Pressing A', date: '2020-01-01', country: 'US', track_count: 12, format: 'CD' },
  { id: 'bbb', title: 'Pressing B', date: '2021-05-01', country: 'JP', track_count: 13, format: 'LP' },
];
const pressingsHtml = renderPressingsList(sample, 'aaa');
assert(pressingsHtml.includes('data-expand-mbid="aaa"'),
  'renderPressingsList wires current pressing as expandable row');
assert(/data-expand-mbid="aaa"[^>]*disabled|disabled[^>]*data-expand-mbid="aaa"/.test(pressingsHtml),
  'renderPressingsList marks current pressing disabled');
assert(pressingsHtml.includes('current pressing'), 'renderPressingsList labels current pressing');
assert(pressingsHtml.includes('data-expand-mbid="bbb"'), 'renderPressingsList includes sibling');
assert(!/<button[^>]*data-expand-mbid="bbb"[^>]*disabled/.test(pressingsHtml),
  'renderPressingsList does not disable non-current siblings');
assert(pressingsHtml.includes('data-mbid="bbb"') &&
  /replace-picker-confirm[^>]*data-mbid="bbb"/.test(pressingsHtml),
  'renderPressingsList renders pick-button for non-current pressing');
assert(!/replace-picker-confirm[^>]*data-mbid="aaa"/.test(pressingsHtml),
  'renderPressingsList omits pick-button for current pressing');
assert(!pressingsHtml.includes('aaa</small>') && !pressingsHtml.includes('bbb</small>'),
  'renderPressingsList does not expose MBID to operator (visual-noise cleanup)');
assert(/2020.*US.*CD.*12t/.test(pressingsHtml) || pressingsHtml.includes('US · 2020 · CD · 12t'),
  'renderPressingsList renders meta in country·year·format·Nt order');

assertEqual(
  renderRequestsList([]).includes('No active requests'),
  true,
  'renderRequestsList empty → friendly message',
);
const reqHtml = renderRequestsList([
  { id: 42, mb_release_id: 'old-uuid', status: 'wanted', artist_name: 'Pet Grief', album_title: 'X' },
]);
assert(reqHtml.includes('data-rid="42"'), 'renderRequestsList carries id (pick button)');
assert(reqHtml.includes('Pet Grief'), 'renderRequestsList includes artist');
assert(reqHtml.includes('data-expand-mbid="old-uuid"'),
  'renderRequestsList row expands by MBID');
assert(reqHtml.includes('data-tracks-for="old-uuid"'),
  'renderRequestsList renders lazy tracklist container per row');
assert(!/<small[^>]*>[^<]*old-uuid/.test(reqHtml),
  'renderRequestsList hides the MBID from the operator');

const dlg = renderConfirmDialog({
  sourceRequestId: 4194,
  targetMbid: '18056805-33f5-3e99-aa4b-5f5919c4f8af',
  targetLabel: 'Pet Grief — New Pressing',
});
assert(dlg.includes('Replace request #4194'), 'confirm dialog includes source id');
assert(dlg.includes('18056805-33f5-3e99-aa4b-5f5919c4f8af'), 'confirm dialog includes target mbid');
assert(dlg.includes('issue #278'), 'confirm dialog mentions orphan transfer issue #278');
assert(dlg.includes('replace-picker-cancel'), 'confirm dialog has cancel button id');
assert(dlg.includes('replace-picker-confirm'), 'confirm dialog has confirm button id');
assert(dlg.includes('frozen for audit'), 'confirm dialog explains supersede semantics');

assert(renderStandardHeader('Pet Grief — Old').includes('Switch'),
  'renderStandardHeader carries "Switch" verb');
assert(renderInvertedHeader('Pet Grief — New').includes('replace an existing request'),
  'renderInvertedHeader carries inverted-mode verb');

// Tracklist container is rendered hidden until row.open
assert(pressingsHtml.includes('data-tracks-for="aaa"'),
  'renderPressingsList renders tracklist container per row');

// formatLength
assertEqual(formatLength(0), '0:00', 'formatLength 0 → 0:00');
assertEqual(formatLength(7), '0:07', 'formatLength <60s pads seconds');
assertEqual(formatLength(63), '1:03', 'formatLength 63s → 1:03');
assertEqual(formatLength(263.4), '4:23', 'formatLength rounds');
assertEqual(formatLength(null), '', 'formatLength null → empty');
assertEqual(formatLength(undefined), '', 'formatLength undefined → empty');
assertEqual(formatLength(NaN), '', 'formatLength NaN → empty');

// renderTracklist
assert(renderTracklist([]).includes('No tracks'),
  'renderTracklist empty → friendly message');
const tlHtml = renderTracklist([
  { disc_number: 1, track_number: 1, title: 'Aaa', length_seconds: 200 },
  { disc_number: 1, track_number: 2, title: 'Bbb <em>', length_seconds: 263.4 },
]);
assert(tlHtml.includes('Aaa'), 'renderTracklist includes title');
assert(tlHtml.includes('4:23'), 'renderTracklist formats track length');
assert(tlHtml.includes('&lt;em&gt;'), 'renderTracklist escapes titles');
assert(!/Disc 1/.test(tlHtml), 'renderTracklist hides disc header for single-disc');

const multiDiscHtml = renderTracklist([
  { disc_number: 1, track_number: 1, title: 'A', length_seconds: 60 },
  { disc_number: 2, track_number: 1, title: 'B', length_seconds: 60 },
]);
assert(multiDiscHtml.includes('Disc 1') && multiDiscHtml.includes('Disc 2'),
  'renderTracklist shows disc headers for multi-disc');

// renderSourcePanel
const loadingPanel = renderSourcePanel({
  label: 'Pet Grief — Old',
  meta: 'US · 2020 · CD · 12t',
  tracks: null,
  loading: true,
});
assert(loadingPanel.includes('Current request:'),
  'renderSourcePanel labels the panel "Current request:"');
assert(loadingPanel.includes('Pet Grief — Old'), 'renderSourcePanel includes the label');
assert(loadingPanel.includes('US · 2020 · CD · 12t'),
  'renderSourcePanel renders meta line on summary');
assert(loadingPanel.includes('Loading'), 'renderSourcePanel loading state shows placeholder');
assert(loadingPanel.includes('replace-picker-source-body'),
  'renderSourcePanel exposes the body container for lazy fill');

const loadedPanel = renderSourcePanel({
  label: 'X',
  tracks: [{ disc_number: 1, track_number: 1, title: 'Z', length_seconds: 120 }],
});
assert(loadedPanel.includes('Z'), 'renderSourcePanel renders tracks when loaded');
assert(loadedPanel.includes('2:00'), 'renderSourcePanel renders track duration');

const errorPanel = renderSourcePanel({ label: 'X', tracks: null, error: 'HTTP 500' });
assert(errorPanel.includes('HTTP 500'), 'renderSourcePanel renders error message');

// pickBestDistance — picks the lowest-distance ok result; null when none scored
assertEqual(pickBestDistance([]), null, 'pickBestDistance [] → null');
assertEqual(
  pickBestDistance([
    { outcome: 'fetch_failed' },
    { outcome: 'wrong_release_group' },
  ]),
  null,
  'pickBestDistance all-errors → null',
);
const best = pickBestDistance([
  { outcome: 'ok', distance: 0.21, matched_tracks: 10, total_mb_tracks: 12 },
  { outcome: 'ok', distance: 0.07, matched_tracks: 12, total_mb_tracks: 12 },
  { outcome: 'no_audio' },
]);
assertEqual(best?.distance, 0.07, 'pickBestDistance picks lowest');
assertEqual(best?.matched_tracks, 12, 'pickBestDistance carries metadata');

// formatDistanceBadge — empty for null, formatted otherwise
assertEqual(formatDistanceBadge(null), '', 'formatDistanceBadge null → empty');
assertEqual(
  formatDistanceBadge({ outcome: 'ok', distance: 0.0712,
                         matched_tracks: 12, total_mb_tracks: 12 }),
  'best 0.07 (12/12)',
  'formatDistanceBadge ok result',
);
assertEqual(
  formatDistanceBadge({ outcome: 'ok', distance: 0.42,
                         matched_tracks: 8, total_mb_tracks: 12 }),
  'best 0.42 (8/12)',
  'formatDistanceBadge partial match',
);
assertEqual(
  formatDistanceBadge({ outcome: 'ok', distance: 0.0 }),
  'best 0.00',
  'formatDistanceBadge no track-count metadata → distance only',
);

// runWithConcurrency — caps in-flight workers; preserves input order
{
  const order = [];
  let inFlight = 0;
  let peak = 0;
  const items = [1, 2, 3, 4, 5, 6, 7, 8];
  const results = await runWithConcurrency(items, 3, async (item) => {
    inFlight++; peak = Math.max(peak, inFlight);
    await new Promise((r) => setTimeout(r, 5 + Math.random() * 5));
    inFlight--;
    order.push(item);
    return item * 10;
  });
  assertEqual(results.length, items.length,
    'runWithConcurrency preserves count');
  for (let i = 0; i < items.length; i++) {
    assertEqual(results[i], items[i] * 10,
      `runWithConcurrency preserves index ${i}`);
  }
  assert(peak <= 3,
    `runWithConcurrency caps in-flight workers (peak=${peak})`);
}
{
  // limit larger than item count — still completes correctly
  const results = await runWithConcurrency([1, 2], 99, async (n) => n + 100);
  assertEqual(results[0], 101, 'runWithConcurrency oversize limit ok [0]');
  assertEqual(results[1], 102, 'runWithConcurrency oversize limit ok [1]');
}
{
  // empty input — resolves immediately
  const results = await runWithConcurrency([], 4, async () => 'never-called');
  assertEqual(results.length, 0, 'runWithConcurrency [] → []');
}

assertEqual(replaceEsc('<script>'), '&lt;script&gt;', 'esc escapes <');
assertEqual(replaceEsc('a&b'), 'a&amp;b', 'esc escapes &');
assertEqual(replaceEsc('"x"'), '&quot;x&quot;', 'esc escapes "');

// renderReplaceButton (release_actions.js) — U9
import { renderReplaceButton } from '../web/js/release_actions.js';

const stdBtn = renderReplaceButton({
  mode: 'standard',
  sourceRequestId: 4194,
  releaseGroupId: 'rg-1',
  sourceLabel: 'Pet Grief — Old',
}, { stopPropagation: true });
assert(stdBtn.includes('window.openReplacePicker'),
  'renderReplaceButton standard wires through window.openReplacePicker');
assert(stdBtn.includes('sourceRequestId: 4194'),
  'renderReplaceButton standard carries sourceRequestId');
assert(stdBtn.includes('releaseGroupId'),
  'renderReplaceButton standard carries releaseGroupId');

const invEnabled = renderReplaceButton({
  mode: 'inverted',
  targetMbid: 'new-mbid',
  releaseGroupId: 'rg-1',
  targetLabel: 'Pet Grief — New',
}, { enabled: true });
assert(invEnabled.includes('targetMbid'),
  'renderReplaceButton inverted enabled wires targetMbid');
assert(!invEnabled.includes('disabled'),
  'renderReplaceButton inverted enabled is not disabled');

const invDisabled = renderReplaceButton({
  mode: 'inverted',
  targetMbid: 'new-mbid',
  releaseGroupId: 'rg-1',
  targetLabel: 'Pet Grief — New',
}, { enabled: false });
assert(invDisabled.includes('disabled'),
  'renderReplaceButton inverted disabled carries disabled attr');
assert(!invDisabled.includes('window.openReplacePicker'),
  'renderReplaceButton inverted disabled does not wire onclick');

// Null-RG handling: legacy rows have releaseGroupId=null. The picker
// lazy-resolves. The button must still render, with an explicit JS
// ``null`` literal in the onclick payload so the picker can detect the
// missing RG.
const stdNullRg = renderReplaceButton({
  mode: 'standard',
  sourceRequestId: 4194,
  releaseGroupId: null,
  sourceLabel: 'Pet Grief — Old',
}, { stopPropagation: true });
assert(stdNullRg.includes('window.openReplacePicker'),
  'renderReplaceButton standard renders with null releaseGroupId');
assert(stdNullRg.includes('releaseGroupId: null'),
  'renderReplaceButton standard encodes null RG as JS null literal');

const invNullRg = renderReplaceButton({
  mode: 'inverted',
  targetMbid: 'new-mbid',
  releaseGroupId: null,
  targetLabel: 'Pet Grief — New',
}, { enabled: true });
assert(invNullRg.includes('window.openReplacePicker'),
  'renderReplaceButton inverted renders with null releaseGroupId');
assert(invNullRg.includes('releaseGroupId: null'),
  'renderReplaceButton inverted encodes null RG as JS null literal');

// Standard mode without sourceRequestId still returns empty.
const stdNoSource = renderReplaceButton({
  mode: 'standard',
  releaseGroupId: 'rg-1',
});
assertEqual(stdNoSource, '',
  'renderReplaceButton standard returns empty without sourceRequestId');

// Active-RG Set lookup — U9 enable logic
const activeRgSet = new Set(['rg-1', 'rg-2']);
assertEqual(activeRgSet.has('rg-1'), true, 'active-RG Set hit');
assertEqual(activeRgSet.has('rg-not-active'), false, 'active-RG Set miss');

// --- long_tail.js pure helpers (U3) ---
console.log('long_tail.js __test__');
{
  const {
    bandLabel,
    deriveBandTabs,
    defaultBand,
    filterRows,
    countOtherBandMatches,
    renderLongTailRow,
  } = longTailTest;

  // A mixed cohort spanning Missing + several on-disk bands, deliberately
  // out of canonical order so the ordering assertion is meaningful.
  const cohort = [
    { id: 1, artist_name: 'Mount Eerie', album_title: 'Clear Moon', band: 'transparent' },
    { id: 2, artist_name: 'The Mountain Goats', album_title: 'Tallahassee', band: 'missing' },
    { id: 3, artist_name: 'Bill Callahan', album_title: 'Apocalypse', band: 'poor' },
    { id: 4, artist_name: 'Smog', album_title: 'Knock Knock', band: 'missing' },
    { id: 5, artist_name: 'Grouper', album_title: 'Dragging a Dead Deer', band: 'unknown' },
    { id: 6, artist_name: 'Tim Hecker', album_title: 'Ravedeath, 1972', band: 'transparent' },
    { id: 7, artist_name: 'Loscil', album_title: 'Submers', band: 'lossless' },
  ];

  // --- bandLabel ---
  assertEqual(bandLabel('missing'), 'Missing', 'bandLabel capitalises missing');
  assertEqual(bandLabel('transparent'), 'Transparent', 'bandLabel capitalises transparent');
  assertEqual(bandLabel(''), '?', 'bandLabel empty -> ?');
  assertEqual(bandLabel(null), '?', 'bandLabel null -> ?');
  assertEqual(bandLabel('LOSSLESS'), 'Lossless', 'bandLabel lower-cases then capitalises');

  // --- deriveBandTabs: ordering Missing-first + ascending QualityRank ---
  const tabs = deriveBandTabs(cohort);
  assertEqual(
    tabs.map((t) => t.band).join(','),
    'missing,unknown,poor,transparent,lossless',
    'deriveBandTabs orders Missing first, then ascending by rank (only present bands)',
  );
  // Counts are correct per band.
  const countOf = (b) => (tabs.find((t) => t.band === b) || {}).count;
  assertEqual(countOf('missing'), 2, 'deriveBandTabs counts Missing rows');
  assertEqual(countOf('transparent'), 2, 'deriveBandTabs counts Transparent rows');
  assertEqual(countOf('poor'), 1, 'deriveBandTabs counts Poor rows');
  assertEqual(countOf('unknown'), 1, 'deriveBandTabs counts Unknown rows');
  assertEqual(countOf('lossless'), 1, 'deriveBandTabs counts Lossless rows');
  // Bands not present in the cohort produce no tab.
  assert(!tabs.some((t) => t.band === 'good'), 'deriveBandTabs omits absent bands');
  // Each tab carries a display label.
  assertEqual((tabs[0]).label, 'Missing', 'deriveBandTabs first tab label is Missing');
  // Empty cohort -> no tabs.
  assertEqual(deriveBandTabs([]).length, 0, 'deriveBandTabs empty cohort -> no tabs');
  // Unrecognised band sorts to the end, not dropped.
  const withWeird = deriveBandTabs([
    { band: 'missing' }, { band: 'sparkle' }, { band: 'good' },
  ]);
  assertEqual(
    withWeird.map((t) => t.band).join(','),
    'missing,good,sparkle',
    'deriveBandTabs sorts unrecognised band to the end',
  );

  // --- defaultBand ---
  assertEqual(defaultBand(tabs), 'missing', 'defaultBand prefers Missing when present');
  assertEqual(
    defaultBand(deriveBandTabs([{ band: 'good' }, { band: 'poor' }])),
    'poor',
    'defaultBand falls back to first canonical band when Missing absent',
  );
  assertEqual(defaultBand([]), null, 'defaultBand empty -> null');

  // --- filterRows: within-band substring match ---
  const missingRows = filterRows(cohort, 'missing', '');
  assertEqual(missingRows.length, 2, 'filterRows missing band, no query -> 2 rows');
  assert(
    missingRows.every((r) => r.band === 'missing'),
    'filterRows only returns rows of the selected band',
  );
  // Substring matches artist.
  const goatHits = filterRows(cohort, 'missing', 'mountain');
  assertEqual(goatHits.length, 1, 'filterRows substring matches artist within band');
  assertEqual(goatHits[0].id, 2, 'filterRows artist-substring hit is the right row');
  // Substring matches album, case-insensitively.
  const knockHits = filterRows(cohort, 'missing', 'KNOCK');
  assertEqual(knockHits.length, 1, 'filterRows substring matches album (case-insensitive)');
  assertEqual(knockHits[0].id, 4, 'filterRows album-substring hit is the right row');
  // A query that only matches rows in OTHER bands -> empty in-band result.
  assertEqual(
    filterRows(cohort, 'missing', 'hecker').length,
    0,
    'filterRows cross-band query -> empty for the selected band',
  );
  // Null band -> no rows (no tab selected).
  assertEqual(filterRows(cohort, null, '').length, 0, 'filterRows null band -> no rows');

  // --- countOtherBandMatches: cross-band hint count ---
  assertEqual(
    countOtherBandMatches(cohort, 'missing', 'hecker'),
    1,
    'countOtherBandMatches counts matches in other bands',
  );
  assertEqual(
    countOtherBandMatches(cohort, 'missing', 'eerie'),
    1,
    'countOtherBandMatches: Mount Eerie (transparent) matches "eerie" outside Missing',
  );
  // Selected-band matches are excluded from the cross-band count.
  assertEqual(
    countOtherBandMatches(cohort, 'missing', 'goats'),
    0,
    'countOtherBandMatches excludes the selected band',
  );
  // Blank query -> 0 (no hint while not searching).
  assertEqual(
    countOtherBandMatches(cohort, 'missing', ''),
    0,
    'countOtherBandMatches blank query -> 0',
  );

  // --- renderLongTailRow: sanity (clickable + detail container) ---
  const rowHtml = renderLongTailRow(cohort[1]);
  assert(
    rowHtml.includes('window.toggleLongTailDetail(2)'),
    'renderLongTailRow wires the row click to toggleLongTailDetail',
  );
  assert(
    rowHtml.includes('id="lt-detail-2"'),
    'renderLongTailRow emits the per-row detail container',
  );
  assert(
    rowHtml.includes('badge-wanted') && rowHtml.includes('Missing'),
    'renderLongTailRow renders a Missing band chip for a missing row',
  );
  // An on-disk-band row gets the rank colour class + capitalised label.
  const transparentRow = renderLongTailRow(cohort[0]);
  assert(
    transparentRow.includes('badge-rank-transparent') && transparentRow.includes('Transparent'),
    'renderLongTailRow renders a rank-coloured chip for an on-disk band',
  );

  // --- renderLongTailBody: the three list states (DOM-free string paint) ---
  const { renderLongTailBody } = longTailTest;
  // Empty cohort -> empty-cohort affordance, never blank.
  state.longTail = { rows: [], band: null, query: '' };
  const emptyCohort = renderLongTailBody();
  assert(
    emptyCohort.includes('No wanted releases in the long tail'),
    'renderLongTailBody empty cohort shows the empty-cohort affordance',
  );
  // Populated cohort -> tab strip + rows for the default (Missing) band.
  state.longTail = { rows: cohort, band: null, query: '' };
  const populated = renderLongTailBody();
  assert(
    populated.includes('lt-band-tabs') && populated.includes('lt-search-input'),
    'renderLongTailBody renders band tabs + search box for a populated cohort',
  );
  assertEqual(
    state.longTail.band, 'missing',
    'renderLongTailBody defaults the selected band to Missing',
  );
  // Empty-band -> a search filters the selected band to zero; the
  // affordance + cross-band hint show, never a blank area.
  state.longTail = { rows: cohort, band: 'missing', query: 'hecker' };
  const emptyBand = renderLongTailBody();
  assert(
    emptyBand.includes('No Missing releases match'),
    'renderLongTailBody empty-band shows the per-band no-match affordance',
  );
  assert(
    emptyBand.includes('1 match in other bands'),
    'renderLongTailBody empty-band surfaces the cross-band match hint',
  );
  // Reset shared state so later tests are not affected.
  state.longTail = { rows: null, band: null, query: '' };
}

// --- long_tail.js action console pure helpers (U4) ---
console.log('long_tail.js __test__ (U4 console)');
{
  const {
    renderUnfindableBody,
    renderPeersBody,
    renderRescuesBody,
    renderSiblingsBody,
    renderYoutubeBody,
    renderConsoleShell,
    renderPanelError,
    youtubeHistoryRows,
    youtubeFailureReason,
    PEERS_VISIBLE_CAP,
  } = longTailTest;

  // --- renderUnfindableBody: categorised vs not-yet-categorised ---
  // Categorised → category badge + forensics rollup.
  const categorised = renderUnfindableBody({
    unfindable: { category: 'wrong_pressing_available', categorised_at: '2026-05-20T00:00:00Z',
      last_artist_probe_match_count: 3, last_artist_probe_at: '2026-05-21T00:00:00Z' },
    search_forensics: { total_searches: 40, with_cands_count: 12, zero_results_count: 5,
      dominant_rejection_reason: 'strict_count', last_search_at: '2026-05-22T00:00:00Z' },
  });
  assert(categorised.includes('wrong_pressing_available'),
    'renderUnfindableBody renders the category for a categorised request');
  assert(categorised.includes('40 searches') && categorised.includes('dominant reject: strict_count'),
    'renderUnfindableBody renders the search-forensics rollup');
  assert(categorised.includes('artist probe: 3 matches'),
    'renderUnfindableBody renders the artist-probe rollup');
  // Not-yet-categorised (unfindable == null) → daily-detection state, NOT
  // an error, NOT blank (R7).
  const uncategorised = renderUnfindableBody({
    unfindable: null,
    search_forensics: { total_searches: 2, with_cands_count: 0, zero_results_count: 2 },
  });
  assert(uncategorised.includes('not yet categorised') && uncategorised.includes('detection runs daily'),
    'renderUnfindableBody renders the not-yet-categorised daily-detection state');
  assert(!uncategorised.toLowerCase().includes("couldn't load"),
    'not-yet-categorised is distinct from an error affordance');
  // category explicitly absent on the unfindable struct also → uncategorised.
  const catNull = renderUnfindableBody({ unfindable: { category: null }, search_forensics: {} });
  assert(catNull.includes('not yet categorised'),
    'renderUnfindableBody treats a null category as not-yet-categorised');

  // --- youtubeHistoryRows: only source==="youtube" rows ---
  // Production-shaped: a youtube_failed row carries its reason in the
  // youtube_metadata JSONB blob (per YoutubeIngestMetadata.reason), NOT in
  // a top-level field.
  const mixedHistory = [
    { source: 'youtube', outcome: 'youtube_failed', created_at: '2026-05-25T00:00:00Z',
      youtube_metadata: { reason: 'track_count_mismatch' } },
    { source: 'request', outcome: 'rejected' },
    { source: 'youtube', outcome: 'youtube_running', created_at: '2026-05-26T00:00:00Z' },
  ];
  assertEqual(youtubeHistoryRows(mixedHistory).length, 2,
    'youtubeHistoryRows keeps only source==="youtube" rows');
  assertEqual(youtubeHistoryRows([]).length, 0, 'youtubeHistoryRows empty → []');
  assertEqual(youtubeHistoryRows(null).length, 0, 'youtubeHistoryRows null → []');

  // --- youtubeFailureReason: reads the production-shaped reason field ---
  assertEqual(
    youtubeFailureReason({ youtube_metadata: { reason: 'track_count_mismatch' } }),
    'track_count_mismatch',
    'youtubeFailureReason reads youtube_metadata.reason (the production field)');
  assertEqual(
    youtubeFailureReason({ error_message: 'yt-dlp died' }),
    'yt-dlp died',
    'youtubeFailureReason falls back to error_message');
  assertEqual(
    youtubeFailureReason({ verdict: 'rejected' }),
    'rejected',
    'youtubeFailureReason falls back to verdict');
  assertEqual(
    youtubeFailureReason({}),
    'unknown',
    'youtubeFailureReason → "unknown" when no reason field present');

  // --- renderRescuesBody: running / failed / success / none ---
  // Active youtube_running row → "rescue running".
  const running = renderRescuesBody(
    [{ source: 'youtube', outcome: 'youtube_running', created_at: '2026-05-26T00:00:00Z' }], false);
  assert(running.includes('rescue running'), 'renderRescuesBody shows "rescue running" for an active youtube_running row');
  // in_flight flag alone (no history) → "rescue running" (KTD4 same predicate).
  assert(renderRescuesBody([], true).includes('rescue running'),
    'renderRescuesBody honours the in_flight_rescue flag with no history');
  // Latest terminal youtube_failed → "last rescue failed: <reason>".
  // Reason comes from the production-shaped youtube_metadata.reason blob.
  const failed = renderRescuesBody(
    [{ source: 'youtube', outcome: 'youtube_failed', created_at: '2026-05-25T00:00:00Z',
       youtube_metadata: { reason: 'track_count_mismatch' } }], false);
  assert(failed.includes('last rescue failed') && failed.includes('track_count_mismatch'),
    'renderRescuesBody shows the failure reason (from youtube_metadata) for a terminal youtube_failed row');
  // A terminal youtube_success is NOT a failure (distinct from youtube_failed).
  const succeeded = renderRescuesBody(
    [{ source: 'youtube', outcome: 'youtube_success', created_at: '2026-05-24T00:00:00Z' }], false);
  assert(!succeeded.includes('last rescue failed'),
    'renderRescuesBody does NOT render a failure for a youtube_success row');
  assert(succeeded.includes('youtube_success'),
    'renderRescuesBody lists a youtube_success attempt');
  // No youtube rows at all → "no rescue attempts".
  assert(renderRescuesBody([{ source: 'request', outcome: 'success' }], false).includes('No rescue attempts'),
    'renderRescuesBody shows "no rescue attempts" when there are no youtube rows');

  // --- renderPeersBody: cap + show-all toggle ---
  const fewPeers = {
    variant: 'v1', final_state: 'Completed', outcome: 'no_match',
    top_candidates: [
      { username: 'a', dir: 'x', filetype: 'flac', matched_tracks: 1, total_tracks: 1, avg_ratio: 1, missing_titles: [], file_count: 1 },
    ],
  };
  const fewHtml = renderPeersBody(fewPeers, 7);
  assert(fewHtml.includes('p-forensic') && !fewHtml.includes('show all'),
    'renderPeersBody under the cap renders the plain forensic block (no show-all)');
  const manyCands = [];
  for (let i = 0; i < PEERS_VISIBLE_CAP + 4; i++) {
    manyCands.push({ username: `u${i}`, dir: `d${i}`, filetype: 'flac',
      matched_tracks: 1, total_tracks: 1, avg_ratio: 1, missing_titles: [], file_count: 1 });
  }
  const manyHtml = renderPeersBody(
    { variant: 'v1', final_state: 'Completed', outcome: 'no_match', top_candidates: manyCands }, 7);
  assert(manyHtml.includes(`show all ${manyCands.length} peers`),
    'renderPeersBody over the cap offers a show-all toggle with the full count');
  assert(manyHtml.includes('window.toggleLongTailPeers(7)'),
    'renderPeersBody wires the show-all toggle to toggleLongTailPeers with the row id');
  assert(manyHtml.includes('lt-peers-full'),
    'renderPeersBody pre-renders the full block for the toggle');
  // Null last_search → forensic block "no data yet" (not a crash).
  assert(renderPeersBody(null, 7).includes('No search forensic data yet'),
    'renderPeersBody null last_search → forensic "no data yet"');

  // --- renderSiblingsBody: rows + empty ---
  const siblings = renderSiblingsBody({ releases: [
    { id: 'r1', title: 'Pressing A', date: '2008-01-01', country: 'US', track_count: 14, format: 'CD',
      in_library: true, library_rank: 'transparent', pipeline_status: null },
    { id: 'r2', title: 'Pressing B', date: '2000-01-01', country: 'GB', track_count: 10, format: 'CD',
      in_library: false, pipeline_status: 'wanted' },
  ] });
  assert(siblings.includes('Pressing A') && siblings.includes('Pressing B'),
    'renderSiblingsBody renders each sibling pressing');
  assert(siblings.includes('in library') && siblings.includes('badge-rank-transparent'),
    'renderSiblingsBody renders the in-library badge with the rank colour');
  assert(siblings.includes('badge-wanted') || siblings.includes('wanted'),
    'renderSiblingsBody renders the pipeline status for a sibling already requested');
  assert(renderSiblingsBody({ releases: [] }).includes('No sibling pressings'),
    'renderSiblingsBody empty → "no sibling pressings"');
  assert(renderSiblingsBody(null).includes('No sibling pressings'),
    'renderSiblingsBody null → "no sibling pressings"');

  // --- renderYoutubeBody: four states (display-only matrix in U4) ---
  // never_run (null result) → Check YouTube button, no matrix.
  const ytNever = renderYoutubeBody(null, 9);
  assert(ytNever.includes('Check YouTube') && ytNever.includes('window.checkYoutube(9)'),
    'renderYoutubeBody never_run renders the Check-YouTube stub wired to window.checkYoutube');
  assert(!ytNever.includes('lt-yt-row'),
    'renderYoutubeBody never_run renders no matrix rows (no auto-resolve)');
  // resolved_with_matrix → display-only matrix rows.
  const ytMatrixHtml = renderYoutubeBody({
    outcome: 'ok', from_cache: false, youtube_releases: [
      { yt_browse_id: 'MPREb_z', year: 2008, track_count: 14, tracks: [],
        distances: [{ mbid: 'm', outcome: 'ok', distance: 0.07 }, { mbid: 'n', outcome: 'no_audio' }] },
    ],
  }, 9);
  assert(ytMatrixHtml.includes('MPREb_z') && ytMatrixHtml.includes('lt-yt-row'),
    'renderYoutubeBody resolved_with_matrix renders the display-only matrix rows');
  assert(ytMatrixHtml.includes('dist 0.070'),
    'renderYoutubeBody matrix surfaces the best ok distance');
  // resolved_empty → "not on YouTube Music".
  const ytEmptyHtml = renderYoutubeBody({ outcome: 'ok', youtube_releases: [] }, 9);
  assert(ytEmptyHtml.includes('Not on YouTube Music'),
    'renderYoutubeBody resolved_empty renders the "not on YouTube Music" copy');
  // resolver_failed → error message + retry affordance. The retry button
  // is relabelled "Retry" in U5 (still wired to window.checkYoutube), so
  // assert on the retry verb + the wired handler rather than the original
  // "Check YouTube" label.
  const ytFailedHtml = renderYoutubeBody({ outcome: 'transient', error_message: 'mirror down' }, 9);
  assert(ytFailedHtml.includes('mirror down') && ytFailedHtml.includes('Retry')
    && ytFailedHtml.includes('window.checkYoutube(9)'),
    'renderYoutubeBody resolver_failed renders the error + retry affordance');
  // staleness flag on a cached matrix.
  const ytStaleHtml = renderYoutubeBody({
    outcome: 'ok', from_cache: true, error_message: 'live fetch failed',
    youtube_releases: [{ yt_browse_id: 'b', track_count: 1, tracks: [], distances: [] }],
  }, 9);
  assert(ytStaleHtml.includes('lt-yt-stale'),
    'renderYoutubeBody surfaces a staleness flag on a cached-but-stale matrix');

  // --- renderConsoleShell: band-aware emphasis + panel containers ---
  // Missing row → why-unfindable leads; the per-panel containers exist.
  const missingShell = renderConsoleShell({ id: 11, band: 'missing', source: 'mb', target_format: 'lossless' });
  assert(missingShell.includes('id="lt-panel-unfindable-11"'),
    'renderConsoleShell emits the why-unfindable panel container');
  assert(missingShell.includes('id="lt-panel-peers-11"')
    && missingShell.includes('id="lt-panel-rescues-11"')
    && missingShell.includes('id="lt-panel-siblings-11"')
    && missingShell.includes('id="lt-panel-youtube-11"'),
    'renderConsoleShell emits all five evidence-panel containers');
  // Lead emphasis: the unfindable panel carries the lead class for a Missing row.
  assert(/lt-panel-unfindable[^"]*lt-panel-lead|lt-panel-lead[^"]*lt-panel-unfindable/.test(missingShell.replace(/\n/g, ' '))
    || missingShell.includes('lt-panel lt-panel-unfindable lt-panel-lead'),
    'renderConsoleShell makes why-unfindable the lead panel for a Missing row');
  // The YouTube panel opens in never_run (no auto-resolve) — Check button present.
  assert(missingShell.includes('window.checkYoutube(11)'),
    'renderConsoleShell opens the YouTube panel in never_run (Check-YouTube stub, no auto-resolve)');
  // On-disk row → band-vs-intent leads (R8), why-unfindable does not.
  const onDiskShell = renderConsoleShell({ id: 12, band: 'poor', source: 'mb', target_format: 'lossless' });
  assert(onDiskShell.includes('Quality vs intent') && onDiskShell.includes('lt-band-intent'),
    'renderConsoleShell leads an on-disk row with the band-vs-intent header');
  assert(onDiskShell.indexOf('lt-band-intent') < onDiskShell.indexOf('lt-panel-unfindable-12'),
    'renderConsoleShell orders band-vs-intent BEFORE why-unfindable for an on-disk row');

  // --- partial-failure render: one panel error, others still render ---
  // Simulate the loaders' per-panel catch: the siblings panel gets the
  // error affordance while the other panels render their content. The pure
  // pieces guarantee a failing panel never blanks or drops its siblings.
  const errBody = renderPanelError('sibling pressings');
  assert(errBody.includes("Couldn't load sibling pressings") && errBody.includes('other panels are unaffected'),
    'renderPanelError renders the isolated per-panel error affordance');
  // Compose the shell + a per-panel error swap + a sibling content render,
  // proving the three coexist (the independent-load contract).
  const composed = renderConsoleShell({ id: 13, band: 'missing' })
    + renderUnfindableBody({ unfindable: { category: 'artist_absent' }, search_forensics: {} })
    + renderPanelError('sibling pressings');
  assert(composed.includes('artist_absent') && composed.includes("Couldn't load sibling pressings"),
    'a panel error and other panels\' content coexist (independent-load contract)');
}

// --- long_tail.js U5 rescue flow pure helpers ---
console.log('long_tail.js __test__ (U5 rescue flow)');
{
  const {
    youtubeBestDistance,
    youtubeRescueTargets,
    rescueOutcomeCopy,
    canStartInFlight,
    renderYoutubeBody,
    renderRescueConfirm,
  } = longTailTest;

  // --- youtubeBestDistance: lowest ok distance, ignores non-ok rows ---
  assertEqual(
    youtubeBestDistance({ distances: [
      { mbid: 'a', outcome: 'ok', distance: 0.21 },
      { mbid: 'b', outcome: 'ok', distance: 0.07 },
      { mbid: 'c', outcome: 'no_audio' },
    ] }),
    0.07,
    'youtubeBestDistance picks the lowest ok distance');
  assertEqual(
    youtubeBestDistance({ distances: [{ mbid: 'a', outcome: 'no_audio' }] }),
    null,
    'youtubeBestDistance → null when no ok row scored');
  assertEqual(youtubeBestDistance({}), null, 'youtubeBestDistance no distances → null');
  assertEqual(youtubeBestDistance({ distances: null }), null, 'youtubeBestDistance null distances → null');

  // --- youtubeRescueTargets: each target carries its browse id + meta ---
  const resolverOk = {
    outcome: 'ok', from_cache: false, youtube_releases: [
      { yt_browse_id: 'MPREb_one', year: 2008, track_count: 14, tracks: [],
        distances: [{ mbid: 'm', outcome: 'ok', distance: 0.07 }, { mbid: 'n', outcome: 'no_audio' }] },
      { yt_browse_id: 'MPREb_two', year: 2000, track_count: 10, tracks: [],
        distances: [{ mbid: 'p', outcome: 'ok', distance: 0.19 }] },
    ],
  };
  const targets = youtubeRescueTargets(resolverOk);
  assertEqual(targets.length, 2, 'youtubeRescueTargets yields one target per release');
  assertEqual(targets[0].yt_browse_id, 'MPREb_one', 'target 0 carries its browse id');
  assertEqual(targets[1].yt_browse_id, 'MPREb_two', 'target 1 carries its browse id');
  assertEqual(targets[0].year, 2008, 'target carries year');
  assertEqual(targets[0].track_count, 14, 'target carries track_count');
  assertEqual(targets[0].best_distance, 0.07, 'target carries best ok distance');
  assertEqual(targets[1].best_distance, 0.19, 'second target best distance');
  // A release missing a browse id is NOT a pickable target (the submit
  // needs the id).
  const targetsWithBad = youtubeRescueTargets({
    outcome: 'ok', youtube_releases: [
      { yt_browse_id: '', year: 1999, track_count: 8, distances: [] },
      { yt_browse_id: 'MPREb_keep', year: 2001, track_count: 9, distances: [] },
    ],
  });
  assertEqual(targetsWithBad.length, 1, 'youtubeRescueTargets drops a release with no browse id');
  assertEqual(targetsWithBad[0].yt_browse_id, 'MPREb_keep', 'kept the release that has a browse id');

  // resolved_empty → NO rescue targets (rescue affordance hidden).
  assertEqual(
    youtubeRescueTargets({ outcome: 'ok', youtube_releases: [] }).length,
    0,
    'youtubeRescueTargets: resolved_empty yields no rescue targets');
  // resolver_failed → no targets.
  assertEqual(
    youtubeRescueTargets({ outcome: 'transient', error_message: 'down' }).length,
    0,
    'youtubeRescueTargets: resolver_failed yields no targets');
  // never_run (null) → no targets.
  assertEqual(youtubeRescueTargets(null).length, 0, 'youtubeRescueTargets: null yields no targets');

  // --- renderYoutubeBody: matrix rows are pickable rescue targets (U5) ---
  const matrixHtml = renderYoutubeBody(resolverOk, 9);
  assert(matrixHtml.includes('window.pickYoutubeRescue(9, ')
    && matrixHtml.includes('MPREb_one') && matrixHtml.includes('MPREb_two'),
    'renderYoutubeBody resolved_with_matrix makes each release a pickable rescue target');
  assert(matrixHtml.includes('Rescue from this'),
    'renderYoutubeBody matrix rows carry a "Rescue from this" button');
  // resolved_empty HIDES the rescue affordance (R9 — nothing to pick).
  const emptyHtml = renderYoutubeBody({ outcome: 'ok', youtube_releases: [] }, 9);
  assert(!emptyHtml.includes('Rescue from this') && emptyHtml.includes('Not on YouTube Music'),
    'renderYoutubeBody resolved_empty hides the rescue affordance and shows "not on YouTube Music"');
  assert(emptyHtml.includes('Re-check'),
    'renderYoutubeBody resolved_empty offers a re-check');
  // resolver_failed → Retry affordance.
  assert(renderYoutubeBody({ outcome: 'transient', error_message: 'mirror down' }, 9).includes('Retry'),
    'renderYoutubeBody resolver_failed offers a Retry affordance');

  // --- rescueOutcomeCopy: every ingest outcome → its intended copy ---
  // accepted → success tone, "rescue queued".
  const accepted = rescueOutcomeCopy({ outcome: 'accepted', download_log_id: 42 });
  assertEqual(accepted.tone, 'success', 'rescueOutcomeCopy accepted → success tone');
  assert(accepted.title.toLowerCase().includes('queued'), 'rescueOutcomeCopy accepted title says queued');
  assert(accepted.detail.includes('42'), 'rescueOutcomeCopy accepted surfaces the download_log_id');
  // in_flight → error tone, surfaces the existing download_log_id.
  const inFlight = rescueOutcomeCopy({ outcome: 'in_flight', download_log_id: 7 });
  assertEqual(inFlight.tone, 'error', 'rescueOutcomeCopy in_flight → error tone');
  assert(inFlight.detail.includes('already running') && inFlight.detail.includes('7'),
    'rescueOutcomeCopy in_flight surfaces the existing download_log_id');
  // wrong_state → "request changed — refresh".
  const wrongState = rescueOutcomeCopy({ outcome: 'wrong_state' });
  assert(wrongState.detail.toLowerCase().includes('refresh'),
    'rescueOutcomeCopy wrong_state tells the operator to refresh');
  // no_resolver_mapping → "re-run Check YouTube".
  const noMapping = rescueOutcomeCopy({ outcome: 'no_resolver_mapping' });
  assert(noMapping.detail.toLowerCase().includes('re-run check youtube'),
    'rescueOutcomeCopy no_resolver_mapping tells the operator to re-run Check YouTube');
  // track_count_precheck_failed → shows the precheck mismatch detail.
  const trackMismatch = rescueOutcomeCopy({
    outcome: 'track_count_precheck_failed', detail: 'expected 14, got 10' });
  assert(trackMismatch.detail.includes('expected 14, got 10'),
    'rescueOutcomeCopy track_count_precheck_failed surfaces the mismatch detail');
  // transient → retry.
  const transient = rescueOutcomeCopy({ outcome: 'transient' });
  assert(transient.detail.toLowerCase().includes('retry'),
    'rescueOutcomeCopy transient tells the operator to retry');
  assertEqual(transient.tone, 'error', 'rescueOutcomeCopy transient → error tone');
  // request_not_found → refresh.
  assert(rescueOutcomeCopy({ outcome: 'request_not_found' }).detail.toLowerCase().includes('refresh'),
    'rescueOutcomeCopy request_not_found tells the operator to refresh');
  // unknown outcome → generic error (never blank), surfaces the error field.
  const unknown = rescueOutcomeCopy({ outcome: 'who_knows', error: 'boom' });
  assertEqual(unknown.tone, 'error', 'rescueOutcomeCopy unknown → error tone');
  assert(unknown.detail.length > 0, 'rescueOutcomeCopy unknown → non-blank detail');
  // null result → generic error, never throws.
  assertEqual(rescueOutcomeCopy(null).tone, 'error', 'rescueOutcomeCopy null → error tone (no throw)');

  // --- canStartInFlight: double-fire guard predicate ---
  const inFlightSet = new Set();
  assertEqual(canStartInFlight(inFlightSet, 5), true,
    'canStartInFlight: nothing outstanding → may start');
  inFlightSet.add(5);
  assertEqual(canStartInFlight(inFlightSet, 5), false,
    'canStartInFlight: an outstanding call for the id → suppressed (double-fire guard)');
  assertEqual(canStartInFlight(inFlightSet, 6), true,
    'canStartInFlight: a different id is independent');

  // --- renderRescueConfirm: reuses the .confirm-box shell ---
  const confirm = renderRescueConfirm(11, 'MPREb_x', { artist_name: 'Smog', album_title: 'Knock Knock' });
  assert(confirm.includes('confirm-box') && confirm.includes('MPREb_x'),
    'renderRescueConfirm renders the confirm-box shell carrying the target browse id');
  assert(confirm.includes('Smog') && confirm.includes('Knock Knock'),
    'renderRescueConfirm labels the request being rescued');
  assert(confirm.includes('id="lt-rescue-confirm"') && confirm.includes('id="lt-rescue-cancel"'),
    'renderRescueConfirm wires confirm + cancel buttons');
}

// --- Summary ---
console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
