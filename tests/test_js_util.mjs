/**
 * Unit tests for web/js/util.js — pure utility functions.
 * Run with: node tests/test_js_util.mjs
 */

import { qualityLabel, qualityLabelShort, toAWST, awstDate, awstTime, awstDateTime, esc, jsArg, overrideToIntent, detectSource, externalReleaseUrl, sourceLabel } from '../web/js/util.js';
import { applyLabelFilters, sortByYearDesc, buildLabelSearchUrl, parseYear } from '../web/js/labels.js';

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

// --- Summary ---
console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
