/**
 * Unit tests for web/js/util.js — pure utility functions.
 *
 * It also holds the HTML tests for fragments belonging to four other
 * modules, and those call their renderers directly on purpose (issue
 * #1346). Each of those modules HAS a composed entry, and each entry is
 * driven where it belongs: `openReplacePicker` in
 * `tests/test_js_release_actions.mjs`, `renderLabelDetail` in
 * `tests/test_js_labels.mjs`, `renderConsoleShell` and `renderLongTailBody`
 * here. What the direct calls cover is the fragment's own HTML, which is
 * cheaper and sharper read one fragment at a time than as a needle in a
 * whole modal. The five long-tail-console panel bodies could not be
 * reached through the shell anyway: it renders `renderPanelLoading`
 * placeholders and the bodies arrive later from three separate endpoints.
 *
 * Run with: node tests/test_js_util.mjs
 */

import { qualityLabel, qualityLabelShort, toAWST, awstDate, awstTime, awstDateTime, esc, jsArg, overrideToIntent, detectSource, externalReleaseUrl, sourceLabel, youtubeBrowseUrl, renderForensicBlock, parsePastedId, youtubeSectionState, consoleEmphasis, withApplyDistance, isExternalAuthInterruption, wrongMatchExplorerFailureCopy } from '../web/js/util.js';
import { state } from '../web/js/state.js';
import { applyLabelFilters, sortByYearDesc, buildLabelSearchUrl, buildLabelDetailUrl, loadLabelReleases, parseYear, renderLabelLinks, distinctFormats, renderPaginationControls, renderLabelRows } from '../web/js/labels.js';
import {
  bandLabel,
  countOtherBandMatches,
  defaultBand,
  deriveBandTabs,
  filterRows,
  renderLongTailBody,
  renderLongTailRow,
} from '../web/js/long_tail.js';
import {
  PEERS_VISIBLE_CAP,
  acceptDisabledReason,
  buildAcceptSiblingOptions,
  canAcceptSibling,
  consoleStates,
  intentToggleTarget,
  renderActionsBar,
  renderConsoleShell,
  renderPanelError,
  renderPeersBody,
  renderRescueConfirm,
  renderRescuesBody,
  renderSiblingsBody,
  renderUnfindableBody,
  renderYoutubeBody,
  rescueOutcomeCopy,
  youtubeBestDistance,
  youtubeFailureReason,
  youtubeHistoryRows,
  youtubeRescueTargets,
} from '../web/js/long_tail_console.js';
import { wrapFetchWithSessionGuard, buildSessionExpiredOverlay, installSessionGuard } from '../web/js/session.js';

const t = suite(import.meta.url);

// --- qualityLabel tests ---
t.section('qualityLabel()');
t.equal(qualityLabel('FLAC', 1000), 'FLAC', 'FLAC ignores bitrate');
t.equal(qualityLabel('ALAC', 800), 'ALAC', 'ALAC ignores bitrate');
t.equal(qualityLabel('MP3', 320), 'MP3 320', 'MP3 320kbps');
t.equal(qualityLabel('MP3', 295), 'MP3 320', 'MP3 295 rounds to 320');
t.equal(qualityLabel('MP3', 245), 'MP3 V0', 'MP3 245 = V0');
t.equal(qualityLabel('MP3', 220), 'MP3 V0', 'MP3 220 = V0');
t.equal(qualityLabel('MP3', 190), 'MP3 V2', 'MP3 190 = V2');
t.equal(qualityLabel('MP3', 170), 'MP3 V2', 'MP3 170 = V2');
t.equal(qualityLabel('MP3', 128), 'MP3 128k', 'MP3 128 shows raw');
t.equal(qualityLabel('MP3', 0), 'MP3', 'MP3 0 bitrate = just format');
t.equal(qualityLabel('MP3', null), 'MP3', 'MP3 null bitrate = just format');
t.equal(qualityLabel('Opus', 192), 'OPUS 192k', 'Opus never borrows MP3 V-level names');
t.equal(qualityLabel('AAC', 256), 'AAC 256k', 'AAC never borrows MP3 V-level names');
t.equal(qualityLabel('Vorbis', 192), 'VORBIS 192k', 'Vorbis uses its native codec label');
t.equal(qualityLabel('WMA', 256), 'WMA 256k', 'WMA uses its native codec label');
t.equal(qualityLabel(null, 320), '?', 'null format = ?');
t.equal(qualityLabel('', 320), '?', 'empty format = ?');
t.equal(qualityLabel('MP3,FLAC', 250), 'MP3 V0', 'comma-separated uses first');

// --- qualityLabelShort tests ---
t.section('qualityLabelShort()');
t.equal(qualityLabelShort('MP3', 245), 'M V0', 'MP3 245 -> M V0');
t.equal(qualityLabelShort('MP3', 190), 'M V2', 'MP3 190 -> M V2');
t.equal(qualityLabelShort('MP3', 320), 'M 320', 'MP3 320 -> M 320');
t.equal(qualityLabelShort('MP3', 128), 'M 128', 'MP3 128 -> M 128');
t.equal(qualityLabelShort('FLAC', 1000), 'F', 'FLAC -> F (no bitrate suffix)');
t.equal(qualityLabelShort('ALAC', 800), 'AL', 'ALAC -> AL');
t.equal(qualityLabelShort('WAV', 1411), 'W', 'WAV -> W');
t.equal(qualityLabelShort('Opus', 128), 'O 128', 'Opus 128 -> O 128');
t.equal(qualityLabelShort('AAC', 192), 'A 192', 'AAC 192 -> A 192');
t.equal(qualityLabelShort('OGG', 192), 'OG 192', 'OGG -> OG');
t.equal(qualityLabelShort('Vorbis', 192), 'V 192', 'Vorbis -> V');
t.equal(qualityLabelShort('WMA', 256), 'WM 256', 'WMA -> WM');
t.equal(qualityLabelShort('', 320), '?', 'empty format');
t.equal(qualityLabelShort(null, 320), '?', 'null format');
t.equal(qualityLabelShort('MP3', 0), 'M', 'zero bitrate shows format only');
t.equal(qualityLabelShort('MP3', null), 'M', 'null bitrate shows format only');

// --- toAWST tests ---
t.section('toAWST()');
// UTC midnight = 8am AWST
t.equal(toAWST('2026-04-01T00:00:00Z'), '2026-04-01T08:00:00', 'UTC midnight = 08:00 AWST');
t.equal(toAWST('2026-04-01T16:00:00Z'), '2026-04-02T00:00:00', 'UTC 16:00 = next day 00:00 AWST');
t.equal(toAWST('2026-12-31T20:00:00Z'), '2027-01-01T04:00:00', 'year boundary');

// --- awstDate tests ---
t.section('awstDate()');
t.equal(awstDate('2026-04-01T00:00:00Z'), '2026-04-01', 'date from UTC midnight');

// --- awstTime tests ---
t.section('awstTime()');
t.equal(awstTime('2026-04-01T00:00:00Z'), '08:00', 'time from UTC midnight');

// --- awstDateTime tests ---
t.section('awstDateTime()');
t.equal(awstDateTime('2026-04-01T00:00:00Z'), '2026-04-01 08:00', 'datetime from UTC midnight');

// --- esc tests ---
t.section('esc()');
t.equal(esc('hello'), 'hello', 'plain text unchanged');
t.equal(esc('<script>alert(1)</script>'), '&lt;script&gt;alert(1)&lt;/script&gt;', 'escapes HTML tags');
t.equal(esc('a & b'), 'a &amp; b', 'escapes ampersand');
t.equal(esc('"quotes"'), '&quot;quotes&quot;', 'escapes double quotes');
t.equal(esc("Guns N' Roses"), 'Guns N&#39; Roses', 'escapes single quotes');
t.equal(esc('back\\slash'), 'back&#92;slash', 'escapes backslashes');
t.equal(esc("it\\'s"), 'it&#92;&#39;s', 'escapes backslash+quote combo');
t.equal(esc(''), '', 'empty string');
t.equal(esc(null), '', 'null returns empty');
t.equal(esc(undefined), '', 'undefined returns empty');

// --- overrideToIntent tests ---
t.section('overrideToIntent()');
t.equal(overrideToIntent(null), 'default', 'null → default');
t.equal(overrideToIntent(undefined), 'default', 'undefined → default');
t.equal(overrideToIntent(''), 'default', 'empty string → default');
t.equal(overrideToIntent('lossless'), 'lossless', '"lossless" → lossless');
t.equal(overrideToIntent('flac'), 'lossless', '"flac" (backward compat) → lossless');
t.equal(overrideToIntent('flac,mp3 v0,mp3 320'), 'default', 'CSV → default');
t.equal(overrideToIntent('unknown'), 'default', 'unknown → default');

// --- jsArg tests ---
t.section('jsArg()');
t.equal(jsArg("Kid A's"), '&quot;Kid A&#39;s&quot;', 'encodes apostrophes inside JS string literal');
t.equal(jsArg(null), '&quot;&quot;', 'null becomes empty string literal');

// --- detectSource tests ---
t.section('detectSource()');
t.equal(detectSource('89ad4ac3-39f7-470e-963a-56509c546377'), 'musicbrainz', 'UUID → musicbrainz');
t.equal(detectSource(' 89AD4AC3-39F7-470E-963A-56509C546377 '), 'musicbrainz', 'UUID whitespace/case normalizes');
t.equal(detectSource('2048516'), 'discogs', 'numeric → discogs');
t.equal(detectSource(' 0012856590 '), 'discogs', 'numeric whitespace/leading zeros normalize');
t.equal(detectSource(''), 'unknown', 'empty → unknown');
t.equal(detectSource('0'), 'unknown', 'zero sentinel → unknown');
t.equal(detectSource(null), 'unknown', 'null → unknown');
t.equal(detectSource(undefined), 'unknown', 'undefined → unknown');
t.equal(detectSource('NONE'), 'unknown', 'NONE → unknown');

// --- externalReleaseUrl tests ---
t.section('externalReleaseUrl()');
t.equal(
  externalReleaseUrl('89ad4ac3-39f7-470e-963a-56509c546377'),
  'https://musicbrainz.org/release/89ad4ac3-39f7-470e-963a-56509c546377',
  'MB UUID → musicbrainz.org'
);
t.equal(
  externalReleaseUrl('2048516'),
  'https://www.discogs.com/release/2048516',
  'Discogs numeric → discogs.com'
);
t.equal(
  externalReleaseUrl('not-a-real-id'),
  '',
  'unknown id → empty external URL'
);

// --- sourceLabel tests ---
t.section('sourceLabel()');
t.equal(sourceLabel('89ad4ac3-39f7-470e-963a-56509c546377'), 'MusicBrainz', 'UUID → MusicBrainz');
t.equal(sourceLabel('2048516'), 'Discogs', 'numeric → Discogs');
t.equal(sourceLabel('not-a-real-id'), '', 'unknown id → empty source label');

// --- youtubeBrowseUrl tests ---
t.section('youtubeBrowseUrl()');
t.equal(
  youtubeBrowseUrl('MPREb_abc123'),
  'https://music.youtube.com/browse/MPREb_abc123',
  'browse id → YT Music album URL');
t.equal(youtubeBrowseUrl('  MPREb_xy  '), 'https://music.youtube.com/browse/MPREb_xy',
  'trims whitespace');
t.equal(youtubeBrowseUrl(''), '', 'empty → empty');
t.equal(youtubeBrowseUrl(null), '', 'null → empty');
t.equal(youtubeBrowseUrl(undefined), '', 'undefined → empty');

// --- parseYear tests ---
t.section('parseYear()');
t.equal(parseYear('2003'), 2003, 'year-only string');
t.equal(parseYear('2003-04-15'), 2003, 'full ISO date');
t.equal(parseYear('2003-04'), 2003, 'year-month');
t.equal(parseYear(''), null, 'empty string → null');
t.equal(parseYear(null), null, 'null → null');
t.equal(parseYear(undefined), null, 'undefined → null');
t.equal(parseYear('not-a-year'), null, 'garbage → null');

// --- buildLabelSearchUrl tests ---
t.section('buildLabelSearchUrl()');
t.equal(buildLabelSearchUrl('hymen'), '/api/discogs/label/search?q=hymen', 'simple query');
t.equal(buildLabelSearchUrl('warp records'), '/api/discogs/label/search?q=warp%20records', 'spaces encoded');
t.equal(buildLabelSearchUrl('a&b'), '/api/discogs/label/search?q=a%26b', 'special chars encoded');
t.equal(buildLabelSearchUrl('björk'), '/api/discogs/label/search?q=bj%C3%B6rk', 'unicode encoded');

// --- buildLabelDetailUrl tests ---
t.section('buildLabelDetailUrl()');
t.equal(buildLabelDetailUrl('757'), '/api/discogs/label/757', 'no opts: no query string');
t.equal(
  buildLabelDetailUrl('757', { include_sublabels: true }),
  '/api/discogs/label/757?include_sublabels=true',
  'include_sublabels=true emitted');
t.equal(
  buildLabelDetailUrl('757', { include_sublabels: false }),
  '/api/discogs/label/757?include_sublabels=false',
  'include_sublabels=false emitted');
t.equal(
  buildLabelDetailUrl('757', { include_sublabels: true, page: 2, per_page: 50 }),
  '/api/discogs/label/757?include_sublabels=true&page=2&per_page=50',
  'pagination params emitted in order');
t.equal(
  buildLabelDetailUrl(757, { page: 3 }),
  '/api/discogs/label/757?page=3',
  'numeric labelId coerced to string');
t.equal(
  buildLabelDetailUrl('757', { include_sublabels: undefined, page: undefined, per_page: undefined }),
  '/api/discogs/label/757',
  'undefined opts produce no params');

async function captureLoadLabelUrl(opts) {
  let seenUrl = '';
  const globals = stubGlobals({
    fetch: async (url) => {
      seenUrl = String(url);
      return { ok: true, json: async () => ({ ok: true }) };
    },
  });
  try {
    await loadLabelReleases('757', opts);
    return seenUrl;
  } finally {
    globals.restore();
  }
}

t.equal(
  await captureLoadLabelUrl({ page: 1 }),
  '/api/discogs/label/757?page=1',
  'default label load omits include_sublabels so route auto-flip can run');
t.equal(
  await captureLoadLabelUrl({ include_sublabels: true, page: 2 }),
  '/api/discogs/label/757?include_sublabels=true&page=2',
  'explicit include_sublabels=true is preserved');
t.equal(
  await captureLoadLabelUrl({ include_sublabels: false, page: 2 }),
  '/api/discogs/label/757?include_sublabels=false&page=2',
  'explicit include_sublabels=false is preserved');

// --- renderPaginationControls tests ---
t.section('renderPaginationControls()');
t.equal(renderPaginationControls(1, 1), '', 'pages=1 → empty');
t.equal(renderPaginationControls(1, 0), '', 'pages=0 → empty');
const ctrl_p1_of_5 = renderPaginationControls(1, 5);
t.contains(ctrl_p1_of_5, 'Page 1 of 5', 'p1/5: position label rendered');
t.contains(ctrl_p1_of_5, 'disabled', 'p1/5: prev button is disabled');
t.contains(ctrl_p1_of_5, 'window.goToLabelPage(2)', 'p1/5: next button targets page 2');
const ctrl_p5_of_5 = renderPaginationControls(5, 5);
t.contains(ctrl_p5_of_5, 'window.goToLabelPage(4)', 'p5/5: prev button targets page 4');
t.ok(ctrl_p5_of_5.match(/disabled/g).length === 1, 'p5/5: only next button is disabled');
const ctrl_p3_of_5 = renderPaginationControls(3, 5);
t.excludes(ctrl_p3_of_5, 'disabled', 'p3/5: neither button disabled');
t.contains(ctrl_p3_of_5, 'window.goToLabelPage(2)', 'p3/5: prev → page 2');
t.contains(ctrl_p3_of_5, 'window.goToLabelPage(4)', 'p3/5: next → page 4');

// --- renderLabelRows tests ---
t.section('renderLabelRows()');
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
  t.contains(body.innerHTML, 'Greetings From Birmingham', 'label row renders release title');
  t.contains(body.innerHTML, 'window.toggleReleaseDetail(&quot;12856590&quot;)',
    'label row opens exact Discogs release details');
  t.contains(body.innerHTML, 'id="reldet-12856590"',
    'label row renders matching release-detail container');
  t.excludes(body.innerHTML, 'window.loadReleaseGroup(&quot;12856590&quot;',
    'label row does not route Discogs release id through release-group loader');
}

// --- applyLabelFilters tests ---
t.section('applyLabelFilters()');
const ROWS = [
  { id: '1', title: 'A', date: '2000-01-01', format: 'CD',  in_library: false },
  { id: '2', title: 'B', date: '2001-06-15', format: 'LP',  in_library: true  },
  { id: '3', title: 'C', date: '2002',       format: 'CD, EP', in_library: false },
  { id: '4', title: 'D', date: '2003-04-01', format: 'LP, Album', in_library: true },
  { id: '5', title: 'E', date: '2004-12-01', format: 'Vinyl', in_library: false },
  { id: '6', title: 'F', date: '',           format: 'CD',  in_library: false },
];

t.equal(applyLabelFilters(ROWS, {}).length, 6, 'empty filters returns all rows');
t.equal(applyLabelFilters(ROWS, { yearMin: null, yearMax: null, format: '', hideHeld: false }).length, 6, 'null/empty filters returns all rows');

const yearFilt = applyLabelFilters(ROWS, { yearMin: 2001, yearMax: 2003 });
t.equal(yearFilt.length, 3, 'year [2001..2003] inclusive matches 3 rows');
t.equal(yearFilt.map(r => r.id).join(','), '2,3,4', 'year filter keeps correct rows');

const yearOnlyMin = applyLabelFilters(ROWS, { yearMin: 2003 });
t.equal(yearOnlyMin.map(r => r.id).join(','), '4,5', 'yearMin alone (drops empty-date row when filtered)');

const yearOnlyMax = applyLabelFilters(ROWS, { yearMax: 2001 });
t.equal(yearOnlyMax.map(r => r.id).join(','), '1,2', 'yearMax alone');

// Empty-date rows survive year filtering ONLY when no year filter applied
const emptyDateNoFilter = applyLabelFilters(ROWS, { format: '' });
t.equal(emptyDateNoFilter.find(r => r.id === '6') !== undefined, true,
  'empty-date row survives when no year filter applied');
const emptyDateYearFilter = applyLabelFilters(ROWS, { yearMin: 2000, yearMax: 2010 });
t.equal(emptyDateYearFilter.find(r => r.id === '6'), undefined,
  'empty-date row dropped when year filter active');

const fmtLP = applyLabelFilters(ROWS, { format: 'LP' });
t.equal(fmtLP.map(r => r.id).join(','), '2,4', 'format LP matches substring');
const fmtCD = applyLabelFilters(ROWS, { format: 'CD' });
t.equal(fmtCD.map(r => r.id).join(','), '1,3,6', 'format CD matches substring');
const fmtEmpty = applyLabelFilters(ROWS, { format: '' });
t.equal(fmtEmpty.length, 6, 'empty format means no filter');

const hideHeld = applyLabelFilters(ROWS, { hideHeld: true });
t.equal(hideHeld.map(r => r.id).join(','), '1,3,5,6', 'hideHeld excludes in_library:true');

// All filters layered
const layered = applyLabelFilters(ROWS, { yearMin: 2000, yearMax: 2003, format: 'CD', hideHeld: true });
t.equal(layered.map(r => r.id).join(','), '1,3', 'layered filters intersect correctly');

// --- sortByYearDesc tests ---
t.section('sortByYearDesc()');
const SORTED = sortByYearDesc([
  { id: '1', date: '2003-04-01' },
  { id: '2', date: '2001-01-01' },
  { id: '3', date: '' },
  { id: '4', date: '2003-12-31' },
  { id: '5', date: null },
]);
t.equal(SORTED.map(r => r.id).join(','), '1,4,2,3,5',
  'year desc; missing year sorts last; equal-year stable by input order');

// stability across equal years
const STABLE = sortByYearDesc([
  { id: 'a', date: '2010' },
  { id: 'b', date: '2010' },
  { id: 'c', date: '2010' },
]);
t.equal(STABLE.map(r => r.id).join(','), 'a,b,c', 'equal years preserve input order (stable)');

// does not mutate input
const ORIG = [{ id: '1', date: '2000' }, { id: '2', date: '2010' }];
sortByYearDesc(ORIG);
t.equal(ORIG[0].id, '1', 'sortByYearDesc does not mutate input');

// --- renderLabelLinks tests (U7) ---
t.section('renderLabelLinks()');

// Single Discogs-style label (id + name) → clickable link.
const hymen = renderLabelLinks([{ id: 757, name: 'Hymen Records' }]);
t.contains(hymen, 'Hymen Records', 'renders the label name');
t.contains(hymen, 'data-label-id="757"', 'tags the link with data-label-id="757"');
t.contains(hymen, 'window.openLabelDetail', 'wires window.openLabelDetail call');
t.contains(hymen, 'class="label-link"', 'tags the anchor with the label-link class');
t.match(hymen, /<a\b/i, 'renders an anchor element');

// Empty input → empty string.
t.equal(renderLabelLinks([]), '', 'empty array → empty string');
t.equal(renderLabelLinks(null), '', 'null → empty string');
t.equal(renderLabelLinks(undefined), '', 'undefined → empty string');

// MB-style label (no id) → plain text, no anchor.
const mbOnly = renderLabelLinks([{ name: 'Some MB Label' }]);
t.equal(mbOnly, 'Some MB Label', 'MB-style (no id) renders plain text');
t.notMatch(mbOnly, /<a\b/i, 'MB-style renders no anchor element');

// id explicitly null → plain text (Phase B placeholder).
const mbExplicitNull = renderLabelLinks([{ id: null, name: 'MB Label' }]);
t.equal(mbExplicitNull, 'MB Label', 'explicit id=null renders plain text');

// Multiple labels with usable IDs → comma-separated links.
const warpDual = renderLabelLinks([
  { id: 757, name: 'Warp Records' },
  { id: 758, name: 'Warp Singles' },
]);
t.contains(warpDual, 'Warp Records', 'first label name rendered');
t.contains(warpDual, 'Warp Singles', 'second label name rendered');
t.contains(warpDual, 'data-label-id="757"', 'first link has correct id attr');
t.contains(warpDual, 'data-label-id="758"', 'second link has correct id attr');
t.equal((warpDual.match(/<a\b/gi) || []).length, 2, 'two anchor elements rendered');
t.contains(warpDual, '</a>, <a', 'anchors are separated by ", "');

// Mixed: one with id (link), one without (text).
const mixed = renderLabelLinks([
  { id: 757, name: 'Hymen Records' },
  { name: 'Plaintext Co.' },
]);
t.contains(mixed, 'Hymen Records', 'mixed: linked name present');
t.contains(mixed, 'Plaintext Co.', 'mixed: plain name present');
t.equal((mixed.match(/<a\b/gi) || []).length, 1, 'mixed: only the id-bearing entry becomes a link');

// XSS guard — name with <script> is escaped, no raw tag in output.
const xss = renderLabelLinks([{ id: 1, name: '<script>alert(1)</script>' }]);
t.excludes(xss, '<script>', 'XSS guard: raw <script> tag not present in output');
t.contains(xss, '&lt;script&gt;', 'XSS guard: angle brackets entity-escaped');

// XSS guard via name with quotes — should not break out of jsArg().
const xssQuote = renderLabelLinks([{ id: 1, name: 'Bad", alert(1), "X' }]);
t.excludes(xssQuote, '", alert', 'XSS guard: quote escapes prevent attribute break-out');
t.contains(xssQuote, '&quot;', 'XSS guard: double quotes are entity-escaped');

// Empty / falsy entries skipped.
t.equal(renderLabelLinks([null, undefined, { id: 1, name: '' }, { id: 2, name: 'OK' }]),
  '<a href="#" class="label-link" data-label-id="2" onclick="event.stopPropagation(); event.preventDefault(); window.openLabelDetail(&quot;2&quot;, &quot;OK&quot;)">OK</a>',
  'null/undefined/empty-name entries are skipped');

// Numeric-string id is honored.
const stringId = renderLabelLinks([{ id: '12345', name: 'String ID Label' }]);
t.contains(stringId, 'data-label-id="12345"', 'string id is preserved');
t.contains(stringId, '<a', 'string id renders as link');

// --- distinctFormats tests (review-fix #9) ---
t.section('distinctFormats()');

// Empty input → empty array.
const emptyFmts = distinctFormats([]);
t.equal(Array.isArray(emptyFmts), true, 'empty input returns an array');
t.equal(emptyFmts.length, 0, 'empty input → empty array');

// Single row, single format.
t.equal(distinctFormats([{ format: 'CD' }]).join(','), 'CD',
  'single row single format');

// Duplicates dedup'd; sorted alphabetically.
const dups = distinctFormats([
  { format: 'CD' }, { format: 'CD' }, { format: 'LP' }, { format: 'CD' },
]);
t.equal(dups.join(','), 'CD,LP', 'duplicates collapse, sort applied');

// Multi-value formats (joined Discogs string) split on commas.
const multi = distinctFormats([
  { format: 'LP, Album' },
  { format: 'CD, EP' },
  { format: 'Vinyl, LP' }, // LP appears in two rows, dedup'd
]);
t.equal(multi.join(','), 'Album,CD,EP,LP,Vinyl',
  'comma-joined formats split, dedup, alphabetized');

// Whitespace trimmed; empty tokens dropped.
const ws = distinctFormats([
  { format: '  CD  ,  LP ,, ' },
  { format: '' },
]);
t.equal(ws.join(','), 'CD,LP', 'whitespace trimmed, empty tokens dropped');

// Missing/null format field on a row — row skipped, no crash.
const nullFmt = distinctFormats([
  { format: null },
  { format: undefined },
  { /* no format key */ },
  { format: 'CD' },
]);
t.equal(nullFmt.join(','), 'CD', 'null/undefined/missing format fields skipped');

// All missing → empty.
t.equal(distinctFormats([{}, { format: '' }]).join(','), '',
  'no usable formats → empty array');

// --- applyLabelFilters NaN year guard tests (review-fix #10) ---
t.section('applyLabelFilters() NaN year guard');

const NAN_ROWS = [
  { id: '1', date: '2000-01-01', format: 'CD', in_library: false },
  { id: '2', date: '2010-01-01', format: 'CD', in_library: false },
  { id: '3', date: '',           format: 'CD', in_library: false },
];

// Explicit NaN bounds must behave as "no bound", not "drop everything".
const nanMin = applyLabelFilters(NAN_ROWS, { yearMin: NaN });
t.equal(nanMin.length, 3, 'NaN yearMin treated as no lower bound');

const nanMax = applyLabelFilters(NAN_ROWS, { yearMax: NaN });
t.equal(nanMax.length, 3, 'NaN yearMax treated as no upper bound');

const nanBoth = applyLabelFilters(NAN_ROWS, { yearMin: NaN, yearMax: NaN });
t.equal(nanBoth.length, 3, 'both NaN bounds → no filter');

// NaN min + valid max → max still applies, undated still drops.
const mixedNan = applyLabelFilters(NAN_ROWS, { yearMin: NaN, yearMax: 2005 });
t.equal(mixedNan.map(r => r.id).join(','), '1',
  'valid yearMax with NaN yearMin still filters correctly');

// --- renderForensicBlock tests ---
t.section('renderForensicBlock()');
const noBlock = renderForensicBlock(null);
t.contains(noBlock, 'No search forensic data yet',
  'null last_search → "no forensic data" message');
t.contains(noBlock, 'p-forensic',
  'null last_search still wraps in .p-forensic for layout');

const emptyTopBlock = renderForensicBlock({
  variant: 'v1_year', final_state: 'Completed', outcome: 'no_match',
  top_candidates: [],
});
t.contains(emptyTopBlock, 'v1_year',
  'variant tag rendered');
t.contains(emptyTopBlock, 'Completed',
  'final_state rendered');
t.contains(emptyTopBlock, 'No candidates captured',
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
t.contains(populatedBlock, 'alice', 'first candidate username rendered');
t.contains(populatedBlock, 'bob', 'second candidate username rendered');
t.contains(populatedBlock, '26/26',
  'matched/total rendered for first row');
t.contains(populatedBlock, '0.95',
  'avg_ratio rendered to 2 decimals');
t.contains(populatedBlock, 'flac',
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
t.excludes(xssBlock, '<script>x</script>',
  'malicious username escaped');
t.excludes(xssBlock, '"><img>',
  'malicious dir escaped');

// --- youtubeSectionState tests (U4 four-state classifier) ---
t.section('youtubeSectionState()');
// null / undefined / non-object → never_run (the side-effectful GET has
// not been run; U4 must NOT auto-call it).
t.equal(youtubeSectionState(null).state, 'never_run', 'null → never_run');
t.equal(youtubeSectionState(undefined).state, 'never_run', 'undefined → never_run');
// A truthy object with no `outcome` field is a malformed/failed resolve
// (its outcome is not "ok"), NOT never_run — only null/undefined defaults
// to the not-yet-run state.
t.equal(youtubeSectionState({}).state, 'resolver_failed', 'object with no outcome → resolver_failed');
t.equal(youtubeSectionState(null).stale, false, 'never_run is never stale');
// ok + releases → resolved_with_matrix.
const ytMatrix = youtubeSectionState({
  outcome: 'ok',
  youtube_releases: [{ yt_browse_id: 'MPREb_x', distances: [] }],
  from_cache: false,
});
t.equal(ytMatrix.state, 'resolved_with_matrix', 'ok + releases>0 → resolved_with_matrix');
t.equal(ytMatrix.stale, false, 'fresh matrix not stale');
t.equal(ytMatrix.message, '', 'fresh matrix needs no message');
// ok + empty releases → resolved_empty.
const ytEmpty = youtubeSectionState({ outcome: 'ok', youtube_releases: [], from_cache: false });
t.equal(ytEmpty.state, 'resolved_empty', 'ok + releases==0 → resolved_empty');
t.contains(ytEmpty.message, 'Not on YouTube Music', 'resolved_empty surfaces "not on YouTube Music" copy');
// ok + missing youtube_releases key → resolved_empty (no releases).
t.equal(youtubeSectionState({ outcome: 'ok' }).state, 'resolved_empty', 'ok + no releases key → resolved_empty');
// transient / 503 outcomes → resolver_failed.
t.equal(youtubeSectionState({ outcome: 'transient' }).state, 'resolver_failed', 'transient → resolver_failed');
t.equal(youtubeSectionState({ outcome: 'unresolved_timeout' }).state, 'resolver_failed', 'unresolved_timeout → resolver_failed');
t.equal(youtubeSectionState({ outcome: 'unresolved_mirror_unavailable' }).state, 'resolver_failed', 'mirror unavailable → resolver_failed');
t.equal(youtubeSectionState({ outcome: 'not_found' }).state, 'resolver_failed', 'not_found → resolver_failed');
const ytFail = youtubeSectionState({ outcome: 'transient', error_message: 'mirror down' });
t.contains(ytFail.message, 'mirror down', 'resolver_failed surfaces the error_message');
// from_cache + error_message → staleness flag on an otherwise-resolved state.
const ytStaleMatrix = youtubeSectionState({
  outcome: 'ok',
  youtube_releases: [{ yt_browse_id: 'MPREb_y', distances: [] }],
  from_cache: true,
  error_message: 'live YT fetch failed; served cache',
});
t.equal(ytStaleMatrix.state, 'resolved_with_matrix', 'cached matrix still resolves with matrix');
t.equal(ytStaleMatrix.stale, true, 'from_cache + error_message sets the staleness flag');
t.contains(ytStaleMatrix.message, 'stale', 'stale matrix surfaces a staleness message');
// from_cache WITHOUT error_message is a clean cache hit, not stale.
t.equal(
  youtubeSectionState({ outcome: 'ok', youtube_releases: [{ yt_browse_id: 'z', distances: [] }], from_cache: true }).stale,
  false,
  'from_cache alone (no error_message) is not stale',
);

// --- consoleEmphasis tests (U4 band-aware emphasis selector) ---
t.section('consoleEmphasis()');
t.equal(consoleEmphasis({ band: 'missing' }).lead, 'unfindable', 'Missing band leads with unfindable panel');
t.equal(consoleEmphasis({ band: 'MISSING' }).lead, 'unfindable', 'Missing band is case-insensitive');
t.equal(consoleEmphasis({ band: '' }).lead, 'unfindable', 'no band (treated Missing-like) leads with unfindable');
t.equal(consoleEmphasis({}).lead, 'unfindable', 'missing band key leads with unfindable');
t.equal(consoleEmphasis(null).lead, 'unfindable', 'null row leads with unfindable');
t.equal(consoleEmphasis({ band: 'poor' }).lead, 'band_vs_intent', 'on-disk band leads with band-vs-intent');
t.equal(consoleEmphasis({ band: 'transparent' }).lead, 'band_vs_intent', 'on-disk transparent leads with band-vs-intent');
// An on-disk row carrying an unfindable_category still leads with unfindable
// (the operator's first question is "why stuck", even if a copy exists).
t.equal(
  consoleEmphasis({ band: 'poor', unfindable_category: 'wrong_pressing_available' }).lead,
  'unfindable',
  'on-disk row with an unfindable_category leads with unfindable',
);

// --- parsePastedId tests (search-by-ID) ---
t.section('parsePastedId()');

function assertParse(input, expected, msg) {
  t.deepEqual(parsePastedId(input), expected, msg);
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
assertParse(
  'https://www.discogs.com/release/32457180-Various-Rock-Christmas?utm_source=mobile#images',
  { family: 'discogs', kind: 'release', id: '32457180' },
  'phone-copied Discogs URL with slug, tracking query, and fragment',
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
assertParse('0', null, 'zero is not a Discogs release identity');
assertParse('000123', null, 'leading-zero Discogs identity rejected');
assertParse(
  'https://www.discogs.com/release/000123-Slug',
  null,
  'leading-zero Discogs URL identity rejected',
);

// Hostile URL boundaries: a trusted hostname must be the exact parsed origin,
// never a path fragment, credential, suffix, or non-HTTP scheme.
assertParse(
  'https://evil.example/musicbrainz.org/release/c1f6a2c9-bcba-4e69-96f5-233c85b2830a',
  null,
  'MusicBrainz hostname embedded in an attacker path rejected',
);
assertParse(
  'https://evil.example/discogs.com/release/32457180',
  null,
  'Discogs hostname embedded in an attacker path rejected',
);
assertParse(
  'javascript://musicbrainz.org/release/c1f6a2c9-bcba-4e69-96f5-233c85b2830a',
  null,
  'javascript scheme rejected',
);
assertParse(
  'ftp://www.discogs.com/release/32457180',
  null,
  'non-HTTP URL scheme rejected',
);
assertParse(
  'https://musicbrainz.org.evil.example/release/c1f6a2c9-bcba-4e69-96f5-233c85b2830a',
  null,
  'trusted-host suffix rejected',
);
assertParse(
  'https://musicbrainz.org@evil.example/release/c1f6a2c9-bcba-4e69-96f5-233c85b2830a',
  null,
  'trusted hostname in URL credentials rejected',
);
assertParse(
  'https://:x@musicbrainz.org/release/c1f6a2c9-bcba-4e69-96f5-233c85b2830a',
  null,
  'userinfo rejected even when the parsed host is trusted',
);
assertParse(
  'https://musicbrainz.org:444/release/c1f6a2c9-bcba-4e69-96f5-233c85b2830a',
  null,
  'non-canonical port rejected',
);
assertParse(
  'https://musicbrainz.org/release/c1f6a2c9-bcba-4e69-96f5-233c85b2830a/extra',
  null,
  'extra MusicBrainz path components rejected',
);
assertParse(
  'https://www.discogs.com/release/32457180/extra',
  null,
  'extra Discogs path components rejected',
);
assertParse(
  'https://musicbrainz.org/release/------------------------------------',
  null,
  'malformed 36-character UUID rejected before the resolver',
);
assertParse(
  `https://musicbrainz.org/release/c1f6a2c9-bcba-4e69-96f5-233c85b2830a?${'x'.repeat(2048)}`,
  null,
  'oversized pasted input rejected before URL parsing',
);

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
  distanceIsPartial,
  distanceIncompleteQualifier,
  runWithConcurrency,
  renderMasterlessNote,
  extractTracklist,
  esc as replaceEsc,
} from '../web/js/replace_picker.js';

t.equal(
  renderPressingsList([], 'whatever').includes('No pressings'),
  true,
  'renderPressingsList empty → friendly message',
);

const sample = [
  { id: 'aaa', title: 'Pressing A', date: '2020-01-01', country: 'US', track_count: 12, format: 'CD' },
  { id: 'bbb', title: 'Pressing B', date: '2021-05-01', country: 'JP', track_count: 13, format: 'LP' },
];
const pressingsHtml = renderPressingsList(sample, 'aaa');
t.contains(pressingsHtml, 'data-expand-mbid="aaa"',
  'renderPressingsList wires current pressing as expandable row');
t.match(pressingsHtml, /data-expand-mbid="aaa"[^>]*disabled|disabled[^>]*data-expand-mbid="aaa"/,
  'renderPressingsList marks current pressing disabled');
t.contains(pressingsHtml, 'current pressing', 'renderPressingsList labels current pressing');
t.contains(pressingsHtml, 'data-expand-mbid="bbb"', 'renderPressingsList includes sibling');
t.notMatch(pressingsHtml, /<button[^>]*data-expand-mbid="bbb"[^>]*disabled/,
  'renderPressingsList does not disable non-current siblings');
t.contains(pressingsHtml, 'data-mbid="bbb"',
  'renderPressingsList tags the non-current pressing with its own mbid');
t.match(pressingsHtml, /replace-picker-confirm[^>]*data-mbid="bbb"/,
  'renderPressingsList renders pick-button for non-current pressing');
t.notMatch(pressingsHtml, /replace-picker-confirm[^>]*data-mbid="aaa"/,
  'renderPressingsList omits pick-button for current pressing');
t.excludes(pressingsHtml, 'aaa</small>',
  'renderPressingsList does not expose the current MBID to the operator');
t.excludes(pressingsHtml, 'bbb</small>',
  'renderPressingsList does not expose a sibling MBID to the operator');
t.ok(/2020.*US.*CD.*12t/.test(pressingsHtml) || pressingsHtml.includes('US · 2020 · CD · 12t'),
  'renderPressingsList renders meta in country·year·format·Nt order');

t.equal(
  renderRequestsList([]).includes('No active requests'),
  true,
  'renderRequestsList empty → friendly message',
);
const reqHtml = renderRequestsList([
  { id: 42, mb_release_id: 'old-uuid', status: 'wanted', artist_name: 'Pet Grief', album_title: 'X' },
]);
t.contains(reqHtml, 'data-rid="42"', 'renderRequestsList carries id (pick button)');
t.contains(reqHtml, 'Pet Grief', 'renderRequestsList includes artist');
t.contains(reqHtml, 'data-expand-mbid="old-uuid"',
  'renderRequestsList row expands by MBID');
t.contains(reqHtml, 'data-tracks-for="old-uuid"',
  'renderRequestsList renders lazy tracklist container per row');
t.notMatch(reqHtml, /<small[^>]*>[^<]*old-uuid/, 'renderRequestsList hides the MBID from the operator');

const dlg = renderConfirmDialog({
  sourceRequestId: 4194,
  targetMbid: '18056805-33f5-3e99-aa4b-5f5919c4f8af',
  targetLabel: 'Pet Grief — New Pressing',
});
t.contains(dlg, 'Replace request #4194', 'confirm dialog includes source id');
t.contains(dlg, '18056805-33f5-3e99-aa4b-5f5919c4f8af', 'confirm dialog includes target mbid');
t.contains(dlg, 'issue #278', 'confirm dialog mentions orphan transfer issue #278');
t.contains(dlg, 'replace-picker-cancel', 'confirm dialog has cancel button id');
t.contains(dlg, 'replace-picker-confirm', 'confirm dialog has confirm button id');
t.contains(dlg, 'frozen for audit', 'confirm dialog explains supersede semantics');

t.contains(renderStandardHeader('Pet Grief — Old'), 'Switch',
  'renderStandardHeader carries "Switch" verb');
t.contains(renderInvertedHeader('Pet Grief — New'), 'replace an existing request',
  'renderInvertedHeader carries inverted-mode verb');

// Tracklist container is rendered hidden until row.open
t.contains(pressingsHtml, 'data-tracks-for="aaa"',
  'renderPressingsList renders tracklist container per row');

// formatLength
t.equal(formatLength(0), '0:00', 'formatLength 0 → 0:00');
t.equal(formatLength(7), '0:07', 'formatLength <60s pads seconds');
t.equal(formatLength(63), '1:03', 'formatLength 63s → 1:03');
t.equal(formatLength(263.4), '4:23', 'formatLength rounds');
t.equal(formatLength(null), '', 'formatLength null → empty');
t.equal(formatLength(undefined), '', 'formatLength undefined → empty');
t.equal(formatLength(NaN), '', 'formatLength NaN → empty');

// renderTracklist
t.contains(renderTracklist([]), 'No tracks',
  'renderTracklist empty → friendly message');
const tlHtml = renderTracklist([
  { disc_number: 1, track_number: 1, title: 'Aaa', length_seconds: 200 },
  { disc_number: 1, track_number: 2, title: 'Bbb <em>', length_seconds: 263.4 },
]);
t.contains(tlHtml, 'Aaa', 'renderTracklist includes title');
t.contains(tlHtml, '4:23', 'renderTracklist formats track length');
t.contains(tlHtml, '&lt;em&gt;', 'renderTracklist escapes titles');
t.notMatch(tlHtml, /Disc 1/, 'renderTracklist hides disc header for single-disc');

const multiDiscHtml = renderTracklist([
  { disc_number: 1, track_number: 1, title: 'A', length_seconds: 60 },
  { disc_number: 2, track_number: 1, title: 'B', length_seconds: 60 },
]);
t.contains(multiDiscHtml, 'Disc 1',
  'renderTracklist shows the disc 1 header for a multi-disc release');
t.contains(multiDiscHtml, 'Disc 2',
  'renderTracklist shows the disc 2 header for a multi-disc release');

// renderSourcePanel
const loadingPanel = renderSourcePanel({
  label: 'Pet Grief — Old',
  meta: 'US · 2020 · CD · 12t',
  tracks: null,
  loading: true,
});
t.contains(loadingPanel, 'Current request:',
  'renderSourcePanel labels the panel "Current request:"');
t.contains(loadingPanel, 'Pet Grief — Old', 'renderSourcePanel includes the label');
t.contains(loadingPanel, 'US · 2020 · CD · 12t',
  'renderSourcePanel renders meta line on summary');
t.contains(loadingPanel, 'Loading', 'renderSourcePanel loading state shows placeholder');
t.contains(loadingPanel, 'replace-picker-source-body',
  'renderSourcePanel exposes the body container for lazy fill');

const loadedPanel = renderSourcePanel({
  label: 'X',
  tracks: [{ disc_number: 1, track_number: 1, title: 'Z', length_seconds: 120 }],
});
t.contains(loadedPanel, 'Z', 'renderSourcePanel renders tracks when loaded');
t.contains(loadedPanel, '2:00', 'renderSourcePanel renders track duration');

const errorPanel = renderSourcePanel({ label: 'X', tracks: null, error: 'HTTP 500' });
t.contains(errorPanel, 'HTTP 500', 'renderSourcePanel renders error message');

// pickBestDistance — picks the lowest-distance ok result; null when none scored
t.equal(pickBestDistance([]), null, 'pickBestDistance [] → null');
t.equal(
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
t.equal(best?.distance, 0.07, 'pickBestDistance picks lowest');
t.equal(best?.matched_tracks, 12, 'pickBestDistance carries metadata');

// formatDistanceBadge — empty for null, formatted otherwise
t.equal(formatDistanceBadge(null), '', 'formatDistanceBadge null → empty');
t.equal(
  formatDistanceBadge({ outcome: 'ok', distance: 0.0712,
                         matched_tracks: 12, total_mb_tracks: 12 }),
  'best 0.07 (12/12)',
  'formatDistanceBadge ok result',
);
t.equal(
  formatDistanceBadge({ outcome: 'ok', distance: 0.42,
                         matched_tracks: 8, total_mb_tracks: 12 }),
  'best 0.42 (8/12)',
  'formatDistanceBadge partial match',
);
t.equal(
  formatDistanceBadge({ outcome: 'ok', distance: 0.0 }),
  'best 0.00',
  'formatDistanceBadge no track-count metadata → distance only',
);
// Issue #1063: ``partial_read`` means the service was refused part of the
// folder, so the score covers fewer local tracks than the album holds.
// The Replace picker is where the operator chooses a pressing — a bare
// number there states a completeness we did not earn. The composed
// service->badge property lives in tests/test_protected_path_truth_generated.py.
//
// Issue #1086: the badge must also say WHICH kind of incompleteness this
// is, visibly — not only in a hover-only title attribute (the recurring
// defect this series keeps finding). A world failure (EACCES/EIO/etc.,
// ``partial_read_is_containment`` false/absent) reads "may be transient";
// a containment refusal (a symlink or socket, ``partial_read_is_containment``
// true) reads "refused: symlink or special file" — never worded like a
// flaky disk.
t.equal(
  formatDistanceBadge({ outcome: 'ok', distance: 0.0712,
                        matched_tracks: 6, total_mb_tracks: 12,
                        partial_read: '07.flac: could not be read, may be '
                          + 'transient (EACCES)' }),
  'best 0.07 (6/12) · incomplete manifest (may be transient)',
  'formatDistanceBadge flags a world-failure partial manifest, visibly',
);
t.equal(
  formatDistanceBadge({ outcome: 'ok', distance: 0.0712,
                        matched_tracks: 6, total_mb_tracks: 12,
                        partial_read: '07.flac: this is a symlink, refused '
                          + 'rather than followed out of the quarantine root',
                        partial_read_is_containment: true }),
  'best 0.07 (6/12) · incomplete manifest (refused: symlink or special file)',
  'formatDistanceBadge flags a containment refusal, visibly and distinctly',
);
t.equal(
  formatDistanceBadge({ outcome: 'ok', distance: 0.0712,
                        matched_tracks: 12, total_mb_tracks: 12,
                        partial_read: null }),
  'best 0.07 (12/12)',
  'formatDistanceBadge must still work: a complete read stays unadorned',
);
t.ok(
  distanceIsPartial({ outcome: 'ok', distance: 0.1,
                      partial_read: 'x: Input/output error' }),
  'distanceIsPartial true for a recorded refusal',
);
t.ok(
  !distanceIsPartial({ outcome: 'ok', distance: 0.1, partial_read: '' }),
  'distanceIsPartial false for an empty reason',
);
t.ok(!distanceIsPartial(null), 'distanceIsPartial false for no result');

// distanceIncompleteQualifier — the structured discriminator, not a
// substring match on the free-text reason.
t.equal(
  distanceIncompleteQualifier(null), '',
  'distanceIncompleteQualifier empty for no result',
);
t.equal(
  distanceIncompleteQualifier({ outcome: 'ok', distance: 0.1, partial_read: null }),
  '',
  'distanceIncompleteQualifier empty for a complete read',
);
t.equal(
  distanceIncompleteQualifier({
    outcome: 'ok', distance: 0.1, partial_read: 'x: Permission denied',
  }),
  'may be transient',
  'distanceIncompleteQualifier defaults to world-failure wording',
);
t.equal(
  distanceIncompleteQualifier({
    outcome: 'ok', distance: 0.1, partial_read: 'x: symlink refused',
    partial_read_is_containment: true,
  }),
  'refused: symlink or special file',
  'distanceIncompleteQualifier reads the containment discriminator',
);
t.equal(
  distanceIncompleteQualifier({
    outcome: 'ok', distance: 0.1, partial_read: 'x: Permission denied',
    partial_read_is_containment: false,
  }),
  'may be transient',
  'distanceIncompleteQualifier: false is still world-failure wording',
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
  t.equal(results.length, items.length,
    'runWithConcurrency preserves count');
  for (let i = 0; i < items.length; i++) {
    t.equal(results[i], items[i] * 10,
      `runWithConcurrency preserves index ${i}`);
  }
  t.ok(peak <= 3,
    `runWithConcurrency caps in-flight workers (peak=${peak})`);
}
{
  // limit larger than item count — still completes correctly
  const results = await runWithConcurrency([1, 2], 99, async (n) => n + 100);
  t.equal(results[0], 101, 'runWithConcurrency oversize limit ok [0]');
  t.equal(results[1], 102, 'runWithConcurrency oversize limit ok [1]');
}
{
  // empty input — resolves immediately
  const results = await runWithConcurrency([], 4, async () => 'never-called');
  t.equal(results.length, 0, 'runWithConcurrency [] → []');
}

t.equal(replaceEsc('<script>'), '&lt;script&gt;', 'esc escapes <');
t.equal(replaceEsc('a&b'), 'a&amp;b', 'esc escapes &');
t.equal(replaceEsc('"x"'), '&quot;x&quot;', 'esc escapes "');

// mapDiscogsMasterReleases (the client-side MB/Discogs release-group shape
// adapter) and its dedicated coverage are deleted (#501 item 1) — GET
// /api/release-group/<id> now dispatches numeric ids to the Discogs master
// endpoint server-side (web/routes/browse.py::get_release_group) and
// returns the identical shape, so replace_picker.js's runStandard needs no
// anchor-shape branch or client-side mapping at all. Parity is proven
// server-side by
// tests/web/test_routes_browse.py::test_release_group_numeric_id_forwards_to_discogs.

// renderMasterlessNote — R2/AE1 copy, pure/DOM-free.
t.contains(renderMasterlessNote().toLowerCase(), 'no other pressings',
  'renderMasterlessNote explains there is nothing to swap to');

// extractTracklist — GET /api/release/<id> (or /api/discogs/release/<id>,
// same shape) payload → the tracklist array `fetchTracklist` hands to
// renderTracklist. No MB/Discogs branch needed: both backends already
// emit identical track objects (disc_number/track_number/title/
// length_seconds), proven server-side by
// test_release_detail_numeric_id_forwards_to_discogs.
const discogsReleasePayload = {
  id: '1122334',
  title: 'Hold Your Colour (CD, Album)',
  tracks: [
    { disc_number: 1, track_number: 1, title: 'Slam', length_seconds: 245 },
    { disc_number: 1, track_number: 2, title: 'Voyager', length_seconds: 260 },
  ],
};
const extracted = extractTracklist(discogsReleasePayload);
t.equal(extracted.length, 2, 'extractTracklist preserves track count');
t.equal(extracted[0].title, 'Slam', 'extractTracklist preserves track title');
t.contains(renderTracklist(extracted), 'Slam',
  'extractTracklist output renders through the existing renderTracklist');
t.equal(extractTracklist({}).length, 0, 'extractTracklist({}) → []');
t.equal(extractTracklist(null).length, 0, 'extractTracklist(null) → []');

// Anchor-shape dispatch test removed (#501 item 1) — replace_picker.js no
// longer branches on the anchor's shape client-side; detectSource's own
// exhaustive coverage lives at the top of this file.

// renderReplaceButton (release_actions.js) — U9
import { renderReplaceButton } from '../web/js/release_actions.js';

import { element, stubGlobals, suite } from './js_harness.mjs';

const stdBtn = renderReplaceButton({
  mode: 'standard',
  sourceRequestId: 4194,
  releaseGroupId: 'rg-1',
  sourceLabel: 'Pet Grief — Old',
}, { stopPropagation: true });
t.contains(stdBtn, 'window.openReplacePicker',
  'renderReplaceButton standard wires through window.openReplacePicker');
t.contains(stdBtn, 'sourceRequestId: 4194',
  'renderReplaceButton standard carries sourceRequestId');
t.contains(stdBtn, 'releaseGroupId',
  'renderReplaceButton standard carries releaseGroupId');

const invEnabled = renderReplaceButton({
  mode: 'inverted',
  targetMbid: 'new-mbid',
  releaseGroupId: 'rg-1',
  targetLabel: 'Pet Grief — New',
}, { enabled: true });
t.contains(invEnabled, 'targetMbid',
  'renderReplaceButton inverted enabled wires targetMbid');
t.excludes(invEnabled, 'disabled',
  'renderReplaceButton inverted enabled is not disabled');

const invDisabled = renderReplaceButton({
  mode: 'inverted',
  targetMbid: 'new-mbid',
  releaseGroupId: 'rg-1',
  targetLabel: 'Pet Grief — New',
}, { enabled: false });
t.contains(invDisabled, 'disabled',
  'renderReplaceButton inverted disabled carries disabled attr');
t.excludes(invDisabled, 'window.openReplacePicker',
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
t.contains(stdNullRg, 'window.openReplacePicker',
  'renderReplaceButton standard renders with null releaseGroupId');
t.contains(stdNullRg, 'releaseGroupId: null',
  'renderReplaceButton standard encodes null RG as JS null literal');

const invNullRg = renderReplaceButton({
  mode: 'inverted',
  targetMbid: 'new-mbid',
  releaseGroupId: null,
  targetLabel: 'Pet Grief — New',
}, { enabled: true });
t.contains(invNullRg, 'window.openReplacePicker',
  'renderReplaceButton inverted renders with null releaseGroupId');
t.contains(invNullRg, 'releaseGroupId: null',
  'renderReplaceButton inverted encodes null RG as JS null literal');

// Standard mode without sourceRequestId still returns empty.
const stdNoSource = renderReplaceButton({
  mode: 'standard',
  releaseGroupId: 'rg-1',
});
t.equal(stdNoSource, '',
  'renderReplaceButton standard returns empty without sourceRequestId');

// Active-RG Set lookup — U9 enable logic
const activeRgSet = new Set(['rg-1', 'rg-2']);
t.equal(activeRgSet.has('rg-1'), true, 'active-RG Set hit');
t.equal(activeRgSet.has('rg-not-active'), false, 'active-RG Set miss');

// --- long_tail.js pure helpers (U3) ---
t.section('long_tail.js pure helpers');
{
  // A mixed cohort spanning Missing + several on-disk bands, deliberately
  // out of canonical order so the ordering assertion is meaningful.
  const cohort = [
    { id: 1, artist_name: 'Mount Eerie', album_title: 'Clear Moon', band: 'transparent',
      mb_release_id: '89ad4ac3-39f7-470e-963a-56509c546377', track_count: 9 },
    { id: 2, artist_name: 'The Mountain Goats', album_title: 'Tallahassee', band: 'missing',
      mb_release_id: '2048516', discogs_release_id: '2048516', track_count: 14 },
    { id: 3, artist_name: 'Bill Callahan', album_title: 'Apocalypse', band: 'poor' },
    { id: 4, artist_name: 'Smog', album_title: 'Knock Knock', band: 'missing' },
    { id: 5, artist_name: 'Grouper', album_title: 'Dragging a Dead Deer', band: 'unknown' },
    { id: 6, artist_name: 'Tim Hecker', album_title: 'Ravedeath, 1972', band: 'transparent' },
    { id: 7, artist_name: 'Loscil', album_title: 'Submers', band: 'lossless' },
  ];

  // --- bandLabel ---
  t.equal(bandLabel('missing'), 'Missing', 'bandLabel capitalises missing');
  t.equal(bandLabel('transparent'), 'Transparent', 'bandLabel capitalises transparent');
  t.equal(bandLabel(''), '?', 'bandLabel empty -> ?');
  t.equal(bandLabel(null), '?', 'bandLabel null -> ?');
  t.equal(bandLabel('LOSSLESS'), 'Lossless', 'bandLabel lower-cases then capitalises');

  // --- deriveBandTabs: ordering Missing-first + ascending QualityRank ---
  const tabs = deriveBandTabs(cohort);
  t.equal(
    tabs.map((t) => t.band).join(','),
    'missing,unknown,poor,transparent,lossless',
    'deriveBandTabs orders Missing first, then ascending by rank (only present bands)',
  );
  // Counts are correct per band.
  const countOf = (b) => (tabs.find((t) => t.band === b) || {}).count;
  t.equal(countOf('missing'), 2, 'deriveBandTabs counts Missing rows');
  t.equal(countOf('transparent'), 2, 'deriveBandTabs counts Transparent rows');
  t.equal(countOf('poor'), 1, 'deriveBandTabs counts Poor rows');
  t.equal(countOf('unknown'), 1, 'deriveBandTabs counts Unknown rows');
  t.equal(countOf('lossless'), 1, 'deriveBandTabs counts Lossless rows');
  // Bands not present in the cohort produce no tab.
  t.ok(!tabs.some((t) => t.band === 'good'), 'deriveBandTabs omits absent bands');
  // Each tab carries a display label.
  t.equal((tabs[0]).label, 'Missing', 'deriveBandTabs first tab label is Missing');
  // Empty cohort -> no tabs.
  t.equal(deriveBandTabs([]).length, 0, 'deriveBandTabs empty cohort -> no tabs');
  // Unrecognised band sorts to the end, not dropped.
  const withWeird = deriveBandTabs([
    { band: 'missing' }, { band: 'sparkle' }, { band: 'good' },
  ]);
  t.equal(
    withWeird.map((t) => t.band).join(','),
    'missing,good,sparkle',
    'deriveBandTabs sorts unrecognised band to the end',
  );

  // --- defaultBand ---
  t.equal(defaultBand(tabs), 'missing', 'defaultBand prefers Missing when present');
  t.equal(
    defaultBand(deriveBandTabs([{ band: 'good' }, { band: 'poor' }])),
    'poor',
    'defaultBand falls back to first canonical band when Missing absent',
  );
  t.equal(defaultBand([]), null, 'defaultBand empty -> null');

  // --- filterRows: within-band substring match ---
  const missingRows = filterRows(cohort, 'missing', '');
  t.equal(missingRows.length, 2, 'filterRows missing band, no query -> 2 rows');
  t.ok(
    missingRows.every((r) => r.band === 'missing'),
    'filterRows only returns rows of the selected band',
  );
  // Substring matches artist.
  const goatHits = filterRows(cohort, 'missing', 'mountain');
  t.equal(goatHits.length, 1, 'filterRows substring matches artist within band');
  t.equal(goatHits[0].id, 2, 'filterRows artist-substring hit is the right row');
  // Substring matches album, case-insensitively.
  const knockHits = filterRows(cohort, 'missing', 'KNOCK');
  t.equal(knockHits.length, 1, 'filterRows substring matches album (case-insensitive)');
  t.equal(knockHits[0].id, 4, 'filterRows album-substring hit is the right row');
  // A query that only matches rows in OTHER bands -> empty in-band result.
  t.equal(
    filterRows(cohort, 'missing', 'hecker').length,
    0,
    'filterRows cross-band query -> empty for the selected band',
  );
  // Null band -> no rows (no tab selected).
  t.equal(filterRows(cohort, null, '').length, 0, 'filterRows null band -> no rows');

  // --- countOtherBandMatches: cross-band hint count ---
  t.equal(
    countOtherBandMatches(cohort, 'missing', 'hecker'),
    1,
    'countOtherBandMatches counts matches in other bands',
  );
  t.equal(
    countOtherBandMatches(cohort, 'missing', 'eerie'),
    1,
    'countOtherBandMatches: Mount Eerie (transparent) matches "eerie" outside Missing',
  );
  // Selected-band matches are excluded from the cross-band count.
  t.equal(
    countOtherBandMatches(cohort, 'missing', 'goats'),
    0,
    'countOtherBandMatches excludes the selected band',
  );
  // Blank query -> 0 (no hint while not searching).
  t.equal(
    countOtherBandMatches(cohort, 'missing', ''),
    0,
    'countOtherBandMatches blank query -> 0',
  );

  // --- renderLongTailRow: sanity (clickable + detail container) ---
  const rowHtml = renderLongTailRow(cohort[1]);
  t.contains(
    rowHtml,
    'window.toggleLongTailDetail(2)',
    'renderLongTailRow wires the row click to toggleLongTailDetail',
  );
  t.contains(
    rowHtml,
    'id="lt-detail-2"',
    'renderLongTailRow emits the per-row detail container',
  );
  t.contains(
    rowHtml,
    'badge-wanted',
    'renderLongTailRow gives a missing row the wanted badge class',
  );
  t.contains(
    rowHtml,
    'Missing',
    'renderLongTailRow labels that chip Missing',
  );
  // Meta row = year · MB/Discogs · N tracks. cohort[1] carries a numeric
  // (Discogs) release id + 14 tracks.
  t.contains(
    rowHtml,
    'Discogs',
    'renderLongTailRow meta shows the Discogs mirror label',
  );
  t.contains(
    rowHtml,
    '14 tracks',
    'renderLongTailRow meta shows that release\u2019s track count',
  );
  // The low-signal pipeline `source` chip and unfindable_category chip are
  // gone — meta is now the pressing-disambiguation triple.
  t.excludes(
    rowHtml,
    'lt-meta-chip" title="unfindable category"',
    'renderLongTailRow no longer renders the unfindable_category chip',
  );
  // An on-disk-band row gets the rank colour class + capitalised label, and
  // a UUID (MusicBrainz) release id surfaces the MusicBrainz label.
  const transparentRow = renderLongTailRow(cohort[0]);
  t.contains(
    transparentRow,
    'badge-rank-transparent',
    'renderLongTailRow colours an on-disk row by its rank',
  );
  t.contains(
    transparentRow,
    'Transparent',
    'renderLongTailRow names that rank on the chip',
  );
  t.contains(
    transparentRow,
    'MusicBrainz',
    'renderLongTailRow meta shows the MusicBrainz mirror label for a UUID release id',
  );
  t.contains(
    transparentRow,
    '9 tracks',
    'renderLongTailRow meta shows that release\u2019s track count',
  );
  // Singular track label for a single-track pressing.
  t.contains(
    renderLongTailRow({ id: 9, artist_name: 'A', album_title: 'B', band: 'missing',
      mb_release_id: '123', track_count: 1 }), '1 track<',
    'renderLongTailRow renders singular "1 track"',
  );

  // --- renderLongTailBody: the three list states (DOM-free string paint) ---
  // Empty cohort -> empty-cohort affordance, never blank.
  state.longTail = { rows: [], band: null, query: '' };
  const emptyCohort = renderLongTailBody();
  t.contains(
    emptyCohort,
    'No wanted releases in the long tail',
    'renderLongTailBody empty cohort shows the empty-cohort affordance',
  );
  // Populated cohort -> tab strip + rows for the default (Missing) band.
  state.longTail = { rows: cohort, band: null, query: '' };
  const populated = renderLongTailBody();
  t.contains(
    populated,
    'lt-band-tabs',
    'renderLongTailBody renders band tabs for a populated cohort',
  );
  t.contains(
    populated,
    'lt-search-input',
    'renderLongTailBody renders the search box for a populated cohort',
  );
  t.contains(
    populated, 
      `lt-band-tab active-status" type="button" onclick="window.setLongTailBand('missing')`,
    'renderLongTailBody renders Missing as the default active tab',
  );
  t.equal(
    state.longTail.band, null,
    'renderLongTailBody is pure — it does not mutate state.longTail.band',
  );
  // Empty-band -> a search filters the selected band to zero; the
  // affordance + cross-band hint show, never a blank area.
  state.longTail = { rows: cohort, band: 'missing', query: 'hecker' };
  const emptyBand = renderLongTailBody();
  t.contains(
    emptyBand,
    'No Missing releases match',
    'renderLongTailBody empty-band shows the per-band no-match affordance',
  );
  t.contains(
    emptyBand,
    '1 match in other bands',
    'renderLongTailBody empty-band surfaces the cross-band match hint',
  );
  // Reset shared state so later tests are not affected.
  state.longTail = { rows: null, band: null, query: '' };
}

// --- long_tail_console.js action console pure helpers (U4) ---
t.section('long_tail_console.js pure helpers (U4 console)');
{
  // --- renderUnfindableBody: categorised vs not-yet-categorised ---
  // Categorised → category badge + forensics rollup.
  const categorised = renderUnfindableBody({
    unfindable: { category: 'wrong_pressing_available', categorised_at: '2026-05-20T00:00:00Z',
      last_artist_probe_match_count: 3, last_artist_probe_at: '2026-05-21T00:00:00Z' },
    search_forensics: { total_searches: 40, with_cands_count: 12, zero_results_count: 5,
      dominant_rejection_reason: 'strict_count', last_search_at: '2026-05-22T00:00:00Z' },
  });
  t.contains(categorised, 'wrong_pressing_available',
    'renderUnfindableBody renders the category for a categorised request');
  t.contains(categorised, '40 searches',
    'renderUnfindableBody rolls up the search count');
  t.contains(categorised, 'dominant reject: strict_count',
    'renderUnfindableBody rolls up the dominant reject reason');
  t.contains(categorised, 'artist probe: 3 matches',
    'renderUnfindableBody renders the artist-probe rollup');
  // Not-yet-categorised (unfindable == null) → daily-detection state, NOT
  // an error, NOT blank (R7).
  const uncategorised = renderUnfindableBody({
    unfindable: null,
    search_forensics: { total_searches: 2, with_cands_count: 0, zero_results_count: 2 },
  });
  t.contains(uncategorised, 'not yet categorised',
    'renderUnfindableBody names the not-yet-categorised state');
  t.contains(uncategorised, 'detection runs daily',
    'renderUnfindableBody says when detection will categorise it');
  t.excludes(uncategorised.toLowerCase(), "couldn't load",
    'not-yet-categorised is distinct from an error affordance');
  // category explicitly absent on the unfindable struct also → uncategorised.
  const catNull = renderUnfindableBody({ unfindable: { category: null }, search_forensics: {} });
  t.contains(catNull, 'not yet categorised',
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
  t.equal(youtubeHistoryRows(mixedHistory).length, 2,
    'youtubeHistoryRows keeps only source==="youtube" rows');
  t.equal(youtubeHistoryRows([]).length, 0, 'youtubeHistoryRows empty → []');
  t.equal(youtubeHistoryRows(null).length, 0, 'youtubeHistoryRows null → []');

  // --- youtubeFailureReason: reads the production-shaped reason field ---
  t.equal(
    youtubeFailureReason({ youtube_metadata: { reason: 'track_count_mismatch' } }),
    'track_count_mismatch',
    'youtubeFailureReason reads youtube_metadata.reason (the production field)');
  t.equal(
    youtubeFailureReason({ error_message: 'yt-dlp died' }),
    'yt-dlp died',
    'youtubeFailureReason falls back to error_message');
  t.equal(
    youtubeFailureReason({ verdict: 'rejected' }),
    'rejected',
    'youtubeFailureReason falls back to verdict');
  t.equal(
    youtubeFailureReason({}),
    'unknown',
    'youtubeFailureReason → "unknown" when no reason field present');

  // --- renderRescuesBody: running / failed / success / none ---
  // Active youtube_running row → "rescue running".
  const running = renderRescuesBody(
    [{ source: 'youtube', outcome: 'youtube_running', created_at: '2026-05-26T00:00:00Z' }], false);
  t.contains(running, 'rescue running', 'renderRescuesBody shows "rescue running" for an active youtube_running row');
  // in_flight flag alone (no history) → "rescue running" (KTD4 same predicate).
  t.contains(renderRescuesBody([], true), 'rescue running',
    'renderRescuesBody honours the in_flight_rescue flag with no history');
  // Latest terminal youtube_failed → "last rescue failed: <reason>".
  // Reason comes from the production-shaped youtube_metadata.reason blob.
  const failed = renderRescuesBody(
    [{ source: 'youtube', outcome: 'youtube_failed', created_at: '2026-05-25T00:00:00Z',
       youtube_metadata: { reason: 'track_count_mismatch' } }], false);
  t.contains(failed, 'last rescue failed',
    'renderRescuesBody marks a terminal youtube_failed row as failed');
  t.contains(failed, 'track_count_mismatch',
    'renderRescuesBody shows that row\u2019s failure reason from youtube_metadata');
  // The importer records a linked failure as source=youtube, outcome=failed.
  // It is newer than the canonical handoff's older youtube_success origin and
  // must win as the terminal rescue result with its persisted diagnosis.
  const importerFailed = renderRescuesBody([
    { source: 'youtube', outcome: 'failed', created_at: '2026-05-27T00:00:00Z',
      source_download_log_id: 42,
      error_message: 'YouTube import attempt failed: beets acknowledgement was ambiguous' },
    { source: 'youtube', outcome: 'youtube_success', created_at: '2026-05-26T00:00:00Z' },
  ], false);
  t.contains(importerFailed, 'last rescue failed',
    'renderRescuesBody treats the newest linked importer failure as terminal');
  t.contains(importerFailed,
    'YouTube import attempt failed: beets acknowledgement was ambiguous',
    'renderRescuesBody carries that importer failure\u2019s own detail');
  t.excludes(importerFailed, 'youtube_success',
    'renderRescuesBody does not fall back to the older canonical handoff after an importer failure');
  // A terminal youtube_success is NOT a failure (distinct from youtube_failed).
  const succeeded = renderRescuesBody(
    [{ source: 'youtube', outcome: 'youtube_success', created_at: '2026-05-24T00:00:00Z' }], false);
  t.excludes(succeeded, 'last rescue failed',
    'renderRescuesBody does NOT render a failure for a youtube_success row');
  t.contains(succeeded, 'youtube_success',
    'renderRescuesBody lists a youtube_success attempt');
  // No youtube rows at all → "no rescue attempts".
  t.contains(renderRescuesBody([{ source: 'request', outcome: 'success' }], false), 'No rescue attempts',
    'renderRescuesBody shows "no rescue attempts" when there are no youtube rows');

  // --- renderPeersBody: cap + show-all toggle ---
  const fewPeers = {
    variant: 'v1', final_state: 'Completed', outcome: 'no_match',
    top_candidates: [
      { username: 'a', dir: 'x', filetype: 'flac', matched_tracks: 1, total_tracks: 1, avg_ratio: 1, missing_titles: [], file_count: 1 },
    ],
  };
  const fewHtml = renderPeersBody(fewPeers, 7);
  t.contains(fewHtml, 'p-forensic',
    'renderPeersBody under the cap renders the plain forensic block');
  t.excludes(fewHtml, 'show all',
    'renderPeersBody under the cap offers no show-all control');
  const manyCands = [];
  for (let i = 0; i < PEERS_VISIBLE_CAP + 4; i++) {
    manyCands.push({ username: `u${i}`, dir: `d${i}`, filetype: 'flac',
      matched_tracks: 1, total_tracks: 1, avg_ratio: 1, missing_titles: [], file_count: 1 });
  }
  const manyHtml = renderPeersBody(
    { variant: 'v1', final_state: 'Completed', outcome: 'no_match', top_candidates: manyCands }, 7);
  t.contains(manyHtml, `show all ${manyCands.length} peers`,
    'renderPeersBody over the cap offers a show-all toggle with the full count');
  t.contains(manyHtml, 'window.toggleLongTailPeers(7)',
    'renderPeersBody wires the show-all toggle to toggleLongTailPeers with the row id');
  t.contains(manyHtml, 'lt-peers-full',
    'renderPeersBody pre-renders the full block for the toggle');
  // Null last_search → forensic block "no data yet" (not a crash).
  t.contains(renderPeersBody(null, 7), 'No search forensic data yet',
    'renderPeersBody null last_search → forensic "no data yet"');

  // --- renderSiblingsBody: rows + empty ---
  const siblings = renderSiblingsBody({ releases: [
    { id: 'r1', title: 'Pressing A', date: '2008-01-01', country: 'US', track_count: 14, format: 'CD',
      in_library: true, library_rank: 'transparent', pipeline_status: null },
    { id: 'r2', title: 'Pressing B', date: '2000-01-01', country: 'GB', track_count: 10, format: 'CD',
      in_library: false, pipeline_status: 'wanted' },
  ] });
  t.contains(siblings, 'Pressing A', 'renderSiblingsBody renders the first sibling pressing');
  t.contains(siblings, 'Pressing B', 'renderSiblingsBody renders the second sibling pressing');
  t.contains(siblings, 'in library', 'renderSiblingsBody renders the in-library badge');
  t.contains(siblings, 'badge-rank-transparent',
    'renderSiblingsBody colours that badge by rank');
  t.ok(siblings.includes('badge-wanted') || siblings.includes('wanted'),
    'renderSiblingsBody renders the pipeline status for a sibling already requested');
  t.contains(renderSiblingsBody({ releases: [] }), 'No sibling pressings',
    'renderSiblingsBody empty → "no sibling pressings"');
  t.contains(renderSiblingsBody(null), 'No sibling pressings',
    'renderSiblingsBody null → "no sibling pressings"');

  // --- renderYoutubeBody: four states (display-only matrix in U4) ---
  // never_run (null result) → Check YouTube button, no matrix.
  const ytNever = renderYoutubeBody(null, 9);
  t.contains(ytNever, 'Check YouTube',
    'renderYoutubeBody never_run renders the Check-YouTube stub');
  t.contains(ytNever, 'window.checkYoutube(9)',
    'renderYoutubeBody never_run wires that stub to window.checkYoutube');
  t.excludes(ytNever, 'lt-yt-row',
    'renderYoutubeBody never_run renders no matrix rows (no auto-resolve)');
  // resolved_with_matrix → display-only matrix rows.
  const ytMatrixHtml = renderYoutubeBody({
    outcome: 'ok', from_cache: false, youtube_releases: [
      { yt_browse_id: 'MPREb_z', year: 2008, track_count: 14, tracks: [],
        distances: [{ mbid: 'm', outcome: 'ok', distance: 0.07 }, { mbid: 'n', outcome: 'no_audio' }] },
    ],
  }, 9);
  t.contains(ytMatrixHtml, 'MPREb_z',
    'renderYoutubeBody resolved_with_matrix names the resolved browse id');
  t.contains(ytMatrixHtml, 'lt-yt-row',
    'renderYoutubeBody resolved_with_matrix renders the display-only matrix rows');
  t.contains(ytMatrixHtml, 'dist 0.070',
    'renderYoutubeBody matrix surfaces the best ok distance');
  // resolved_empty → "not on YouTube Music".
  const ytEmptyHtml = renderYoutubeBody({ outcome: 'ok', youtube_releases: [] }, 9);
  t.contains(ytEmptyHtml, 'Not on YouTube Music',
    'renderYoutubeBody resolved_empty renders the "not on YouTube Music" copy');
  // resolver_failed → error message + retry affordance. The retry button
  // is relabelled "Retry" in U5 (still wired to window.checkYoutube), so
  // assert on the retry verb + the wired handler rather than the original
  // "Check YouTube" label.
  const ytFailedHtml = renderYoutubeBody({ outcome: 'transient', error_message: 'mirror down' }, 9);
  t.contains(ytFailedHtml, 'mirror down',
    'renderYoutubeBody resolver_failed shows the resolver error');
  t.contains(ytFailedHtml, 'Retry',
    'renderYoutubeBody resolver_failed offers a retry');
  t.contains(ytFailedHtml, 'window.checkYoutube(9)',
    'renderYoutubeBody resolver_failed wires that retry to window.checkYoutube');
  // staleness flag on a cached matrix.
  const ytStaleHtml = renderYoutubeBody({
    outcome: 'ok', from_cache: true, error_message: 'live fetch failed',
    youtube_releases: [{ yt_browse_id: 'b', track_count: 1, tracks: [], distances: [] }],
  }, 9);
  t.contains(ytStaleHtml, 'lt-yt-stale',
    'renderYoutubeBody surfaces a staleness flag on a cached-but-stale matrix');

  // --- renderConsoleShell: band-aware emphasis + panel containers ---
  // Missing row → why-unfindable leads; the per-panel containers exist.
  const missingShell = renderConsoleShell({ id: 11, band: 'missing', source: 'mb', target_format: 'lossless' });
  t.contains(missingShell, 'id="lt-panel-unfindable-11"',
    'renderConsoleShell emits the why-unfindable panel container');
  for (const panel of ['peers', 'rescues', 'siblings', 'youtube']) {
    t.contains(missingShell, `id="lt-panel-${panel}-11"`,
      `renderConsoleShell emits the ${panel} evidence-panel container`);
  }
  // Lead emphasis: the unfindable panel carries the lead class for a Missing row.
  t.ok(/lt-panel-unfindable[^"]*lt-panel-lead|lt-panel-lead[^"]*lt-panel-unfindable/.test(missingShell.replace(/\n/g, ' '))
    || missingShell.includes('lt-panel lt-panel-unfindable lt-panel-lead'),
    'renderConsoleShell makes why-unfindable the lead panel for a Missing row');
  // The YouTube panel opens in never_run (no auto-resolve) — Check button present.
  t.contains(missingShell, 'window.checkYoutube(11)',
    'renderConsoleShell opens the YouTube panel in never_run (Check-YouTube stub, no auto-resolve)');
  // #398: the cohort row carries mb_release_group_id, so accept-sibling is
  // in its final ENABLED state at shell render — no detail-fetch stamp, no
  // post-hoc action-bar patch.
  const rgShell = renderConsoleShell({
    id: 11, band: 'missing', source: 'request',
    target_format: 'lossless', mb_release_group_id: 'rg-11' });
  t.contains(rgShell, 'window.longTailAcceptSibling(11)',
    'renderConsoleShell wires accept-sibling at open when the row carries an rg (#398)');
  t.match(missingShell, /lt-act-accept[^>]*disabled/,
    'renderConsoleShell disables accept-sibling at open when the row has no rg');
  // #398: a cached resolver result renders the matrix at shell time, so a
  // console restore after a list re-render doesn't reset a resolved
  // YouTube panel back to never_run.
  const cachedYtShell = renderConsoleShell(
    { id: 11, band: 'missing', source: 'request' },
    { outcome: 'ok', youtube_releases: [
      { yt_browse_id: 'MPREb_x', track_count: 10, tracks: [], distances: [] }] });
  t.contains(cachedYtShell, 'Rescue from this',
    'renderConsoleShell renders a cached resolver matrix instead of never_run (#398)');
  t.contains(cachedYtShell, 'MPREb_x',
    'renderConsoleShell carries the cached matrix\u2019s own browse id');
  // On-disk row → band-vs-intent leads (R8), why-unfindable does not.
  const onDiskShell = renderConsoleShell({ id: 12, band: 'poor', source: 'mb', target_format: 'lossless' });
  t.contains(onDiskShell, 'Quality vs intent',
    'renderConsoleShell leads an on-disk row with the band-vs-intent header');
  t.contains(onDiskShell, 'lt-band-intent',
    'renderConsoleShell gives that header its band-vs-intent class');
  t.ok(onDiskShell.indexOf('lt-band-intent') < onDiskShell.indexOf('lt-panel-unfindable-12'),
    'renderConsoleShell orders band-vs-intent BEFORE why-unfindable for an on-disk row');

  // --- partial-failure render: one panel error, others still render ---
  // Simulate the loaders' per-panel catch: the siblings panel gets the
  // error affordance while the other panels render their content. The pure
  // pieces guarantee a failing panel never blanks or drops its siblings.
  const errBody = renderPanelError('sibling pressings');
  t.contains(errBody, "Couldn't load sibling pressings",
    'renderPanelError names the panel that failed');
  t.contains(errBody, 'other panels are unaffected',
    'renderPanelError says the failure is isolated to that panel');
  // Compose the shell + a per-panel error swap + a sibling content render,
  // proving the three coexist (the independent-load contract).
  const composed = renderConsoleShell({ id: 13, band: 'missing' })
    + renderUnfindableBody({ unfindable: { category: 'artist_absent' }, search_forensics: {} })
    + renderPanelError('sibling pressings');
  t.contains(composed, 'artist_absent',
    'a healthy panel keeps its content beside a failed one (independent-load contract)');
  t.contains(composed, "Couldn't load sibling pressings",
    'the failed panel keeps its error beside healthy content (independent-load contract)');
}

// --- long_tail_console.js U5 rescue flow pure helpers ---
t.section('long_tail_console.js pure helpers (U5 rescue flow)');
{
  // --- youtubeBestDistance: lowest ok distance, ignores non-ok rows ---
  t.equal(
    youtubeBestDistance({ distances: [
      { mbid: 'a', outcome: 'ok', distance: 0.21 },
      { mbid: 'b', outcome: 'ok', distance: 0.07 },
      { mbid: 'c', outcome: 'no_audio' },
    ] }),
    0.07,
    'youtubeBestDistance picks the lowest ok distance');
  t.equal(
    youtubeBestDistance({ distances: [{ mbid: 'a', outcome: 'no_audio' }] }),
    null,
    'youtubeBestDistance → null when no ok row scored');
  t.equal(youtubeBestDistance({}), null, 'youtubeBestDistance no distances → null');
  t.equal(youtubeBestDistance({ distances: null }), null, 'youtubeBestDistance null distances → null');

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
  t.equal(targets.length, 2, 'youtubeRescueTargets yields one target per release');
  t.equal(targets[0].yt_browse_id, 'MPREb_one', 'target 0 carries its browse id');
  t.equal(targets[1].yt_browse_id, 'MPREb_two', 'target 1 carries its browse id');
  t.equal(targets[0].year, 2008, 'target carries year');
  t.equal(targets[0].track_count, 14, 'target carries track_count');
  t.equal(targets[0].best_distance, 0.07, 'target carries best ok distance');
  t.equal(targets[1].best_distance, 0.19, 'second target best distance');
  // A release missing a browse id is NOT a pickable target (the submit
  // needs the id).
  const targetsWithBad = youtubeRescueTargets({
    outcome: 'ok', youtube_releases: [
      { yt_browse_id: '', year: 1999, track_count: 8, distances: [] },
      { yt_browse_id: 'MPREb_keep', year: 2001, track_count: 9, distances: [] },
    ],
  });
  t.equal(targetsWithBad.length, 1, 'youtubeRescueTargets drops a release with no browse id');
  t.equal(targetsWithBad[0].yt_browse_id, 'MPREb_keep', 'kept the release that has a browse id');

  // resolved_empty → NO rescue targets (rescue affordance hidden).
  t.equal(
    youtubeRescueTargets({ outcome: 'ok', youtube_releases: [] }).length,
    0,
    'youtubeRescueTargets: resolved_empty yields no rescue targets');
  // resolver_failed → no targets.
  t.equal(
    youtubeRescueTargets({ outcome: 'transient', error_message: 'down' }).length,
    0,
    'youtubeRescueTargets: resolver_failed yields no targets');
  // never_run (null) → no targets.
  t.equal(youtubeRescueTargets(null).length, 0, 'youtubeRescueTargets: null yields no targets');

  // --- renderYoutubeBody: matrix rows are pickable rescue targets (U5) ---
  const matrixHtml = renderYoutubeBody(resolverOk, 9);
  t.contains(matrixHtml, 'window.pickYoutubeRescue(9, ',
    'renderYoutubeBody resolved_with_matrix routes a pick through window.pickYoutubeRescue');
  for (const browseId of ['MPREb_one', 'MPREb_two']) {
    t.contains(matrixHtml, browseId,
      `renderYoutubeBody resolved_with_matrix makes ${browseId} a pickable rescue target`);
  }
  t.contains(matrixHtml, 'Rescue from this',
    'renderYoutubeBody matrix rows carry a "Rescue from this" button');
  // resolved_empty HIDES the rescue affordance (R9 — nothing to pick).
  const emptyHtml = renderYoutubeBody({ outcome: 'ok', youtube_releases: [] }, 9);
  t.excludes(emptyHtml, 'Rescue from this',
    'renderYoutubeBody resolved_empty hides the rescue affordance');
  t.contains(emptyHtml, 'Not on YouTube Music',
    'renderYoutubeBody resolved_empty says the album is not on YouTube Music');
  t.contains(emptyHtml, 'Re-check',
    'renderYoutubeBody resolved_empty offers a re-check');
  // resolver_failed → Retry affordance.
  t.contains(renderYoutubeBody({ outcome: 'transient', error_message: 'mirror down' }, 9), 'Retry',
    'renderYoutubeBody resolver_failed offers a Retry affordance');

  // --- rescueOutcomeCopy: every ingest outcome → its intended copy ---
  // accepted → success tone, "rescue queued".
  const accepted = rescueOutcomeCopy({ outcome: 'accepted', download_log_id: 42 });
  t.equal(accepted.tone, 'success', 'rescueOutcomeCopy accepted → success tone');
  t.contains(accepted.title.toLowerCase(), 'queued', 'rescueOutcomeCopy accepted title says queued');
  t.contains(accepted.detail, '42', 'rescueOutcomeCopy accepted surfaces the download_log_id');
  // in_flight → error tone, surfaces the existing download_log_id.
  const inFlight = rescueOutcomeCopy({ outcome: 'in_flight', download_log_id: 7 });
  t.equal(inFlight.tone, 'error', 'rescueOutcomeCopy in_flight → error tone');
  t.contains(inFlight.detail, 'already running',
    'rescueOutcomeCopy in_flight says a rescue is already running');
  t.contains(inFlight.detail, '7',
    'rescueOutcomeCopy in_flight surfaces the existing download_log_id');
  // wrong_state → "request changed — refresh".
  const wrongState = rescueOutcomeCopy({ outcome: 'wrong_state' });
  t.contains(wrongState.detail.toLowerCase(), 'refresh',
    'rescueOutcomeCopy wrong_state tells the operator to refresh');
  // no_resolver_mapping → explicit resolver actions.
  const noMapping = rescueOutcomeCopy({ outcome: 'no_resolver_mapping' });
  t.contains(noMapping.detail, 'Search YouTube',
    'rescueOutcomeCopy no_resolver_mapping names the search action');
  t.contains(noMapping.detail, 'Check URL',
    'rescueOutcomeCopy no_resolver_mapping names the manual URL action');
  // track_count_precheck_failed → shows the precheck mismatch detail.
  const trackMismatch = rescueOutcomeCopy({
    outcome: 'track_count_precheck_failed', detail: 'expected 14, got 10' });
  t.contains(trackMismatch.detail, 'expected 14, got 10',
    'rescueOutcomeCopy track_count_precheck_failed surfaces the mismatch detail');
  // transient → retry.
  const transient = rescueOutcomeCopy({ outcome: 'transient' });
  t.contains(transient.detail.toLowerCase(), 'retry',
    'rescueOutcomeCopy transient tells the operator to retry');
  t.equal(transient.tone, 'error', 'rescueOutcomeCopy transient → error tone');
  // request_not_found → refresh.
  t.contains(rescueOutcomeCopy({ outcome: 'request_not_found' }).detail.toLowerCase(), 'refresh',
    'rescueOutcomeCopy request_not_found tells the operator to refresh');
  // unknown outcome → generic error (never blank), surfaces the error field.
  const unknown = rescueOutcomeCopy({ outcome: 'who_knows', error: 'boom' });
  t.equal(unknown.tone, 'error', 'rescueOutcomeCopy unknown → error tone');
  t.ok(unknown.detail.length > 0, 'rescueOutcomeCopy unknown → non-blank detail');
  // null result → generic error, never throws.
  t.equal(rescueOutcomeCopy(null).tone, 'error', 'rescueOutcomeCopy null → error tone (no throw)');

  // The double-fire guard predicate is now `consoleCanStart` (part of the
  // #481 item 1 console-state consolidation) — covered in
  // tests/test_js_long_tail_console.mjs, not here.

  // --- renderRescueConfirm: reuses the .confirm-box shell ---
  const confirm = renderRescueConfirm(11, 'MPREb_x', { artist_name: 'Smog', album_title: 'Knock Knock' });
  t.contains(confirm, 'confirm-box', 'renderRescueConfirm renders the confirm-box shell');
  t.contains(confirm, 'MPREb_x', 'renderRescueConfirm carries the target browse id');
  t.contains(confirm, 'Smog', 'renderRescueConfirm labels the artist being rescued');
  t.contains(confirm, 'Knock Knock', 'renderRescueConfirm labels the album being rescued');
  t.contains(confirm, 'id="lt-rescue-confirm"', 'renderRescueConfirm wires the confirm button');
  t.contains(confirm, 'id="lt-rescue-cancel"', 'renderRescueConfirm wires the cancel button');
}

// --- long_tail_console.js U6 secondary-action pure helpers ---
t.section('long_tail_console.js pure helpers (U6 secondary actions)');
{
  // --- canAcceptSibling: MB-only predicate (KTD7) ---
  t.equal(
    canAcceptSibling({ source: 'request' }, 'rg-1'),
    true,
    'canAcceptSibling: MB request with a release group → true');
  t.equal(
    canAcceptSibling({ source: 'discogs' }, 'rg-1'),
    false,
    'canAcceptSibling: Discogs-sourced request → false (no MB↔Discogs adapter)');
  t.equal(
    canAcceptSibling({ source: 'DISCOGS' }, 'rg-1'),
    false,
    'canAcceptSibling: Discogs source is case-insensitive');
  t.equal(
    canAcceptSibling({ source: 'request' }, null),
    false,
    'canAcceptSibling: no release group → false even for an MB request');
  t.equal(
    canAcceptSibling({ source: 'request' }, ''),
    false,
    'canAcceptSibling: empty release group → false');
  t.equal(
    canAcceptSibling(null, 'rg-1'),
    false,
    'canAcceptSibling: null row → false (no throw)');

  // --- acceptDisabledReason: one-line reason, empty when enabled ---
  t.equal(
    acceptDisabledReason({ source: 'request' }, 'rg-1'),
    '',
    'acceptDisabledReason: enabled → empty string');
  t.contains(
    acceptDisabledReason({ source: 'discogs' }, 'rg-1').toLowerCase(),
    'discogs',
    'acceptDisabledReason: Discogs reason mentions Discogs');
  t.contains(
    acceptDisabledReason({ source: 'request' }, null).toLowerCase(),
    'release group',
    'acceptDisabledReason: no-rg reason mentions the missing release group');

  // --- intentToggleTarget: lossless ⇄ default toggle ---
  t.equal(
    intentToggleTarget('lossless'),
    'default',
    'intentToggleTarget: lossless target_format → toggle to default (accept floor)');
  t.equal(
    intentToggleTarget('flac'),
    'default',
    'intentToggleTarget: flac target_format reads as lossless → toggle to default');
  t.equal(
    intentToggleTarget(null),
    'lossless',
    'intentToggleTarget: no target_format (default intent) → toggle to lossless');
  t.equal(
    intentToggleTarget(''),
    'lossless',
    'intentToggleTarget: empty target_format → toggle to lossless');
  t.equal(
    intentToggleTarget('mp3'),
    'lossless',
    'intentToggleTarget: non-lossless target_format → toggle to lossless');

  // --- buildAcceptSiblingOptions: standard-mode picker options ---
  const opts = buildAcceptSiblingOptions({
    id: 77, artist_name: 'Smog', album_title: 'Knock Knock', mb_release_group_id: 'rg-9' });
  t.equal(opts.sourceRequestId, 77, 'buildAcceptSiblingOptions carries the source request id');
  t.equal(opts.releaseGroupId, 'rg-9', 'buildAcceptSiblingOptions carries the release group id');
  t.contains(opts.sourceLabel, 'Smog',
    'buildAcceptSiblingOptions puts the artist in the source label');
  t.contains(opts.sourceLabel, 'Knock Knock',
    'buildAcceptSiblingOptions puts the album in the source label');
  t.equal(
    buildAcceptSiblingOptions({ id: 5, artist_name: 'A', album_title: 'B' }).releaseGroupId,
    null,
    'buildAcceptSiblingOptions: no rg on row → null (picker lazily resolves)');

  // --- renderActionsBar: enable/disable + intent badge + triage buttons ---
  const mbBar = renderActionsBar({
    id: 12, source: 'request', mb_release_group_id: 'rg-1', target_format: null });
  t.contains(mbBar, 'window.longTailAcceptSibling(12)',
    'renderActionsBar: MB row wires the accept-sibling handler');
  t.notMatch(mbBar, /lt-act-accept[^>]*disabled/, 'renderActionsBar: MB row accept-sibling is enabled');
  t.contains(mbBar, 'window.longTailSetIntent(12, this)',
    'renderActionsBar wires the set-intent toggle to its initiating control');
  t.contains(mbBar, 'data-pipeline-request-id="12"',
    'renderActionsBar stamps the bar with the request identity');
  // Triage buttons wired to the existing update / delete endpoints.
  t.contains(mbBar, 'window.longTailSetImported(12, this)',
    'renderActionsBar wires Set imported to its initiating control');
  t.contains(mbBar, 'Set imported', 'renderActionsBar labels the Set imported button');
  t.contains(mbBar, 'window.longTailDeleteRequest(12, this)',
    'renderActionsBar wires Delete request to its initiating control');
  t.contains(mbBar, 'Delete request', 'renderActionsBar labels the Delete request button');
  // The re-search button is gone.
  t.excludes(mbBar, 'longTailReSearch',
    'renderActionsBar no longer wires a re-search handler');
  t.excludes(mbBar, 'lt-act-research',
    'renderActionsBar no longer renders the re-search button');
  // default intent (no target_format) → toggle offers "switch to lossless".
  t.contains(mbBar, 'switch to lossless',
    'renderActionsBar: default intent offers "switch to lossless"');
  t.contains(mbBar, 'lt-intent-default',
    'renderActionsBar: default intent renders the default badge');

  const discogsBar = renderActionsBar({
    id: 13, source: 'discogs', mb_release_group_id: null, target_format: 'lossless' });
  t.match(discogsBar, /lt-act-accept[^>]*disabled/, 'renderActionsBar: Discogs row disables accept-sibling');
  t.ok(discogsBar.toLowerCase().includes('musicbrainz-only')
    || discogsBar.toLowerCase().includes('discogs'),
    'renderActionsBar: Discogs row shows the one-line disable reason');
  t.excludes(discogsBar, 'window.longTailAcceptSibling(13)',
    'renderActionsBar: disabled accept-sibling does not wire the handler onclick');
  // lossless intent → badge + "accept current floor" toggle.
  t.contains(discogsBar, 'lt-intent-lossless',
    'renderActionsBar: lossless intent renders the lossless badge');
  t.contains(discogsBar, 'accept current floor',
    'renderActionsBar: lossless intent offers "accept current floor"');

  // MB row with no release group → accept-sibling disabled.
  const noRgBar = renderActionsBar({
    id: 14, source: 'request', mb_release_group_id: null, target_format: null });
  t.match(noRgBar, /lt-act-accept[^>]*disabled/,
    'renderActionsBar: MB row with no release group disables accept-sibling');
}

// --- long_tail_console.js #398 / #481 item 1 console-persistence state ---
t.section('long_tail_console.js console persistence (#398 / #481 item 1)');
{
  // Open-console tracking lives in the module-scoped `consoleStates` map
  // (#481 item 1 — no longer on shared state; see tests/test_js_long_tail_console.mjs
  // for the pure open/close/prune/canStart/settle transition coverage).
  t.ok(consoleStates instanceof Map,
    'consoleStates is a Map (open-console ids persist across re-renders)');
  // The DOM-side entry points are no-ops outside a browser — must not throw
  // when the module is imported into the Node test runner, and must not
  // fabricate console state for a row nobody opened.
  const { restoreLongTailConsoles, toggleLongTailDetail } = await import('../web/js/long_tail_console.js');
  restoreLongTailConsoles();
  toggleLongTailDetail(1);
  t.ok(consoleStates.size === 0,
    'DOM-side no-ops leave consoleStates untouched in Node');
}

{
  // #865: the card's Distance cell appends the apply-time distance (#863)
  // only when one was persisted — legacy rows render unchanged.
  t.equal(withApplyDistance('0.082', null), '0.082',
    'null apply distance leaves the cell unchanged');
  t.equal(withApplyDistance('0.082', undefined), '0.082',
    'undefined apply distance leaves the cell unchanged');
  t.equal(withApplyDistance('—', null), '—',
    'em-dash cell unchanged without apply distance');
  t.equal(withApplyDistance('0.082', 0.5637),
    '0.082 <span class="p-hist-was">· apply 0.564</span>',
    'numeric apply distance appends the suffix');
  t.equal(withApplyDistance('—', '0.5637'),
    '— <span class="p-hist-was">· apply 0.564</span>',
    'string apply distance parses and appends');
  t.equal(withApplyDistance('0.082', 'not-a-number'), '0.082',
    'unparseable apply distance leaves the cell unchanged');
}

{
  // #924: behind an operator's external authorizer, an expired session is
  // answered by that component, not by Cratedigger. `fetch` follows the
  // portal redirect transparently, so the call site sees ok=true with an
  // HTML body and reports a generic load failure. Cratedigger itself never
  // emits 401 and never sends a Location header, so both signals below are
  // unambiguously someone else's response.
  t.ok(isExternalAuthInterruption({ status: 401, redirected: false }),
    '401 is an external authorizer — Cratedigger never emits one');
  t.ok(isExternalAuthInterruption({ status: 200, redirected: true }),
    'a followed redirect is an external authorizer — the app never redirects');
  t.ok(isExternalAuthInterruption({ status: 200, redirected: true, url: 'https://auth.example.test/' }),
    'a cross-origin portal redirect is an interruption');
  t.ok(!isExternalAuthInterruption({ status: 200, redirected: false }),
    'an ordinary successful response is not an interruption');
  t.ok(!isExternalAuthInterruption({ status: 404, redirected: false }),
    'a genuine application 404 is not an interruption');
  t.ok(!isExternalAuthInterruption({ status: 403, redirected: false }),
    'the application provenance 403 is not an interruption');
  t.ok(!isExternalAuthInterruption({ status: 500, redirected: false }),
    'an application error is not an interruption');
  t.ok(!isExternalAuthInterruption(null),
    'a missing response is not an interruption');
  t.ok(!isExternalAuthInterruption(undefined),
    'an undefined response is not an interruption');
  t.ok(!isExternalAuthInterruption({}),
    'a response with no status or redirect flag is not an interruption');
}

{
  // #1099: the Wrong Matches file explorer's whole-root load-failure catch
  // used to say "Failed to load file explorer" for every non-ok status —
  // a 404 (definitive absence), a 422 (a containment decision a retry can
  // never satisfy) and a 503 (a retryable world failure) all got the exact
  // same wording. wrongMatchExplorerFailureCopy is the pure function that
  // turns the status into honest, status-specific copy.
  t.contains(wrongMatchExplorerFailureCopy(404, null), 'not',
    '404 gets an absence-family lead sentence');
  t.equal(wrongMatchExplorerFailureCopy(404, null),
    'This wrong-match folder could not be located.',
    '404 with no server detail appends nothing extra');
  t.equal(wrongMatchExplorerFailureCopy(404, 'Wrong-match files not found: /x'),
    'This wrong-match folder could not be located. Wrong-match files not found: /x',
    '404 appends the server detail');

  const refused = wrongMatchExplorerFailureCopy(422, null);
  t.contains(refused.toLowerCase(), 'refused',
    '422 gets a containment-family lead sentence naming the refusal');
  t.excludes(refused.toLowerCase(), 'not found',
    '422 copy must never say "not found" — the name may well exist');

  // Review round 1: the 503 bucket also carries the unclassified
  // residual code (a `failed_path` lexically outside every configured
  // quarantine root, for example) — a data mismatch, not a disk hiccup
  // — so the copy must not PROMISE a retry will succeed.
  const unavailable = wrongMatchExplorerFailureCopy(503, null);
  t.contains(unavailable.toLowerCase(), 'could not be read',
    '503 gets its own lead sentence (world-failure + residual family)');
  t.excludes(unavailable.toLowerCase(), 'a retry may succeed',
    '503 copy never promises a retry will succeed (residual bucket)');
  t.excludes(unavailable.toLowerCase(), 'temporarily unavailable',
    '503 copy never calls the residual bucket temporary');
  // "the storage refused or failed" uses "refused" in its ordinary
  // English sense (a generic storage-layer non-answer) — the ban is on
  // the SPECIFIC 422 containment phrase, not the bare word.
  t.excludes(unavailable.toLowerCase(), 'containment decision',
    '503 must not borrow the specific containment-decision wording');

  t.equal(wrongMatchExplorerFailureCopy(200, null),
    'Failed to load file explorer.',
    'an unrecognized status falls back to the generic sentence');
  t.equal(wrongMatchExplorerFailureCopy(undefined, null),
    'Failed to load file explorer.',
    'a missing status (e.g. a thrown non-HTTP error) falls back to the generic sentence');
  t.equal(wrongMatchExplorerFailureCopy(undefined, 'network error'),
    'Failed to load file explorer. network error',
    'the fallback still appends whatever detail is available');
}

{
  // #924: the session guard's own behaviour. The DOM is the external edge
  // here, so it is faked; the guard, the predicate, and the overlay builder
  // are the real production functions.
  function fakeDocument() {
    const byId = new Map();
    let reloads = 0;
    const makeElement = (tag) => element({
      tag,
      type: '',
      listeners: {},
      addEventListener(name, fn) { this.listeners[name] = fn; },
      append(...kids) { this.children.push(...kids); },
    });
    const doc = {
      body: makeElement('body'),
      location: { reload() { reloads += 1; } },
      createElement: makeElement,
      getElementById: (id) => byId.get(id) || null,
      reloadCount: () => reloads,
    };
    const originalAppend = doc.body.appendChild.bind(doc.body);
    doc.body.appendChild = (kid) => { byId.set(kid.id, kid); return originalAppend(kid); };
    return doc;
  }

  const okResponse = { status: 200, redirected: false };
  const expiredResponse = { status: 200, redirected: true };

  // An ordinary response passes through untouched and shows no overlay.
  {
    const doc = fakeDocument();
    const guarded = wrapFetchWithSessionGuard(async () => okResponse, doc);
    const result = await guarded('/api/pipeline/dashboard');
    t.equal(result, okResponse, 'ordinary responses pass through the guard');
    t.equal(doc.getElementById('session-expired-overlay'), null,
      'an ordinary response shows no expired-session overlay');
  }

  // An authorizer-interrupted response never reaches the call site as data.
  {
    const doc = fakeDocument();
    const guarded = wrapFetchWithSessionGuard(async () => expiredResponse, doc);
    await t.rejects(
      () => guarded('/api/pipeline/dashboard'),
      'an interrupted response rejects instead of returning HTML as data',
    );
    const overlay = doc.getElementById('session-expired-overlay');
    t.ok(overlay !== null, 'an interrupted response shows the expired-session overlay');
    t.equal(overlay.getAttribute('role'), 'alertdialog',
      'the overlay announces itself as an alert dialog');
    t.equal(overlay.getAttribute('aria-modal'), 'true',
      'the overlay is a modal for assistive technology');
  }

  // Concurrent in-flight requests must not stack overlays.
  {
    const doc = fakeDocument();
    const guarded = wrapFetchWithSessionGuard(async () => expiredResponse, doc);
    for (const _ of [0, 1, 2]) {
      try { await guarded('/api/pipeline/dashboard'); } catch (e) { /* expected */ }
    }
    t.equal(doc.body.children.length, 1,
      'repeated interruptions show exactly one overlay');
  }

  // main.js installs the guard at module scope, and the Node test runner
  // imports main.js. A window-less install must be a silent no-op or every
  // JS suite that touches main.js dies on import.
  t.equal(installSessionGuard(undefined), false,
    'installing without a window is a no-op');
  t.equal(installSessionGuard({}), false,
    'installing without fetch or document is a no-op');

  // The overlay's recovery is a document-level reload, which is the only
  // navigation that lets the external component run its redirect flow.
  {
    const doc = fakeDocument();
    const overlay = buildSessionExpiredOverlay(doc);
    const panel = overlay.children[0];
    const button = panel.children.find((child) => child.tag === 'button');
    t.ok(button !== undefined, 'the overlay offers a recovery control');
    button.listeners['click']();
    t.equal(doc.reloadCount(), 1, 'the recovery control reloads the document');
  }
}

// --- Summary ---
t.done();
