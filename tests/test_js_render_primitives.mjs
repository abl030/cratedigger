/**
 * Unit tests for web/js/render_primitives.js — the shared row / track /
 * toggle primitives (issue #575 PR3).
 * Run with: node tests/test_js_render_primitives.mjs
 */

import {
  formatDuration,
  formatTrackMeta,
  renderBeetsTrackRow,
  renderExpectedTrackRow,
  renderReleaseRow,
  renderDetailRow,
  renderExternalLinkRow,
  toggleExpand,
  toggleSection,
} from '../web/js/render_primitives.js';

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

function assertExcludes(haystack, needle, msg) {
  if (!haystack.includes(needle)) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg} - unexpectedly found '${needle}'`);
  }
}

/** Minimal element stub for toggleExpand — classList + innerHTML only. */
function fakeEl() {
  const classes = new Set();
  return {
    innerHTML: '',
    classList: {
      contains: (c) => classes.has(c),
      add: (c) => classes.add(c),
      remove: (c) => classes.delete(c),
    },
  };
}

console.log('formatDuration()');
{
  assertEqual(formatDuration(null), '', 'null -> empty');
  assertEqual(formatDuration(0), '', 'zero -> empty (falsy, matches old inline code)');
  assertEqual(formatDuration(225), '3:45', 'whole minutes/seconds');
  assertEqual(formatDuration(125), '2:05', 'seconds zero-padded');
  // The old inline copies did Math.round(len % 60) which produced '1:60'
  // for 119.7s. The primitive rounds total seconds first.
  assertEqual(formatDuration(119.7), '2:00', 'rounding carries into minutes (old code said 1:60)');
  assertEqual(formatDuration(59.6), '1:00', 'sub-minute rounding carry');
}

console.log('formatTrackMeta()');
{
  const hires = formatTrackMeta({ format: 'FLAC', bitrate: 1024000, bitdepth: 24, samplerate: 96000 });
  assertEqual(hires, 'FLAC 1024kbps 24bit 96.0kHz', 'hi-res FLAC shows depth and rate');
  const cd = formatTrackMeta({ format: 'FLAC', bitrate: 900000, bitdepth: 16, samplerate: 44100 });
  assertEqual(cd, 'FLAC 900kbps', 'CD-spec depth/rate suppressed');
  assertEqual(formatTrackMeta({ format: 'MP3', bitrate: 320000 }), 'MP3 320kbps', 'lossy format + bitrate');
  assertEqual(formatTrackMeta({}), '', 'empty track -> empty meta');
  assertEqual(formatTrackMeta({ bitrate: 320000 }), '320kbps', 'bitrate only');
}

console.log('renderBeetsTrackRow()');
{
  const html = renderBeetsTrackRow({
    disc: 2, track: 3, title: 'A <b> Song', length: 225,
    bitrate: 320000, format: 'MP3', bitdepth: 16, samplerate: 44100,
  });
  assertContains(html, 'class="lib-track"', 'row uses lib-track class');
  assertContains(html, '2.3. A &lt;b&gt; Song', 'disc prefix + escaped title');
  assertContains(html, '3:45', 'duration rendered');
  assertContains(html, 'class="lib-track-meta"', 'meta span present');
  assertContains(html, 'MP3 320kbps', 'meta content rendered');

  const disc1 = renderBeetsTrackRow({ disc: 1, track: 4, title: 'T', length: 0 });
  assertContains(disc1, '>4. T', 'no disc prefix on disc 1');
  assertExcludes(disc1, '1.4.', 'disc 1 prefix suppressed');
  assertExcludes(disc1, 'color:#555', 'no duration span when length missing');
}

console.log('renderExpectedTrackRow()');
{
  const html = renderExpectedTrackRow({
    disc_number: 2, track_number: 5, title: 'T & Co', length_seconds: 65,
  });
  assertContains(html, 'class="lib-track"', 'expected row uses lib-track class');
  assertContains(html, '2.5. T &amp; Co', 'disc prefix + escaped title');
  assertContains(html, '1:05', 'duration rendered');
  assertExcludes(html, 'lib-track-meta', 'no quality meta on expected tracks');

  const disc1 = renderExpectedTrackRow({ disc_number: 1, track_number: 9, title: 'X' });
  assertContains(disc1, '>9. X', 'no disc prefix on disc 1');

  // Discogs index/heading rows (sub-EP titles on a combined release)
  // arrive with track_number 0 — render as an unnumbered heading, not
  // "0. Feather Figure Single".
  const heading = renderExpectedTrackRow({ disc_number: 1, track_number: 0, title: 'Feather Figure Single' });
  assertExcludes(heading, '0.', 'no zero prefix on heading rows');
  assertContains(heading, 'Feather Figure Single', 'heading title rendered');
  assertContains(heading, 'lib-track-heading', 'heading rows are visually distinct');
}

console.log('renderReleaseRow()');
{
  const minimal = renderReleaseRow({ onclick: 'x()', titleHtml: 'Title' });
  assertContains(minimal, 'class="release"', 'default row class');
  assertContains(minimal, 'onclick="x()"', 'onclick attribute');
  assertContains(minimal, 'class="release-info"', 'info wrapper');
  assertContains(minimal, '<div class="release-title">Title</div>', 'title html verbatim');
  assertExcludes(minimal, 'release-meta', 'no meta line when metaLines omitted');
  assertExcludes(minimal, 'data-release-id', 'no data attr when id omitted');
  assertExcludes(minimal, 'release-detail', 'no detail div when detail omitted');

  const full = renderReleaseRow({
    rowClass: 'rg',
    dataReleaseId: 'abc"def',
    style: 'opacity:0.5;',
    onclick: 'y()',
    titleHtml: 'T',
    metaLines: ['m1', 'm2'],
    actionsHtml: '<button>b</button>',
    detail: { id: 'reldet-1' },
  });
  assertContains(full, 'class="rg"', 'row class override');
  assertContains(full, 'data-release-id="abc&quot;def"', 'data attr escaped');
  assertContains(full, 'style="opacity:0.5;"', 'style attr');
  assertContains(full, '>m1</div>', 'first meta line');
  assertContains(full, '>m2</div>', 'second meta line');
  assertEqual((full.match(/class="release-meta"/g) || []).length, 2, 'one meta div per line');
  assertContains(full, '<button>b</button>', 'actions html verbatim');
  assertContains(full, '<div class="release-detail" id="reldet-1"></div>', 'detail placeholder');

  const custom = renderReleaseRow({
    onclick: 'z()', titleHtml: 'T',
    detail: { id: 'disamb-rg-9', className: 'releases' },
  });
  assertContains(custom, '<div class="releases" id="disamb-rg-9"></div>', 'detail class override');
}

console.log('renderDetailRow()');
{
  const html = renderDetailRow('Path', '<a>x</a>');
  assertContains(html, 'class="p-detail-row"', 'row wrapper');
  assertContains(html, '<span class="p-detail-label">Path</span>', 'label span');
  assertContains(html, '<span class="p-detail-value"><a>x</a></span>', 'value html verbatim');

  const styled = renderDetailRow('A<b', 'v', { valueStyle: 'font-size:0.85em;' });
  assertContains(styled, 'A&lt;b', 'label escaped');
  assertContains(styled, '<span class="p-detail-value" style="font-size:0.85em;">v</span>', 'value style attr');
}

console.log('renderExternalLinkRow()');
{
  const mb = renderExternalLinkRow('9a7c2e1b-2f4d-4b3a-9c8d-1e2f3a4b5c6d');
  assertContains(mb, 'MusicBrainz', 'MB label');
  assertContains(mb, 'https://musicbrainz.org/release/9a7c2e1b-2f4d-4b3a-9c8d-1e2f3a4b5c6d', 'MB url');
  assertContains(mb, '9a7c2e1b...', 'truncated id as link text');
  assertContains(mb, 'target="_blank" rel="noopener"', 'link opens externally');
  assertContains(mb, 'onclick="event.stopPropagation()"',
    'link click must not bubble into the row toggle');

  const dg = renderExternalLinkRow('123456');
  assertContains(dg, 'Discogs', 'Discogs label');
  assertContains(dg, 'https://www.discogs.com/release/123456', 'Discogs url');

  assertEqual(renderExternalLinkRow('not-a-release-id'), '', 'unknown source -> empty');
  assertEqual(renderExternalLinkRow(''), '', 'empty id -> empty');
}

console.log('toggleExpand() — open, close, reload, errors');
await (async () => {
  // Closed -> open: loading placeholder shown before loader runs, loader
  // output kept after.
  const el = fakeEl();
  let sawLoadingDuringLoad = false;
  await toggleExpand(el, (target) => {
    sawLoadingDuringLoad = target.innerHTML.includes('Loading...');
    target.innerHTML = 'CONTENT';
  });
  assertEqual(sawLoadingDuringLoad, true, 'loading placeholder set before loader runs');
  assertEqual(el.classList.contains('open'), true, 'panel opened');
  assertEqual(el.innerHTML, 'CONTENT', 'loader output kept');

  // Open -> close: loader must NOT run.
  let loaderRan = false;
  await toggleExpand(el, () => { loaderRan = true; });
  assertEqual(el.classList.contains('open'), false, 'panel closed');
  assertEqual(loaderRan, false, 'loader not called on close');

  // Re-open: loader runs again (no caching — badge overlays can change
  // between opens, matching every previous per-view implementation).
  let calls = 0;
  const el2 = fakeEl();
  const counting = (target) => { calls++; target.innerHTML = 'C' + calls; };
  await toggleExpand(el2, counting);
  await toggleExpand(el2, counting); // close
  await toggleExpand(el2, counting); // open again
  assertEqual(calls, 2, 'loader runs on every open');
  assertEqual(el2.innerHTML, 'C2', 'second open re-rendered');

  // Async loader rejection -> default error placeholder, panel stays open.
  const el3 = fakeEl();
  await toggleExpand(el3, async () => { throw new Error('boom'); });
  assertContains(el3.innerHTML, 'Failed to load', 'default error text');
  assertEqual(el3.classList.contains('open'), true, 'panel stays open on error');

  // Custom error text (pipeline detail says "Failed to load details").
  const el4 = fakeEl();
  await toggleExpand(el4, () => { throw new Error('boom'); },
    { errorText: 'Failed to load details' });
  assertContains(el4.innerHTML, 'Failed to load details', 'custom error text');

  // Null element: no crash, no throw.
  await toggleExpand(null, () => {});
  passed++;
})();

console.log('toggleSection()');
{
  const classes = new Set();
  const header = {
    nextElementSibling: {
      classList: {
        toggle: (c) => (classes.has(c) ? classes.delete(c) : classes.add(c)),
      },
    },
  };
  toggleSection(header);
  assertEqual(classes.has('open'), true, 'first call opens');
  toggleSection(header);
  assertEqual(classes.has('open'), false, 'second call closes');
  toggleSection({ nextElementSibling: null });
  passed++; // no crash on missing sibling
}

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed > 0 ? 1 : 0);
