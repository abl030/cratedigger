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

import { suite } from './js_harness.mjs';

const t = suite(import.meta.url);

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

t.section('formatDuration()');
{
  t.equal(formatDuration(null), '', 'null -> empty');
  t.equal(formatDuration(0), '', 'zero -> empty (falsy, matches old inline code)');
  t.equal(formatDuration(225), '3:45', 'whole minutes/seconds');
  t.equal(formatDuration(125), '2:05', 'seconds zero-padded');
  // The old inline copies did Math.round(len % 60) which produced '1:60'
  // for 119.7s. The primitive rounds total seconds first.
  t.equal(formatDuration(119.7), '2:00', 'rounding carries into minutes (old code said 1:60)');
  t.equal(formatDuration(59.6), '1:00', 'sub-minute rounding carry');
}

t.section('formatTrackMeta()');
{
  const hires = formatTrackMeta({ format: 'FLAC', bitrate: 1024000, bitdepth: 24, samplerate: 96000 });
  t.equal(hires, 'FLAC 1024kbps 24bit 96.0kHz', 'hi-res FLAC shows depth and rate');
  const cd = formatTrackMeta({ format: 'FLAC', bitrate: 900000, bitdepth: 16, samplerate: 44100 });
  t.equal(cd, 'FLAC 900kbps', 'CD-spec depth/rate suppressed');
  t.equal(formatTrackMeta({ format: 'MP3', bitrate: 320000 }), 'MP3 320kbps', 'lossy format + bitrate');
  t.equal(formatTrackMeta({}), '', 'empty track -> empty meta');
  t.equal(formatTrackMeta({ bitrate: 320000 }), '320kbps', 'bitrate only');
}

t.section('renderBeetsTrackRow()');
{
  const html = renderBeetsTrackRow({
    disc: 2, track: 3, title: 'A <b> Song', length: 225,
    bitrate: 320000, format: 'MP3', bitdepth: 16, samplerate: 44100,
  });
  t.contains(html, 'class="lib-track"', 'row uses lib-track class');
  t.contains(html, '2.3. A &lt;b&gt; Song', 'disc prefix + escaped title');
  t.contains(html, '3:45', 'duration rendered');
  t.contains(html, 'class="lib-track-meta"', 'meta span present');
  t.contains(html, 'MP3 320kbps', 'meta content rendered');

  const hostileFormat = '</span><img src=x onerror=alert(1)>';
  const hostile = renderBeetsTrackRow({
    track: 5, title: 'T', format: hostileFormat,
  });
  t.contains(hostile, '&lt;/span&gt;&lt;img src=x onerror=alert(1)&gt;',
    'format is escaped at the shared track-row HTML boundary');
  t.excludes(hostile, hostileFormat,
    'raw track format cannot close the metadata span');

  const disc1 = renderBeetsTrackRow({ disc: 1, track: 4, title: 'T', length: 0 });
  t.contains(disc1, '>4. T', 'no disc prefix on disc 1');
  t.excludes(disc1, '1.4.', 'disc 1 prefix suppressed');
  t.excludes(disc1, 'color:#555', 'no duration span when length missing');
}

t.section('renderExpectedTrackRow()');
{
  const html = renderExpectedTrackRow({
    disc_number: 2, track_number: 5, title: 'T & Co', length_seconds: 65,
  });
  t.contains(html, 'class="lib-track"', 'expected row uses lib-track class');
  t.contains(html, '2.5. T &amp; Co', 'disc prefix + escaped title');
  t.contains(html, '1:05', 'duration rendered');
  t.excludes(html, 'lib-track-meta', 'no quality meta on expected tracks');

  const disc1 = renderExpectedTrackRow({ disc_number: 1, track_number: 9, title: 'X' });
  t.contains(disc1, '>9. X', 'no disc prefix on disc 1');

  // Discogs index/heading rows (sub-EP titles on a combined release)
  // arrive with track_number 0 — render as an unnumbered heading, not
  // "0. Feather Figure Single".
  const heading = renderExpectedTrackRow({ disc_number: 1, track_number: 0, title: 'Feather Figure Single' });
  t.excludes(heading, '0.', 'no zero prefix on heading rows');
  t.contains(heading, 'Feather Figure Single', 'heading title rendered');
  t.contains(heading, 'lib-track-heading', 'heading rows are visually distinct');
}

t.section('renderReleaseRow()');
{
  const minimal = renderReleaseRow({ onclick: 'x()', titleHtml: 'Title' });
  t.contains(minimal, 'class="release"', 'default row class');
  t.contains(minimal, 'onclick="x()"', 'onclick attribute');
  t.contains(minimal, 'class="release-info"', 'info wrapper');
  t.contains(minimal, '<div class="release-title">Title</div>', 'title html verbatim');
  t.excludes(minimal, 'release-meta', 'no meta line when metaLines omitted');
  t.excludes(minimal, 'data-release-id', 'no data attr when id omitted');
  t.excludes(minimal, 'release-detail', 'no detail div when detail omitted');

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
  t.contains(full, 'class="rg"', 'row class override');
  t.contains(full, 'data-release-id="abc&quot;def"', 'data attr escaped');
  t.contains(full, 'style="opacity:0.5;"', 'style attr');
  t.contains(full, '>m1</div>', 'first meta line');
  t.contains(full, '>m2</div>', 'second meta line');
  t.equal((full.match(/class="release-meta"/g) || []).length, 2, 'one meta div per line');
  t.contains(full, '<button>b</button>', 'actions html verbatim');
  t.contains(full, '<div class="release-detail" id="reldet-1"></div>', 'detail placeholder');

  const custom = renderReleaseRow({
    onclick: 'z()', titleHtml: 'T',
    detail: { id: 'disamb-rg-9', className: 'releases' },
  });
  t.contains(custom, '<div class="releases" id="disamb-rg-9"></div>', 'detail class override');
}

t.section('renderDetailRow()');
{
  const html = renderDetailRow('Path', '<a>x</a>');
  t.contains(html, 'class="p-detail-row"', 'row wrapper');
  t.contains(html, '<span class="p-detail-label">Path</span>', 'label span');
  t.contains(html, '<span class="p-detail-value"><a>x</a></span>', 'value html verbatim');

  const styled = renderDetailRow('A<b', 'v', { valueStyle: 'font-size:0.85em;' });
  t.contains(styled, 'A&lt;b', 'label escaped');
  t.contains(styled, '<span class="p-detail-value" style="font-size:0.85em;">v</span>', 'value style attr');
}

t.section('renderExternalLinkRow()');
{
  const mb = renderExternalLinkRow('9a7c2e1b-2f4d-4b3a-9c8d-1e2f3a4b5c6d');
  t.contains(mb, 'MusicBrainz', 'MB label');
  t.contains(mb, 'https://musicbrainz.org/release/9a7c2e1b-2f4d-4b3a-9c8d-1e2f3a4b5c6d', 'MB url');
  t.contains(mb, '9a7c2e1b...', 'truncated id as link text');
  t.contains(mb, 'target="_blank" rel="noopener"', 'link opens externally');
  t.contains(mb, 'onclick="event.stopPropagation()"',
    'link click must not bubble into the row toggle');

  const dg = renderExternalLinkRow('123456');
  t.contains(dg, 'Discogs', 'Discogs label');
  t.contains(dg, 'https://www.discogs.com/release/123456', 'Discogs url');

  t.equal(renderExternalLinkRow('not-a-release-id'), '', 'unknown source -> empty');
  t.equal(renderExternalLinkRow(''), '', 'empty id -> empty');
}

t.section('toggleExpand() — open, close, reload, errors');
await (async () => {
  // Closed -> open: loading placeholder shown before loader runs, loader
  // output kept after.
  const el = fakeEl();
  let sawLoadingDuringLoad = false;
  await toggleExpand(el, (target) => {
    sawLoadingDuringLoad = target.innerHTML.includes('Loading...');
    target.innerHTML = 'CONTENT';
  });
  t.equal(sawLoadingDuringLoad, true, 'loading placeholder set before loader runs');
  t.equal(el.classList.contains('open'), true, 'panel opened');
  t.equal(el.innerHTML, 'CONTENT', 'loader output kept');

  // Open -> close: loader must NOT run.
  let loaderRan = false;
  await toggleExpand(el, () => { loaderRan = true; });
  t.equal(el.classList.contains('open'), false, 'panel closed');
  t.equal(loaderRan, false, 'loader not called on close');

  // Re-open: loader runs again (no caching — badge overlays can change
  // between opens, matching every previous per-view implementation).
  let calls = 0;
  const el2 = fakeEl();
  const counting = (target) => { calls++; target.innerHTML = 'C' + calls; };
  await toggleExpand(el2, counting);
  await toggleExpand(el2, counting); // close
  await toggleExpand(el2, counting); // open again
  t.equal(calls, 2, 'loader runs on every open');
  t.equal(el2.innerHTML, 'C2', 'second open re-rendered');

  // Async loader rejection -> default error placeholder, panel stays open.
  const el3 = fakeEl();
  await toggleExpand(el3, async () => { throw new Error('boom'); });
  t.contains(el3.innerHTML, 'Failed to load', 'default error text');
  t.equal(el3.classList.contains('open'), true, 'panel stays open on error');

  // Custom error text (pipeline detail says "Failed to load details").
  const el4 = fakeEl();
  await toggleExpand(el4, () => { throw new Error('boom'); },
    { errorText: 'Failed to load details' });
  t.contains(el4.innerHTML, 'Failed to load details', 'custom error text');

  // Null element: no crash, no throw.
  await toggleExpand(null, () => {});
  t.pass('a null element neither crashes nor throws');
})();

t.section('toggleSection()');
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
  t.equal(classes.has('open'), true, 'first call opens');
  toggleSection(header);
  t.equal(classes.has('open'), false, 'second call closes');
  toggleSection({ nextElementSibling: null });
  t.pass('a missing sibling neither crashes nor throws');
}

t.done();
