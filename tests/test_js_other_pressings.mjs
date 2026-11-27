/**
 * Unit tests for web/js/other_pressings.js (issue #1445).
 * Run with: node tests/test_js_other_pressings.mjs
 */

import {
  checkOtherPressings,
  renderOtherPressingsControl,
  renderOtherPressingsResult,
} from '../web/js/other_pressings.js';
import { renderDownloadHistoryItem as renderDownloadHistoryFixture } from '../web/js/history.js';
import { renderHeldUpgradeCard } from '../web/js/wrong-matches.js';
import { esc } from '../web/js/util.js';

import { element, stubGlobals, suite } from './js_harness.mjs';

const t = suite(import.meta.url);

const RAZORLIGHT = {
  outcome: 'checked',
  download_log_id: 28366,
  sentences: [
    'This copy matches 2 other pressings: UK 12" Vinyl, 2004 and UK CD, 2004. All 13 tracks line up within two seconds on each.',
    "Both have 'To the Sea' and 'Fall, Fall, Fall' where this release lists 'Hang By, Hang By' and 'To the Sea'.",
    'The listed lengths were typed in by hand and may be wrong.',
  ],
  target_url: 'https://musicbrainz.org/release/5aa7f80c-bc26-4efc-9aac-317b7e2a84eb',
  matching_pressings: [
    { description: 'UK 12" Vinyl, 2004', url: 'https://musicbrainz.org/release/07039d4c-f94e-47e6-8ed8-a34cdc867640' },
    { description: 'UK CD, 2004', url: 'https://musicbrainz.org/release/d63463aa-2636-3c34-a919-f13a6f7a5e2d' },
  ],
};

/** A button inside a control whose result box the code finds by class. */
function control() {
  const box = element();
  const button = element({ textContent: 'Check other pressings' });
  button.parentElement = {
    querySelector: (/** @type {string} */ selector) => (
      selector === '.other-pressings-result' ? box : null),
  };
  return { box, button };
}

t.section('renderOtherPressingsControl()');
{
  const html = renderOtherPressingsControl(28366);
  t.contains(html, 'window.checkOtherPressings(28366, this)', 'button calls the handler with the row id');
  t.contains(html, '>Check other pressings</button>', 'button label');
  t.contains(html, 'class="other-pressings-result"', 'result box present');
  t.equal(renderOtherPressingsControl(undefined), '', 'no id, no control');
  t.equal(renderOtherPressingsControl('12; alert(1)'), '', 'a non-integer id renders nothing');
}

t.section('renderOtherPressingsResult() shows every sentence, escaped, and the links');
{
  const html = renderOtherPressingsResult(RAZORLIGHT);
  for (const sentence of RAZORLIGHT.sentences) {
    t.contains(html, esc(sentence), 'sentence rendered escaped');
  }
  t.excludes(html, '12" Vinyl', 'a raw double quote never reaches the markup');
  t.contains(html, 'href="https://musicbrainz.org/release/d63463aa-2636-3c34-a919-f13a6f7a5e2d"', 'matching pressing linked');
  t.contains(html, '>This release</a>', 'the release itself linked');
  t.contains(html, 'rel="noopener"', 'links open safely');
}

t.section('renderOtherPressingsResult() refuses a link that is not a MusicBrainz release');
{
  const html = renderOtherPressingsResult({
    sentences: ['x'],
    target_url: 'javascript:alert(1)',
    matching_pressings: [{ description: '<b>', url: 'https://evil.example/release/x' }],
  });
  t.excludes(html, 'javascript:', 'no script URL');
  t.excludes(html, 'evil.example', 'no foreign URL');
  t.excludes(html, 'On MusicBrainz', 'no link line without a valid link');
}

t.section('checkOtherPressings() shows loading, then the sentences');
{
  const { box, button } = control();
  /** @type {string[]} */
  const urls = [];
  /** @type {string[]} */
  const seenWhileLoading = [];
  const globals = stubGlobals({
    fetch: async (/** @type {string} */ url) => {
      urls.push(url);
      seenWhileLoading.push(box.innerHTML);
      t.equal(button.disabled, true, 'button disabled while checking');
      return { status: 200, json: async () => RAZORLIGHT };
    },
  });
  await checkOtherPressings(28366, button);
  t.deepEqual(urls, ['/api/other-pressings/28366'], 'asks the route for this row');
  t.contains(seenWhileLoading[0], 'Checking MusicBrainz', 'loading line shown first');
  t.contains(box.innerHTML, esc(RAZORLIGHT.sentences[1]), 'answer shown');
  t.equal(button.disabled, false, 'button enabled again');
  t.equal(button.textContent, 'Check again', 'button offers a re-run');
  globals.restore();
}

t.section('checkOtherPressings() shows the server failure sentence on a 503');
{
  const { box, button } = control();
  const globals = stubGlobals({
    fetch: async () => ({
      status: 503,
      json: async () => ({
        outcome: 'mirror_unavailable',
        sentences: ['Could not reach MusicBrainz, so other pressings were not checked. Try again later.'],
      }),
    }),
  });
  await checkOtherPressings(1, button);
  t.contains(box.innerHTML, 'Could not reach MusicBrainz', 'mirror failure sentence shown');
  t.excludes(box.innerHTML, 'Traceback', 'never a stack trace');
  globals.restore();
}

t.section('checkOtherPressings() survives a network failure or a non-JSON reply');
{
  for (const fetchImpl of [
    async () => { throw new Error('offline'); },
    async () => ({ status: 502, json: async () => { throw new SyntaxError('html'); } }),
  ]) {
    const { box, button } = control();
    const globals = stubGlobals({ fetch: fetchImpl });
    await checkOtherPressings(1, button);
    t.contains(box.innerHTML, 'The check could not be run right now. Try again later.', 'plain failure sentence');
    t.equal(button.disabled, false, 'button usable again');
    globals.restore();
  }
}

t.section('History detail offers the check beside the track-length lines');
{
  const html = renderDownloadHistoryFixture({
    id: 28366,
    outcome: 'success',
    soulseek_username: 'peer',
    created_at: '2026-06-01T10:00:00+00:00',
    downloaded_label: 'FLAC',
    verdict: 'FLAC',
    track_length_warning: '2 of 13 tracks are a different length from the release.',
    track_length_shape: 'several_tracks_differ',
    track_length_label: 'several differ',
    track_length_badge_class: 'badge-warn',
    track_length_slots: [],
  });
  t.contains(html, 'window.checkOtherPressings(28366, this)', 'control rendered for this row');
  const plain = renderDownloadHistoryFixture({
    id: 5, outcome: 'success', soulseek_username: 'peer',
    created_at: '2026-06-01T10:00:00+00:00', downloaded_label: 'FLAC', verdict: 'FLAC',
  });
  t.excludes(plain, 'checkOtherPressings', 'no control without a track-length shape');
}

t.section('Each Held upgrades card offers the check for the new copy');
{
  const html = renderHeldUpgradeCard({
    held_download_log_id: 40001,
    download_log_id: 40002,
    request_id: 7,
    soulseek_username: 'peer',
    sentence: 'The new copy differs from your current copy on track 3.',
    rows: [],
  });
  t.contains(html, 'window.checkOtherPressings(40001, this)', 'checks the held new copy, not the delete row');
}

t.done();
