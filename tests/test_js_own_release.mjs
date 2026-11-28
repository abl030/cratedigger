/**
 * Unit tests for web/js/own_release.js ("Import as its own release").
 * Run with: node tests/test_js_own_release.mjs
 */

import {
  cancelOwnRelease,
  chosenReleaseId,
  confirmOwnRelease,
  normalizeReleaseId,
  openOwnRelease,
  ownReleaseChoices,
  renderOwnReleaseControl,
  renderOwnReleasePanel,
} from '../web/js/own_release.js';

import { domStub, element, stubGlobals, suite } from './js_harness.mjs';

const t = suite(import.meta.url);

const ITUNES = '10d3d7fb-2955-4a00-b708-0f57a9ba3c8d';
const STANDARD = '9e6b47e9-9bad-4a9a-87e8-d02e56387762';

const CHECK = {
  outcome: 'checked',
  sentences: ['Track 16 fits the iTunes exclusive version.'],
  matching_pressings: [
    { release_id: ITUNES, description: 'AU Digital Media, 2011', url: `https://musicbrainz.org/release/${ITUNES}` },
  ],
  same_listing_pressings: [
    { release_id: STANDARD, description: 'AU CD, 2011', url: `https://musicbrainz.org/release/${STANDARD}` },
    { release_id: ITUNES, description: 'duplicate row', url: null },
    { release_id: 'not-an-id', description: '<b>bad</b>', url: 'javascript:alert(1)' },
  ],
};

/**
 * A panel whose inputs the code reads through querySelector.
 * @param {{typed?: string, picked?: string}} [state]
 */
function panel(state = {}) {
  const errorLine = element();
  const node = element();
  node.querySelector = (/** @type {string} */ selector) => {
    if (selector === '.own-release-input') return { value: state.typed ?? '' };
    if (selector === '.own-release-error') return errorLine;
    if (selector.endsWith(':checked')) {
      return state.picked === undefined ? null : { value: state.picked };
    }
    return null;
  };
  return { node, errorLine };
}

/** A button whose parent holds the panel, as the rendered control does. */
function controlWith(/** @type {any} */ panelNode) {
  const button = element({ textContent: 'Import as its own release' });
  button.parentElement = {
    querySelector: (/** @type {string} */ selector) => (
      selector === '.own-release-panel' ? panelNode : null),
  };
  return button;
}

t.section('renderOwnReleaseControl()');
{
  const html = renderOwnReleaseControl(43106, 4825);
  t.contains(html, 'window.openOwnRelease(43106, 4825, this)', 'button calls the handler with both ids');
  t.contains(html, '>Import as its own release</button>', 'button label');
  t.contains(html, 'class="own-release-panel"', 'panel present');
  t.equal(renderOwnReleaseControl('1; alert(1)', 4825), '', 'a non-integer log id renders nothing');
  t.equal(renderOwnReleaseControl(1, undefined), '', 'no request id, no control');
}

t.section('normalizeReleaseId() accepts only exact release ids');
{
  t.equal(normalizeReleaseId(` ${ITUNES.toUpperCase()} `), ITUNES, 'MB id trimmed and lowercased');
  t.equal(normalizeReleaseId('3279218'), '3279218', 'Discogs numeric id');
  t.equal(normalizeReleaseId('0123'), null, 'leading zero is not a Discogs id');
  t.equal(normalizeReleaseId('https://musicbrainz.org/release/x'), null, 'a URL is not an id');
  t.equal(normalizeReleaseId(42), null, 'not a string');
}

t.section('ownReleaseChoices() offers matches first, once each, valid ids only');
{
  const choices = ownReleaseChoices(CHECK);
  t.deepEqual(choices.map((c) => c.release_id), [ITUNES, STANDARD], 'order and dedupe');
  t.equal(choices[0].why, 'The files fit this pressing', 'match labelled');
  t.equal(choices[1].why, 'Same track lengths as the request', 'same-listing labelled');
  t.deepEqual(ownReleaseChoices(null), [], 'no payload, no choices');
}

t.section('renderOwnReleasePanel() says what happens, escapes, and confirms inline');
{
  const html = renderOwnReleasePanel(43106, 4825, ownReleaseChoices(CHECK), CHECK.sentences);
  t.contains(html, 'Your current copy stays until the import succeeds', 'explains the order');
  t.contains(html, 'Any other copies waiting here for this album are deleted now.', 'says what goes now');
  t.contains(html, `value="${ITUNES}" checked`, 'first choice preselected');
  t.contains(html, 'name="own-release-43106"', 'radios grouped per candidate');
  t.contains(html, 'window.confirmOwnRelease(43106, 4825, this)', 'inline confirm wired');
  t.contains(html, 'Yes, import as this release', 'confirm label');
  t.contains(html, CHECK.sentences[0], 'check answer shown');
  t.excludes(html, 'javascript:', 'no script URL');
  t.excludes(html, '<b>bad</b>', 'no raw markup');
  const empty = renderOwnReleasePanel(1, 2, []);
  t.contains(empty, 'No other pressing matched', 'empty state offers the paste box');
}

t.section('chosenReleaseId(): a pasted id wins, a bad paste is refused');
{
  t.deepEqual(chosenReleaseId(panel({ picked: ITUNES }).node, 1), { target: ITUNES, problem: null }, 'radio');
  t.deepEqual(chosenReleaseId(panel({ picked: ITUNES, typed: '3279218' }).node, 1), { target: '3279218', problem: null }, 'paste wins');
  t.equal(chosenReleaseId(panel({ typed: 'nope' }).node, 1).target, null, 'bad paste refused');
  t.equal(chosenReleaseId(panel().node, 1).problem, 'Pick a release, or paste its id.', 'nothing chosen');
}

t.section('openOwnRelease() asks the other-pressings route, then shows the choices');
{
  const { node } = panel();
  const button = controlWith(node);
  /** @type {string[]} */
  const urls = [];
  const globals = stubGlobals({
    fetch: async (/** @type {string} */ url) => {
      urls.push(url);
      t.equal(button.disabled, true, 'button disabled while checking');
      return { status: 200, json: async () => CHECK };
    },
  });
  await openOwnRelease(43106, 4825, button);
  t.deepEqual(urls, ['/api/other-pressings/43106'], 'asks for this candidate');
  t.contains(node.innerHTML, `value="${ITUNES}"`, 'choices rendered');
  t.equal(button.disabled, false, 'button usable again');
  globals.restore();
}

t.section('openOwnRelease() still offers the paste box when the check fails');
{
  const { node } = panel();
  const button = controlWith(node);
  const globals = stubGlobals({ fetch: async () => { throw new Error('offline'); } });
  await openOwnRelease(1, 2, button);
  t.contains(node.innerHTML, 'own-release-input', 'paste box shown');
  t.contains(node.innerHTML, 'No other pressing matched', 'empty state');
  globals.restore();
}

t.section('confirmOwnRelease() posts Replace with adopt + after-import');
{
  const { node } = panel({ picked: ITUNES });
  const button = controlWith(node);
  /** @type {any[]} */
  const posts = [];
  const globals = stubGlobals({
    document: domStub(),
    fetch: async (/** @type {string} */ url, /** @type {any} */ init) => {
      posts.push({ url, body: JSON.parse(init.body), method: init.method });
      return {
        ok: true,
        status: 200,
        json: async () => ({
          outcome: 'replaced', adopt_import_job_id: 555, displace_album_id: 15136, warnings: [],
        }),
      };
    },
  });
  await confirmOwnRelease(43106, 4825, button);
  t.deepEqual(posts, [{
    url: '/api/pipeline/4825/replace',
    method: 'POST',
    body: { target_mb_release_id: ITUNES, adopt_download_log_id: 43106, displace_after_import: true },
  }], 'one Replace call with the adopted candidate');
  t.contains(node.innerHTML, 'Import job 555 queued', 'job named');
  t.contains(node.innerHTML, 'Your current copy stays until it imports', 'order restated');
  globals.restore();
}

t.section('confirmOwnRelease() refuses without a choice and shows a server refusal');
{
  const empty = panel();
  const globals = stubGlobals({
    document: domStub(),
    fetch: async () => { throw new Error('must not be called'); },
  });
  await confirmOwnRelease(1, 2, controlWith(empty.node));
  t.equal(empty.errorLine.textContent, 'Pick a release, or paste its id.', 'asks for a choice');
  globals.restore();

  const refused = panel({ picked: ITUNES });
  const button = controlWith(refused.node);
  const globals2 = stubGlobals({
    document: domStub(),
    fetch: async () => ({
      ok: false,
      status: 422,
      json: async () => ({ outcome: 'target_release_group_mismatch', error_message: 'not in this release group' }),
    }),
  });
  await confirmOwnRelease(1, 2, button);
  t.equal(refused.errorLine.textContent, 'not in this release group', 'server message shown');
  t.equal(button.disabled, false, 'can try again');
  globals2.restore();
}

t.section('cancelOwnRelease() clears the panel');
{
  const { node } = panel();
  node.innerHTML = 'something';
  cancelOwnRelease(controlWith(node));
  t.equal(node.innerHTML, '', 'panel emptied');
}

t.done();
