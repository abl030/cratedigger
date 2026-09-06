/**
 * Unit tests for web/js/replace_picker.js's inverted mode with a compare
 * pair (issue #1366 part 2): the picker searches the row's own group AND
 * the paired group on the other pathway, lists paired candidates with
 * the pair named, confirms a cross-pathway pick with an explicit note,
 * and posts the operator's opt-in only for that pick.
 *
 * The DOM is a small fake modal: `innerHTML` is captured as a string,
 * `querySelector('#id')` answers only ids present in that string with an
 * element whose click handler the test can fire, and everything else is
 * absent — enough to drive the real `openReplacePicker` end to end.
 *
 * Run with: node tests/test_js_replace_picker.mjs
 */

import {
  onlyCurrentPressing,
  openReplacePicker,
  renderConfirmDialog,
  renderInvertedHeader,
  renderRequestsList,
  renderSolePressingNote,
} from '../web/js/replace_picker.js';

import { stubGlobals, suite } from './js_harness.mjs';

const t = suite(import.meta.url);

function fakeModal() {
  const handlers = new Map();
  const modal = {
    style: {},
    _html: '',
    set innerHTML(value) { this._html = value; handlers.clear(); },
    get innerHTML() { return this._html; },
    querySelector(selector) {
      if (selector === '.confirm-overlay') return null;
      const id = selector.startsWith('#') ? selector.slice(1) : null;
      if (id && this._html.includes(`id="${id}"`)) {
        return {
          addEventListener(type, fn) { handlers.set(`${id}:${type}`, fn); },
        };
      }
      return null;
    },
    // Standard mode's pressing list: one row object per rendered
    // `<li class="replace-picker-row">`, with just the children
    // `wireRows` looks up. The confirm button exists only where the
    // markup rendered one (never on the current pressing).
    querySelectorAll(selector) {
      if (selector !== '.replace-picker-row') return [];
      const blocks = this._html.matchAll(/<li class="replace-picker-row" data-mbid-row="([^"]+)">([\s\S]*?)<\/li>/g);
      return Array.from(blocks, ([, mbid, inner]) => {
        const pick = {
          disabled: /class="replace-picker-pick" disabled/.test(inner),
          textContent: `pressing ${mbid}`,
          getAttribute: (name) => (name === 'data-expand-mbid' ? mbid : name === 'aria-expanded' ? 'false' : null),
          setAttribute() {},
          addEventListener(type, fn) { handlers.set(`row:${mbid}:pick:${type}`, fn); },
        };
        const confirm = inner.includes('class="replace-picker-confirm"') ? {
          getAttribute: () => null,
          addEventListener(type, fn) { handlers.set(`row:${mbid}:confirm:${type}`, fn); },
        } : null;
        const panel = { innerHTML: '', dataset: {} };
        const slot = { hidden: true };
        return {
          classList: { add() {}, remove() {} },
          querySelector(sel) {
            if (sel.startsWith('button.replace-picker-pick')) return pick;
            if (sel.startsWith('.replace-picker-detail[')) return panel;
            if (sel.startsWith('.replace-picker-detail-actions-slot[')) return slot;
            if (sel === 'button.replace-picker-confirm') return confirm;
            return null;
          },
        };
      });
    },
    async click(id) {
      const fn = handlers.get(`${id}:click`);
      if (!fn) throw new Error(`no click handler bound for #${id}`);
      await fn({ stopPropagation() {} });
    },
    hasRowConfirm(mbid) { return handlers.has(`row:${mbid}:confirm:click`); },
    async clickRowConfirm(mbid) {
      const fn = handlers.get(`row:${mbid}:confirm:click`);
      if (!fn) throw new Error(`no confirm handler bound for pressing ${mbid}`);
      await fn({ stopPropagation() {} });
    },
  };
  return modal;
}

function okJson(body) {
  return { ok: true, status: 200, json: async () => body };
}

/** Drive the inverted picker with routed fetch answers; returns the modal, the calls and the promise. */
function drive(options, routes) {
  const modal = fakeModal();
  const calls = [];
  stubGlobals({
    document: { getElementById: (id) => (id === 'replace-picker-modal' ? modal : null) },
    fetch: async (url, init) => {
      calls.push({ url: String(url), init: init || null });
      for (const [prefix, answer] of routes) {
        if (String(url).startsWith(prefix)) return answer(String(url), init);
      }
      throw new Error(`unexpected fetch: ${url}`);
    },
  });
  const done = openReplacePicker(options);
  return { modal, calls, done };
}

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

const MB_REQUEST = {
  id: 2839, mb_release_id: 'a0a2b395-7989-4ec7-99f9-9bc9425c53b7',
  mb_release_group_id: '6f151223-f3a3-3e57-810f-598f7897006c',
  status: 'imported', artist_name: 'Muse', album_title: 'Absolution',
  processing_owner: null,
};

t.section('inverted with a paired master — the other pathway\'s request is the single candidate; confirm carries the cross-pathway note and opt-in');
{
  const { modal, calls, done } = drive({
    targetMbid: '793320',
    releaseGroupId: '11052',
    targetLabel: 'Muse — Absolution',
    pairedGroupId: '6f151223-f3a3-3e57-810f-598f7897006c',
    pairedGroupKind: 'work',
    pairedLabel: 'Absolution',
  }, [
    ['/api/pipeline/requests-by-rg/11052', () => okJson({ requests: [] })],
    ['/api/pipeline/requests-by-rg/6f151223', () => okJson({ requests: [MB_REQUEST] })],
    ['/api/pipeline/2839/replace', () => okJson({ outcome: 'replaced', request_id: 2839, new_request_id: 9029 })],
  ]);
  await flush(); await flush(); await flush();
  t.contains(modal.innerHTML, 'Replace request #2839?', 'a single paired candidate goes straight to confirm');
  t.contains(modal.innerHTML, 'Cross-pathway:', 'the confirm dialog carries the cross-pathway note');
  t.contains(modal.innerHTML, 'paired with &quot;Absolution&quot;', 'and names the pair');
  t.contains(modal.innerHTML, 'asserts the two are the same album', 'and says what confirming asserts');
  await modal.click('replace-picker-confirm');
  await flush();
  const post = calls.find((c) => c.url === '/api/pipeline/2839/replace');
  t.ok(post !== undefined, 'the confirm posted to the chosen request\'s replace route');
  t.deepEqual(JSON.parse(post.init.body), { target_mb_release_id: '793320', cross_pathway: true },
    'the POST carries the operator\'s cross-pathway opt-in for a candidate found through the pair');
  const result = await done;
  t.equal(result.outcome, 'confirmed', 'the picker resolves confirmed');
  t.equal(result.sourceRequestId, 2839, 'with the paired request as the source');
  t.equal(result.targetMbid, '793320', 'and the clicked pressing as the target');
}

t.section('inverted with a same-pathway candidate only — no note, opt-in explicitly false');
{
  const { modal, calls } = drive({
    targetMbid: 'fdd45566-5c8b-4beb-8ec3-1b5f93a01319',
    releaseGroupId: '6f151223-f3a3-3e57-810f-598f7897006c',
    targetLabel: 'Muse — Absolution (JP)',
    pairedGroupId: '11052',
    pairedGroupKind: 'work',
    pairedLabel: 'Absolution',
  }, [
    ['/api/pipeline/requests-by-rg/6f151223', () => okJson({ requests: [MB_REQUEST] })],
    ['/api/pipeline/requests-by-rg/11052', () => okJson({ requests: [] })],
    ['/api/pipeline/2839/replace', () => okJson({ outcome: 'replaced', request_id: 2839, new_request_id: 9030 })],
  ]);
  await flush(); await flush(); await flush();
  t.excludes(modal.innerHTML, 'Cross-pathway:', 'a same-pathway candidate gets no cross-pathway note');
  await modal.click('replace-picker-confirm');
  await flush();
  const post = calls.find((c) => c.url === '/api/pipeline/2839/replace');
  t.deepEqual(JSON.parse(post.init.body), { target_mb_release_id: 'fdd45566-5c8b-4beb-8ec3-1b5f93a01319', cross_pathway: false },
    'the POST sends an explicit false for a same-pathway candidate (the route field is strict)');
}

t.section('inverted with a paired MASTERLESS release — looked up by exact release, not by group');
{
  const masterless = {
    id: 8840, mb_release_id: '3938744', mb_release_group_id: null,
    status: 'wanted', artist_name: 'Deloris', album_title: 'Fraulein', processing_owner: null,
  };
  const { modal, calls } = drive({
    targetMbid: '19016167-1ba2-41ab-9bec-bf9ed2ac995c',
    releaseGroupId: '1c9e2970-b221-30ab-93c6-7896b52a240b',
    targetLabel: 'Deloris — Fraulein',
    pairedGroupId: '3938744',
    pairedGroupKind: 'release',
    pairedLabel: 'Fraulein',
  }, [
    ['/api/pipeline/requests-by-rg/1c9e2970', () => okJson({ requests: [] })],
    ['/api/pipeline/requests-by-release/3938744', () => okJson({ requests: [masterless] })],
  ]);
  await flush(); await flush(); await flush();
  t.ok(calls.some((c) => c.url === '/api/pipeline/requests-by-release/3938744'),
    'a release-kind pair is fetched through requests-by-release');
  t.ok(!calls.some((c) => c.url.includes('requests-by-rg/3938744')),
    'and never through requests-by-rg');
  t.contains(modal.innerHTML, 'Replace request #8840?', 'the masterless Discogs request is the candidate');
  t.contains(modal.innerHTML, 'Cross-pathway:', 'and replacing it is cross-pathway');
}

t.section('inverted from a masterless Discogs row with a pair — no own group, no lazy resolve, straight to the pair');
{
  const { modal, calls } = drive({
    targetMbid: '3938744',
    releaseGroupId: null,
    targetLabel: 'Deloris — Fraulein (Discogs)',
    pairedGroupId: '1c9e2970-b221-30ab-93c6-7896b52a240b',
    pairedGroupKind: 'work',
    pairedLabel: 'Fraulein',
  }, [
    ['/api/pipeline/requests-by-rg/1c9e2970', () => okJson({ requests: [{
      id: 425, mb_release_id: '19016167-1ba2-41ab-9bec-bf9ed2ac995c',
      mb_release_group_id: '1c9e2970-b221-30ab-93c6-7896b52a240b',
      status: 'wanted', artist_name: 'Deloris', album_title: 'Fraulein', processing_owner: null,
    }] })],
  ]);
  await flush(); await flush(); await flush();
  t.ok(!calls.some((c) => c.url.startsWith('/api/release/')),
    'no lazy release-group resolve is attempted for a masterless row that has a pair');
  t.contains(modal.innerHTML, 'Replace request #425?', 'the MB request under the paired release group is the candidate');
}

t.section('inverted — both sides hold requests: the list names the paired one and the pick decides the opt-in');
{
  const discogsReq = {
    id: 9028, mb_release_id: '793320', mb_release_group_id: '11052',
    status: 'wanted', artist_name: 'Muse', album_title: 'Absolution', processing_owner: null,
  };
  const { modal } = drive({
    targetMbid: '1502048',
    releaseGroupId: '11052',
    targetLabel: 'Muse — Absolution (AU CDr)',
    pairedGroupId: '6f151223-f3a3-3e57-810f-598f7897006c',
    pairedGroupKind: 'work',
    pairedLabel: 'Absolution',
  }, [
    ['/api/pipeline/requests-by-rg/11052', () => okJson({ requests: [discogsReq] })],
    ['/api/pipeline/requests-by-rg/6f151223', () => okJson({ requests: [MB_REQUEST] })],
    ['/api/release/', () => okJson({ tracks: [] })],
  ]);
  await flush(); await flush(); await flush();
  t.contains(modal.innerHTML, '<strong>#9028</strong>', 'the own-group request is listed');
  t.contains(modal.innerHTML, '<strong>#2839</strong>', 'the paired request is listed too');
  t.contains(modal.innerHTML, 'on the other pathway — paired with &quot;Absolution&quot;', 'the paired candidate says so');
  t.contains(modal.innerHTML, 'or its paired &quot;Absolution&quot; on the other pathway', 'the list header says the pair was searched too');
  const html = renderRequestsList([discogsReq, { ...MB_REQUEST, viaPairing: true, pairingLabel: 'A "quoted" <b>' }]);
  t.equal((html.match(/on the other pathway/g) || []).length, 1, 'only the paired candidate carries the pairing line');
  t.contains(html, 'paired with &quot;A &quot;quoted&quot; &lt;b&gt;&quot;', 'the pairing label is HTML-escaped');
}

t.section('inverted — no request on either side says so');
{
  const { modal } = drive({
    targetMbid: '793320', releaseGroupId: '11052', targetLabel: 'Muse — Absolution',
    pairedGroupId: '6f151223-f3a3-3e57-810f-598f7897006c', pairedGroupKind: 'work', pairedLabel: 'Absolution',
  }, [
    ['/api/pipeline/requests-by-rg/', () => okJson({ requests: [] })],
  ]);
  await flush(); await flush(); await flush();
  t.contains(modal.innerHTML, 'No active requests for this album on either pathway to replace.',
    'the empty state names both pathways when a pair was searched');
}

t.section('inverted with a group and NO pair — own-group candidates only, no note, explicit false');
{
  const { modal, calls } = drive({
    targetMbid: 'fdd45566-5c8b-4beb-8ec3-1b5f93a01319',
    releaseGroupId: '6f151223-f3a3-3e57-810f-598f7897006c',
    targetLabel: 'Muse — Absolution (JP)',
  }, [
    ['/api/pipeline/requests-by-rg/6f151223', () => okJson({ requests: [MB_REQUEST] })],
    ['/api/pipeline/2839/replace', () => okJson({ outcome: 'replaced', request_id: 2839, new_request_id: 9031 })],
  ]);
  await flush(); await flush(); await flush();
  t.ok(!calls.some((c) => c.url.startsWith('/api/release/')), 'nothing is resolved: the row\'s own group is the only key');
  t.ok(!calls.some((c) => c.url.includes('requests-by-release')), 'and no pair is fetched');
  t.contains(modal.innerHTML, 'Replace request #2839?', 'the own-group request is the candidate');
  t.excludes(modal.innerHTML, 'Cross-pathway:', 'with no cross-pathway note');
  await modal.click('replace-picker-confirm');
  await flush();
  const post = calls.find((c) => c.url === '/api/pipeline/2839/replace');
  t.deepEqual(JSON.parse(post.init.body), { target_mb_release_id: 'fdd45566-5c8b-4beb-8ec3-1b5f93a01319', cross_pathway: false },
    'the POST carries an explicit false when no pair was ever involved');
}

t.section('standard mode (Pipeline tab, Wrong Matches) — the pressing switcher posts cross_pathway: false, never the opt-in');
{
  const CURRENT = 'a0a2b395-7989-4ec7-99f9-9bc9425c53b7';
  const OTHER = 'fdd45566-5c8b-4beb-8ec3-1b5f93a01319';
  const pressing = (id, country) => ({ id, title: 'Absolution', status: 'Official', country, date: '2003-09-15', format: 'CD', track_count: 14 });
  const { modal, calls, done } = drive({
    sourceRequestId: 2839,
    releaseGroupId: '6f151223-f3a3-3e57-810f-598f7897006c',
    sourceLabel: 'Muse — Absolution',
  }, [
    ['/api/release-group/6f151223', () => okJson({ releases: [pressing(CURRENT, 'GB'), pressing(OTHER, 'JP')] })],
    ['/api/pipeline/2839/replace', () => okJson({ outcome: 'replaced', request_id: 2839, new_request_id: 9032 })],
    ['/api/pipeline/2839', () => okJson({ request: { mb_release_id: CURRENT } })],
    ['/api/release/', () => okJson({ tracks: [] })],
    ['/api/wrong-matches', () => okJson([])],
  ]);
  for (let i = 0; i < 6; i++) await flush();
  t.contains(modal.innerHTML, 'data-mbid-row="' + OTHER + '"', 'the sibling pressing is listed');
  t.excludes(modal.innerHTML, 'no other pressing on file', 'a group with a sibling carries no sole-pressing note');
  t.ok(!modal.hasRowConfirm(CURRENT), 'the current pressing has no "Use this pressing" action');
  t.ok(modal.hasRowConfirm(OTHER), 'the sibling does');
  await modal.clickRowConfirm(OTHER);
  await flush();
  t.contains(modal.innerHTML, 'Replace request #2839?', 'picking the sibling reaches the confirm dialog');
  t.excludes(modal.innerHTML, 'Cross-pathway:', 'a same-group switch carries no cross-pathway note');
  await modal.click('replace-picker-confirm');
  await flush();
  const post = calls.find((c) => c.url === '/api/pipeline/2839/replace');
  t.ok(post !== undefined, 'the confirm posted to the source request\'s replace route');
  t.deepEqual(JSON.parse(post.init.body), { target_mb_release_id: OTHER, cross_pathway: false },
    'standard mode never asserts a cross-pathway identity: the opt-in is an explicit false');
  const result = await done;
  t.equal(result.outcome, 'confirmed', 'the picker resolves confirmed');
  t.equal(result.targetMbid, OTHER, 'with the picked sibling as the target');
}

t.section('standard mode — a release group whose only pressing on file is the current one says so instead of dead-ending (issue #1382 item 1)');
{
  const CURRENT = 'a0a2b395-7989-4ec7-99f9-9bc9425c53b7';
  const { modal } = drive({
    sourceRequestId: 425,
    releaseGroupId: '1c9e2970-b221-30ab-93c6-7896b52a240b',
    sourceLabel: 'Deloris — Fraulein',
  }, [
    ['/api/release-group/1c9e2970', () => okJson({ releases: [{ id: CURRENT, title: 'Fraulein', status: 'Official', country: 'AU', date: '1998', format: 'CD', track_count: 12 }] })],
    ['/api/pipeline/425', () => okJson({ request: { mb_release_id: CURRENT } })],
    ['/api/release/', () => okJson({ tracks: [] })],
    ['/api/wrong-matches', () => okJson([])],
  ]);
  for (let i = 0; i < 6; i++) await flush();
  t.contains(modal.innerHTML, 'This release group has no other pressing on file — there is nothing to switch to.',
    'the picker explains why there is nothing to pick');
  t.contains(modal.innerHTML, 'data-mbid-row="' + CURRENT + '"', 'the current pressing is still listed, disabled');
  t.ok(!modal.hasRowConfirm(CURRENT), 'and it has no "Use this pressing" action');
  t.equal(onlyCurrentPressing([{ id: 'x' }], 'x'), true, 'a lone current pressing is the sole-pressing case');
  t.equal(onlyCurrentPressing([{ id: 'x' }, { id: 'x' }], 'x'), true, 'so is a group listing the current pressing twice');
  t.equal(onlyCurrentPressing([{ id: 'x' }, { id: 'y' }], 'x'), false, 'a sibling is not');
  t.equal(onlyCurrentPressing([], 'x'), false, 'and an empty group is its own message, not this one');
  t.contains(renderSolePressingNote(), 'no other pressing on file', 'the note names the situation');
}

t.section('pure renderers — confirm note and header copy');
{
  const plain = renderConfirmDialog({ sourceRequestId: 1, targetMbid: 'x' });
  t.excludes(plain, 'Cross-pathway', 'no note without the flag');
  const cross = renderConfirmDialog({ sourceRequestId: 1, targetMbid: 'x', crossPathway: true, pairingLabel: 'L <i>' });
  t.contains(cross, 'request #1 is on the other pathway', 'the note names the request');
  t.contains(cross, 'paired with &quot;L &lt;i&gt;&quot;', 'the pairing label is escaped');
  t.contains(renderInvertedHeader('T', 'P <b>'), 'or its paired &quot;P &lt;b&gt;&quot; on the other pathway', 'header names the pair, escaped');
  t.excludes(renderInvertedHeader('T'), 'other pathway', 'header without a pair is unchanged');
}

t.done();
