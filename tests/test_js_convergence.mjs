/** Frontend convergence prompt/action contract (#978). */
import { readFileSync } from 'node:fs';

import { element, stubGlobals, suite } from './js_harness.mjs';

// Assigned BEFORE the dynamic import below: web/js/convergence.js reads
// `document` at module-evaluation time, so this ordering is load-bearing.
stubGlobals({ window: {} });
stubGlobals({ document: { querySelector() { return null; } } });

const {
  convergenceBadge,
  renderConvergencePrompt,
  stopConvergedSearch,
} = await import('../web/js/convergence.js');

const t = suite(import.meta.url);

const signal = {
  request_id: 41,
  observation_count: 8,
  distinct_peer_count: 6,
  distinct_candidate_snapshot_count: 5,
  distinct_codec_count: 3,
  cliff_hz: 15000,
  raw_cliff_min_hz: 14780,
  raw_cliff_max_hz: 15220,
  cliff_spread_hz: 440,
  latest_qualifying_log_id: 99,
  signal_token: 'a'.repeat(64),
};

t.section('badge and prompt copy');

t.match(convergenceBadge(signal), /search converged/i,
  'the badge names the converged search');
const wanted = renderConvergencePrompt(signal, 'wanted', 'recents');
t.match(wanted, /6 peers/i, 'the prompt reports the distinct peer count');
t.match(wanted, /at least five qualifying observations/i,
  'the prompt states the qualifying-observation floor');
t.match(wanted, /at least five distinct peers/i,
  'the prompt states the distinct-peer floor');
t.match(wanted, /3 codecs/i, 'the prompt reports the distinct codec count');
t.match(wanted, /raw cliffs 14\.8 kHz-15\.2 kHz \(440 Hz spread\)/i,
  'the prompt reports the raw cliff range and its spread');
t.match(wanted, /shared 15\.0 kHz band/i,
  'the prompt reports the shared cliff band');
t.match(wanted, /provisional—not proof/i,
  'the prompt calls the signal provisional, not proof');
t.match(
  wanted,
  /<button class="p-btn convergence-stop"[^>]*>Stop searching<\/button>/,
  'the convergence action remains a native button',
);
t.match(wanted, /&quot;signal_token&quot;:&quot;aaaaaaaa/,
  'the signal token is HTML-escaped into the action payload');
t.notMatch(wanted, /all.*exact|identical cliff/i,
  'the prompt never claims the cliffs are all exact or identical');
t.notMatch(wanted, />Accept</,
  'the prompt offers no Accept action');
t.match(renderConvergencePrompt(signal, 'unsearchable'), /Searching stopped/i,
  'an unsearchable request renders the stopped state');

t.section('codec noun agreement');

for (const [count, expected] of [[0, 'codecs'], [1, 'codec'], [2, 'codecs']]) {
  const rendered = renderConvergencePrompt(
    { ...signal, distinct_codec_count: count },
    'wanted',
  );
  t.match(
    rendered,
    new RegExp(`· ${count} ${expected} · raw cliffs`),
    `${count} uses the correct codec noun`,
  );
}

t.section('exact-band precision');

const exactBand = renderConvergencePrompt({
  ...signal,
  cliff_hz: 15000,
  raw_cliff_min_hz: 15000,
  raw_cliff_max_hz: 15000,
  cliff_spread_hz: 0,
}, 'wanted');
t.match(
  exactBand,
  /raw cliffs 15\.0 kHz-15\.0 kHz \(0 Hz spread\) · shared 15\.0 kHz band/i,
  'exact raw values keep one-decimal kHz precision and the truthful spread',
);

t.section('stop-action style rule');

const indexHtml = readFileSync(new URL('../web/index.html', import.meta.url), 'utf8');
const convergenceStopRule = indexHtml.match(/\.convergence-stop\s*\{(?<rules>[^}]*)\}/);
t.ok(convergenceStopRule, 'the convergence action has a dedicated style rule');
// The counting harness does not abort on the assertion above, so the rule
// body is read defensively: a missing rule fails BOTH claims rather than
// throwing on `.groups` and skipping every later assertion in the file.
t.match(
  convergenceStopRule ? convergenceStopRule.groups.rules : '',
  /min-height:\s*24px\s*;/,
  'the convergence action has at least a 24px target height',
);

function response(status, body, { raw = null } = {}) {
  return {
    ok: status >= 200 && status < 300,
    status,
    async text() { return raw ?? JSON.stringify(body); },
  };
}

function buttonFixture() {
  const prompt = element();
  const button = element({
    textContent: 'Stop searching',
    closest(selector) { return selector === '.convergence-prompt' ? prompt : null; },
  });
  return { button, prompt };
}

let recentsRefreshes = 0;
let browseRefreshes = 0;
const toasts = [];
window.loadRecents = async () => { recentsRefreshes += 1; };
window.reloadBrowseArtist = async () => { browseRefreshes += 1; };
window.toast = (...args) => toasts.push(args);

t.section('successful stop');

let request;
stubGlobals({ fetch: async (url, options) => {
  request = { url, options };
  return response(200, { outcome: 'stopped' });
} });
const successFixture = buttonFixture();
await stopConvergedSearch(signal, successFixture.button, 'recents');
t.equal(request.url, '/api/triage/41/stop-converged-search',
  'the stop action posts to the request-scoped triage route');
t.deepEqual(JSON.parse(request.options.body), {
  confirm: 'STOP', signal_token: 'a'.repeat(64),
}, 'the stop body carries the confirmation and the signal token');
t.equal(recentsRefreshes, 1, 'success refreshes the originating Recents surface');
t.equal(browseRefreshes, 0, 'success does not refetch unrelated Browse state');
t.match(successFixture.prompt.innerHTML, /Searching stopped/,
  'success rewrites the prompt into the stopped state');

t.section('busy state is synchronous and blocks a second activation');

let releaseFetch;
let fetchCount = 0;
stubGlobals({ fetch: () => {
  fetchCount += 1;
  return new Promise(resolve => { releaseFetch = resolve; });
} });
const doubleFixture = buttonFixture();
const first = stopConvergedSearch(signal, doubleFixture.button, 'recents');
const second = await stopConvergedSearch(signal, doubleFixture.button, 'recents');
t.equal(second, null, 'a second activation returns without an outcome');
t.equal(fetchCount, 1, 'a second activation submits no second request');
t.equal(doubleFixture.button.disabled, true, 'the in-flight button is disabled');
t.equal(doubleFixture.button.getAttribute('aria-busy'), 'true',
  'the in-flight button reports aria-busy');
releaseFetch(response(200, { outcome: 'stopped' }));
await first;

t.section('stale outcomes');

for (const status of [409, 422]) {
  stubGlobals({ fetch: async () => response(status, { outcome: 'stale' }) });
  const staleFixture = buttonFixture();
  const before = browseRefreshes;
  await stopConvergedSearch(signal, staleFixture.button, 'library-detail');
  t.equal(staleFixture.prompt.removed, true, `${status} removes stale prompt`);
  t.equal(browseRefreshes, before + 1, `${status} refreshes Library origin`);
}

t.section('network failure restores the control');

stubGlobals({ fetch: async () => { throw new TypeError('network down'); } });
const networkFixture = buttonFixture();
const network = await stopConvergedSearch(signal, networkFixture.button, 'recents');
t.equal(network.outcome, 'unavailable', 'a transport failure reports unavailable');
t.equal(networkFixture.button.disabled, false,
  'a transport failure re-enables the button');
t.equal(networkFixture.button.getAttribute('aria-busy'), 'false',
  'a transport failure clears aria-busy');
t.equal(networkFixture.button.textContent, 'Stop searching',
  'a transport failure restores the button label');

t.section('malformed error body');

stubGlobals({ fetch: async () => response(503, {}, { raw: '<html>proxy error</html>' }) });
const malformedFixture = buttonFixture();
await stopConvergedSearch(signal, malformedFixture.button, 'recents');
t.equal(malformedFixture.button.disabled, false,
  'an unparseable error body re-enables the button');
t.match(toasts.at(-1)[0], /HTTP 503/,
  'an unparseable error body toasts the raw HTTP status');

t.done();
