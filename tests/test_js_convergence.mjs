/** Frontend convergence prompt/action contract (#978). */
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

global.window = {};
global.document = { querySelector() { return null; } };

const {
  convergenceBadge,
  renderConvergencePrompt,
  stopConvergedSearch,
} = await import('../web/js/convergence.js');

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

assert.match(convergenceBadge(signal), /search converged/i);
const wanted = renderConvergencePrompt(signal, 'wanted', 'recents');
assert.match(wanted, /6 peers/i);
assert.match(wanted, /at least five qualifying observations/i);
assert.match(wanted, /at least five distinct peers/i);
assert.match(wanted, /3 codecs/i);
assert.match(wanted, /raw cliffs 14\.8 kHz-15\.2 kHz \(440 Hz spread\)/i);
assert.match(wanted, /shared 15\.0 kHz band/i);
assert.match(wanted, /provisional—not proof/i);
assert.match(
  wanted,
  /<button class="p-btn convergence-stop"[^>]*>Stop searching<\/button>/,
  'the convergence action remains a native button',
);
assert.match(wanted, /&quot;signal_token&quot;:&quot;aaaaaaaa/);
assert.doesNotMatch(wanted, /all.*exact|identical cliff/i);
assert.doesNotMatch(wanted, />Accept</);
assert.match(renderConvergencePrompt(signal, 'unsearchable'), /Searching stopped/i);

for (const [count, expected] of [[0, 'codecs'], [1, 'codec'], [2, 'codecs']]) {
  const rendered = renderConvergencePrompt(
    { ...signal, distinct_codec_count: count },
    'wanted',
  );
  assert.match(
    rendered,
    new RegExp(`· ${count} ${expected} · raw cliffs`),
    `${count} uses the correct codec noun`,
  );
}

const exactBand = renderConvergencePrompt({
  ...signal,
  cliff_hz: 15000,
  raw_cliff_min_hz: 15000,
  raw_cliff_max_hz: 15000,
  cliff_spread_hz: 0,
}, 'wanted');
assert.match(
  exactBand,
  /raw cliffs 15\.0 kHz-15\.0 kHz \(0 Hz spread\) · shared 15\.0 kHz band/i,
  'exact raw values keep one-decimal kHz precision and the truthful spread',
);

const indexHtml = readFileSync(new URL('../web/index.html', import.meta.url), 'utf8');
const convergenceStopRule = indexHtml.match(/\.convergence-stop\s*\{(?<rules>[^}]*)\}/);
assert.ok(convergenceStopRule, 'the convergence action has a dedicated style rule');
assert.match(
  convergenceStopRule.groups.rules,
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
  const prompt = {
    innerHTML: '',
    removed: false,
    remove() { this.removed = true; },
  };
  const button = {
    dataset: {},
    disabled: false,
    textContent: 'Stop searching',
    attributes: {},
    setAttribute(name, value) { this.attributes[name] = value; },
    closest(selector) { return selector === '.convergence-prompt' ? prompt : null; },
  };
  return { button, prompt };
}

let recentsRefreshes = 0;
let browseRefreshes = 0;
const toasts = [];
window.loadRecents = async () => { recentsRefreshes += 1; };
window.reloadBrowseArtist = async () => { browseRefreshes += 1; };
window.toast = (...args) => toasts.push(args);

let request;
global.fetch = async (url, options) => {
  request = { url, options };
  return response(200, { outcome: 'stopped' });
};
const successFixture = buttonFixture();
await stopConvergedSearch(signal, successFixture.button, 'recents');
assert.equal(request.url, '/api/triage/41/stop-converged-search');
assert.deepEqual(JSON.parse(request.options.body), {
  confirm: 'STOP', signal_token: 'a'.repeat(64),
});
assert.equal(recentsRefreshes, 1, 'success refreshes the originating Recents surface');
assert.equal(browseRefreshes, 0, 'success does not refetch unrelated Browse state');
assert.match(successFixture.prompt.innerHTML, /Searching stopped/);

// Busy state is synchronous and a second activation cannot submit.
let releaseFetch;
let fetchCount = 0;
global.fetch = () => {
  fetchCount += 1;
  return new Promise(resolve => { releaseFetch = resolve; });
};
const doubleFixture = buttonFixture();
const first = stopConvergedSearch(signal, doubleFixture.button, 'recents');
const second = await stopConvergedSearch(signal, doubleFixture.button, 'recents');
assert.equal(second, null);
assert.equal(fetchCount, 1);
assert.equal(doubleFixture.button.disabled, true);
assert.equal(doubleFixture.button.attributes['aria-busy'], 'true');
releaseFetch(response(200, { outcome: 'stopped' }));
await first;

for (const status of [409, 422]) {
  global.fetch = async () => response(status, { outcome: 'stale' });
  const staleFixture = buttonFixture();
  const before = browseRefreshes;
  await stopConvergedSearch(signal, staleFixture.button, 'library-detail');
  assert.equal(staleFixture.prompt.removed, true, `${status} removes stale prompt`);
  assert.equal(browseRefreshes, before + 1, `${status} refreshes Library origin`);
}

global.fetch = async () => { throw new TypeError('network down'); };
const networkFixture = buttonFixture();
const network = await stopConvergedSearch(signal, networkFixture.button, 'recents');
assert.equal(network.outcome, 'unavailable');
assert.equal(networkFixture.button.disabled, false);
assert.equal(networkFixture.button.attributes['aria-busy'], 'false');
assert.equal(networkFixture.button.textContent, 'Stop searching');

global.fetch = async () => response(503, {}, { raw: '<html>proxy error</html>' });
const malformedFixture = buttonFixture();
await stopConvergedSearch(signal, malformedFixture.button, 'recents');
assert.equal(malformedFixture.button.disabled, false);
assert.match(toasts.at(-1)[0], /HTTP 503/);

console.log('test_js_convergence: all assertions passed');
