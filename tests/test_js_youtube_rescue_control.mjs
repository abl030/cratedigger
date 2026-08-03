import { checkYoutubeRescue, renderYoutubeRescueControl, youtubeResolverPayload } from '../web/js/youtube_rescue_control.js';

let failed = 0;
function ok(value, message) { if (!value) { failed++; console.error(`FAIL: ${message}`); } }

const identifier = '129bebd8-a7b9-4099-b0bc-545b704e7a95';
ok(JSON.stringify(youtubeResolverPayload(identifier, '')) === JSON.stringify({ identifier }), 'blank watch URL omitted');
ok(JSON.stringify(youtubeResolverPayload(identifier, 'https://music.youtube.com/watch?v=dGYXkhMAvLk')) === JSON.stringify({ identifier, watch_url: 'https://music.youtube.com/watch?v=dGYXkhMAvLk' }), 'canonical watch URL forwarded exactly');

const failedHtml = renderYoutubeRescueControl('release-1', 1, identifier, { outcome: 'transient', error_message: 'mirror down' });
ok(failedHtml.includes('mirror down'), 'resolver failures remain visible');
const matrixHtml = renderYoutubeRescueControl('release-1', 1, identifier, { outcome: 'ok', youtube_releases: [{ yt_browse_id: 'MPREb_kb5fohQCJ6d', year: 2026, track_count: 1, distances: [{ mbid: identifier, outcome: 'ok', distance: 0, total_mb_tracks: 1 }] }] });
ok(matrixHtml.includes('window.pickYoutubeRescue(1,'), 'matrix choice remains confirm-routed');
ok(!matrixHtml.includes('"window.checkYoutubeRescue("release-1"'), 'inline handler quoting remains safe');
ok(matrixHtml.includes('event.stopPropagation()'), 'all inline control interactions stop parent propagation');
ok(matrixHtml.includes('https://music.youtube.com/browse/MPREb_kb5fohQCJ6d') && matrixHtml.includes('2026 · 1t · exact dist 0.000'), 'choices retain exact pressing evidence');

function fakeHost(watchUrl = '') {
  const buttons = [];
  const result = { innerHTML: '', querySelectorAll: () => buttons };
  const input = { value: watchUrl };
  return { dataset: {}, querySelector: (s) => s === 'input' ? input : result, result, buttons };
}
const host = fakeHost('https://music.youtube.com/watch?v=dGYXkhMAvLk');
globalThis.document = { getElementById: () => host };
let calls = 0;
globalThis.fetch = async (_url, options) => { calls++; return { ok: true, status: 200, json: async () => ({ outcome: 'ok', youtube_releases: [] }) }; };
await Promise.all([checkYoutubeRescue('release-1', 1, identifier), checkYoutubeRescue('release-1', 1, identifier)]);
ok(calls === 1, 'same-instance busy guard suppresses double resolver fire');
ok(host.result.innerHTML.includes('No YouTube album found'), 'successful result replaces result region');
await checkYoutubeRescue('release-1', 1, identifier);
ok(calls === 2 && !host.result.innerHTML.includes('No YouTube album foundNo YouTube album found'), 'repeated resolver check replaces rather than appends');
delete globalThis.document;
delete globalThis.fetch;

const networkHost = fakeHost();
globalThis.document = { getElementById: () => networkHost };
globalThis.fetch = async () => { throw new Error('offline'); };
await checkYoutubeRescue('release-network', 1, identifier);
ok(networkHost.result.innerHTML.includes('Could not reach the resolver'), 'resolver network rejection is visible');
delete globalThis.document; delete globalThis.fetch;

// A deferred response from an old generation must not repaint its replacement.
const staleHost = fakeHost();
globalThis.document = { getElementById: () => staleHost };
let releaseDeferred;
globalThis.fetch = () => new Promise((resolve) => { releaseDeferred = resolve; });
const staleRun = checkYoutubeRescue('release-stale', 1, identifier);
staleHost.dataset.generation = '99';
releaseDeferred({ ok: true, status: 200, json: async () => ({ outcome: 'ok', youtube_releases: [] }) });
await staleRun;
ok(staleHost.result.innerHTML === '', 'stale detached generation cannot overwrite current result');

// Button cancellation is reusable; accepted submit carries browse_id only and
// the submitting guard admits one concurrent POST.
const button = { dataset: { browseId: 'MPREb_kb5fohQCJ6d' }, addEventListener: (_name, listener) => { button.listener = listener; } };
const submitHost = fakeHost(); submitHost.result.querySelectorAll = () => [button];
globalThis.document = { getElementById: (id) => id === 'yt-rescue-release-submit' ? submitHost : { style: {}, textContent: '' } };
let confirms = false; globalThis.window = { confirm: () => confirms };
const requests = []; let releaseSubmit;
globalThis.fetch = (url, options) => {
  requests.push({ url, options });
  if (url.endsWith('youtube-album')) return Promise.resolve({ ok: true, status: 200, json: async () => ({ outcome: 'ok', youtube_releases: [{ yt_browse_id: button.dataset.browseId, distances: [{ mbid: identifier, outcome: 'ok', distance: 0, total_mb_tracks: 1 }] }] }) });
  return new Promise((resolve) => { releaseSubmit = resolve; });
};
await checkYoutubeRescue('release-submit', 1, identifier);
await button.listener({ stopPropagation() {} });
ok(requests.length === 1, 'cancel keeps choice active without submit');
confirms = true;
const submitA = button.listener({ stopPropagation() {} });
const submitB = button.listener({ stopPropagation() {} });
await Promise.resolve();
ok(requests.length === 2 && requests[1].url === '/api/pipeline/1/youtube-rescue' && requests[1].options.body === JSON.stringify({ browse_id: 'MPREb_kb5fohQCJ6d' }), 'accept submits exact browse_id once');
releaseSubmit({ ok: true, json: async () => ({ outcome: 'accepted' }) });
await Promise.all([submitA, submitB]);
delete globalThis.document; delete globalThis.fetch; delete globalThis.window;

if (failed) process.exit(1);
console.log('12 passed, 0 failed');
