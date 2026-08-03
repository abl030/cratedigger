import { checkYoutubeRescue, renderYoutubeRescueControl, youtubeResolverPayload } from '../web/js/youtube_rescue_control.js';

let failed = 0;
function ok(value, message) { if (!value) { failed++; console.error(`FAIL: ${message}`); } }

const identifier = '129bebd8-a7b9-4099-b0bc-545b704e7a95';
ok(JSON.stringify(youtubeResolverPayload(identifier, '')) === JSON.stringify({ identifier }), 'blank watch URL omitted');
ok(JSON.stringify(youtubeResolverPayload(identifier, 'https://music.youtube.com/watch?v=dGYXkhMAvLk')) === JSON.stringify({ identifier, watch_url: 'https://music.youtube.com/watch?v=dGYXkhMAvLk' }), 'canonical watch URL forwarded exactly');

const failedHtml = renderYoutubeRescueControl('release-1', 1, identifier, { outcome: 'transient', error_message: 'mirror down' });
ok(failedHtml.includes('mirror down'), 'resolver failures remain visible');
ok(failedHtml.includes('Paste a YouTube video or playlist URL, then click Check URL.'), 'control explains how a pasted URL is submitted');
ok(failedHtml.includes('Search YouTube') && failedHtml.includes('Check URL'), 'search and manual URL actions are explicit separate buttons');
const matrixHtml = renderYoutubeRescueControl('release-1', 1, identifier, { outcome: 'ok', youtube_releases: [{ yt_browse_id: 'MPREb_kb5fohQCJ6d', year: 2026, track_count: 1, distances: [{ mbid: identifier, outcome: 'ok', distance: 0, total_mb_tracks: 1 }] }] });
ok(matrixHtml.includes('window.pickYoutubeRescue(1,'), 'matrix choice remains confirm-routed');
ok(!matrixHtml.includes('"window.checkYoutubeRescue("release-1"'), 'inline handler quoting remains safe');
ok(matrixHtml.includes('event.stopPropagation()'), 'all inline control interactions stop parent propagation');
ok(matrixHtml.includes('https://music.youtube.com/browse/MPREb_kb5fohQCJ6d') && matrixHtml.includes('2026 · 1t · exact dist 0.000'), 'choices retain exact pressing evidence');
const duplicateHtml = renderYoutubeRescueControl('release-dup', 1, identifier, { outcome: 'ok', youtube_releases: [{ yt_browse_id: 'MPREb_dup', distances: [{ mbid: identifier, outcome: 'ok', distance: 0, total_mb_tracks: 1 }, { mbid: identifier, outcome: 'ok', distance: 0, total_mb_tracks: 1 }] }] });
ok(duplicateHtml.includes('disabled') && duplicateHtml.includes('Exact evidence required'), 'duplicate exact evidence disables rescue');
const mixedDuplicateHtml = renderYoutubeRescueControl('release-mixed', 1, identifier, { outcome: 'ok', youtube_releases: [{ yt_browse_id: 'MPREb_mixed', distances: [{ mbid: identifier, outcome: 'ok', distance: 0, total_mb_tracks: 1 }, { mbid: identifier, outcome: 'distance_failed', distance: null, total_mb_tracks: null }] }] });
ok(mixedDuplicateHtml.includes('disabled') && mixedDuplicateHtml.includes('Exact evidence required'), 'one valid plus one invalid exact entry stays disabled');

function fakeHost(watchUrl = '') {
  const buttons = [];
  const result = { innerHTML: '', querySelectorAll: () => buttons };
  const input = { value: watchUrl };
  return { dataset: {}, querySelector: (s) => s === 'input' ? input : result, result, buttons };
}
const host = fakeHost('https://music.youtube.com/watch?v=dGYXkhMAvLk');
globalThis.document = { getElementById: () => host };
let calls = 0;
const resolverBodies = [];
globalThis.fetch = async (_url, options) => { calls++; resolverBodies.push(options.body); return { ok: true, status: 200, json: async () => ({ outcome: 'ok', youtube_releases: [] }) }; };
await Promise.all([checkYoutubeRescue('release-1', 1, identifier), checkYoutubeRescue('release-1', 1, identifier)]);
ok(calls === 1, 'same-instance busy guard suppresses double resolver fire');
ok(resolverBodies[0] === JSON.stringify({ identifier }), 'Search YouTube ignores a populated manual URL field');
ok(host.result.innerHTML.includes('No YouTube album found'), 'successful result replaces result region');
await checkYoutubeRescue('release-1', 1, identifier, true);
ok(calls === 2 && !host.result.innerHTML.includes('No YouTube album foundNo YouTube album found'), 'repeated resolver check replaces rather than appends');
ok(resolverBodies[1] === JSON.stringify({ identifier, watch_url: 'https://music.youtube.com/watch?v=dGYXkhMAvLk' }), 'Check URL submits the pasted URL');
delete globalThis.document;
delete globalThis.fetch;

const blankHost = fakeHost('');
globalThis.document = { getElementById: () => blankHost };
globalThis.fetch = async () => { throw new Error('blank Check URL must not fetch'); };
await checkYoutubeRescue('release-blank', 1, identifier, true);
ok(blankHost.result.innerHTML.includes('Paste a YouTube video or playlist URL first.'), 'blank Check URL gets visible corrective feedback');
delete globalThis.document; delete globalThis.fetch;

const playlistHtml = renderYoutubeRescueControl('release-playlist', 1, identifier, { outcome: 'ok', youtube_releases: [{ yt_browse_id: 'PLC0playlist', yt_url: 'https://music.youtube.com/playlist?list=PLC0playlist', distances: [{ mbid: identifier, outcome: 'ok', distance: 0, total_mb_tracks: 10 }] }] });
ok(playlistHtml.includes('href="https://music.youtube.com/playlist?list=PLC0playlist"'), 'playlist candidate links to its validated playlist URL');
const unsafeLinkHtml = renderYoutubeRescueControl('release-unsafe', 1, identifier, { outcome: 'ok', youtube_releases: [{ yt_browse_id: 'MPREb_safe', yt_url: 'javascript:alert(1)', distances: [{ mbid: identifier, outcome: 'ok', distance: 0, total_mb_tracks: 1 }] }] });
ok(unsafeLinkHtml.includes('href="https://music.youtube.com/browse/MPREb_safe"') && !unsafeLinkHtml.includes('href="javascript:'), 'untrusted candidate URL falls back to the safe browse-id URL');

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

const rejectButton = { dataset: { browseId: 'MPREb_kb5fohQCJ6d' }, addEventListener: (_name, listener) => { rejectButton.listener = listener; } };
const rejectHost = fakeHost(); rejectHost.result.querySelectorAll = () => [rejectButton];
const toastNode = { style: {}, textContent: '' };
globalThis.document = { getElementById: (id) => id === 'yt-rescue-release-reject' ? rejectHost : toastNode };
globalThis.window = { confirm: () => true };
globalThis.fetch = (url) => url.endsWith('youtube-album')
  ? Promise.resolve({ ok: true, status: 200, json: async () => ({ outcome: 'ok', youtube_releases: [{ yt_browse_id: rejectButton.dataset.browseId, distances: [{ mbid: identifier, outcome: 'ok', distance: 0, total_mb_tracks: 1 }] }] }) })
  : Promise.reject(new Error('offline'));
await checkYoutubeRescue('release-reject', 1, identifier);
await rejectButton.listener({ stopPropagation() {} });
ok(toastNode.textContent.includes('network unavailable') && rejectHost.dataset.submitting === 'false', 'rescue rejection toasts visibly and clears submit guard for retry');
delete globalThis.document; delete globalThis.fetch; delete globalThis.window;

if (failed) process.exit(1);
console.log('13 passed, 0 failed');
