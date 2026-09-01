import { checkYoutubeRescue, renderYoutubeRescueControl, youtubeResolverPayload } from '../web/js/youtube_rescue_control.js';

import { stubGlobals, suite } from './js_harness.mjs';

const t = suite(import.meta.url);

t.section('youtubeResolverPayload()');
const identifier = '129bebd8-a7b9-4099-b0bc-545b704e7a95';
t.deepEqual(youtubeResolverPayload(identifier, ''), { identifier }, 'blank watch URL omitted');
t.deepEqual(youtubeResolverPayload(identifier, 'https://music.youtube.com/watch?v=dGYXkhMAvLk'), { identifier, watch_url: 'https://music.youtube.com/watch?v=dGYXkhMAvLk' }, 'canonical watch URL forwarded exactly');

t.section('renderYoutubeRescueControl()');
const failedHtml = renderYoutubeRescueControl('release-1', 1, identifier, { outcome: 'transient', error_message: 'mirror down' });
t.contains(failedHtml, 'mirror down', 'resolver failures remain visible');
t.contains(failedHtml, 'Paste a YouTube video or playlist URL, then click Check URL.', 'control explains how a pasted URL is submitted');
t.contains(failedHtml, 'Search YouTube', 'the search action is its own button');
t.contains(failedHtml, 'Check URL', 'the manual URL action is a separate button');
const matrixHtml = renderYoutubeRescueControl('release-1', 1, identifier, { outcome: 'ok', youtube_releases: [{ yt_browse_id: 'MPREb_kb5fohQCJ6d', year: 2026, track_count: 1, distances: [{ mbid: identifier, outcome: 'ok', distance: 0, total_mb_tracks: 1 }] }] });
t.contains(matrixHtml, 'window.pickYoutubeRescue(1,', 'matrix choice remains confirm-routed');
t.excludes(matrixHtml, '"window.checkYoutubeRescue("release-1"', 'inline handler quoting remains safe');
t.contains(matrixHtml, 'event.stopPropagation()', 'all inline control interactions stop parent propagation');
t.contains(matrixHtml, 'https://music.youtube.com/browse/MPREb_kb5fohQCJ6d', 'a choice links its exact browse id');
t.contains(matrixHtml, '2026 · 1t · exact dist 0.000', 'a choice carries its exact pressing evidence');
const duplicateHtml = renderYoutubeRescueControl('release-dup', 1, identifier, { outcome: 'ok', youtube_releases: [{ yt_browse_id: 'MPREb_dup', distances: [{ mbid: identifier, outcome: 'ok', distance: 0, total_mb_tracks: 1 }, { mbid: identifier, outcome: 'ok', distance: 0, total_mb_tracks: 1 }] }] });
t.contains(duplicateHtml, 'disabled', 'duplicate exact evidence disables rescue');
t.contains(duplicateHtml, 'Exact evidence required', 'duplicate exact evidence says why it is disabled');
const mixedDuplicateHtml = renderYoutubeRescueControl('release-mixed', 1, identifier, { outcome: 'ok', youtube_releases: [{ yt_browse_id: 'MPREb_mixed', distances: [{ mbid: identifier, outcome: 'ok', distance: 0, total_mb_tracks: 1 }, { mbid: identifier, outcome: 'distance_failed', distance: null, total_mb_tracks: null }] }] });
t.contains(mixedDuplicateHtml, 'disabled', 'one valid plus one invalid exact entry stays disabled');
t.contains(mixedDuplicateHtml, 'Exact evidence required', 'the mixed pair says why it is disabled');

function fakeHost(watchUrl = '') {
  const buttons = [];
  const result = { innerHTML: '', querySelectorAll: () => buttons };
  const input = { value: watchUrl };
  return { dataset: {}, querySelector: (s) => s === 'input' ? input : result, result, buttons };
}

t.section('checkYoutubeRescue() — busy guard and manual URL submission');
{
  const host = fakeHost('https://music.youtube.com/watch?v=dGYXkhMAvLk');
  let calls = 0;
  const resolverBodies = [];
  const globals = stubGlobals({
    document: { getElementById: () => host },
    fetch: async (_url, options) => { calls++; resolverBodies.push(options.body); return { ok: true, status: 200, json: async () => ({ outcome: 'ok', youtube_releases: [] }) }; },
  });
  await Promise.all([checkYoutubeRescue('release-1', 1, identifier), checkYoutubeRescue('release-1', 1, identifier)]);
  t.equal(calls, 1, 'same-instance busy guard suppresses double resolver fire');
  t.equal(resolverBodies[0], JSON.stringify({ identifier }), 'Search YouTube ignores a populated manual URL field');
  t.contains(host.result.innerHTML, 'No YouTube album found', 'successful result replaces result region');
  await checkYoutubeRescue('release-1', 1, identifier, true);
  t.equal(calls, 2, 'a repeated resolver check calls the resolver again');
  t.excludes(host.result.innerHTML, 'No YouTube album foundNo YouTube album found',
    'a repeated resolver check replaces the result region rather than appending');
  t.equal(resolverBodies[1], JSON.stringify({ identifier, watch_url: 'https://music.youtube.com/watch?v=dGYXkhMAvLk' }), 'Check URL submits the pasted URL');
  globals.restore();
}

t.section('checkYoutubeRescue() — a blank Check URL never fetches');
{
  const blankHost = fakeHost('');
  const globals = stubGlobals({
    document: { getElementById: () => blankHost },
    fetch: async () => { throw new Error('blank Check URL must not fetch'); },
  });
  await checkYoutubeRescue('release-blank', 1, identifier, true);
  t.contains(blankHost.result.innerHTML, 'Paste a YouTube video or playlist URL first.', 'blank Check URL gets visible corrective feedback');
  globals.restore();
}

t.section('renderYoutubeRescueControl() — candidate link safety');
const playlistHtml = renderYoutubeRescueControl('release-playlist', 1, identifier, { outcome: 'ok', youtube_releases: [{ yt_browse_id: 'PLC0playlist', yt_url: 'https://music.youtube.com/playlist?list=PLC0playlist', distances: [{ mbid: identifier, outcome: 'ok', distance: 0, total_mb_tracks: 10 }] }] });
t.contains(playlistHtml, 'href="https://music.youtube.com/playlist?list=PLC0playlist"', 'playlist candidate links to its validated playlist URL');
const unsafeLinkHtml = renderYoutubeRescueControl('release-unsafe', 1, identifier, { outcome: 'ok', youtube_releases: [{ yt_browse_id: 'MPREb_safe', yt_url: 'javascript:alert(1)', distances: [{ mbid: identifier, outcome: 'ok', distance: 0, total_mb_tracks: 1 }] }] });
t.contains(unsafeLinkHtml, 'href="https://music.youtube.com/browse/MPREb_safe"', 'untrusted candidate URL falls back to the safe browse-id URL');
t.excludes(unsafeLinkHtml, 'href="javascript:', 'the untrusted javascript: URL never reaches the href');

t.section('checkYoutubeRescue() — a network rejection is visible');
{
  const networkHost = fakeHost();
  const globals = stubGlobals({
    document: { getElementById: () => networkHost },
    fetch: async () => { throw new Error('offline'); },
  });
  await checkYoutubeRescue('release-network', 1, identifier);
  t.contains(networkHost.result.innerHTML, 'Could not reach the resolver', 'resolver network rejection is visible');
  globals.restore();
}

t.section('checkYoutubeRescue() — a stale generation cannot repaint its replacement');
{
  const staleHost = fakeHost();
  let releaseDeferred;
  const globals = stubGlobals({
    document: { getElementById: () => staleHost },
    fetch: () => new Promise((resolve) => { releaseDeferred = resolve; }),
  });
  const staleRun = checkYoutubeRescue('release-stale', 1, identifier);
  staleHost.dataset.generation = '99';
  releaseDeferred({ ok: true, status: 200, json: async () => ({ outcome: 'ok', youtube_releases: [] }) });
  await staleRun;
  t.equal(staleHost.result.innerHTML, '', 'stale detached generation cannot overwrite current result');
  globals.restore();
}

t.section('pickYoutubeRescue() — cancellation is reusable and accept submits once');
{
  // Button cancellation is reusable; accepted submit carries browse_id only
  // and the submitting guard admits one concurrent POST.
  const button = { dataset: { browseId: 'MPREb_kb5fohQCJ6d' }, addEventListener: (_name, listener) => { button.listener = listener; } };
  const submitHost = fakeHost(); submitHost.result.querySelectorAll = () => [button];
  let confirms = false;
  const requests = []; let releaseSubmit;
  const globals = stubGlobals({
    document: { getElementById: (id) => id === 'yt-rescue-release-submit' ? submitHost : { style: {}, textContent: '' } },
    window: { confirm: () => confirms },
    fetch: (url, options) => {
      requests.push({ url, options });
      if (url.endsWith('youtube-album')) return Promise.resolve({ ok: true, status: 200, json: async () => ({ outcome: 'ok', youtube_releases: [{ yt_browse_id: button.dataset.browseId, distances: [{ mbid: identifier, outcome: 'ok', distance: 0, total_mb_tracks: 1 }] }] }) });
      return new Promise((resolve) => { releaseSubmit = resolve; });
    },
  });
  await checkYoutubeRescue('release-submit', 1, identifier);
  await button.listener({ stopPropagation() {} });
  t.equal(requests.length, 1, 'cancel keeps choice active without submit');
  confirms = true;
  const submitA = button.listener({ stopPropagation() {} });
  const submitB = button.listener({ stopPropagation() {} });
  await Promise.resolve();
  t.ok(requests.length === 2 && requests[1].url === '/api/pipeline/1/youtube-rescue' && requests[1].options.body === JSON.stringify({ browse_id: 'MPREb_kb5fohQCJ6d' }), 'accept submits exact browse_id once');
  releaseSubmit({ ok: true, json: async () => ({ outcome: 'accepted' }) });
  await Promise.all([submitA, submitB]);
  globals.restore();
}

t.section('pickYoutubeRescue() — a rejected submit toasts and clears its guard');
{
  const rejectButton = { dataset: { browseId: 'MPREb_kb5fohQCJ6d' }, addEventListener: (_name, listener) => { rejectButton.listener = listener; } };
  const rejectHost = fakeHost(); rejectHost.result.querySelectorAll = () => [rejectButton];
  const toastNode = { style: {}, textContent: '' };
  const globals = stubGlobals({
    document: { getElementById: (id) => id === 'yt-rescue-release-reject' ? rejectHost : toastNode },
    window: { confirm: () => true },
    fetch: (url) => url.endsWith('youtube-album')
      ? Promise.resolve({ ok: true, status: 200, json: async () => ({ outcome: 'ok', youtube_releases: [{ yt_browse_id: rejectButton.dataset.browseId, distances: [{ mbid: identifier, outcome: 'ok', distance: 0, total_mb_tracks: 1 }] }] }) })
      : Promise.reject(new Error('offline')),
  });
  await checkYoutubeRescue('release-reject', 1, identifier);
  await rejectButton.listener({ stopPropagation() {} });
  t.contains(toastNode.textContent, 'network unavailable',
    'a rescue rejection toasts the network failure visibly');
  t.equal(rejectHost.dataset.submitting, 'false',
    'a rescue rejection clears the submit guard for a retry');
  globals.restore();
}

t.done();
