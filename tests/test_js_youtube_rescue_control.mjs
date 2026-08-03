import { renderYoutubeRescueControl, youtubeResolverPayload } from '../web/js/youtube_rescue_control.js';

let failed = 0;
function ok(value, message) { if (!value) { failed++; console.error(`FAIL: ${message}`); } }

const identifier = '129bebd8-a7b9-4099-b0bc-545b704e7a95';
ok(JSON.stringify(youtubeResolverPayload(identifier, '')) === JSON.stringify({ identifier }), 'blank watch URL omitted');
ok(JSON.stringify(youtubeResolverPayload(identifier, 'https://music.youtube.com/watch?v=dGYXkhMAvLk')) === JSON.stringify({ identifier, watch_url: 'https://music.youtube.com/watch?v=dGYXkhMAvLk' }), 'canonical watch URL forwarded exactly');

const failedHtml = renderYoutubeRescueControl('release-1', 1, identifier, { outcome: 'transient', error_message: 'mirror down' });
ok(failedHtml.includes('mirror down'), 'resolver failures remain visible');
const matrixHtml = renderYoutubeRescueControl('release-1', 1, identifier, { outcome: 'ok', youtube_releases: [{ yt_browse_id: 'MPREb_kb5fohQCJ6d' }] });
ok(matrixHtml.includes('window.pickYoutubeRescue(1,'), 'matrix choice remains confirm-routed');
ok(!matrixHtml.includes('"window.checkYoutubeRescue("release-1"'), 'inline handler quoting remains safe');

if (failed) process.exit(1);
console.log('4 passed, 0 failed');
