/** Current-state library badge quality tests. */

import { renderStatusBadges } from '../web/js/badges.js';

let passed = 0;
let failed = 0;

function assertContains(haystack, needle, message) {
  if (haystack.includes(needle)) passed++;
  else {
    failed++;
    console.error(`  FAIL: ${message} - '${needle}' not in output`);
  }
}

function assertExcludes(haystack, needle, message) {
  if (!haystack.includes(needle)) passed++;
  else {
    failed++;
    console.error(`  FAIL: ${message} - unexpectedly found '${needle}'`);
  }
}

console.log('renderStatusBadges() uses average while retaining the min floor');
{
  const html = renderStatusBadges({
    id: 'request-6039',
    in_library: true,
    library_format: 'MP3',
    library_min_bitrate: 194,
    library_avg_bitrate: 288,
    library_rank: 'transparent',
  });
  assertContains(html, 'in library · M V0', 'avg 288 drives badge label');
  assertContains(html, 'badge-rank-transparent', 'canonical avg rank drives colour');
  assertExcludes(html, 'M V2', 'min 194 does not drive badge label');
}

console.log('renderStatusBadges() escapes fallback quality labels at the badge HTML boundary');
{
  const formats = '</span><img src=x onerror=alert(1)>';
  const html = renderStatusBadges({
    in_library: true,
    library_format: formats,
  });
  assertContains(html, 'in library · &lt;/SPAN&gt;&lt;IMG SRC=X ONERROR=ALERT(1)&gt;',
    'unknown format label is rendered as text');
  assertExcludes(html, formats.toUpperCase(),
    'unknown format label cannot close the badge or create markup');
}

console.log('renderStatusBadges() marks a provisional lossless-source install');
{
  const html = renderStatusBadges({
    id: 'request-3652',
    in_library: true,
    library_format: 'Opus',
    library_avg_bitrate: 102,
    library_rank: 'transparent',
    pipeline_status: 'wanted',
    pipeline_provisional: true,
    pipeline_verified_lossless: false,
  });
  assertContains(html, 'badge-provisional', 'provisional install renders chip');
  assertContains(html, '>provisional<', 'chip label reads provisional');
  assertExcludes(html, 'badge-verified', 'provisional never claims verified');
}

console.log('renderStatusBadges() marks a verified lossless install');
{
  const html = renderStatusBadges({
    id: 'request-8877',
    in_library: true,
    library_format: 'Opus',
    library_avg_bitrate: 131,
    library_rank: 'transparent',
    pipeline_status: 'imported',
    pipeline_verified_lossless: true,
    pipeline_provisional: false,
  });
  assertContains(html, 'badge-verified', 'verified install renders chip');
  assertContains(html, 'badge-rank-lossless',
    'verified identity reuses the brightest lossless bucket colour');
  assertContains(html, '>verified<', 'chip label reads verified');
  assertExcludes(html, 'badge-provisional', 'verified never doubles as provisional');
}

console.log('renderStatusBadges() renders no identity chip without pipeline identity');
{
  const html = renderStatusBadges({
    id: 'request-1',
    in_library: true,
    library_format: 'MP3',
    library_avg_bitrate: 288,
    library_rank: 'transparent',
    pipeline_status: 'wanted',
  });
  assertExcludes(html, 'badge-verified', 'plain install has no verified chip');
  assertExcludes(html, 'badge-provisional', 'plain install has no provisional chip');
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
