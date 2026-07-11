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

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
