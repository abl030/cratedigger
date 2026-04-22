/**
 * Unit tests for web/js/library.js pure helpers.
 * Run with: node tests/test_js_library.mjs
 */

import { buildDeleteConfirmHtml } from '../web/js/library.js';

let passed = 0;
let failed = 0;

function assertContains(haystack, needle, msg) {
  if (haystack.includes(needle)) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg} — '${needle}' not in output`);
  }
}

function assertExcludes(haystack, needle, msg) {
  if (!haystack.includes(needle)) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg} — unexpectedly found '${needle}'`);
  }
}

console.log('buildDeleteConfirmHtml() escapes user-visible text and JS args');
{
  const html = buildDeleteConfirmHtml(
    42,
    'Mum & Dad',
    "Kid A's <special>",
    10,
    1712,
    "rel-10'oops",
  );
  assertContains(html, 'Mum &amp; Dad - Kid A&#39;s &lt;special&gt;', 'artist/album escaped in overlay body');
  assertContains(html, 'window.executeBeetsDeletion(42, this, 1712, &quot;rel-10&#39;oops&quot;)', 'release id encoded as JS string arg');
  assertContains(html, 'matching pipeline request/history', 'pipeline note rendered when release id provided');
}

console.log('buildDeleteConfirmHtml() omits pipeline note without release id');
{
  const html = buildDeleteConfirmHtml(7, 'Bodyjar', 'Plastic Skies', 12, null, '');
  assertContains(html, 'window.executeBeetsDeletion(7, this, null, &quot;&quot;)', 'empty release id still encoded safely');
  assertExcludes(html, 'matching pipeline request/history', 'no pipeline note without release id');
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
