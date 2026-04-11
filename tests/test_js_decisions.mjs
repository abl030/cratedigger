/**
 * Unit tests for web/js/decisions.js — pure HTML-rendering functions.
 * Run with: node tests/test_js_decisions.mjs
 *
 * These cover the pure functions exported from decisions.js. DOM-touching
 * entry points (loadDecisions, renderSimulatorForm) are exercised via live
 * deploy, not unit tests — the pure helpers they call are covered here.
 */

import { renderPolicyBadges } from '../web/js/decisions.js';

let passed = 0;
let failed = 0;

function assert(condition, msg) {
  if (condition) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg}`);
  }
}

function assertContains(haystack, needle, msg) {
  if (haystack.includes(needle)) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg} — expected to contain ${JSON.stringify(needle)}\n    in: ${haystack}`);
  }
}

function assertNotContains(haystack, needle, msg) {
  if (!haystack.includes(needle)) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg} — expected NOT to contain ${JSON.stringify(needle)}\n    in: ${haystack}`);
  }
}

// --- renderPolicyBadges tests ---
console.log('renderPolicyBadges()');

// Happy path: all three fields present, default cfg
const defaultHtml = renderPolicyBadges({
  rank_gate_min_rank: 'EXCELLENT',
  rank_bitrate_metric: 'avg',
  rank_within_tolerance_kbps: 5,
});
assertContains(defaultHtml, 'class="dp-policy"', 'wraps in .dp-policy container');
assertContains(defaultHtml, 'Gate min rank', 'label present');
assertContains(defaultHtml, 'EXCELLENT', 'default gate rank rendered');
assertContains(defaultHtml, 'Bitrate metric', 'metric label present');
assertContains(defaultHtml, 'avg', 'default avg metric rendered');
assertContains(defaultHtml, 'Within-rank tolerance', 'tolerance label present');
assertContains(defaultHtml, '5 kbps', 'default tolerance rendered with unit');

// Custom cfg: median metric, lower gate, larger tolerance
const customHtml = renderPolicyBadges({
  rank_gate_min_rank: 'GOOD',
  rank_bitrate_metric: 'median',
  rank_within_tolerance_kbps: 12,
});
assertContains(customHtml, 'GOOD', 'custom gate rank surfaced');
assertContains(customHtml, 'median', 'custom MEDIAN metric surfaced');
assertContains(customHtml, '12 kbps', 'custom tolerance surfaced');
assertNotContains(customHtml, 'EXCELLENT', 'custom cfg does not leak default gate');
assertNotContains(customHtml, '>avg<', 'custom cfg does not leak default metric');

// Zero tolerance — must still render, not fall through to "?"
const zeroTolHtml = renderPolicyBadges({
  rank_gate_min_rank: 'TRANSPARENT',
  rank_bitrate_metric: 'min',
  rank_within_tolerance_kbps: 0,
});
assertContains(zeroTolHtml, '0 kbps', 'zero tolerance still renders (not falsy trap)');
assertContains(zeroTolHtml, 'TRANSPARENT', 'TRANSPARENT rank surfaced');

// Missing fields fall through to "?" (defensive during boot / stale cache)
const emptyHtml = renderPolicyBadges({});
assertContains(emptyHtml, '?', 'missing fields render as ?');
assertContains(emptyHtml, 'class="dp-policy"', 'empty payload still renders container');
// All three badges present even when empty
const qmarkCount = (emptyHtml.match(/\?/g) || []).length;
assert(qmarkCount >= 3, `expected >=3 "?" placeholders for missing fields, got ${qmarkCount}`);

// Null / undefined argument — must not throw
const nullHtml = renderPolicyBadges(null);
assertContains(nullHtml, 'class="dp-policy"', 'null payload renders container');
const undefHtml = renderPolicyBadges(undefined);
assertContains(undefHtml, 'class="dp-policy"', 'undefined payload renders container');

// HTML escaping — defense in depth against a mischievous backend
const xssHtml = renderPolicyBadges({
  rank_gate_min_rank: '<script>alert(1)</script>',
  rank_bitrate_metric: 'a & b',
  rank_within_tolerance_kbps: 5,
});
assertNotContains(xssHtml, '<script>alert(1)</script>', 'raw script tag must be escaped');
assertContains(xssHtml, '&lt;script&gt;', 'script tag becomes &lt;script&gt;');
assertContains(xssHtml, 'a &amp; b', 'ampersand escaped');

// --- Summary ---
console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
