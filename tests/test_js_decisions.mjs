/**
 * Unit tests for web/js/decisions.js — specifically renderPolicyBadges.
 * Run with: node tests/test_js_decisions.mjs
 *
 * Scope: covers renderPolicyBadges (issue #68) only. The other pure
 * exports in decisions.js — renderDiagram and renderStage — are exercised
 * end-to-end via the test_pipeline_constants_contract route test plus
 * live deploy verification of the Decisions tab. Future PRs that change
 * the stage/diagram layout should consider adding dedicated unit tests
 * here alongside renderPolicyBadges. DOM-touching entry points
 * (loadDecisions, renderSimulatorForm) stay deferred to live deploy.
 */

import { loadDecisions, renderPolicyBadges, DS_PRESETS } from '../web/js/decisions.js';
import { state } from '../web/js/state.js';

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

// Revisit behavior — opening the Decisions tab twice must refetch constants
// so runtime config changes show up without a full page reload (issue #68).
console.log('\nloadDecisions()');
const decisionsEl = { innerHTML: '' };
global.document = {
  getElementById(id) {
    return id === 'decisions-content' ? decisionsEl : null;
  },
};
const payloads = [
  {
    constants: {
      rank_gate_min_rank: 'EXCELLENT',
      rank_bitrate_metric: 'avg',
      rank_within_tolerance_kbps: 5,
    },
    stages: [],
    paths: [],
    path_labels: {},
  },
  {
    constants: {
      rank_gate_min_rank: 'GOOD',
      rank_bitrate_metric: 'median',
      rank_within_tolerance_kbps: 10,
    },
    stages: [],
    paths: [],
    path_labels: {},
  },
];
let fetchCalls = 0;
global.fetch = async () => {
  const payload = payloads[fetchCalls];
  fetchCalls++;
  return {
    ok: true,
    async json() { return payload; },
  };
};
state.dsConstants = null;
await loadDecisions();
await loadDecisions();
assert(fetchCalls === 2, `expected loadDecisions() to fetch twice, got ${fetchCalls}`);
assertContains(decisionsEl.innerHTML, 'GOOD', 'second tab open renders fresh gate rank');
assertContains(decisionsEl.innerHTML, '10 kbps', 'second tab open renders fresh tolerance');

// --- DS_PRESETS contract: avg_bitrate must be explicit in every preset ---
// Issue #93 round 3: presets that omit avg_bitrate inherit a stale value
// from a prior run, silently producing the wrong stage0_spectral_gate.
// This pins the contract: every preset sets avg_bitrate (even to '' for FLAC).
console.log('\nDS_PRESETS contract');
for (const [name, preset] of Object.entries(DS_PRESETS)) {
  assert('avg_bitrate' in preset,
         `preset "${name}" missing avg_bitrate — stale field inherited from prior preset`);
  assert('existing_avg_bitrate' in preset,
         `preset "${name}" missing existing_avg_bitrate — stale field inherited from prior preset`);
}

// The vbr_v0 preset must represent genuine V0 (high avg → gate skips)
assert(DS_PRESETS.vbr_v0.avg_bitrate === '245',
       `vbr_v0 preset must have avg_bitrate='245' (genuine V0), got ${DS_PRESETS.vbr_v0.avg_bitrate}`);

// The vbr_transcode preset must trigger the gate (low avg)
assert(DS_PRESETS.vbr_transcode !== undefined,
       'vbr_transcode preset missing — documents the Go! Team shape from issue #93');
assert(DS_PRESETS.vbr_transcode.avg_bitrate === '182',
       `vbr_transcode preset must have avg_bitrate='182' (below 210 threshold), got ${DS_PRESETS.vbr_transcode.avg_bitrate}`);

// --- Summary ---
console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
