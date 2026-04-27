/**
 * Unit tests for web/js/decisions.js.
 * Run with: node tests/test_js_decisions.mjs
 *
 * Scope: covers renderPolicyBadges (issue #68), Decisions-tab refresh,
 * simulator serialization, and selected runnable presets. The diagram
 * layout exports are still exercised end-to-end via the
 * test_pipeline_constants_contract route test plus live deploy verification
 * of the Decisions tab. Future PRs that change the stage/diagram layout
 * should consider adding dedicated unit tests here.
 */

import { loadDecisions, renderPolicyBadges, DS_PRESETS, dsPreset, runSimulator, renderSimulatorResults } from '../web/js/decisions.js';
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

const SIMULATOR_FIELDS = [
  'is_flac', 'min_bitrate', 'is_cbr', 'avg_bitrate',
  'spectral_grade', 'spectral_bitrate',
  'existing_min_bitrate', 'existing_avg_bitrate',
  'existing_spectral_grade', 'existing_spectral_bitrate',
  'override_min_bitrate', 'post_conversion_min_bitrate',
  'converted_count', 'verified_lossless',
  'candidate_v0_probe_avg', 'existing_v0_probe_avg',
  'supported_lossless_source',
  'target_format', 'verified_lossless_target',
  'audio_check_mode', 'audio_corrupt',
  'import_mode', 'has_nested_audio',
];

function installSimulatorDom() {
  const resultsEl = { innerHTML: '' };
  const fields = {};
  for (const name of SIMULATOR_FIELDS) fields[name] = { value: 'stale' };
  global.document = {
    getElementById(id) {
      if (id === 'ds-results') return resultsEl;
      if (!id.startsWith('ds-')) return null;
      const key = id.slice(3);
      return fields[key] || null;
    },
  };
  return { fields, resultsEl };
}

function queryParams(url) {
  return Object.fromEntries(new URL(url, 'http://example.test').searchParams.entries());
}

function assertQueryEquals(actualUrl, expected, msg) {
  const actual = queryParams(actualUrl);
  const actualJson = JSON.stringify(actual, Object.keys(actual).sort());
  const expectedJson = JSON.stringify(expected, Object.keys(expected).sort());
  assert(actualJson === expectedJson,
         `${msg} — expected ${expectedJson}, got ${actualJson}`);
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
assertContains(decisionsEl.innerHTML, "window.dsPreset('provisional_bride')",
  'Decisions tab renders the Mountain Goats / Bride live preset control');
assertContains(decisionsEl.innerHTML, "window.dsPreset('provisional_creek_reject')",
  'Decisions tab renders the Iron & Wine / Creek live preset control');

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
  assert('existing_spectral_grade' in preset,
         `preset "${name}" missing existing_spectral_grade — stale field inherited from prior preset`);
  assert('candidate_v0_probe_avg' in preset,
         `preset "${name}" missing candidate_v0_probe_avg — stale probe inherited from prior preset`);
  assert('existing_v0_probe_avg' in preset,
         `preset "${name}" missing existing_v0_probe_avg — stale probe inherited from prior preset`);
  assert('supported_lossless_source' in preset,
         `preset "${name}" missing supported_lossless_source — stale probe source flag inherited`);
}

// The vbr_v0 preset must represent genuine V0 (high avg → gate skips)
assert(DS_PRESETS.vbr_v0.avg_bitrate === '245',
       `vbr_v0 preset must have avg_bitrate='245' (genuine V0), got ${DS_PRESETS.vbr_v0.avg_bitrate}`);

// The vbr_transcode preset must trigger the gate (low avg)
assert(DS_PRESETS.vbr_transcode !== undefined,
       'vbr_transcode preset missing — documents the Go! Team shape from issue #93');
assert(DS_PRESETS.vbr_transcode.avg_bitrate === '182',
       `vbr_transcode preset must have avg_bitrate='182' (below 210 threshold), got ${DS_PRESETS.vbr_transcode.avg_bitrate}`);

// Live provisional-lossless examples from 2026-04-27 stay pinned as
// runnable presets so the Decisions tab documents the real V0-probe shapes.
assert(DS_PRESETS.provisional_bride !== undefined,
       'provisional_bride preset missing — documents Mountain Goats / Bride live source');
assert(DS_PRESETS.provisional_bride.spectral_grade === 'likely_transcode',
       `provisional_bride spectral_grade must be likely_transcode, got ${DS_PRESETS.provisional_bride.spectral_grade}`);
assert(DS_PRESETS.provisional_bride.candidate_v0_probe_avg === '214',
       `provisional_bride candidate_v0_probe_avg must be 214, got ${DS_PRESETS.provisional_bride.candidate_v0_probe_avg}`);
assert(DS_PRESETS.provisional_bride.existing_v0_probe_avg === '',
       `provisional_bride existing_v0_probe_avg must be empty, got ${DS_PRESETS.provisional_bride.existing_v0_probe_avg}`);
assert(DS_PRESETS.provisional_bride.verified_lossless_target === 'opus 128',
       `provisional_bride verified_lossless_target must be opus 128, got ${DS_PRESETS.provisional_bride.verified_lossless_target}`);

assert(DS_PRESETS.provisional_creek_reject !== undefined,
       'provisional_creek_reject preset missing — documents Iron & Wine / The Creek Drank the Cradle live reject');
assert(DS_PRESETS.provisional_creek_reject.spectral_grade === 'likely_transcode',
       `provisional_creek_reject spectral_grade must be likely_transcode, got ${DS_PRESETS.provisional_creek_reject.spectral_grade}`);
assert(DS_PRESETS.provisional_creek_reject.spectral_bitrate === '96',
       `provisional_creek_reject spectral_bitrate must be 96, got ${DS_PRESETS.provisional_creek_reject.spectral_bitrate}`);
assert(DS_PRESETS.provisional_creek_reject.candidate_v0_probe_avg === '171',
       `provisional_creek_reject candidate_v0_probe_avg must be 171, got ${DS_PRESETS.provisional_creek_reject.candidate_v0_probe_avg}`);
assert(DS_PRESETS.provisional_creek_reject.existing_v0_probe_avg === '228',
       `provisional_creek_reject existing_v0_probe_avg must be 228, got ${DS_PRESETS.provisional_creek_reject.existing_v0_probe_avg}`);
assert(DS_PRESETS.provisional_creek_reject.existing_spectral_grade === 'likely_transcode',
       `provisional_creek_reject existing_spectral_grade must be likely_transcode, got ${DS_PRESETS.provisional_creek_reject.existing_spectral_grade}`);

console.log('\nrunSimulator()');
{
  installSimulatorDom();
  const values = {
    is_flac: 'false',
    min_bitrate: '171',
    is_cbr: 'false',
    avg_bitrate: '196',
    spectral_grade: 'likely_transcode',
    spectral_bitrate: '160',
    existing_min_bitrate: '246',
    existing_avg_bitrate: '261',
    existing_spectral_grade: 'genuine',
    existing_spectral_bitrate: '128',
    override_min_bitrate: '',
    post_conversion_min_bitrate: '',
    converted_count: '0',
    verified_lossless: 'false',
    candidate_v0_probe_avg: '228',
    existing_v0_probe_avg: '171',
    supported_lossless_source: 'true',
    target_format: '',
    verified_lossless_target: '',
    audio_check_mode: 'normal',
    audio_corrupt: 'false',
    import_mode: 'auto',
    has_nested_audio: 'false',
  };
  for (const [key, value] of Object.entries(values)) {
    document.getElementById('ds-' + key).value = value;
  }
  let fetchedUrl = '';
  global.fetch = async (url) => {
    fetchedUrl = url;
    return {
      ok: true,
      async json() {
        return {
          preimport_audio: 'pass',
          preimport_nested: 'skipped_auto',
          stage0_spectral_gate: 'would_run',
          stage1_spectral: 'import_upgrade',
          stage2_import: 'downgrade',
          stage3_quality_gate: null,
          final_status: 'imported',
          imported: false,
          denylisted: false,
          keep_searching: true,
        };
      },
    };
  };

  await runSimulator();
  assertContains(fetchedUrl, 'candidate_v0_probe_avg=228',
    'runSimulator serializes candidate V0 probe average');
  assertContains(fetchedUrl, 'existing_v0_probe_avg=171',
    'runSimulator serializes existing V0 probe average');
  assertContains(fetchedUrl, 'supported_lossless_source=true',
    'runSimulator serializes supported lossless source flag');
  assertContains(fetchedUrl, 'existing_spectral_grade=genuine',
    'runSimulator serializes existing spectral grade');
  assertContains(fetchedUrl, 'existing_spectral_bitrate=128',
    'runSimulator still serializes existing spectral bitrate');
}

console.log('\ndsPreset() live examples');
{
  installSimulatorDom();
  let fetchedUrl = '';
  global.fetch = async (url) => {
    fetchedUrl = url;
    return {
      ok: true,
      async json() {
        return {
          preimport_audio: 'pass',
          preimport_nested: 'skipped_auto',
          stage0_spectral_gate: 'skipped_flac',
          stage1_spectral: 'import_no_exist',
          stage2_import: 'provisional_lossless_upgrade',
          stage3_quality_gate: null,
          final_status: 'wanted',
          imported: true,
          denylisted: true,
          keep_searching: true,
          target_final_format: 'opus 128',
          verified_lossless: false,
        };
      },
    };
  };

  dsPreset('provisional_bride');
  await Promise.resolve();
  await Promise.resolve();
  assertQueryEquals(fetchedUrl, {
    is_flac: 'true',
    is_cbr: 'false',
    spectral_grade: 'likely_transcode',
    existing_min_bitrate: '320',
    existing_avg_bitrate: '320',
    post_conversion_min_bitrate: '214',
    converted_count: '1',
    verified_lossless: 'false',
    candidate_v0_probe_avg: '214',
    supported_lossless_source: 'true',
    verified_lossless_target: 'opus 128',
    audio_check_mode: 'normal',
    audio_corrupt: 'false',
    import_mode: 'auto',
    has_nested_audio: 'false',
  }, 'provisional_bride submits the live Mountain Goats / Bride query');
  assertContains(document.getElementById('ds-results').innerHTML,
    'provisional_lossless_upgrade',
    'provisional_bride renders the mocked provisional upgrade result');
}

{
  installSimulatorDom();
  let fetchedUrl = '';
  global.fetch = async (url) => {
    fetchedUrl = url;
    return {
      ok: true,
      async json() {
        return {
          preimport_audio: 'pass',
          preimport_nested: 'skipped_auto',
          stage0_spectral_gate: 'skipped_flac',
          stage1_spectral: 'reject',
          stage2_import: 'suspect_lossless_downgrade',
          stage3_quality_gate: null,
          final_status: 'wanted',
          imported: false,
          denylisted: true,
          keep_searching: true,
          target_final_format: null,
          verified_lossless: false,
        };
      },
    };
  };

  dsPreset('provisional_creek_reject');
  await Promise.resolve();
  await Promise.resolve();
  assertQueryEquals(fetchedUrl, {
    is_flac: 'true',
    is_cbr: 'false',
    spectral_grade: 'likely_transcode',
    spectral_bitrate: '96',
    existing_min_bitrate: '220',
    existing_avg_bitrate: '228',
    existing_spectral_grade: 'likely_transcode',
    existing_spectral_bitrate: '96',
    post_conversion_min_bitrate: '165',
    converted_count: '11',
    verified_lossless: 'false',
    candidate_v0_probe_avg: '171',
    existing_v0_probe_avg: '228',
    supported_lossless_source: 'true',
    verified_lossless_target: 'opus 128',
    audio_check_mode: 'normal',
    audio_corrupt: 'false',
    import_mode: 'auto',
    has_nested_audio: 'false',
  }, 'provisional_creek_reject submits the live Iron & Wine / Creek query');
  assertContains(document.getElementById('ds-results').innerHTML,
    'suspect_lossless_downgrade',
    'provisional_creek_reject renders the mocked source-downgrade result');
}

{
  const resultsEl = { innerHTML: '' };
  global.document = {
    getElementById(id) {
      return id === 'ds-results' ? resultsEl : null;
    },
  };
  renderSimulatorResults({
    preimport_audio: 'pass',
    preimport_nested: 'skipped_auto',
    stage0_spectral_gate: 'skipped_flac',
    stage1_spectral: 'reject',
    stage2_import: 'suspect_lossless_probe_missing',
    stage3_quality_gate: null,
    final_status: 'wanted',
    imported: false,
    denylisted: true,
    keep_searching: true,
  });
  assertContains(resultsEl.innerHTML,
    '<span class="ds-outcome ds-red">suspect_lossless_probe_missing</span>',
    'renderSimulatorResults marks suspect lossless missing-probe as red');
}

// --- Summary ---
console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
