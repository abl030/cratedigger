// @ts-check
import { state, API } from './state.js';
import { esc } from './util.js';

/**
 * Load decision pipeline constants and render the simulator.
 */
export async function loadDecisions() {
  const el = document.getElementById('decisions-content');
  el.innerHTML = '<div class="loading">Loading...</div>';
  try {
    const r = await fetch(`${API}/api/pipeline/constants`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    state.dsConstants = await r.json();
    renderSimulatorForm();
  } catch (e) { el.innerHTML = '<div class="loading">Failed to load</div>'; }
}

/**
 * Render a single decision stage.
 * @param {Object} s - Stage object from the decision tree
 * @param {Object} consts - Constants for placeholder substitution
 * @returns {string} HTML string
 */
export function renderStage(s, consts) {
  const colorCls = c => c === 'green' ? 'dp-out-green' : c === 'red' ? 'dp-out-red' : 'dp-out-amber';
  // Replace {CONSTANT} placeholders with actual values
  function sub(text) {
    return text.replace(/\{(\w+)\}/g, (_, k) => consts[k] != null ? consts[k] : k);
  }
  let html = `<div class="dp-stage">
    <div class="dp-stage-hdr">
      <span class="dp-stage-title">${s.title}</span>
      <span class="dp-stage-fn">${s.function}</span>
    </div>
    <div class="dp-stage-when">${s.when}</div>`;
  for (const r of s.rules) {
    html += `<div class="dp-rule">
      <span class="dp-rule-cond">${sub(r.condition)}</span>
      <span class="dp-rule-result ${colorCls(r.color)}">\u2192 ${sub(r.result)}</span>
    </div>`;
    if (r.effect) html += `<div class="dp-rule"><span class="dp-rule-cond"></span><span class="dp-rule-effect">${r.effect}</span></div>`;
  }
  if (s.note) html += `<div class="dp-note">${sub(s.note)}</div>`;
  html += '</div>';
  return html;
}

/**
 * Render the full decision diagram from the tree data.
 * @param {Object} tree - Decision tree with stages, constants, paths
 * @returns {string} HTML string
 */
export function renderDiagram(tree) {
  const consts = tree.constants;
  const stages = tree.stages;
  const paths = tree.paths || [];
  const labels = tree.path_labels || {};

  // Split stages by path
  const byPath = {};
  const shared = [];
  for (const s of stages) {
    if (s.path === 'shared') { shared.push(s); continue; }
    if (!byPath[s.path]) byPath[s.path] = [];
    byPath[s.path].push(s);
  }

  let html = '';

  // Render branching paths side by side
  if (paths.length > 0) {
    html += '<div style="display:flex;gap:12px;margin-bottom:4px;">';
    for (const p of paths) {
      const pStages = byPath[p] || [];
      html += '<div style="flex:1;">';
      html += `<div style="text-align:center;font-size:0.85em;font-weight:600;color:#888;margin-bottom:6px;">${labels[p] || p}</div>`;
      for (let i = 0; i < pStages.length; i++) {
        if (i > 0) html += '<div class="dp-arrow">\u2502</div>';
        html += renderStage(pStages[i], consts);
      }
      html += '</div>';
    }
    html += '</div>';
  }

  // Merge arrow
  if (paths.length > 0 && shared.length > 0) {
    html += '<div class="dp-arrow">\u2502</div>';
  }

  // Render shared stages
  for (let i = 0; i < shared.length; i++) {
    if (i > 0) html += '<div class="dp-arrow">\u2502</div>';
    html += renderStage(shared[i], consts);
  }

  return html;
}

/**
 * Render the live rank policy as labeled badges.
 *
 * Reads gate_min_rank / bitrate_metric / within_rank_tolerance_kbps from
 * the constants payload so operators see at-a-glance what cfg the
 * deployed `config.ini` is running — the same snapshot the transcode
 * threshold, gate decision, and compare_quality tiebreaker all use
 * (issue #68, following on from #75 which made the API surface
 * coherent). Missing fields render as "?" so the function stays safe
 * against partial payloads during boot or cache invalidation.
 * @param {Object} constants - The `constants` sub-object from
 *   `/api/pipeline/constants`.
 * @returns {string} HTML string — a `.dp-policy` container with three
 *   `.dp-policy-badge` pills.
 */
export function renderPolicyBadges(constants) {
  const c = constants || {};
  const gate = c.rank_gate_min_rank != null ? String(c.rank_gate_min_rank) : '?';
  const metric = c.rank_bitrate_metric != null ? String(c.rank_bitrate_metric) : '?';
  const tol = c.rank_within_tolerance_kbps != null
    ? `${c.rank_within_tolerance_kbps} kbps`
    : '?';
  // Pure HTML: esc() every dynamic value even though the backend types are
  // pinned — defense in depth if the constants payload ever grows richer
  // fields (e.g. custom labels). The title attribute is a hardcoded static
  // string; if you ever interpolate into it, route the value through esc()
  // or a dedicated attribute encoder.
  return `<div class="dp-policy" title="Live rank policy from config.ini">
    <span class="dp-policy-badge">
      <span class="dp-policy-badge-label">Gate min rank</span>
      <span class="dp-policy-badge-value">${esc(gate)}</span>
    </span>
    <span class="dp-policy-badge">
      <span class="dp-policy-badge-label">Bitrate metric</span>
      <span class="dp-policy-badge-value">${esc(metric)}</span>
    </span>
    <span class="dp-policy-badge">
      <span class="dp-policy-badge-label">Within-rank tolerance</span>
      <span class="dp-policy-badge-value">${esc(tol)}</span>
    </span>
  </div>`;
}

/**
 * Render the simulator form with diagram and input fields.
 */
export function renderSimulatorForm() {
  const el = document.getElementById('decisions-content');
  const c = state.dsConstants.constants;
  const diagram = renderDiagram(state.dsConstants);
  const policy = renderPolicyBadges(c);

  el.innerHTML = `<div class="ds">
    <div class="ds-title">Decision Pipeline</div>
    ${policy}
    ${diagram}
    <div class="dp-section-title" style="margin-top:24px;">Simulator</div>
    <div style="color:#666;font-size:0.85em;margin-bottom:12px;">
      Calls the real decision functions with your inputs.
    </div>
    <div class="ds-presets">
      <span class="ds-preset" onclick="window.dsPreset('virginia')">Virginia EP (lo-fi FLAC)</span>
      <span class="ds-preset" onclick="window.dsPreset('mtngoats')">Mtn Goats (fake 320)</span>
      <span class="ds-preset" onclick="window.dsPreset('genuine_flac')">Genuine FLAC</span>
      <span class="ds-preset" onclick="window.dsPreset('cbr320')">CBR 320 (no spectral)</span>
      <span class="ds-preset" onclick="window.dsPreset('vbr_v0')">VBR V0 MP3</span>
      <span class="ds-preset" onclick="window.dsPreset('vbr_transcode')">VBR Transcode (#93)</span>
    </div>
    <div class="ds-form" id="ds-form">
      <div class="ds-field">
        <label>File type</label>
        <select id="ds-is_flac">
          <option value="false">MP3</option>
          <option value="true">FLAC</option>
        </select>
      </div>
      <div class="ds-field">
        <label>Min bitrate (kbps)</label>
        <input type="number" id="ds-min_bitrate" placeholder="e.g. 256">
      </div>
      <div class="ds-field">
        <label>CBR?</label>
        <select id="ds-is_cbr">
          <option value="false">No (VBR)</option>
          <option value="true">Yes (CBR)</option>
        </select>
      </div>
      <div class="ds-field">
        <label>Avg bitrate (kbps, VBR gate)</label>
        <input type="number" id="ds-avg_bitrate" placeholder="gate threshold 210">
      </div>
      <div class="ds-field">
        <label>Spectral grade</label>
        <select id="ds-spectral_grade">
          <option value="">(none)</option>
          <option value="genuine">genuine</option>
          <option value="marginal">marginal</option>
          <option value="suspect">suspect</option>
          <option value="likely_transcode">likely_transcode</option>
        </select>
      </div>
      <div class="ds-field">
        <label>Spectral bitrate (kbps)</label>
        <input type="number" id="ds-spectral_bitrate" placeholder="cliff estimate">
      </div>
      <div class="ds-field">
        <label>Existing min bitrate (kbps)</label>
        <input type="number" id="ds-existing_min_bitrate" placeholder="in beets">
      </div>
      <div class="ds-field">
        <label>Existing avg bitrate (kbps)</label>
        <input type="number" id="ds-existing_avg_bitrate" placeholder="AVG policy">
      </div>
      <div class="ds-field">
        <label>Existing spectral bitrate</label>
        <input type="number" id="ds-existing_spectral_bitrate" placeholder="on disk">
      </div>
      <div class="ds-field">
        <label>Override min bitrate</label>
        <input type="number" id="ds-override_min_bitrate" placeholder="pipeline DB">
      </div>
      <div class="ds-field">
        <label>Post-conversion bitrate</label>
        <input type="number" id="ds-post_conversion_min_bitrate" placeholder="after FLAC\u2192V0">
      </div>
      <div class="ds-field">
        <label>Converted count</label>
        <input type="number" id="ds-converted_count" placeholder="FLAC files" value="0">
      </div>
      <div class="ds-field">
        <label>Verified lossless (existing)</label>
        <select id="ds-verified_lossless">
          <option value="false">No</option>
          <option value="true">Yes</option>
        </select>
      </div>
      <div class="ds-field">
        <label>Target format on disk</label>
        <select id="ds-target_format">
          <option value="">Default (convert if needed)</option>
          <option value="flac">flac</option>
        </select>
      </div>
      <div class="ds-field">
        <label>Verified lossless target</label>
        <input type="text" id="ds-verified_lossless_target" placeholder="e.g. opus 128">
      </div>
      <div class="ds-run"><button onclick="window.runSimulator()">Run Pipeline</button></div>
    </div>
    <div id="ds-results"></div>
  </div>`;
}

/** @type {Object<string, Object<string, string>>} */
// Every preset explicitly sets avg_bitrate so switching presets resets
// the field — a stale value from a prior run would otherwise produce the
// wrong stage0_spectral_gate decision (issue #93 codex round 3).
// FLAC presets set avg_bitrate='' since the gate skips FLAC unconditionally.
export const DS_PRESETS = {
  virginia: {
    is_flac: 'true', min_bitrate: '', is_cbr: 'false', avg_bitrate: '',
    spectral_grade: 'genuine', spectral_bitrate: '',
    existing_min_bitrate: '192', existing_avg_bitrate: '',
    existing_spectral_bitrate: '',
    override_min_bitrate: '', post_conversion_min_bitrate: '209',
    converted_count: '12', verified_lossless: 'false',
    target_format: '', verified_lossless_target: '',
  },
  mtngoats: {
    is_flac: 'false', min_bitrate: '138', is_cbr: 'false', avg_bitrate: '138',
    spectral_grade: 'genuine', spectral_bitrate: '128',
    existing_min_bitrate: '173', existing_avg_bitrate: '',
    existing_spectral_bitrate: '128',
    override_min_bitrate: '320', post_conversion_min_bitrate: '',
    converted_count: '0', verified_lossless: 'false',
    target_format: '', verified_lossless_target: '',
  },
  genuine_flac: {
    is_flac: 'true', min_bitrate: '', is_cbr: 'false', avg_bitrate: '',
    spectral_grade: 'genuine', spectral_bitrate: '',
    existing_min_bitrate: '192', existing_avg_bitrate: '',
    existing_spectral_bitrate: '',
    override_min_bitrate: '', post_conversion_min_bitrate: '245',
    converted_count: '12', verified_lossless: 'false',
    target_format: '', verified_lossless_target: 'opus 128',
  },
  cbr320: {
    is_flac: 'false', min_bitrate: '320', is_cbr: 'true', avg_bitrate: '320',
    spectral_grade: '', spectral_bitrate: '',
    existing_min_bitrate: '', existing_avg_bitrate: '',
    existing_spectral_bitrate: '',
    override_min_bitrate: '', post_conversion_min_bitrate: '',
    converted_count: '0', verified_lossless: 'false',
    target_format: '', verified_lossless_target: '',
  },
  vbr_v0: {
    // Genuine V0: avg 245 >= threshold → stage 0 = skipped_vbr_high_avg.
    is_flac: 'false', min_bitrate: '245', is_cbr: 'false', avg_bitrate: '245',
    spectral_grade: '', spectral_bitrate: '',
    existing_min_bitrate: '', existing_avg_bitrate: '',
    existing_spectral_bitrate: '',
    override_min_bitrate: '', post_conversion_min_bitrate: '',
    converted_count: '0', verified_lossless: 'false',
    target_format: '', verified_lossless_target: '',
  },
  vbr_transcode: {
    // Go! Team shape (issue #93): avg 182 < threshold → stage 0 = would_run,
    // then stage 1 rejects on the supplied likely_transcode spectral.
    is_flac: 'false', min_bitrate: '126', is_cbr: 'false', avg_bitrate: '182',
    spectral_grade: 'likely_transcode', spectral_bitrate: '96',
    existing_min_bitrate: '', existing_avg_bitrate: '',
    existing_spectral_bitrate: '',
    override_min_bitrate: '', post_conversion_min_bitrate: '',
    converted_count: '0', verified_lossless: 'false',
    target_format: '', verified_lossless_target: '',
  },
};

/**
 * Apply a preset to the simulator form and run.
 * @param {string} name - Preset key
 */
export function dsPreset(name) {
  const p = DS_PRESETS[name];
  if (!p) return;
  for (const [key, val] of Object.entries(p)) {
    const el = document.getElementById('ds-' + key);
    if (el) /** @type {HTMLInputElement} */ (el).value = val;
  }
  runSimulator();
}

/**
 * Run the decision simulator with current form values.
 */
export async function runSimulator() {
  const fields = ['is_flac','min_bitrate','is_cbr','avg_bitrate',
    'spectral_grade','spectral_bitrate',
    'existing_min_bitrate','existing_avg_bitrate',
    'existing_spectral_bitrate','override_min_bitrate',
    'post_conversion_min_bitrate','converted_count','verified_lossless',
    'target_format','verified_lossless_target'];
  const params = new URLSearchParams();
  for (const f of fields) {
    const v = /** @type {HTMLInputElement|null} */ (document.getElementById('ds-' + f))?.value;
    if (v) params.set(f, v);
  }
  const el = document.getElementById('ds-results');
  el.innerHTML = '<div class="loading">Running...</div>';
  try {
    const r = await fetch(`${API}/api/pipeline/simulate?${params}`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    renderSimulatorResults(data);
  } catch (e) { el.innerHTML = '<div class="loading">Failed</div>'; }
}

/**
 * Render simulator results.
 * @param {Object} r - Simulator response
 */
export function renderSimulatorResults(r) {
  const el = document.getElementById('ds-results');

  function stageColor(val) {
    if (!val) return 'ds-skip';
    if (['import', 'import_upgrade', 'import_no_exist', 'accept',
         'preflight_existing',
         'skipped_vbr_high_avg', 'skipped_flac'].includes(val)) return 'ds-green';
    if (['reject', 'downgrade', 'transcode_downgrade'].includes(val)) return 'ds-red';
    return 'ds-amber';
  }
  function stageHtml(title, val) {
    const cls = stageColor(val);
    const display = val ? `<span class="ds-outcome ${cls}">${val}</span>` : '<span class="ds-skip">skipped</span>';
    return `<div class="ds-stage"><div class="ds-stage-title">${title}</div><div class="ds-stage-value">${display}</div></div>`;
  }
  function boolBadge(val, trueLabel, falseLabel) {
    return val
      ? `<span class="ds-outcome ds-green">${trueLabel}</span>`
      : `<span class="ds-outcome ds-red">${falseLabel}</span>`;
  }

  let html = '<div class="ds-results">';
  html += stageHtml('Stage 0: Spectral Gate Trigger', r.stage0_spectral_gate);
  html += stageHtml('Stage 1: Pre-import Spectral', r.stage1_spectral);
  html += stageHtml('Stage 2: Import Decision', r.stage2_import);
  html += stageHtml('Stage 3: Quality Gate', r.stage3_quality_gate);

  html += `<div class="ds-summary">
    <div class="ds-summary-row"><span class="ds-summary-label">Final status</span>
      <span class="ds-outcome ${r.final_status === 'imported' ? 'ds-green' : 'ds-amber'}">${r.final_status || 'none'}</span></div>
    <div class="ds-summary-row"><span class="ds-summary-label">Imported to beets?</span>
      ${boolBadge(r.imported, 'yes', 'no')}</div>
    <div class="ds-summary-row"><span class="ds-summary-label">Final target format</span>
      <span class="ds-outcome ${r.target_final_format ? 'ds-green' : 'ds-skip'}">${esc(r.target_final_format || 'keep V0/default')}</span></div>
    <div class="ds-summary-row"><span class="ds-summary-label">Source denylisted?</span>
      ${boolBadge(!r.denylisted, 'no', 'yes')}</div>
    <div class="ds-summary-row"><span class="ds-summary-label">Keep searching?</span>
      ${r.keep_searching ? '<span class="ds-outcome ds-amber">yes</span>' : '<span class="ds-outcome ds-green">no (done)</span>'}</div>
  </div>`;

  html += '</div>';
  el.innerHTML = html;
}
