/** Frontend convergence prompt/action contract (#978). */
import assert from 'node:assert/strict';

global.window = {};
global.document = {
  querySelector() { return null; },
};

const {
  convergenceBadge,
  renderConvergencePrompt,
  stopConvergedSearch,
} = await import('../web/js/convergence.js');

const signal = {
  request_id: 41,
  observation_count: 8,
  distinct_peer_count: 6,
  distinct_candidate_snapshot_count: 5,
  cliff_hz: 15000,
  latest_qualifying_log_id: 99,
};

assert.match(convergenceBadge(signal), /search converged/i);
const wanted = renderConvergencePrompt(signal, 'wanted');
assert.match(wanted, /6 distinct peers/i);
assert.match(wanted, /15 kHz/i);
assert.match(wanted, /provisional—not proof/i);
assert.match(wanted, />Stop searching</);
assert.doesNotMatch(wanted, />Accept</);
assert.match(renderConvergencePrompt(signal, 'unsearchable'), /Searching stopped/i);

let request;
global.fetch = async (url, options) => {
  request = { url, options };
  return { ok: true, status: 200, json: async () => ({ outcome: 'stopped' }) };
};
await stopConvergedSearch(signal);
assert.equal(request.url, '/api/triage/41/stop-converged-search');
assert.deepEqual(JSON.parse(request.options.body), {
  confirm: 'STOP', latest_qualifying_log_id: 99, cliff_hz: 15000,
});

console.log('test_js_convergence: all assertions passed');
