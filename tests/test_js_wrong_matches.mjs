/**
 * Unit tests for web/js/wrong-matches.js polling behavior.
 * Run with: node tests/test_js_wrong_matches.mjs
 */

import { __test__ } from '../web/js/wrong-matches.js';

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

function assertEqual(actual, expected, msg) {
  if (actual === expected) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg} — expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
}

function installDom() {
  const wrongMatches = { innerHTML: '' };
  const toast = {
    textContent: '',
    className: '',
    style: { display: 'none' },
  };
  globalThis.document = {
    getElementById(id) {
      if (id === 'wrong-matches-content') return wrongMatches;
      if (id === 'toast') return toast;
      return null;
    },
  };
  globalThis.setTimeout = (fn) => {
    fn();
    return 0;
  };
  return { wrongMatches, toast };
}

async function runPoll(job) {
  const calls = [];
  const dom = installDom();
  globalThis.fetch = async (url) => {
    calls.push(url);
    if (String(url).startsWith('/api/import-jobs/')) {
      return {
        ok: true,
        json: async () => ({ job }),
      };
    }
    if (url === '/api/wrong-matches') {
      return {
        ok: true,
        json: async () => ({ groups: [] }),
      };
    }
    throw new Error(`unexpected fetch: ${url}`);
  };
  const btn = { textContent: '', style: {} };
  await __test__.pollImportJob(17, btn);
  return { calls, dom, btn };
}

console.log('_pollImportJob() refreshes after completed jobs');
{
  const { calls, dom, btn } = await runPoll({
    status: 'completed',
    message: 'Import completed',
  });
  assertEqual(btn.textContent, 'Imported', 'button shows imported');
  assert(calls.includes('/api/wrong-matches'), 'refreshes wrong matches after completion');
  assert(dom.wrongMatches.innerHTML.includes('No wrong matches'), 'renders refreshed empty state');
  assertEqual(dom.toast.className, 'toast', 'completion toast is not an error');
}

console.log('_pollImportJob() refreshes after failed jobs');
{
  const { calls, dom, btn } = await runPoll({
    status: 'failed',
    message: 'Pre-import gate rejected',
  });
  assertEqual(btn.textContent, 'Failed', 'button shows failed');
  assert(calls.includes('/api/wrong-matches'), 'refreshes wrong matches after failure');
  assert(dom.wrongMatches.innerHTML.includes('No wrong matches'), 'renders refreshed empty state');
  assertEqual(dom.toast.className, 'toast error', 'failure toast is an error');
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
