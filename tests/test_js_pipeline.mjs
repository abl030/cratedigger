/**
 * Unit tests for web/js/pipeline.js navigation helpers.
 * Run with: node tests/test_js_pipeline.mjs
 */

import { __test__ } from '../web/js/pipeline.js';
import { state } from '../web/js/state.js';

let passed = 0;
let failed = 0;

function assertContains(haystack, needle, msg) {
  if (haystack.includes(needle)) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg} - '${needle}' not in output`);
  }
}

function assertExcludes(haystack, needle, msg) {
  if (!haystack.includes(needle)) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg} - unexpectedly found '${needle}'`);
  }
}

console.log('renderPipelineNav() refreshes the queue subtab');
{
  state.pipelineView = 'queue';
  const html = __test__.renderPipelineNav();
  assertContains(html, 'window.setPipelineView(\'queue\')', 'queue tab rendered');
  assertContains(html, 'window.setPipelineView(\'dashboard\')', 'dashboard tab rendered');
  assertContains(html, 'window.loadPipeline()', 'queue refresh reloads pipeline queue');
  assertContains(html, 'subtab-refresh', 'refresh uses shared subtab layout');
  assertExcludes(html, 'window.loadPipelineDashboard()">Refresh', 'queue refresh does not load dashboard');
}

console.log('renderPipelineNav() refreshes the dashboard subtab');
{
  state.pipelineView = 'dashboard';
  const html = __test__.renderPipelineNav();
  assertContains(html, 'window.loadPipelineDashboard()', 'dashboard refresh reloads dashboard metrics');
  assertContains(html, 'subtab-refresh', 'refresh uses shared subtab layout');
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
