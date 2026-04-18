/**
 * Unit tests for web/js/release_actions.js — pure toolbar HTML rendering.
 * Run with: node tests/test_js_release_actions.mjs
 */

// Stub the browser-only deps the module imports.
import { pipelineStore } from '../web/js/state.js';
import { renderActionToolbar } from '../web/js/release_actions.js';

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

function clearStore() {
  pipelineStore.clear();
}

console.log('renderActionToolbar — fresh row (Add request enabled)');
clearStore();
{
  const html = renderActionToolbar({ id: 'rel-1', in_library: false });
  assertContains(html, '>Add request</button>', 'shows Add request label');
  assertContains(html, '>Remove request</button>', 'shows Remove request label');
  assertContains(html, '>Remove from beets</button>', 'shows Remove from beets label');
  // Add enabled, others disabled
  assertContains(html, 'window.addRelease', 'Add request is wired up');
  assertExcludes(html, 'window.upgradeAlbum', 'Upgrade not used in this state');
  assertExcludes(html, 'window.disambRemove', 'Remove request is disabled');
  assertExcludes(html, 'window.confirmDeleteBeets', 'Remove from beets is disabled');
}

console.log('renderActionToolbar — in library (Acquire collapses to Upgrade)');
clearStore();
{
  const html = renderActionToolbar({
    id: 'rel-2',
    in_library: true,
    beets_album_id: 42,
    artist: 'Bodyjar',
    album: 'Plastic Skies',
    track_count: 12,
  });
  // Acquire button shows Upgrade when in library
  assertContains(html, '>Upgrade</button>', 'Acquire shows Upgrade when in library');
  assertExcludes(html, '>Add request</button>', 'no separate Add request button');
  assertContains(html, "window.upgradeAlbum('rel-2'", 'Upgrade wired up');
  // Remove from beets: enabled
  assertContains(html, 'window.confirmDeleteBeets(42', 'Remove from beets enabled');
  assertContains(html, "'Bodyjar'", 'artist passed to delete confirm');
  assertContains(html, "'Plastic Skies'", 'album passed to delete confirm');
  assertContains(html, ', 12)', 'track count passed to delete confirm');
}

console.log('renderActionToolbar — pipeline status wanted (in flight, neither add nor upgrade)');
clearStore();
{
  const html = renderActionToolbar({
    id: 'rel-3',
    in_library: false,
    pipeline_status: 'wanted',
    pipeline_id: 100,
  });
  // Acquire defaults to disabled "Add request" since nothing actionable
  assertContains(html, '>Add request</button>', 'shows Add request label disabled');
  assertExcludes(html, 'window.addRelease', 'Add disabled (already in pipeline)');
  assertExcludes(html, 'window.upgradeAlbum', 'Upgrade disabled (only wanted)');
  // Remove request: enabled
  assertContains(html, 'window.disambRemove(100', 'Remove request enabled with pipeline_id');
  assertExcludes(html, 'window.confirmDeleteBeets', 'Remove from beets disabled');
}

console.log('renderActionToolbar — pipeline status imported (Acquire shows Upgrade)');
clearStore();
{
  const html = renderActionToolbar({
    id: 'rel-4',
    in_library: false,
    pipeline_status: 'imported',
    pipeline_id: 101,
  });
  // Acquire shows Upgrade when pipeline=imported
  assertContains(html, '>Upgrade</button>', 'Acquire shows Upgrade when imported');
  assertContains(html, 'window.upgradeAlbum', 'Upgrade wired up');
  assertExcludes(html, 'window.disambRemove', 'Remove request disabled (not wanted)');
}

console.log('renderActionToolbar — upgrade already queued (Acquire shows Queued)');
clearStore();
{
  const html = renderActionToolbar({
    id: 'rel-5',
    in_library: true,
    beets_album_id: 50,
    upgrade_queued: true,
  });
  assertContains(html, '>Queued</button>', 'shows Queued label when upgrade queued');
  assertExcludes(html, 'window.upgradeAlbum', 'no upgrade handler when queued');
}

console.log('renderActionToolbar — pipelineStore overlay');
clearStore();
pipelineStore.set('rel-6', { status: 'wanted', id: 200 });
{
  const html = renderActionToolbar({
    id: 'rel-6',
    in_library: false,
    pipeline_status: null,
    pipeline_id: null,
  });
  assertContains(html, 'window.disambRemove(200', 'pipelineStore overrides backend status');
}

console.log('renderActionToolbar — never crashes on minimal input');
clearStore();
{
  const html = renderActionToolbar({ id: 'rel-7' });
  assertContains(html, '>Add request</button>', 'minimal input renders Acquire button');
  assertContains(html, '>Remove request</button>', 'minimal input renders Remove request');
  assertContains(html, '>Remove from beets</button>', 'minimal input renders Remove from beets');
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
