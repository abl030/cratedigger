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

console.log('renderActionToolbar — fresh row (not in library, not in pipeline)');
clearStore();
{
  const html = renderActionToolbar({ id: 'rel-1', in_library: false });
  assertContains(html, 'Add request', 'has Add request label');
  assertContains(html, 'Upgrade', 'has Upgrade label');
  assertContains(html, 'Remove request', 'has Remove request label');
  assertContains(html, 'Remove from beets', 'has Remove from beets label');
  // Add enabled, others disabled
  assertContains(html, 'window.addRelease', 'Add request is wired up');
  assertContains(html, '>Upgrade</button>', 'Upgrade renders');
  assertExcludes(html, 'window.upgradeAlbum', 'Upgrade is disabled (no handler)');
  assertExcludes(html, 'window.disambRemove', 'Remove request is disabled');
  assertExcludes(html, 'window.confirmDeleteBeets', 'Remove from beets is disabled');
}

console.log('renderActionToolbar — in library, not in pipeline');
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
  // Add request: disabled (already owned)
  assertExcludes(html, 'window.addRelease', 'Add request disabled when owned');
  // Upgrade: enabled
  assertContains(html, "window.upgradeAlbum('rel-2'", 'Upgrade enabled when in library');
  // Remove request: disabled (no pipeline entry)
  assertExcludes(html, 'window.disambRemove', 'Remove request disabled with no pipeline entry');
  // Remove from beets: enabled
  assertContains(html, 'window.confirmDeleteBeets(42', 'Remove from beets enabled with beets_album_id');
  assertContains(html, "'Bodyjar'", 'artist passed to delete confirm');
  assertContains(html, "'Plastic Skies'", 'album passed to delete confirm');
  assertContains(html, ', 12)', 'track count passed to delete confirm');
}

console.log('renderActionToolbar — pipeline status wanted');
clearStore();
{
  const html = renderActionToolbar({
    id: 'rel-3',
    in_library: false,
    pipeline_status: 'wanted',
    pipeline_id: 100,
  });
  // Add: disabled (already in pipeline)
  assertExcludes(html, 'window.addRelease', 'Add disabled when already in pipeline');
  // Upgrade: disabled (not imported, not in library)
  assertExcludes(html, 'window.upgradeAlbum', 'Upgrade disabled when only wanted');
  // Remove request: enabled
  assertContains(html, 'window.disambRemove(100', 'Remove request enabled with pipeline_id');
  // Remove from beets: disabled
  assertExcludes(html, 'window.confirmDeleteBeets', 'Remove from beets disabled (not in library)');
}

console.log('renderActionToolbar — pipeline status imported (queue another upgrade)');
clearStore();
{
  const html = renderActionToolbar({
    id: 'rel-4',
    in_library: false,
    pipeline_status: 'imported',
    pipeline_id: 101,
  });
  // Upgrade: enabled when imported
  assertContains(html, 'window.upgradeAlbum', 'Upgrade enabled when pipeline=imported');
  // Remove request: disabled (not 'wanted')
  assertExcludes(html, 'window.disambRemove', 'Remove request disabled when not wanted');
}

console.log('renderActionToolbar — upgrade already queued');
clearStore();
{
  const html = renderActionToolbar({
    id: 'rel-5',
    in_library: true,
    beets_album_id: 50,
    upgrade_queued: true,
  });
  // Upgrade button shows "Queued" disabled
  assertContains(html, '>Queued</button>', 'shows Queued label when upgrade queued');
  assertExcludes(html, 'window.upgradeAlbum', 'no upgrade handler when queued');
}

console.log('renderActionToolbar — pipelineStore overlay');
clearStore();
pipelineStore.set('rel-6', { status: 'wanted', id: 200 });
{
  // Backend says no pipeline; pipelineStore says wanted. Store wins.
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
  assertContains(html, 'Add request', 'minimal input still renders all 4 buttons');
  assertContains(html, 'Upgrade', 'minimal input renders Upgrade');
  assertContains(html, 'Remove request', 'minimal input renders Remove request');
  assertContains(html, 'Remove from beets', 'minimal input renders Remove from beets');
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
