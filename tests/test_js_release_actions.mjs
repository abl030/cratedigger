/**
 * Unit tests for web/js/release_actions.js — pure toolbar HTML rendering.
 * Run with: node tests/test_js_release_actions.mjs
 */

import { pipelineStore } from '../web/js/state.js';
import { renderActionToolbar } from '../web/js/release_actions.js';

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

function clearStore() {
  pipelineStore.clear();
}

console.log('Acquire button — fresh row → Add request enabled');
clearStore();
{
  const html = renderActionToolbar({ id: 'rel-1', in_library: false });
  assertContains(html, '>Add request</button>', 'shows Add request label');
  assertContains(html, "window.addRelease('rel-1'", 'Add wired up');
  assertExcludes(html, '>Upgrade</button>', 'no Upgrade in this state');
  assertExcludes(html, '>Remove request</button>', 'no Remove request in this state');
  assertContains(html, '>Remove from beets</button>', 'Remove from beets always rendered');
  assertExcludes(html, 'window.confirmDeleteBeets', 'Remove from beets disabled');
}

console.log('Acquire button — in library, no pipeline → Upgrade enabled');
clearStore();
{
  const html = renderActionToolbar({
    id: 'rel-2', in_library: true, beets_album_id: 42,
    artist: 'Bodyjar', album: 'Plastic Skies', track_count: 12,
  });
  assertContains(html, '>Upgrade</button>', 'shows Upgrade label');
  assertContains(html, "window.upgradeAlbum('rel-2'", 'Upgrade wired up');
  assertExcludes(html, '>Add request</button>', 'no Add request');
  assertExcludes(html, '>Remove request</button>', 'no Remove request');
  assertContains(html, 'window.confirmDeleteBeets(42', 'Remove from beets enabled');
  assertContains(html, ", null, 'rel-2')", 'release id passed to delete confirm');
  assertContains(html, "'Bodyjar'", 'artist passed to delete confirm');
  assertContains(html, "'Plastic Skies'", 'album passed to delete confirm');
}

console.log('Acquire button — in library + wanted → Remove request (the user-reported bug)');
clearStore();
{
  // Plastic Skies on Bodyjar: in_library=true, pipeline_status='wanted'.
  // Old behaviour incorrectly showed Upgrade green; new behaviour shows
  // Remove request because the album is already in the pipeline.
  const html = renderActionToolbar({
    id: 'rel-3', in_library: true, beets_album_id: 42,
    pipeline_status: 'wanted', pipeline_id: 1712,
  });
  assertContains(html, '>Remove request</button>', 'wanted → Remove request');
  assertContains(html, 'window.disambRemove(1712', 'Remove wired up');
  assertExcludes(html, '>Upgrade</button>', 'no Upgrade — wanted wins');
  // Remove from beets still independent
  assertContains(html, 'window.confirmDeleteBeets(42', 'Remove from beets still enabled');
  assertContains(html, ", 1712, 'rel-3')", 'pipeline context passed to delete confirm');
}

console.log('Acquire button — not in library + wanted → Remove request');
clearStore();
{
  const html = renderActionToolbar({
    id: 'rel-4', in_library: false,
    pipeline_status: 'wanted', pipeline_id: 200,
  });
  assertContains(html, '>Remove request</button>', 'wanted (no library) → Remove request');
  assertContains(html, 'window.disambRemove(200', 'Remove wired up');
  assertExcludes(html, '>Add request</button>', 'no Add request when wanted');
}

console.log('Acquire button — downloading → Remove request enabled (cancellable)');
clearStore();
{
  // User report: "downloading" pressing showed Remove request greyed
  // out. The backend's /api/pipeline/delete handles any status, so
  // there's no reason to disable. Cratedigger's next poll cycle drops
  // any orphan slskd transfer when the row is gone.
  const html = renderActionToolbar({
    id: 'rel-5', in_library: false,
    pipeline_status: 'downloading', pipeline_id: 300,
  });
  assertContains(html, '>Remove request</button>', 'downloading shows Remove request label');
  assertContains(html, 'window.disambRemove(300', 'Remove request enabled mid-download');
}

console.log('Acquire button — pipeline=imported (no library) → Upgrade enabled');
clearStore();
{
  const html = renderActionToolbar({
    id: 'rel-6', in_library: false,
    pipeline_status: 'imported', pipeline_id: 400,
  });
  assertContains(html, '>Upgrade</button>', 'imported → Upgrade');
  assertContains(html, 'window.upgradeAlbum', 'Upgrade wired up');
  assertExcludes(html, '>Remove request</button>', 'no Remove request when imported');
}

console.log('Acquire button — pipelineStore overlay');
clearStore();
pipelineStore.set('rel-7', { status: 'wanted', id: 500 });
{
  // Backend snapshot says no pipeline; pipelineStore (live mutation
  // overlay) says wanted. Store wins.
  const html = renderActionToolbar({
    id: 'rel-7', in_library: false,
    pipeline_status: null, pipeline_id: null,
  });
  assertContains(html, 'window.disambRemove(500', 'pipelineStore overrides backend');
}

console.log('Acquire button — manual review → disabled Add request');
clearStore();
{
  const html = renderActionToolbar({
    id: 'rel-8', in_library: false,
    pipeline_status: 'manual', pipeline_id: 600,
  });
  assertContains(html, '>Add request</button>', 'manual falls through to disabled Add request');
  assertExcludes(html, 'window.addRelease', 'Add disabled in manual state');
  assertExcludes(html, 'window.disambRemove', 'no Remove handler in manual state');
}

console.log('Acquire button — minimal input never crashes');
clearStore();
{
  const html = renderActionToolbar({ id: 'rel-9' });
  assertContains(html, 'action-toolbar', 'toolbar wrapper present');
  assertContains(html, '>Add request</button>', 'falls back to Add request');
  assertContains(html, '>Remove from beets</button>', 'Remove from beets always present');
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
