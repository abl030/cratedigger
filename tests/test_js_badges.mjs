/** Current-state library badge quality tests. */

import { renderStatusBadges } from '../web/js/badges.js';
import { pipelineStore, updatePipelineStatus } from '../web/js/state.js';

let passed = 0;
let failed = 0;

function assertContains(haystack, needle, message) {
  if (haystack.includes(needle)) passed++;
  else {
    failed++;
    console.error(`  FAIL: ${message} - '${needle}' not in output`);
  }
}

function assertExcludes(haystack, needle, message) {
  if (!haystack.includes(needle)) passed++;
  else {
    failed++;
    console.error(`  FAIL: ${message} - unexpectedly found '${needle}'`);
  }
}

console.log('renderStatusBadges() uses average while retaining the min floor');
{
  const html = renderStatusBadges({
    id: 'request-6039',
    in_library: true,
    library_format: 'MP3',
    library_min_bitrate: 194,
    library_avg_bitrate: 288,
    library_rank: 'transparent',
  });
  assertContains(html, '>in library</span>', 'current holding remains a distinct presence fact');
  assertContains(html, '>M V0</span>', 'avg 288 drives the independent quality badge');
  assertContains(html, 'aria-label="current library quality: M V0"',
    'the abbreviated quality band has an accessible label');
  assertContains(html, 'badge-rank-transparent', 'canonical avg rank drives colour');
  assertExcludes(html, 'M V2', 'min 194 does not drive badge label');
  assertExcludes(html, 'in library ·', 'presence and quality are not collapsed');
}

console.log('renderStatusBadges() escapes fallback quality labels at the badge HTML boundary');
{
  const formats = '</span><img src=x onerror=alert(1)>';
  const html = renderStatusBadges({
    in_library: true,
    library_format: formats,
  });
  assertContains(html, '>&lt;/SPAN&gt;&lt;IMG SRC=X ONERROR=ALERT(1)&gt;</span>',
    'unknown format label is rendered as text');
  assertExcludes(html, formats.toUpperCase(),
    'unknown format label cannot close the badge or create markup');
}

console.log('renderStatusBadges() derives independent presence, acquisition, and tracking families');
{
  const statuses = [
    null,
    'wanted',
    'downloading',
    'processing',
    'imported',
    'unsearchable',
    'replaced',
  ];
  for (const inLibrary of [false, true]) {
    for (const hasCapturedHistory of [false, true]) {
      for (const pipelineStatus of statuses) {
        const html = renderStatusBadges({
          in_library: inLibrary,
          has_captured_history: hasCapturedHistory,
          pipeline_status: pipelineStatus,
        });
        const world = `held=${inLibrary}, captured=${hasCapturedHistory}, status=${pipelineStatus}`;
        const facts = [
          ['badge-library', inLibrary, 'current holding'],
          ['badge-captured', hasCapturedHistory, 'captured history'],
          ['badge-missing', hasCapturedHistory && !inLibrary, 'captured-but-missing presence'],
          ['badge-untracked', inLibrary && pipelineStatus === null, 'held-without-request tracking'],
          ['badge-replaced', pipelineStatus === 'replaced', 'superseded request'],
          ['badge-imported', false, 'obsolete imported duplicate'],
        ];
        for (const [className, expected, label] of facts) {
          if (expected) {
            assertContains(html, className, `${label} renders for ${world}`);
          } else {
            assertExcludes(html, className, `${label} is absent for ${world}`);
          }
        }
        assertExcludes(html, 'identity drift', `no composite identity state for ${world}`);
        assertExcludes(html, 'holding unknown', `no unknown-authority state for ${world}`);
      }
    }
  }
}

console.log('renderStatusBadges() keeps carried proof independent of current presence and lifecycle');
{
  const missing = renderStatusBadges({
    in_library: false,
    has_captured_history: true,
    pipeline_status: 'wanted',
    pipeline_verified_lossless: true,
  });
  assertContains(missing, '>captured<', 'history remains visible after the holding disappears');
  assertContains(missing, '>missing<', 'current absence remains visible beside history');
  assertContains(missing, '>verified<', 'carried proof survives current absence');
  assertContains(missing, '>wanted<', 'current acquisition lifecycle remains independent');
  assertExcludes(missing, 'search complete',
    'verified proof does not make a lifecycle claim');

  const replaced = renderStatusBadges({
    in_library: false,
    has_captured_history: true,
    pipeline_status: 'replaced',
    pipeline_provisional: true,
  });
  assertContains(replaced, '>captured<', 'superseded history remains captured');
  assertContains(replaced, '>missing<', 'superseded history can be currently missing');
  assertContains(replaced, '>provisional<', 'provisional evidence remains independent');
  assertContains(replaced, '>replaced<', 'superseded tracking renders explicitly');
  assertExcludes(replaced, '>imported<', 'captured history is not duplicated as imported status');
}

console.log('renderStatusBadges() never combines a live mutation with stale historical facts');
{
  pipelineStore.clear();
  updatePipelineStatus('status-only-reopen', 'wanted', 51);
  const reopened = renderStatusBadges({
    id: 'status-only-reopen',
    in_library: false,
    has_captured_history: true,
    pipeline_status: 'imported',
    pipeline_id: 50,
    pipeline_verified_lossless: true,
  });
  assertContains(reopened, '>wanted<', 'live lifecycle overlays the stale row');
  assertExcludes(reopened, '>captured<', 'stale status-only capture fallback is invalidated');
  assertExcludes(reopened, '>verified<', 'stale proof is invalidated with the row projection');

  updatePipelineStatus('deleted-request', null, null);
  const deleted = renderStatusBadges({
    id: 'deleted-request',
    in_library: true,
    has_captured_history: true,
    pipeline_status: 'imported',
    pipeline_id: 52,
    pipeline_provisional: true,
  });
  assertContains(deleted, '>untracked<', 'request deletion leaves an explicit local tombstone');
  assertExcludes(deleted, '>captured<', 'deleted request history is not borrowed from the stale row');
  assertExcludes(deleted, '>provisional<', 'deleted request proof is not borrowed from the stale row');

  const refetched = renderStatusBadges({
    id: 'status-only-reopen',
    in_library: false,
    has_captured_history: true,
    pipeline_status: 'wanted',
    pipeline_id: 51,
    pipeline_verified_lossless: true,
  });
  assertContains(refetched, '>captured<', 'matching refetch restores durable history');
  assertContains(refetched, '>verified<', 'matching refetch restores authoritative proof');
  pipelineStore.clear();
}

console.log('renderStatusBadges() marks a provisional lossless-source install');
{
  const html = renderStatusBadges({
    id: 'request-3652',
    in_library: true,
    library_format: 'Opus',
    library_avg_bitrate: 102,
    library_rank: 'transparent',
    pipeline_status: 'wanted',
    pipeline_provisional: true,
    pipeline_verified_lossless: false,
  });
  assertContains(html, 'badge-provisional', 'provisional install renders chip');
  assertContains(html, '>provisional<', 'chip label reads provisional');
  assertExcludes(html, 'badge-verified', 'provisional never claims verified');
}

console.log('renderStatusBadges() marks a verified lossless install');
{
  const html = renderStatusBadges({
    id: 'request-8877',
    in_library: true,
    library_format: 'Opus',
    library_avg_bitrate: 131,
    library_rank: 'transparent',
    pipeline_status: 'imported',
    pipeline_verified_lossless: true,
    pipeline_provisional: false,
  });
  assertContains(html, 'badge-verified', 'verified install renders chip');
  assertContains(html, 'badge-rank-lossless',
    'verified identity reuses the brightest lossless bucket colour');
  assertContains(html, '>verified<', 'chip label reads verified');
  assertExcludes(html, 'badge-provisional', 'verified never doubles as provisional');
}

console.log('renderStatusBadges() renders no identity chip without pipeline identity');
{
  const html = renderStatusBadges({
    id: 'request-1',
    in_library: true,
    library_format: 'MP3',
    library_avg_bitrate: 288,
    library_rank: 'transparent',
    pipeline_status: 'wanted',
  });
  assertExcludes(html, 'badge-verified', 'plain install has no verified chip');
  assertExcludes(html, 'badge-provisional', 'plain install has no provisional chip');
}

console.log('renderStatusBadges() renders processing from the exact owner state');
{
  const html = renderStatusBadges({
    id: 'request-processing',
    in_library: false,
    pipeline_status: 'processing',
    processing_owner: {
      job_id: 908,
      status: 'queued',
      preview_status: 'evidence_ready',
    },
  });
  assertContains(html, 'badge-processing', 'processing uses its dedicated badge');
  assertContains(html, '>waiting to import<', 'canonical owner presentation drives label');
  assertContains(html, 'job #908', 'badge title names exact owner');
  assertExcludes(html, 'downloading', 'processor ownership is not labelled as transfer ownership');
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
