/** Current-state library badge quality tests. */

import { renderStatusBadges } from '../web/js/badges.js';
import { pipelineStore, updatePipelineStatus } from '../web/js/state.js';

import { suite } from './js_harness.mjs';

const t = suite(import.meta.url);

t.section('renderStatusBadges() uses average while retaining the min floor');
{
  const html = renderStatusBadges({
    id: 'request-6039',
    in_library: true,
    library_format: 'MP3',
    library_min_bitrate: 194,
    library_avg_bitrate: 288,
    library_rank: 'transparent',
  });
  t.contains(html, '>in library</span>', 'current holding remains a distinct presence fact');
  t.contains(html, '>M V0</span>', 'avg 288 drives the independent quality badge');
  t.contains(html, 'aria-label="current library quality: M V0"',
    'the abbreviated quality band has an accessible label');
  t.contains(html, 'badge-rank-transparent', 'canonical avg rank drives colour');
  t.excludes(html, 'M V2', 'min 194 does not drive badge label');
  t.excludes(html, 'in library ·', 'presence and quality are not collapsed');
}

t.section('renderStatusBadges() escapes fallback quality labels at the badge HTML boundary');
{
  const formats = '</span><img src=x onerror=alert(1)>';
  const html = renderStatusBadges({
    in_library: true,
    library_format: formats,
  });
  t.contains(html, '>&lt;/SPAN&gt;&lt;IMG SRC=X ONERROR=ALERT(1)&gt;</span>',
    'unknown format label is rendered as text');
  t.excludes(html, formats.toUpperCase(),
    'unknown format label cannot close the badge or create markup');
}

t.section('renderStatusBadges() derives independent presence, acquisition, and tracking families');
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
            t.contains(html, className, `${label} renders for ${world}`);
          } else {
            t.excludes(html, className, `${label} is absent for ${world}`);
          }
        }
        t.excludes(html, 'identity drift', `no composite identity state for ${world}`);
        t.excludes(html, 'holding unknown', `no unknown-authority state for ${world}`);
      }
    }
  }
}

t.section('renderStatusBadges() keeps carried proof independent of current presence and lifecycle');
{
  const missing = renderStatusBadges({
    in_library: false,
    has_captured_history: true,
    pipeline_status: 'wanted',
    pipeline_verified_lossless: true,
  });
  t.contains(missing, '>captured<', 'history remains visible after the holding disappears');
  t.contains(missing, '>missing<', 'current absence remains visible beside history');
  t.contains(missing, '>verified<', 'carried proof survives current absence');
  t.contains(missing, '>wanted<', 'current acquisition lifecycle remains independent');
  t.excludes(missing, 'search complete',
    'verified proof does not make a lifecycle claim');

  const replaced = renderStatusBadges({
    in_library: false,
    has_captured_history: true,
    pipeline_status: 'replaced',
    pipeline_provisional: true,
  });
  t.contains(replaced, '>captured<', 'superseded history remains captured');
  t.contains(replaced, '>missing<', 'superseded history can be currently missing');
  t.contains(replaced, '>provisional<', 'provisional evidence remains independent');
  t.contains(replaced, '>replaced<', 'superseded tracking renders explicitly');
  t.excludes(replaced, '>imported<', 'captured history is not duplicated as imported status');
}

t.section('renderStatusBadges() never combines a live mutation with stale historical facts');
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
  t.contains(reopened, '>wanted<', 'live lifecycle overlays the stale row');
  t.excludes(reopened, '>captured<', 'stale status-only capture fallback is invalidated');
  t.excludes(reopened, '>verified<', 'stale proof is invalidated with the row projection');

  updatePipelineStatus('deleted-request', null, null);
  const deleted = renderStatusBadges({
    id: 'deleted-request',
    in_library: true,
    has_captured_history: true,
    pipeline_status: 'imported',
    pipeline_id: 52,
    pipeline_provisional: true,
  });
  t.contains(deleted, '>untracked<', 'request deletion leaves an explicit local tombstone');
  t.excludes(deleted, '>captured<', 'deleted request history is not borrowed from the stale row');
  t.excludes(deleted, '>provisional<', 'deleted request proof is not borrowed from the stale row');

  const authoritativeOwner = {
    job_id: 303,
    status: 'recovery_required',
    preview_status: 'evidence_ready',
  };
  /** @type {Array<[string, import('../web/js/release_action_state.js').ProcessingOwnerProjection]>} */
  const staleOwnerAxes = [
    ['job-id', { ...authoritativeOwner, job_id: 302 }],
    ['status', { ...authoritativeOwner, status: 'running' }],
    ['preview-status', { ...authoritativeOwner, preview_status: 'waiting' }],
  ];
  for (const [axis, staleProjection] of staleOwnerAxes) {
    const releaseId = `processing-owner-${axis}`;
    updatePipelineStatus(releaseId, 'processing', 53, authoritativeOwner);
    const staleOwner = renderStatusBadges({
      id: releaseId,
      in_library: false,
      has_captured_history: true,
      pipeline_status: 'processing',
      pipeline_id: 53,
      processing_owner: staleProjection,
      pipeline_verified_lossless: true,
    });
    t.contains(staleOwner, '>needs recovery<',
      `${axis} mismatch does not expire a newer exact-owner overlay`);
    t.contains(staleOwner, 'job #303',
      `${axis} mismatch retains the live job-specific recovery reason`);
    t.excludes(staleOwner, '>captured<',
      `${axis} mismatch keeps stale row facts suppressed`);
    t.notOk(!pipelineStore.has(releaseId),
      `${axis} mismatch cannot acknowledge the lifecycle overlay`);
  }

  updatePipelineStatus(
    'processing-owner-refresh',
    'processing',
    53,
    authoritativeOwner,
  );

  const matchingOwner = renderStatusBadges({
    id: 'processing-owner-refresh',
    in_library: false,
    has_captured_history: true,
    pipeline_status: 'processing',
    pipeline_id: 53,
    processing_owner: authoritativeOwner,
    pipeline_verified_lossless: true,
  });
  t.contains(matchingOwner, '>needs recovery<',
    'a complete matching refetch retains the authoritative owner state');
  t.contains(matchingOwner, '>captured<',
    'a complete matching refetch restores authoritative historical facts');
  t.notOk(pipelineStore.has('processing-owner-refresh'),
    'the complete owner projection acknowledges the lifecycle overlay');

  const refetched = renderStatusBadges({
    id: 'status-only-reopen',
    in_library: false,
    has_captured_history: true,
    pipeline_status: 'wanted',
    pipeline_id: 51,
    pipeline_verified_lossless: true,
  });
  t.contains(refetched, '>captured<', 'matching refetch restores durable history');
  t.contains(refetched, '>verified<', 'matching refetch restores authoritative proof');
  t.notOk(pipelineStore.has('status-only-reopen'),
    'matching refetch acknowledges and expires the local lifecycle overlay');

  const laterProcessing = renderStatusBadges({
    id: 'status-only-reopen',
    in_library: false,
    has_captured_history: true,
    pipeline_status: 'processing',
    pipeline_id: 51,
    processing_owner: {
      job_id: 99,
      status: 'running',
      preview_status: 'evidence_ready',
    },
    pipeline_verified_lossless: true,
  });
  t.contains(laterProcessing, 'badge-processing',
    'later server lifecycle remains visible after acknowledgement');
  t.contains(laterProcessing, '>captured<',
    'later authoritative row retains acquisition history');
  t.contains(laterProcessing, '>verified<',
    'later authoritative row retains proof');
  pipelineStore.clear();
}

t.section('renderStatusBadges() marks a provisional lossless-source install');
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
  t.contains(html, 'badge-provisional', 'provisional install renders chip');
  t.contains(html, '>provisional<', 'chip label reads provisional');
  t.excludes(html, 'badge-verified', 'provisional never claims verified');
}

t.section('renderStatusBadges() marks a verified lossless install');
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
  t.contains(html, 'badge-verified', 'verified install renders chip');
  t.contains(html, 'badge-rank-lossless',
    'verified identity reuses the brightest lossless bucket colour');
  t.contains(html, '>verified<', 'chip label reads verified');
  t.excludes(html, 'badge-provisional', 'verified never doubles as provisional');
}

t.section('renderStatusBadges() renders no identity chip without pipeline identity');
{
  const html = renderStatusBadges({
    id: 'request-1',
    in_library: true,
    library_format: 'MP3',
    library_avg_bitrate: 288,
    library_rank: 'transparent',
    pipeline_status: 'wanted',
  });
  t.excludes(html, 'badge-verified', 'plain install has no verified chip');
  t.excludes(html, 'badge-provisional', 'plain install has no provisional chip');
}

t.section('renderStatusBadges() renders processing from the exact owner state');
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
  t.contains(html, 'badge-processing', 'processing uses its dedicated badge');
  t.contains(html, '>waiting to import<', 'canonical owner presentation drives label');
  t.contains(html, 'job #908', 'badge title names exact owner');
  t.excludes(html, 'downloading', 'processor ownership is not labelled as transfer ownership');
}

t.done();
