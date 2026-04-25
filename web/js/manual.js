// @ts-check
import { API, toast } from './state.js';
import { esc } from './util.js';

/**
 * Load and display manual import candidates from the Complete folder.
 */
export async function loadManualImport() {
  const el = document.getElementById('manual-content');
  el.innerHTML = '<div class="loading">Scanning Complete folder...</div>';
  try {
    const r = await fetch(`${API}/api/manual-import/scan`);
    const data = await r.json();
    renderManualImport(data, el);
  } catch (e) {
    el.innerHTML = '<div style="color:#f66;">Failed to scan folder</div>';
  }
}

/**
 * Render manual import folder listing.
 * @param {Object} data - Scan response with folders array
 * @param {HTMLElement} el - Target element
 */
export function renderManualImport(data, el) {
  const folders = data.folders || [];
  if (folders.length === 0) {
    el.innerHTML = '<div style="color:#888;padding:12px;">No audio folders found in Complete directory.</div>';
    return;
  }

  let html = `<div style="margin:8px 0;color:#888;">${folders.length} folders with audio files, ${data.wanted_count} wanted requests</div>`;

  html += folders.map(f => {
    const match = f.match;
    let matchHtml = '';
    let btnHtml = '';

    if (match) {
      const pct = Math.round(match.score * 100);
      matchHtml = `<div style="margin-top:4px;">
        <span style="color:#6d6;">Match (${pct}%):</span>
        <span style="color:#aaa;">${esc(match.artist)} - ${esc(match.album)}</span>
        <span style="color:#555;font-size:0.85em;">(request #${match.request_id})</span>
      </div>`;
      btnHtml = `<button class="btn btn-add" style="font-size:0.8em;padding:2px 10px;" onclick="event.stopPropagation(); window.runManualImport(${match.request_id}, '${f.path.replace(/'/g, "\\'")}', this)">Import</button>`;
    } else {
      matchHtml = '<div style="margin-top:4px;color:#777;">No matching wanted request</div>';
    }

    return `<div class="release" style="margin:4px 0;">
      <div class="release-info">
        <div class="release-title">${esc(f.name)}</div>
        <div class="release-meta" style="color:#777;">${f.file_count} files — parsed: ${esc(f.artist || '?')} - ${esc(f.album || '?')}</div>
        ${matchHtml}
      </div>
      ${btnHtml}
    </div>`;
  }).join('');

  el.innerHTML = html;
}

/**
 * Poll a queued import job until completion or failure.
 * @param {number} jobId
 * @param {HTMLButtonElement} btn
 */
async function pollImportJob(jobId, btn) {
  for (let i = 0; i < 240; i++) {
    await new Promise(resolve => setTimeout(resolve, 2000));
    try {
      const r = await fetch(`${API}/api/import-jobs/${jobId}`);
      if (!r.ok) continue;
      const data = await r.json();
      const job = data.job || {};
      if (job.status === 'queued' || job.status === 'running') {
        btn.textContent = job.status[0].toUpperCase() + job.status.slice(1);
        continue;
      }
      if (job.status === 'completed') {
        btn.textContent = 'Imported';
        btn.style.background = '#1a4a2a';
        toast(job.message || 'Import completed');
        return;
      }
      if (job.status === 'failed') {
        btn.textContent = 'Failed';
        btn.style.background = '#5a2a2a';
        btn.style.color = '#f88';
        toast(job.message || job.error || 'Import failed', true);
        return;
      }
    } catch (_e) {
      // Keep polling through transient request failures.
    }
  }
  btn.textContent = 'Queued';
}

/**
 * Run manual import for a folder matched to a pipeline request.
 * @param {number} requestId
 * @param {string} path
 * @param {HTMLButtonElement} btn
 */
export async function runManualImport(requestId, path, btn) {
  if (!confirm(`Import folder to request #${requestId}?`)) return;
  btn.disabled = true;
  btn.textContent = 'Importing...';
  try {
    const r = await fetch(`${API}/api/manual-import/import`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({request_id: requestId, path: path}),
    });
    const data = await r.json();
    if (data.status === 'queued') {
      btn.textContent = 'Queued';
      btn.style.background = '#1a3654';
      toast(`Queued import: ${data.artist} - ${data.album}`);
      if (data.job_id) {
        await pollImportJob(data.job_id, btn);
      }
    } else {
      btn.textContent = 'Failed';
      btn.style.background = '#5a2a2a';
      btn.style.color = '#f88';
      toast(data.message || 'Import failed', true);
    }
  } catch (e) {
    btn.textContent = 'Error';
    toast('Import request failed', true);
  }
}
