// @ts-check

/**
 * Render a shared live Library-row corpus through the production badge path.
 *
 * Each JSONL input row is the exact `/api/library/artist` album shape plus a
 * synthetic integer `_corpus_id`. Output matches `render_differential.py`
 * `RenderedRow`, so the established Python diff engine remains the one census
 * implementation.
 */

import fs from 'node:fs';
import { pathToFileURL } from 'node:url';

import { renderLibraryAlbumRow } from '../web/js/library.js';


/**
 * @param {Record<string, unknown>} row
 * @returns {{id: number, fields: {row_html: string}}}
 */
export function renderLibraryBadgeCorpusRow(row) {
  const corpusId = row._corpus_id;
  if (!Number.isInteger(corpusId)) {
    throw new Error('Library badge corpus row needs an integer _corpus_id');
  }
  return {
    id: /** @type {number} */ (corpusId),
    fields: {
      // Render the complete production Library row. This includes the exact
      // row-to-BadgeItem adapter, shared badge renderer, and surrounding HTML;
      // a narrower target could earn misleading zeros by stopping early.
      row_html: renderLibraryAlbumRow(row),
    },
  };
}


/**
 * @param {string[]} argv
 * @returns {{corpus: string, out: string|null}}
 */
function parseArgs(argv) {
  let corpus = '';
  let out = null;
  for (let index = 0; index < argv.length; index += 2) {
    const option = argv[index];
    const value = argv[index + 1];
    if (!value || (option !== '--corpus' && option !== '--out')) {
      throw new Error('usage: render_library_badges.mjs --corpus PATH [--out PATH]');
    }
    if (option === '--corpus') corpus = value;
    if (option === '--out') out = value;
  }
  if (!corpus) {
    throw new Error('usage: render_library_badges.mjs --corpus PATH [--out PATH]');
  }
  return { corpus, out };
}


/**
 * @param {string[]} argv
 * @returns {number}
 */
export function main(argv) {
  try {
    const { corpus, out } = parseArgs(argv);
    const rendered = [];
    const lines = fs.readFileSync(corpus, 'utf8').split('\n');
    for (let index = 0; index < lines.length; index += 1) {
      const line = lines[index];
      if (!line.trim()) continue;
      const parsed = JSON.parse(line);
      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
        throw new Error(`${corpus}:${index + 1}: corpus line is not an object`);
      }
      rendered.push(JSON.stringify(renderLibraryBadgeCorpusRow(parsed)));
    }
    const payload = rendered.length ? `${rendered.join('\n')}\n` : '';
    if (out) fs.writeFileSync(out, payload, 'utf8');
    else process.stdout.write(payload);
    console.error(`rendered ${rendered.length} rows`);
    return 0;
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    console.error(`render-library-badges: ${detail}`);
    return 1;
  }
}


if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  process.exitCode = main(process.argv.slice(2));
}
