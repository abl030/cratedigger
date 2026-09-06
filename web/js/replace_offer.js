// @ts-check

/**
 * The Browse-search inverted Replace button's offer decision (issue
 * #1366 part 2).
 *
 * A pressing row on the Browse tab is a candidate NEW pressing; the
 * button offers to replace an existing request with it. Two key sets
 * decide whether such a request exists: the row's OWN key (its MB
 * release group, or its Discogs master — the same column, KTD-1) and the
 * key of the group the artist compare PAIRED it with on the other pathway
 * (a Discogs master or masterless release for an MB row; an MB release
 * group for a Discogs row). Either side holding an active request is an
 * offer; the picker then asks the operator which request to replace and
 * shows the pair explicitly.
 *
 * Not every surface runs the compare. The artist page does (its rows say
 * whether the pairing is still pending or has been checked); the Browse
 * search results, the Various Artists fallback card, and other
 * expansions outside an artist page never do, and their offers say
 * nothing about pairing at all — they keep the plain own-group copy.
 *
 * This module is the pure decision: the same inputs always give the same
 * enabled flag, reason code and tooltip, and the tooltip is honest about
 * WHAT was checked. It never describes a pending pairing or a failed
 * lookup as confirmed absence, and never mentions a pairing on a surface
 * that has none. `tests/test_js_replace_offer.mjs` pins each reason;
 * `tests/test_replace_offer_generated.py` patrols the invariants over
 * the whole input space through a Node worker.
 */

/**
 * @typedef {Object} ReplacePair
 * @property {string} id            // the paired group's key: MB RG UUID, Discogs master id, or masterless Discogs release id
 * @property {'work'|'release'} kind  // 'release' only for a masterless Discogs release
 * @property {'mb'|'discogs'} source
 * @property {string} label         // the paired row's title, for copy
 */

/**
 * Whether this row's surface consulted the artist compare: 'none' — the
 * surface has no compare (search results, the VA fallback card); 'pending'
 * — the artist page's compare has not landed yet; 'checked' — it has.
 * @typedef {'none'|'pending'|'checked'} ReplacePairingState
 */

/**
 * @typedef {Object} ReplaceOfferInput
 * @property {string|null} ownKey        // the row's own lookup key; null for a masterless Discogs release row
 * @property {boolean} ownActive         // an active request holds ownKey (ignored when ownKey is null)
 * @property {boolean} lookupFailed      // the active-key fetch failed, so neither set could be consulted
 * @property {ReplacePairingState} pairing
 * @property {ReplacePair|null} pair     // the compare's counterpart for this row's group, if any (only when pairing is 'checked')
 * @property {boolean} pairActive        // an active request holds the pair's key (ignored when pair is null)
 * @property {'mb'|'discogs'} rowSource  // the row's own pathway
 */

/**
 * @typedef {'own'|'paired'|'lookup_unavailable'|'no_request'|'masterless'|'pairing_unchecked'|'no_pair'|'pair_inactive'|'masterless_no_pair'} ReplaceOfferReason
 */

/**
 * @typedef {Object} ReplaceOffer
 * @property {boolean} enabled
 * @property {ReplaceOfferReason} reason
 * @property {string} title  // tooltip; empty only for an enabled own-key offer
 */

/**
 * Every reason the decision can return. The mjs suite pins this list; the
 * generated patrol re-derives its own copy and would report an addition
 * here as an unknown reason until it is taught the new one.
 */
export const REPLACE_OFFER_REASONS = Object.freeze([
  'own', 'paired', 'lookup_unavailable', 'no_request', 'masterless',
  'pairing_unchecked', 'no_pair', 'pair_inactive', 'masterless_no_pair',
]);

/** The pre-#1366 own-group copy, kept verbatim on surfaces with no compare. */
export const NO_REQUEST_TITLE = 'No existing request in this release group';

/**
 * Human noun for the paired group, by what it is.
 * @param {ReplacePair} pair
 * @returns {string}
 */
export function pairNoun(pair) {
  if (pair.source === 'mb') return 'MusicBrainz release group';
  return pair.kind === 'release' ? 'Discogs release' : 'Discogs master';
}

/**
 * Human noun for whatever the other pathway would have paired this row
 * with, when nothing did.
 * @param {'mb'|'discogs'} rowSource
 * @returns {string}
 */
function otherPathwayNoun(rowSource) {
  return rowSource === 'mb' ? 'Discogs master or release' : 'MusicBrainz release group';
}

/**
 * Decide the inverted Replace button's state for one pressing row.
 *
 * Order: an active request on the row's own key wins, then one on the
 * paired key. Otherwise the explanation names the FIRST thing that
 * could not be answered: a failed lookup (only when there was a key to
 * look up), then — on a surface without a compare — plain own-group
 * absence; on an artist page, a pairing still pending, then the absence
 * of a pair, then a pair with no request on either side.
 *
 * @param {ReplaceOfferInput} input
 * @returns {ReplaceOffer}
 */
export function replaceOfferState(input) {
  const ownKey = input.ownKey || null;
  const pair = input.pairing === 'checked' ? (input.pair || null) : null;
  if (ownKey !== null && input.ownActive) {
    return { enabled: true, reason: 'own', title: '' };
  }
  if (pair !== null && input.pairActive) {
    return {
      enabled: true,
      reason: 'paired',
      title: `Replaces the request held on the other pathway: ${pairNoun(pair)} "${pair.label}"`,
    };
  }
  const hasAnyKey = ownKey !== null || pair !== null;
  if (input.lookupFailed && hasAnyKey) {
    // Name only the keys this row actually had to check: a masterless
    // row has no release group of its own, only its pair.
    const scope = pair === null
      ? 'this release group'
      : (ownKey === null
        ? `the paired ${pairNoun(pair)}`
        : `this release group or its paired ${pairNoun(pair)}`);
    return {
      enabled: false,
      reason: 'lookup_unavailable',
      title: `Could not check for an existing request in ${scope}. Collapse and re-expand to retry.`,
    };
  }
  if (input.pairing === 'none') {
    return {
      enabled: false,
      reason: ownKey !== null ? 'no_request' : 'masterless',
      title: NO_REQUEST_TITLE,
    };
  }
  if (input.pairing !== 'checked') {
    return {
      enabled: false,
      reason: 'pairing_unchecked',
      title: ownKey !== null
        ? "No existing request in this release group; the other pathway's pairing could not be checked."
        : "This release has no master; the other pathway's pairing could not be checked.",
    };
  }
  if (pair === null) {
    if (ownKey === null) {
      return {
        enabled: false,
        reason: 'masterless_no_pair',
        title: `This release has no master and no paired ${otherPathwayNoun(input.rowSource)}.`,
      };
    }
    return {
      enabled: false,
      reason: 'no_pair',
      title: `No existing request in this release group; no paired ${otherPathwayNoun(input.rowSource)} was found for this album.`,
    };
  }
  return {
    enabled: false,
    reason: 'pair_inactive',
    title: `No existing request for this album on either pathway (paired with ${pairNoun(pair)} "${pair.label}").`,
  };
}
