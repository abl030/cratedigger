/**
 * The cross-module `window.*` handlers, declared for the modules that call
 * them (issue #1390).
 *
 * `main.js` installs the whole onclick surface with
 * `Object.assign(window, {…})`, which type-checks nothing: `Object.assign`
 * takes any object and returns an intersection tsc never looks inside. So a
 * module that CALLS one of those handlers gets TS2339 on a bare `Window`,
 * and that is what this file answers.
 *
 * Only the handlers a `web/js` module reads are here. The rest of the
 * surface `main.js` installs is reached from `onclick` attribute strings in
 * rendered HTML, which no checker parses; declaring them would claim a
 * coverage that does not exist. Adding a cross-module call means adding its
 * line here, which is the gate working.
 *
 * Each is typed as the export it actually is, so a signature change
 * propagates rather than needing a second edit here.
 */

declare global {
  interface Window {
    /** `analysis.js` — drop a pipeline request from the disambiguation overlay. */
    disambRemove: typeof import('./analysis.js').disambRemove;
    /** `pipeline.js` — reload the Pipeline tab. */
    loadPipeline: typeof import('./pipeline.js').loadPipeline;
    /** `recents.js` — reload the Recents tab. */
    loadRecents: typeof import('./recents.js').loadRecents;
    /** `browse.js` — re-render the open artist page in place. */
    reloadBrowseArtist: typeof import('./browse.js').reloadBrowseArtist;
    /** `state.js` — the toast notifier. */
    toast: typeof import('./state.js').toast;
  }
}

export {};
