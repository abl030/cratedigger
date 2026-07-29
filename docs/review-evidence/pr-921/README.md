# PR 921 browser and accessibility evidence

Captured on 2026-07-29 from the isolated PR worktree with
[the web development server](../../../scripts/web_dev_server.py) in
`--data live-db` mode with `--preview-insecure-warning`. The server used a
read-only PostgreSQL session and blocked mutation requests.

The screenshots exercise the exact insecure-mode footer against both short
pages and live long-form data:

| Tab | Content | Desktop 1280x720 | Mobile 390x844 | 200% reflow equivalent |
| --- | --- | --- | --- | --- |
| Browse | short | [desktop](pr921-browse-desktop.png) | [mobile](pr921-browse-mobile.png) | [640 CSS px](pr921-browse-200pct-equivalent.png) |
| Recents | long, 54,975-67,204 CSS px | [desktop footer](pr921-recents-desktop-bottom.png) | [mobile footer](pr921-recents-mobile-bottom.png) | [640 CSS px footer](pr921-recents-200pct-equivalent-bottom.png) |
| Pipeline | long dashboard and tables | [desktop](pr921-pipeline-desktop.png) | [mobile](pr921-pipeline-mobile.png) | [640 CSS px](pr921-pipeline-200pct-equivalent.png) |
| Wrong Matches | short empty state | [desktop](pr921-wrong-matches-desktop.png) | [mobile](pr921-wrong-matches-mobile.png) | [640 CSS px](pr921-wrong-matches-200pct-equivalent.png) |

The 640 CSS px captures are the reflow equivalent of viewing a 1280 px
desktop viewport at 200% zoom.

Playwright geometry checks ran after each tab finished loading at all three
viewport sizes. All 12 combinations returned:

- exactly one native `footer` containing
  `Authentication is disabled for this Cratedigger instance.`;
- `position: static` for both the footer and the read-only dev badge;
- no footer/badge intersection; and
- no horizontal document overflow.

Computed footer text contrast was 9.81:1 (`rgb(214, 183, 122)` over
`rgb(17, 17, 17)`), above WCAG AA. Accessibility snapshots retain the
footer as a `contentinfo` landmark and representative page structure:

- [Browse](pr921-browse-desktop-a11y.md)
- [Recents footer](pr921-recents-a11y-summary.md)
- [Pipeline](pr921-pipeline-a11y-summary.md)
- [Wrong Matches](pr921-wrong-matches-desktop-a11y.md)

The main reviewer visually inspected every retained screenshot. The only
browser console errors were two missing favicon sizes in the development
server; no application request or rendering error occurred.
