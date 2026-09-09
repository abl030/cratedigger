"""The one rule deciding which URLs serve a file out of ``web/``.

Both servers ask this module, so neither can answer a URL the other
refuses. They used to spell the rule separately and had drifted
(issue #1390 residual 8, measured 2026-09-09): ``web/server.py`` required
the ``/js/`` prefix AND the ``.js`` suffix, while
``scripts/web_dev_server.py`` resolved any path under ``web/`` and served
whatever it found. On the dev server ``GET /server.py``,
``GET /routes/pipeline.py``, ``GET /classify.py`` and ``GET
/js/../server.py`` all returned module source with a 200, and
``GET /js/jsconfig.json`` and ``GET /js/globals.d.ts`` served the two files
#1390 had just added. Production 404s every one of them.

Nothing there is secret, and the dev server binds loopback. The cost is
fidelity: an operator screenshots a change on the dev server before
shipping it, so a URL that works only there is an instrument reading high.

The rule itself, unchanged from what production already enforced:

* ``/js/<name>.js`` serves ``web/js/<name>.js``. The name is taken through
  ``os.path.basename``, so ``..`` cannot walk out of ``web/js/``: it
  resolves to a sibling that does not exist, and the caller 404s.
* the four browser icon paths serve ``web/assets/`` (issue #161).
* everything else is not a static file. ``/`` is the index, which each
  server renders its own way and neither resolves through here.

Resolution says nothing about existence: the caller stats the path and
404s a miss, which is what makes ``/js/nope.js`` and a deleted icon behave
the same way.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

#: The directory this module lives in, and the only one it resolves into.
WEB_ROOT = Path(__file__).resolve().parent

#: The URL prefix and suffix a JavaScript module request must carry.
JS_URL_PREFIX = "/js/"
JS_URL_SUFFIX = ".js"

JS_CONTENT_TYPE = "application/javascript; charset=utf-8"

#: URL path -> (filename under ``web/assets/``, content type).
ICON_ASSETS: dict[str, tuple[str, str]] = {
    "/favicon.ico": ("favicon.ico", "image/x-icon"),
    "/favicon-16x16.png": ("favicon-16x16.png", "image/png"),
    "/favicon-32x32.png": ("favicon-32x32.png", "image/png"),
    "/apple-touch-icon.png": ("apple-touch-icon.png", "image/png"),
}


@dataclass(frozen=True)
class StaticFile:
    """One resolved static file: where it is, and how to send it."""

    path: Path
    content_type: str
    cache_control: str


def resolve_static_file(url_path: str) -> StaticFile | None:
    """Resolve one GET path to a static file under ``web/``, or ``None``.

    ``url_path`` is the parsed path with no query string, exactly as both
    servers already hold it. Percent-escapes are NOT decoded here, because
    neither caller decodes them either: ``/js/%2e%2e/x.js`` stays a literal
    name that resolves to a file nobody has.
    """
    if url_path.startswith(JS_URL_PREFIX) and url_path.endswith(JS_URL_SUFFIX):
        # `basename` is the traversal guard, and it is production's own: the
        # result holds no separator, so the join cannot leave `web/js/`.
        # `/js/../server.py` fails the suffix test above; `/js/../server.js`
        # passes it and resolves to `web/js/server.js`, a file nobody has.
        name = os.path.basename(url_path[len(JS_URL_PREFIX):])
        return StaticFile(
            path=WEB_ROOT / "js" / name,
            content_type=JS_CONTENT_TYPE,
            cache_control="no-cache",
        )
    icon = ICON_ASSETS.get(url_path)
    if icon is not None:
        filename, content_type = icon
        return StaticFile(
            path=WEB_ROOT / "assets" / filename,
            content_type=content_type,
            cache_control="public, max-age=86400",
        )
    return None
