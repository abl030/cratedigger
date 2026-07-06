"""Shared deferred-import accessor for the `web.server` module.

Every route module needs the server singleton (`_db()`, `_beets_db()`,
`_serialize_row()`, `mb_api`, `check_pipeline()`, …), but importing
`web.server` at module-load time is circular — `server.py` imports every
route module to build `ALL_ROUTES`. Each module used to carry its own
byte-identical `_server()` copy; #522 folds them into this one accessor.

Tests patch attributes on `web.server` directly; this returns that same
module object, so those patches are always respected.
"""

from __future__ import annotations


def _server():
    """Deferred import of `web.server` to dodge the import cycle."""
    from web import server
    return server
