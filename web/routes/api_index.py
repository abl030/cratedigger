"""Self-documenting API surface — ``GET /api/_index``.

Split from web/routes/pipeline.py (#481 item 3). Walks the merged
``web.server.ALL_ROUTES`` registry and emits one row per registered route:
path/pattern, method, description, and the Pydantic ``*Request`` model name
extracted from the handler's body via an AST walk. Frontends and
``pipeline-cli routes`` both consume this to build self-documenting
indexes — keep the response shape stable.
"""

import textwrap

from web.routes._registry import RouteHandler, RouteRegistration, route


# --- U18 step 2: /api/_index — self-documenting API surface ----------------
#
# Walks ``web.server.Handler``'s merged dispatch tables and emits one row per
# registered route: path/pattern, method, description, and the Pydantic
# ``*Request`` model name extracted from the handler's body. The Pydantic
# field comes from ``inspect.getsource`` + an AST walk for the
# ``parse_body(h, body, SomeRequest)`` call — see
# ``code-quality.md`` § "HTTP request bodies — use pydantic.BaseModel".
#
# Frontends and the CLI's ``routes`` command both consume this to build
# self-documenting indexes — keep the response shape stable.

def _extract_request_model(fn: object) -> str | None:
    """Pull the Pydantic ``*Request`` model name from a POST handler.

    Walks the handler's AST and returns the class name of the first
    ``parse_body(h, body, X)`` call (third positional argument). Returns
    ``None`` if the handler doesn't use ``parse_body`` or if the source
    is unavailable (e.g. .pyc-only deploys).

    Uses the same AST-walk pattern as ``tests/test_pydantic_route_audit.py
    ::_handler_uses_parse_body`` — no regex brittleness on non-canonical
    arg shapes.
    """
    if not callable(fn):
        return None
    import ast
    import inspect
    try:
        source = inspect.getsource(fn)
    except (OSError, TypeError):
        return None
    try:
        tree = ast.parse(textwrap.dedent(source))
    except SyntaxError:
        return None
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        target = node.func
        if isinstance(target, ast.Name) and target.id == "parse_body":
            pass
        elif isinstance(target, ast.Attribute) and target.attr == "parse_body":
            pass
        else:
            continue
        # Third positional arg is the Pydantic model class.
        if len(node.args) < 3:
            continue
        cls_arg = node.args[2]
        if isinstance(cls_arg, ast.Name):
            return cls_arg.id
        if isinstance(cls_arg, ast.Attribute):
            return cls_arg.attr
    return None


def get_api_index(h: RouteHandler, params: dict[str, list[str]]) -> None:
    """``GET /api/_index`` — self-documenting API surface.

    Returns a list of ``{method, path, description, request_model}`` rows
    sorted by ``(method, path)``. ``path`` is the registered string for
    static routes and the regex pattern string for pattern routes.
    ``request_model`` is the Pydantic model name for POST handlers that
    use ``parse_body``; null otherwise.

    #496: reads directly from the merged ``RouteRegistration`` list
    (``srv.ALL_ROUTES``) rather than four separate dispatch/description
    dicts — one source of truth for path, method, and description.
    """
    from web import server as srv

    entries: list[dict[str, object]] = [
        {
            "method": r.method,
            "path": r.path,
            "description": r.description,
            "request_model": (
                _extract_request_model(r.handler) if r.method == "POST" else None
            ),
        }
        for r in srv.ALL_ROUTES
    ]

    entries.sort(key=lambda e: (str(e["method"]), str(e["path"])))
    h._json(entries)


ROUTES: list[RouteRegistration] = [
    route(
        "GET", "/api/_index", get_api_index,
        "Self-documenting API surface — every route's path, method, "
        "description, and Pydantic request model.",
        classified=True,
    ),
]
