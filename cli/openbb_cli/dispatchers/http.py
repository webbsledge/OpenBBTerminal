"""HTTP dispatcher — thin client that talks to a long-running openbb-platform-api server."""

from __future__ import annotations

import inspect
import re
from typing import Any

import httpx

from openbb_cli.auth import AuthContext, AuthDecision, AuthHook
from openbb_cli.dispatchers._unpack import unpack_response
from openbb_cli.dispatchers.openapi_schema import (
    PROVIDER_SECTION_SPLIT_RE,
    PROVIDER_TAG_RE,
)
from openbb_cli.dispatchers.protocol import Request, Response, ResponseError

_PATH_TEMPLATE_RE = re.compile(r"\{([^}]+)\}")


def _help_for_provider(text: str | None, provider: str) -> str | None:
    r"""Pick the sections of an OpenBB-merged help string that apply to ``provider``.

    OpenBB Platform concatenates per-provider help into one description,
    sections separated by ``;\\n    `` and tagged with ``(provider:
    <comma-list>)``. Shared sections carry no tag. Surfacing the merged
    blob under each provider in ``--describe`` produces nonsense — cboe's
    ``symbol`` shouldn't show ``(provider: intrinio)``. Keep only the
    sections this provider participates in (shared + own), strip the
    annotation, rejoin.
    """
    if not text:
        return text
    sections = PROVIDER_SECTION_SPLIT_RE.split(text)
    target = provider.lower()
    kept: list[str] = []
    for raw_section in sections:
        section = raw_section.strip()
        if not section:
            continue
        match = PROVIDER_TAG_RE.search(section)
        if match is None:
            kept.append(section)
            continue
        tag_providers = {
            p.strip().lower() for p in match.group(1).split(",") if p.strip()
        }
        if target in tag_providers:
            kept.append(PROVIDER_TAG_RE.sub("", section).rstrip())
    return "\n".join(kept) if kept else None


def _slim_param(p: dict[str, Any]) -> dict[str, Any]:
    """Strip empty / falsy fields from a normalized parameter entry.

    The on-disk spec stores every parameter with the full canonical key
    set so the dispatcher's parser builders are uniform. ``--describe``
    is consumer-facing — flags like ``is_list: false``, ``choices: []``,
    ``help: null`` are pure noise. Drop them.
    """
    out: dict[str, Any] = {
        "name": p["name"],
        "in": p.get("in", "query"),
        "type": p.get("type", "string"),
    }
    if p.get("is_list"):
        out["is_list"] = True
    if p.get("required"):
        out["required"] = True
    if p.get("default") is not None:
        out["default"] = p["default"]
    if p.get("choices"):
        out["choices"] = p["choices"]
    if p.get("help"):
        out["help"] = p["help"]
    return out


def _group_by_provider(
    raw_params: list[dict[str, Any]],
    body_params: list[dict[str, Any]],
    providers: list[str],
    output_schema: dict[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    """Group parameters and output schemas per provider.

    Multi-provider OpenBB endpoints are a discriminated union: the
    ``provider`` flag selects a different parameter set *and* a different
    response data class. Showing one merged ``parameters`` array misleads
    the user — flags only valid for ``intrinio`` look like they apply to
    ``cboe``. Group instead: for each provider, list every parameter that
    targets it (shared params with no ``providers`` tag are repeated under
    every provider, since they're inputs every variant accepts) plus the
    request body fields, and pick the matching response schema variant.
    """
    out: dict[str, dict[str, Any]] = {}
    for provider in providers:
        provider_params: list[dict[str, Any]] = []
        for p in raw_params:
            if p.get("name") == "provider":
                continue
            tags = p.get("providers") or []
            if tags and provider not in tags:
                continue
            slim = _slim_param(p)
            slim["help"] = _help_for_provider(slim.get("help"), provider)
            if not slim.get("help"):
                slim.pop("help", None)
            provider_params.append(slim)
        provider_params.extend(body_params)
        out[provider] = {
            "parameters": provider_params,
            "output_schema": _provider_output_schema(output_schema, provider),
        }
    return out


def _provider_output_schema(
    output_schema: dict[str, Any] | None, provider: str
) -> dict[str, Any] | None:
    """Pick the per-provider variant out of a OneOf-of-data-classes results list.

    OpenBB Platform multi-provider endpoints declare ``results`` as
    ``anyOf[{items: {oneOf: [<ProviderA>Data, <ProviderB>Data, ...]}, type:
    array}, null]``. Each variant's ``title`` starts with the provider name
    in mixed case (``IntrinioEquityQuoteData``, ``FMPEquityQuoteData``,
    ``YFinanceEquityQuoteData``). Match case-insensitively on the title
    prefix and return that single variant — the OBBject wrapper above
    ``results`` is dropped because the user asked for "the actual output
    schema", not the envelope.
    """
    if not isinstance(output_schema, dict):
        return None
    results = (output_schema.get("properties") or {}).get("results")
    if not isinstance(results, dict):
        return None
    candidates: list[Any] = []
    for variant in results.get("anyOf") or [results]:
        if not isinstance(variant, dict) or variant.get("type") == "null":
            continue
        if variant.get("type") == "array" and isinstance(variant.get("items"), dict):
            inner = variant["items"]
            candidates.extend(inner.get("oneOf") or [inner])
        else:
            candidates.extend(variant.get("oneOf") or [variant])
    target = provider.lower()
    for cand in candidates:
        if not isinstance(cand, dict):
            continue
        title = (cand.get("title") or "").lower()
        if title.startswith(target):
            return cand
    if len(candidates) == 1 and isinstance(candidates[0], dict):
        return candidates[0]
    return None


def _body_schema_to_params(
    body_schema: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """Flatten a request-body object schema into per-field parameter entries.

    POST endpoints (econometrics, technical, quantitative, charting routes)
    declare their inputs in ``requestBody.content[json].schema``, not in
    ``parameters[]``. Surfacing those properties as ``in: "body"`` entries
    in ``--describe`` lets a script writer see every input — query, path,
    *and* body — in one flat list. Items schemas for arrays of objects
    (e.g. ``data: list[Data]``) are kept inline so the user can see the
    element shape.
    """
    if not isinstance(body_schema, dict) or body_schema.get("type") != "object":
        return []
    properties = body_schema.get("properties") or {}
    required_set = set(body_schema.get("required") or [])
    out: list[dict[str, Any]] = []
    for name, schema in properties.items():
        if not isinstance(schema, dict):
            continue
        schema_type = schema.get("type")
        is_list = schema_type == "array"
        if is_list:
            items = schema.get("items") or {}
            type_name = items.get("type") if isinstance(items, dict) else None
            type_name = type_name or "object"
        else:
            type_name = schema_type or "string"
        entry: dict[str, Any] = {
            "name": name,
            "in": "body",
            "type": type_name,
        }
        if is_list:
            entry["is_list"] = True
        if name in required_set:
            entry["required"] = True
        if "default" in schema and schema["default"] is not None:
            entry["default"] = schema["default"]
        help_text = schema.get("description") or schema.get("title")
        if help_text:
            entry["help"] = help_text
        if is_list:
            items = schema.get("items")
            if isinstance(items, dict) and items.get("type") == "object":
                entry["items"] = items
        out.append(entry)
    return out


def _decode_response(response: httpx.Response) -> Any:
    """Return the response body as JSON when the server says so, otherwise text.

    Servers that honor a ``--format`` query param (e.g. Congress.gov accepts
    ``?format=xml`` and returns an XML body with ``content-type:
    application/xml``) would otherwise hit ``response.json()`` and raise
    ``JSONDecodeError``. Sniff the content type first and fall back gracefully.
    """
    content_type = (response.headers.get("content-type") or "").lower().split(";", 1)[0]
    if content_type.endswith("json") or content_type == "":
        try:
            return response.json()
        except ValueError:
            return response.text
    return response.text


def _shape_result(payload: Any) -> Any:
    """Apply the same envelope unwrap codegen does, then return a tidy result.

    Wraps the unpacked rows + metadata in the OBBject-shaped dict the rest of
    the CLI rendering expects: a single row dict (when there's exactly one)
    or the row list otherwise. Metadata, when present, is exposed alongside
    so downstream renderers can still surface it.
    """
    if not isinstance(payload, (dict, list)):
        return payload
    rows, metadata = unpack_response(payload)
    if not rows and not metadata:
        return payload
    body: Any
    if len(rows) == 1:
        sole = rows[0]
        body = sole.get("value", sole) if set(sole.keys()) == {"value"} else sole
    else:
        body = [r.get("value", r) if set(r.keys()) == {"value"} else r for r in rows]
    if metadata:
        return {"results": body, "metadata": metadata}
    return body


class HttpDispatcher:
    """Multi-tenant dispatcher backed by openbb-platform-api over HTTP.

    The heavy ``import openbb`` cost lives on the server, so cold-starting this
    dispatcher is fast — useful when ``openbb foo bar`` is invoked many times
    or by many users sharing a server.

    Two per-command maps drive routing:

    * ``command_methods``: ``{dotted.command: "get" | "post"}``. OpenBB
      Platform endpoints are mostly GET, a few POST; without this the
      dispatcher falls back to POST and 405s on GETs.
    * ``command_url_paths``: ``{dotted.command: "/api/.../{template}"}``.
      For specs whose URLs include path placeholders (NY Fed Markets API,
      Stripe, GitHub, etc.) the dispatcher needs the original URL template
      to substitute path params into. Without it the dispatcher reconstructs
      the URL from the dotted command — fine for OpenBB Platform but loses
      placeholders.
    """

    def __init__(
        self,
        base_url: str,
        *,
        client: httpx.AsyncClient | None = None,
        timeout: float = 60.0,
        api_prefix: str = "/api/v1",
        command_methods: dict[str, str] | None = None,
        command_url_paths: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        query_params: dict[str, str] | None = None,
        spec_doc: dict[str, Any] | None = None,
        auth_hook: AuthHook | None = None,
        namespace: str | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_prefix = "/" + api_prefix.strip("/")
        self._timeout = timeout
        if client is not None:
            self._client: httpx.AsyncClient | None = client
            self._owns_client = False
        else:
            self._client = None
            self._owns_client = True
        self._command_methods = command_methods or {}
        self._command_url_paths = command_url_paths or {}
        self._headers = dict(headers) if headers else {}
        self._query_params = dict(query_params) if query_params else {}
        self._spec_doc = spec_doc or {}
        self._auth_hook = auth_hook
        self._namespace = namespace

    def _url_for(self, command: str) -> str:
        """Build the fully-qualified URL for a command, no path-param substitution.

        Used when no URL template is registered for the command (the OpenBB
        Platform default). For specs that register templates, see
        ``_resolve_url`` which also handles ``{placeholder}`` substitution.
        """
        path = command.replace(".", "/").strip("/")
        return f"{self._base_url}{self._api_prefix}/{path}"

    def _resolve_url(
        self, command: str, params: dict[str, Any]
    ) -> tuple[str, dict[str, Any]]:
        """Build the URL for ``command`` and split path-substituted params off.

        Returns ``(url, remaining_params)``. ``remaining_params`` is what goes
        on the query string (GET) or in the JSON body (POST).
        """
        template = self._command_url_paths.get(command)
        if template is None:
            return self._url_for(command), dict(params)

        consumed: set[str] = set()

        def _substitute(match: re.Match[str]) -> str:
            key = match.group(1)
            consumed.add(key)
            value = params.get(key)
            if value is None:
                return match.group(0)
            return str(value)

        path = _PATH_TEMPLATE_RE.sub(_substitute, template)
        remaining = {k: v for k, v in params.items() if k not in consumed}
        return f"{self._base_url}{path}", remaining

    def _method_for(self, command: str, override: str | None) -> str:
        """Resolve the HTTP method for ``command``: explicit override > map > default."""
        if override is not None:
            return override.lower()
        return self._command_methods.get(command, "post").lower()

    async def _invoke_auth_hook(
        self, request: Request, method_lower: str
    ) -> AuthDecision:
        """Run the configured auth hook (if any) and return its decision.

        Hooks may be sync or async — we await coroutines and pass plain
        callables straight through. A hook that raises is converted into an
        ``allow=False`` decision so the rest of ``dispatch`` reports it as a
        structured error response instead of letting it crash the dispatch
        loop. Returns an empty allow-decision when no hook is configured so
        the caller's merge logic stays one branch.
        """
        if self._auth_hook is None:
            return AuthDecision()
        ctx = AuthContext(
            namespace=self._namespace,
            command=request.command,
            params=dict(request.params or {}),
            method=method_lower,
        )
        try:
            result = self._auth_hook(ctx)
            if inspect.isawaitable(result):
                result = await result
        except Exception as exc:  # noqa: BLE001 — surface hook failures as deny
            return AuthDecision(allow=False, deny_reason=f"auth hook raised: {exc}")
        if not isinstance(result, AuthDecision):
            return AuthDecision(
                allow=False,
                deny_reason=(
                    f"auth hook returned {type(result).__name__}; expected AuthDecision"
                ),
            )
        return result

    async def dispatch(self, request: Request, method: str | None = None) -> Response:
        """Send ``request`` to the platform-api endpoint that maps to its command.

        Reserved introspection commands ``__commands__`` and ``__schema__``
        short-circuit before HTTP routing — they read directly from the spec
        the dispatcher was built from, so they work in one-shot, batch, and
        REPL flows without round-tripping the upstream server.

        Method resolution order: explicit ``method`` > constructor's
        ``command_methods`` map > fallback ``"post"``. Path parameters
        registered in ``command_url_paths`` are substituted into the URL and
        excluded from the query string / body.

        When this dispatcher owns its ``httpx.AsyncClient``, a fresh client is
        created per dispatch so each ``asyncio.run`` invocation (the REPL's
        sync-bridge path) gets a client bound to the current event loop —
        cached clients leak ``Event loop is closed`` after the first call.
        """
        if request.command == "__commands__":
            return await self._list_commands(request)
        if request.command == "__schema__":
            return await self._describe_command(request)
        try:
            url, body_or_query = self._resolve_url(
                request.command, request.params or {}
            )
            method_lower = self._method_for(request.command, method)
            decision = await self._invoke_auth_hook(request, method_lower)
            if not decision.allow:
                return Response(
                    id=request.id,
                    ok=False,
                    error=ResponseError(
                        type="AccessDenied",
                        message=decision.deny_reason or "auth hook denied request",
                    ),
                )
            extra_headers = decision.headers or None
            extra_query = decision.query_params or None
            async with self._client_context() as client:
                if method_lower == "get":
                    merged_query = {
                        **self._query_params,
                        **(extra_query or {}),
                        **body_or_query,
                    }
                    response = await client.get(
                        url, params=merged_query, headers=extra_headers
                    )
                else:
                    merged_query = {**self._query_params, **(extra_query or {})}
                    response = await client.post(
                        url,
                        params=merged_query or None,
                        json=body_or_query,
                        headers=extra_headers,
                    )
            response.raise_for_status()
            payload = _decode_response(response)
            return Response(
                id=request.id, ok=True, result=_shape_result(payload), error=None
            )
        except httpx.HTTPStatusError as exc:
            try:
                detail = exc.response.json()
            except ValueError:
                detail = exc.response.text
            return Response(
                id=request.id,
                ok=False,
                error=ResponseError(
                    type=f"HTTP{exc.response.status_code}",
                    message=str(detail),
                ),
            )
        except httpx.RequestError as exc:
            return Response(
                id=request.id,
                ok=False,
                error=ResponseError(type=type(exc).__name__, message=str(exc)),
            )
        except Exception as exc:  # noqa: BLE001 — same isolation contract as LocalDispatcher
            return Response(
                id=request.id,
                ok=False,
                error=ResponseError(type=type(exc).__name__, message=str(exc)),
            )

    async def _list_commands(self, request: Request) -> Response:
        """Return ``[{name, description}, ...]`` for every command the auth hook permits.

        When an auth hook is configured, every command is gated through it
        so RBAC implementations can hide endpoints the caller isn't
        authorized to call. Hook signature is the same as for dispatch
        (``AuthContext`` → ``AuthDecision``); a deny silently drops the
        entry from the listing — visibility, not an error. With no hook
        configured every command is included, matching the original
        behavior.
        """
        commands = self._spec_doc.get("commands", {})
        rows: list[dict[str, Any]] = []
        for name, meta in commands.items():
            if self._auth_hook is not None:
                decision = await self._invoke_auth_hook(
                    Request(command=name), method_lower="list"
                )
                if not decision.allow:
                    continue
            rows.append(
                {
                    "name": name,
                    "description": (meta.get("description") or "").split(".")[0] or "",
                }
            )
        rows.sort(key=lambda r: r["name"])
        return Response(id=request.id, ok=True, result=rows, error=None)

    async def _describe_command(self, request: Request) -> Response:
        """Return ``{name, parameters, output_schema}`` for one command.

        Slim by design — anyone scripting against the CLI needs to know "what
        flags do I pass" and "what comes back." Method / url_path /
        description / per-status response matrices are noise at that layer
        and live in the raw spec for the few cases they're needed.

        ``parameters`` is the unified input surface: query + path entries
        from OpenAPI ``parameters[]`` followed by the request body's
        top-level fields (each tagged ``in: "body"``). Empty / null per-
        parameter fields (``default``, ``choices``, ``is_list`` when false,
        etc.) are stripped so each entry shows only what's meaningful.

        ``output_schema`` is the success-response body schema, already
        dereferenced — for OpenBB Platform that's the ``OBBject[...]``
        wrapper with the concrete results model spliced in.

        When an auth hook is configured it gates the describe response too
        — RBAC implementations can deny schema introspection for
        unauthorized commands so the surface stays consistent with what
        the caller is allowed to actually invoke.
        """
        name = (request.params or {}).get("name")
        if not name:
            return Response(
                id=request.id,
                ok=False,
                error=ResponseError(
                    type="MissingParameter",
                    message="__schema__ requires --name=<command>",
                ),
            )
        cmd_spec = self._spec_doc.get("commands", {}).get(name)
        if cmd_spec is None:
            return Response(
                id=request.id,
                ok=False,
                error=ResponseError(
                    type="UnknownCommand",
                    message=(
                        f"command not in spec: {name!r}. "
                        "Use __commands__ to list available commands."
                    ),
                ),
            )
        if self._auth_hook is not None:
            decision = await self._invoke_auth_hook(
                Request(command=name), method_lower="schema"
            )
            if not decision.allow:
                return Response(
                    id=request.id,
                    ok=False,
                    error=ResponseError(
                        type="AccessDenied",
                        message=decision.deny_reason or "auth hook denied describe",
                    ),
                )
        body_params = _body_schema_to_params(cmd_spec.get("request_body_schema"))
        providers = cmd_spec.get("providers") or []
        if providers:
            # Optional ``provider`` param narrows the response to one
            # provider's slice — same shape a single-provider command
            # would return ({name, parameters, output_schema}). Lets
            # callers request just ``equity.price.quote:intrinio``
            # without scrolling past the other five providers.
            requested_provider = (request.params or {}).get("provider")
            grouped = _group_by_provider(
                cmd_spec.get("parameters") or [],
                body_params,
                providers,
                cmd_spec.get("response_schema"),
            )
            if requested_provider:
                slice_ = grouped.get(requested_provider)
                if slice_ is None:
                    return Response(
                        id=request.id,
                        ok=False,
                        error=ResponseError(
                            type="UnknownProvider",
                            message=(
                                f"provider {requested_provider!r} not declared "
                                f"by {name!r}. Available: "
                                f"{', '.join(sorted(grouped))}."
                            ),
                        ),
                    )
                return Response(
                    id=request.id,
                    ok=True,
                    result={
                        "name": name,
                        "provider": requested_provider,
                        **slice_,
                    },
                    error=None,
                )
            return Response(
                id=request.id,
                ok=True,
                result={"name": name, "providers": grouped},
                error=None,
            )
        params: list[dict[str, Any]] = [
            _slim_param(p) for p in (cmd_spec.get("parameters") or [])
        ]
        params.extend(body_params)
        return Response(
            id=request.id,
            ok=True,
            result={
                "name": name,
                "parameters": params,
                "output_schema": cmd_spec.get("response_schema"),
            },
            error=None,
        )

    def _client_context(self):
        """Yield an ``httpx.AsyncClient`` for one dispatch.

        Fresh client when we own ourselves (avoids cross-loop reuse from
        repeated ``asyncio.run`` calls in the REPL). Otherwise a no-op
        wrapper around the externally-managed client.
        """
        from contextlib import asynccontextmanager

        owns = self._owns_client
        client_ref = self._client
        timeout = self._timeout
        headers = self._headers or None

        @asynccontextmanager
        async def _ctx():
            if owns:
                async with httpx.AsyncClient(timeout=timeout, headers=headers) as c:
                    yield c
            else:
                yield client_ref

        return _ctx()

    async def aclose(self) -> None:
        """No-op.

        Owned clients are created and torn down per dispatch via
        ``_client_context``. Externally-supplied clients are owned by the
        caller and must be closed by them — closing them here would surprise
        callers that share a single client across multiple dispatchers.
        """
        return None


def http_dispatcher_from_spec(
    spec_doc: dict[str, Any],
    *,
    headers: dict[str, str] | None = None,
    query_params: dict[str, str] | None = None,
    auth_hook: AuthHook | None = None,
    namespace: str | None = None,
) -> HttpDispatcher:
    """Build an ``HttpDispatcher`` from a loaded ``.spec`` document.

    Pre-extracted methods + URL templates come straight from the spec — no
    network fetch. ``headers`` and ``query_params`` are sent on every
    dispatched request. ``auth_hook`` (when supplied) runs before each
    network dispatch and may inject extra headers / query params or deny
    the call outright; ``namespace`` is what the hook sees in
    ``AuthContext.namespace`` and lets a single shared hook tell which
    backend it was invoked for.
    """
    commands = spec_doc.get("commands", {})
    methods = {cmd: meta.get("method", "post") for cmd, meta in commands.items()}
    url_paths = {
        cmd: meta["url_path"] for cmd, meta in commands.items() if meta.get("url_path")
    }
    return HttpDispatcher(
        spec_doc["base_url"],
        api_prefix=spec_doc.get("api_prefix", "/api/v1"),
        command_methods=methods,
        command_url_paths=url_paths,
        headers=headers,
        query_params=query_params,
        spec_doc=spec_doc,
        auth_hook=auth_hook,
        namespace=namespace,
    )


def http_dispatcher_from_server(
    base_url: str,
    *,
    headers: dict[str, str] | None = None,
    query_params: dict[str, str] | None = None,
    auth_hook: AuthHook | None = None,
    namespace: str | None = None,
) -> HttpDispatcher:
    """Build an ``HttpDispatcher`` by fetching the server's OpenAPI document.

    Delegates to ``build_spec_document`` so the live-fetch path goes through
    the same spec normalization the ``--generate-spec`` path uses: HTML-
    embedded specs are auto-extracted, ``$ref`` parameters are resolved, the
    ``servers[0].url`` segment is prepended to the base URL (so
    ``--server https://api.congress.gov`` correctly hits ``/v3/...``), and
    api-prefix detection is applied. ``headers`` and ``query_params`` are
    sent on both the OpenAPI fetch and every dispatched request. The
    ``auth_hook`` only fires on dispatches — the initial OpenAPI fetch uses
    static auth so the spec discovery path stays predictable.
    """
    from openbb_cli.dispatchers.openapi_schema import fetch_openapi
    from openbb_cli.dispatchers.spec import build_spec_document

    openapi = fetch_openapi(base_url, headers=headers, query_params=query_params)
    spec_doc = build_spec_document(openapi, base_url=base_url)
    return http_dispatcher_from_spec(
        spec_doc,
        headers=headers,
        query_params=query_params,
        auth_hook=auth_hook,
        namespace=namespace,
    )
