"""HTTP dispatcher — thin client that talks to a long-running openbb-platform-api server."""

from __future__ import annotations

import re
from typing import Any

import httpx

from openbb_cli.dispatchers.protocol import Request, Response, ResponseError

_PATH_TEMPLATE_RE = re.compile(r"\{([^}]+)\}")


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
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_prefix = "/" + api_prefix.strip("/")
        if client is not None:
            self._client = client
            self._owns_client = False
        else:
            self._client = httpx.AsyncClient(timeout=timeout, headers=headers or None)
            self._owns_client = True
        self._command_methods = command_methods or {}
        self._command_url_paths = command_url_paths or {}
        self._headers = dict(headers) if headers else {}

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

    async def dispatch(self, request: Request, method: str | None = None) -> Response:
        """Send ``request`` to the platform-api endpoint that maps to its command.

        Method resolution order: explicit ``method`` > constructor's
        ``command_methods`` map > fallback ``"post"``. Path parameters
        registered in ``command_url_paths`` are substituted into the URL and
        excluded from the query string / body.
        """
        try:
            url, body_or_query = self._resolve_url(
                request.command, request.params or {}
            )
            if self._method_for(request.command, method) == "get":
                response = await self._client.get(url, params=body_or_query)
            else:
                response = await self._client.post(url, json=body_or_query)
            response.raise_for_status()
            payload: Any = response.json()
            return Response(id=request.id, ok=True, result=payload, error=None)
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

    async def aclose(self) -> None:
        """Close the HTTP client only if this dispatcher created it."""
        if self._owns_client:
            await self._client.aclose()


def http_dispatcher_from_spec(
    spec_doc: dict[str, Any], *, headers: dict[str, str] | None = None
) -> HttpDispatcher:
    """Build an ``HttpDispatcher`` from a loaded ``.spec`` document.

    Pre-extracted methods + URL templates come straight from the spec — no
    network fetch. ``headers`` are sent on every dispatched request.
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
    )


def http_dispatcher_from_server(
    base_url: str, *, headers: dict[str, str] | None = None
) -> HttpDispatcher:
    """Build an ``HttpDispatcher`` by fetching the server's OpenAPI document.

    Costs one HTTP roundtrip + a small parse on construction. Use this when
    no precomputed ``.spec`` is available; otherwise prefer
    ``http_dispatcher_from_spec`` for instant cold-start. ``headers`` are
    sent on both the OpenAPI fetch and every dispatched request.
    """
    from openbb_cli.dispatchers.openapi_schema import (
        fetch_openapi,
        url_to_command,
    )

    openapi = fetch_openapi(base_url, headers=headers)
    methods: dict[str, str] = {}
    url_paths: dict[str, str] = {}
    for url, ops in openapi.get("paths", {}).items():
        for verb in ("get", "post"):
            if verb in ops:
                cmd = url_to_command(url)
                methods.setdefault(cmd, verb)
                url_paths.setdefault(cmd, url)
                break
    return HttpDispatcher(
        base_url,
        command_methods=methods,
        command_url_paths=url_paths,
        headers=headers,
    )
