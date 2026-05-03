"""Tests for openbb_cli.dispatchers.http — HttpDispatcher against openbb-platform-api."""

import json

import httpx
import pytest

from openbb_cli.dispatchers.http import HttpDispatcher
from openbb_cli.dispatchers.protocol import Request


def _make_dispatcher(handler) -> HttpDispatcher:
    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport, base_url="http://test")
    return HttpDispatcher("http://test", client=client)


@pytest.mark.asyncio
async def test_dispatch_success_translates_dotted_path():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["body"] = json.loads(request.content) if request.content else {}
        return httpx.Response(200, json={"echo": captured["body"]})

    d = _make_dispatcher(handler)
    try:
        resp = await d.dispatch(
            Request(
                id="x", command="equity.price.historical", params={"symbol": "AAPL"}
            )
        )
    finally:
        await d.aclose()

    assert resp.ok is True
    assert resp.id == "x"
    # The single-key ``echo`` wrapper gets stripped by the response
    # unwrap, matching how an installed extension surfaces the same row.
    assert resp.result == {"symbol": "AAPL"}
    assert "/api/v1/equity/price/historical" in captured["url"]


@pytest.mark.asyncio
async def test_dispatch_4xx_yields_http_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"detail": "not found"})

    d = _make_dispatcher(handler)
    try:
        resp = await d.dispatch(Request(command="nope"))
    finally:
        await d.aclose()

    assert resp.ok is False
    assert resp.error is not None
    assert resp.error.type == "HTTP404"


@pytest.mark.asyncio
async def test_dispatch_5xx_with_text_body():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="internal explosion")

    d = _make_dispatcher(handler)
    try:
        resp = await d.dispatch(Request(command="equity.q"))
    finally:
        await d.aclose()

    assert resp.ok is False
    assert resp.error is not None
    assert resp.error.type == "HTTP500"


@pytest.mark.asyncio
async def test_dispatch_request_error():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("dns failed")

    d = _make_dispatcher(handler)
    try:
        resp = await d.dispatch(Request(command="x"))
    finally:
        await d.aclose()

    assert resp.ok is False
    assert resp.error is not None
    assert resp.error.type == "ConnectError"


@pytest.mark.asyncio
async def test_dispatch_unexpected_exception_isolated():
    def handler(request: httpx.Request) -> httpx.Response:
        raise RuntimeError("boom")

    d = _make_dispatcher(handler)
    try:
        resp = await d.dispatch(Request(command="x"))
    finally:
        await d.aclose()

    assert resp.ok is False
    assert resp.error is not None
    assert resp.error.type in {"RuntimeError", "TransportError"}


@pytest.mark.asyncio
async def test_aclose_owns_client_when_constructed_internally():
    d = HttpDispatcher("http://x")
    assert d._owns_client is True
    await d.aclose()


@pytest.mark.asyncio
async def test_aclose_does_not_close_external_client():
    transport = httpx.MockTransport(lambda r: httpx.Response(200, json={}))
    client = httpx.AsyncClient(transport=transport)
    d = HttpDispatcher("http://x", client=client)
    await d.aclose()
    response = await client.get("http://x/")
    assert response.status_code == 200
    await client.aclose()


def test_url_for_strips_separators():
    d = HttpDispatcher("http://srv/")
    assert d._url_for("equity.price") == "http://srv/api/v1/equity/price"


def test_custom_api_prefix():
    d = HttpDispatcher("http://srv", api_prefix="custom/v2")
    assert d._url_for("ping") == "http://srv/custom/v2/ping"


@pytest.mark.asyncio
async def test_dispatch_unwraps_single_array_envelope():
    """``{"refRates": [...]}`` -> the rows list, matching codegen behavior."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "refRates": [
                    {"effectiveDate": "2026-04-30", "type": "TGCR"},
                    {"effectiveDate": "2026-04-29", "type": "TGCR"},
                ]
            },
        )

    d = _make_dispatcher(handler)
    try:
        resp = await d.dispatch(Request(command="rates.last"))
    finally:
        await d.aclose()
    assert resp.ok is True
    assert resp.result == [
        {"effectiveDate": "2026-04-30", "type": "TGCR"},
        {"effectiveDate": "2026-04-29", "type": "TGCR"},
    ]


@pytest.mark.asyncio
async def test_dispatch_splits_metadata_from_rows():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"asOfDate": "2026-04-30", "operations": [{"id": 1}, {"id": 2}]},
        )

    d = _make_dispatcher(handler)
    try:
        resp = await d.dispatch(Request(command="x"))
    finally:
        await d.aclose()
    assert resp.ok is True
    assert resp.result == {
        "results": [{"id": 1}, {"id": 2}],
        "metadata": {"asOfDate": "2026-04-30"},
    }


@pytest.mark.asyncio
async def test_dispatch_unwraps_array_of_scalars_into_value_field_then_flattens():
    """Single-key envelope around a string array -> a flat list of strings."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"asOfDates": ["2026-04-30", "2026-04-23"]})

    d = _make_dispatcher(handler)
    try:
        resp = await d.dispatch(Request(command="x"))
    finally:
        await d.aclose()
    # Each scalar gets wrapped as ``{value: x}`` by ``unpack_response``;
    # ``_shape_result`` then unwraps the synthetic ``value`` key for clean
    # rendering.
    assert resp.result == ["2026-04-30", "2026-04-23"]


@pytest.mark.asyncio
async def test_dispatch_passes_text_response_through_unchanged():
    """Non-JSON responses (XML, CSV, plain text) bypass the unwrap entirely."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, text="<xml>raw</xml>", headers={"content-type": "application/xml"}
        )

    d = _make_dispatcher(handler)
    try:
        resp = await d.dispatch(Request(command="x"))
    finally:
        await d.aclose()
    assert resp.result == "<xml>raw</xml>"


@pytest.mark.asyncio
async def test_dispatch_returns_payload_unchanged_when_unwrap_yields_nothing():
    """Empty list response: ``unpack_response`` produces ``([], {})``;
    the dispatcher leaves the original payload untouched rather than
    collapsing to ``None``."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[])

    d = _make_dispatcher(handler)
    try:
        resp = await d.dispatch(Request(command="x"))
    finally:
        await d.aclose()
    assert resp.result == []


@pytest.mark.asyncio
async def test_dispatch_uses_get_when_command_methods_says_so():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = str(request.url)
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport, base_url="http://t")
    d = HttpDispatcher(
        "http://t",
        client=client,
        command_methods={"equity.quote": "get"},
    )
    try:
        await d.dispatch(Request(command="equity.quote", params={"symbol": "AAPL"}))
    finally:
        await d.aclose()
    assert captured["method"] == "GET"
    assert "symbol=AAPL" in captured["url"]


@pytest.mark.asyncio
async def test_dispatch_falls_back_to_post_without_method_map():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        return httpx.Response(200, json={})

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport, base_url="http://t")
    d = HttpDispatcher("http://t", client=client)
    try:
        await d.dispatch(Request(command="equity.quote", params={"symbol": "X"}))
    finally:
        await d.aclose()
    assert captured["method"] == "POST"


@pytest.mark.asyncio
async def test_dispatch_explicit_method_overrides_map():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        return httpx.Response(200, json={})

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport, base_url="http://t")
    d = HttpDispatcher(
        "http://t",
        client=client,
        command_methods={"x": "post"},
    )
    try:
        await d.dispatch(Request(command="x"), method="get")
    finally:
        await d.aclose()
    assert captured["method"] == "GET"


def test_http_dispatcher_from_spec_extracts_methods():
    from openbb_cli.dispatchers.http import http_dispatcher_from_spec

    spec_doc = {
        "base_url": "http://h",
        "api_prefix": "/api/v1",
        "commands": {
            "equity.quote": {"method": "get"},
            "scratch.rebuild": {"method": "post"},
        },
    }
    d = http_dispatcher_from_spec(spec_doc)
    assert d._command_methods == {"equity.quote": "get", "scratch.rebuild": "post"}
    assert d._base_url == "http://h"


def test_http_dispatcher_from_server_fetches_and_maps(monkeypatch):
    """``http_dispatcher_from_server`` synthesizes a methods map from openapi.json."""
    from openbb_cli.dispatchers import http as http_mod

    fake_openapi = {
        "paths": {
            "/api/v1/x/y": {"get": {}},
            "/api/v1/foo/bar": {"post": {}},
            "/api/v1/skip": {"delete": {}},
        }
    }

    def fake_fetch(
        base_url, *, timeout=10.0, path=None, headers=None, query_params=None
    ):
        return fake_openapi

    monkeypatch.setattr(
        "openbb_cli.dispatchers.openapi_schema.fetch_openapi", fake_fetch
    )
    d = http_mod.http_dispatcher_from_server("http://h")
    assert d._command_methods == {"x.y": "get", "foo.bar": "post"}


@pytest.mark.asyncio
async def test_dispatch_sends_constructor_headers():
    """Headers passed at construction time go on every request."""
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["headers"] = dict(request.headers)
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)
    dispatcher = HttpDispatcher(
        "http://t",
        headers={"Authorization": "Bearer xyz", "X-Tenant": "acme"},
        client=httpx.AsyncClient(
            transport=transport,
            base_url="http://t",
            headers={"Authorization": "Bearer xyz", "X-Tenant": "acme"},
        ),
        command_methods={"x": "get"},
    )
    try:
        await dispatcher.dispatch(Request(command="x"))
    finally:
        await dispatcher.aclose()
    assert captured["headers"]["authorization"] == "Bearer xyz"
    assert captured["headers"]["x-tenant"] == "acme"


def test_http_dispatcher_owns_client_when_headers_passed():
    """No explicit client → dispatcher remembers headers + builds a fresh
    client per ``dispatch()`` call. We assert the bookkeeping; the actual
    header round-trip is covered by the ``handler``-based dispatch tests
    above which observe headers on the wire.
    """
    d = HttpDispatcher(
        "http://h",
        headers={"X-Foo": "bar"},
    )
    assert d._headers == {"X-Foo": "bar"}
    assert d._owns_client is True
    assert d._client is None


def test_http_dispatcher_from_spec_passes_headers():
    """``http_dispatcher_from_spec`` forwards headers to the dispatcher."""
    from openbb_cli.dispatchers.http import http_dispatcher_from_spec

    spec_doc = {
        "base_url": "http://h",
        "api_prefix": "/api/v1",
        "commands": {"foo": {"method": "get", "url_path": "/api/v1/foo"}},
    }
    d = http_dispatcher_from_spec(spec_doc, headers={"X-API-Key": "k"})
    assert d._headers == {"X-API-Key": "k"}


@pytest.mark.asyncio
async def test_dispatch_substitutes_path_params_into_url_template():
    """``{format}``-style placeholders are substituted from the request params."""
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport, base_url="http://t")
    d = HttpDispatcher(
        "http://t",
        client=client,
        command_methods={"fxs.list.counterparties": "get"},
        command_url_paths={
            "fxs.list.counterparties": "/api/fxs/list/counterparties.{format}"
        },
    )
    try:
        await d.dispatch(
            Request(
                command="fxs.list.counterparties",
                params={"format": "json", "extra": "value"},
            )
        )
    finally:
        await d.aclose()
    assert "/api/fxs/list/counterparties.json" in captured["url"]
    assert "format=" not in captured["url"]
    assert "extra=value" in captured["url"]


@pytest.mark.asyncio
async def test_dispatch_keeps_placeholder_when_param_missing():
    """A missing path param leaves the placeholder so the server returns a clean error."""
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        return httpx.Response(200, json={})

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport, base_url="http://t")
    d = HttpDispatcher(
        "http://t",
        client=client,
        command_methods={"x": "get"},
        command_url_paths={"x": "/api/x/{missing}"},
    )
    try:
        await d.dispatch(Request(command="x", params={}))
    finally:
        await d.aclose()
    assert "%7Bmissing%7D" in captured["url"] or "{missing}" in captured["url"]


@pytest.mark.asyncio
async def test_dispatch_invokes_sync_auth_hook_and_merges_headers():
    """Sync hook returning headers/query gets merged into the request."""
    from openbb_cli.auth import AuthContext, AuthDecision

    captured: dict[str, object] = {}
    seen: list[AuthContext] = []

    def hook(ctx):
        seen.append(ctx)
        return AuthDecision(headers={"X-User": "alice"}, query_params={"trace": "1"})

    def handler(request: httpx.Request) -> httpx.Response:
        captured["headers"] = dict(request.headers)
        captured["url"] = str(request.url)
        return httpx.Response(200, json={})

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport, base_url="http://test")
    d = HttpDispatcher(
        "http://test",
        client=client,
        command_methods={"foo": "get"},
        auth_hook=hook,
        namespace="ns1",
    )
    try:
        resp = await d.dispatch(Request(command="foo", params={"a": 1}))
    finally:
        await d.aclose()
    assert resp.ok
    assert captured["headers"]["x-user"] == "alice"
    assert "trace=1" in captured["url"]
    assert seen[0].namespace == "ns1"
    assert seen[0].command == "foo"
    assert seen[0].params == {"a": 1}


@pytest.mark.asyncio
async def test_dispatch_supports_async_auth_hook():
    """Awaited coroutine hooks work the same as sync ones."""
    from openbb_cli.auth import AuthDecision

    async def hook(ctx):
        return AuthDecision(headers={"X-Async": "yes"})

    captured: dict[str, dict[str, str]] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["h"] = dict(request.headers)
        return httpx.Response(200, json={})

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport, base_url="http://test")
    d = HttpDispatcher(
        "http://test",
        client=client,
        command_methods={"foo": "get"},
        auth_hook=hook,
    )
    try:
        await d.dispatch(Request(command="foo"))
    finally:
        await d.aclose()
    assert captured["h"]["x-async"] == "yes"


@pytest.mark.asyncio
async def test_dispatch_auth_hook_deny_short_circuits():
    """``allow=False`` produces an AccessDenied error and skips the network call."""
    from openbb_cli.auth import AuthDecision

    network_calls: list[str] = []

    def hook(ctx):
        return AuthDecision(allow=False, deny_reason="role too low")

    def handler(request: httpx.Request) -> httpx.Response:
        network_calls.append(str(request.url))
        return httpx.Response(200, json={})

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport, base_url="http://test")
    d = HttpDispatcher("http://test", client=client, auth_hook=hook)
    try:
        resp = await d.dispatch(Request(command="x"))
    finally:
        await d.aclose()
    assert not resp.ok
    assert resp.error.type == "AccessDenied"
    assert "role too low" in resp.error.message
    assert network_calls == []


@pytest.mark.asyncio
async def test_dispatch_auth_hook_raises_becomes_deny():
    """A raising hook becomes an ``AccessDenied`` response, not a crash."""

    def hook(ctx):
        raise RuntimeError("vault unreachable")

    transport = httpx.MockTransport(lambda r: httpx.Response(500))
    client = httpx.AsyncClient(transport=transport, base_url="http://test")
    d = HttpDispatcher("http://test", client=client, auth_hook=hook)
    try:
        resp = await d.dispatch(Request(command="x"))
    finally:
        await d.aclose()
    assert not resp.ok
    assert resp.error.type == "AccessDenied"
    assert "vault unreachable" in resp.error.message


@pytest.mark.asyncio
async def test_dispatch_auth_hook_wrong_return_type_denies():
    """A hook returning the wrong shape is treated as a deny, not silent ok."""

    def hook(ctx):
        return {"headers": {"x": "1"}}  # not an AuthDecision

    transport = httpx.MockTransport(lambda r: httpx.Response(200))
    client = httpx.AsyncClient(transport=transport, base_url="http://test")
    d = HttpDispatcher("http://test", client=client, auth_hook=hook)
    try:
        resp = await d.dispatch(Request(command="x"))
    finally:
        await d.aclose()
    assert not resp.ok
    assert resp.error.type == "AccessDenied"
    assert "expected AuthDecision" in resp.error.message


@pytest.mark.asyncio
async def test_list_commands_filters_out_denied_entries():
    """``__commands__`` runs the auth hook for each command; deny → drop."""
    from openbb_cli.auth import AuthDecision

    def hook(ctx):
        # Only allow ``allowed.cmd``
        if ctx.command == "allowed.cmd":
            return AuthDecision()
        return AuthDecision(allow=False, deny_reason="rbac")

    d = HttpDispatcher(
        "http://test",
        spec_doc={
            "commands": {
                "allowed.cmd": {"description": "Visible."},
                "denied.cmd": {"description": "Hidden."},
            }
        },
        auth_hook=hook,
    )
    resp = await d.dispatch(Request(command="__commands__"))
    assert resp.ok
    names = [r["name"] for r in resp.result]
    assert names == ["allowed.cmd"]


@pytest.mark.asyncio
async def test_describe_denied_returns_access_denied():
    """``__schema__`` for a denied command returns ``AccessDenied``."""
    from openbb_cli.auth import AuthDecision

    d = HttpDispatcher(
        "http://test",
        spec_doc={
            "commands": {
                "secret.thing": {
                    "description": "Hidden.",
                    "parameters": [],
                    "providers": [],
                    "request_body_schema": None,
                    "response_schema": None,
                }
            }
        },
        auth_hook=lambda ctx: AuthDecision(allow=False, deny_reason="rbac"),
    )
    resp = await d.dispatch(
        Request(command="__schema__", params={"name": "secret.thing"})
    )
    assert not resp.ok
    assert resp.error.type == "AccessDenied"
    assert "rbac" in resp.error.message


@pytest.mark.asyncio
async def test_describe_returns_slim_shape_for_non_multi_provider():
    """Single-provider commands get the flat ``{name, parameters, output_schema}``."""
    d = HttpDispatcher(
        "http://test",
        spec_doc={
            "commands": {
                "foo": {
                    "description": "F",
                    "parameters": [
                        {
                            "name": "x",
                            "in": "query",
                            "type": "string",
                            "is_list": False,
                            "required": True,
                            "default": None,
                            "choices": [],
                            "help": None,
                            "providers": [],
                        }
                    ],
                    "providers": [],
                    "request_body_schema": None,
                    "response_schema": {"type": "object"},
                }
            }
        },
    )
    resp = await d._describe_command(
        Request(command="__schema__", params={"name": "foo"})
    )
    assert resp.ok
    assert set(resp.result) == {"name", "parameters", "output_schema"}
    p = resp.result["parameters"][0]
    # Empty/falsy fields stripped
    assert "is_list" not in p
    assert "default" not in p
    assert "choices" not in p
    assert "help" not in p
    assert p["required"] is True


@pytest.mark.asyncio
async def test_describe_groups_by_provider_when_present():
    """Multi-provider commands return ``{name, providers: {ns: {...}, ...}}``."""
    d = HttpDispatcher(
        "http://test",
        spec_doc={
            "commands": {
                "q": {
                    "description": "Q",
                    "parameters": [
                        {
                            "name": "symbol",
                            "in": "query",
                            "type": "string",
                            "required": True,
                            "providers": [],
                        },
                        {
                            "name": "use_cache",
                            "in": "query",
                            "type": "boolean",
                            "providers": ["cboe"],
                        },
                        {
                            "name": "source",
                            "in": "query",
                            "type": "string",
                            "providers": ["intrinio"],
                        },
                    ],
                    "providers": ["cboe", "intrinio"],
                    "request_body_schema": None,
                    "response_schema": None,
                }
            }
        },
    )
    resp = await d._describe_command(
        Request(command="__schema__", params={"name": "q"})
    )
    assert resp.ok
    by = resp.result["providers"]
    assert set(by) == {"cboe", "intrinio"}
    cboe_params = [p["name"] for p in by["cboe"]["parameters"]]
    intrinio_params = [p["name"] for p in by["intrinio"]["parameters"]]
    assert cboe_params == ["symbol", "use_cache"]
    assert intrinio_params == ["symbol", "source"]


@pytest.mark.asyncio
async def test_describe_with_provider_suffix_returns_single_slice():
    """Passing ``provider`` narrows the output to that provider's slice."""
    d = HttpDispatcher(
        "http://test",
        spec_doc={
            "commands": {
                "q": {
                    "description": "Q",
                    "parameters": [
                        {
                            "name": "symbol",
                            "in": "query",
                            "type": "string",
                            "required": True,
                            "providers": [],
                        },
                        {
                            "name": "source",
                            "in": "query",
                            "type": "string",
                            "providers": ["intrinio"],
                        },
                    ],
                    "providers": ["cboe", "intrinio"],
                    "request_body_schema": None,
                    "response_schema": None,
                }
            }
        },
    )
    resp = await d._describe_command(
        Request(command="__schema__", params={"name": "q", "provider": "intrinio"})
    )
    assert resp.ok
    assert resp.result["provider"] == "intrinio"
    assert set(resp.result) == {"name", "provider", "parameters", "output_schema"}
    names = [p["name"] for p in resp.result["parameters"]]
    assert names == ["symbol", "source"]


@pytest.mark.asyncio
async def test_describe_unknown_provider_returns_error():
    d = HttpDispatcher(
        "http://test",
        spec_doc={
            "commands": {
                "q": {
                    "description": "Q",
                    "parameters": [],
                    "providers": ["cboe"],
                    "request_body_schema": None,
                    "response_schema": None,
                }
            }
        },
    )
    resp = await d._describe_command(
        Request(command="__schema__", params={"name": "q", "provider": "bogus"})
    )
    assert not resp.ok
    assert resp.error.type == "UnknownProvider"
    assert "Available: cboe" in resp.error.message


def test_help_for_provider_keeps_only_relevant_sections():
    """Per-provider help filtering: drops ``(provider: X)`` blocks for other providers."""
    from openbb_cli.dispatchers.http import _help_for_provider

    text = (
        "Shared header for everyone.;\n    "
        "FMP says hi. (provider: fmp);\n    "
        "Intrinio says hi. (provider: intrinio)"
    )
    assert _help_for_provider(text, "fmp") == (
        "Shared header for everyone.\nFMP says hi."
    )
    assert _help_for_provider(text, "intrinio") == (
        "Shared header for everyone.\nIntrinio says hi."
    )


def test_help_for_provider_returns_none_when_nothing_applies():
    from openbb_cli.dispatchers.http import _help_for_provider

    text = "Only for cboe. (provider: cboe)"
    assert _help_for_provider(text, "fmp") is None


def test_help_for_provider_passthrough_for_empty():
    from openbb_cli.dispatchers.http import _help_for_provider

    assert _help_for_provider(None, "fmp") is None
    assert _help_for_provider("", "fmp") == ""


def test_provider_output_schema_picks_matching_variant():
    """``IntrinioEquityQuoteData`` matches provider ``intrinio`` by case-insensitive prefix."""
    from openbb_cli.dispatchers.http import _provider_output_schema

    schema = {
        "properties": {
            "results": {
                "anyOf": [
                    {
                        "items": {
                            "oneOf": [
                                {"title": "IntrinioEquityQuoteData", "type": "object"},
                                {"title": "FMPEquityQuoteData", "type": "object"},
                            ]
                        },
                        "type": "array",
                    },
                    {"type": "null"},
                ]
            }
        }
    }
    intrinio = _provider_output_schema(schema, "intrinio")
    assert intrinio is not None
    assert intrinio["title"] == "IntrinioEquityQuoteData"


def test_provider_output_schema_handles_non_array_results():
    """Single-record results with a top-level ``oneOf`` work the same way."""
    from openbb_cli.dispatchers.http import _provider_output_schema

    schema = {
        "properties": {
            "results": {
                "oneOf": [
                    {"title": "IntrinioFoo", "type": "object"},
                    {"title": "FMPFoo", "type": "object"},
                ]
            }
        }
    }
    out = _provider_output_schema(schema, "fmp")
    assert out is not None
    assert out["title"] == "FMPFoo"


def test_provider_output_schema_returns_none_for_invalid_input():
    from openbb_cli.dispatchers.http import _provider_output_schema

    assert _provider_output_schema(None, "fmp") is None
    assert _provider_output_schema({}, "fmp") is None


def test_body_schema_to_params_flattens_object_properties():
    """Request-body fields surface as ``in: body`` parameter entries."""
    from openbb_cli.dispatchers.http import _body_schema_to_params

    body = {
        "type": "object",
        "required": ["data"],
        "properties": {
            "data": {
                "type": "array",
                "items": {"type": "object", "title": "Datum"},
                "title": "Data",
            },
            "x_columns": {
                "type": "array",
                "items": {"type": "string"},
                "title": "Columns",
            },
        },
    }
    params = _body_schema_to_params(body)
    by_name = {p["name"]: p for p in params}
    assert by_name["data"]["in"] == "body"
    assert by_name["data"]["is_list"] is True
    assert by_name["data"]["required"] is True
    assert "items" in by_name["data"]  # complex item schema preserved
    assert by_name["x_columns"]["is_list"] is True
    assert "items" not in by_name["x_columns"]  # primitive item, dropped


def test_body_schema_to_params_skips_non_object_or_empty():
    from openbb_cli.dispatchers.http import _body_schema_to_params

    assert _body_schema_to_params(None) == []
    assert _body_schema_to_params({}) == []
    assert _body_schema_to_params({"type": "string"}) == []


def test_http_dispatcher_from_server_passes_headers(monkeypatch):
    """``http_dispatcher_from_server`` forwards headers to both fetch and dispatcher."""
    from openbb_cli.dispatchers import http as http_mod

    captured: dict[str, dict[str, str] | None] = {}

    def fake_fetch(
        base_url, *, timeout=10.0, path=None, headers=None, query_params=None
    ):
        captured["headers"] = headers
        return {"paths": {"/api/v1/x": {"get": {}}}}

    monkeypatch.setattr(
        "openbb_cli.dispatchers.openapi_schema.fetch_openapi", fake_fetch
    )
    d = http_mod.http_dispatcher_from_server(
        "http://h", headers={"Authorization": "Bearer abc"}
    )
    assert captured["headers"] == {"Authorization": "Bearer abc"}
    assert d._headers == {"Authorization": "Bearer abc"}


@pytest.mark.asyncio
async def test_describe_missing_name_returns_error():
    """``__schema__`` without ``--name`` returns a structured error."""
    d = HttpDispatcher("http://test", spec_doc={"commands": {}})
    resp = await d._describe_command(Request(command="__schema__", params={}))
    assert not resp.ok
    assert resp.error.type == "MissingParameter"
    assert "--name" in resp.error.message


@pytest.mark.asyncio
async def test_describe_unknown_command_returns_error():
    """Asking for a command that isn't in the spec returns ``UnknownCommand``."""
    d = HttpDispatcher("http://test", spec_doc={"commands": {"a": {}}})
    resp = await d._describe_command(
        Request(command="__schema__", params={"name": "missing.cmd"})
    )
    assert not resp.ok
    assert resp.error.type == "UnknownCommand"
    assert "missing.cmd" in resp.error.message


# --- _slim_param branch coverage ---


def test_slim_param_drops_empty_choices_and_default_zero():
    """Falsy fields (empty choices, missing default) are stripped."""
    from openbb_cli.dispatchers.http import _slim_param

    out = _slim_param(
        {
            "name": "x",
            "in": "query",
            "type": "integer",
            "is_list": False,
            "required": False,
            "default": None,
            "choices": [],
            "help": None,
        }
    )
    assert out == {"name": "x", "in": "query", "type": "integer"}


def test_slim_param_keeps_default_zero():
    """``default=0`` is meaningful — must NOT be stripped (only None is)."""
    from openbb_cli.dispatchers.http import _slim_param

    out = _slim_param({"name": "x", "in": "query", "type": "integer", "default": 0})
    assert out["default"] == 0


def test_slim_param_keeps_choices_and_help_when_set():
    from openbb_cli.dispatchers.http import _slim_param

    out = _slim_param(
        {
            "name": "x",
            "in": "query",
            "type": "string",
            "choices": ["a", "b"],
            "help": "pick",
        }
    )
    assert out["choices"] == ["a", "b"]
    assert out["help"] == "pick"


# --- _help_for_provider edge: empty section ---


def test_help_for_provider_skips_empty_sections_between_separators():
    from openbb_cli.dispatchers.http import _help_for_provider

    text = "shared.;\n    ;\n    Foo. (provider: fmp)"
    out = _help_for_provider(text, "fmp")
    assert out == "shared.\nFoo."


# --- _provider_output_schema edges ---


def test_provider_output_schema_returns_none_when_results_field_absent():
    from openbb_cli.dispatchers.http import _provider_output_schema

    schema = {"properties": {}}
    assert _provider_output_schema(schema, "fmp") is None


def test_provider_output_schema_skips_unmatched_titles():
    """When no oneOf variant matches the provider, return None (not arbitrary first)."""
    from openbb_cli.dispatchers.http import _provider_output_schema

    schema = {
        "properties": {
            "results": {
                "anyOf": [
                    {
                        "items": {
                            "oneOf": [
                                {"title": "AlphaData", "type": "object"},
                                {"title": "BetaData", "type": "object"},
                            ]
                        },
                        "type": "array",
                    },
                    {"type": "null"},
                ]
            }
        }
    }
    assert _provider_output_schema(schema, "gamma") is None


def test_provider_output_schema_skips_non_dict_oneof_entries():
    from openbb_cli.dispatchers.http import _provider_output_schema

    schema = {
        "properties": {
            "results": {"oneOf": ["not-a-dict", {"title": "FMPx", "type": "object"}]}
        }
    }
    out = _provider_output_schema(schema, "fmp")
    assert out is not None
    assert out["title"] == "FMPx"


# --- _body_schema_to_params edges ---


def test_body_schema_to_params_skips_non_dict_property():
    """Defensive: a property whose schema isn't a dict gets skipped."""
    from openbb_cli.dispatchers.http import _body_schema_to_params

    body = {
        "type": "object",
        "properties": {"good": {"type": "string"}, "bad": "junk"},
    }
    out = _body_schema_to_params(body)
    assert [p["name"] for p in out] == ["good"]


def test_body_schema_to_params_handles_array_with_non_dict_items():
    """``items`` that isn't a dict falls back to type 'object'."""
    from openbb_cli.dispatchers.http import _body_schema_to_params

    body = {"type": "object", "properties": {"x": {"type": "array", "items": True}}}
    out = _body_schema_to_params(body)
    assert out[0]["type"] == "object"


def test_body_schema_to_params_records_default_and_help():
    from openbb_cli.dispatchers.http import _body_schema_to_params

    body = {
        "type": "object",
        "properties": {
            "limit": {"type": "integer", "default": 5, "description": "count"}
        },
    }
    out = _body_schema_to_params(body)
    assert out[0]["default"] == 5
    assert out[0]["help"] == "count"


# --- _decode_response branches ---


def test_decode_response_text_for_xml_content_type():
    """Non-JSON content type returns the raw text body."""
    from openbb_cli.dispatchers.http import _decode_response

    response = httpx.Response(
        200, text="<root>x</root>", headers={"content-type": "application/xml"}
    )
    assert _decode_response(response) == "<root>x</root>"


def test_decode_response_falls_back_to_text_when_json_invalid():
    """JSON content type with a body that won't parse falls back to text."""
    from openbb_cli.dispatchers.http import _decode_response

    response = httpx.Response(
        200, text="not json", headers={"content-type": "application/json"}
    )
    assert _decode_response(response) == "not json"


def test_decode_response_no_content_type_attempts_json():
    """Empty content type defaults to JSON parsing."""
    from openbb_cli.dispatchers.http import _decode_response

    response = httpx.Response(200, text='{"a": 1}', headers={"content-type": ""})
    assert _decode_response(response) == {"a": 1}


# --- _client_context external client branch ---


@pytest.mark.asyncio
async def test_client_context_yields_external_client_when_not_owned():
    """External clients are yielded as-is, not closed by the dispatcher."""
    transport = httpx.MockTransport(lambda r: httpx.Response(200, json={}))
    external = httpx.AsyncClient(transport=transport, base_url="http://t")
    d = HttpDispatcher("http://t", client=external)
    async with d._client_context() as c:
        assert c is external
    # Not closed
    response = await external.get("http://t/")
    assert response.status_code == 200
    await external.aclose()


def test_slim_param_keeps_is_list_when_true():
    """``is_list: True`` lands in the slim output."""
    from openbb_cli.dispatchers.http import _slim_param

    out = _slim_param({"name": "x", "in": "query", "type": "string", "is_list": True})
    assert out["is_list"] is True


def test_provider_output_schema_falls_back_to_only_candidate_with_no_title_match():
    """Single candidate that doesn't title-match the provider is returned anyway."""
    from openbb_cli.dispatchers.http import _provider_output_schema

    schema = {
        "properties": {
            "results": {
                "anyOf": [
                    {"oneOf": [{"title": "GenericData", "type": "object"}]},
                    {"type": "null"},
                ]
            }
        }
    }
    out = _provider_output_schema(schema, "fmp")
    assert out is not None
    assert out["title"] == "GenericData"


@pytest.mark.asyncio
async def test_dispatch_routes_list_commands_through_dispatch():
    """``dispatch(Request(command="__commands__"))`` reaches the introspection path."""
    d = HttpDispatcher(
        "http://test",
        spec_doc={"commands": {"foo": {"description": "F"}}},
    )
    resp = await d.dispatch(Request(command="__commands__"))
    assert resp.ok
    assert resp.result == [{"name": "foo", "description": "F"}]


@pytest.mark.asyncio
async def test_dispatch_routes_describe_through_dispatch():
    """``dispatch(Request(command="__schema__"))`` reaches ``_describe_command``."""
    d = HttpDispatcher(
        "http://test",
        spec_doc={
            "commands": {
                "foo": {
                    "description": "F",
                    "parameters": [],
                    "providers": [],
                    "request_body_schema": None,
                    "response_schema": None,
                }
            }
        },
    )
    resp = await d.dispatch(Request(command="__schema__", params={"name": "foo"}))
    assert resp.ok
    assert resp.result["name"] == "foo"


def test_group_by_provider_skips_provider_discriminator():
    """The ``provider`` flag itself is filtered out per-provider — already implicit."""
    from openbb_cli.dispatchers.http import _group_by_provider

    raw_params = [
        {"name": "provider", "type": "string", "providers": []},
        {"name": "symbol", "type": "string", "providers": []},
    ]
    grouped = _group_by_provider(raw_params, [], ["cboe"], None)
    names = [p["name"] for p in grouped["cboe"]["parameters"]]
    assert names == ["symbol"]  # ``provider`` itself dropped


@pytest.mark.asyncio
async def test_dispatch_with_owned_client_uses_fresh_async_client(monkeypatch):
    """An owned client (no ``client=`` arg) takes the ``owns`` branch in ``_client_context``,
    constructing a fresh ``httpx.AsyncClient`` per dispatch.
    """
    constructed: list[bool] = []
    transport = httpx.MockTransport(lambda r: httpx.Response(200, json={"ok": True}))
    real_async_client = httpx.AsyncClient

    def factory(*args, **kwargs):
        constructed.append(True)
        kwargs.pop("transport", None)
        return real_async_client(*args, transport=transport, **kwargs)

    from openbb_cli.dispatchers import http as http_mod

    monkeypatch.setattr(http_mod.httpx, "AsyncClient", factory)
    d = HttpDispatcher("http://t", command_methods={"foo": "get"})
    resp = await d.dispatch(Request(command="foo"))
    assert resp.ok
    assert constructed == [True]
