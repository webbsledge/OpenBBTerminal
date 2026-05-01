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
    assert resp.result == {"echo": {"symbol": "AAPL"}}
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

    def fake_fetch(base_url, *, timeout=10.0, path=None, headers=None):
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
    """When no explicit client is supplied, the dispatcher builds one with the headers applied."""
    d = HttpDispatcher(
        "http://h",
        headers={"X-Foo": "bar"},
    )
    assert d._headers == {"X-Foo": "bar"}
    assert d._owns_client is True
    assert d._client.headers["x-foo"] == "bar"


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


def test_http_dispatcher_from_server_passes_headers(monkeypatch):
    """``http_dispatcher_from_server`` forwards headers to both fetch and dispatcher."""
    from openbb_cli.dispatchers import http as http_mod

    captured: dict[str, dict[str, str] | None] = {}

    def fake_fetch(base_url, *, timeout=10.0, path=None, headers=None):
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
