"""Targeted tests that fill the remaining coverage gaps."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import os
import time
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from fastapi import HTTPException, Request
from langchain_core.messages import AIMessage, HumanMessage

from openbb_agent_server.persistence.sqlite_store import SqliteHistoryStore
from openbb_agent_server.runtime.context import (
    FileRef,
    RunContext,
    bind,
    runtime_state,
)
from openbb_agent_server.runtime.principal import UserPrincipal


def _request(headers: dict[str, str]) -> Request:
    raw = [(k.lower().encode(), v.encode()) for k, v in headers.items()]
    return Request({"type": "http", "headers": raw})


@pytest_asyncio.fixture
async def api_key_backend(tmp_path: Path):
    from openbb_agent_server.plugins.auth.api_key_table import ApiKeyTableAuthBackend

    url = f"sqlite+aiosqlite:///{tmp_path / 'auth.db'}"
    history = SqliteHistoryStore(url)
    await history.init_schema()
    backend = ApiKeyTableAuthBackend(db_url=url)
    try:
        yield backend
    finally:
        await backend.aclose()
        await history.aclose()


@pytest.mark.asyncio
async def test_api_key_table_rejects_empty_fragments(api_key_backend) -> None:
    """Reject an api key with empty fragments."""
    with pytest.raises(HTTPException) as exc:
        await api_key_backend.authenticate(_request({"x-api-key": "oba_."}))
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_api_key_table_wrong_secret_returns_403(api_key_backend) -> None:
    issued = await api_key_backend.issue(user_id="alice")
    parts = issued.plaintext.split(".")
    forged = parts[0] + "." + "x" * 32
    with pytest.raises(HTTPException) as exc:
        await api_key_backend.authenticate(_request({"x-api-key": forged}))
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_api_key_table_issue_updates_existing_user_metadata(
    api_key_backend,
) -> None:
    """Fill empty user metadata on a second issue() call."""
    await api_key_backend.issue(user_id="bob")
    await api_key_backend.issue(
        user_id="bob", display_name="Bob", email="bob@example.com"
    )
    p = await api_key_backend.authenticate(
        _request({"x-api-key": (await api_key_backend.issue(user_id="bob")).plaintext})
    )
    assert p.display_name == "Bob"
    assert p.email == "bob@example.com"


def test_oidc_jwt_extract_scopes_handles_list_value() -> None:
    from openbb_agent_server.plugins.auth.oidc_jwt import OidcJwtAuthBackend

    assert OidcJwtAuthBackend._extract_scopes({"scopes": ["a", "b", "c"]}) == (
        "a",
        "b",
        "c",
    )
    assert OidcJwtAuthBackend._extract_scopes({"scope": ("x", "y")}) == ("x", "y")
    assert OidcJwtAuthBackend._extract_scopes({"scope": 42}) == ()


def test_tool_call_announcer_handles_object_shaped_tool_call() -> None:
    from openbb_agent_server.plugins.middleware.tool_call_announcer import (
        _from_tool_call,
        _tool_name,
    )

    class _ObjectToolCall:
        name = "transcribe_audio"

    request = type("R", (), {"tool_call": _ObjectToolCall()})()
    assert _from_tool_call(_ObjectToolCall(), "name") == "transcribe_audio"
    assert _tool_name(request) == "transcribe_audio"


def test_tool_call_ledger_safe_json_falls_back_for_unserializable() -> None:
    from openbb_agent_server.plugins.middleware.tool_call_ledger import _safe_json

    class _NotSerialisable:
        def __repr__(self) -> str:
            return "<obj>"

    out = _safe_json(_NotSerialisable())
    assert out == {"__str__": "<obj>"}


def test_memory_writer_split_drops_short_and_none_lines() -> None:
    from openbb_agent_server.memory.writer import _split

    assert _split("NONE") == []
    assert _split("") == []
    assert _split("- short\n- this is a real one") == ["this is a real one"]


def test_fake_provider_yields_string_messages_as_ai_chunks() -> None:
    """Yield string responses as AIMessageChunks."""
    from openbb_agent_server.plugins.models.fake_provider import FakeProvider

    provider = FakeProvider(responses=["plain string"])
    ctx = RunContext(
        principal=UserPrincipal(user_id="u"),
        trace_id="t",
        run_id="r",
        conversation_id="c",
    )
    model = provider.build(ctx, {})
    out = model.invoke("hello")
    assert "plain string" in out.content


def test_bucket_refill_is_noop_when_no_time_elapsed() -> None:
    from openbb_agent_server.plugins.models.groq_rate_limiter import _Bucket

    bucket = _Bucket.of(capacity=10, period_seconds=10)
    bucket.consume(5)
    snap_a = bucket.available
    bucket.last_refill = time.monotonic() + 1.0
    bucket.refill()
    assert bucket.available == snap_a


@pytest.mark.asyncio
async def test_aacquire_blocking_waits_for_refill() -> None:
    from openbb_agent_server.plugins.models.groq_rate_limiter import GroqRateLimiter

    limiter = GroqRateLimiter(rpm=60, rpd=None, tpm=None, tpd=None)
    for _ in range(60):
        assert await limiter.aacquire(blocking=False) is True
    t0 = time.monotonic()
    assert await limiter.aacquire(blocking=True) is True
    elapsed = time.monotonic() - t0
    assert 0.5 <= elapsed <= 2.5


def test_artifacts_decode_if_string_passes_through_non_string_inputs() -> None:
    from openbb_agent_server.plugins.tools.artifacts import _decode_if_string

    sentinel = {"already": "decoded"}
    assert _decode_if_string(sentinel) is sentinel
    assert _decode_if_string("not json") == "not json"


def test_mcp_http_read_mcp_table_swallows_bootstrap_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from openbb_agent_server.plugins.tools import mcp_http

    def raiser(*_a, **_k):
        raise RuntimeError("simulated cascade failure")

    monkeypatch.setattr(
        "openbb_agent_server.app.config.bootstrap_launcher_config", raiser
    )
    assert mcp_http._read_mcp_table("anywhere.toml") == {}


def test_mcp_local_resolve_command_via_which(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from openbb_agent_server.plugins.tools import mcp_local

    fake = tmp_path / "openbb-mcp"
    fake.write_text("#!/bin/sh\necho hi", encoding="utf-8")
    fake.chmod(0o755)
    monkeypatch.setattr(
        "openbb_agent_server.plugins.tools.mcp_local.shutil.which",
        lambda name: str(fake),
    )
    assert mcp_local._resolve_command("openbb-mcp") == str(fake)


def test_mcp_local_ensure_arg_skips_when_already_present() -> None:
    from openbb_agent_server.plugins.tools.mcp_local import _ensure_arg

    args = ["--config-file", "/x.toml"]
    assert _ensure_arg(args, "--config-file", "/y.toml") == args


def test_mcp_local_read_mcp_table_swallows_bootstrap_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from openbb_agent_server.plugins.tools import mcp_local

    def raiser(*_a, **_k):
        raise RuntimeError("simulated cascade failure")

    monkeypatch.setattr(
        "openbb_agent_server.app.config.bootstrap_launcher_config", raiser
    )
    assert mcp_local._read_mcp_table("anywhere.toml") == {}


@pytest.mark.asyncio
async def test_mcp_local_skips_args_extension_when_transport_already_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Skip args extension when transport is already set."""
    from openbb_agent_server.plugins.tools import mcp_local

    captured: dict[str, Any] = {}

    class _FakeMCP:
        def __init__(self, *, connections: dict[str, Any]) -> None:
            captured.update(connections)

        async def get_tools(self) -> list[Any]:
            return []

    monkeypatch.setattr(
        "openbb_agent_server.plugins.tools.mcp_local.MultiServerMCPClient",
        _FakeMCP,
    )
    monkeypatch.setattr(
        "openbb_agent_server.plugins.tools.mcp_local._resolve_command",
        lambda cmd: "/usr/bin/openbb-mcp",
    )

    src = mcp_local.LocalMcpToolSource(args=["--transport", "stdio", "--quiet"])
    ctx = RunContext(
        principal=UserPrincipal(user_id="u"),
        trace_id="t",
        run_id="r",
        conversation_id="c",
    )
    await src.tools(ctx, {})
    args = captured["openbb"]["args"]
    assert args.count("--transport") == 1


def test_memory_recall_bind_store_replaces_constructor_default() -> None:
    from openbb_agent_server.memory.sqlite_store import SqliteMemoryStore
    from openbb_agent_server.plugins.tools.memory_recall import MemoryRecallToolSource

    src = MemoryRecallToolSource()
    fake_store = SqliteMemoryStore.__new__(SqliteMemoryStore)
    src._bind_store(fake_store)
    assert src._store is fake_store


def test_python_module_flatten_keeps_bad_factory_intact() -> None:
    from openbb_agent_server.plugins.tools.python_module import _flatten

    def factory_with_required_arg(x):  # noqa: ANN001
        return x

    with pytest.raises(TypeError):
        _flatten(factory_with_required_arg)


def test_widget_data_stable_json_falls_back_to_str_for_unserialisable() -> None:
    from openbb_agent_server.plugins.tools.widget_data import _stable_json

    class _Weird:
        def __repr__(self) -> str:
            return "<weird>"

        def __str__(self) -> str:
            return "<weird-str>"

    a: dict[str, Any] = {}
    a["self"] = a
    out = _stable_json(a)
    assert isinstance(out, str)


def test_web_search_ddg_search_yields_results(monkeypatch: pytest.MonkeyPatch) -> None:
    from openbb_agent_server.plugins.tools import web_search

    class _StubDDGS:
        def __enter__(self) -> _StubDDGS:
            return self

        def __exit__(self, *exc: Any) -> None:
            return None

        def text(self, query: str, max_results: int) -> list[dict[str, str]]:
            return [
                {"title": "T1", "href": "https://x.test/1", "body": "snip"},
                {"title": "T2", "link": "https://x.test/2", "snippet": "snip2"},
            ][:max_results]

    monkeypatch.setattr("ddgs.DDGS", _StubDDGS)
    out = web_search._ddg_search("apple", k=2)
    assert out == [
        {"title": "T1", "url": "https://x.test/1", "snippet": "snip"},
        {"title": "T2", "url": "https://x.test/2", "snippet": "snip2"},
    ]


@pytest.mark.asyncio
async def test_web_search_tool_runs_against_stubbed_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from openbb_agent_server.plugins.tools import web_search
    from openbb_agent_server.plugins.tools.web_search import WebSearchToolSource
    from openbb_agent_server.runtime.context import RunContext as _RC

    monkeypatch.setattr(
        web_search,
        "_ddg_search",
        lambda q, k: [{"title": "T", "url": "u", "snippet": "s"}],
    )
    ctx = _RC(
        principal=UserPrincipal(user_id="u"),
        trace_id="t",
        run_id="r",
        conversation_id="c",
        workspace_options=frozenset({"search-web"}),
    )
    src = WebSearchToolSource()
    [tool] = await src.tools(ctx, {})

    with bind(ctx):
        runtime_state()  # ensure scratch dict exists for emit
        out = await tool.ainvoke({"query": "apple", "k": 1})
    assert out == [{"title": "T", "url": "u", "snippet": "s"}]


def test_web_search_tavily_requires_key() -> None:
    from openbb_agent_server.plugins.tools.web_search import _build_search_callable

    with pytest.raises(RuntimeError, match="tavily"):
        _build_search_callable("tavily", {})


def test_pdf_extract_resolve_pdf_bytes_fetches_url() -> None:
    from openbb_agent_server.plugins.tools.pdf_extract import _resolve_pdf_bytes

    class _FakeResp:
        content = b"PDF-BYTES"

        def raise_for_status(self) -> None:
            return None

    class _FakeClient:
        def get(self, url: str, headers: dict | None = None) -> _FakeResp:
            return _FakeResp()

    ref = FileRef(name="x.pdf", url="https://x.test/x.pdf")
    assert _resolve_pdf_bytes(ref, _FakeClient()) == b"PDF-BYTES"


def test_pdf_extract_resolve_pdf_bytes_raises_when_no_source() -> None:
    from openbb_agent_server.plugins.tools.pdf_extract import _resolve_pdf_bytes

    ref = FileRef(name="x.pdf")
    with pytest.raises(RuntimeError, match="has no url or data_base64"):
        _resolve_pdf_bytes(ref, http_client=None)


def test_runtime_state_raises_when_no_run_bound() -> None:
    with pytest.raises(LookupError, match="runtime state"):
        runtime_state()


def test_runtime_builder_tool_messages_round_trip_as_toolmessage() -> None:
    """Round-trip role:tool messages as ToolMessage."""
    from langchain_core.messages import ToolMessage

    from openbb_agent_server.protocol.schemas import ChatMessage
    from openbb_agent_server.runtime.builder import _to_lc_messages

    out = _to_lc_messages(
        [
            ChatMessage(role="human", content="hi"),
            ChatMessage(role="ai", content="ok"),
            ChatMessage(role="tool", content="result", tool_call_id="abc"),
        ]
    )
    assert isinstance(out[0], HumanMessage)
    assert isinstance(out[1], AIMessage)
    assert isinstance(out[-1], ToolMessage)
    assert out[-1].tool_call_id == "abc"


def test_protocol_adapter_passes_citations_through() -> None:
    """Buffer citations through _translate and flush them as one."""
    from openbb_agent_server.protocol.adapter import DeepAgentEventAdapter
    from openbb_agent_server.protocol.schemas import CitationCollectionSSE

    adapter = DeepAgentEventAdapter(client_tool_names=set())
    inline = adapter._translate(
        {
            "type": "custom",
            "data": {
                "type": "citations",
                "citations": [
                    {
                        "id": "c-1",
                        "source_info": {
                            "type": "web",
                            "name": "T",
                            "origin": "https://x.example",
                        },
                        "details": [{"text": "snippet"}],
                    }
                ],
            },
        }
    )
    assert inline == []
    [cc] = adapter._drain_citations()
    assert isinstance(cc, CitationCollectionSSE)
    assert cc.data.citations[0].id == "c-1"
    assert cc.data.citations[0].source_info.origin == "https://x.example"


def test_main_propagates_model_provider_and_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_run(*args: Any, **kwargs: Any) -> None:
        captured["called"] = True
        captured.update(kwargs)

    monkeypatch.setattr("uvicorn.run", fake_run)
    from openbb_agent_server.main import main

    main(
        [
            "--host",
            "127.0.0.1",
            "--model-provider",
            "fake",
            "--model-name",
            "fake-model",
            "--port",
            "0",
        ]
    )
    assert os.environ.get("OPENBB_AGENT_MODEL_PROVIDER") == "fake"
    assert os.environ.get("OPENBB_AGENT_MODEL_NAME") == "fake-model"


def test_main_keys_list_prints_no_keys_message_when_empty(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("OPENBB_AGENT_AUTH_BACKEND", "api_key_table")
    monkeypatch.setenv(
        "OPENBB_AGENT_AUTH_CONFIG",
        f'{{"db_url": "sqlite+aiosqlite:///{tmp_path / "auth.db"}"}}',
    )
    monkeypatch.setenv("OPENBB_AGENT_DATA_DIR", str(tmp_path))

    async def _init() -> None:
        store = SqliteHistoryStore(f"sqlite+aiosqlite:///{tmp_path / 'auth.db'}")
        await store.init_schema()
        await store.aclose()

    asyncio.run(_init())

    from openbb_agent_server.main import main

    main(["keys", "list"])
    out = capsys.readouterr().out
    assert "(no keys)" in out


def test_create_app_resolves_settings_from_toml_when_omitted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from openbb_agent_server.app.app import create_app

    monkeypatch.delenv("OPENBB_AGENT_BOOTSTRAP_TOML", raising=False)
    monkeypatch.setenv("OPENBB_AGENT_DATA_DIR", str(tmp_path))
    app = create_app()
    assert app is not None


def test_settings_resolve_profile_inherits_metadata_when_overlay_omits_it() -> None:
    from openbb_agent_server.app.settings import AgentServerSettings

    settings = AgentServerSettings(
        profiles={"alt": {"system_prompt_file": "/etc/openbb/alt.md"}}
    )
    profile = settings.resolve_profile("alt")
    assert profile.metadata.name == settings.metadata.name


def test_postgres_checkpointer_constructor_stores_url() -> None:
    from openbb_agent_server.plugins.checkpointers.postgres import (
        PostgresCheckpointerProvider,
    )

    provider = PostgresCheckpointerProvider(url="postgresql://h/db", extra="ignored")
    assert provider._explicit_url == "postgresql://h/db"
    assert provider._cm is None


def test_postgres_resolve_url_normalises_sqlalchemy_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from openbb_agent_server.plugins.checkpointers.postgres import (
        PostgresCheckpointerProvider,
    )

    class _Settings:
        def resolved_db_url(self) -> str:
            return "postgresql+psycopg://u:p@h/db"

    monkeypatch.delenv("OPENBB_AGENT_CHECKPOINTER_URL", raising=False)
    provider = PostgresCheckpointerProvider()
    assert provider._resolve_url(_Settings()) == "postgresql://u:p@h/db"


def test_postgres_resolve_url_uses_env_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from openbb_agent_server.plugins.checkpointers.postgres import (
        PostgresCheckpointerProvider,
    )

    class _Settings:
        def resolved_db_url(self) -> str:
            return "sqlite:///nope.db"

    monkeypatch.setenv("OPENBB_AGENT_CHECKPOINTER_URL", "postgresql+asyncpg://u@h/db")
    provider = PostgresCheckpointerProvider()
    assert provider._resolve_url(_Settings()) == "postgresql://u@h/db"


def test_postgres_resolve_url_rejects_non_postgres(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from openbb_agent_server.plugins.checkpointers.postgres import (
        PostgresCheckpointerProvider,
    )

    class _Settings:
        def resolved_db_url(self) -> str:
            return "sqlite:///nope.db"

    monkeypatch.delenv("OPENBB_AGENT_CHECKPOINTER_URL", raising=False)
    provider = PostgresCheckpointerProvider()
    with pytest.raises(RuntimeError, match="postgresql://"):
        provider._resolve_url(_Settings())


@pytest.mark.asyncio
async def test_mcp_http_per_call_config_headers_merge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from openbb_agent_server.plugins.tools import mcp_http

    captured: dict[str, Any] = {}

    class _FakeMCP:
        def __init__(self, *, connections: dict[str, Any]) -> None:
            captured.update(connections)

        async def get_tools(self) -> list[Any]:
            return []

    monkeypatch.setattr(
        "openbb_agent_server.plugins.tools.mcp_http.MultiServerMCPClient",
        _FakeMCP,
    )

    src = mcp_http.HttpMcpToolSource(url="http://x/mcp")
    ctx = RunContext(
        principal=UserPrincipal(user_id="u"),
        trace_id="t",
        run_id="r",
        conversation_id="c",
    )
    await src.tools(ctx, {"headers": {"X-Tenant": "abc"}})
    assert captured["openbb"]["headers"]["X-Tenant"] == "abc"


@pytest.mark.asyncio
async def test_mcp_local_appends_transport_when_caller_omits_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from openbb_agent_server.plugins.tools import mcp_local

    captured: dict[str, Any] = {}

    class _FakeMCP:
        def __init__(self, *, connections: dict[str, Any]) -> None:
            captured.update(connections)

        async def get_tools(self) -> list[Any]:
            return []

    monkeypatch.setattr(
        "openbb_agent_server.plugins.tools.mcp_local.MultiServerMCPClient",
        _FakeMCP,
    )
    monkeypatch.setattr(
        "openbb_agent_server.plugins.tools.mcp_local._resolve_command",
        lambda cmd: "/usr/bin/openbb-mcp",
    )

    src = mcp_local.LocalMcpToolSource(args=["--quiet"])
    ctx = RunContext(
        principal=UserPrincipal(user_id="u"),
        trace_id="t",
        run_id="r",
        conversation_id="c",
    )
    await src.tools(ctx, {})
    assert captured["openbb"]["args"][-2:] == ["--transport", "stdio"]


@pytest.mark.asyncio
async def test_pdf_extract_page_range_filters(monkeypatch: pytest.MonkeyPatch) -> None:
    from openbb_agent_server.plugins.tools.pdf_extract import PdfExtractToolSource

    class _Page:
        def __init__(self, idx: int) -> None:
            self._idx = idx

        def extract_text(self) -> str:
            return f"page-{self._idx}"

        def extract_words(self) -> list[dict[str, Any]]:
            return []

    class _Pdf:
        pages = [_Page(1), _Page(2), _Page(3)]

        def __enter__(self) -> _Pdf:
            return self

        def __exit__(self, *exc: Any) -> None:
            return None

    class _Plumber:
        def open(self, _stream: Any) -> _Pdf:
            return _Pdf()

    monkeypatch.setitem(__import__("sys").modules, "pdfplumber", _Plumber())

    pdf_b64 = base64.b64encode(b"%PDF-fake").decode()
    ctx = RunContext(
        principal=UserPrincipal(user_id="u"),
        trace_id="t",
        run_id="r",
        conversation_id="c",
        uploaded_files=(
            FileRef(name="f.pdf", mime="application/pdf", data_base64=pdf_b64),
        ),
    )

    src = PdfExtractToolSource()
    tools = await src.tools(ctx, {})
    extract = next(t for t in tools if t.name == "pdf_extract")
    with bind(ctx):
        out = await extract.ainvoke({"name": "f.pdf", "page_range": [2, 2]})
    pages = out["pages"]
    assert len(pages) == 1 and pages[0]["page"] == 2 and pages[0]["text"] == "page-2"


def test_web_search_ddg_falls_back_to_duckduckgo_search(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fall through to duckduckgo_search when ddgs is unimportable."""
    import sys

    from openbb_agent_server.plugins.tools import web_search

    sentinel_results: list[dict[str, str]] = [{"title": "T", "href": "u", "body": "s"}]

    class _StubDDGS:
        def __enter__(self) -> _StubDDGS:
            return self

        def __exit__(self, *exc: Any) -> None:
            return None

        def text(self, query: str, max_results: int) -> list[dict[str, str]]:
            return sentinel_results

    fake_module = type("M", (), {"DDGS": _StubDDGS})
    monkeypatch.setitem(sys.modules, "ddgs", None)
    monkeypatch.setitem(sys.modules, "duckduckgo_search", fake_module)

    out = web_search._ddg_search("apple", k=1)
    assert out == [{"title": "T", "url": "u", "snippet": "s"}]


@pytest.mark.asyncio
async def test_fake_provider_streams_string_chunk_via_astream() -> None:
    from openbb_agent_server.plugins.models.fake_provider import FakeProvider

    provider = FakeProvider(responses=["streamed"])
    model = provider.build(
        RunContext(
            principal=UserPrincipal(user_id="u"),
            trace_id="t",
            run_id="r",
            conversation_id="c",
        ),
        {},
    )
    chunks = []
    async for chunk in model.astream("hi"):
        chunks.append(chunk.content)
    assert "".join(chunks) == "streamed"


@pytest.mark.asyncio
async def test_usage_aggregate_filters_by_conversation_id(tmp_path: Path) -> None:
    """Filter aggregate usage by conversation_id."""
    from openbb_agent_server.persistence.sqlite_store import SqliteHistoryStore
    from openbb_agent_server.persistence.store import UsageRecord

    url = f"sqlite+aiosqlite:///{tmp_path / 'h.db'}"
    store = SqliteHistoryStore(url)
    await store.init_schema()
    principal = UserPrincipal(user_id="u")
    convo_id = "convo-1"
    trace_id = "trace-1"
    await store.begin_trace(
        principal=principal,
        trace_id=trace_id,
        conversation_id=convo_id,
        run_id="r",
    )
    await store.record_usage(
        principal=principal,
        trace_id=trace_id,
        usage=UsageRecord(
            trace_id=trace_id,
            user_id="u",
            model="x",
            input_tokens=10,
            output_tokens=5,
            cache_read=0,
            cache_creation=0,
            cost_usd=0.0,
        ),
    )

    out = await store.usage_summary(principal=principal, conversation_id=convo_id)
    assert out["by_model"]
    by = {row["model"]: row for row in out["by_model"]}
    assert by["x"]["input_tokens"] == 10
    assert by["x"]["output_tokens"] == 5
    await store.aclose()


def test_runtime_builder_resolves_middleware_from_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from openbb_agent_server.app.settings import AgentMetadata, AgentProfile
    from openbb_agent_server.runtime import builder

    sentinel_mw = object()

    class _FakeMW:
        def build(self, ctx: Any, config: dict[str, Any]) -> Any:
            assert "model" in config
            return sentinel_mw

    monkeypatch.setattr(
        "openbb_agent_server.runtime.builder.registry.load",
        lambda group, name: _FakeMW(),
    )

    profile = AgentProfile(
        name="p",
        model_provider="fake",
        model_name="x",
        model_config={},
        tool_sources=(),
        subagents=(),
        middleware=("call_limit",),
        skills=(),
        features={},
        system_prompt_file=None,
        metadata=AgentMetadata(),
        tool_source_config={},
    )
    ctx = RunContext(
        principal=UserPrincipal(user_id="u"),
        trace_id="t",
        run_id="r",
        conversation_id="c",
    )
    out = builder._resolve_middleware(ctx, profile, model=object())
    assert out == [sentinel_mw]


def test_runtime_builder_subagent_carries_model_kwarg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Forward a non-None spec.model into the subagent dict."""
    from openbb_agent_server.app.settings import (
        AgentMetadata,
        AgentProfile,
    )
    from openbb_agent_server.runtime import builder

    sentinel_model = object()

    class _Spec:
        name = "researcher"
        description = "d"
        system_prompt = "s"
        tools: tuple[str, ...] = ()
        model = sentinel_model

    monkeypatch.setattr(
        "openbb_agent_server.runtime.builder.registry.load",
        lambda _group, _name: _Spec(),
    )

    profile = AgentProfile(
        name="p",
        model_provider="fake",
        model_name="x",
        model_config={},
        tool_sources=(),
        subagents=("researcher",),
        middleware=(),
        skills=(),
        features={},
        system_prompt_file=None,
        metadata=AgentMetadata(),
        tool_source_config={},
    )
    out = builder._resolve_subagents(profile, main_tools=[])
    assert out and out[0]["model"] is sentinel_model


def test_settings_resolve_profile_metadata_overlay_partial() -> None:
    """Use base metadata for fields a partial overlay omits."""
    from openbb_agent_server.app.settings import AgentServerSettings

    settings = AgentServerSettings(profiles={"alt": {"metadata": {"name": "Alt-Name"}}})
    profile = settings.resolve_profile("alt")
    assert profile.metadata.name == "Alt-Name"
    assert profile.metadata.description == settings.metadata.description


def test_snowflake_credentials_layer_picks_every_supported_env_var() -> None:
    from openbb_agent_server.plugins.tools.snowflake_tools.source import (
        _credentials_from_ctx,
    )

    ctx = RunContext(
        principal=UserPrincipal(user_id="u"),
        trace_id="t",
        run_id="r",
        conversation_id="c",
        api_keys={
            "SNOWFLAKE_ACCOUNT": "acct",
            "SNOWFLAKE_USER": "user",
            "SNOWFLAKE_PASSWORD": "pw",
            "SNOWFLAKE_PRIVATE_KEY": "pk",
            "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE": "pkpass",
            "SNOWFLAKE_AUTHENTICATOR": "oauth",
            "SNOWFLAKE_TOKEN": "tok",
            "SNOWFLAKE_ROLE": "role",
            "SNOWFLAKE_WAREHOUSE": "wh",
            "SNOWFLAKE_DATABASE": "db",
            "SNOWFLAKE_SCHEMA": "sch",
            "SNOWFLAKE_HOST": "h.example",
            "SNOWFLAKE_REGION": "us-east-1",
        },
    )
    creds = _credentials_from_ctx(ctx, base=None)
    assert creds.account == "acct"
    assert creds.user == "user"
    assert creds.password == "pw"
    assert creds.private_key == "pk"
    assert creds.private_key_passphrase == "pkpass"
    assert creds.authenticator == "oauth"
    assert creds.token == "tok"
    assert creds.role == "role"
    assert creds.warehouse == "wh"
    assert creds.database == "db"
    assert creds.schema_ == "sch"
    assert creds.host == "h.example"
    assert creds.region == "us-east-1"


def test_snowflake_tool_surface_invokes_every_function(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Drive each snowflake tool through a stub client."""
    from openbb_agent_server.plugins.tools.snowflake_tools import (
        SnowflakeToolSource,
        cortex as cortex_mod,
    )
    from openbb_agent_server.plugins.tools.snowflake_tools.client import (
        SnowflakeCredentials,
    )

    class _Result:
        def __init__(self) -> None:
            self.columns = ("a",)
            self.rows = [(1,)]
            self.row_count = 1
            self.truncated = False
            self.query_id = "qid"
            self.elapsed_ms = 1
            self.statement_kind = "SELECT"

    class _StubClient:
        max_rows = 10

        def execute(
            self, sql: str, params: Any = None, max_rows: int | None = None
        ) -> _Result:
            return _Result()

    cortex_calls: list[str] = []

    def _record(name: str):
        def fn(*_a: Any, **_k: Any) -> Any:
            cortex_calls.append(name)
            if name == "cortex_sentiment":
                return 0.5
            if name == "cortex_embed":
                return [0.1, 0.2]
            if name == "cortex_classify_text":
                return {"label": "LABEL", "score": 0.9}
            if name == "cortex_extract_answer":
                return {"answer": "x"}
            if name == "cortex_search":
                return {"results": [{"chunk": "c", "title": "T", "url": "u"}]}
            if name == "cortex_analyst":
                return {"messages": []}
            return "stub"

        return fn

    for fn_name in (
        "cortex_complete",
        "cortex_summarize",
        "cortex_sentiment",
        "cortex_translate",
        "cortex_classify_text",
        "cortex_extract_answer",
        "cortex_embed",
        "cortex_search",
        "cortex_analyst",
    ):
        monkeypatch.setattr(cortex_mod, fn_name, _record(fn_name))

    creds = SnowflakeCredentials(account="a", user="u")
    tools = SnowflakeToolSource.build_tools(_StubClient(), creds, max_rows=10)
    by_name = {t.name: t for t in tools}

    by_name["snowflake_list_databases"].invoke({})
    by_name["snowflake_list_schemas"].invoke({"database": "DB"})
    by_name["snowflake_list_tables"].invoke({"database": "DB", "schema": "S"})
    by_name["snowflake_describe"].invoke({"object_path": "DB.S.T"})
    by_name["snowflake_get_table_info"].invoke({"table": "DB.S.T"})
    by_name["snowflake_get_table_sample_data"].invoke({"table": "DB.S.T"})
    by_name["snowflake_get_multiple_table_definitions"].invoke({"tables": ["DB.S.T"]})
    by_name["snowflake_search_catalog"].invoke({"pattern": "%cust%"})
    by_name["snowflake_explain"].invoke({"sql": "SELECT 1"})
    by_name["snowflake_query_history"].invoke({})
    by_name["snowflake_cortex_complete"].invoke({"prompt": "hi"})
    by_name["snowflake_cortex_summarize"].invoke({"text": "the quick brown"})
    by_name["snowflake_cortex_sentiment"].invoke({"text": "happy"})
    by_name["snowflake_cortex_translate"].invoke(
        {"text": "hi", "target_language": "fr"}
    )
    by_name["snowflake_cortex_classify"].invoke({"text": "x", "categories": ["a", "b"]})
    by_name["snowflake_cortex_extract_answer"].invoke({"question": "q", "context": "c"})
    by_name["snowflake_cortex_embed"].invoke({"text": "x"})
    by_name["snowflake_cortex_search"].invoke(
        {"database": "DB", "schema": "S", "service": "SVC", "query": "q"}
    )
    by_name["snowflake_cortex_analyst"].invoke({"messages": [{"role": "user"}]})

    assert {
        "cortex_complete",
        "cortex_summarize",
        "cortex_sentiment",
        "cortex_translate",
        "cortex_classify_text",
        "cortex_extract_answer",
        "cortex_embed",
        "cortex_search",
        "cortex_analyst",
    } <= set(cortex_calls)


def test_snowflake_get_table_info_rejects_short_path() -> None:
    """Reject a short table path in snowflake_get_table_info."""
    from openbb_agent_server.plugins.tools.snowflake_tools import SnowflakeToolSource
    from openbb_agent_server.plugins.tools.snowflake_tools.client import (
        SnowflakeCredentials,
    )

    class _StubClient:
        max_rows = 10

        def execute(self, *_a: Any, **_k: Any) -> Any:
            raise AssertionError("execute should not be reached")

    creds = SnowflakeCredentials(account="a", user="u")
    tools = SnowflakeToolSource.build_tools(_StubClient(), creds, max_rows=10)
    info = next(t for t in tools if t.name == "snowflake_get_table_info")
    with pytest.raises(Exception, match="DB.SCHEMA.TABLE"):
        info.invoke({"table": "OnlyOnePart"})


def _qresult_factory(*, rows_per_call: list[list[tuple[Any, ...]]] | None = None):
    """Fake Snowflake connection that hands out cursors over canned rows."""

    class _Cursor:
        sfqid = "stub-qid"

        def __init__(self, rows: list[tuple[Any, ...]]) -> None:
            self._rows = rows
            self._executed: list[Any] = []

        def execute(self, sql: str, params: Any = None) -> None:
            self._executed.append((sql, params))

        @property
        def description(self) -> list[tuple[str, ...]]:
            return [("col0",)]

        def fetchmany(self, n: int) -> list[tuple[Any, ...]]:
            out = self._rows[:n]
            self._rows = self._rows[n:]
            return out

        def fetchall(self) -> list[tuple[Any, ...]]:
            out = self._rows
            self._rows = []
            return out

        def close(self) -> None:  # noqa: D401
            pass

    class _Conn:
        def __init__(self, batches: list[list[tuple[Any, ...]]]) -> None:
            self._batches = batches

        def cursor(self) -> _Cursor:
            rows = self._batches.pop(0) if self._batches else []
            return _Cursor(rows)

        def close(self) -> None:
            pass

    batches = list(rows_per_call or [[]])
    return _Conn(batches)


def test_snowflake_client_context_manager_open_close() -> None:
    from openbb_agent_server.plugins.tools.snowflake_tools.client import (
        SnowflakeClient,
        SnowflakeCredentials,
    )

    creds = SnowflakeCredentials(account="a", user="u")
    factory_calls = {"open": 0, "close": 0}

    class _Conn:
        def cursor(self) -> Any:
            class _C:
                def execute(self_inner, *_a: Any, **_k: Any) -> None:
                    pass

                def close(self_inner) -> None:
                    pass

            return _C()

        def close(self) -> None:
            factory_calls["close"] += 1

    def _factory(_creds: Any) -> Any:
        factory_calls["open"] += 1
        return _Conn()

    client = SnowflakeClient(creds, connection_factory=_factory)
    with client as bound:
        assert bound is client
        assert factory_calls["open"] == 1
    assert factory_calls["close"] == 1


def test_snowflake_client_truncates_when_rows_exceed_cap() -> None:
    from openbb_agent_server.plugins.tools.snowflake_tools.client import (
        SnowflakeClient,
        SnowflakeCredentials,
    )

    rows = [(i,) for i in range(10)]

    class _Cursor:
        sfqid = "qid"
        description = [("v",)]

        def __init__(self) -> None:
            self._rows: list[tuple[Any, ...]] | None = rows

        def execute(self, *_a: Any, **_k: Any) -> None:
            pass

        def fetchmany(self, n: int) -> list[tuple[Any, ...]]:
            assert self._rows is not None
            return self._rows[:n]

        def fetchall(self) -> list[tuple[Any, ...]]:  # pragma: no cover
            assert self._rows is not None
            return list(self._rows)

        def close(self) -> None:
            pass

    class _Conn:
        def cursor(self) -> _Cursor:
            return _Cursor()

        def close(self) -> None:
            pass

    creds = SnowflakeCredentials(account="a", user="u")
    client = SnowflakeClient(creds, connection_factory=lambda _c: _Conn(), max_rows=3)
    result = client.execute("SELECT v FROM x")
    assert result.row_count == 3
    assert result.truncated is True


def test_snowflake_client_reconnects_when_session_expired_on_cursor_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reconnect when the session expires on cursor open."""
    from openbb_agent_server.plugins.tools.snowflake_tools import client as client_mod
    from openbb_agent_server.plugins.tools.snowflake_tools.client import (
        SnowflakeClient,
        SnowflakeCredentials,
    )

    monkeypatch.setattr(client_mod, "_is_session_expired", lambda _exc: True)

    class _Cursor:
        sfqid = "q"
        description = [("v",)]

        def execute(self, *_a: Any, **_k: Any) -> None:
            pass

        def fetchmany(self, n: int) -> list[tuple[Any, ...]]:
            return [(1,)]

        def fetchall(self) -> list[tuple[Any, ...]]:
            return [(1,)]

        def close(self) -> None:
            pass

    class _Conn:
        def __init__(self, expire_on_first: bool) -> None:
            self._expire_on_first = expire_on_first
            self._cursor_calls = 0

        def cursor(self) -> _Cursor:
            self._cursor_calls += 1
            if self._cursor_calls == 1 and self._expire_on_first:
                raise RuntimeError("session expired")
            return _Cursor()

        def close(self) -> None:
            pass

    class _ConnInitFirst(_Conn):
        def __init__(self) -> None:
            super().__init__(expire_on_first=False)
            self._init_done = False

        def cursor(self) -> _Cursor:
            self._cursor_calls += 1
            if not self._init_done:
                self._init_done = True
                return _Cursor()
            raise RuntimeError("session expired")

    seq = iter([_ConnInitFirst(), _Conn(expire_on_first=False)])

    def _factory(_creds: Any) -> Any:
        return next(seq)

    creds = SnowflakeCredentials(account="a", user="u")
    client = SnowflakeClient(creds, connection_factory=_factory, max_rows=10)
    result = client.execute("SELECT 1")
    assert result.row_count == 1


def test_snowflake_client_reconnects_when_session_expired_on_execute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reconnect when the session expires on execute."""
    from openbb_agent_server.plugins.tools.snowflake_tools import client as client_mod
    from openbb_agent_server.plugins.tools.snowflake_tools.client import (
        SnowflakeClient,
        SnowflakeCredentials,
    )

    monkeypatch.setattr(client_mod, "_is_session_expired", lambda _exc: True)

    class _Cursor:
        sfqid = "q"
        description = [("v",)]

        def __init__(self, fail: bool) -> None:
            self._fail = fail

        def execute(self, *_a: Any, **_k: Any) -> None:
            if self._fail:
                raise RuntimeError("session expired during execute")

        def fetchmany(self, n: int) -> list[tuple[Any, ...]]:
            return [(1,)]

        def fetchall(self) -> list[tuple[Any, ...]]:
            return [(1,)]

        def close(self) -> None:
            pass

    class _Conn:
        def __init__(self, cursors: list[_Cursor]) -> None:
            self._cursors = cursors

        def cursor(self) -> _Cursor:
            return self._cursors.pop(0)

        def close(self) -> None:
            pass

    conn1 = _Conn([_Cursor(fail=False), _Cursor(fail=True)])
    conn2 = _Conn([_Cursor(fail=False), _Cursor(fail=False)])
    factory = iter([conn1, conn2])

    creds = SnowflakeCredentials(account="a", user="u")
    client = SnowflakeClient(
        creds, connection_factory=lambda _c: next(factory), max_rows=10
    )
    result = client.execute("SELECT 1", params={"x": 1})
    assert result.row_count == 1


def test_snowflake_credentials_load_private_key_from_file(tmp_path: Path) -> None:
    """Load a private key from a file path."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    from openbb_agent_server.plugins.tools.snowflake_tools.client import (
        SnowflakeCredentials,
    )

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    pem_path = tmp_path / "key.pem"
    pem_path.write_bytes(pem)

    creds = SnowflakeCredentials(account="a", user="u", private_key=str(pem_path))
    der = creds._loaded_private_key()
    assert isinstance(der, bytes)


def test_cortex_scalar_raises_when_no_rows() -> None:
    from openbb_agent_server.plugins.tools.snowflake_tools import cortex as cortex_mod
    from openbb_agent_server.plugins.tools.snowflake_tools.client import QueryResult

    empty = QueryResult(
        sql="SELECT",
        statement_kind="SELECT",
        columns=[],
        rows=[],
        row_count=0,
        truncated=False,
        query_id="q",
        elapsed_ms=0,
    )
    with pytest.raises(RuntimeError, match="no rows"):
        cortex_mod._scalar(empty)


def test_cortex_parse_json_passes_dict_and_list_through() -> None:
    from openbb_agent_server.plugins.tools.snowflake_tools.cortex import _parse_json

    assert _parse_json({"a": 1}) == {"a": 1}
    assert _parse_json([1, 2]) == [1, 2]
    assert _parse_json("not-json") == "not-json"
    assert _parse_json(42) == 42


def test_cortex_search_forwards_optional_columns_and_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from openbb_agent_server.plugins.tools.snowflake_tools import cortex as cortex_mod
    from openbb_agent_server.plugins.tools.snowflake_tools.client import (
        SnowflakeCredentials,
    )

    captured: dict[str, Any] = {}

    class _Resp:
        def raise_for_status(self) -> None:
            pass

        def json(self) -> dict[str, Any]:
            return {"results": []}

    class _Client:
        def post(
            self, url: str, headers: dict[str, str], json: dict[str, Any]
        ) -> _Resp:
            captured["body"] = json
            return _Resp()

        def close(self) -> None:
            captured["closed"] = True

    monkeypatch.setattr(cortex_mod.httpx, "Client", lambda **_kw: _Client())
    cortex_mod.cortex_search(
        SnowflakeCredentials(account="a", user="u", token="tok", authenticator="oauth"),
        database="DB",
        schema="S",
        service="SVC",
        query="q",
        columns=["c1", "c2"],
        filter_={"status": "ok"},
    )
    assert captured["body"]["columns"] == ["c1", "c2"]
    assert captured["body"]["filter"] == {"status": "ok"}
    assert captured.get("closed") is True


def test_cortex_analyst_supports_semantic_view(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from openbb_agent_server.plugins.tools.snowflake_tools import cortex as cortex_mod
    from openbb_agent_server.plugins.tools.snowflake_tools.client import (
        SnowflakeCredentials,
    )

    captured: dict[str, Any] = {}

    class _Resp:
        def raise_for_status(self) -> None:
            pass

        def json(self) -> dict[str, Any]:
            return {}

    class _Client:
        def post(
            self, url: str, headers: dict[str, str], json: dict[str, Any]
        ) -> _Resp:
            captured["body"] = json
            return _Resp()

        def close(self) -> None:
            captured["closed"] = True

    monkeypatch.setattr(cortex_mod.httpx, "Client", lambda **_kw: _Client())
    cortex_mod.cortex_analyst(
        SnowflakeCredentials(account="a", user="u", token="tok", authenticator="oauth"),
        messages=[{"role": "user"}],
        semantic_view="DB.S.MY_VIEW",
    )
    assert captured["body"]["semantic_view"] == "DB.S.MY_VIEW"
    assert captured.get("closed") is True


def test_safety_classify_returns_empty_for_blank_sql() -> None:
    from openbb_agent_server.plugins.tools.snowflake_tools.safety import classify

    assert classify("   ") == "EMPTY"


def test_safety_is_read_only_accepts_show_describe_etc() -> None:
    from openbb_agent_server.plugins.tools.snowflake_tools.safety import is_read_only

    assert is_read_only("SHOW DATABASES")
    assert is_read_only("DESCRIBE DB.S.T")
    assert is_read_only("EXPLAIN SELECT 1")
    assert is_read_only("USE DB.S")


def test_safety_is_read_only_rejects_with_returning() -> None:
    """Reject a statement with no read-only base type."""
    from openbb_agent_server.plugins.tools.snowflake_tools.safety import is_read_only

    assert is_read_only("COPY INTO @stage FROM tbl") is False


def test_safety_is_read_only_rejects_unsafe_command() -> None:
    """Reject a Command-shaped statement outside the allow-list."""
    from openbb_agent_server.plugins.tools.snowflake_tools.safety import is_read_only

    assert is_read_only("GRANT ROLE analyst TO USER bob") is False


def test_safety_is_read_only_rejects_neither_mutating_nor_read_only() -> None:
    """Reject a statement in neither the mutating nor read-only type set."""
    from openbb_agent_server.plugins.tools.snowflake_tools.safety import is_read_only

    assert is_read_only("SET QUERY_TAG = 'x'") is False


def test_snowflake_client_propagates_non_session_error_on_cursor_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Propagate a non-session error raised on cursor open."""
    from openbb_agent_server.plugins.tools.snowflake_tools import client as client_mod
    from openbb_agent_server.plugins.tools.snowflake_tools.client import (
        SnowflakeClient,
        SnowflakeCredentials,
    )

    monkeypatch.setattr(client_mod, "_is_session_expired", lambda _exc: False)

    class _Cursor:
        def execute(self, *_a: Any, **_k: Any) -> None:
            pass

        def close(self) -> None:
            pass

    class _Conn:
        def __init__(self) -> None:
            self._calls = 0

        def cursor(self) -> _Cursor:
            self._calls += 1
            if self._calls > 1:
                raise RuntimeError("hard failure")
            return _Cursor()

        def close(self) -> None:
            pass

    creds = SnowflakeCredentials(account="a", user="u")
    client = SnowflakeClient(creds, connection_factory=lambda _c: _Conn())
    with pytest.raises(RuntimeError, match="hard failure"):
        client.execute("SELECT 1")


def test_snowflake_client_retries_execute_with_no_params(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retry execute when params is None."""
    from openbb_agent_server.plugins.tools.snowflake_tools import client as client_mod
    from openbb_agent_server.plugins.tools.snowflake_tools.client import (
        SnowflakeClient,
        SnowflakeCredentials,
    )

    monkeypatch.setattr(client_mod, "_is_session_expired", lambda _exc: True)

    class _Cursor:
        sfqid = "q"
        description = [("v",)]

        def __init__(self, fail: bool) -> None:
            self._fail = fail

        def execute(self, *_a: Any, **_k: Any) -> None:
            if self._fail:
                raise RuntimeError("session expired")

        def fetchmany(self, n: int) -> list[tuple[Any, ...]]:
            return [(1,)]

        def fetchall(self) -> list[tuple[Any, ...]]:
            return [(1,)]

        def close(self) -> None:
            pass

    class _Conn:
        def __init__(self, cursors: list[_Cursor]) -> None:
            self._cursors = cursors

        def cursor(self) -> _Cursor:
            return self._cursors.pop(0)

        def close(self) -> None:
            pass

    conn1 = _Conn([_Cursor(fail=False), _Cursor(fail=True)])
    conn2 = _Conn([_Cursor(fail=False), _Cursor(fail=False)])
    factory = iter([conn1, conn2])

    creds = SnowflakeCredentials(account="a", user="u")
    client = SnowflakeClient(
        creds, connection_factory=lambda _c: next(factory), max_rows=10
    )
    result = client.execute("SELECT 1")
    assert result.row_count == 1


def test_snowflake_get_multiple_table_definitions_collects_per_table_errors() -> None:
    """Collect per-table errors in snowflake_get_multiple_table_definitions."""
    from openbb_agent_server.plugins.tools.snowflake_tools import SnowflakeToolSource
    from openbb_agent_server.plugins.tools.snowflake_tools.client import (
        SnowflakeCredentials,
    )

    class _Boom:
        max_rows = 10

        def execute(self, *_a: Any, **_k: Any) -> Any:
            raise RuntimeError("boom")

    creds = SnowflakeCredentials(account="a", user="u")
    tools = SnowflakeToolSource.build_tools(_Boom(), creds, max_rows=10)
    multi = next(
        t for t in tools if t.name == "snowflake_get_multiple_table_definitions"
    )
    out = multi.invoke({"tables": ["DB.S.T"]})
    assert out["tables"]["DB.S.T"]["error"]


def test_settings_resolve_profile_overlay_with_no_metadata_at_all() -> None:
    """Reuse base metadata when the overlay omits it entirely."""
    from openbb_agent_server.app.settings import AgentServerSettings

    settings = AgentServerSettings(profiles={"alt": {}})
    profile = settings.resolve_profile("alt")
    assert profile.metadata.name == settings.metadata.name
    assert profile.metadata.description == settings.metadata.description


def test_settings_resolve_profile_overlay_with_non_dict_metadata_falls_back() -> None:
    """Reuse base metadata when the overlay metadata is not a dict."""
    from openbb_agent_server.app.settings import AgentServerSettings

    settings = AgentServerSettings(profiles={"alt": {"metadata": "not-a-dict"}})
    profile = settings.resolve_profile("alt")
    assert profile.metadata is settings.metadata


def test_settings_resolve_profile_flattens_nested_model_table() -> None:
    """Flatten a nested profile model table."""
    from openbb_agent_server.app.settings import AgentServerSettings

    settings = AgentServerSettings(
        model_provider="anthropic",
        model_name="claude-opus-4-7",
        profiles={
            "alt": {
                "model": {
                    "provider": "groq",
                    "name": "moonshotai/kimi-k2-instruct",
                    "config": {"temperature": 0.3, "max_tokens": 2048},
                },
            },
        },
    )
    profile = settings.resolve_profile("alt")
    assert profile.model_provider == "groq"
    assert profile.model_name == "moonshotai/kimi-k2-instruct"
    assert profile.model_config_ == {"temperature": 0.3, "max_tokens": 2048}


def test_settings_resolve_profile_flat_model_keys_still_win_over_nested() -> None:
    """Let flat model keys win over the nested model table."""
    from openbb_agent_server.app.settings import AgentServerSettings

    settings = AgentServerSettings(
        profiles={
            "alt": {
                "model_provider": "groq",
                "model_name": "qwen/qwen3-32b",
                "model": {"provider": "anthropic", "name": "claude-opus-4-7"},
            },
        },
    )
    profile = settings.resolve_profile("alt")
    assert profile.model_provider == "groq"
    assert profile.model_name == "qwen/qwen3-32b"


def test_settings_resolve_profile_partial_model_overlay_inherits_other_fields() -> None:
    """Inherit base model_name when the overlay sets only provider."""
    from openbb_agent_server.app.settings import AgentServerSettings

    settings = AgentServerSettings(
        model_provider="anthropic",
        model_name="claude-opus-4-7",
        profiles={"alt": {"model": {"provider": "groq"}}},
    )
    profile = settings.resolve_profile("alt")
    assert profile.model_provider == "groq"
    assert profile.model_name == "claude-opus-4-7"


def test_router_cancel_matches_user_id(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Match the cancel event by user id."""
    import asyncio as _asyncio

    from fastapi.testclient import TestClient

    from openbb_agent_server.app import router as router_mod
    from openbb_agent_server.app.app import create_app
    from openbb_agent_server.app.settings import AgentServerSettings
    from openbb_agent_server.runtime import services

    services.reset()
    monkeypatch.setenv("OPENBB_AGENT_AUTH_BACKEND", "none")
    monkeypatch.setenv(
        "OPENBB_AGENT_DB_URL", f"sqlite+aiosqlite:///{tmp_path / 'h.db'}"
    )
    monkeypatch.setenv("OPENBB_AGENT_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("OPENBB_AGENT_MODEL_PROVIDER", "fake")
    monkeypatch.setenv("OPENBB_AGENT_FAKE_RESPONSES", '["x"]')
    monkeypatch.setenv("OPENBB_AGENT_TOOL_SOURCES", "[]")
    monkeypatch.setenv("OPENBB_AGENT_SUBAGENTS", "[]")
    monkeypatch.setenv("OPENBB_AGENT_MIDDLEWARE", "[]")

    settings = AgentServerSettings()
    app = create_app(settings)

    ev = _asyncio.Event()
    router_mod._cancellations[("anonymous", "preset-run")] = ev

    with TestClient(app) as client:
        resp = client.post("/v1/conversations/cv-1/cancel")
    assert resp.status_code == 202
    assert ev.is_set()
    router_mod._cancellations.clear()


def test_router_streams_error_status_when_run_raises(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Stream an error status when run_agent raises."""
    from fastapi.testclient import TestClient

    from openbb_agent_server.app.app import create_app
    from openbb_agent_server.app.settings import AgentServerSettings
    from openbb_agent_server.runtime import services

    services.reset()
    monkeypatch.setenv("OPENBB_AGENT_AUTH_BACKEND", "none")
    monkeypatch.setenv(
        "OPENBB_AGENT_DB_URL", f"sqlite+aiosqlite:///{tmp_path / 'h.db'}"
    )
    monkeypatch.setenv("OPENBB_AGENT_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("OPENBB_AGENT_MODEL_PROVIDER", "fake")
    monkeypatch.setenv("OPENBB_AGENT_FAKE_RESPONSES", '["x"]')
    monkeypatch.setenv("OPENBB_AGENT_TOOL_SOURCES", "[]")
    monkeypatch.setenv("OPENBB_AGENT_SUBAGENTS", "[]")
    monkeypatch.setenv("OPENBB_AGENT_MIDDLEWARE", "[]")

    async def _boom(*args: Any, **kwargs: Any):
        raise RuntimeError("simulated agent failure")
        yield  # noqa: F811, ASYNC131  — required so this stays an async generator

    monkeypatch.setattr("openbb_agent_server.app.router.run_agent", _boom)

    settings = AgentServerSettings()
    app = create_app(settings)
    with TestClient(app) as client:
        resp = client.post(
            "/v1/query",
            json={
                "messages": [{"role": "human", "content": "hi"}],
                "conversation_id": "cv-err",
            },
        )
    body = resp.text
    assert "simulated agent failure" in body
    assert "ERROR" in body


def test_router_cancel_during_stream_short_circuits(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Short-circuit the stream when a cancel event fires mid-stream."""
    from fastapi.testclient import TestClient

    from openbb_agent_server.app import router as router_mod
    from openbb_agent_server.app.app import create_app
    from openbb_agent_server.app.settings import AgentServerSettings
    from openbb_agent_server.protocol.schemas import StatusUpdateSSE
    from openbb_agent_server.runtime import services

    services.reset()
    monkeypatch.setenv("OPENBB_AGENT_AUTH_BACKEND", "none")
    monkeypatch.setenv(
        "OPENBB_AGENT_DB_URL", f"sqlite+aiosqlite:///{tmp_path / 'h.db'}"
    )
    monkeypatch.setenv("OPENBB_AGENT_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("OPENBB_AGENT_MODEL_PROVIDER", "fake")
    monkeypatch.setenv("OPENBB_AGENT_FAKE_RESPONSES", '["x"]')
    monkeypatch.setenv("OPENBB_AGENT_TOOL_SOURCES", "[]")
    monkeypatch.setenv("OPENBB_AGENT_SUBAGENTS", "[]")
    monkeypatch.setenv("OPENBB_AGENT_MIDDLEWARE", "[]")

    async def _slow_stream(*args: Any, **kwargs: Any):
        yield StatusUpdateSSE(event_type="INFO", message="step", details={})
        for ev in list(router_mod._cancellations.values()):
            ev.set()
        yield StatusUpdateSSE(event_type="INFO", message="should-not-emit", details={})

    monkeypatch.setattr("openbb_agent_server.app.router.run_agent", _slow_stream)

    settings = AgentServerSettings()
    app = create_app(settings)
    with TestClient(app) as client:
        resp = client.post(
            "/v1/query",
            json={
                "messages": [{"role": "human", "content": "hi"}],
                "conversation_id": "cv-cancel",
            },
        )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_runtime_builder_run_agent_resolves_profile_when_omitted(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Resolve the profile when run_agent is given none."""
    import sys
    import types

    from openbb_agent_server.app.settings import AgentServerSettings
    from openbb_agent_server.protocol.schemas import QueryRequest

    fake_module = types.ModuleType("deepagents")

    class _FakeAgent:
        async def astream(self, *a: Any, **kw: Any):
            if False:  # pragma: no cover — make this an async generator
                yield None

    fake_module.create_deep_agent = lambda **_kw: _FakeAgent()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "deepagents", fake_module)

    monkeypatch.setenv("OPENBB_AGENT_AUTH_BACKEND", "none")
    monkeypatch.setenv("OPENBB_AGENT_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("OPENBB_AGENT_MODEL_PROVIDER", "fake")
    monkeypatch.setenv("OPENBB_AGENT_FAKE_RESPONSES", '["x"]')
    monkeypatch.setenv("OPENBB_AGENT_TOOL_SOURCES", "[]")
    monkeypatch.setenv("OPENBB_AGENT_SUBAGENTS", "[]")
    monkeypatch.setenv("OPENBB_AGENT_MIDDLEWARE", "[]")

    from langgraph.checkpoint.memory import InMemorySaver

    from openbb_agent_server.runtime import services as _services_mod

    _services_mod.set_services(checkpointer=InMemorySaver())
    settings = AgentServerSettings()

    real_resolve = AgentServerSettings.resolve_profile
    seen: list[str | None] = []

    def _patched(self: Any, name: str | None = None):
        seen.append(name)
        return real_resolve(self, name)

    monkeypatch.setattr(AgentServerSettings, "resolve_profile", _patched)

    from openbb_agent_server.runtime.builder import run_agent

    ctx = RunContext(
        principal=UserPrincipal(user_id="u"),
        trace_id="t",
        run_id="r",
        conversation_id="c",
        agent_name="default",
    )
    body = QueryRequest(
        messages=[{"role": "human", "content": "hi"}],
        conversation_id="c",
        run_id="r",
    )

    gen = run_agent(ctx=ctx, body=body, settings=settings, profile=None)
    with contextlib.suppress(StopAsyncIteration, Exception):
        async for _ev in gen:
            pass
    assert "default" in seen


def test_runtime_builder_passes_skills_when_set(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Pass profile skills through to the agent kwargs."""
    import sys
    import types

    captured_kwargs: dict[str, Any] = {}

    class _FakeAgent:
        async def astream(self, *a: Any, **kw: Any):
            if False:  # pragma: no cover — make this an async generator
                yield None

    def _capture(**kwargs: Any) -> _FakeAgent:
        captured_kwargs.update(kwargs)
        return _FakeAgent()

    fake_module = types.ModuleType("deepagents")
    fake_module.create_deep_agent = _capture  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "deepagents", fake_module)

    monkeypatch.setenv("OPENBB_AGENT_AUTH_BACKEND", "none")
    monkeypatch.setenv("OPENBB_AGENT_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("OPENBB_AGENT_MODEL_PROVIDER", "fake")
    monkeypatch.setenv("OPENBB_AGENT_FAKE_RESPONSES", '["x"]')
    monkeypatch.setenv("OPENBB_AGENT_TOOL_SOURCES", "[]")
    monkeypatch.setenv("OPENBB_AGENT_SUBAGENTS", "[]")
    monkeypatch.setenv("OPENBB_AGENT_MIDDLEWARE", "[]")
    monkeypatch.setenv("OPENBB_AGENT_SKILLS", '["/skills/finance"]')

    from langgraph.checkpoint.memory import InMemorySaver

    from openbb_agent_server.app.settings import AgentServerSettings
    from openbb_agent_server.protocol.schemas import QueryRequest
    from openbb_agent_server.runtime import services
    from openbb_agent_server.runtime.builder import run_agent

    services.set_services(checkpointer=InMemorySaver())

    settings = AgentServerSettings()
    profile = settings.resolve_profile()

    ctx = RunContext(
        principal=UserPrincipal(user_id="u"),
        trace_id="t",
        run_id="r",
        conversation_id="c",
        agent_name="default",
    )
    body = QueryRequest(
        messages=[{"role": "human", "content": "hi"}],
        conversation_id="c",
        run_id="r",
    )

    seen_events: list[Any] = []

    captured_error: list[BaseException] = []

    async def _drive() -> None:
        gen = run_agent(ctx=ctx, body=body, settings=settings, profile=profile)
        try:
            async for ev in gen:
                seen_events.append(ev)
        except BaseException as exc:  # noqa: BLE001
            captured_error.append(exc)

    asyncio.run(_drive())
    if "skills" not in captured_kwargs:
        raise AssertionError(
            f"create_deep_agent never received skills kwarg. "
            f"captured_kwargs={captured_kwargs!r}, "
            f"seen_events={[(type(e).__name__, getattr(e, 'message', None)) for e in seen_events]}, "
            f"errors={captured_error!r}"
        )
    assert captured_kwargs["skills"] == ["/skills/finance"]


def test_fake_provider_string_message_yields_chunk_via_stream() -> None:
    """Yield a string message as a chunk via stream."""
    from openbb_agent_server.plugins.models.fake_provider import (
        _ToolAwareFakeChatModel,
    )

    model = _ToolAwareFakeChatModel(messages=iter(["raw-string"]))
    chunks = list(model.stream("hi"))
    assert any("raw-string" in str(c.content) for c in chunks)


def test_web_search_tavily_returns_callable_when_key_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Build the tavily callable when a key is present."""
    from openbb_agent_server.plugins.tools.web_search import _build_search_callable

    callable_ = _build_search_callable("tavily", {"TAVILY_API_KEY": "stub"})
    assert callable(callable_)


def test_system_prompt_never_contains_user_identity(tmp_path: Path) -> None:
    """Keep user identity out of the system prompt."""
    from openbb_agent_server.app.settings import AgentMetadata, AgentProfile
    from openbb_agent_server.runtime.builder import (
        _build_system_prompt,
        _load_system_prompt,
    )

    sensitive_email = "alice@confidential.example.com"
    ctx = RunContext(
        principal=UserPrincipal(
            user_id=sensitive_email,
            display_name="Alice Confidential",
            email=sensitive_email,
        ),
        trace_id="trace-123",
        run_id="r",
        conversation_id="c",
    )
    out = _build_system_prompt(ctx)
    assert sensitive_email not in out
    assert "Alice Confidential" not in out
    assert "trace-123" not in out

    profile = AgentProfile(
        name="default",
        metadata=AgentMetadata(),
        model_provider="fake",
        model_name="x",
        model_config={},
        tool_sources=(),
        subagents=(),
        middleware=(),
        skills=(),
        features={},
        system_prompt_file=None,
        tool_source_config={},
    )
    out = _load_system_prompt(ctx, profile)
    assert sensitive_email not in out
    assert "Alice Confidential" not in out
    assert "trace-123" not in out

    custom = tmp_path / "custom.md"
    custom.write_text("Custom prompt {user_id} {display_name} {trace_id}.")
    profile_custom = profile.model_copy(update={"system_prompt_file": str(custom)})
    out = _load_system_prompt(ctx, profile_custom)
    assert sensitive_email not in out
    assert "Alice Confidential" not in out
    assert "trace-123" not in out


def test_router_coerce_feature_handles_dict_and_other_shapes() -> None:
    """Coerce features from bools and dict shapes."""
    from openbb_agent_server.app.router import _coerce_feature

    assert _coerce_feature(True) is True
    assert _coerce_feature(False) is False
    assert _coerce_feature({"default": True}) is True
    assert _coerce_feature({"default": False}) is False
    assert _coerce_feature({}) is False
    assert _coerce_feature(1) is True
    assert _coerce_feature(0) is False
    assert _coerce_feature(None) is False


def test_router_agents_json_emits_image_when_set(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Include the image in the agents.json entry when image_url is set."""
    from fastapi.testclient import TestClient

    from openbb_agent_server.app.app import create_app
    from openbb_agent_server.app.settings import AgentServerSettings
    from openbb_agent_server.runtime import services

    services.reset()
    monkeypatch.setenv("OPENBB_AGENT_AUTH_BACKEND", "none")
    monkeypatch.setenv("OPENBB_AGENT_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("OPENBB_AGENT_MODEL_PROVIDER", "fake")
    monkeypatch.setenv("OPENBB_AGENT_FAKE_RESPONSES", '["x"]')
    monkeypatch.setenv("OPENBB_AGENT_TOOL_SOURCES", "[]")
    monkeypatch.setenv("OPENBB_AGENT_SUBAGENTS", "[]")
    monkeypatch.setenv("OPENBB_AGENT_MIDDLEWARE", "[]")
    monkeypatch.setenv("OPENBB_AGENT_META_IMAGE_URL", "https://x.test/logo.png")

    with TestClient(create_app(AgentServerSettings())) as client:
        body = client.get("/agents.json").json()
    assert body["default"]["image"] == "https://x.test/logo.png"


def test_settings_resolve_profile_handles_non_dict_model_overlay() -> None:
    """Fall back to base when a profile model overlay is not a dict."""
    from openbb_agent_server.app.settings import AgentServerSettings

    settings = AgentServerSettings(
        model_provider="anthropic",
        model_name="claude-opus-4-7",
        profiles={"alt": {"model": "not-a-dict"}},
    )
    profile = settings.resolve_profile("alt")
    assert profile.model_provider == "anthropic"
    assert profile.model_name == "claude-opus-4-7"


def test_vertex_provider_forwards_credentials_kwarg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("langchain_google_genai")
    captured: dict[str, Any] = {}

    class _Recorder:
        def __init__(self, **kw: Any) -> None:
            captured.update(kw)

    monkeypatch.setattr("langchain_google_genai.ChatGoogleGenerativeAI", _Recorder)

    from openbb_agent_server.plugins.models.vertex_provider import VertexProvider

    sentinel = object()
    VertexProvider(project="p", credentials=sentinel).build(
        RunContext(
            principal=UserPrincipal(user_id="u"),
            trace_id="t",
            run_id="r",
            conversation_id="c",
        ),
        {},
    )
    assert captured.get("credentials") is sentinel


def test_router_agents_json_drops_profiles_with_invalid_agent_id(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Drop profiles whose agent id violates the Workspace regex."""
    from fastapi.testclient import TestClient

    from openbb_agent_server.app.app import create_app
    from openbb_agent_server.app.settings import AgentServerSettings
    from openbb_agent_server.runtime import services

    services.reset()
    monkeypatch.setenv("OPENBB_AGENT_AUTH_BACKEND", "none")
    monkeypatch.setenv(
        "OPENBB_AGENT_DB_URL", f"sqlite+aiosqlite:///{tmp_path / 'h.db'}"
    )
    monkeypatch.setenv("OPENBB_AGENT_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("OPENBB_AGENT_MODEL_PROVIDER", "fake")
    monkeypatch.setenv("OPENBB_AGENT_TOOL_SOURCES", "[]")
    monkeypatch.setenv("OPENBB_AGENT_SUBAGENTS", "[]")
    monkeypatch.setenv("OPENBB_AGENT_MIDDLEWARE", "[]")
    monkeypatch.setenv("OPENBB_AGENT_FAKE_RESPONSES", '["x"]')
    monkeypatch.setenv(
        "OPENBB_AGENT_PROFILES",
        json.dumps({"Bad_ID": {}, "fine-id": {}}),
    )

    app = create_app(AgentServerSettings())
    with TestClient(app) as client:
        body = client.get("/agents.json").json()
    assert "default" in body
    assert "fine-id" in body
    assert "Bad_ID" not in body


def test_router_agents_json_skips_unresolvable_profile(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Skip a profile that resolve_profile cannot resolve."""
    from fastapi.testclient import TestClient

    from openbb_agent_server.app.app import create_app
    from openbb_agent_server.app.settings import AgentServerSettings
    from openbb_agent_server.runtime import services

    services.reset()
    monkeypatch.setenv("OPENBB_AGENT_AUTH_BACKEND", "none")
    monkeypatch.setenv(
        "OPENBB_AGENT_DB_URL", f"sqlite+aiosqlite:///{tmp_path / 'h.db'}"
    )
    monkeypatch.setenv("OPENBB_AGENT_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("OPENBB_AGENT_MODEL_PROVIDER", "fake")
    monkeypatch.setenv("OPENBB_AGENT_TOOL_SOURCES", "[]")
    monkeypatch.setenv("OPENBB_AGENT_SUBAGENTS", "[]")
    monkeypatch.setenv("OPENBB_AGENT_MIDDLEWARE", "[]")
    monkeypatch.setenv("OPENBB_AGENT_PROFILES", json.dumps({"alt": {}}))
    monkeypatch.setenv("OPENBB_AGENT_FAKE_RESPONSES", '["x"]')

    settings = AgentServerSettings()
    app = create_app(settings)

    real_resolve = AgentServerSettings.resolve_profile

    def _resolve(self: Any, name: str | None = None):
        if name == "alt":
            raise KeyError("simulated")
        return real_resolve(self, name)

    monkeypatch.setattr(AgentServerSettings, "resolve_profile", _resolve)
    with TestClient(app) as client:
        body = client.get("/agents.json").json()
    assert "default" in body
    assert "alt" not in body
