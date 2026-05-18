"""web_search tool source tests."""

from __future__ import annotations

from typing import Any

import pytest

from openbb_agent_server.plugins.tools import web_search
from openbb_agent_server.plugins.tools.web_search import WebSearchToolSource
from openbb_agent_server.runtime import emit
from openbb_agent_server.runtime.context import RunContext
from openbb_agent_server.runtime.principal import UserPrincipal


def _ctx(
    api_keys: dict[str, str] | None = None,
    *,
    search_enabled: bool = True,
) -> RunContext:
    return RunContext(
        principal=UserPrincipal(user_id="u"),
        trace_id="t",
        run_id="r",
        conversation_id="c",
        api_keys=api_keys or {},
        workspace_options=(
            frozenset({"search-web"}) if search_enabled else frozenset()
        ),
    )


@pytest.fixture
def captured_emits(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    monkeypatch.setattr(emit, "_writer", lambda: out.append)
    return out


@pytest.mark.asyncio
async def test_default_provider_is_duckduckgo() -> None:
    src = WebSearchToolSource()
    tools = await src.tools(_ctx(), {})
    assert len(tools) == 1
    assert tools[0].name == "web_search"


@pytest.mark.asyncio
async def test_web_search_filtered_when_user_has_not_opted_in() -> None:
    """Return no tools when the search-web feature is off."""
    src = WebSearchToolSource()
    tools = await src.tools(_ctx(search_enabled=False), {})
    assert tools == []


@pytest.mark.asyncio
async def test_search_emits_citations_for_results(
    monkeypatch: pytest.MonkeyPatch,
    captured_emits: list[dict[str, Any]],
) -> None:
    monkeypatch.setattr(
        web_search,
        "_ddg_search",
        lambda q, k: [
            {"title": "Page A", "url": "http://a", "snippet": "snippet a"},
            {"title": "Page B", "url": "http://b", "snippet": "snippet b"},
        ],
    )
    src = WebSearchToolSource()
    [tool] = await src.tools(_ctx(), {})
    results = tool.invoke({"query": "anything", "k": 2})
    assert len(results) == 2
    citations_emits = [e for e in captured_emits if e["type"] == "citations"]
    assert len(citations_emits) == 2


@pytest.mark.asyncio
async def test_tavily_provider_requires_api_key() -> None:
    src = WebSearchToolSource(provider="tavily")
    with pytest.raises(RuntimeError):
        await src.tools(_ctx(), {})


@pytest.mark.asyncio
async def test_provider_overridable_via_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    src = WebSearchToolSource(provider="tavily")
    monkeypatch.setattr(web_search, "_ddg_search", lambda q, k: [])
    tools = await src.tools(_ctx(api_keys={}), {"provider": "duckduckgo"})
    assert tools[0].name == "web_search"
