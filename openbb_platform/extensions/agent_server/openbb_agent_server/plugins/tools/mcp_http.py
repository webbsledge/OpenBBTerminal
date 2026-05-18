"""``mcp_http`` tool source — connect to a running MCP server over HTTP/SSE."""

from __future__ import annotations

import logging
import os
from typing import Any

from langchain_core.tools import BaseTool
from langchain_mcp_adapters.client import MultiServerMCPClient

from openbb_agent_server.runtime.context import RunContext
from openbb_agent_server.runtime.plugins import ToolSource

logger = logging.getLogger("openbb_agent_server.tools.mcp_http")


_CONFIG_FILE_ENVS: tuple[str, ...] = (
    "OPENBB_AGENT_MCP_CONFIG",
    "OPENBB_MCP_CONFIG",
    "OPENBB_AGENT_CONFIG",
    "OPENBB_API_CONFIG",
    "OPENBB_CONFIG",
)

_VALID_TRANSPORTS: frozenset[str] = frozenset({"streamable_http", "sse", "websocket"})

_TRANSPORT_ALIASES: dict[str, str] = {
    "streamable-http": "streamable_http",
    "streamable_http": "streamable_http",
    "sse": "sse",
    "websocket": "websocket",
    "ws": "websocket",
}


def _normalise_transport(value: str) -> str:
    out = _TRANSPORT_ALIASES.get(value, value)
    if out not in _VALID_TRANSPORTS:
        raise ValueError(
            f"unsupported transport {value!r}; expected one of "
            f"{sorted(_VALID_TRANSPORTS)} (or the dash form 'streamable-http')."
        )
    return out


def _resolve_config_file(explicit: str | None) -> str | None:
    if explicit:
        return explicit
    for var in _CONFIG_FILE_ENVS:
        v = os.environ.get(var)
        if v:
            return v
    return None


def _read_mcp_table(config_path: str | None) -> dict[str, Any]:
    """Re-walk the cascade and return the mcp table."""
    from openbb_agent_server.app.config import bootstrap_launcher_config

    try:
        cfg = bootstrap_launcher_config(explicit_path=config_path)
    except Exception:
        logger.debug("mcp_http: could not re-read TOML cascade", exc_info=True)
        return {}
    section = cfg.get("mcp")
    return dict(section) if isinstance(section, dict) else {}


def _build_url(host: str, port: int | str, transport: str) -> str:
    path = "/sse" if transport == "sse" else "/mcp"
    if host.startswith(("http://", "https://")):
        base = host.rstrip("/")
        host_part = base.split("//", 1)[1]
        if port and ":" not in host_part:
            base = f"{base}:{port}"
        return f"{base}{path}"
    return f"http://{host}:{port}{path}"


class HttpMcpToolSource(ToolSource):
    """Connect to a running MCP server over HTTP/SSE/WebSocket."""

    name = "mcp_http"

    def __init__(
        self,
        *,
        url: str | None = None,
        transport: str | None = None,
        headers: dict[str, str] | None = None,
        server_name: str = "openbb",
        config_file: str | None = None,
    ) -> None:
        if transport is not None:
            transport = _normalise_transport(transport)
        self._url = url
        self._transport = transport
        self._headers = dict(headers or {})
        self._server_name = server_name
        self._config_file = config_file

    async def tools(self, ctx: RunContext, config: dict[str, Any]) -> list[BaseTool]:
        config_file = _resolve_config_file(
            config.get("config_file") or self._config_file
        )
        mcp_section = _read_mcp_table(config_file)

        raw_transport = (
            config.get("transport")
            or self._transport
            or mcp_section.get("transport")
            or "streamable_http"
        )
        transport = _normalise_transport(str(raw_transport))

        url = config.get("url") or self._url
        if not url:
            host = mcp_section.get("host")
            port = mcp_section.get("port")
            if not host or not port:
                raise RuntimeError(
                    "mcp_http: no URL configured. Either set ``url`` "
                    "directly (constructor or [agent.tool_source_config.mcp_http].url) "
                    "or provide ``[mcp].host`` and ``[mcp].port`` in your openbb.toml."
                )
            url = _build_url(str(host), port, transport)

        headers: dict[str, str] = {**self._headers}
        spec = mcp_section.get("spec")
        if isinstance(spec, dict):
            spec_headers = spec.get("headers") or {}
            if isinstance(spec_headers, dict):
                for k, v in spec_headers.items():
                    headers.setdefault(str(k), str(v))
        for k, v in dict(config.get("headers", {})).items():
            headers[k] = v
        for k, v in ctx.api_keys.items():
            headers.setdefault(f"X-OPENBB-{k}", v)

        if mcp_section:
            logger.debug(
                "mcp_http: connecting to %s (transport=%s, [mcp] keys=%s)",
                url,
                transport,
                sorted(mcp_section.keys()),
            )
        else:
            logger.debug("mcp_http: connecting to %s (transport=%s)", url, transport)

        from typing import cast

        connections: dict[str, Any] = {
            self._server_name: {
                "transport": transport,
                "url": url,
                "headers": headers,
            }
        }
        client = MultiServerMCPClient(connections=cast(Any, connections))
        return await client.get_tools()
