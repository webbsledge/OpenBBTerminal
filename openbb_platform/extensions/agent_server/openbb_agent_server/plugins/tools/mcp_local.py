"""``mcp_local`` tool source — spawn the OpenBB MCP server and surface its tools."""

from __future__ import annotations

import logging
import os
import shutil
import sys
from pathlib import Path
from typing import Any

from langchain_core.tools import BaseTool
from langchain_mcp_adapters.client import MultiServerMCPClient

from openbb_agent_server.runtime.context import RunContext
from openbb_agent_server.runtime.plugins import ToolSource

logger = logging.getLogger("openbb_agent_server.tools.mcp_local")


_CONFIG_FILE_ENVS: tuple[str, ...] = (
    "OPENBB_AGENT_MCP_CONFIG",
    "OPENBB_MCP_CONFIG",
    "OPENBB_AGENT_CONFIG",
    "OPENBB_API_CONFIG",
    "OPENBB_CONFIG",
)


def _resolve_command(command: str) -> str | None:
    """Try ``which``, then the venv's bin dir, then the literal path."""
    if os.path.sep in command:
        return command if Path(command).is_file() else None
    found = shutil.which(command)
    if found:
        return found
    sibling = Path(sys.executable).resolve().parent / command
    if sibling.is_file():
        return str(sibling)
    return None


def _resolve_config_file(explicit: str | None) -> str | None:
    if explicit:
        return explicit
    for var in _CONFIG_FILE_ENVS:
        v = os.environ.get(var)
        if v:
            return v
    return None


def _read_mcp_table(config_path: str | None) -> dict[str, Any]:
    """Re-walk the TOML cascade and return the ``[mcp]`` table."""
    from openbb_agent_server.app.config import bootstrap_launcher_config

    try:
        cfg = bootstrap_launcher_config(explicit_path=config_path)
    except Exception:
        logger.debug("mcp_local: could not re-read TOML cascade", exc_info=True)
        return {}
    section = cfg.get("mcp")
    return dict(section) if isinstance(section, dict) else {}


def _ensure_arg(args: list[str], flag: str, value: str) -> list[str]:
    """Append ``[flag, value]`` unless ``flag`` is already present."""
    if flag in args:
        return args
    return [*args, flag, value]


class LocalMcpToolSource(ToolSource):
    """Spawn ``openbb-mcp`` over stdio and surface its tools."""

    name = "mcp_local"

    #: ``openbb-mcp`` defaults to ``streamable-http`` transport when run
    #: with no args. We need stdio because we own the subprocess pipe.
    DEFAULT_ARGS: tuple[str, ...] = ("--transport", "stdio")

    def __init__(
        self,
        *,
        command: str | None = None,
        args: list[str] | None = None,
        env: dict[str, str] | None = None,
        config_file: str | None = None,
    ) -> None:
        self._command = command or "openbb-mcp"
        self._args = list(args) if args is not None else list(self.DEFAULT_ARGS)
        self._env = dict(env or {})
        self._config_file = config_file

    async def tools(self, ctx: RunContext, config: dict[str, Any]) -> list[BaseTool]:
        command = config.get("command", self._command)
        resolved = _resolve_command(command)
        if resolved is None:
            raise RuntimeError(
                f"mcp_local: command {command!r} not found. Install "
                "openbb-mcp-server (it ships the openbb-mcp CLI), or set "
                "[agent.tool_source_config.mcp_local].command to an absolute path."
            )

        # Resolve which TOML the subprocess should read.
        config_file = _resolve_config_file(
            config.get("config_file") or self._config_file
        )

        # Read [mcp] from that cascade for visibility — purely informational.
        # The subprocess gets the same [mcp] via --config-file below.
        mcp_section = _read_mcp_table(config_file)
        if mcp_section:
            logger.debug(
                "mcp_local: forwarding [mcp] to openbb-mcp (keys=%s, config=%s)",
                sorted(mcp_section.keys()),
                config_file or "<cascade>",
            )

        args = list(config.get("args", self._args))
        # Always force stdio (override anything in [mcp].transport).
        if "--transport" not in args:
            args = [*args, "--transport", "stdio"]
        # Forward the config path so the subprocess reads the same [mcp].
        if config_file:
            args = _ensure_arg(args, "--config-file", config_file)

        # Merge: process env -> plugin env -> per-call env -> ctx api_keys.
        sub_env = {**os.environ, **self._env, **dict(config.get("env", {}))}
        sub_env.update(ctx.api_keys)

        client = MultiServerMCPClient(
            connections={
                "openbb": {
                    "transport": "stdio",
                    "command": resolved,
                    "args": args,
                    "env": sub_env,
                }
            }
        )
        return await client.get_tools()
