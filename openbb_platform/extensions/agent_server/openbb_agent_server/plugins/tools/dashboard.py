"""``dashboard`` tool source — client-side widget / dashboard interactions."""

from __future__ import annotations

from typing import Any

from openbb_agent_server.plugins.tools.client_side import _make_tool
from openbb_agent_server.runtime.context import RunContext
from openbb_agent_server.runtime.plugins import ToolSource

_DEFAULT_TOOLS: tuple[dict[str, Any], ...] = (
    {
        "name": "open_widget",
        "description": (
            "Open / focus a specific widget on the user's current dashboard. "
            "Use when the user asks to 'show me the X widget'."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "widget_id": {
                    "type": "string",
                    "description": "uuid of the widget to focus.",
                }
            },
            "required": ["widget_id"],
        },
    },
    {
        "name": "highlight_widget",
        "description": "Visually highlight a widget for the user.",
        "parameters": {
            "type": "object",
            "properties": {
                "widget_id": {"type": "string"},
                "duration_ms": {"type": "integer"},
            },
            "required": ["widget_id"],
        },
    },
    {
        "name": "change_dashboard",
        "description": "Switch the user to a different dashboard by id.",
        "parameters": {
            "type": "object",
            "properties": {"dashboard_id": {"type": "string"}},
            "required": ["dashboard_id"],
        },
    },
    {
        "name": "add_widget_to_dashboard",
        "description": (
            "Add a new widget to the user's current dashboard. Provide the "
            "widget template id and any required params."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "widget_type": {"type": "string"},
                "params": {"type": "object"},
            },
            "required": ["widget_type"],
        },
    },
)


class DashboardToolSource(ToolSource):
    """Default client-side dashboard interaction tools."""

    name = "dashboard"

    def __init__(self, *, tools: list[dict[str, Any]] | None = None) -> None:
        self._specs = tuple(tools) if tools else _DEFAULT_TOOLS

    async def tools(self, ctx: RunContext, config: dict[str, Any]) -> list[Any]:
        specs = config.get("tools", self._specs)
        return [_make_tool(s) for s in specs]
