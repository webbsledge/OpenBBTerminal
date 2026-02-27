"""Tool registry for managing MCP tools and tool discovery."""

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from fastmcp.server.openapi import OpenAPITool

from openbb_mcp_server.models.tools import ToggleResult


@dataclass
class ToolRegistry:
    """Keeps track of categories, subcategories and tool instances.

    In FastMCP v3, enabled/disabled state is managed via server-level
    visibility transforms (``mcp.enable()`` / ``mcp.disable()``).  The
    registry therefore maintains its own ``_enabled_names`` set as the
    source of truth for the admin discovery tools, and synchronises
    changes back to the FastMCP server through ``_mcp``.
    """

    _by_category: dict[str, dict[str, dict[str, OpenAPITool]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(dict))
    )
    _by_name: dict[str, OpenAPITool] = field(default_factory=dict)
    _enabled_names: set[str] = field(default_factory=set)
    _mcp: Any = field(default=None)

    def register_tool(
        self,
        *,
        category: str,
        subcategory: str,
        tool_name: str,
        tool: OpenAPITool,
        enabled: bool = True,
    ) -> None:
        """Register a tool in the registry."""
        self._by_category[category][subcategory][tool_name] = tool
        self._by_name[tool_name] = tool
        if enabled:
            self._enabled_names.add(tool_name)
        else:
            self._enabled_names.discard(tool_name)

    def set_mcp(self, mcp: Any) -> None:
        """Attach the FastMCP server instance for live enable/disable syncing."""
        self._mcp = mcp

    def initialize_enabled(self, enabled_names: set[str]) -> None:
        """Overwrite the enabled set (called once after server creation)."""
        self._enabled_names = set(enabled_names)

    def is_enabled(self, tool_name: str) -> bool:
        """Return True if the tool is currently enabled."""
        return tool_name in self._enabled_names

    def get_categories(self) -> Mapping[str, Mapping[str, Mapping[str, OpenAPITool]]]:
        """Get immutable view of all categories and their tools."""
        return self._by_category

    def get_category_tools(
        self, category: str, subcategory: str | None = None
    ) -> dict[str, OpenAPITool]:
        """Get tools in a category, optionally filtered by subcategory."""
        if subcategory is None:
            # flatten all subcategories
            return {
                name: tool
                for subcat_tools in self._by_category.get(category, {}).values()
                for name, tool in subcat_tools.items()
            }
        return self._by_category.get(category, {}).get(subcategory, {})

    def get_tool(self, tool_name: str) -> OpenAPITool | None:
        """Get a tool by name."""
        return self._by_name.get(tool_name)

    def get_category_subcategories(
        self, category: str
    ) -> dict[str, dict[str, OpenAPITool]] | None:
        """Get all subcategories for a specific category."""
        return self._by_category.get(category)

    def toggle_tools(self, tool_names: list[str], enable: bool) -> ToggleResult:
        """Enable or disable a list of tools, returning a status message."""
        successful, failed = [], []

        for name in tool_names:
            if name in self._by_name:
                if enable:
                    self._enabled_names.add(name)
                else:
                    self._enabled_names.discard(name)
                successful.append(name)
            else:
                failed.append(name)

        # Sync with FastMCP v3 server-level visibility transforms
        if self._mcp and successful:
            names_set = set(successful)
            if enable:
                self._mcp.enable(names=names_set)
            else:
                self._mcp.disable(names=names_set)

        action = "activated" if enable else "deactivated"
        parts: list[str] = []

        if successful:
            parts.append(f"{action.capitalize()}: {', '.join(successful)}")
        if failed:
            parts.append(f"Not found: {', '.join(failed)}")

        message = " ".join(parts) if parts else "No tools processed."

        return ToggleResult(
            action=action,
            successful=successful,
            failed=failed,
            message=message,
        )

    def clear(self) -> None:
        """Clear the registry."""
        self._by_category.clear()
        self._by_name.clear()
