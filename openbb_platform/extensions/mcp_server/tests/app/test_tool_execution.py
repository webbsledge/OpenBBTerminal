"""Tests for tool execution, prompt rendering, and bundled skill integration.

These tests exercise the actual closure-defined tools (discovery and prompt)
and verify that the real bundled skill files render correctly through the
StaticPrompt pipeline.
"""

# pylint: disable=protected-access,unused-argument

import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastmcp.server.openapi import OpenAPITool
from fastmcp.utilities.openapi import HTTPRoute
from mcp.types import TextContent
from openbb_mcp_server.app.app import create_mcp_server
from openbb_mcp_server.models.prompts import StaticPrompt
from openbb_mcp_server.models.registry import ToolRegistry
from openbb_mcp_server.models.settings import MCPSettings

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SKILLS_DIR = Path(__file__).resolve().parent.parent.parent / (
    "openbb_mcp_server" + os.sep + "skills"
)


def _make_tool(name: str, description: str = "desc", enabled: bool = True):
    """Create a minimal OpenAPITool mock with .enabled / .enable / .disable."""
    tool = OpenAPITool(
        MagicMock(),
        HTTPRoute(path=f"/{name}", method="GET"),
        name=name,
        description=description,
        parameters={},
        director=MagicMock(),
    )
    if not enabled:
        tool.disable()
    return tool


def _capture_decorated_tools(mock_mcp_instance):
    """Patch ``mock_mcp_instance.tool`` so the undecorated closures are captured.

    Returns a dict mapping ``func.__name__`` → the original function object.
    """
    decorated: dict[str, object] = {}

    def tool_decorator_factory(*args, **kwargs):
        def decorator(func):
            decorated[func.__name__] = func
            return MagicMock()

        return decorator

    mock_mcp_instance.tool = MagicMock(side_effect=tool_decorator_factory)
    return decorated


def _build_server(
    settings: MCPSettings,
    *,
    registry: ToolRegistry | None = None,
    prompts_json: list | None = None,
):
    """Construct the MCP server with controlled mocks and return helpers.

    Returns ``(mcp_mock, decorated_functions, registry)``.
    """
    fastapi_app = FastAPI()

    mock_processed_data = MagicMock()
    mock_processed_data.route_lookup = {}
    mock_processed_data.route_maps = []
    mock_processed_data.prompt_definitions = prompts_json or []

    mock_mcp_instance = MagicMock()
    mock_mcp_instance._prompt_manager = MagicMock()
    mock_mcp_instance._prompt_manager.render_prompt = AsyncMock()

    decorated = _capture_decorated_tools(mock_mcp_instance)

    if registry is None:
        registry = ToolRegistry()

    with (
        patch(
            "openbb_mcp_server.app.app.process_fastapi_routes_for_mcp",
            return_value=mock_processed_data,
        ),
        patch(
            "openbb_mcp_server.app.app.ToolRegistry",
            return_value=registry,
        ),
        patch(
            "openbb_mcp_server.app.app.FastMCP.from_fastapi",
            return_value=mock_mcp_instance,
        ),
    ):
        create_mcp_server(settings, fastapi_app)

    return mock_mcp_instance, decorated, registry


# ===================================================================
# Discovery tool execution tests
# ===================================================================


class TestAvailableCategories:
    """Tests for the ``available_categories`` discovery tool."""

    def test_returns_categories_with_subcategories(self):
        registry = ToolRegistry()
        registry.register_tool(
            category="equity",
            subcategory="price",
            tool_name="equity_price_historical",
            tool=_make_tool("equity_price_historical"),
        )
        registry.register_tool(
            category="equity",
            subcategory="fundamental",
            tool_name="equity_fundamental_income",
            tool=_make_tool("equity_fundamental_income"),
        )
        registry.register_tool(
            category="economy",
            subcategory="general",
            tool_name="economy_cpi",
            tool=_make_tool("economy_cpi"),
        )

        settings = MCPSettings(enable_tool_discovery=True)  # type: ignore
        _, decorated, _ = _build_server(settings, registry=registry)

        result = decorated["available_categories"]()
        names = {c.name for c in result}
        assert names == {"equity", "economy"}

        equity = next(c for c in result if c.name == "equity")
        assert equity.total_tools == 2
        subcat_names = {s.name for s in equity.subcategories}
        assert subcat_names == {"price", "fundamental"}

    def test_empty_registry(self):
        settings = MCPSettings(enable_tool_discovery=True)  # type: ignore
        _, decorated, _ = _build_server(settings, registry=ToolRegistry())
        assert decorated["available_categories"]() == []


class TestAvailableTools:
    """Tests for the ``available_tools`` discovery tool."""

    def _setup(self):
        registry = ToolRegistry()
        registry.register_tool(
            category="equity",
            subcategory="price",
            tool_name="equity_price_historical",
            tool=_make_tool("equity_price_historical", "Get historical prices"),
        )
        registry.register_tool(
            category="equity",
            subcategory="price",
            tool_name="equity_price_quote",
            tool=_make_tool("equity_price_quote", "Get quote", enabled=False),
        )
        settings = MCPSettings(enable_tool_discovery=True)  # type: ignore
        _, decorated, _ = _build_server(settings, registry=registry)
        return decorated

    def test_list_tools_in_category(self):
        decorated = self._setup()
        result = decorated["available_tools"](category="equity")
        names = {t.name for t in result}
        assert names == {"equity_price_historical", "equity_price_quote"}

    def test_list_tools_in_subcategory(self):
        decorated = self._setup()
        result = decorated["available_tools"](category="equity", subcategory="price")
        assert len(result) == 2

    def test_reflects_active_state(self):
        decorated = self._setup()
        result = decorated["available_tools"](category="equity")
        by_name = {t.name: t for t in result}
        assert by_name["equity_price_historical"].active is True
        assert by_name["equity_price_quote"].active is False

    def test_unknown_category_raises(self):
        decorated = self._setup()
        with pytest.raises(ValueError, match="not found"):
            decorated["available_tools"](category="nonexistent")

    def test_unknown_subcategory_raises(self):
        decorated = self._setup()
        with pytest.raises(ValueError, match="not found"):
            decorated["available_tools"](category="equity", subcategory="options")


class TestToggleTools:
    """Tests for ``activate_tools`` and ``deactivate_tools``."""

    def _setup(self):
        registry = ToolRegistry()
        registry.register_tool(
            category="equity",
            subcategory="price",
            tool_name="equity_price_historical",
            tool=_make_tool("equity_price_historical", enabled=False),
        )
        registry.register_tool(
            category="equity",
            subcategory="price",
            tool_name="equity_price_quote",
            tool=_make_tool("equity_price_quote", enabled=True),
        )
        settings = MCPSettings(enable_tool_discovery=True)  # type: ignore
        _, decorated, reg = _build_server(settings, registry=registry)
        return decorated, reg

    def test_activate_tools(self):
        decorated, registry = self._setup()
        msg = decorated["activate_tools"](tool_names=["equity_price_historical"])
        assert "Activated" in msg
        assert registry.get_tool("equity_price_historical").enabled is True

    def test_deactivate_tools(self):
        decorated, registry = self._setup()
        msg = decorated["deactivate_tools"](tool_names=["equity_price_quote"])
        assert "Deactivated" in msg
        assert registry.get_tool("equity_price_quote").enabled is False

    def test_activate_unknown_tool(self):
        decorated, _ = self._setup()
        msg = decorated["activate_tools"](tool_names=["nonexistent"])
        assert "Not found" in msg

    def test_activate_mixed(self):
        decorated, registry = self._setup()
        msg = decorated["activate_tools"](
            tool_names=["equity_price_historical", "nonexistent"]
        )
        assert "Activated" in msg
        assert "Not found" in msg
        assert registry.get_tool("equity_price_historical").enabled is True

    def test_discovery_tools_not_registered_when_disabled(self):
        settings = MCPSettings(enable_tool_discovery=False)  # type: ignore
        _, decorated, _ = _build_server(settings)
        assert "available_categories" not in decorated
        assert "available_tools" not in decorated
        assert "activate_tools" not in decorated
        assert "deactivate_tools" not in decorated


# ===================================================================
# Prompt tool execution tests
# ===================================================================


class TestListPrompts:
    """Tests for the ``list_prompts`` tool."""

    @pytest.mark.asyncio
    async def test_list_prompts_returns_prompt_data(self):
        settings = MCPSettings()  # type: ignore
        mock_mcp, decorated, _ = _build_server(settings)

        # Simulate mcp.get_prompts() returning a dict of prompt objects
        mock_prompt = MagicMock()
        mock_prompt.name = "test_prompt"
        mock_prompt.tags = {"server"}
        mock_prompt.arguments = [{"name": "arg1", "type": "str"}]
        mock_mcp.get_prompts = AsyncMock(return_value={"test_prompt": mock_prompt})

        result = await decorated["list_prompts"]()
        assert len(result) == 1
        assert result[0]["name"] == "test_prompt"
        assert "server" in result[0]["tags"]

    @pytest.mark.asyncio
    async def test_list_prompts_empty(self):
        settings = MCPSettings()  # type: ignore
        mock_mcp, decorated, _ = _build_server(settings)
        mock_mcp.get_prompts = AsyncMock(return_value={})

        result = await decorated["list_prompts"]()
        assert result == []


class TestExecutePrompt:
    """Tests for the ``execute_prompt`` tool."""

    @pytest.mark.asyncio
    async def test_execute_with_default_args_filled(self):
        """Default argument values from the prompt definition are merged in."""
        prompt_def = {
            "name": "my_prompt",
            "description": "Test",
            "content": "Hello {name}, focus on {aspect}",
            "arguments": [
                {"name": "name", "type": "str"},
                {"name": "aspect", "type": "str", "default": "fundamentals"},
            ],
        }
        settings = MCPSettings()  # type: ignore
        mock_mcp, decorated, _ = _build_server(settings, prompts_json=[prompt_def])

        await decorated["execute_prompt"](
            prompt_name="my_prompt", arguments={"name": "AAPL"}
        )
        mock_mcp._prompt_manager.render_prompt.assert_called_with(
            name="my_prompt",
            arguments={"name": "AAPL", "aspect": "fundamentals"},
        )

    @pytest.mark.asyncio
    async def test_execute_override_default_arg(self):
        """Explicitly passed args override defaults."""
        prompt_def = {
            "name": "my_prompt",
            "description": "Test",
            "content": "Hello {name} {aspect}",
            "arguments": [
                {"name": "name", "type": "str"},
                {"name": "aspect", "type": "str", "default": "fundamentals"},
            ],
        }
        settings = MCPSettings()  # type: ignore
        mock_mcp, decorated, _ = _build_server(settings, prompts_json=[prompt_def])

        await decorated["execute_prompt"](
            prompt_name="my_prompt",
            arguments={"name": "AAPL", "aspect": "technicals"},
        )
        mock_mcp._prompt_manager.render_prompt.assert_called_with(
            name="my_prompt",
            arguments={"name": "AAPL", "aspect": "technicals"},
        )

    @pytest.mark.asyncio
    async def test_execute_unknown_prompt_falls_through(self):
        """An unknown prompt name still calls render_prompt (may raise later)."""
        settings = MCPSettings()  # type: ignore
        mock_mcp, decorated, _ = _build_server(settings)

        await decorated["execute_prompt"](prompt_name="unknown", arguments={"x": 1})
        mock_mcp._prompt_manager.render_prompt.assert_called_with(
            name="unknown", arguments={"x": 1}
        )


# ===================================================================
# Bundled skill rendering tests
# ===================================================================


class TestBundledSkillRendering:
    """Verify each real skill file renders through StaticPrompt without error."""

    EXPECTED_SKILLS = {
        "develop_extension": "Build an OpenBB Platform Extension",
        "build_workspace_app": "Build and Run OpenBB Workspace Applications",
        "configure_mcp_server": "Configure and Build the OpenBB MCP Server",
        "work_with_server": "Working With the OpenBB MCP Server",
    }

    def test_skills_directory_exists(self):
        assert SKILLS_DIR.is_dir(), f"Skills directory not found: {SKILLS_DIR}"

    def test_all_expected_skills_present(self):
        md_files = {f.stem for f in SKILLS_DIR.glob("*.md") if f.stem != "__init__"}
        for name in self.EXPECTED_SKILLS:
            assert name in md_files, f"Missing skill file: {name}.md"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "skill_name,expected_heading",
        list(EXPECTED_SKILLS.items()),
        ids=list(EXPECTED_SKILLS.keys()),
    )
    async def test_skill_renders_without_error(self, skill_name, expected_heading):
        """Each skill file renders to a PromptMessage with full content intact."""
        skill_file = SKILLS_DIR / f"{skill_name}.md"
        content = skill_file.read_text(encoding="utf-8")
        assert len(content) > 100, f"Skill {skill_name} seems too short"

        prompt = StaticPrompt(
            name=skill_name,
            description=expected_heading,
            content=content,
            arguments=None,
            tags={"skill"},
        )
        rendered = await prompt.render()

        assert len(rendered) == 1
        assert rendered[0].role == "user"
        assert isinstance(rendered[0].content, TextContent)
        assert rendered[0].content.text == content

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "skill_name",
        list(EXPECTED_SKILLS.keys()),
        ids=list(EXPECTED_SKILLS.keys()),
    )
    async def test_skill_preserves_curly_braces(self, skill_name):
        """Skills with code blocks containing {} must not raise KeyError."""
        skill_file = SKILLS_DIR / f"{skill_name}.md"
        content = skill_file.read_text(encoding="utf-8")

        prompt = StaticPrompt(
            name=skill_name,
            description="test",
            content=content,
            arguments=None,
            tags={"skill"},
        )
        # This would raise KeyError if str.format() is incorrectly applied
        rendered = await prompt.render()
        assert rendered[0].content.text == content

    @pytest.mark.asyncio
    async def test_skill_render_with_empty_arguments(self):
        """Passing empty dict as arguments should still bypass str.format."""
        skill_file = SKILLS_DIR / "develop_extension.md"
        content = skill_file.read_text(encoding="utf-8")

        prompt = StaticPrompt(
            name="develop_extension",
            description="test",
            content=content,
            arguments=None,
            tags={"skill"},
        )
        # Explicit empty dict should also be safe
        rendered = await prompt.render(arguments={})
        assert rendered[0].content.text == content

    def test_skill_description_from_first_heading(self):
        """Verify the heading extraction logic matches expected descriptions."""
        for skill_name, expected_heading in self.EXPECTED_SKILLS.items():
            skill_file = SKILLS_DIR / f"{skill_name}.md"
            content = skill_file.read_text(encoding="utf-8")
            first_line = content.strip().split("\n")[0]
            # First line should be a markdown heading matching expected
            heading = first_line.lstrip("# ").strip()
            assert (
                heading == expected_heading
            ), f"Skill '{skill_name}' heading mismatch: got '{heading}', expected '{expected_heading}'"


# ===================================================================
# Integration: skills loaded and accessible via prompt tools
# ===================================================================


class TestSkillsIntegration:
    """Test skills loading end-to-end through create_mcp_server."""

    @patch("openbb_mcp_server.app.app.process_fastapi_routes_for_mcp")
    @patch("openbb_mcp_server.app.app.ToolRegistry")
    @patch("openbb_mcp_server.app.app.FastMCP.from_fastapi")
    def test_bundled_skills_registered_as_prompts(
        self, mock_from_fastapi, mock_tool_registry, mock_process_routes
    ):
        """All four bundled skills are registered via mcp.add_prompt."""
        settings = MCPSettings()  # type: ignore  (uses default skills dir)
        fastapi_app = FastAPI()

        mock_processed_data = MagicMock()
        mock_processed_data.route_lookup = {}
        mock_processed_data.route_maps = []
        mock_processed_data.prompt_definitions = []
        mock_process_routes.return_value = mock_processed_data

        mock_registry_instance = MagicMock()
        mock_tool_registry.return_value = mock_registry_instance

        mock_mcp_instance = MagicMock()
        mock_from_fastapi.return_value = mock_mcp_instance

        create_mcp_server(settings, fastapi_app)

        skill_calls = [
            call
            for call in mock_mcp_instance.add_prompt.call_args_list
            if hasattr(call[0][0], "tags") and "skill" in call[0][0].tags
        ]
        skill_names = {call[0][0].name for call in skill_calls}
        assert skill_names == {
            "develop_extension",
            "build_workspace_app",
            "configure_mcp_server",
            "work_with_server",
        }

    @patch("openbb_mcp_server.app.app.process_fastapi_routes_for_mcp")
    @patch("openbb_mcp_server.app.app.ToolRegistry")
    @patch("openbb_mcp_server.app.app.FastMCP.from_fastapi")
    def test_skill_content_not_empty(
        self, mock_from_fastapi, mock_tool_registry, mock_process_routes
    ):
        """Each registered skill has non-trivial content."""
        settings = MCPSettings()  # type: ignore
        fastapi_app = FastAPI()

        mock_processed_data = MagicMock()
        mock_processed_data.route_lookup = {}
        mock_processed_data.route_maps = []
        mock_processed_data.prompt_definitions = []
        mock_process_routes.return_value = mock_processed_data

        mock_registry_instance = MagicMock()
        mock_tool_registry.return_value = mock_registry_instance

        mock_mcp_instance = MagicMock()
        mock_from_fastapi.return_value = mock_mcp_instance

        create_mcp_server(settings, fastapi_app)

        skill_calls = [
            call
            for call in mock_mcp_instance.add_prompt.call_args_list
            if hasattr(call[0][0], "tags") and "skill" in call[0][0].tags
        ]
        for call in skill_calls:
            prompt = call[0][0]
            assert (
                len(prompt.content) > 500
            ), f"Skill '{prompt.name}' content is suspiciously short ({len(prompt.content)} chars)"

    @patch("openbb_mcp_server.app.app.process_fastapi_routes_for_mcp")
    @patch("openbb_mcp_server.app.app.ToolRegistry")
    @patch("openbb_mcp_server.app.app.FastMCP.from_fastapi")
    def test_skill_has_no_arguments(
        self, mock_from_fastapi, mock_tool_registry, mock_process_routes
    ):
        """Skills are static prompts with no arguments."""
        settings = MCPSettings()  # type: ignore
        fastapi_app = FastAPI()

        mock_processed_data = MagicMock()
        mock_processed_data.route_lookup = {}
        mock_processed_data.route_maps = []
        mock_processed_data.prompt_definitions = []
        mock_process_routes.return_value = mock_processed_data

        mock_registry_instance = MagicMock()
        mock_tool_registry.return_value = mock_registry_instance

        mock_mcp_instance = MagicMock()
        mock_from_fastapi.return_value = mock_mcp_instance

        create_mcp_server(settings, fastapi_app)

        skill_calls = [
            call
            for call in mock_mcp_instance.add_prompt.call_args_list
            if hasattr(call[0][0], "tags") and "skill" in call[0][0].tags
        ]
        for call in skill_calls:
            prompt = call[0][0]
            assert (
                prompt.arguments is None
            ), f"Skill '{prompt.name}' should have no arguments"
